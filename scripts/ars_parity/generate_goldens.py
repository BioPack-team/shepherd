"""Generate golden parity fixtures by RUNNING the upstream ARS code.

This script executes inside the pinned NCATSTranslator/Relay checkout using
its own dependency set (.venv-relay -- see docs/ARS_PARITY_REGISTER.md), so
the goldens record what the original functions actually produce for the
corpus, not what we believe they produce.

Usage:
    PYTHONHASHSEED=0 .venv-relay/bin/python scripts/ars_parity/generate_goldens.py \
        [--relay /path/to/relay/checkout]

PYTHONHASHSEED=0 keeps the handful of set-order-dependent upstream code paths
(list(set(...)) unions) deterministic; the comparison harness additionally
falls back to order-insensitive list comparison, so the seed is belt and
braces rather than a hard requirement.
"""

import argparse
import copy
import json
import pathlib
import sys
import types

REPO = pathlib.Path(__file__).resolve().parents[2]
CORPUS = REPO / "tests/fixtures/ars_corpus"
GOLDENS = REPO / "tests/fixtures/ars_goldens"
DEFAULT_RELAY = pathlib.Path("/home/user/ncatstranslator/relay")
RELAY_COMMIT = "3e65975db287a73afa4388b7dbaf3c64d0d218c4"


def bootstrap_django(relay: pathlib.Path):
    sys.path.insert(0, str(relay / "tr_sys"))

    # Stub packages that aren't installable here and aren't needed for the
    # pure functions under test.
    annotator_mod = types.ModuleType("biothings_annotator.annotator")

    class _Annotator:  # pragma: no cover - never called during generation
        async def annotate_curie_list(self, curies):
            raise RuntimeError("annotator stub")

    annotator_mod.Annotator = _Annotator
    pkg = types.ModuleType("biothings_annotator")
    pkg.annotator = annotator_mod
    sys.modules["biothings_annotator"] = pkg
    sys.modules["biothings_annotator.annotator"] = annotator_mod

    # status_report drags in Levenshtein + a local JSON; api.py only needs
    # the module object to import.
    sys.modules["tr_ars.status_report"] = types.ModuleType("tr_ars.status_report")

    import django
    from django.conf import settings as dj_settings

    dj_settings.configure(
        DEBUG=False,
        DATABASES={
            "default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}
        },
        INSTALLED_APPS=["django.contrib.contenttypes", "django.contrib.auth", "tr_ars"],
        USE_CELERY=False,
        DEFAULT_HOST="http://localhost:8000",
        DEFAULT_AUTO_FIELD="django.db.models.AutoField",
        USE_TZ=True,
        CELERY_BROKER_URL="memory://",
        CELERY_RESULT_BACKEND="cache+memory://",
    )
    django.setup()


def jsonable(obj):
    """Round-trip through json with numpy/sympy floats coerced."""

    def coerce(o):
        try:
            return float(o)
        except (TypeError, ValueError):
            return str(o)

    return json.loads(json.dumps(obj, default=coerce))


def load(name):
    return json.loads((CORPUS / name).read_text())


class MesgStub:
    """Just enough of a Message for remove_blocked / phantom removal."""

    def __init__(self, pk="00000000-0000-0000-0000-00000000abcd"):
        self.id = pk
        self.saved = None

    def save_compressed_dict(self, data):
        self.saved = data

    def save(self, *a, **k):
        pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--relay", type=pathlib.Path, default=DEFAULT_RELAY)
    args = parser.parse_args()
    bootstrap_django(args.relay)

    from tr_ars import scoring, utils

    GOLDENS.mkdir(parents=True, exist_ok=True)
    out = {"_relay_commit": RELAY_COMMIT}

    # ---------------- mergeDicts ----------------
    md_cases = []
    for case in load("mergedicts_cases.json"):
        dc = copy.deepcopy(case["dcurrent"])
        dm = copy.deepcopy(case["dmerged"])
        try:
            result = utils.mergeDicts(dc, dm)
            md_cases.append({"name": case["name"], "output": jsonable(result)})
        except Exception as e:  # record error-parity cases
            md_cases.append({"name": case["name"], "raises": type(e).__name__})
    out["mergedicts"] = md_cases

    # ---------------- mergeMessages ----------------
    aragorn = load("response_aragorn.json")
    arax = load("response_arax.json")

    def tmsg(resp):
        return utils.TranslatorMessage(copy.deepcopy(resp["message"]))

    merged_ab = utils.mergeMessages([tmsg(aragorn), tmsg(arax)], "pk-ab")
    out["merge_ab"] = jsonable(merged_ab.to_dict())
    merged_ba = utils.mergeMessages([tmsg(arax), tmsg(aragorn)], "pk-ba")
    out["merge_ba"] = jsonable(merged_ba.to_dict())

    # pipeline-realistic: scrub + decorate + normalize_scores first (what
    # pre_merge_process does to each callback), then merge.
    pipe_a = copy.deepcopy(aragorn)
    pipe_b = copy.deepcopy(arax)
    for resp, infores in ((pipe_a, "infores:aragorn"), (pipe_b, "infores:arax")):
        utils.scrub_null_attributes(resp)
        utils.decorate_edges_with_infores(resp, infores)
        utils.normalize_scores(resp, "k", "agent")
    merged_pipe = utils.mergeMessages(
        [
            utils.TranslatorMessage(copy.deepcopy(pipe_a["message"])),
            utils.TranslatorMessage(copy.deepcopy(pipe_b["message"])),
        ],
        "pk-pipe",
    )
    out["merge_pipeline_ab"] = jsonable(merged_pipe.to_dict())
    out["premerged_inputs"] = {
        "aragorn": jsonable(pipe_a),
        "arax": jsonable(pipe_b),
    }
    out["get_msg_stats"] = jsonable(utils.get_msg_stats(merged_ab.to_dict()))

    # ---------------- scrub_null_attributes ----------------
    scrub = load("scrub_input.json")
    utils.scrub_null_attributes(scrub)
    out["scrub"] = jsonable(scrub)

    # ---------------- decorate_edges_with_infores ----------------
    dec_cases = []
    for case in load("decorate_cases.json"):
        data = copy.deepcopy(case["data"])
        try:
            utils.decorate_edges_with_infores(data, case["inforesid"])
            dec_cases.append({"name": case["name"], "output": jsonable(data)})
        except Exception as e:
            dec_cases.append({"name": case["name"], "raises": type(e).__name__})
    out["decorate"] = dec_cases

    # ---------------- normalizeScores / ScoreStatCalc ----------------
    ns_cases = []
    for case in load("scores_cases.json"):
        entry = {"name": case["name"]}
        try:
            normalized = utils.normalizeScores(copy.deepcopy(case["results"]))
            entry["normalized"] = jsonable(normalized)
        except Exception as e:
            # e.g. IndexError: upstream pops one rank per result but only
            # ranked the results that had scores -- a mixed corpus crashes.
            entry["normalized_raises"] = type(e).__name__
        try:
            entry["stat"] = jsonable(utils.ScoreStatCalc(copy.deepcopy(case["results"])))
        except Exception as e:
            entry["stat_raises"] = type(e).__name__
        ns_cases.append(entry)
    out["scores"] = ns_cases

    # normalize_scores on a full response
    full = copy.deepcopy(aragorn)
    utils.normalize_scores(full, "key", "agent")
    out["normalize_scores_full"] = jsonable(full)

    # ---------------- appraise_confidence / get_confidence ----------------
    # Replaced the external Appraiser call in post_process (Relay PR #884).
    conf_cases = []
    for case in load("scores_cases.json"):
        entry = {"name": case["name"]}
        results = copy.deepcopy(case["results"])
        try:
            utils.appraise_confidence(results)
            entry["output"] = jsonable(results)
        except Exception as e:
            entry["raises"] = type(e).__name__
        conf_cases.append(entry)
    out["appraise_confidence"] = conf_cases

    # ---------------- scoring ----------------
    oc_cases = []
    for case in load("ordering_cases.json"):
        results = copy.deepcopy(case["results"])
        ranked = scoring.compute_from_results(results)
        oc_cases.append({"name": case["name"], "output": jsonable(ranked)})
    out["compute_from_results"] = oc_cases

    sugeno_grid = []
    for conf, nov, clin in [
        (0.9, 0.2, 0.3),
        (0.1, 0.8, 0.0),
        (0.5, 0.0, 0.0),
        (0.0, 0.0, 0.0),
        (1.0, 1.0, 1.0),
        (0.3, 0.3, 0.9),
    ]:
        _, weights, sugeno = scoring.compute_sugeno(conf, nov, clin, 0)
        sugeno_grid.append(
            {
                "input": [conf, nov, clin],
                "weights": jsonable(weights),
                "sugeno": float(sugeno),
                "weighted_mean": float(
                    scoring.compute_weighted_mean(conf, nov, clin, 0)
                ),
            }
        )
    out["sugeno_grid"] = sugeno_grid

    rank_cases = []
    for scores in [[0.5, 0.5, 0.3], [0.1, 0.9, 0.5, 0.9], [1.0], [], [0.2, 0.2, 0.2]]:
        rank_cases.append(
            {
                "scores": scores,
                "rank": jsonable(scoring.compute_sugeno_rank(scores)),
            }
        )
    out["sugeno_rank"] = rank_cases

    # ---------------- remove_blocked ----------------
    blocked = load("response_blocklist.json")
    mesg = MesgStub()
    data = copy.deepcopy(blocked)
    report = utils.remove_blocked(mesg, data)
    out["remove_blocked"] = {
        "data": jsonable(data),
        "report_pk": report[0],
        "removed_node_names": jsonable([n.get("name") for n in report[1]]),
        "removed_result_count": len(report[2]),
    }

    # ---------------- remove_phantom_support_graphs ----------------
    phantom = copy.deepcopy(aragorn)
    # break the aux reference: e2 references aux1, which we delete
    del phantom["message"]["auxiliary_graphs"]["aux1"]
    utils.remove_phantom_support_graphs(phantom)
    out["remove_phantom"] = jsonable(phantom)
    intact = copy.deepcopy(aragorn)
    utils.remove_phantom_support_graphs(intact)
    out["remove_phantom_intact"] = jsonable(intact)

    # ---------------- filters ----------------
    fin = load("filters_input.json")
    out["filters"] = {
        "hop_3": jsonable(
            utils.hop_level_filter(copy.deepcopy(fin["results"]), 3)
        ),
        "hop_4": jsonable(
            utils.hop_level_filter(copy.deepcopy(fin["results"]), 4)
        ),
        "score_20_80": jsonable(
            utils.score_filter(copy.deepcopy(fin["results"]), [20, 80])
        ),
        "node_type_gene": jsonable(
            utils.node_type_filter(
                copy.deepcopy(fin["kg_nodes"]),
                copy.deepcopy(fin["results"]),
                ["Gene"],
            )
        ),
        "node_type_chemical": jsonable(
            utils.node_type_filter(
                copy.deepcopy(fin["kg_nodes"]),
                copy.deepcopy(fin["results"]),
                ["ChemicalEntity"],
            )
        ),
        "spec_node": jsonable(
            utils.specific_node_filter(
                copy.deepcopy(fin["results"]), ["NCBIGene:5468"]
            )
        ),
    }

    # ---------------- validate (verdict parity) ----------------
    verdicts = {}
    verdicts["aragorn"] = utils.validate(copy.deepcopy(aragorn))
    verdicts["arax"] = utils.validate(copy.deepcopy(arax))
    verdicts["empty"] = utils.validate(load("response_empty.json"))
    verdicts["blocklist"] = utils.validate(copy.deepcopy(blocked))

    broken_no_message = {"foo": "bar"}
    verdicts["no_message"] = utils.validate(broken_no_message)

    broken_edge = copy.deepcopy(aragorn)
    del broken_edge["message"]["knowledge_graph"]["edges"]["e1"]["predicate"]
    verdicts["edge_missing_predicate"] = utils.validate(broken_edge)

    broken_result = copy.deepcopy(aragorn)
    del broken_result["message"]["results"][0]["node_bindings"]
    verdicts["result_missing_node_bindings"] = utils.validate(broken_result)

    broken_qg = copy.deepcopy(aragorn)
    broken_qg["message"]["query_graph"]["nodes"]["sn"]["categories"] = "notalist"
    verdicts["qg_categories_not_list"] = utils.validate(broken_qg)

    broken_binding = copy.deepcopy(aragorn)
    del broken_binding["message"]["results"][0]["node_bindings"]["sn"][0]["id"]
    verdicts["binding_missing_id"] = utils.validate(broken_binding)

    broken_kg = copy.deepcopy(aragorn)
    broken_kg["message"]["knowledge_graph"] = {"nodes": {}}
    verdicts["kg_missing_edges"] = utils.validate(broken_kg)

    results_not_list = copy.deepcopy(aragorn)
    results_not_list["message"]["results"] = {"a": 1}
    verdicts["results_not_list"] = utils.validate(results_not_list)

    out["validate"] = verdicts

    path = GOLDENS / "goldens.json"
    path.write_text(json.dumps(out, indent=2, sort_keys=True) + "\n")
    print(f"wrote {path} ({path.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
