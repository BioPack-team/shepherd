"""TRAPI 2.0 goldens for the ARAX parity tests, derived offline from upstream's 1.6 record.

The parity goldens record what upstream ARAX (RTXteam/RTX @ 9485431, TRAPI 1.6)
does: each suite's ``run_upstream.py`` ran upstream's own code on the recorded
TRAPI 1.6 inputs (``inputs_trapi16.json.gz``) against the mock Retriever in its
1.6 mode, and wrote ``goldens_trapi16.json.gz``. Shepherd's ARAX port speaks
TRAPI 2.0, and upstream cannot be run on 2.0, so the goldens the parity tests
compare against (``goldens.json.gz``) are *translations* of that record, made
here with TOM's own 1.6 -> 2.0 dict transforms
(``translator_tom.model_dicts.dict_up_version``) plus the hand rules below.
The 2.0 inputs the port runs on (the case modules and the mock Retriever's 2.0
mode) are the same translation of the recorded inputs; test_trapi2_goldens.py
checks all three stay in step.

Regenerating (both steps are deterministic)::

    # 1. the upstream record (needs an RTX checkout, see each run_upstream.py)
    PYTHONHASHSEED=0 RTX_CODE=/path/to/RTX/code python tests/unit/arax/expand_parity/run_upstream.py
    ...                                          # query_parity, response_parity, aux_parity
    # 2. the 2.0 goldens (needs translator_tom, i.e. the Shepherd venv)
    python tests/unit/arax/trapi2_goldens.py [--check]

What TOM does, per recorded field:
  - envelopes (query_parity ``envelope``, response_parity lookups) -> Response;
    KP request bodies (``requests[].body``) and the query plan's recorded
    Retriever ``query`` -> Query; expand_parity ``qg`` -> QueryGraph, ``kg`` ->
    KnowledgeGraph, ``aux`` -> AuxiliaryGraph each.
  - node / edge bindings collapse to one ``{"ids": [...]}`` per qnode / qedge
    (NodeBinding.query_id and binding attributes are gone), and an Analysis with
    no edge bindings loses ``edge_bindings``;
  - ``biolink:knowledge_level`` / ``biolink:agent_type`` attributes become the
    required top-level Edge members, ``not_provided`` when the edge had none
    (every Retriever edge of the mock universe, and ARAX's own subclass_of
    edges; but see H9);
  - ``attribute_constraints`` / ``qualifier_constraints`` fold into the QEdge's
    ``constraints`` object (a qualifier set becomes a plain ``{type: value}``
    dict), ``intermediate_categories`` becomes ``required_intermediate_categories``;
  - aux graphs lose ``attributes``; ``schema_version`` becomes ``2.0.0``;
  - nulls are dropped (2.0 has none; the port's ``to_dict`` omits None), and so
    are the empty optional containers 2.0 forbids (``logs``, qnode
    ``constraints``, qedge ``attribute_constraints``, ``analyses``, ...).

Hand rules on top (what TOM does not own):
  - H1 permitted empties are kept: TOM's prune drops every empty optional array,
    but 2.0 allows (and for ``results`` recommends) ``results: []``, an empty
    ``knowledge_graph.edges``, and ``attributes: []`` on nodes, edges, analyses
    and sub-attributes. Upstream emits them and the port keeps upstream's code
    there, so the translation keeps them (as the ARS goldens do,
    scripts/ars_parity/trapi2.py).
  - H2 an empty query graph is dropped: ARAX's template envelope (every error
    before a query graph exists) has ``query_graph: {nodes: {}, edges: {}}``;
    2.0 requires ``nodes`` to be non-empty, and ``query_graph`` is optional, so
    the 2.0 envelope has no ``query_graph``. (Only for envelopes ARAX made: a
    stored document /response serves back keeps what it has.)
  - H3 a recorded Query's own ``parameters`` are kept when TOM moves
    ``log_level`` / ``bypass_cache`` into them (TOM replaces the object).
  - H4 ARAX-internal records are not TRAPI and are kept verbatim: expand_parity
    ``node_qnode_keys``, ``edge_qedge_keys``, ``qg_filled``, ``kryptonite`` and
    ``node_query_ids`` (see below); the log lines (but H4b) and messages (ARAX's own
    text: none quotes a TRAPI 1.x member of the exchanged objects; the ARAXi
    parameter names they echo, like ``is_set``, are still ARAXi's).
  - H4b the one log line that pretty-prints a TRAPI model's ``to_dict()``
    (Expand's unsupported-constraint error) loses its ``'x': None`` members,
    as the port's ``to_dict`` omits None (``log_text``).
  - H5 ``node_query_ids`` (ARAX's in-memory ``Node.query_ids``: which query
    curie a KG node fulfils) is kept as recorded. 2.0 has no
    ``NodeBinding.query_id``, so the mock Retriever's 2.0 answers carry none
    (TOM drops it) and ARAX falls back on its own rule for a binding without
    one (``trapi_querier._get_kg_to_qg_mappings_from_results``): a qnode with a
    single id implies that id -- the same parent upstream recorded, so every
    such case keeps its golden. Where a subclass child binds to a qnode with
    several ids, 2.0 cannot say which parent it fulfils (the port takes it from
    a subclass_of KG edge when Retriever sends one; the mock sends none, as
    upstream's never did); those cases (``QUERY_ID_LOST``, pairs from
    ``query_id_lost_pairs``) are TRAPI 2.0 differences, pinned in the parity
    tests.
  - H6 response_parity: ARAX's ``validation_result.version`` (and the fake
    validator's ``info.checked.<trapi>.<biolink>`` key and dumped text) name
    the TRAPI version ARAX validates against: ``2.0.0`` (ARAX's only valid
    version under TRAPI 2.0; the ``local_trapi_1_5`` stored response still
    declares ``1.5.0``, which is no longer a valid version, so it is validated
    as the default ``2.0.0`` too, and keeps declaring ``1.5.0``).
  - H7 aux_parity: the meta-KG (and its simple view) and autocomplete answers
    are the same objects in 2.0 (a MetaKnowledgeGraph shape change would be
    additions only); the goldens are carried over unchanged.
  - H8 response_parity: in ARAX's attribute-stripped (``X``) view, a KG edge
    keeps the stored edge's ``knowledge_level`` / ``agent_type`` (see
    ``_unstrip_levels``).
  - H9 the edges ARAX's overlays make without KL/AT attributes upstream get
    the values the 2.0 port gives them (ARAX_EDGE_LEVELS: Jaccard and Fisher's
    exact test ``statistical_association`` / ``automated_agent``, as NGD's
    own; COHD ``statistical_association`` / ``data_analysis_pipeline``)
    instead of TOM's ``not_provided``.
  - H11 an Analysis with neither ``edge_bindings`` nor ``path_bindings`` is
    dropped, and a result's emptied ``analyses`` with it: TRAPI 2.0 requires
    an Analysis to have one or both (a schema anyOf, which the ARS validator
    and ``prune_response`` enforce; TOM's model does not). Upstream gives a
    one-node query's result (tpl_one_node) such an analysis, carrying the
    result's score (``{"resource_id": "infores:arax", "score": 1.0}``): that
    score is lost with it in 2.0. The same holds for the mock Retriever's
    one-node answers.
  - H12 a qnode whose recorded ``is_set: true`` the 2.0 case expresses as
    ``set_interpretation: COLLATE`` (``collated_qnodes``) echoes both in the
    envelope's query graph: the vendored QNode reads COLLATE as ``is_set``
    and keeps the client's own value.
  - H10 nulls go wherever the port's ``to_dict`` omits them, which is in
    every ARAX model object, including ARAX's own envelope ``operations``
    (an Operations model TOM sees as an opaque extra); plain-dict members
    (``query_options``, ``validation_result``) keep theirs, as the port does.

The case inputs (``expand_case``, ``query_case``, ``response_case``) are the
same translation, plus: an unpinned qnode's ``is_set: true`` becomes
``set_interpretation: COLLATE``; a stored log entry's timestamp gains its
``Z`` offset; a stored response kept as JSON text in an ARS child's logs is
translated too.
"""

import base64
import copy
import gzip
import json
import os
import re
import sys
from typing import Any

HERE = os.path.dirname(os.path.abspath(__file__))
SUITES = ("expand_parity", "query_parity", "response_parity", "aux_parity")
KL = "biolink:knowledge_level"
AT = "biolink:agent_type"

# H5: (suite, case) whose upstream run relied on a NodeBinding.query_id for a
# subclass child bound to a qnode with several ids, which TRAPI 2.0 cannot
# express. Computed by query_id_lost_cases() and pinned by the tests.
QUERY_ID_LOST = {
    "expand_parity": (
        "affects_qualified",
        "fda_constraint",
        "fda_constraint_not",
        "kryptonite",
        "ks_allowlist_retriever",
        "ks_denylist",
        "prune_threshold",
        "three_hop",
    ),
    "query_parity": (
        "filter_kg_after_expand_discrete_and_property",
        "filter_kg_after_expand_numeric",
        "wf_filter_kgraph_after_fill",
    ),
}


# ---------------------------------------------------------------------------
# Recorded inputs / goldens on disk
# ---------------------------------------------------------------------------


def _encode(obj: Any) -> Any:
    """JSON-safe form of a case structure (tuples, bytes, non-string keys)."""
    if isinstance(obj, tuple):
        return {"$tuple": [_encode(x) for x in obj]}
    if isinstance(obj, bytes):
        return {"$bytes": base64.b64encode(obj).decode()}
    if isinstance(obj, list):
        return [_encode(x) for x in obj]
    if isinstance(obj, dict):
        if all(isinstance(k, str) for k in obj):
            return {k: _encode(v) for k, v in obj.items()}
        return {"$dict": [[_encode(k), _encode(v)] for k, v in obj.items()]}
    return obj


def _decode(obj: Any) -> Any:
    if isinstance(obj, list):
        return [_decode(x) for x in obj]
    if isinstance(obj, dict):
        if set(obj) == {"$tuple"}:
            return tuple(_decode(x) for x in obj["$tuple"])
        if set(obj) == {"$bytes"}:
            return base64.b64decode(obj["$bytes"])
        if set(obj) == {"$dict"}:
            return {_decode(k): _decode(v) for k, v in obj["$dict"]}
        return {k: _decode(v) for k, v in obj.items()}
    return obj


def write_json_gz(path: str, obj: Any) -> None:
    """Byte-for-byte reproducible: sorted keys, no gzip timestamp or name."""
    data = json.dumps(obj, sort_keys=True).encode()
    with open(path, "wb") as f:
        f.write(gzip.compress(data, mtime=0))


def read_json_gz(path: str) -> Any:
    with gzip.open(path, "rt") as f:
        return json.load(f)


def load_inputs16(suite: str) -> dict:
    """The TRAPI 1.6 inputs upstream's goldens were recorded with (as the case
    modules held them), e.g. ``load_inputs16("query_parity")["CASES"]``."""
    return _decode(read_json_gz(os.path.join(HERE, suite, "inputs_trapi16.json.gz")))


def write_inputs16(suite: str, **values: Any) -> None:
    write_json_gz(os.path.join(HERE, suite, "inputs_trapi16.json.gz"), _encode(values))


def golden_paths(suite: str) -> tuple:
    return (
        os.path.join(HERE, suite, "goldens_trapi16.json.gz"),
        os.path.join(HERE, suite, "goldens.json.gz"),
    )


# ---------------------------------------------------------------------------
# TOM + the hand rules, per TRAPI object
# ---------------------------------------------------------------------------


def _tom(data: dict, source_name: str) -> dict:
    from translator_tom import v1_6  # the Shepherd venv (not upstream's)
    from translator_tom.model_dicts import dict_up_version

    return dict_up_version(copy.deepcopy(data), getattr(v1_6, source_name))


def _restore_attributes(original: Any, new: Any, lifted: bool = False) -> None:
    """H1 on an attribute list: sub-attribute ``attributes: []`` kept.

    ``lifted``: the list is an edge's, from which TOM removed KL/AT."""
    if not isinstance(original, list) or not isinstance(new, list):
        return
    if lifted:
        original = [
            a
            for a in original
            if not (isinstance(a, dict) and a.get("attribute_type_id") in (KL, AT))
        ]
    for a, b in zip(original, new):
        if not isinstance(a, dict) or not isinstance(b, dict):
            continue
        if a.get("attributes") == [] and "attributes" not in b:
            b["attributes"] = []
        _restore_attributes(a.get("attributes"), b.get("attributes"))


def _restore_element_attributes(original: Any, new: Any, lifted: bool) -> None:
    """H1 on a map of nodes / edges (/ a list of analyses): ``attributes``."""
    if isinstance(original, dict) and isinstance(new, dict):
        pairs = [(original.get(k), v) for k, v in new.items()]
    elif isinstance(original, list) and isinstance(new, list):
        pairs = list(zip(original, new))
    else:
        return
    for a, b in pairs:
        if not isinstance(a, dict) or not isinstance(b, dict):
            continue
        if isinstance(a.get("attributes"), list) and "attributes" not in b:
            # an edge whose only attributes were KL/AT keeps an empty list
            b["attributes"] = []
        _restore_attributes(a.get("attributes"), b.get("attributes"), lifted)


def knowledge_graph(original: dict, new: dict = None) -> dict:
    """A 1.6 KnowledgeGraph dict in 2.0 (``new``: TOM's output, when already made)."""
    if new is None:
        new = _tom(original, "KnowledgeGraph")
    if isinstance(original.get("edges"), dict) and "edges" not in new:
        new["edges"] = {}
    _restore_element_attributes(original.get("nodes"), new.get("nodes"), False)
    _restore_element_attributes(original.get("edges"), new.get("edges"), True)
    for key, edge in (new.get("edges") or {}).items():
        levels = arax_edge_levels(original["edges"][key])
        if levels:
            edge["knowledge_level"], edge["agent_type"] = levels
    return new


# H9: (predicate, primary knowledge source) of the edges ARAX makes itself
# without KL/AT attributes upstream, and the KL/AT the 2.0 port gives them
# (2.0 requires both on every edge). NGD, xDTD and the others upstream already
# stamped; TOM lifts theirs.
ARAX_EDGE_LEVELS = {
    ("biolink:has_jaccard_index_with", "infores:arax"): (
        "statistical_association",
        "automated_agent",
    ),
    ("biolink:has_fisher_exact_test_p_value_with", "infores:arax"): (
        "statistical_association",
        "automated_agent",
    ),
    ("biolink:associated_with", "infores:cohd"): (
        "statistical_association",
        "data_analysis_pipeline",
    ),
}


def arax_edge_levels(edge: dict):
    """H9: the 2.0 (knowledge_level, agent_type) of an ARAX-made 1.6 edge
    that had no KL/AT attributes, else None (TOM's lift / default stands)."""
    if any(
        a.get("attribute_type_id") in (KL, AT)
        for a in edge.get("attributes") or []
        if isinstance(a, dict)
    ):
        return None
    primary = [
        s.get("resource_id")
        for s in edge.get("sources") or []
        if s.get("resource_role") == "primary_knowledge_source"
    ]
    for source in primary:
        levels = ARAX_EDGE_LEVELS.get((edge.get("predicate"), source))
        if levels:
            return levels
    return None


def message(original: dict, new: dict, produced: bool = True) -> dict:
    """H1/H2 on a Message TOM already translated."""
    if isinstance(original.get("results"), list) and "results" not in new:
        new["results"] = []
    for result, new_result in zip(
        original.get("results") or [], new.get("results") or []
    ):
        _restore_element_attributes(
            result.get("analyses"), new_result.get("analyses"), False
        )
    for new_result in new.get("results") or []:  # H11
        analyses = [
            a
            for a in new_result.get("analyses") or []
            if a.get("edge_bindings") or a.get("path_bindings")
        ]
        if analyses:
            new_result["analyses"] = analyses
        else:
            new_result.pop("analyses", None)
    if isinstance(original.get("knowledge_graph"), dict) and isinstance(
        new.get("knowledge_graph"), dict
    ):
        knowledge_graph(original["knowledge_graph"], new["knowledge_graph"])
    qg = new.get("query_graph")
    if produced and isinstance(qg, dict) and not qg.get("nodes"):
        del new["query_graph"]  # H2
    return new


def response(original: Any, produced: bool = True) -> Any:
    """A recorded 1.6 Response dict (envelope) in 2.0.

    ``produced``: ARAX made it (H2 applies); a document ARAX only stores and
    serves back (response_parity) keeps an empty query graph as it is."""
    if not isinstance(original, dict) or not isinstance(original.get("message"), dict):
        return copy.deepcopy(original)
    new = _tom(original, "Response")
    message(original["message"], new["message"], produced)
    if isinstance(new.get("operations"), dict):  # H10
        new["operations"] = {
            k: v for k, v in new["operations"].items() if v is not None
        }
    return new


def query(original: Any) -> Any:
    """A recorded 1.6 Query dict (a KP request body, a case) in 2.0."""
    if not isinstance(original, dict):
        return copy.deepcopy(original)
    if not isinstance(original.get("message"), dict):
        # an ARAXi-only query (operations, no message): TOM's Query needs one
        new = _tom(dict(original, message={}), "Query")
        new.pop("message", None)
    else:
        new = _tom(original, "Query")
        message(original["message"], new["message"])
    if isinstance(original.get("parameters"), dict):  # H3
        new["parameters"] = {**original["parameters"], **new.get("parameters", {})}
    return new


def query_graph(original: Any) -> Any:
    if not isinstance(original, dict):
        return copy.deepcopy(original)
    return _tom(original, "QueryGraph")


def auxiliary_graphs(original: Any) -> Any:
    if not isinstance(original, dict):
        return copy.deepcopy(original)
    return {k: _tom(v, "AuxiliaryGraph") for k, v in original.items()}


def query_plan(plan: Any) -> Any:
    """ARAX's query plan (as recorded, normalized): each KP entry's ``query``."""
    if not isinstance(plan, dict):
        return copy.deepcopy(plan)
    out = copy.deepcopy(plan)
    for entries in out.values():
        for entry in entries.values():
            if isinstance(entry, dict) and isinstance(entry.get("query"), dict):
                entry["query"] = query(entry["query"])
    return out


def requests(recorded: list) -> list:
    return [dict(r, body=query(r["body"])) for r in recorded]


# ---------------------------------------------------------------------------
# Per suite
# ---------------------------------------------------------------------------


def expand_record(rec: dict) -> dict:
    out = copy.deepcopy(rec)  # H4: the ARAX-internal fields as recorded
    out["qg"] = query_graph(rec["qg"])
    out["kg"] = knowledge_graph(rec["kg"]) if isinstance(rec["kg"], dict) else None
    out["aux"] = auxiliary_graphs(rec["aux"])
    out["plan"] = query_plan(rec["plan"])
    out["requests"] = requests(rec["requests"])
    out["logs"] = [(level, log_text(text)) for level, text in rec["logs"]]
    return out


def log_text(text: str) -> str:
    """H4b: a log line that pretty-prints a model's to_dict() (Expand's
    unsupported-constraint error quotes the AttributeConstraint) loses the
    None members, which the port's to_dict omits under 2.0."""
    if text.startswith("Unsupported constraint(s) detected"):
        text = re.sub(r"\n '[A-Za-z_]+': None,", "", text)
    return text


def expand_goldens(g16: dict) -> dict:
    return {case: expand_record(rec) for case, rec in g16.items()}


def collated_qnodes(case: tuple) -> list:
    """The qnode keys of a recorded query that query_case turned from an
    unpinned ``is_set: true`` into ``set_interpretation: COLLATE``."""
    qg = (case[1].get("message") or {}).get("query_graph") or {}
    return sorted(
        key
        for key, qnode in (qg.get("nodes") or {}).items()
        if qnode.get("is_set") is True and not qnode.get("ids")
    )


def query_goldens(g16: dict) -> dict:
    collated = {
        c[0]: collated_qnodes(c) for c in load_inputs16("query_parity")["CASES"]
    }
    out = {}
    for case, rec in g16.items():
        new = copy.deepcopy(rec)
        new["envelope"] = response(rec["envelope"])
        qg = new["envelope"]["message"].get("query_graph") or {}
        for key in collated.get(case, []):  # H12
            if key in (qg.get("nodes") or {}):
                qg["nodes"][key]["set_interpretation"] = "COLLATE"
        qo = new["envelope"].get("query_options")
        if isinstance(qo, dict) and "query_plan" in qo:
            qo["query_plan"] = query_plan(qo["query_plan"])
        new["requests"] = requests(rec["requests"])
        out[case] = new
    return out


def _validation_version(result: Any) -> Any:
    """H6."""
    if not isinstance(result, dict) or not isinstance(
        result.get("validation_result"), dict
    ):
        return result
    text = json.dumps(result["validation_result"], sort_keys=True)
    for old in ("1.6.0", "1.5.0"):
        text = text.replace(f'"version": "{old}"', '"version": "2.0.0"')
        text = text.replace(f"info.checked.{old}.", "info.checked.2.0.0.")
    result["validation_result"] = json.loads(text)
    return result


def response_lookup(result: Any) -> Any:
    """One /response/{id} lookup result: an envelope, a Z component, an error."""
    if isinstance(result, dict) and isinstance(result.get("message"), dict):
        declared = result.get("schema_version")
        new = response(result, produced=False)
        if declared not in (None, "1.6.0"):
            new["schema_version"] = declared  # H6: a stored 1.5.0 stays 1.5.0
        return _validation_version(new)
    return _validation_version(copy.deepcopy(result))


def _stored_edge_levels(data: dict) -> dict:
    """{edge key: (knowledge_level, agent_type)} of every stored / fetched
    document a response case can reach, as they are in 2.0 (H8)."""
    levels = {}
    docs = [doc for _, doc in (data.get("local") or {}).values()]
    for _, raw in list((data.get("urls") or {}).values()) + list(
        (data.get("ars") or {}).values()
    ):
        try:
            value = json.loads(raw)
        except ValueError:
            continue
        if isinstance(value, dict) and isinstance(value.get("fields"), dict):
            value = value["fields"].get("data")
        docs.append(value)
    for doc in docs:
        if not isinstance(doc, dict) or not isinstance(doc.get("message"), dict):
            continue
        kg = response(doc, produced=False)["message"].get("knowledge_graph") or {}
        for key, edge in (kg.get("edges") or {}).items():
            levels[key] = (edge["knowledge_level"], edge["agent_type"])
    return levels


def _unstrip_levels(result: Any, levels: dict) -> Any:
    """H8: ARAX's X view strips an edge's ``attributes`` and ``sources`` (and
    adds ``detail_lookup``). Upstream's stripped edges thus lost their KL/AT
    attributes, which TOM reads as ``not_provided``; in 2.0 they are edge
    members, not attributes, and the stripped view keeps the stored edge's."""
    if not isinstance(result, dict) or not isinstance(result.get("message"), dict):
        return result
    kg = result["message"].get("knowledge_graph") or {}
    for key, edge in (kg.get("edges") or {}).items():
        if "detail_lookup" in edge and key in levels:
            edge["knowledge_level"], edge["agent_type"] = levels[key]
    return result


def response_goldens(g16: dict) -> dict:
    cases = {c[0]: c for c in load_inputs16("response_parity")["CASES"]}
    out = {}
    for case, results in g16.items():
        levels = _stored_edge_levels(cases[case][2])
        out[case] = [_unstrip_levels(response_lookup(r), levels) for r in results]
    return out


def aux_goldens(g16: dict) -> dict:
    return copy.deepcopy(g16)  # H7


TRANSLATE = {
    "expand_parity": expand_goldens,
    "query_parity": query_goldens,
    "response_parity": response_goldens,
    "aux_parity": aux_goldens,
}


def translate(suite: str) -> dict:
    """The 2.0 goldens of a suite, from its recorded 1.6 goldens."""
    return TRANSLATE[suite](read_json_gz(golden_paths(suite)[0]))


# ---------------------------------------------------------------------------
# Inputs (for the equivalence test): the 2.0 cases are this of the 1.6 ones
# ---------------------------------------------------------------------------


def expand_case(case: tuple) -> tuple:
    name, qg, params, qo = case
    return (name, query_graph(qg), params, qo)


def query_case(case: tuple) -> tuple:
    """A recorded ARAXQuery input in 2.0. ARAXi text is not TRAPI and stays.

    Hand rule: an unpinned qnode's 1.x ``is_set: true`` is 2.0's
    ``set_interpretation: COLLATE`` (the one 2.0 word for "collate the matching
    nodes into one result"); ARAX reads it as its ``is_set``, so the envelope
    echoes ``is_set: true`` as upstream recorded."""
    name, q = case
    new = query(q)
    qg = (new.get("message") or {}).get("query_graph") or {}
    for qnode in (qg.get("nodes") or {}).values():
        if qnode.get("is_set") is True and not qnode.get("ids"):
            del qnode["is_set"]
            qnode["set_interpretation"] = "COLLATE"
    return (name, new)


def response_case(case: tuple) -> tuple:
    """A /response case: every stored or fetched TRAPI document in 2.0."""
    name, steps, data = case
    data = copy.deepcopy(data)

    def doc(value):
        if isinstance(value, dict) and isinstance(value.get("logs"), list):
            # ARAX reads an ARS child whose logs are strings as the JSON
            # response held by the first one
            value = dict(value, logs=[text(entry) for entry in value["logs"]])
        value = response_lookup(value)
        for entry in (value.get("logs") or []) if isinstance(value, dict) else []:
            # 2.0 timestamps carry an offset (the lookup normalizes them away)
            if isinstance(entry, dict) and len(entry.get("timestamp") or "") == 19:
                entry["timestamp"] += "Z"
        return value

    def text(entry):
        try:
            parsed = json.loads(entry) if isinstance(entry, str) else None
        except ValueError:
            return entry
        if isinstance(parsed, dict) and isinstance(parsed.get("message"), dict):
            return json.dumps(doc(parsed))
        return entry

    def body(raw):
        try:
            value = json.loads(raw)
        except ValueError:
            return raw
        if isinstance(value, dict) and isinstance(value.get("fields"), dict):
            fields = value["fields"]  # an ARS message: its data is the response
            fields["data"] = doc(fields.get("data"))
        elif isinstance(value, dict):
            value = doc(value)
        return json.dumps(value).encode()

    for key, (arax_id, envelope) in (data.get("local") or {}).items():
        data["local"][key] = (arax_id, doc(envelope))
    for url, (status, raw) in (data.get("urls") or {}).items():
        data["urls"][url] = (status, body(raw))
    for key, (status, raw) in (data.get("ars") or {}).items():
        data["ars"][key] = (status, body(raw))
    return (name, steps, data)


# ---------------------------------------------------------------------------
# H5 analysis
# ---------------------------------------------------------------------------


def query_id_lost_pairs(requests: list) -> dict:
    """{KG node: parents} that a case's recorded exchange named only through a
    NodeBinding.query_id on a qnode with several ids -- what TRAPI 2.0 cannot
    carry. Replays each recorded request through the mock Retriever's 1.6
    mode; a parent the same node also got from a single-id qnode (implied in
    2.0 as well) is not lost."""
    sys.path.insert(0, os.path.join(HERE, "expand_parity"))
    import mock_retriever

    lost, kept = {}, {}
    for r in requests:
        code, answer = mock_retriever.answer(r["body"], trapi="1.6")
        if code != 200:
            continue
        qnodes = r["body"]["message"]["query_graph"]["nodes"]
        for result in answer["message"]["results"]:
            for qk, bindings in result["node_bindings"].items():
                ids = qnodes[qk].get("ids") or []
                for b in bindings:
                    if "query_id" in b and len(ids) > 1:
                        lost.setdefault(b["id"], set()).add(b["query_id"])
                    elif len(ids) == 1:
                        kept.setdefault(b["id"], set()).add(ids[0])
    out = {}
    for node, parents in lost.items():
        parents = parents - kept.get(node, set())
        if parents:
            out[node] = sorted(parents)
    return out


def query_id_lost_cases(suite: str) -> list:
    """The cases with query_id_lost_pairs (QUERY_ID_LOST)."""
    return sorted(
        case
        for case, rec in read_json_gz(golden_paths(suite)[0]).items()
        if query_id_lost_pairs(rec.get("requests") or [])
    )


def main(argv: list) -> int:
    check = "--check" in argv
    stale = []
    for suite in SUITES:
        new = translate(suite)
        path = golden_paths(suite)[1]
        if check:
            if read_json_gz(path) != json.loads(json.dumps(new, sort_keys=True)):
                stale.append(suite)
        else:
            write_json_gz(path, new)
            print(f"wrote {os.path.relpath(path, HERE)} ({len(new)} cases)")
    if stale:
        print("stale 2.0 goldens (re-run without --check):", ", ".join(stale))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))


# ---------------------------------------------------------------------------
# TOM validation of the 2.0 goldens
# ---------------------------------------------------------------------------


def _check(kind: str, obj: Any, where: str, errors: list) -> None:
    from translator_tom import v2_0

    try:
        getattr(v2_0, kind).from_dict(copy.deepcopy(obj))
    except Exception as e:  # pydantic's ValidationError, or a TOM error
        errors.append(f"{where}: {kind}: {str(e).splitlines()[:3]}")


def _check_plan(plan: Any, where: str, errors: list) -> None:
    for qedge, entries in (plan or {}).items():
        for kp, entry in entries.items():
            if isinstance(entry, dict) and isinstance(entry.get("query"), dict):
                _check("Query", entry["query"], f"{where}.{qedge}.{kp}.query", errors)


# Stored response_parity documents that are deliberately not valid 2.0: the
# fixture's edges k1 (provenance only in a legacy primary_knowledge_source
# attribute) and k2 (no provenance at all) exercise ARAX's provenance summary
# on documents it did not write (an ARS child can be anything); 2.0 requires
# Edge.sources, so they are kept as they are and set aside here.
LEGACY_PROVENANCE_EDGES = ("k1", "k2")


def response_doc_for_validation(doc: dict) -> dict:
    """What of a /response lookup result is TRAPI, for validation.

    Set aside: the lookup's own normalized log timestamps ("T"); ARAX's X
    view (attributes and sources stripped, ``detail_lookup`` /
    ``has_these_support_graphs`` added -- a UI view, not TRAPI), whose edges
    are dropped; and LEGACY_PROVENANCE_EDGES."""
    doc = copy.deepcopy(doc)
    for entry in doc.get("logs") or []:
        if isinstance(entry, dict) and entry.get("timestamp") == "T":
            entry["timestamp"] = "2026-01-01T00:00:00Z"
    qg = doc["message"].get("query_graph")
    if isinstance(qg, dict) and not qg.get("nodes"):
        del doc["message"]["query_graph"]  # local_no_results stores an empty one
    kg = doc["message"].get("knowledge_graph") or {}
    for key in list(kg.get("edges") or {}):
        if key in LEGACY_PROVENANCE_EDGES or "detail_lookup" in kg["edges"][key]:
            del kg["edges"][key]
    for node in (kg.get("nodes") or {}).values():
        node.pop("detail_lookup", None)
    return doc


def validation_errors(suite: str, goldens: dict) -> list:
    """Every TRAPI object in a suite's 2.0 goldens, checked with TOM 2.0."""
    errors = []
    for case, rec in goldens.items():
        if suite == "expand_parity":
            if rec["qg"] is not None:
                _check("QueryGraph", rec["qg"], f"{case}.qg", errors)
            if rec["kg"] is not None:
                _check("KnowledgeGraph", rec["kg"], f"{case}.kg", errors)
            for k, v in rec["aux"].items():
                _check("AuxiliaryGraph", v, f"{case}.aux.{k}", errors)
            _check_plan(rec["plan"], f"{case}.plan", errors)
        if suite == "query_parity":
            _check("Response", rec["envelope"], f"{case}.envelope", errors)
            qo = rec["envelope"].get("query_options") or {}
            _check_plan(qo.get("query_plan"), f"{case}.query_plan", errors)
        if suite in ("expand_parity", "query_parity"):
            for i, r in enumerate(rec["requests"]):
                _check("Query", r["body"], f"{case}.requests[{i}]", errors)
        if suite == "response_parity":
            for i, r in enumerate(rec):
                if isinstance(r, dict) and isinstance(r.get("message"), dict):
                    _check(
                        "Response",
                        response_doc_for_validation(r),
                        f"{case}[{i}]",
                        errors,
                    )
        if suite == "aux_parity" and case == "meta_kg":
            for name, results in rec.items():
                for i, r in enumerate(results):
                    if isinstance(r, dict) and "edges" in r:
                        _check("MetaKnowledgeGraph", r, f"{name}[{i}]", errors)
    return errors
