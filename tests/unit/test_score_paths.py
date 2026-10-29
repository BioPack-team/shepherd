"""Tests for the score_paths worker and its sequence-reranker ``scorer``.

The reranker itself is a TorchScript model the pathfinder embeddings LMDB
holds; here a fake model stands in, so what is checked is everything around
it: how a TRAPI path is read into nodes, categories and hop phrases, the
tokens and features looked up for it, and how the model's output becomes each
analysis' score.

torch is not a test requirement (its default Linux wheel is gigabytes), so when
it is missing a stand-in with the handful of calls the scorer makes is
installed in its place.
"""

import contextlib
import hashlib
import io
import json
import logging
import sys
import types
from pathlib import Path

import lmdb
import numpy as np
import pytest

try:
    import torch  # noqa: F401
except ImportError:
    _torch = types.ModuleType("torch")
    _torch.int64 = np.int64
    _torch.from_numpy = np.asarray
    _torch.full = lambda shape, value, dtype=None: np.full(shape, value, dtype=dtype)
    _torch.zeros = lambda shape, dtype=None: np.zeros(shape, dtype=dtype)
    _torch.inference_mode = contextlib.nullcontext
    _torch.set_num_threads = lambda n: None

    def _no_jit(*args, **kwargs):
        raise RuntimeError("torch is not installed")

    _torch.jit = types.SimpleNamespace(load=_no_jit)
    sys.modules["torch"] = _torch

# The worker imports its scorer as a top-level module, as it runs in its image.
sys.path.insert(0, str(Path(__file__).parents[2] / "workers" / "score_paths"))
import scorer  # noqa: E402
from workers.score_paths import worker as score_worker  # noqa: E402

logger = logging.getLogger(__name__)


# --- fakes -------------------------------------------------------------------


class FakeBmt:
    """The few biolink Toolkit calls the scorer makes."""

    predicates = {
        "related to",
        "interacts with",
        "affects",
        "affected by",
        "causes",
        "caused by",
        "treats",
        "treated by",
        "broken",
    }
    symmetric = {"related to", "interacts with"}
    inverses = {
        "affects": "affected by",
        "affected by": "affects",
        "causes": "caused by",
        "caused by": "causes",
        "treats": "treated by",
        "treated by": "treats",
    }
    depths = {
        "biolink:NamedThing": 0,
        "biolink:BiologicalEntity": 2,
        "biolink:Disease": 4,
        "biolink:Gene": 5,
        "biolink:SmallMolecule": 6,
    }
    mixins = {"biolink:GeneOrGeneProduct", "biolink:ChemicalOrDrugOrTreatment"}

    def is_predicate(self, name):
        return name in self.predicates

    def is_symmetric(self, name):
        return name in self.symmetric

    def get_inverse(self, name):
        if name == "broken":
            raise AttributeError("declared inverse is not in the schema")
        return self.inverses.get(name)

    def is_category(self, name):
        return name in self.depths

    def get_element_depth(self, name):
        return self.depths[name]

    def is_mixin(self, name):
        return name in self.mixins


BMT = FakeBmt()


class FakeTxn:
    def __init__(self, data):
        self.data = data

    def get(self, key):
        return self.data.get(key)


class _Output:
    def __init__(self, values):
        self.values = values

    def float(self):
        return self

    def numpy(self):
        return self.values


class FakeModel:
    """Scores a path by the first value of its second node's token, and
    records what it was given."""

    def __init__(self):
        self.calls = []

    def score_tokens(self, node_t, edge_t, n_hops, scalars, extra):
        node_t, edge_t, n_hops, scalars = (
            np.asarray(x) for x in (node_t, edge_t, n_hops, scalars)
        )
        self.calls.append(
            {
                "node_t": node_t,
                "edge_t": edge_t,
                "n_hops": n_hops,
                "scalars": scalars,
            }
        )
        return _Output(node_t[:, 1, 0].astype(np.float32))


# The model's token width; the SAPBERT width the LMDB records is never read
DIM = 2
SCALAR_NAMES = (
    ["n_hops_scaled"]
    + [name for name, _, _ in scorer._NODE_COLUMNS]
    + [f"{base}_{stat}" for base in scorer._EDGE_BASES for stat in ("mean", "min")]
)


def make_config(**overrides):
    config = {
        "embed_dim": 768,
        "token_dim": DIM,
        "scalar_names": SCALAR_NAMES,
        "node_context": True,
        "edge_context": True,
        "length_scale": {2: (0.1, 2.0)},
        "biolink_version": "4.2.5",
    }
    config.update(overrides)
    return config


def store(**vectors):
    """An LMDB stand-in holding the given vectors, keyed as the pathfinder
    embeddings hold them."""
    data = {}
    for kind, entries in vectors.items():
        for text, values in entries.items():
            data[scorer.lmdb_key(kind, text)] = np.asarray(
                values, dtype=scorer.DTYPES[kind]
            ).tobytes()
    return data


def edge(subject, obj, predicate="biolink:related_to", **extra):
    return {"subject": subject, "object": obj, "predicate": predicate, **extra}


def node(*categories):
    return {"categories": list(categories)}


def sigmoid(z):
    return 1.0 / (1.0 + np.exp(-z))


# --- keys --------------------------------------------------------------------


def test_lmdb_key():
    assert scorer.lmdb_key("node", "MONDO:1") == b"node:MONDO:1"


def test_lmdb_key_hashes_what_lmdb_cannot_hold():
    text = "x" * 600

    key = scorer.lmdb_key("phrase", text)

    expected = hashlib.sha256(f"phrase:{text}".encode()).hexdigest()
    assert key == f"phrase#{expected}".encode()
    assert len(key) < scorer.MAX_KEY_BYTES


def test_edge_context_key_ignores_the_order_of_the_pair():
    key = scorer.edge_context_key("B:2", "A:1")

    digest = hashlib.blake2b(b"A:1|B:2", digest_size=8).digest()
    assert key == b"edge_context#" + digest
    assert scorer.edge_context_key("A:1", "B:2") == key


# --- loading -----------------------------------------------------------------


def test_load_reranker(mocker):
    model = mocker.Mock()
    load = mocker.patch.object(scorer.torch.jit, "load", return_value=model)
    txn = FakeTxn(
        {
            scorer.MODEL_KEY: b"model bytes",
            scorer.CONFIG_KEY: json.dumps(
                {
                    "length_scale": {"2": [0.5, 2], "3": [1, 3]},
                    "embed_dim": 4,
                    "token_dim": 2,
                }
            ).encode(),
        }
    )

    loaded, config = scorer.load_reranker(txn)

    assert loaded is model
    model.eval.assert_called_once()
    blob = load.call_args.args[0]
    assert isinstance(blob, io.BytesIO) and blob.getvalue() == b"model bytes"
    assert config == {
        "length_scale": {2: (0.5, 2.0), 3: (1.0, 3.0)},
        "embed_dim": 4,
        "token_dim": 2,
    }


def test_load_reranker_refuses_sapbert_vectors(mocker):
    """An LMDB of SAPBERT's vectors (pathfinder embeddings version 1) has no
    token width: its vectors are not the tokens the model scores."""
    load = mocker.patch.object(scorer.torch.jit, "load")
    txn = FakeTxn(
        {
            scorer.MODEL_KEY: b"model bytes",
            scorer.CONFIG_KEY: json.dumps(
                {"length_scale": {"2": [0.5, 2]}, "embed_dim": 4}
            ).encode(),
        }
    )

    with pytest.raises(RuntimeError, match="serve version 2"):
        scorer.load_reranker(txn)
    load.assert_not_called()


@pytest.mark.parametrize("missing", [scorer.MODEL_KEY, scorer.CONFIG_KEY])
def test_load_reranker_without_one(missing):
    data = {scorer.MODEL_KEY: b"model", scorer.CONFIG_KEY: b"{}"}
    del data[missing]

    with pytest.raises(RuntimeError, match="no reranker"):
        scorer.load_reranker(FakeTxn(data))


def test_load_toolkit_reads_the_trained_biolink_release(mocker):
    toolkit = mocker.patch.object(scorer, "Toolkit")

    scorer.load_toolkit({"biolink_version": "4.2.5"})

    toolkit.assert_called_once_with(
        schema="https://raw.githubusercontent.com/biolink/biolink-model/"
        "refs/tags/v4.2.5/biolink-model.yaml",
        predicate_map="https://raw.githubusercontent.com/biolink/biolink-model/"
        "refs/tags/v4.2.5/predicate_mapping.yaml",
    )


# --- reading a path ----------------------------------------------------------


def test_predicate_names():
    assert scorer.predicate_name("biolink:affected_by") == "affected by"
    assert scorer.predicate_curie("affected by") == "biolink:affected_by"


@pytest.mark.parametrize(
    "predicate, inverse",
    [
        ("biolink:affects", "biolink:affected_by"),
        ("biolink:interacts_with", "biolink:interacts_with"),
        ("biolink:has_chemical_role", "biolink:is_chemical_role_of"),
        ("is_chemical_role_of", "biolink:has_chemical_role"),
        ("biolink:not_a_predicate", None),
        ("biolink:broken", None),
    ],
)
def test_inverse_predicate(predicate, inverse):
    assert scorer.inverse_predicate(BMT, predicate) == inverse


@pytest.mark.parametrize(
    "categories, expected",
    [
        (
            ("biolink:BiologicalEntity", "biolink:Gene", "biolink:NamedThing"),
            "biolink:Gene",
        ),
        (("biolink:GeneOrGeneProduct", "biolink:Disease"), "biolink:Disease"),
        (("biolink:GeneOrGeneProduct",), "biolink:GeneOrGeneProduct"),
    ],
)
def test_most_specific_category(categories, expected):
    assert scorer.most_specific_category(BMT, categories) == expected


def test_most_specific_category_needs_a_biolink_one():
    with pytest.raises(ValueError, match="No biolink categories"):
        scorer.most_specific_category(BMT, ("not:a_category",))


def qualifiers(**slots):
    return [
        {"qualifier_type_id": f"biolink:{slot}", "qualifier_value": value}
        for slot, value in slots.items()
    ]


@pytest.mark.parametrize(
    "slots, forward, backward",
    [
        ({}, "affects", "affected by"),
        (
            {
                "qualified_predicate": "biolink:causes",
                "object_aspect_qualifier": "activity",
                "object_direction_qualifier": "increased",
            },
            "causes increased activity of",
            "has increased activity caused by",
        ),
        (
            {"object_aspect_qualifier": "activity_or_abundance"},
            "affects activity or abundance of",
            "has activity or abundance affected by",
        ),
        # a qualified predicate only reads with a direction or without an aspect
        (
            {
                "qualified_predicate": "biolink:causes",
                "object_aspect_qualifier": "activity",
            },
            "affects activity of",
            "has activity affected by",
        ),
        # a qualified predicate with no inverse reads backwards as the bare one
        (
            {"qualified_predicate": "biolink:regulates"},
            "regulates",
            "affected by",
        ),
        # these have no wording, so the edge reads as its bare predicate
        ({"object_direction_qualifier": "increased"}, "affects", "affected by"),
        ({"subject_aspect_qualifier": "activity"}, "affects", "affected by"),
    ],
)
def test_edge_phrase(slots, forward, backward):
    affects = edge("A", "B", "biolink:affects", qualifiers=qualifiers(**slots))

    assert scorer.edge_phrase(BMT, affects, backward=False) == forward
    assert scorer.edge_phrase(BMT, affects, backward=True) == backward


def test_edge_phrase_cannot_walk_back_an_edge_with_no_inverse():
    one_way = edge("A", "B", "biolink:not_a_predicate")

    assert scorer.edge_phrase(BMT, one_way, backward=False) == "not a predicate"
    assert scorer.edge_phrase(BMT, one_way, backward=True) is None


def test_edge_phrase_rejects_a_qualifier_it_cannot_read():
    bad = edge(
        "A",
        "B",
        "biolink:affects",
        qualifiers=qualifiers(object_aspect_qualifier="activity; drop table"),
    )

    with pytest.raises(ValueError, match="cannot be read"):
        scorer.edge_phrase(BMT, bad, backward=False)


def negated(value):
    return {"attributes": [{"attribute_type_id": "biolink:negated", "value": value}]}


def test_hop_phrase_reads_every_edge_of_the_hop():
    edges = {
        "e1": edge("A", "B", "biolink:affects"),
        "e2": edge("B", "A", "biolink:treats"),
        "e3": edge("A", "B", "biolink:affects"),
        "sub": edge("A", "B", "biolink:subclass_of"),
        "other": edge("A", "C", "biolink:causes"),
    }

    phrase = scorer.hop_phrase(BMT, "A", "B", list(edges), edges)

    assert phrase == "[affects or treated by]"
    assert scorer.hop_phrase(BMT, "A", "B", ["e1", "missing"], edges) == "affects"


def test_hop_phrase_drops_a_phrase_only_negated_edges_carry():
    edges = {
        "not": edge("A", "B", "biolink:affects", **negated(True)),
        "yes": edge("A", "B", "biolink:treats", **negated(False)),
        "also_not": edge(
            "A",
            "B",
            "biolink:causes",
            attributes=[{"original_attribute_name": "negated", "value": True}],
        ),
        "affirmed": edge("A", "B", "biolink:causes"),
    }

    assert scorer.hop_phrase(BMT, "A", "B", list(edges), edges) == "[causes or treats]"
    assert scorer.hop_phrase(BMT, "A", "B", ["not"], edges) is None


def test_hop_phrase_rejects_a_negated_that_is_not_a_boolean():
    edges = {"e": edge("A", "B", "biolink:affects", **negated("yes"))}

    with pytest.raises(ValueError, match="expected a boolean"):
        scorer.hop_phrase(BMT, "A", "B", ["e"], edges)


def inferred():
    return {
        "attributes": [
            {"attribute_type_id": "biolink:support_graphs", "value": ["aux_e"]}
        ]
    }


def test_hop_phrase_reads_an_endpoint_s_own_edges_before_inferred_ones():
    edges = {
        "own": edge("A", "B", "biolink:affects"),
        "child": edge("A", "B", "biolink:treats", **inferred()),
        "child_back": edge("B", "A", "biolink:causes", **inferred()),
        "not": edge("A", "B", "biolink:affects", **negated(True)),
    }

    assert scorer.hop_phrase(BMT, "A", "B", list(edges), edges) == "affects"
    assert (
        scorer.hop_phrase(BMT, "A", "B", ["child", "child_back"], edges)
        == "[caused by or treats]"
    )
    assert scorer.hop_phrase(BMT, "A", "B", ["not", "child"], edges) == "treats"


def test_hop_phrase_with_nothing_walkable():
    edges = {"e": edge("B", "A", "biolink:not_a_predicate")}

    assert scorer.hop_phrase(BMT, "A", "B", ["e"], edges) is None


def test_order_path_nodes():
    edges = {
        "bt": edge("T", "B"),
        "ab": edge("A", "B"),
        "sa": edge("S", "A"),
        "sa2": edge("A", "S"),
    }

    assert scorer.order_path_nodes("S", "T", list(edges), edges) == [
        "S",
        "A",
        "B",
        "T",
    ]


def test_order_path_nodes_needs_a_connected_path():
    edges = {"sa": edge("S", "A"), "bt": edge("B", "T")}

    with pytest.raises(ValueError, match="Could not construct path"):
        scorer.order_path_nodes("S", "T", ["sa", "bt", "gone"], edges)


def test_read_path():
    kgraph = {
        "nodes": {
            "S": node("biolink:BiologicalEntity", "biolink:Disease"),
            "A": node(),
            "T": node("biolink:SmallMolecule"),
        },
        "edges": {
            "sa": edge("S", "A", "biolink:affects"),
            "ta": edge("T", "A", "biolink:treats"),
        },
    }

    assert scorer.read_path(BMT, "S", "T", ["sa", "ta"], kgraph) == (
        ["S", "A", "T"],
        ["biolink:Disease", "biolink:NamedThing", "biolink:SmallMolecule"],
        ["affects", "treated by"],
    )


def test_read_path_needs_every_node():
    kgraph = {"nodes": {"S": node()}, "edges": {"st": edge("S", "T")}}

    with pytest.raises(ValueError, match="T is not in the knowledge graph"):
        scorer.read_path(BMT, "S", "T", ["st"], kgraph)


def test_read_path_needs_every_hop_walkable():
    kgraph = {
        "nodes": {"S": node(), "T": node()},
        "edges": {"ts": edge("T", "S", "biolink:not_a_predicate")},
    }

    with pytest.raises(ValueError, match="no edge of the path can be walked"):
        scorer.read_path(BMT, "S", "T", ["ts"], kgraph)


def test_is_direct_path():
    kgraph = {
        "edges": {
            "st": edge("S", "T"),
            "ts": edge("T", "S"),
            "sa": edge("S", "A"),
        }
    }

    assert scorer.is_direct_path("S", "T", ["st", "ts"], kgraph)
    assert not scorer.is_direct_path("S", "T", ["st", "sa"], kgraph)
    assert not scorer.is_direct_path("S", "T", ["missing"], kgraph)
    assert not scorer.is_direct_path("S", "T", [], kgraph)


# --- features ----------------------------------------------------------------


def test_path_scalars():
    node_table = {
        "S": [1, 2, 0.5, 1],
        "A": [3, 4, 0.0, 0],
        "T": [5, 0, 1.0, 1],
    }
    edge_table = {
        frozenset("SA"): [1, 10, 1, 1, 4],
        frozenset("AT"): [3, 0, 3, 2, 6],
    }

    scalars = scorer.path_scalars(
        SCALAR_NAMES,
        ["S", "A", "T"],
        lambda c: np.array(node_table[c], dtype=np.float32),
        lambda a, b: np.array(edge_table[frozenset((a, b))], dtype=np.float32),
    )

    got = dict(zip(SCALAR_NAMES, scalars.tolist()))
    assert got["n_hops_scaled"] == 2 / scorer.N_HOPS_SCALE
    assert got["deg_out_max"] == 5 and got["deg_out_mean"] == 3
    assert got["deg_in_max"] == 4 and got["deg_in_mean"] == 2
    assert got["ic_mean"] == 0.5
    assert got["ic_present_frac"] == pytest.approx(2 / 3)
    assert got["edge_count_mean"] == 2 and got["edge_count_min"] == 1
    assert got["pubs_mean"] == 5 and got["pubs_min"] == 0
    assert got["attributes_seen_mean"] == 5 and got["attributes_seen_min"] == 4


def test_path_scalars_without_the_context_tables():
    scalars = scorer.path_scalars(SCALAR_NAMES, ["S", "A", "B", "T"])

    assert scalars[0] == 3 / scorer.N_HOPS_SCALE
    assert not scalars[1:].any()


def test_path_features():
    data = store(
        node={"S": [1, 2], "T": [3, 4]},
        category={"biolink:Disease": [5, 6], "biolink:Gene": [7, 8]},
        phrase={"affects": [0.25, 0.5]},
        node_context={"S": [2, 0, 0, 0]},
    )
    data[scorer.edge_context_key("S", "T")] = np.array(
        [4, 0, 0, 0, 0], dtype=np.float32
    ).tobytes()
    kept = {}

    node_t, edge_t, scalars = scorer.path_features(
        FakeTxn(data),
        make_config(),
        ["S", "T"],
        ["biolink:Disease", "biolink:Gene"],
        ["affects"],
        kept,
    )

    # a node's token is its vector plus its category's
    assert node_t.tolist() == [[6, 8], [10, 12]]
    assert edge_t.tolist() == [[0.25, 0.5]]
    got = dict(zip(SCALAR_NAMES, scalars.tolist()))
    # T has no node context: zeros
    assert got["deg_out_max"] == 2 and got["deg_out_mean"] == 1
    assert got["edge_count_mean"] == 4
    # the categories and phrase are kept for the next path, the nodes are not
    assert set(kept) == {
        ("category", "biolink:Disease"),
        ("category", "biolink:Gene"),
        ("phrase", "affects"),
    }


def test_path_features_names_the_missing_vector():
    data = store(node={"S": [1, 2]}, category={"biolink:Disease": [0, 0]})

    with pytest.raises(KeyError, match="node:T"):
        scorer.path_features(
            FakeTxn(data),
            make_config(),
            ["S", "T"],
            ["biolink:Disease", "biolink:Disease"],
            ["related to"],
        )


# --- scoring a message -------------------------------------------------------


def scoring_message():
    """One result with a path of every kind, and one with no bindings."""
    kgraph = {
        "nodes": {name: node("biolink:Disease") for name in ("S", "A", "B", "C", "T")},
        "edges": {
            "st": edge("S", "T"),
            "sa": edge("S", "A", "biolink:affects"),
            "at": edge("A", "T", "biolink:affects"),
            "sb": edge("S", "B"),
            "bt": edge("B", "T"),
            "ac": edge("A", "C"),
            "ct": edge("C", "T"),
        },
    }
    aux = {
        "direct": {"edges": ["st"]},
        "via_a": {"edges": ["sa", "at"]},
        "via_b": {"edges": ["sb", "bt"]},
        "broken": {"edges": ["sa"]},
        "three_hops": {"edges": ["sa", "ac", "ct"]},
    }

    def analysis(aux_id):
        return {"path_bindings": {"p0": [{"id": aux_id}]}}

    return {
        "message": {
            "query_graph": {"paths": {"p0": {"subject": "n0", "object": "n1"}}},
            "knowledge_graph": kgraph,
            "auxiliary_graphs": aux,
            "results": [
                {
                    "node_bindings": {"n0": [{"id": "S"}], "n1": [{"id": "T"}]},
                    "analyses": [
                        analysis("direct"),
                        analysis("via_a"),
                        analysis("via_b"),
                        analysis("no_such_aux"),
                        {"path_bindings": {}},
                        analysis("broken"),
                        analysis("three_hops"),
                        {**analysis("via_a"), "score": 0.9},
                    ],
                },
                {"node_bindings": {}, "analyses": [analysis("direct")]},
            ],
        }
    }


def scoring_store():
    # B has no vector: its path cannot be scored
    return FakeTxn(
        store(
            node={"S": [0, 0], "A": [0.5, 0], "C": [1, 0], "T": [0, 0]},
            category={"biolink:Disease": [0, 0]},
            phrase={"affects": [0, 0], "related to": [0, 0]},
        )
    )


@pytest.mark.parametrize("chunk_size", [1, 1024])
def test_score_message(chunk_size):
    message = scoring_message()
    model = FakeModel()

    stats = scorer.score_message(
        message, scoring_store(), model, make_config(), BMT, chunk_size
    )

    scored, unbound = message["message"]["results"]
    via_a = sigmoid((0.5 - 0.1) / 2.0)
    assert [a["score"] for a in scored["analyses"]] == pytest.approx(
        [1.0, via_a, 0.0, 0.0, 0.0, 0.0, 0.0, via_a]
    )
    assert unbound["analyses"][0]["score"] == 0.0
    assert {k: v for k, v in stats.items() if k != "model_seconds"} == {
        "analyses": 9,
        "scored": 2,
        "direct": 1,
        "no_binding": 3,
        "bad_path": 1,
        "no_scale": 1,
        "missing": 1,
        "missing_samples": ["node:B"],
    }
    assert len(model.calls) == (2 if chunk_size == 1 else 1)
    for call in model.calls:
        assert call["n_hops"].tolist() == [2] * len(call["n_hops"])
        assert call["node_t"].shape[1:] == (3, DIM)
        assert call["edge_t"].shape[1:] == (2, DIM)
        assert call["scalars"].shape[1:] == (len(SCALAR_NAMES),)


def test_score_message_ranks_lengths_on_their_own_scales():
    message = scoring_message()
    config = make_config(length_scale={2: (0.1, 2.0), 3: (0.25, 0.5)})
    model = FakeModel()

    stats = scorer.score_message(message, scoring_store(), model, config, BMT, 1024)

    scores = [a["score"] for a in message["message"]["results"][0]["analyses"]]
    assert scores[1] == pytest.approx(sigmoid((0.5 - 0.1) / 2.0))
    # the three-hop path's second node is A as well, on the three-hop scale
    assert scores[6] == pytest.approx(sigmoid((0.5 - 0.25) / 0.5))
    assert stats["no_scale"] == 0
    assert sorted(call["n_hops"][0] for call in model.calls) == [2, 3]


# --- the worker --------------------------------------------------------------


class FakeEnv:
    @contextlib.contextmanager
    def begin(self):
        yield scoring_store()


@pytest.fixture
def scoring_state(mocker):
    mocker.patch.object(score_worker, "embedding_env", FakeEnv())
    mocker.patch.object(score_worker, "model", FakeModel())
    mocker.patch.object(score_worker, "config", make_config())
    mocker.patch.object(score_worker, "bmt", BMT)


def test_score_paths_scores_and_saves(mocker, scoring_state, caplog):
    message = scoring_message()
    mocker.patch.object(score_worker, "get_message_sync", return_value=message)
    save = mocker.patch.object(score_worker, "save_message_sync")

    with caplog.at_level(logging.INFO):
        score_worker.score_paths("rid", logger)

    save.assert_called_once_with("rid", message)
    assert message["message"]["results"][0]["analyses"][0]["score"] == 1.0
    assert "Scored 2/9 analyses" in caplog.text
    assert "missing embedding: 1" in caplog.text


def test_score_paths_still_saves_when_scoring_fails(mocker, scoring_state):
    message = scoring_message()
    mocker.patch.object(score_worker, "get_message_sync", return_value=message)
    mocker.patch.object(scorer, "score_message", side_effect=RuntimeError("boom"))
    save = mocker.patch.object(score_worker, "save_message_sync")

    score_worker.score_paths("rid", logger)

    save.assert_called_once()
    analyses = message["message"]["results"][0]["analyses"]
    # an analysis that had a score keeps it; the rest get 0
    assert [a["score"] for a in analyses] == [0.0] * 7 + [0.9]


def test_score_paths_retries_the_save(mocker, scoring_state):
    mocker.patch.object(
        score_worker, "get_message_sync", return_value=scoring_message()
    )
    mocker.patch.object(score_worker.time, "sleep")
    save = mocker.patch.object(
        score_worker,
        "save_message_sync",
        side_effect=[ConnectionError("down"), ConnectionError("down"), None],
    )

    score_worker.score_paths("rid", logger)

    assert save.call_count == 3


def test_validate_scoring_data_needs_a_reranker(tmp_path, mocker):
    mocker.patch.object(score_worker, "EMBEDDING_DIR", str(tmp_path))
    env = lmdb.open(str(tmp_path))
    with env.begin(write=True) as txn:
        txn.put(b"node:S", b"\x00\x00")
    env.close()

    with pytest.raises(RuntimeError, match="no reranker"):
        score_worker._validate_scoring_data(logger)


def test_validate_scoring_data(tmp_path, mocker, caplog):
    mocker.patch.object(score_worker, "EMBEDDING_DIR", str(tmp_path))
    mocker.patch.object(scorer.torch.jit, "load", return_value=mocker.Mock())
    env = lmdb.open(str(tmp_path))
    with env.begin(write=True) as txn:
        txn.put(scorer.MODEL_KEY, b"model")
        txn.put(
            scorer.CONFIG_KEY,
            json.dumps(
                {
                    "length_scale": {"3": [0, 1], "2": [0, 1]},
                    "token_dim": 128,
                    "biolink_version": "4.2.5",
                    "node_context": True,
                    "edge_context": False,
                }
            ).encode(),
        )
    env.close()

    with caplog.at_level(logging.INFO):
        score_worker._validate_scoring_data(logger)

    assert "2 entries" in caplog.text
    assert "reranker scores [2, 3]-hop paths, biolink 4.2.5" in caplog.text
