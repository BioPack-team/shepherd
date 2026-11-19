"""Cache hits must answer in the caller's own ids, and a transient failure must
not be cached (#211).

The cache key ignored node/edge/path ids, but a hit hands back the source
query's tree, whose query graph and bindings use the source's ids -- the
recorded label map was never applied. So a caller who asked the same
question under other ids got results it couldn't map onto its query graph.
And a run in which an ARA merely timed out was cached for a week, serving
every identical query without that ARA's results.
"""

import logging
import uuid

import pytest

import shepherd_utils.ars.cache as cache
from shepherd_utils.config import settings

LOGGER = logging.getLogger(__name__)

QG = {
    "nodes": {"sn": {"ids": ["MONDO:0005148"]}, "on": {"categories": ["biolink:Drug"]}},
    "edges": {
        "t_edge": {"subject": "on", "object": "sn", "predicates": ["biolink:treats"]}
    },
}


def _key(qg):
    return cache.cache_key({"message": {"query_graph": qg}})[0]


def _relabel(qg, node_map, edge_map):
    return {
        "nodes": {node_map[k]: v for k, v in qg["nodes"].items()},
        "edges": {
            edge_map[k]: {
                **v,
                "subject": node_map[v["subject"]],
                "object": node_map[v["object"]],
            }
            for k, v in qg["edges"].items()
        },
    }


def test_the_same_question_under_other_ids_is_a_different_entry():
    renamed = _relabel(QG, {"sn": "n0", "on": "n1"}, {"t_edge": "e0"})
    assert _key(QG) != _key(renamed)


def test_the_same_ids_written_in_another_order_share_an_entry():
    reordered = {
        "edges": dict(QG["edges"]),
        "nodes": {"on": QG["nodes"]["on"], "sn": QG["nodes"]["sn"]},
    }
    assert _key(QG) == _key(reordered)


def test_structure_still_canonicalizes_whatever_the_ids():
    renamed = _relabel(QG, {"sn": "n0", "on": "n1"}, {"t_edge": "e0"})
    assert cache.canonical_graph(QG)[0] == cache.canonical_graph(renamed)[0]


# --- what gets cached -------------------------------------------------------------


def _child(status, code=200, result_count=3):
    return {
        "id": uuid.uuid4(),
        "status": status,
        "code": code,
        "agent_name": "ara-shepherd-aragorn",
        "inforesid": "infores:shepherd-aragorn",
        "result_count": result_count,
    }


def _merged(pk, result_count=3):
    return {
        "id": pk,
        "status": "D",
        "code": 200,
        "agent_name": cache.MERGE_AGENT_NAME,
        "inforesid": cache.MERGE_INFORESID,
        "result_count": result_count,
    }


@pytest.fixture
def partial_allowed(monkeypatch):
    monkeypatch.setattr(settings, "ars_cache_store_partial", True)


async def test_a_run_where_an_ara_timed_out_is_not_cached(partial_allowed):
    merged_pk = uuid.uuid4()
    parent = {"id": uuid.uuid4(), "merged_version": merged_pk}
    children = [
        _child("D"),
        _child("E", code=598, result_count=None),
        _merged(merged_pk),
    ]
    assert await cache._cacheable(parent, children, LOGGER) is False


async def test_a_run_with_a_lasting_ara_error_can_still_be_cached(partial_allowed):
    """A refusal the ARA would give again (501: doesn't do this query) is
    part of the answer, not a blip."""
    merged_pk = uuid.uuid4()
    parent = {"id": uuid.uuid4(), "merged_version": merged_pk}
    children = [
        _child("D"),
        _child("E", code=501, result_count=None),
        _merged(merged_pk),
    ]
    assert await cache._cacheable(parent, children, LOGGER) is True


async def test_a_clean_run_is_cached(partial_allowed):
    merged_pk = uuid.uuid4()
    parent = {"id": uuid.uuid4(), "merged_version": merged_pk}
    assert await cache._cacheable(parent, [_child("D"), _merged(merged_pk)], LOGGER)
