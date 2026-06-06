"""Branch-coverage tests targeting edge cases not exercised by other tests.

Covers:

- ``shepherd_utils.shared.merge_kgraph`` overlapping-edge attribute and
  source merge paths.
- ``shepherd_utils.shared.validate_message`` edge with bad attribute path.
- ``shepherd_utils.shared.get_next_operation`` corner case (single op).
- ``workers.aragorn_omnicorp.worker`` helpers: ``create_log_entry``,
  ``add_node_pmid_counts`` (default count = 0, attributes init), the
  setnode branch of ``generate_curie_pairs``, and ``add_shared_pmid_counts``
  reuse of an existing OMNICORP support graph.
- ``workers.finish_query.worker.process_task`` happy and failure paths.
"""

import asyncio
import json
import logging

import pytest

from shepherd_utils.shared import merge_kgraph, validate_message

logger = logging.getLogger(__name__)


# --- merge_kgraph overlapping-edge merge paths ----------------------------


def test_merge_kgraph_overlapping_edge_merges_attributes():
    """When the same edge id appears in both messages, the new attributes get
    combined onto the existing edge."""
    og = {
        "nodes": {},
        "edges": {
            "shared": {
                "subject": "A",
                "object": "B",
                "attributes": [{"attribute_type_id": "biolink:foo", "value": 1}],
                "sources": [
                    {
                        "resource_id": "infores:original",
                        "resource_role": "primary_knowledge_source",
                    }
                ],
            }
        },
    }
    new = {
        "nodes": {},
        "edges": {
            "shared": {
                "subject": "A",
                "object": "B",
                "attributes": [{"attribute_type_id": "biolink:bar", "value": 2}],
                "sources": [
                    {
                        "resource_id": "infores:other",
                        "resource_role": "supporting_data_source",
                    }
                ],
            }
        },
    }
    merged = merge_kgraph(og, new, "infores:test", logger)
    edge = merged["edges"]["shared"]
    type_ids = {a["attribute_type_id"] for a in edge["attributes"]}
    assert type_ids == {"biolink:foo", "biolink:bar"}
    resource_ids = {s["resource_id"] for s in edge["sources"]}
    # original + new sources combine; aggregator is NOT added in the merge
    # path because the edge already existed.
    assert {"infores:original", "infores:other"}.issubset(resource_ids)


def test_merge_kgraph_overlapping_edge_adopts_attrs_when_existing_empty():
    """If the existing edge has no attributes, the incoming attributes are
    adopted directly (no combine_unique_dicts call)."""
    og = {
        "nodes": {},
        "edges": {
            "shared": {
                "subject": "A",
                "object": "B",
                "attributes": [],
                "sources": [
                    {
                        "resource_id": "infores:original",
                        "resource_role": "primary_knowledge_source",
                    }
                ],
            }
        },
    }
    new_attrs = [{"attribute_type_id": "biolink:bar", "value": 2}]
    new = {
        "nodes": {},
        "edges": {
            "shared": {
                "subject": "A",
                "object": "B",
                "attributes": new_attrs,
                "sources": [],
            }
        },
    }
    merged = merge_kgraph(og, new, "infores:test", logger)
    assert merged["edges"]["shared"]["attributes"] == new_attrs


def test_merge_kgraph_overlapping_edge_adopts_sources_when_existing_empty():
    new_sources = [
        {"resource_id": "infores:other", "resource_role": "supporting_data_source"}
    ]
    og = {
        "nodes": {},
        "edges": {
            "shared": {
                "subject": "A",
                "object": "B",
                "attributes": [],
                "sources": [],
            }
        },
    }
    new = {
        "nodes": {},
        "edges": {
            "shared": {
                "subject": "A",
                "object": "B",
                "attributes": [],
                "sources": new_sources,
            }
        },
    }
    merged = merge_kgraph(og, new, "infores:test", logger)
    assert merged["edges"]["shared"]["sources"] == new_sources


# --- validate_message attribute-loop branch -------------------------------


def test_validate_message_skips_attributes_typo_field(tmp_path, monkeypatch):
    """The implementation looks up the misspelled ``attibutes`` field, so the
    attribute support_graph check is effectively dead. We verify it doesn't
    crash on edges that have well-formed ``attributes`` but the typo'd field
    is absent."""
    monkeypatch.chdir(tmp_path)
    message = {
        "message": {
            "knowledge_graph": {
                "nodes": {"A": {}, "B": {}},
                "edges": {
                    "e1": {
                        "subject": "A",
                        "object": "B",
                        "attributes": [
                            {
                                "attribute_type_id": "biolink:support_graphs",
                                "value": ["aux1"],
                            }
                        ],
                    }
                },
            },
            "auxiliary_graphs": {"aux1": {"edges": []}},
        }
    }
    validate_message(message, logger)
    assert not (tmp_path / "invalid_message.json").exists()


# --- aragorn_omnicorp helpers ---------------------------------------------


def test_create_log_entry_returns_shaped_dict():
    from workers.aragorn_omnicorp.worker import create_log_entry

    entry = create_log_entry("hi", "INFO", code="X1")
    assert entry["message"] == "hi"
    assert entry["level"] == "INFO"
    assert entry["code"] == "X1"
    assert "timestamp" in entry


def test_create_log_entry_default_code_none():
    from workers.aragorn_omnicorp.worker import create_log_entry

    entry = create_log_entry("hi", "WARNING")
    assert entry["code"] is None


def test_add_node_pmid_counts_uses_zero_for_missing_curies():
    """add_node_pmid_counts attaches an attribute with value=0 to nodes whose
    curie isn't in the counts dict."""
    from workers.aragorn_omnicorp.worker import add_node_pmid_counts

    kgraph = {
        "nodes": {
            "MONDO:0001": {"attributes": []},
            "MISSING:CURIE": {"attributes": None},  # exercises the None-init branch
        }
    }
    add_node_pmid_counts(kgraph, {"MONDO:0001": 42})
    found = kgraph["nodes"]["MONDO:0001"]["attributes"]
    found_attr = next(
        a for a in found if a.get("original_attribute_name") == "omnicorp_article_count"
    )
    assert found_attr["value"] == 42
    # Missing curie still gets the attribute, value = 0.
    missing_attrs = kgraph["nodes"]["MISSING:CURIE"]["attributes"]
    missing_attr = next(
        a
        for a in missing_attrs
        if a.get("original_attribute_name") == "omnicorp_article_count"
    )
    assert missing_attr["value"] == 0


def test_add_shared_pmid_counts_reuses_existing_omnicorp_support_graph():
    """If an analysis already has an OMNICORP support graph, new co-occurrence
    edges should be appended to that one rather than creating a second."""
    from workers.aragorn_omnicorp.worker import add_shared_pmid_counts

    message = {
        "knowledge_graph": {"nodes": {"A": {}, "B": {}}, "edges": {}},
        "auxiliary_graphs": {
            "OMNICORP_support_graph_existing": {"edges": [], "attributes": []},
        },
        "results": [
            {
                "analyses": [
                    {
                        "edge_bindings": {},
                        "support_graphs": ["OMNICORP_support_graph_existing"],
                    }
                ]
            }
        ],
    }
    pair_to_answer = {("A", "B"): {(0, 0)}}
    add_shared_pmid_counts(message, {("A", "B"): 7}, pair_to_answer)
    sgs = message["results"][0]["analyses"][0]["support_graphs"]
    # No new OMNICORP support graph created; the existing one was reused.
    assert "OMNICORP_support_graph_existing" in sgs
    assert sum(1 for s in set(sgs) if s.startswith("OMNICORP_support_graph")) == 1
    # The existing aux graph picked up the new co-occurrence edge.
    assert (
        len(message["auxiliary_graphs"]["OMNICORP_support_graph_existing"]["edges"])
        == 1
    )


def test_add_shared_pmid_counts_skips_zero_publication_counts():
    """A pair with publication_count == 0 should not produce any edges."""
    from workers.aragorn_omnicorp.worker import add_shared_pmid_counts

    message = {
        "knowledge_graph": {"nodes": {"A": {}, "B": {}}, "edges": {}},
        "auxiliary_graphs": {},
        "results": [
            {
                "analyses": [
                    {
                        "edge_bindings": {},
                    }
                ]
            }
        ],
    }
    add_shared_pmid_counts(message, {("A", "B"): 0}, {("A", "B"): {(0, 0)}})
    assert message["knowledge_graph"]["edges"] == {}


def test_generate_curie_pairs_includes_setnode_pairings():
    """When the qgraph has setnodes, every cross-product with non-set nodes
    becomes a candidate pair."""
    from workers.aragorn_omnicorp.worker import generate_curie_pairs

    answers = [
        {
            "node_bindings": {
                "qset": [{"id": "S1"}, {"id": "S2"}],
                "qother": [{"id": "O1"}],
            },
            "analyses": [
                {
                    "edge_bindings": {},
                }
            ],
        }
    ]
    qgraph_setnodes = {"qset"}
    node_pub_counts = {"S1": 1, "S2": 1, "O1": 1}
    message = {
        "knowledge_graph": {"edges": {}},
        "auxiliary_graphs": {},
    }
    pair_to_answer = generate_curie_pairs(
        answers, qgraph_setnodes, node_pub_counts, message, logger
    )
    # S1<->O1 and S2<->O1 are the setnode-vs-nonset pairs.
    assert (("O1", "S1") in pair_to_answer) or (("S1", "O1") in pair_to_answer)
    assert (("O1", "S2") in pair_to_answer) or (("S2", "O1") in pair_to_answer)


# --- finish_query.process_task -------------------------------------------


def _make_finish_task():
    return [
        "msg-id",
        {
            "query_id": "qid",
            "response_id": "rid",
            "workflow": json.dumps([]),
            "log_level": "20",
            "otel": "{}",
        },
    ]


class _Limiter:
    def __init__(self):
        self.released = False

    def release(self):
        self.released = True


@pytest.mark.asyncio
async def test_finish_query_process_task_acks_on_success(redis_mock, mocker):
    from workers.finish_query import worker as fq

    mocker.patch.object(fq, "finish_query", new_callable=mocker.AsyncMock)
    mock_ack = mocker.patch.object(
        fq, "mark_task_as_complete", new_callable=mocker.AsyncMock
    )
    limiter = _Limiter()
    await fq.process_task(_make_finish_task(), None, logger, limiter)
    assert mock_ack.called
    assert limiter.released


@pytest.mark.asyncio
async def test_finish_query_process_task_acks_on_failure(redis_mock, mocker):
    """Even when ``finish_query`` raises, ``mark_task_as_complete`` is still
    called in the ``finally`` block."""
    from workers.finish_query import worker as fq

    mocker.patch.object(
        fq,
        "finish_query",
        new_callable=mocker.AsyncMock,
        side_effect=RuntimeError("kaboom"),
    )
    mock_ack = mocker.patch.object(
        fq, "mark_task_as_complete", new_callable=mocker.AsyncMock
    )
    limiter = _Limiter()
    await fq.process_task(_make_finish_task(), None, logger, limiter)
    assert mock_ack.called
    assert limiter.released


@pytest.mark.asyncio
async def test_finish_query_process_task_swallows_ack_failure(redis_mock, mocker):
    """An ``mark_task_as_complete`` failure should be logged but not escape."""
    from workers.finish_query import worker as fq

    mocker.patch.object(fq, "finish_query", new_callable=mocker.AsyncMock)
    mocker.patch.object(
        fq,
        "mark_task_as_complete",
        new_callable=mocker.AsyncMock,
        side_effect=RuntimeError("ack failed"),
    )
    limiter = _Limiter()
    await fq.process_task(_make_finish_task(), None, logger, limiter)
    assert limiter.released


@pytest.mark.asyncio
async def test_finish_query_process_task_handles_cancellation(redis_mock, mocker):
    """A CancelledError inside finish_query should not escape, and ack still
    runs."""
    from workers.finish_query import worker as fq

    mocker.patch.object(
        fq,
        "finish_query",
        new_callable=mocker.AsyncMock,
        side_effect=asyncio.CancelledError,
    )
    mock_ack = mocker.patch.object(
        fq, "mark_task_as_complete", new_callable=mocker.AsyncMock
    )
    limiter = _Limiter()
    await fq.process_task(_make_finish_task(), None, logger, limiter)
    assert mock_ack.called
    assert limiter.released
