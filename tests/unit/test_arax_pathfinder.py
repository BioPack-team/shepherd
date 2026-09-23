"""Tests for the arax_pathfinder process-pool entrypoint.

The worker imports ``pathfinder.Pathfinder`` and ``biolink_helper_pkg``, which
are installed only inside its container (see
``workers/arax_pathfinder/requirements.txt``) and are not in
``test-requirements.txt``. We stub both into ``sys.modules`` before importing
the worker, the same trick ``tests/conftest.py`` uses for ``shepherd_utils.otel``.
"""

import copy
import logging
import sys
import types
from unittest.mock import MagicMock

import pytest

if "pathfinder" not in sys.modules:
    _pathfinder_pkg = types.ModuleType("pathfinder")
    _pathfinder_mod = types.ModuleType("pathfinder.Pathfinder")
    _pathfinder_mod.Pathfinder = MagicMock(name="Pathfinder")
    _pathfinder_pkg.Pathfinder = _pathfinder_mod
    sys.modules["pathfinder"] = _pathfinder_pkg
    sys.modules["pathfinder.Pathfinder"] = _pathfinder_mod

if "pathfinder.telemetry" not in sys.modules:
    # child_bootstrap()'s return value is unpacked as (tracer, ctx) and ctx is
    # attached/detached via the real opentelemetry.context API, so it has to be
    # a real Context -- an arbitrary Mock would raise inside otel_context.attach.
    import opentelemetry.context as _otel_context

    _pathfinder_telemetry_mod = types.ModuleType("pathfinder.telemetry")
    _pathfinder_telemetry_mod.inject_context = MagicMock(
        name="inject_context", return_value={}
    )
    _pathfinder_telemetry_mod.child_bootstrap = MagicMock(
        name="child_bootstrap",
        return_value=(MagicMock(name="child_tracer"), _otel_context.get_current()),
    )
    _pathfinder_telemetry_mod.flush_child = MagicMock(name="flush_child")
    sys.modules["pathfinder"].telemetry = _pathfinder_telemetry_mod
    sys.modules["pathfinder.telemetry"] = _pathfinder_telemetry_mod

if "biolink_helper_pkg" not in sys.modules:
    _biolink_mod = types.ModuleType("biolink_helper_pkg")
    _biolink_mod.BiolinkHelper = MagicMock(name="BiolinkHelper")
    sys.modules["biolink_helper_pkg"] = _biolink_mod

from translator_tom import Message  # noqa: E402

from workers.arax_pathfinder import worker as pf_worker  # noqa: E402

LOGGER = logging.getLogger(__name__)

QUERY = {
    "message": {
        "query_graph": {
            "nodes": {
                "n0": {"ids": ["MONDO:0005148"]},
                "n1": {"ids": ["CHEBI:15365"]},
            },
            # No "constraints": [] -- an empty list is invalid TRAPI 2.0 and
            # is stripped from incoming queries by normalize_query_graph.
            "paths": {"p0": {"subject": "n0", "object": "n1"}},
        }
    }
}

# What the catrax-pathfinder library returns: still TRAPI 1.x shaped.
PATHS_RESULT = (
    {
        "id": "r0",
        "analyses": [
            {
                "resource_id": "infores:arax",
                "score": 1.0,
                "path_bindings": {"p0": [{"id": "aux0", "attributes": []}]},
            }
        ],
        "node_bindings": {
            "n0": [{"id": "MONDO:0005148", "attributes": []}],
            "n1": [{"id": "CHEBI:15365", "attributes": []}],
        },
    },
    {"aux0": {"edges": ["e0"], "attributes": []}},
    {
        "nodes": {"MONDO:0005148": {}},
        "edges": {"e0": {"predicate": "biolink:related_to"}},
    },
)

# What Retriever's rehydrate endpoint returns (TRAPI 2.0).
REHYDRATED_KG = {
    "nodes": {
        "MONDO:0005148": {"categories": ["biolink:Disease"]},
        "CHEBI:15365": {"categories": ["biolink:SmallMolecule"]},
    },
    "edges": {
        "e0": {
            "subject": "CHEBI:15365",
            "predicate": "biolink:treats",
            "object": "MONDO:0005148",
            "knowledge_level": "knowledge_assertion",
            "agent_type": "manual_agent",
            "sources": [
                {
                    "resource_id": "infores:x",
                    "resource_role": "primary_knowledge_source",
                }
            ],
        }
    },
}


def _patch_query(mocker, query=None):
    return mocker.patch(
        "workers.arax_pathfinder.worker.get_message_sync",
        return_value=copy.deepcopy(query if query is not None else QUERY),
    )


def test_pathfinder_task_searches_rehydrates_and_saves(mocker):
    """The process-pool entrypoint reads by id, searches, and writes back.

    Only the two ids cross into the child: the message is loaded with
    ``get_message_sync``, assembled, and persisted with ``save_message_sync`` --
    the knowledge graph never has to be pickled back to the parent.
    """
    _patch_query(mocker)
    search = mocker.patch(
        "workers.arax_pathfinder.worker.execute_pathfinding",
        return_value=copy.deepcopy(PATHS_RESULT),
    )
    rehydrate = mocker.patch(
        "workers.arax_pathfinder.worker.rehydrate",
        return_value=copy.deepcopy(REHYDRATED_KG),
    )
    save = mocker.patch("workers.arax_pathfinder.worker.save_message_sync")

    pf_worker.arax_pathfinder_task("query-1", "resp-1", {}, LOGGER)

    search.assert_called_once()
    rehydrate.assert_called_once()
    save.assert_called_once()
    saved_id, message = save.call_args.args
    assert saved_id == "resp-1"
    kg = message["message"]["knowledge_graph"]
    assert kg["nodes"] == REHYDRATED_KG["nodes"]
    # The 2.0 edge from Retriever keeps its knowledge_level / agent_type.
    assert kg["edges"]["e0"]["knowledge_level"] == "knowledge_assertion"
    assert kg["edges"]["e0"]["agent_type"] == "manual_agent"
    # 1.x aux graphs / results from the pathfinder library are now 2.0.
    assert message["message"]["auxiliary_graphs"] == {"aux0": {"edges": ["e0"]}}
    assert len(message["message"]["results"]) == 1
    result = message["message"]["results"][0]
    assert result["essence"] == "result"
    assert result["node_bindings"] == {
        "n0": {"ids": ["MONDO:0005148"]},
        "n1": {"ids": ["CHEBI:15365"]},
    }
    assert result["analyses"][0]["path_bindings"] == {"p0": {"ids": ["aux0"]}}
    # Provenance is injected before saving.
    assert kg["edges"]["e0"]["sources"][-1] == {
        "resource_id": "infores:shepherd-arax",
        "resource_role": "aggregator_knowledge_source",
        "upstream_resource_ids": ["infores:arax"],
    }
    Message.from_dict(message["message"])
    # Defaults are filled in on the message that gets saved.
    assert message["parameters"]["tiers"] == [0]


def test_pathfinder_task_saves_empty_graphs_when_no_paths_found(mocker):
    """A search that finds nothing still writes a well-formed TRAPI message."""
    _patch_query(mocker)
    mocker.patch(
        "workers.arax_pathfinder.worker.execute_pathfinding",
        return_value=(None, None, None),
    )
    mocker.patch("workers.arax_pathfinder.worker.rehydrate", return_value=None)
    save = mocker.patch("workers.arax_pathfinder.worker.save_message_sync")

    pf_worker.arax_pathfinder_task("query-2", "resp-2", {}, LOGGER)

    _, message = save.call_args.args
    assert message["message"]["results"] == []
    # An empty auxiliary_graphs object is invalid in TRAPI 2.0: omitted.
    assert "auxiliary_graphs" not in message["message"]
    assert message["message"]["knowledge_graph"] == {"nodes": {}, "edges": {}}


def test_pathfinder_1x_kg_edges_are_upgraded(mocker):
    """A 1.x edge (knowledge_level / agent_type as attributes) is converted."""
    _patch_query(mocker)
    mocker.patch(
        "workers.arax_pathfinder.worker.execute_pathfinding",
        return_value=copy.deepcopy(PATHS_RESULT),
    )
    kg_1x = copy.deepcopy(REHYDRATED_KG)
    edge = kg_1x["edges"]["e0"]
    del edge["knowledge_level"], edge["agent_type"]
    edge["attributes"] = [
        {"attribute_type_id": "biolink:knowledge_level", "value": "prediction"},
        {"attribute_type_id": "biolink:agent_type", "value": "computational_model"},
    ]
    kg_1x["nodes"]["CHEBI:15365"] = {"categories": [], "name": "x"}
    mocker.patch("workers.arax_pathfinder.worker.rehydrate", return_value=kg_1x)
    save = mocker.patch("workers.arax_pathfinder.worker.save_message_sync")

    pf_worker.arax_pathfinder_task("query-6", "resp-6", {}, LOGGER)

    _, message = save.call_args.args
    kg = message["message"]["knowledge_graph"]
    assert kg["edges"]["e0"]["knowledge_level"] == "prediction"
    assert kg["edges"]["e0"]["agent_type"] == "computational_model"
    assert kg["nodes"]["CHEBI:15365"]["categories"] == ["biolink:NamedThing"]
    Message.from_dict(message["message"])


def test_parse_query_graph_reads_required_intermediate_categories():
    qgraph = copy.deepcopy(QUERY["message"]["query_graph"])
    qgraph["paths"]["p0"]["constraints"] = [
        {"required_intermediate_categories": ["biolink:Gene"]}
    ]
    _, pinned_ids, categories = pf_worker.parse_query_graph(qgraph)
    assert pinned_ids == ["MONDO:0005148", "CHEBI:15365"]
    assert categories == ["biolink:Gene"]


@pytest.mark.parametrize(
    "qgraph, expected",
    [
        pytest.param(
            {
                "nodes": {"n0": {"ids": ["MONDO:0005148"]}, "n1": {}},
                "paths": {"p0": {"constraints": []}},
            },
            "two pinned nodes",
            id="one_pinned_node",
        ),
        pytest.param(
            {
                "nodes": {
                    "n0": {"ids": ["MONDO:0005148"]},
                    "n1": {"ids": ["CHEBI:15365"]},
                },
                "paths": {
                    "p0": {
                        "constraints": [
                            {"required_intermediate_categories": ["biolink:Gene"]},
                            {"required_intermediate_categories": ["biolink:Drug"]},
                        ]
                    }
                },
            },
            "multiple constraints",
            id="multiple_constraints",
        ),
        pytest.param(
            {
                "nodes": {
                    "n0": {"ids": ["MONDO:0005148"]},
                    "n1": {"ids": ["CHEBI:15365"]},
                },
                "paths": {
                    "p0": {
                        "constraints": [
                            {
                                "required_intermediate_categories": [
                                    "biolink:Gene",
                                    "biolink:Drug",
                                ]
                            }
                        ]
                    }
                },
            },
            "multiple intermediate categories",
            id="multiple_intermediate_categories",
        ),
    ],
)
def test_unanswerable_query_graph_raises(mocker, qgraph, expected):
    """Validation failures raise so run_task_lifecycle can fail the query.

    They used to ``return message, 500``, a value the caller discarded -- so the
    task was wrapped up as a success and no message was ever written to
    ``response_id``, leaving the next worker to ``KeyError`` on it. Raising
    routes the query to ``finish_query`` with an ERROR status instead.
    """
    _patch_query(mocker, {"message": {"query_graph": qgraph}})
    save = mocker.patch("workers.arax_pathfinder.worker.save_message_sync")
    search = mocker.patch("workers.arax_pathfinder.worker.execute_pathfinding")

    with pytest.raises(ValueError, match=expected):
        pf_worker.arax_pathfinder_task("query-3", "resp-3", {}, LOGGER)

    search.assert_not_called()
    save.assert_not_called()


def test_search_failure_propagates_instead_of_saving_error_blob(mocker):
    """A failed search raises rather than persisting a non-TRAPI error blob.

    The old code caught everything and saved ``{"status": "error", ...}`` to the
    response id, then reported success -- so that blob flowed on through the
    workflow. Now the exception reaches ``run_task_lifecycle``.
    """
    _patch_query(mocker)
    mocker.patch(
        "workers.arax_pathfinder.worker.execute_pathfinding",
        side_effect=RuntimeError("sqlite is on fire"),
    )
    save = mocker.patch("workers.arax_pathfinder.worker.save_message_sync")

    with pytest.raises(RuntimeError, match="sqlite is on fire"):
        pf_worker.arax_pathfinder_task("query-4", "resp-4", {}, LOGGER)

    save.assert_not_called()


def test_rehydrate_failure_propagates(mocker):
    """A rehydrate failure fails the task too, rather than saving a partial KG."""
    _patch_query(mocker)
    mocker.patch(
        "workers.arax_pathfinder.worker.execute_pathfinding",
        return_value=copy.deepcopy(PATHS_RESULT),
    )
    mocker.patch(
        "workers.arax_pathfinder.worker.rehydrate",
        side_effect=RuntimeError("retriever unreachable"),
    )
    save = mocker.patch("workers.arax_pathfinder.worker.save_message_sync")

    with pytest.raises(RuntimeError, match="retriever unreachable"):
        pf_worker.arax_pathfinder_task("query-5", "resp-5", {}, LOGGER)

    save.assert_not_called()


def test_descendants_are_memoized_per_child(mocker):
    """The Biolink model is built once per pool child, not once per task.

    ``BiolinkHelper`` construction parses the model, and the old code did it
    inside every task alongside a fresh blocked-list HTTP fetch.
    """
    mocker.patch.object(pf_worker, "_biolink_helper", None)
    mocker.patch.object(pf_worker, "_descendants_cache", {})
    helper = MagicMock()
    helper.get_descendants.return_value = ["biolink:Gene", "biolink:Protein"]
    helper_cls = mocker.patch(
        "workers.arax_pathfinder.worker.BiolinkHelper", return_value=helper
    )

    first = pf_worker.get_descendants("biolink:NamedThing")
    second = pf_worker.get_descendants("biolink:NamedThing")

    assert first == second == {"biolink:Gene", "biolink:Protein"}
    helper_cls.assert_called_once()
    helper.get_descendants.assert_called_once_with("biolink:NamedThing")
