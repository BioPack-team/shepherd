"""Tests for ``workers.arax.worker``, which runs ARAX in-process (DEC-14).

The success and ARAX-error cases run the real ported ARAXQuery on ARAXi plans
that need no KP; whole-query parity with upstream ARAX is covered by
``tests/unit/arax/test_query_parity.py``.
"""

import json
import logging

import pytest

import workers.arax.worker as worker
from workers.arax.worker import INTERNAL_ERROR, ARAXServiceError, arax
from shepherd_utils.db import encode_message

logger = logging.getLogger(__name__)

PATHFINDER_QUERY = {
    "message": {
        "query_graph": {
            "nodes": {"a": {"ids": ["MONDO:0005148"]}, "b": {"ids": ["CHEBI:15365"]}},
            "paths": {"p0": {"subject": "a", "object": "b"}},
        }
    }
}

# An ARAXi plan that needs no KP (or NodeNorm, which add_qnode(ids=...) calls):
# build a query graph and return it
OPERATIONS_QUERY = {
    "operations": {
        "actions": [
            "add_qnode(key=n0, categories=biolink:SmallMolecule)",
            "add_qnode(key=n1, categories=biolink:Disease)",
            "add_qedge(key=e0, subject=n0, object=n1)",
            "return(message=true, store=true)",
        ]
    }
}


def _task():
    return [
        "task_id",
        {
            "query_id": "query_id",
            "response_id": "response_id",
            "workflow": json.dumps([{"id": "arax"}]),
            "log_level": "20",
            "otel": json.dumps({}),
            "metadata": json.dumps({}),
        },
    ]


@pytest.fixture
def db(mocker):
    """Stub the worker's db calls; returns the dict of saved messages."""
    store = {}

    def _setup(message):
        mocker.patch(
            "workers.arax.worker.get_message",
            new_callable=mocker.AsyncMock,
            return_value=message,
        )
        mocker.patch(
            "workers.arax.worker.get_message_sync",
            side_effect=lambda query_id: json.loads(json.dumps(message)),
        )
        mocker.patch(
            "workers.arax.worker.save_message_sync",
            side_effect=lambda response_id, msg: store.__setitem__(response_id, msg),
        )
        return store

    return _setup


@pytest.fixture
def span(mocker):
    span = mocker.MagicMock()
    mocker.patch("workers.arax.worker.get_current_span", return_value=span)
    return span


@pytest.mark.asyncio
async def test_query_runs_in_process_and_saves_arax_response(db, span, mocker):
    mocker.patch.object(worker.settings, "server_url", "http://shepherd.test")
    store = db(OPERATIONS_QUERY)
    task = _task()

    await arax(task, logger)

    saved = store["response_id"]
    assert saved["status"] == "Success"
    assert saved["http_status"] == 200
    assert saved["id"] == "http://shepherd.test/arax/response/response_id"
    assert set(saved["message"]["query_graph"]["nodes"]) == {"n0", "n1"}
    assert saved["operations"]["actions"] == OPERATIONS_QUERY["operations"]["actions"]
    assert saved["tool_version"] == "ARAX 1.6.2"
    # ARAX's log is part of the response, as in ARAX
    assert any(
        "Processing action 'add_qedge'" in entry["message"] for entry in saved["logs"]
    )
    span.set_attribute.assert_any_call("arax.status_code", 200)
    assert json.loads(task[1]["workflow"]) == [{"id": "arax"}]


def test_run_arax_fills_in_the_shepherd_submitter():
    query = json.loads(json.dumps(OPERATIONS_QUERY))
    worker.run_arax(query, "response_id")
    assert query["submitter"].startswith("infores:shepherd-arax:")


@pytest.mark.asyncio
async def test_arax_error_saves_arax_response_and_raises_its_status(db, span):
    store = db({"submitter": "tester"})  # no message and no operations

    with pytest.raises(ARAXServiceError) as excinfo:
        await arax(_task(), logger)

    assert excinfo.value.status_code == 400
    assert "NoQueryMessageOrOperations" in str(excinfo.value)
    span.set_attribute.assert_any_call("arax.status_code", 400)
    saved = store["response_id"]
    # ARAX's own error response, as its /query returns it
    assert saved["status"] == "NoQueryMessageOrOperations"
    assert saved["http_status"] == 400
    assert saved["description"] == "No message or operations present in Query"
    assert saved["submitter"] == "tester"


@pytest.mark.asyncio
async def test_successful_response_gets_shepherd_provenance(db, span, mocker):
    store = db({"message": {}})
    mocker.patch(
        "workers.arax.worker.run_arax",
        return_value=(
            {
                "status": "Success",
                "description": "ok",
                "message": {
                    "knowledge_graph": {
                        "nodes": {},
                        "edges": {
                            "e0": {
                                "subject": "a",
                                "object": "b",
                                "sources": [
                                    {
                                        "resource_id": "infores:arax",
                                        "resource_role": "primary_knowledge_source",
                                    }
                                ],
                            }
                        },
                    },
                    "results": [],
                },
            },
            200,
        ),
    )

    await arax(_task(), logger)

    sources = store["response_id"]["message"]["knowledge_graph"]["edges"]["e0"][
        "sources"
    ]
    assert sources[-1]["resource_id"] == "infores:shepherd-arax"
    assert sources[-1]["upstream_resource_ids"] == ["infores:arax"]


@pytest.mark.asyncio
async def test_unserializable_response_is_an_internal_error(db, span, mocker):
    query = {"message": {"query_graph": {"nodes": {}, "edges": {}}}}
    store = db(query)
    mocker.patch(
        "workers.arax.worker.run_arax",
        return_value=({"status": "Success", "x": float("nan")}, 200),
    )

    with pytest.raises(ARAXServiceError) as excinfo:
        await arax(_task(), logger)

    assert excinfo.value.status_code == INTERNAL_ERROR
    saved = store["response_id"]
    assert saved["status"] == "Error"
    assert f"[HTTP {INTERNAL_ERROR}]" in saved["description"]
    assert saved["message"]["query_graph"] == query["message"]["query_graph"]
    assert saved["message"]["results"] == []


@pytest.mark.asyncio
async def test_numpy_values_are_saved_as_json_numbers(db, span, mocker):
    """ARAX's stdlib json writes numpy floats as numbers; Shepherd's store
    (orjson) rejects them, so the worker saves the serialized form."""
    import numpy as np

    store = db({"message": {}})
    mocker.patch(
        "workers.arax.worker.run_arax",
        return_value=(
            {"status": "Success", "message": {"results": [], "score": np.float64(0.5)}},
            200,
        ),
    )

    await arax(_task(), logger)

    saved = store["response_id"]
    assert type(saved["message"]["score"]) is float
    assert saved["message"]["score"] == 0.5
    encode_message(saved)  # what save_message_sync stores


@pytest.mark.asyncio
async def test_non_json_value_is_an_internal_error(db, span, mocker):
    store = db({"message": {}})
    mocker.patch(
        "workers.arax.worker.run_arax",
        return_value=({"status": "Success", "x": object()}, 200),
    )

    with pytest.raises(ARAXServiceError) as excinfo:
        await arax(_task(), logger)

    assert excinfo.value.status_code == INTERNAL_ERROR
    assert store["response_id"]["status"] == "Error"


@pytest.mark.asyncio
async def test_query_runs_in_the_pool_when_given_one(db, span, mocker):
    store = db(OPERATIONS_QUERY)
    pool = mocker.MagicMock()

    async def run(loop, fn, *args):
        return fn(*args)

    pool.run = mocker.AsyncMock(side_effect=run)
    await arax(_task(), logger, pool=pool)

    assert pool.run.await_args.args[1] is worker.arax_query_task
    assert pool.run.await_args.args[2:] == ("query_id", "response_id")
    assert store["response_id"]["status"] == "Success"


@pytest.mark.asyncio
async def test_pathfinder_query_is_routed_without_running_arax(db, mocker):
    store = db(dict(PATHFINDER_QUERY))
    run = mocker.patch("workers.arax.worker.run_arax")
    task = _task()

    await arax(task, logger)

    assert not run.called
    assert store == {}
    assert json.loads(task[1]["workflow"]) == [{"id": "arax.pathfinder"}]


def test_warm_biolink_cache_builds_the_lookup_map(tmp_path, mocker, caplog):
    """The worker builds the map at startup; the first query then finds it."""
    import os
    import shutil

    mocker.patch.object(worker.settings, "arax_biolink_cache_dir", str(tmp_path))
    shutil.copy(
        os.path.join(
            os.path.dirname(__file__),
            "arax",
            "expand_parity",
            "biolink_lookup_map_4.2.5_v5.pickle",
        ),
        tmp_path,
    )
    caplog.set_level(logging.INFO)

    worker.warm_biolink_cache(logger)

    assert "Biolink lookup map ready" in caplog.text
    assert (tmp_path / "biolink_lookup_map_4.2.5_v5.pickle").exists()


def test_warm_biolink_cache_failure_only_warns(mocker, caplog):
    mocker.patch(
        "shepherd_utils.arax.BiolinkHelper.biolink_helper.get_biolink_helper",
        side_effect=OSError("no network"),
    )

    worker.warm_biolink_cache(logger)  # does not raise

    assert "Could not warm the Biolink cache (OSError: no network)" in caplog.text


def test_biolink_cache_defaults_to_the_arax_data_volume(mocker):
    from shepherd_utils.data_download import arax_biolink_cache_path

    mocker.patch.object(worker.settings, "arax_dbs_dir", "/data/arax_dbs")
    mocker.patch.object(worker.settings, "arax_biolink_cache_dir", "")
    assert arax_biolink_cache_path() == "/data/arax_dbs/biolink"
    mocker.patch.object(worker.settings, "arax_biolink_cache_dir", "/cache")
    assert arax_biolink_cache_path() == "/cache"


def test_kp_cache_refresh_runs_one_pass_at_a_time(mocker, arax_kp_cache_store):
    mocker.patch.object(worker, "_get_sync_data_db", return_value=arax_kp_cache_store)
    refresh = mocker.patch(
        "shepherd_utils.arax.Expand.trapi_query_cacher.KPQueryCacher.refresh_cache"
    )

    assert worker.refresh_kp_cache_once(logger) is True
    refresh.assert_called_once()
    assert arax_kp_cache_store.get(worker.KP_CACHE_REFRESH_LOCK_KEY) is None

    # another replica holds the lock
    arax_kp_cache_store.set(worker.KP_CACHE_REFRESH_LOCK_KEY, "other", ex=100)
    assert worker.refresh_kp_cache_once(logger) is False
    refresh.assert_called_once()
    assert arax_kp_cache_store.get(worker.KP_CACHE_REFRESH_LOCK_KEY) == b"other"


def test_kp_cache_refresh_failure_releases_the_lock(
    mocker, arax_kp_cache_store, caplog
):
    mocker.patch.object(worker, "_get_sync_data_db", return_value=arax_kp_cache_store)
    mocker.patch(
        "shepherd_utils.arax.Expand.trapi_query_cacher.KPQueryCacher.refresh_cache",
        side_effect=RuntimeError("kp down"),
    )
    assert worker.refresh_kp_cache_once(logger) is True
    assert "KP cache refresh failed: RuntimeError: kp down" in caplog.text
    assert arax_kp_cache_store.get(worker.KP_CACHE_REFRESH_LOCK_KEY) is None


@pytest.mark.asyncio
async def test_kp_cache_refresh_loop_is_off_when_disabled(mocker):
    once = mocker.patch.object(worker, "refresh_kp_cache_once")
    mocker.patch.object(worker.settings, "arax_kp_cache_refresh_interval_sec", 0)
    await worker.kp_cache_refresh_loop(None, logger)
    mocker.patch.object(worker.settings, "arax_kp_cache_refresh_interval_sec", 60)
    mocker.patch.object(worker.settings, "arax_kp_cache_enabled", False)
    await worker.kp_cache_refresh_loop(None, logger)
    once.assert_not_called()
