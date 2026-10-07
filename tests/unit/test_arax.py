"""Tests for ``workers.arax.worker``: the legacy path, which sends a query to
the remote ARAX service, and (with ``parameters.arax_internal: true``) the one
that runs ARAX in-process (DEC-14).

The success and ARAX-error cases run the real ported ARAXQuery on ARAXi plans
that need no KP; whole-query parity with upstream ARAX is covered by
``tests/unit/arax/test_query_parity.py``.
"""

import json
import logging

import httpx
import pytest
from translator_tom import Response

import workers.arax.worker as worker
from workers.arax.worker import INTERNAL_ERROR, ARAXServiceError, arax
from shepherd_utils.db import encode_message
from shepherd_utils.logger import attach_query_handler, get_query_handler
from shepherd_utils.trapi import (
    ENVELOPE_MEMBERS,
    finalize_response,
    prepare_stored_response,
)

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
def query_logger():
    """A task logger with a query log handler, as the worker's tasks have."""
    task_logger = logging.getLogger(f"{__name__}.query")
    attach_query_handler(task_logger)
    get_query_handler(task_logger).drain()
    return task_logger


@pytest.fixture
def db(mocker):
    """Stub the worker's db calls; returns the dict of saved messages.

    The response is captured where ``save_response_sync`` stores it, so what
    the tests see is the stored form. The query's log store is
    ``store["logs"]``.
    """
    store = {}

    def _setup(message, internal=True):
        # Most tests here are of the in-process path, which a query opts into
        if internal:
            message = {
                **message,
                "parameters": {
                    **(message.get("parameters") or {}),
                    "arax_internal": True,
                },
            }
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
            "shepherd_utils.db.save_message_sync",
            side_effect=lambda response_id, msg: store.__setitem__(
                response_id, json.loads(json.dumps(msg))
            ),
        )

        async def save_response(response_id, response, task_logger):
            store[response_id] = json.loads(
                json.dumps(prepare_stored_response(response))
            )

        mocker.patch("workers.arax.worker.save_response", side_effect=save_response)

        async def save_logs(response_id, task_logger):
            store.setdefault("logs", {}).setdefault(response_id, []).extend(
                get_query_handler(task_logger).drain()
            )

        mocker.patch("workers.arax.worker.save_logs", side_effect=save_logs)
        return store

    return _setup


@pytest.fixture
def span(mocker):
    span = mocker.MagicMock()
    mocker.patch("workers.arax.worker.get_current_span", return_value=span)
    return span


@pytest.mark.asyncio
async def test_query_runs_in_process_and_saves_arax_response(
    db, span, mocker, query_logger
):
    mocker.patch.object(worker.settings, "server_url", "http://shepherd.test")
    store = db(OPERATIONS_QUERY)
    task = _task()

    await arax(task, query_logger)

    saved = store["response_id"]
    assert saved["status"] == "Success"
    assert saved["http_status"] == 200
    assert saved["id"] == "http://shepherd.test/arax/response/response_id"
    assert set(saved["message"]["query_graph"]["nodes"]) == {"n0", "n1"}
    assert saved["operations"]["actions"] == OPERATIONS_QUERY["operations"]["actions"]
    assert saved["tool_version"] == "ARAX 1.6.2"
    # Stored in Shepherd's stored form: no delivery envelope ...
    assert not set(ENVELOPE_MEMBERS) & set(saved)
    # ... and ARAX's log, part of its response, is in the query's log store,
    # which the delivered response's logs come from
    arax_logs = store["logs"]["response_id"]
    assert any(
        "Processing action 'add_qedge'" in entry["message"] for entry in arax_logs
    )
    assert all(None not in entry.values() for entry in arax_logs)
    span.set_attribute.assert_any_call("arax.status_code", 200)
    assert json.loads(task[1]["workflow"]) == [{"id": "arax"}]


@pytest.mark.asyncio
async def test_stored_arax_response_is_valid_trapi_2(db, span, query_logger):
    """What the worker stores is valid TRAPI 2.0 content, and so is it once
    delivered (the envelope and logs added)."""
    query = json.loads(json.dumps(OPERATIONS_QUERY))
    query["parameters"] = {"log_level": "DEBUG", "timeout": 60}
    store = db(query)

    await arax(_task(), query_logger)

    saved = store["response_id"]
    Response.from_dict(saved)
    delivered = finalize_response(
        json.loads(json.dumps(saved)), query, store["logs"]["response_id"]
    )
    assert delivered["schema_version"] == "2.0.0"
    assert delivered["parameters"] == query["parameters"]
    assert delivered["logs"]
    Response.from_dict(delivered)


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
    # ARAX's own error response, as its /query returns it (in stored form: the
    # query's submitter is not a Response member)
    assert saved["status"] == "NoQueryMessageOrOperations"
    assert saved["http_status"] == 400
    assert saved["description"] == "No message or operations present in Query"
    assert "submitter" not in saved


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
                                "predicate": "biolink:related_to",
                                "knowledge_level": "not_provided",
                                "agent_type": "not_provided",
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
    query_graph = {
        "nodes": {"n0": {"ids": ["CHEBI:1"]}, "n1": {}},
        "edges": {"e0": {"subject": "n0", "object": "n1"}},
    }
    query = {"message": {"query_graph": query_graph}}
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
    Response.from_dict(saved)


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
    encode_message(saved)  # what save_response_sync stores


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
    query_id, response_id, carrier, submitted_at = pool.run.await_args.args[2:]
    assert (query_id, response_id) == ("query_id", "response_id")
    # the task span's context, for the child's spans, and the submit time
    assert isinstance(carrier, dict)
    assert isinstance(submitted_at, float)
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


@pytest.mark.parametrize(
    "parameters",
    [None, {}, {"arax_internal": False}, {"arax_internal": "true"}],
)
def test_queries_use_the_remote_arax_service_by_default(parameters):
    query = {"message": {}}
    if parameters is not None:
        query["parameters"] = parameters
    assert not worker.uses_internal_workers(query)


def test_arax_internal_opts_into_shepherds_workers():
    assert worker.uses_internal_workers({"parameters": {"arax_internal": True}})


LEGACY_RESPONSE = {
    "status": "Success",
    "description": "ok",
    "message": {
        "knowledge_graph": {
            "nodes": {},
            "edges": {
                "e0": {
                    "subject": "a",
                    "object": "b",
                    "predicate": "biolink:related_to",
                    "knowledge_level": "not_provided",
                    "agent_type": "not_provided",
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
    "logs": [{"level": "INFO", "message": "from remote ARAX", "code": None}],
}


@pytest.fixture
def remote_arax(mocker):
    """Stub the remote ARAX service; returns the list of bodies it was sent."""
    sent = []

    def _setup(handler):
        def _handle(request):
            sent.append(json.loads(request.content))
            return handler(request)

        transport = httpx.MockTransport(_handle)
        real_client = httpx.AsyncClient
        mocker.patch(
            "workers.arax.worker.httpx.AsyncClient",
            side_effect=lambda **kwargs: real_client(transport=transport, **kwargs),
        )
        mocker.patch.object(worker.settings, "arax_url", "http://arax.test/query")
        return sent

    return _setup


@pytest.mark.asyncio
async def test_legacy_query_is_sent_to_the_remote_arax_service(
    db, span, mocker, remote_arax, query_logger
):
    query = {"message": {}, "stream_progress": True}
    store = db(query, internal=False)
    sent = remote_arax(lambda request: httpx.Response(200, json=LEGACY_RESPONSE))
    run = mocker.patch("workers.arax.worker.run_arax")
    finish = mocker.patch("workers.arax.worker.finish_progress")
    task = _task()

    await arax(task, query_logger)

    assert not run.called
    # sent non-streaming, with Shepherd's submitter
    assert "stream_progress" not in sent[0]
    assert sent[0]["submitter"].startswith("infores:shepherd-arax:")
    saved = store["response_id"]
    sources = saved["message"]["knowledge_graph"]["edges"]["e0"]["sources"]
    assert sources[-1]["resource_id"] == "infores:shepherd-arax"
    assert "logs" not in saved
    assert any(
        entry["message"] == "from remote ARAX"
        for entry in store["logs"]["response_id"]
    )
    # a streaming client is told there is nothing more to relay
    finish.assert_called_once_with("response_id")
    span.set_attribute.assert_any_call("arax.path", "legacy")
    span.set_attribute.assert_any_call("arax.status_code", 200)
    assert json.loads(task[1]["workflow"]) == [{"id": "arax"}]


@pytest.mark.asyncio
async def test_legacy_arax_error_saves_an_error_response_and_raises_its_status(
    db, span, remote_arax
):
    query = {"message": {"query_graph": {"nodes": {"n0": {"ids": ["CHEBI:1"]}}}}}
    store = db(query, internal=False)
    remote_arax(lambda request: httpx.Response(503, text="down for maintenance"))

    with pytest.raises(ARAXServiceError) as excinfo:
        await arax(_task(), logger)

    assert excinfo.value.status_code == 503
    assert "down for maintenance" in str(excinfo.value)
    saved = store["response_id"]
    assert saved["status"] == "Error"
    assert "[HTTP 503]" in saved["description"]
    assert saved["message"]["results"] == []


@pytest.mark.asyncio
async def test_unreachable_remote_arax_is_a_bad_gateway(db, span, remote_arax):
    store = db({"message": {}}, internal=False)

    def refuse(request):
        raise httpx.ConnectError("connection refused")

    remote_arax(refuse)

    with pytest.raises(ARAXServiceError) as excinfo:
        await arax(_task(), logger)

    assert excinfo.value.status_code == worker.BAD_GATEWAY
    assert store["response_id"]["status"] == "Error"


@pytest.mark.asyncio
async def test_legacy_pathfinder_query_is_still_routed(db, mocker, remote_arax):
    store = db(dict(PATHFINDER_QUERY), internal=False)
    sent = remote_arax(lambda request: httpx.Response(200, json=LEGACY_RESPONSE))
    task = _task()

    await arax(task, logger)

    assert sent == []
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


# --- pool children: prewarm and cold-start tracing ---


def _child_state():
    """Run in a pool child: what its setup looked like before this task."""
    startup = dict(worker._child_startup)
    did_setup = worker.prepare_pool_child()
    return startup, did_setup


@pytest.mark.asyncio
async def test_prewarmed_child_is_ready_before_its_first_task(monkeypatch):
    import asyncio

    from shepherd_utils.process_pool import ProcessPoolManager

    # inherited by the spawned child: no OTLP exporter there
    monkeypatch.setenv("OTEL_ENABLED", "false")
    pool = ProcessPoolManager(
        max_workers=1, name="test arax pool", warmup=worker._warm_pool_child
    )
    try:
        startup, did_setup = await pool.run(asyncio.get_running_loop(), _child_state)
    finally:
        pool.shutdown()
    assert startup["prewarmed"] is True
    assert did_setup is False
    assert (
        startup["process_started_ns"]
        <= startup["setup_started_ns"]
        <= startup["tracer_ready_ns"]
        <= startup["arax_ready_ns"]
    )


def test_process_start_time_is_in_the_past():
    import time

    started = worker._process_started_ns()
    assert started is not None
    assert time.time_ns() - 24 * 3600 * 10**9 < started <= time.time_ns()


@pytest.fixture
def child_spans(monkeypatch, mocker):
    """Run arax_query_task as a fresh pool child, recording its spans."""
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(worker, "tracer", provider.get_tracer("test"))
    monkeypatch.setattr(worker, "_child_startup", {})
    monkeypatch.setattr(worker, "_child_tasks", 0)
    mocker.patch(
        "workers.arax.worker.multiprocessing.parent_process", return_value=object()
    )
    mocker.patch("workers.arax.worker.setup_pool_child_tracer")
    mocker.patch("workers.arax.worker.instrument_arax")
    exporter.tracer = provider.get_tracer("test")
    return exporter


def _query_spans(exporter):
    by_name = {}
    for span in exporter.get_finished_spans():
        by_name.setdefault(span.name, []).append(span)
    return by_name


def test_cold_child_records_its_startup_beside_the_query(db, child_spans):
    from opentelemetry.propagate import inject

    db(OPERATIONS_QUERY)
    with child_spans.tracer.start_as_current_span("arax") as task_span:
        carrier = {}
        inject(carrier)
    worker.arax_query_task("query_id", "r1", carrier)
    worker.arax_query_task("query_id", "r2", carrier)

    spans = _query_spans(child_spans)
    (startup,) = spans["arax.pool.child_startup"]
    first, second = spans["arax.query"]
    task_span_id = task_span.get_span_context().span_id
    assert startup.parent.span_id == task_span_id
    assert first.parent.span_id == task_span_id
    for key in (
        "arax.pool.spawn_and_import_ms",
        "arax.pool.tracer_setup_ms",
        "arax.pool.arax_import_ms",
    ):
        assert startup.attributes[key] >= 0
    assert startup.end_time <= first.start_time
    assert first.attributes["arax.pool.child_cold"] is True
    assert first.attributes["arax.pool.child_prewarmed"] is False
    assert first.attributes["arax.pool.child_task_number"] == 1
    # the same child's next query is warm, with no startup span
    assert second.attributes["arax.pool.child_cold"] is False
    assert second.attributes["arax.pool.child_task_number"] == 2


def test_prewarmed_child_has_no_startup_span(db, child_spans):
    db(OPERATIONS_QUERY)
    worker._warm_pool_child()
    worker.arax_query_task("query_id", "r1")

    spans = _query_spans(child_spans)
    assert "arax.pool.child_startup" not in spans
    (query,) = spans["arax.query"]
    assert query.attributes["arax.pool.child_cold"] is False
    assert query.attributes["arax.pool.child_prewarmed"] is True
