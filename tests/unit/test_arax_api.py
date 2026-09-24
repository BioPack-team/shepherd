"""Tests for Shepherd's ARAX API (``shepherd_server/aras/arax.py``).

The streaming tests connect the real arax worker -- running ARAX's own
``query_return_stream`` on an ARAXi plan that needs no KP -- to the server's
relay through an in-memory stand-in for the Redis progress list.
"""

import json
import logging
import time

import pytest

import shepherd_server.aras.arax as api
import workers.arax.worker as worker
from shepherd_utils.arax_progress import DONE_MARKER

logger = logging.getLogger(__name__)

PLAN = {
    "operations": {
        "actions": [
            "add_qnode(key=n0, categories=biolink:SmallMolecule)",
            "add_qnode(key=n1, categories=biolink:Disease)",
            "add_qedge(key=e0, subject=n0, object=n1)",
            "return(message=true, store=true)",
        ]
    }
}


def _row(state="COMPLETED", status="OK"):
    """A shepherd_brain row, in the column order get_query_state returns."""
    return ("qid", None, None, None, None, None, None, "rid", None, state, status, None)


class FakeShepherd:
    """The data store and progress list both sides share."""

    def __init__(self, mocker, query):
        self.messages = {"qid": json.loads(json.dumps(query))}
        self.progress = []
        self.state = _row(state="QUEUED")
        # worker side
        mocker.patch.object(worker, "get_message_sync", side_effect=self._get)
        mocker.patch.object(worker, "save_message_sync", side_effect=self._save)
        mocker.patch.object(worker, "push_progress", side_effect=self._push)
        mocker.patch.object(
            worker,
            "finish_progress",
            side_effect=lambda rid: self._push(rid, DONE_MARKER),
        )
        # server side
        mocker.patch.object(
            api,
            "run_query",
            new_callable=mocker.AsyncMock,
            return_value=("qid", "rid", logger),
        )
        mocker.patch.object(api, "read_progress", side_effect=self._read)
        mocker.patch.object(api, "get_query_state", side_effect=self._state)
        mocker.patch(
            "shepherd_server.base_routes.get_query_state", side_effect=self._state
        )
        mocker.patch.object(api, "get_message", side_effect=self._aget)
        import fakeredis

        self.redis = fakeredis.aioredis.FakeRedis()
        mocker.patch.object(api.arax_status, "data_db_client", self.redis)
        mocker.patch.object(
            api,
            "get_logs",
            new_callable=mocker.AsyncMock,
            return_value=[{"shepherd": 1}],
        )
        mocker.patch(
            "shepherd_server.base_routes.asyncio.sleep", new_callable=mocker.AsyncMock
        )
        mocker.patch.object(api.asyncio, "sleep", new_callable=mocker.AsyncMock)

    def _get(self, key):
        return json.loads(json.dumps(self.messages[key]))

    def _save(self, key, message):
        self.messages[key] = json.loads(json.dumps(message))

    def _push(self, rid, line):
        assert rid == "rid"
        self.progress.append(line)

    async def _read(self, rid, start):
        return self.progress[start:]

    async def _state(self, qid, logger):
        return self.state

    async def _aget(self, key, logger):
        return self._get(key) if key in self.messages else None

    def run_worker(self):
        summary = worker.arax_query_task("qid", "rid")
        self.state = _row()
        return summary


async def _collect(response):
    chunks = []
    async for chunk in response.body_iterator:
        chunks.append(chunk if isinstance(chunk, str) else chunk.decode())
    return "".join(chunks).splitlines()


@pytest.mark.asyncio
async def test_stream_relays_arax_progress_then_the_response(mocker):
    shepherd = FakeShepherd(mocker, dict(PLAN, stream_progress=True))
    # Let the stream loop start before the query finishes (see the next test)
    from shepherd_utils.arax.ARAX_query import ARAXQuery

    query = ARAXQuery.query

    def slow_query(self, *args, **kwargs):
        time.sleep(0.3)
        return query(self, *args, **kwargs)

    mocker.patch.object(ARAXQuery, "query", slow_query)
    summary = shepherd.run_worker()
    assert summary["http_status"] == 200
    assert shepherd.progress[-1] == DONE_MARKER

    response = await api.arax_stream_query(dict(PLAN, stream_progress=True))
    assert response.status_code == 200
    assert response.media_type == "text/event-stream"
    lines = [json.loads(line) for line in await _collect(response)]

    # ARAX's own stream: log entries and the pid token (query_plan updates too,
    # when a plan has any) ...
    messages = [x["message"] for x in lines if "message" in x and "level" in x]
    assert any("Processing action 'add_qedge'" in m for m in messages)
    tokens = [x for x in lines if set(x) == {"pid", "authorization"}]
    assert len(tokens) == 1
    # the server's own token, which /status?terminate_pid resolves to this query
    assert tokens[0]["pid"] == 1
    assert tokens[0]["authorization"] == api.arax_status.pid_authorization(1)
    # ... then the response, as saved
    final = lines[-1]
    assert final["status"] == "Success"
    assert set(final["message"]["query_graph"]["nodes"]) == {"n0", "n1"}
    assert final == shepherd.messages["rid"]
    # nothing but the done marker is left out
    assert len(lines) == len(shepherd.progress)


@pytest.mark.asyncio
async def test_stream_of_a_failed_query_ends_with_arax_error_envelope(mocker):
    query = {"stream_progress": True, "submitter": "tester"}
    shepherd = FakeShepherd(mocker, query)
    summary = shepherd.run_worker()
    assert summary["http_status"] == 400

    lines = [
        json.loads(line) for line in await _collect(await api.arax_stream_query(query))
    ]
    # ARAX's stream yields nothing when its query thread finishes before the
    # stream loop starts (as an input error does), so only the response comes
    final = lines[-1]
    assert final["tool_version"].startswith("ARAX ")
    assert any(
        x.get("code") == "NoQueryMessageOrOperations" and x.get("level") == "ERROR"
        for x in final["logs"]
    )


@pytest.mark.asyncio
async def test_stream_without_worker_progress_ends_when_the_query_does(mocker):
    """A pathfinder query goes to arax.pathfinder, which relays nothing."""
    shepherd = FakeShepherd(mocker, {})
    shepherd.messages["rid"] = {"message": {"results": [1]}}
    shepherd.state = _row()
    lines = await _collect(await api.arax_stream_query({"stream_progress": True}))
    assert [json.loads(line) for line in lines] == [
        {"logs": [{"shepherd": 1}], "message": {"results": [1]}}
    ]


@pytest.mark.asyncio
async def test_stream_times_out(mocker):
    FakeShepherd(mocker, {})
    lines = await _collect(
        await api.arax_stream_query(
            {"stream_progress": True, "parameters": {"timeout": -1}}
        )
    )
    assert json.loads(lines[-1]) == {
        "status": "TIMEOUT",
        "description": "Query timeout",
    }


@pytest.mark.asyncio
async def test_sync_query_returns_arax_envelope_logs_and_status(mocker):
    shepherd = FakeShepherd(mocker, PLAN)
    shepherd.run_worker()
    response = await api.arax_sync_query(dict(PLAN))
    body = json.loads(bytes(response.body))
    assert response.status_code == 200
    assert body["http_status"] == 200
    # ARAX's own log, not Shepherd's
    assert any("Processing action" in entry["message"] for entry in body["logs"])


@pytest.mark.asyncio
async def test_sync_query_arax_error_keeps_arax_http_status(mocker):
    query = {"submitter": "tester"}
    shepherd = FakeShepherd(mocker, query)
    shepherd.run_worker()
    shepherd.state = _row(status="ERROR")
    response = await api.arax_sync_query(query)
    body = json.loads(bytes(response.body))
    assert response.status_code == 400
    assert body["status"] == "NoQueryMessageOrOperations"


@pytest.mark.asyncio
async def test_sync_query_non_arax_response_is_finished_like_shepherds(mocker):
    shepherd = FakeShepherd(mocker, {})
    shepherd.messages["rid"] = {"message": {}, "status": "Error", "description": "x"}
    shepherd.state = _row(status="ERROR")
    response = await api.arax_sync_query({})
    body = json.loads(bytes(response.body))
    assert response.status_code == 500
    assert body["logs"] == [{"shepherd": 1}]


# ---------------------------------------------------------------------------
# /response (API-07, API-08); behavior parity is in arax/test_response_parity.py
# ---------------------------------------------------------------------------


class _Validator:
    def __init__(self, **kwargs):
        pass

    def check_compliance_of_trapi_response(self, envelope):
        pass

    def get_all_messages(self):
        return {
            "Validate TRAPI Response": {"Standards Test": {"error": {}, "critical": {}}}
        }

    def dumps(self):
        return "ok"


@pytest.fixture
def arax_client(mocker):
    import httpx

    from shepherd_utils.arax.ResponseCache import response_lookup

    async def inline(fn, *args):
        return fn(*args)

    mocker.patch.object(api, "_run_in_pool", side_effect=inline)
    mocker.patch.object(response_lookup, "_TRAPIResponseValidator", _Validator)
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api.ARAX), base_url="http://testserver"
    )


@pytest.mark.asyncio
async def test_get_response_serves_a_stored_response_validated(arax_client, mocker):
    stored = {
        "message": {"knowledge_graph": {"nodes": {}, "edges": {}}},
        "x": float("nan"),
    }
    mocker.patch(
        "shepherd_utils.db.get_message_sync",
        side_effect=lambda i: stored if i == "abc" else {}[i],
    )
    async with arax_client as client:
        response = await client.get("/response/abc")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    # json.dumps, as ARAX's Flask API serializes: NaN passes through
    assert "NaN" in response.text
    body = json.loads(response.text)
    assert body["validation_result"]["status"] == "PASS"
    assert body["validation_result"]["provenance_summary"]["n_sources"] == 0


@pytest.mark.asyncio
async def test_get_response_not_found_is_arax_404(arax_client, mocker):
    mocker.patch("shepherd_utils.db.get_message_sync", side_effect=KeyError)
    async with arax_client as client:
        response = await client.get("/response/nope")
    assert response.status_code == 404
    assert response.json() == {
        "status": 404,
        "title": "Response not found",
        "detail": "There is no response corresponding to response_id=nope",
        "type": "about:blank",
    }


@pytest.mark.asyncio
async def test_post_response_stores_the_callback(arax_client, mocker):
    from shepherd_utils.arax.ResponseCache import response_lookup

    store = mocker.patch.object(response_lookup, "store_callback")
    async with arax_client as client:
        response = await client.post("/response", json={"a": 1})
    assert response.status_code == 200
    assert response.json() == "received!"
    store.assert_called_once_with({"a": 1})


@pytest.mark.asyncio
async def test_fetch_ars_reads_shepherds_own_ars(mocker):
    import datetime
    import uuid
    from unittest.mock import AsyncMock

    import shepherd_utils.ars.db as ars_db

    pk = uuid.uuid4()
    ts = datetime.datetime(2026, 9, 1, tzinfo=datetime.timezone.utc)
    row = {
        "id": pk,
        "name": "",
        "code": 200,
        "status": "D",
        "agent": "ara-shepherd-arax",
        "ref": None,
        "ts": ts,
        "updated_at": ts,
        "url": None,
        "result_count": 1,
        "result_stat": None,
        "retain": False,
        "merge_semaphore": False,
        "merged_version": None,
        "merged_versions_list": None,
        "params": {},
        "clients": [],
    }
    mocker.patch.object(
        ars_db, "get_message_row", new_callable=AsyncMock, return_value=row
    )
    mocker.patch.object(
        ars_db,
        "load_message_bytes",
        new_callable=AsyncMock,
        return_value=b'{"message": {}}',
    )
    status, content = await api._fetch_ars(str(pk), False)
    assert status == 200
    body = json.loads(content)
    assert body["pk"] == str(pk)
    assert body["fields"]["name"] == "ara-shepherd-arax"
    assert body["fields"]["data"] == {"message": {}}


def test_ars_host_is_shepherds(mocker):
    mocker.patch.object(api.settings, "server_url", "https://shepherd.example.org/")
    assert api.ars_host() == "shepherd.example.org"


# ---------------------------------------------------------------------------
# /status (API-09)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_terminate_ends_the_stream(mocker):
    shepherd = FakeShepherd(mocker, dict(PLAN, stream_progress=True))
    shepherd.progress[:] = [
        json.dumps({"timestamp": "t", "level": "INFO", "code": "", "message": "a"})
        + "\n",
        json.dumps({"pid": 4242, "authorization": "x"}) + "\n",
    ]
    response = await api.arax_stream_query(dict(PLAN, stream_progress=True))
    lines = []
    async for chunk in response.body_iterator:
        line = json.loads(chunk)
        lines.append(line)
        if "pid" in line:
            # the client asks to terminate as soon as it has the token
            assert await api.arax_status.terminate(line["pid"], "wrong") == {
                "status": "ERROR",
                "description": "Invalid authorization provided",
            }
            assert await api.arax_status.terminate(
                line["pid"], line["authorization"]
            ) == {"status": "OK", "description": f"Process {line['pid']} terminated"}
    # the stream ended there, with no final envelope
    assert [set(x) for x in lines] == [
        {"timestamp", "level", "code", "message"},
        {"pid", "authorization"},
    ]


@pytest.mark.asyncio
async def test_terminate_unknown_pid(mocker):
    import fakeredis

    mocker.patch.object(
        api.arax_status, "data_db_client", fakeredis.aioredis.FakeRedis()
    )
    pid = 99
    assert await api.arax_status.terminate(
        pid, api.arax_status.pid_authorization(pid)
    ) == {"status": "ERROR", "description": "ERROR: Attempt to terminate pid=99 failed"}


@pytest.fixture
def status_client():
    import httpx

    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api.ARAX), base_url="http://testserver"
    )


@pytest.mark.asyncio
async def test_status_recent_and_active_queries(status_client, mocker):
    import datetime

    start = datetime.datetime(2026, 9, 1, 12, 0, 0)
    rows = [
        (
            "q1",
            start,
            start + datetime.timedelta(seconds=42),
            "sub",
            "1.2.3.4",
            "arax",
            "h",
            "r1",
            None,
            "COMPLETED",
            "OK",
            "done",
        ),
        ("q2", start, None, None, None, "arax", None, "r2", None, "QUEUED", "OK", None),
    ]
    recent = mocker.patch.object(
        api.arax_status,
        "get_recent_queries",
        new_callable=mocker.AsyncMock,
        return_value=rows,
    )
    mocker.patch.object(
        api.arax_status.settings, "server_url", "https://shepherd.example.org"
    )
    async with status_client as client:
        body = (await client.get("/status", params={"last_n_hours": 5})).json()
        await client.get("/status", params={"mode": "active"})
    assert recent.await_args_list[0].args[:3] == ("arax", 5.0, False)
    assert recent.await_args_list[1].args[:3] == ("arax", 24.0, True)
    newest_first = [q["query_id"] for q in body["recent_queries"]]
    assert newest_first == ["q2", "q1"]
    q1 = body["recent_queries"][1]
    assert q1 == {
        "query_id": "q1",
        "pid": None,
        "start_datetime": "2026-09-01 12:00:00",
        "domain": "shepherd.example.org",
        "hostname": "h",
        "instance_name": api.arax_status.settings.server_location,
        "state": "Completed",
        "elapsed": 42,
        "submitter": "sub",
        "response_id": "r1",
        "status": "OK",
        "description": "done",
        "remote_address": "1.2.3.4",
    }
    assert body["recent_queries"][0]["state"] == "started"
    assert "current_datetime" in body


@pytest.mark.asyncio
async def test_status_id_returns_the_input_query(status_client, mocker):
    mocker.patch.object(
        api.arax_status,
        "get_message",
        new_callable=mocker.AsyncMock,
        return_value={"message": 1},
    )
    async with status_client as client:
        body = (await client.get("/status", params={"id": "q1"})).json()
    assert body == {"message": 1}


@pytest.mark.asyncio
async def test_status_site_config_kp_cache_and_system_load(status_client):
    async with status_client as client:
        config = (await client.get("/status", params={"mode": "site_config"})).json()
        kp_cache = (await client.get("/status", params={"mode": "kp_cache"})).json()
        load = (await client.get("/status", params={"mode": "system_load"})).json()
    assert config["config"]["arax_version"] == "1.6.2"
    assert config["config"]["cohd_database_version"] == "1.0_KG2.8.0"
    assert kp_cache["cache_stats"]["n_cached_queries"] == 0
    assert kp_cache["cache_data"] == []
    assert kp_cache["column_data"][0]["key"] == "kp_query_id"
    assert load == []


@pytest.mark.asyncio
async def test_status_recent_pks_reads_shepherds_ars(status_client, mocker):
    import shepherd_utils.arax.NodeSynonymizer.node_synonymizer as ns

    pk = "0b6a7f2c-7c3e-4ab0-9a55-1d2c3e4f5a6b"
    latest = {"latest_3_pks": [pk]}
    message = {"pk": pk, "fields": {"name": "ars-default-agent", "data": None}}
    trace = {
        "message": pk,
        "status": "Done",
        "timestamp": "2026-09-01 12:00:00",
        "query_graph": {
            "nodes": {"n0": {"ids": ["MONDO:1"]}, "n1": {}},
            "edges": {"e0": {"predicates": ["biolink:treats"]}},
        },
        "children": [
            {
                "actor": {"agent": "ara-shepherd-arax"},
                "code": 200,
                "status": "Done",
                "result_count": 4,
            },
            {"actor": {"agent": "ars-ars-agent"}, "code": 200, "status": "Done"},
        ],
    }

    async def latest_pks(n):
        assert n == 3
        return 200, json.dumps(latest).encode()

    async def fetch_ars(key, trace_):
        return 200, json.dumps(trace if trace_ else message).encode()

    mocker.patch.object(api, "_fetch_latest_pks", side_effect=latest_pks)
    mocker.patch.object(api, "_fetch_ars", side_effect=fetch_ars)
    mocker.patch.object(
        ns.NodeSynonymizer,
        "get_normalizer_results",
        lambda self, entities=None, **kw: {entities: {"id": {"name": "a disease"}}},
    )
    mocker.patch.object(ns.NodeSynonymizer, "__init__", lambda self, *a, **k: None)
    mocker.patch.object(api.settings, "server_url", "https://shepherd.example.org")
    async with status_client as client:
        body = (
            await client.get(
                "/status",
                params={
                    "mode": "recent_pks",
                    "last_n_hours": 3,
                    "authorization": "ars.ci.transltr.io",
                },
            )
        ).json()
    assert body == {
        "agents_list": ["shepherd-arax"],
        "pks": {
            pk: {
                "agents": {"shepherd-arax": {"status": "Done", "n_results": 4}},
                "status": "Done",
                "timestamp": "2026-09-01 12:00:00",
                "query": "___ treats a disease",
                "ars_host": "shepherd.example.org",
            }
        },
        "sorted_pk_list": [pk],
    }


# ---------------------------------------------------------------------------
# /entity, /meta_knowledge_graph, /rtxcomplete/nodeslike; behavior parity for
# the last two is in arax/test_aux_parity.py
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_entity_get_and_post_go_to_the_synonymizer(status_client, mocker):
    calls = []

    def fake(entities):
        calls.append(entities)
        return {"MONDO:1": {"id": {"identifier": "MONDO:1"}}}

    mocker.patch.object(api, "_normalizer_results", side_effect=fake)
    async with status_client as client:
        got = await client.get("/entity", params=[("q", "MONDO:1"), ("q", "asthma")])
        posted = await client.post("/entity", json=["MONDO:1"])
    assert got.json() == posted.json() == {"MONDO:1": {"id": {"identifier": "MONDO:1"}}}
    assert calls == [["MONDO:1", "asthma"], ["MONDO:1"]]


@pytest.mark.asyncio
async def test_meta_knowledge_graph_formats(status_client, mocker):
    import shepherd_utils.arax.KnowledgeSources.knowledge_source_metadata as ksm

    base = {
        "nodes": {"biolink:Gene": {"id_prefixes": ["NCBIGene"]}},
        "edges": [
            {
                "subject": "biolink:Gene",
                "predicate": "biolink:interacts_with",
                "object": "biolink:Gene",
            }
        ],
    }
    mocker.patch.object(
        ksm.KnowledgeSourceMetadata,
        "_fetch_retriever_meta_kg",
        lambda self: json.loads(json.dumps(base)),
    )
    mocker.patch.object(
        ksm.KnowledgeSourceMetadata, "cached_meta_knowledge_graph", None
    )
    mocker.patch.object(ksm.KnowledgeSourceMetadata, "cache_timestamp", None)
    mocker.patch.object(
        ksm.KnowledgeSourceMetadata, "_save_backup_meta_kg", lambda self, kg: True
    )
    async with status_client as client:
        simple = (
            await client.get("/meta_knowledge_graph", params={"format": "simple"})
        ).json()
        full = (await client.get("/meta_knowledge_graph")).json()
    assert simple == {
        "predicates_by_categories": {
            "biolink:Gene": {"biolink:Gene": ["biolink:interacts_with"]}
        },
        "supported_predicates": ["biolink:interacts_with"],
    }
    assert full["edges"][0]["knowledge_types"] == ["lookup"]
    assert [a["attribute_type_id"] for a in full["edges"][0]["attributes"]] == [
        "biolink:original_predicate",
        "biolink:knowledge_level",
        "biolink:agent_type",
    ]


def test_retriever_meta_kg_url(mocker):
    import shepherd_utils.arax.KnowledgeSources.knowledge_source_metadata as ksm

    mocker.patch.object(
        ksm.settings, "sync_kg_retrieval_url", "http://retriever.test/query"
    )
    assert ksm._retriever_meta_kg_url() == "http://retriever.test/meta_knowledge_graph"


@pytest.mark.asyncio
async def test_nodeslike_is_jsonp(status_client, mocker, tmp_path):
    import sqlite3

    from shepherd_utils.arax.autocomplete import rtxcomplete

    db = tmp_path / "autocomplete.sqlite"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE terms(term VARCHAR(255))")
    con.executemany("INSERT INTO terms VALUES (?)", [("asthma",), ("astrocytoma",)])
    con.commit()
    con.close()
    mocker.patch.object(type(rtxcomplete.RTXConfig), "autocomplete_path", str(db))
    mocker.patch.object(api.settings, "arax_dbs_dir", str(tmp_path))
    mocker.patch.object(api, "_autocomplete_loaded", False)
    async with status_client as client:
        ok = await client.get(
            "/rtxcomplete/nodeslike",
            params={"word": "ast", "limit": 15, "callback": "cb123();alert(1)"},
        )
        missing = await client.get("/rtxcomplete/nodeslike", params={"word": "ast"})
    assert (
        ok.text
        == 'cb123([{"curie": "??", "name": "asthma", "type": "??"}, {"curie": "??", "name": "astrocytoma", "type": "??"}]);'
    )
    assert ok.headers["content-type"].startswith("text/html")
    assert missing.text == "error"


@pytest.mark.asyncio
async def test_nodeslike_without_the_database_does_not_create_it(
    status_client, mocker, tmp_path
):
    from shepherd_utils.arax.autocomplete import rtxcomplete

    db = tmp_path / "missing.sqlite"
    mocker.patch.object(type(rtxcomplete.RTXConfig), "autocomplete_path", str(db))
    mocker.patch.object(api, "_autocomplete_loaded", False)
    async with status_client as client:
        response = await client.get(
            "/rtxcomplete/nodeslike",
            params={"word": "ast", "limit": 5, "callback": "cb"},
        )
    assert response.text == "error"
    assert not db.exists()


def test_sanitize_callback():
    assert api.sanitize_callback(None) == "autocomplete_callback"
    assert api.sanitize_callback("jQuery_123(x)") == "jQuery_123"
    assert api.sanitize_callback("(evil)") == "autocomplete_callback"
