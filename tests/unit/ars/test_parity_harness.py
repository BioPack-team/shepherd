"""Unit tests for the layer-4 harness pieces that can run without Docker:
the normalizer/diff logic and the mockworld stub endpoints."""

import json
import sys
import pathlib

import httpx
import pytest
import zstandard

HARNESS = pathlib.Path(__file__).resolve().parents[2] / "parity_e2e"
sys.path.insert(0, str(HARNESS))
sys.path.insert(0, str(HARNESS / "mockworld"))

from normalize import diff, normalize, summarize_tree  # noqa: E402
import app as mockworld  # noqa: E402

# ---------------------------------------------------------------------------
# normalizer
# ---------------------------------------------------------------------------


def test_normalize_masks_volatile_values():
    obj = {
        "message": "0c04c9bb-15b7-45e9-9084-b9e2f6850963",
        "timestamp": "2026-09-01 12:00:00.123+00:00",
        "callback": "http://ars.example:8000/ars/api/messages/abc",
        "nested": [{"updated_at": "2026-09-01T12:00:00Z"}],
    }
    n = normalize(obj)
    assert n["message"] == "<uuid>"
    assert n["timestamp"] == "<ts>"
    assert n["callback"].startswith("<host>")
    assert n["nested"][0]["updated_at"] == "<ts>"


def test_diff_equal_after_masking():
    a = {"pk": "11111111-2222-3333-4444-555555555555", "status": "Done"}
    b = {"pk": "99999999-8888-7777-6666-555555555555", "status": "Done"}
    assert diff(a, b) == []


def test_diff_reports_field_level_difference():
    problems = diff({"status": "Done", "code": 200}, {"status": "Error", "code": 200})
    assert problems == ["$.status: 'Done' != 'Error'"]


def test_diff_list_order_insensitive_fallback():
    assert diff({"x": ["a", "b"]}, {"x": ["b", "a"]}) == []
    assert diff({"x": ["a", "b"]}, {"x": ["a", "c"]}) != []


def test_summarize_tree_excludes_merge_children():
    trace = {
        "status": "Done",
        "code": 200,
        "merged_version": "abc",
        "merged_versions_list": "[('m1', 'ara-aragorn')]",
        "children": [
            {
                "actor": {"agent": "ara-aragorn"},
                "status": "Done",
                "code": 200,
                "result_count": 5,
            },
            {
                "actor": {"agent": "ars-ars-agent"},
                "status": "Done",
                "code": 200,
                "result_count": 5,
            },
        ],
    }
    summary = summarize_tree(trace)
    assert summary["parent_status"] == "Done"
    assert len(summary["children"]) == 1
    assert summary["merged_present"] is True
    assert summary["merged_versions_count"] == 1


# ---------------------------------------------------------------------------
# mockworld stubs
# ---------------------------------------------------------------------------


@pytest.fixture
def world():
    transport = httpx.ASGITransport(app=mockworld.APP)
    return httpx.AsyncClient(transport=transport, base_url="http://mockworld")


async def test_mockworld_async_ara_delivers_callback(world, mocker):
    delivered = {}
    real_post = httpx.AsyncClient.post

    async def fake_post(self, url, **kwargs):
        # intercept only the mockworld's outbound callback delivery; the
        # test client's own requests go through untouched
        if str(url).startswith("http://ars.example"):
            delivered["url"] = url
            delivered["json"] = kwargs.get("json")
            return httpx.Response(200, request=httpx.Request("POST", url))
        return await real_post(self, url, **kwargs)

    mocker.patch("httpx.AsyncClient.post", fake_post)
    await world.put(
        "/scenario",
        json={
            "aras": {
                "infores:aragorn": {
                    "mode": "happy",
                    "delay_sec": 0,
                    "response": {"message": {"results": [1]}},
                }
            }
        },
    )
    r = await world.post(
        "/ara/infores:aragorn/asyncquery",
        json={"message": {}, "callback": "http://ars.example/cb"},
    )
    assert r.status_code == 200
    assert r.json()["status"] == "Accepted"
    # let the scheduled callback task run
    import asyncio

    await asyncio.sleep(0.05)
    assert delivered["url"] == "http://ars.example/cb"
    assert delivered["json"] == {"message": {"results": [1]}}


async def test_mockworld_sync_ara_and_error_modes(world):
    await world.put(
        "/scenario",
        json={
            "aras": {
                "infores:improving-agent": {"mode": "empty"},
                "infores:broken": {"mode": "error"},
                "infores:flaky": {"mode": "unavailable"},
            }
        },
    )
    r = await world.post(
        "/ara/infores:improving-agent/query", json={"message": {"query_graph": {}}}
    )
    assert r.status_code == 200
    assert r.json()["message"]["results"] == []
    assert (await world.post("/ara/infores:broken/query", json={})).status_code == 500
    assert (await world.post("/ara/infores:flaky/query", json={})).status_code == 503


async def test_mockworld_registry_resolves_armed_aras(world):
    await world.put("/scenario", json={"aras": {"infores:aragorn": {"mode": "empty"}}})
    r = await world.get("/api/query")
    hits = r.json()["hits"]
    assert len(hits) == 1
    assert hits[0]["info"]["x-translator"]["infores"] == "infores:aragorn"
    assert hits[0]["servers"][0]["url"].endswith("/ara/infores:aragorn")


async def test_mockworld_appraiser_zstd_roundtrip(world):
    await world.put("/scenario", json={"aras": {}})
    payload = {"message": {"results": [{"a": 1}, {"b": 2}]}}
    body = zstandard.compress(json.dumps(payload).encode())
    r = await world.post("/get_appraisal", content=body)
    assert r.status_code == 200
    out = json.loads(zstandard.decompress(r.content))
    assert all("ordering_components" in res for res in out["message"]["results"])
    # deterministic: same input, same components
    r2 = await world.post("/get_appraisal", content=body)
    assert r2.content == r.content


async def test_mockworld_notification_sink_journals(world):
    await world.put("/scenario", json={"aras": {}})
    await world.post(
        "/notify/ui",
        content=json.dumps({"pk": "x", "code": 200}),
        headers={"x-event-signature": "sig"},
    )
    journal = (await world.get("/journal")).json()
    notes = [e for e in journal if e["kind"] == "notification"]
    assert notes[0]["client_id"] == "ui"
    assert notes[0]["signature"] == "sig"
    assert notes[0]["payload"] == {"pk": "x", "code": 200}
