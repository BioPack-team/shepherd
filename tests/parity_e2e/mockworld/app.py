"""Mockworld: the shared fake universe for differential ARS parity runs.

One FastAPI app provides everything both stacks (the original Relay ARS and
Shepherd's port) talk to, so a scenario's inputs are byte-identical:

  - stub ARAs/KPs: POST /ara/{infores}/asyncquery (202-accept then POST the
    canned response back to the received callback URL after a scripted
    delay) and POST /ara/{infores}/query (synchronous)
  - a stub smart-api.info registry (GET /api/query) resolving every infores
    to this server
  - a stub Appraiser (POST /get_appraisal, zstd in/out, deterministic
    ordering_components), Annotator (POST /curie), NodeNorm
  - a notification sink (POST /notify/{client_id}) recording payloads +
    signatures
  - a journal of every inbound request per scenario, and a control API to
    arm scenarios (PUT /scenario) and read the journal (GET /journal)

Behavior per ARA is scripted by the armed scenario:
  {"aras": {"infores:aragorn": {"mode": "happy" | "empty" | "error" |
   "garbage" | "silent" | "slow", "delay_sec": 0.5, "response": {...}}}}
"""

import asyncio
import json
import time
from typing import Any, Dict

import httpx
import zstandard
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

APP = FastAPI(title="ARS parity mockworld")

STATE: Dict[str, Any] = {"scenario": {"aras": {}}, "journal": []}


def journal(kind: str, detail: Dict[str, Any]):
    STATE["journal"].append({"ts": time.time(), "kind": kind, **detail})


@APP.put("/scenario")
async def arm_scenario(request: Request):
    STATE["scenario"] = await request.json()
    STATE["journal"] = []
    return {"armed": True}


@APP.get("/journal")
async def get_journal():
    return STATE["journal"]


def _ara_conf(infores: str) -> Dict[str, Any]:
    return (STATE["scenario"].get("aras") or {}).get(infores, {"mode": "empty"})


def _empty_response(query: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "message": {
            "query_graph": (query.get("message") or {}).get("query_graph", {}),
            "knowledge_graph": {"nodes": {}, "edges": {}},
            "results": [],
            "auxiliary_graphs": {},
        }
    }


def _response_for(infores: str, query: Dict[str, Any]) -> Dict[str, Any]:
    conf = _ara_conf(infores)
    mode = conf.get("mode", "empty")
    if mode == "happy" and conf.get("response"):
        return conf["response"]
    return _empty_response(query)


async def _deliver_callback(infores: str, callback: str, payload: Dict[str, Any]):
    conf = _ara_conf(infores)
    await asyncio.sleep(float(conf.get("delay_sec", 0.2)))
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.post(callback, json=payload)
        journal(
            "callback_delivered",
            {"infores": infores, "callback": callback, "status": r.status_code},
        )
    except Exception as e:  # pragma: no cover - network noise in live runs
        journal("callback_failed", {"infores": infores, "error": str(e)})


@APP.post("/ara/{infores:path}/asyncquery")
async def ara_asyncquery(infores: str, request: Request):
    body = await request.json()
    conf = _ara_conf(infores)
    mode = conf.get("mode", "empty")
    journal(
        "ara_asyncquery",
        {"infores": infores, "mode": mode, "callback": body.get("callback")},
    )
    if mode == "error":
        return Response(content="scripted failure", status_code=500)
    if mode == "unavailable":
        return Response(content="scripted 503", status_code=503)
    if mode == "garbage":
        return Response(content="{not json", media_type="application/json")
    if mode != "silent":
        asyncio.get_running_loop().create_task(
            _deliver_callback(
                infores, body.get("callback"), _response_for(infores, body)
            )
        )
    return JSONResponse({"status": "Accepted", "description": "queued"})


@APP.post("/ara/{infores:path}/query")
async def ara_query(infores: str, request: Request):
    body = await request.json()
    conf = _ara_conf(infores)
    mode = conf.get("mode", "empty")
    journal("ara_query", {"infores": infores, "mode": mode})
    if mode == "error":
        return Response(content="scripted failure", status_code=500)
    if mode == "unavailable":
        return Response(content="scripted 503", status_code=503)
    if mode == "garbage":
        return Response(content="{not json", media_type="application/json")
    if conf.get("delay_sec"):
        await asyncio.sleep(float(conf["delay_sec"]))
    return JSONResponse(_response_for(infores, body))


@APP.get("/api/query")
async def smartapi_registry(request: Request):
    """Every infores in the armed scenario resolves to this mockworld."""
    base = str(request.base_url).rstrip("/")
    hits = []
    for infores in (STATE["scenario"].get("aras") or {}).keys():
        hits.append(
            {
                "_id": infores,
                "_meta": {"last_updated": "2026-01-01T00:00:00+00:00"},
                "info": {
                    "x-trapi": {"version": "1.5.0"},
                    "x-translator": {"infores": infores, "team": ["mock"]},
                },
                "servers": [
                    {"url": f"{base}/ara/{infores}", "x-maturity": "production"},
                    {"url": f"{base}/ara/{infores}", "x-maturity": "development"},
                    {"url": f"{base}/ara/{infores}", "x-maturity": "staging"},
                    {"url": f"{base}/ara/{infores}", "x-maturity": "testing"},
                ],
            }
        )
    return {"hits": hits}


@APP.post("/get_appraisal")
async def appraiser(request: Request):
    conf = STATE["scenario"].get("appraiser", {"mode": "happy"})
    journal("appraise", {"mode": conf.get("mode")})
    if conf.get("mode") == "error":
        return Response(content="appraiser down", status_code=500)
    raw = await request.body()
    if raw[:4] == b"\x28\xb5\x2f\xfd":
        raw = zstandard.ZstdDecompressor().decompress(raw)
    data = json.loads(raw)
    results = (data.get("message") or {}).get("results") or []
    for i, result in enumerate(results):
        # deterministic per-position components so both stacks rank alike
        result["ordering_components"] = {
            "novelty": round(0.1 + (i % 5) * 0.15, 2),
            "confidence": round(0.9 - (i % 7) * 0.1, 2),
            "clinical_evidence": round((i % 3) * 0.3, 2),
        }
    payload = zstandard.ZstdCompressor().compress(json.dumps(data).encode("utf-8"))
    return Response(content=payload, media_type="application/octet-stream")


@APP.post("/curie")
async def annotator(request: Request):
    conf = STATE["scenario"].get("annotator", {"mode": "happy"})
    journal("annotate", {"mode": conf.get("mode")})
    if conf.get("mode") == "error":
        return Response(content="annotator down", status_code=500)
    body = await request.json()
    ids = body.get("ids") or []
    return {curie: {"mock_annotation": {"curie": curie}} for curie in ids}


@APP.post("/get_normalized_nodes")
async def nodenorm(request: Request):
    body = await request.json()
    journal("nodenorm", {"count": len(body.get("curies") or [])})
    return {
        curie: {"id": {"identifier": curie, "label": curie}}
        for curie in (body.get("curies") or [])
    }


@APP.post("/notify/{client_id}")
async def notification_sink(client_id: str, request: Request):
    body = await request.body()
    journal(
        "notification",
        {
            "client_id": client_id,
            "signature": request.headers.get("x-event-signature"),
            "payload": json.loads(body),
        },
    )
    return {"ok": True}


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run(APP, host="0.0.0.0", port=8099)
