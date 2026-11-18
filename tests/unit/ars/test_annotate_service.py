"""settings.annotator_url: annotation sent to a deployed annotator service
(POST /curie) instead of the in-process package. HTTP is stubbed at httpx,
so this checks the request Shepherd sends and how the reply is consumed --
the same consumption loop as the in-process path."""

import json
import logging
from unittest.mock import AsyncMock

import httpx
import pytest

import shepherd_utils.ars.annotate as annotate_mod
from shepherd_utils.config import settings

LOGGER = logging.getLogger(__name__)

SERVICE_URL = "https://annotator.ci.transltr.io"

# what POST /curie returns: annotate_curie_list's dict, as JSON
SERVICE_REPLY = {
    "NCBIGene:5468": [{"_id": "5468", "_score": 1.0, "symbol": "PPARG", "query": "5468"}],
    "MONDO:0005148": [{"notfound": True, "query": "MONDO:0005148"}],
}


def _message():
    return {
        "message": {
            "knowledge_graph": {
                "nodes": {
                    "NCBIGene:5468": {"categories": ["biolink:Gene"], "attributes": []},
                    "MONDO:0005148": {"categories": ["biolink:Disease"], "attributes": []},
                },
                "edges": {},
            }
        }
    }


@pytest.fixture
def service(monkeypatch):
    """Stub the service: record each POST and answer with ``reply``."""
    calls = []
    state = {"status": 200, "reply": SERVICE_REPLY}

    async def fake_post(self, url, **kwargs):
        calls.append((str(url), kwargs))
        request = httpx.Request("POST", url, params=kwargs.get("params"))
        body = state["reply"]
        if isinstance(body, str):
            return httpx.Response(state["status"], text=body, request=request)
        return httpx.Response(state["status"], json=body, request=request)

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)
    monkeypatch.setattr(settings, "annotator_url", SERVICE_URL + "/")
    # the in-process package must not run once the URL is set
    monkeypatch.setattr(
        annotate_mod.annotator,
        "Annotator",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("ran in-process")),
    )
    return calls, state


async def test_service_request_and_annotations(service):
    calls, _ = service
    data = _message()

    await annotate_mod.annotate_nodes(data, "ara-test", LOGGER)

    (url, kwargs), = calls
    assert url == f"{SERVICE_URL}/curie"
    # only query_backend: the service would read raw=false as truthy
    assert kwargs["params"] == {"query_backend": "elasticsearch"}
    assert sorted(kwargs["json"]["ids"]) == ["MONDO:0005148", "NCBIGene:5468"]

    nodes = data["message"]["knowledge_graph"]["nodes"]
    (attribute,) = nodes["NCBIGene:5468"]["attributes"]
    assert attribute == {
        "attribute_type_id": "biothings_annotations",
        "value": SERVICE_REPLY["NCBIGene:5468"],
    }
    # notfound is skipped, as in-process
    assert nodes["MONDO:0005148"]["attributes"] == []


async def test_service_error_surfaces_body(service):
    _, state = service
    state["status"] = 400
    state["reply"] = json.dumps({"message": "Unknown exception occured", "exception": "boom"})

    with pytest.raises(RuntimeError, match=r"annotator service 400 .*boom"):
        await annotate_mod.annotate_nodes(_message(), "ara-test", LOGGER)


async def test_unset_url_runs_in_process(monkeypatch):
    monkeypatch.setattr(settings, "annotator_url", "")
    inst = AsyncMock()
    inst.query_backend = "elasticsearch"
    inst.api_host = "https://biothings.ci.transltr.io"
    inst.annotate_curie_list = AsyncMock(return_value=SERVICE_REPLY)
    monkeypatch.setattr(annotate_mod.annotator, "Annotator", lambda: inst)
    monkeypatch.setattr(
        httpx.AsyncClient,
        "post",
        AsyncMock(side_effect=AssertionError("called the service")),
    )

    data = _message()
    await annotate_mod.annotate_nodes(data, "ara-test", LOGGER)

    inst.annotate_curie_list.assert_awaited_once()
    assert data["message"]["knowledge_graph"]["nodes"]["NCBIGene:5468"]["attributes"]
