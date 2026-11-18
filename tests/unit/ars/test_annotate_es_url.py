"""settings.annotator_es_url: the real biothings_annotator package, pointed at
an arbitrary Elasticsearch base URL instead of one of its hardcoded presets.
HTTP is stubbed at httpx, so this checks where the package sends its queries
and that the hits come back as node attributes."""

import json
import logging

import httpx
import pytest

import shepherd_utils.ars.annotate as annotate_mod
from shepherd_utils.config import settings

LOGGER = logging.getLogger(__name__)

ES_URL = "https://core-components-es.ci.transltr.io"


@pytest.fixture
def es_requests(monkeypatch):
    """Record every request the package makes and answer _msearch with one
    hit per query."""
    seen = []

    async def fake_request(self, method, url, **kwargs):
        seen.append((method, str(url), kwargs))
        lines = [json.loads(line) for line in kwargs["content"].splitlines() if line]
        bodies = lines[1::2]
        responses = [
            {"hits": {"hits": [{"_id": "5468", "_score": 1.0, "_source": {"symbol": "PPARG"}}]}}
            for _ in bodies
        ]
        return httpx.Response(
            200,
            json={"responses": responses},
            request=httpx.Request(method, url),
        )

    monkeypatch.setattr(httpx.AsyncClient, "request", fake_request)
    return seen


def _message():
    return {
        "message": {
            "knowledge_graph": {
                "nodes": {"NCBIGene:5468": {"categories": ["biolink:Gene"], "attributes": []}},
                "edges": {},
            }
        }
    }


async def test_es_url_routes_queries_to_configured_host(monkeypatch, es_requests):
    monkeypatch.setattr(settings, "annotator_es_url", ES_URL)
    # the package's own env vars must not matter once the URL is set
    monkeypatch.setenv("ANNOTATOR_QUERY_BACKEND", "biothings")
    monkeypatch.setenv("ELASTICSEARCH_CONNECTION", "in_cluster")

    data = _message()
    await annotate_mod.annotate_nodes(data, "ara-test", LOGGER)

    assert es_requests, "the annotator made no requests"
    for method, url, kwargs in es_requests:
        assert url == f"{ES_URL}/gene/_msearch"
        assert "Host" not in (kwargs.get("headers") or {})

    (attribute,) = data["message"]["knowledge_graph"]["nodes"]["NCBIGene:5468"]["attributes"]
    assert attribute["attribute_type_id"] == "biothings_annotations"
    assert attribute["value"][0]["symbol"] == "PPARG"


def test_es_url_unset_leaves_package_config_alone(monkeypatch):
    monkeypatch.setattr(settings, "annotator_es_url", "")
    monkeypatch.setenv("ANNOTATOR_QUERY_BACKEND", "elasticsearch")
    monkeypatch.setenv("ELASTICSEARCH_CONNECTION", "ci_forward")

    atr = annotate_mod._new_annotator()

    assert atr.query_backend == "elasticsearch"
    assert atr.elasticsearch_connection == "ci_forward"
    assert annotate_mod._annotator_host(atr) == "http://localhost:9200"


def test_es_url_set_overrides_backend(monkeypatch):
    monkeypatch.setattr(settings, "annotator_es_url", f"  {ES_URL}  ")
    monkeypatch.setenv("ANNOTATOR_QUERY_BACKEND", "biothings")

    atr = annotate_mod._new_annotator()

    assert atr.query_backend == "elasticsearch"
    assert atr.elasticsearch_connection == annotate_mod.SHEPHERD_ES_CONNECTION
    assert annotate_mod._annotator_host(atr) == ES_URL
