"""Tests for ``shepherd_utils.arax_tracing``: the spans the ARAX worker's
queries produce, recorded with an in-memory exporter."""

import asyncio

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import StatusCode

import shepherd_utils.arax_tracing as arax_tracing
import shepherd_utils.db as db
from shepherd_utils.arax.ARAX_query import ARAXQuery
from shepherd_utils.arax.ARAX_response import ARAXResponse
from shepherd_utils.arax.Expand.trapi_query_cacher import KPQueryCacher

# Needs no KP or NodeNorm: build a query graph, resultify it, filter the
# (empty) results, and return
PLAN = {
    "operations": {
        "actions": [
            "add_qnode(key=n0, categories=biolink:SmallMolecule)",
            "add_qnode(key=n1, categories=biolink:Disease)",
            "add_qedge(key=e0, subject=n0, object=n1)",
            "resultify()",
            "filter_results(action=limit_number_of_results, max_results=5)",
            "return(message=true, store=false)",
        ]
    }
}


@pytest.fixture
def spans(monkeypatch):
    """Instrument ARAX and record its spans; returns the exporter."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(arax_tracing, "_tracer", lambda: provider.get_tracer("test"))
    arax_tracing.instrument_arax(http_clients=False)
    # nothing is stored (store=false), but keep the store off the network
    monkeypatch.setattr(db, "save_message_sync", lambda *a: None)
    exporter.tracer = provider.get_tracer("test")
    return exporter


def _by_name(exporter):
    return {span.name: span for span in exporter.get_finished_spans()}


def test_instrumenting_twice_wraps_once(spans):
    from shepherd_utils.arax.ARAX_resultify import ARAXResultify

    apply = ARAXResultify.apply
    arax_tracing._instrumented = False
    arax_tracing.instrument_arax(http_clients=False)
    assert ARAXResultify.apply is apply


def test_each_action_gets_a_span_under_the_callers(spans):
    with spans.tracer.start_as_current_span("arax.query") as parent:
        envelope = ARAXQuery(response_id="r1").query_return_message(dict(PLAN))
    assert envelope.status == "Success"

    by_name = _by_name(spans)
    for name in (
        "arax.resultify",
        "arax.rank",
        "arax.filter_results.limit_number_of_results",
        "arax.transform_results",
    ):
        assert name in by_name, sorted(by_name)
        span = by_name[name]
        assert span.parent.span_id == parent.get_span_context().span_id
        assert span.attributes["arax.response.status"] == "OK"
        assert span.status.status_code is not StatusCode.ERROR

    filter_span = by_name["arax.filter_results.limit_number_of_results"]
    assert filter_span.attributes["arax.param.max_results"] == "5"
    assert filter_span.attributes["arax.results.before"] == 0
    assert filter_span.attributes["arax.results.after"] == 0
    assert filter_span.attributes["arax.results.delta"] == 0
    # ARAX's own log lines ride along as events (DEBUG only as a count)
    resultify = by_name["arax.resultify"]
    assert not any(k.startswith("arax.param") for k in resultify.attributes)
    assert resultify.attributes["arax.log.debug_count"] > 0
    assert [
        e.attributes["arax.log.message"]
        for e in resultify.events
        if e.name == "arax.log.warning"
    ] == ["no results returned; empty knowledge graph"]


def test_stream_spans_continue_the_callers_trace(spans):
    """The stream runs the query on its own thread; its spans still join the
    trace that was current when the stream started."""
    with spans.tracer.start_as_current_span("arax.query") as parent:
        lines = list(ARAXQuery(response_id="r2").query_return_stream(dict(PLAN)))
    assert lines

    resultify = _by_name(spans)["arax.resultify"]
    assert resultify.context.trace_id == parent.get_span_context().trace_id
    assert resultify.parent.span_id == parent.get_span_context().span_id


def test_a_step_that_errors_marks_its_span(spans):
    from shepherd_utils.arax.ARAX_filter_results import ARAXFilterResults
    from shepherd_utils.arax.ARAX_messenger import ARAXMessenger

    response = ARAXResponse()
    ARAXMessenger().create_envelope(response)
    ARAXFilterResults().apply(response, {"action": "nope"})
    assert response.status != "OK"

    span = _by_name(spans)["arax.filter_results.nope"]
    assert span.status.status_code is StatusCode.ERROR
    assert span.attributes["arax.response.error_code"] == response.error_code
    assert span.attributes["arax.log.error_count"] >= 1
    assert any(e.name == "arax.log.error" for e in span.events)


def test_an_exception_in_a_step_is_recorded_and_raised(spans, monkeypatch):
    from shepherd_utils.arax import ARAX_ranker

    def boom(self, response):
        raise RuntimeError("ranker blew up")

    # under the instrumentation's wrapper, as if the ranker itself raised
    wrapped = arax_tracing._step_wrapper("arax.rank", 1)(boom)
    monkeypatch.setattr(ARAX_ranker.ARAXRanker, "aggregate_scores_dmk", wrapped)
    with pytest.raises(RuntimeError, match="ranker blew up"):
        ARAX_ranker.ARAXRanker().aggregate_scores_dmk(ARAXResponse())

    span = _by_name(spans)["arax.rank"]
    assert span.status.status_code is StatusCode.ERROR
    assert any(e.name == "exception" for e in span.events)


def test_bookkeeping_errors_never_fail_the_step(spans):
    """A response the wrappers can't read is still passed through."""
    from shepherd_utils.arax.ARAX_overlay import ARAXOverlay

    calls = []
    wrapped = arax_tracing._step_wrapper("arax.overlay", 1, 2)(
        lambda self, response, params: calls.append(response) or "done"
    )
    assert wrapped(ARAXOverlay(), object(), {"action": 1}) == "done"
    assert len(calls) == 1


@pytest.mark.parametrize(
    "outcome, cache_hit, error",
    [
        (({"message": {"results": [1, 2]}}, 200, 0.5, "from cache"), True, False),
        ((None, -1, 30, "Timeout"), False, True),
        (({"detail": "x"}, 500, 1.0, "HTTP 500"), False, True),
    ],
)
def test_kp_request_span(spans, monkeypatch, outcome, cache_hit, error):
    async def get_cached_result(self, *args, **kwargs):
        return outcome

    wrapped = arax_tracing._kp_request_wrapper(get_cached_result)
    monkeypatch.setattr(KPQueryCacher, "get_result", wrapped)
    cacher = KPQueryCacher.__new__(KPQueryCacher)
    result = asyncio.run(
        cacher.get_result("http://kp/query", {}, "infores:kp", timeout=30)
    )
    assert result == outcome

    span = _by_name(spans)["arax.kp.request"]
    assert span.attributes["arax.kp"] == "infores:kp"
    assert span.attributes["arax.kp.timeout_sec"] == 30
    assert span.attributes["arax.kp.cache_hit"] is cache_hit
    assert span.attributes["http.response.status_code"] == outcome[1]
    assert (span.status.status_code is StatusCode.ERROR) is error
    if outcome[1] == 200:
        assert span.attributes["arax.kp.results"] == 2


def test_expand_kp_span_reports_the_kps_answer(spans):
    from types import SimpleNamespace

    from shepherd_utils.arax.ARAX_expander import ARAXExpander
    from shepherd_utils.arax.Expand.expand_utilities import (
        QGOrganizedKnowledgeGraph,
    )

    kg = QGOrganizedKnowledgeGraph()
    kg.edges_by_qg_id = {"e0": {"x": 1, "y": 2}}
    kg.nodes_by_qg_id = {"n0": {"A": 1}, "n1": {"B": 1, "C": 1}}
    log = ARAXResponse()
    log.update_query_plan("e0", "infores:kp", "Timed out", "timed out after 30s")

    async def expand_edge_async(self, edge_qg, kp_to_use, *args, **kwargs):
        return kg, None, log

    wrapped = arax_tracing._expand_edge_wrapper(expand_edge_async)
    edge_qg = SimpleNamespace(
        edges={"e0": SimpleNamespace(predicates=["biolink:treats"])},
        nodes={"n0": SimpleNamespace(ids=["A", "B"]), "n1": SimpleNamespace(ids=None)},
    )
    assert asyncio.run(wrapped(ARAXExpander(), edge_qg, "infores:kp")) == (
        kg,
        None,
        log,
    )

    span = _by_name(spans)["arax.expand.kp"]
    assert span.attributes["arax.kp"] == "infores:kp"
    assert span.attributes["arax.qedge_key"] == "e0"
    assert span.attributes["arax.qedge.predicates"] == ("biolink:treats",)
    assert span.attributes["arax.input_curies"] == 2
    assert span.attributes["arax.kp.edges"] == 2
    assert span.attributes["arax.kp.nodes"] == 3
    assert span.attributes["arax.kp.status"] == "Timed out"
    assert span.status.status_code is StatusCode.ERROR
