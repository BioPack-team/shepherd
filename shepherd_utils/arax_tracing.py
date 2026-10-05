"""OpenTelemetry spans for the in-process ARAX port (``shepherd_utils/arax/``).

The port stays a line-for-line copy of upstream ARAX (see its README), so it
carries no tracing code of its own. ``instrument_arax`` adds the spans from the
outside instead, the way OpenTelemetry's own auto-instrumentation libraries do:
it wraps a handful of the library's entry points, once per process. The
wrappers only observe; whatever the wrapped call returns or raises passes
through unchanged, and a failure in the span bookkeeping is swallowed rather
than allowed to fail a query.

The trace a query gets, under the worker's ``arax.query`` span::

    arax.interpret_query_graph          query graph -> ARAXi plan
    arax.expand                         one per expand action
      arax.expand.kp                    one per (qedge, KP) Expand queried
        arax.kp.request                 cache lookup or POST to the KP
          POST                          aiohttp client span (KP call)
    arax.overlay.<action>               e.g. arax.overlay.compute_ngd
    arax.filter_kg.<action>
    arax.resultify
    arax.rank                           ARAX's ranker
    arax.filter_results.<action>
    arax.infer / arax.connect
    arax.transform_results              ResultTransformer

Each action span records its ARAXi parameters (``arax.param.*``), the
knowledge graph and result counts before and after it ran, ARAX's own log
lines at INFO and above as span events, and ARAX's status: a step that put the
response in an error state is marked ERROR, as is a KP query that errored or
timed out, so the failing step stands out in the trace.
"""

import functools
import inspect
from typing import Any, Callable, Optional

from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

# Marks a wrapped attribute so instrumenting twice is a no-op
_WRAPPED = "_shepherd_otel_wrapped"
# Set on an ARAXQuery by query_return_stream, read by the query thread it starts
_STREAM_CONTEXT = "_shepherd_otel_context"
# ARAX logs a lot (Expand alone logs a few lines per KP); cap the events a span
# carries. The per-level counts are always complete.
MAX_LOG_EVENTS = 100
# Longest string attribute value recorded (parameters, log lines, errors)
MAX_ATTRIBUTE_LENGTH = 1024
# ARAX's log levels recorded as span events; DEBUG is left out
_EVENT_LEVELS = {"INFO", "WARNING", "ERROR"}
# Expand's query plan statuses that mean the KP gave no usable answer
_KP_FAILED_STATUSES = {"Error", "Timed out"}

_instrumented = False


def _tracer():
    # Looked up on each span: a pool child installs its provider after import
    return trace.get_tracer("shepherd.arax")


def _truncate(value: str) -> str:
    if len(value) <= MAX_ATTRIBUTE_LENGTH:
        return value
    return value[: MAX_ATTRIBUTE_LENGTH - 3] + "..."


def _attribute_value(value: Any):
    """``value`` as an OpenTelemetry attribute value, or None to skip it."""
    if value is None:
        return None
    if isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return _truncate(value)
    if isinstance(value, (list, tuple, set)):
        items = [v for v in value if isinstance(v, (str, bool, int, float))]
        if len(items) == len(value):
            return [_truncate(v) if isinstance(v, str) else v for v in items]
    return _truncate(str(value))


def _set_parameters(span, parameters: Any) -> None:
    if not isinstance(parameters, dict):
        return
    for key, value in parameters.items():
        if not key:
            continue  # ARAXi's "resultify()" parses to {"": "true"}
        value = _attribute_value(value)
        if value is not None:
            span.set_attribute(f"arax.param.{key}", value)


def _message(response):
    try:
        return response.envelope.message
    except AttributeError:
        return None


def _size(container) -> Optional[int]:
    try:
        return len(container)
    except TypeError:
        return None


def _counts(response) -> dict:
    """The response's knowledge graph and result counts."""
    counts = {}
    message = _message(response)
    if message is None:
        return counts
    kg = getattr(message, "knowledge_graph", None)
    if kg is not None:
        nodes = _size(getattr(kg, "nodes", None))
        edges = _size(getattr(kg, "edges", None))
        if nodes is not None:
            counts["kg.nodes"] = nodes
        if edges is not None:
            counts["kg.edges"] = edges
    results = _size(getattr(message, "results", None))
    if results is not None:
        counts["results"] = results
    return counts


def _n_messages(response) -> int:
    return _size(getattr(response, "messages", None)) or 0


def _record_logs(span, response, start: int) -> None:
    """ARAX's log lines from ``start`` on, as span events and level counts."""
    messages = getattr(response, "messages", None) or []
    counts: dict = {}
    events = 0
    for entry in messages[start:]:
        if not isinstance(entry, dict):
            continue
        level = str(entry.get("level") or "")
        counts[level] = counts.get(level, 0) + 1
        if level not in _EVENT_LEVELS or events >= MAX_LOG_EVENTS:
            continue
        attributes = {
            "arax.log.level": level,
            "arax.log.message": _truncate(str(entry.get("message") or "")),
        }
        if entry.get("code"):
            attributes["arax.log.code"] = str(entry["code"])
        span.add_event(f"arax.log.{level.lower()}", attributes)
        events += 1
    for level, count in counts.items():
        span.set_attribute(f"arax.log.{level.lower()}_count", count)


def _record_status(span, response) -> None:
    """ARAX's response status; an error state marks the span ERROR."""
    status = getattr(response, "status", None)
    if status is None:
        return
    span.set_attribute("arax.response.status", str(status))
    if status != "OK":
        error_code = str(getattr(response, "error_code", ""))
        span.set_attribute("arax.response.error_code", error_code)
        span.set_status(
            Status(
                StatusCode.ERROR,
                _truncate(f"{error_code}: {getattr(response, 'message', '')}"),
            )
        )


def _quietly(fn: Callable, *args) -> None:
    """Run span bookkeeping; it must never fail the query it observes."""
    try:
        fn(*args)
    except Exception:  # pragma: no cover - defensive
        pass


def _arg(args, kwargs, index: int, name: str):
    if len(args) > index:
        return args[index]
    return kwargs.get(name)


def _step_wrapper(
    span_name: str,
    response_index: int,
    parameters_index: Optional[int] = None,
    after: Optional[Callable] = None,
):
    """Wrap a sync ARAX step that takes an ARAXResponse (and parameters).

    ``response_index`` / ``parameters_index`` locate them in the positional
    arguments (``self`` included), which is how ARAX_query calls every step.
    The span is named ``span_name``, plus ``.<action>`` when the parameters
    name an action (overlay, filter_kg and filter_results do).
    """

    def factory(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            response = _arg(args, kwargs, response_index, "response")
            parameters = (
                _arg(args, kwargs, parameters_index, "input_parameters")
                if parameters_index is not None
                else None
            )
            name = span_name
            if isinstance(parameters, dict) and isinstance(
                parameters.get("action"), str
            ):
                name = f"{span_name}.{parameters['action']}"
            with _tracer().start_as_current_span(name) as span:
                start = _n_messages(response)
                before = {}

                def _before():
                    _set_parameters(span, parameters)
                    before.update(_counts(response))
                    for key, value in before.items():
                        span.set_attribute(f"arax.{key}.before", value)

                _quietly(_before)
                try:
                    return fn(*args, **kwargs)
                finally:

                    def _after():
                        for key, value in _counts(response).items():
                            span.set_attribute(f"arax.{key}.after", value)
                            if key in before:
                                span.set_attribute(
                                    f"arax.{key}.delta", value - before[key]
                                )
                        _record_logs(span, response, start)
                        _record_status(span, response)
                        if after is not None:
                            after(span, response)

                    _quietly(_after)

        return wrapper

    return factory


def _record_araxi(span, response) -> None:
    data = getattr(response, "data", None) or {}
    commands = data.get("araxi_commands") if isinstance(data, dict) else None
    if isinstance(commands, list):
        span.set_attribute("arax.araxi.n_commands", len(commands))
        span.set_attribute(
            "arax.araxi.commands", [_truncate(str(c)) for c in commands[:50]]
        )


def _expand_edge_wrapper(fn):
    """ARAXExpander.expand_edge_async: one KP answering one qedge."""

    @functools.wraps(fn)
    async def wrapper(self, edge_qg, kp_to_use, *args, **kwargs):
        with _tracer().start_as_current_span("arax.expand.kp") as span:
            qedge_key = None

            def _before():
                nonlocal qedge_key
                qedge_key = next(iter(edge_qg.edges), None)
                span.set_attribute("arax.kp", str(kp_to_use))
                if qedge_key is not None:
                    span.set_attribute("arax.qedge_key", str(qedge_key))
                    qedge = edge_qg.edges[qedge_key]
                    predicates = getattr(qedge, "predicates", None)
                    if predicates:
                        span.set_attribute(
                            "arax.qedge.predicates", [str(p) for p in predicates]
                        )
                n_ids = [
                    len(qnode.ids or []) for qnode in (edge_qg.nodes or {}).values()
                ]
                if n_ids:
                    span.set_attribute("arax.input_curies", max(n_ids))

            _quietly(_before)
            answer = await fn(self, edge_qg, kp_to_use, *args, **kwargs)

            def _after():
                qg_org_kg, _, log = answer
                edges = sum(len(e) for e in qg_org_kg.edges_by_qg_id.values())
                span.set_attribute("arax.kp.edges", edges)
                span.set_attribute("arax.kp.nodes", len(qg_org_kg.get_all_node_keys()))
                plan = (
                    log.query_plan.get("qedge_keys", {})
                    .get(qedge_key, {})
                    .get(kp_to_use, {})
                )
                status = plan.get("status") if isinstance(plan, dict) else None
                if status:
                    description = str(plan.get("description") or "")
                    span.set_attribute("arax.kp.status", str(status))
                    span.set_attribute("arax.kp.description", _truncate(description))
                    if status in _KP_FAILED_STATUSES:
                        span.set_status(
                            Status(StatusCode.ERROR, _truncate(description))
                        )

            _quietly(_after)
            return answer

    return wrapper


def _kp_request_wrapper(fn):
    """KPQueryCacher.get_result: the KP cache lookup, or the POST to the KP."""

    @functools.wraps(fn)
    async def wrapper(self, query_url, query_object, kp_curie, *args, **kwargs):
        with _tracer().start_as_current_span("arax.kp.request") as span:

            def _before():
                span.set_attribute("arax.kp", str(kp_curie))
                span.set_attribute("url.full", str(query_url))
                timeout = _arg(args, kwargs, 0, "timeout")
                if timeout is not None:
                    span.set_attribute("arax.kp.timeout_sec", timeout)
                span.set_attribute(
                    "arax.kp.bypass_cache",
                    bool(_arg(args, kwargs, 1, "bypass_cache")),
                )

            _quietly(_before)
            result = await fn(self, query_url, query_object, kp_curie, *args, **kwargs)

            def _after():
                response_data, http_code, elapsed_time, error = result
                span.set_attribute("arax.kp.cache_hit", error == "from cache")
                if elapsed_time is not None:
                    span.set_attribute("arax.kp.elapsed_sec", float(elapsed_time))
                n_results = self._get_n_results(response_data)
                if n_results is not None:
                    span.set_attribute("arax.kp.results", n_results)
                if error and error != "from cache":
                    span.set_attribute("arax.kp.error", _truncate(str(error)))
                if not isinstance(http_code, int):
                    return
                span.set_attribute("http.response.status_code", http_code)
                # -1 is a timeout (live or cached); 4xx/5xx a KP error
                if http_code == -1 or http_code >= 400:
                    span.set_status(
                        Status(
                            StatusCode.ERROR,
                            "KP timed out" if http_code == -1 else f"HTTP {http_code}",
                        )
                    )

            _quietly(_after)
            return result

    return wrapper


def _stream_wrapper(fn):
    """ARAXQuery.query_return_stream: hand the caller's context to the thread.

    The stream runs the query on a thread of its own (``asynchronous_query``),
    and a new thread starts with an empty OpenTelemetry context, so the
    query's spans would each start a trace of their own. The context current
    when the stream starts is kept on the ARAXQuery for that thread.
    """

    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        setattr(self, _STREAM_CONTEXT, otel_context.get_current())
        yield from fn(self, *args, **kwargs)

    return wrapper


def _stream_thread_wrapper(fn):
    """ARAXQuery.asynchronous_query: run under the stream caller's context."""

    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        ctx = getattr(self, _STREAM_CONTEXT, None)
        token = otel_context.attach(ctx) if ctx is not None else None
        try:
            return fn(self, *args, **kwargs)
        finally:
            if token is not None:
                otel_context.detach(token)

    return wrapper


def _wrap(owner, name: str, factory: Callable) -> None:
    raw = inspect.getattr_static(owner, name)
    is_static = isinstance(raw, staticmethod)
    fn = raw.__func__ if is_static else raw
    if getattr(fn, _WRAPPED, False):
        return
    wrapped = factory(fn)
    setattr(wrapped, _WRAPPED, True)
    setattr(owner, name, staticmethod(wrapped) if is_static else wrapped)


def _instrument_http_clients() -> None:
    """Client spans (and traceparent headers) for ARAX's own HTTP calls:
    aiohttp for KP queries and the Node Normalizer / Name Resolver, requests
    for Connect and a few lookups. Skipped if the instrumentation isn't
    installed."""
    try:
        from opentelemetry.instrumentation.aiohttp_client import (
            AioHttpClientInstrumentor,
        )

        if not AioHttpClientInstrumentor().is_instrumented_by_opentelemetry:
            AioHttpClientInstrumentor().instrument()
    except ImportError:
        pass
    try:
        from opentelemetry.instrumentation.requests import RequestsInstrumentor

        if not RequestsInstrumentor().is_instrumented_by_opentelemetry:
            RequestsInstrumentor().instrument()
    except ImportError:
        pass


def instrument_arax(http_clients: bool = True) -> None:
    """Add Shepherd's spans to the ARAX port. Idempotent, once per process.

    Imports the ARAX library, so it is called where queries run (the arax
    worker's pool children), not at module import. ``http_clients`` also
    turns on the aiohttp / requests client instrumentation.
    """
    global _instrumented
    if _instrumented:
        return
    from shepherd_utils.arax.ARAX_connect import ARAXConnect
    from shepherd_utils.arax.ARAX_expander import ARAXExpander
    from shepherd_utils.arax.ARAX_filter_kg import ARAXFilterKG
    from shepherd_utils.arax.ARAX_filter_results import ARAXFilterResults
    from shepherd_utils.arax.ARAX_infer import ARAXInfer
    from shepherd_utils.arax.ARAX_overlay import ARAXOverlay
    from shepherd_utils.arax.ARAX_query import ARAXQuery
    from shepherd_utils.arax.ARAX_query_graph_interpreter import (
        ARAXQueryGraphInterpreter,
    )
    from shepherd_utils.arax.ARAX_ranker import ARAXRanker
    from shepherd_utils.arax.ARAX_resultify import ARAXResultify
    from shepherd_utils.arax.Expand.trapi_query_cacher import KPQueryCacher
    from shepherd_utils.arax.result_transformer import ResultTransformer

    # The ARAXi actions, as ARAX_query.execute_processing_plan calls them:
    # step.apply(response, parameters)
    for owner, span_name in (
        (ARAXExpander, "arax.expand"),
        (ARAXOverlay, "arax.overlay"),
        (ARAXFilterKG, "arax.filter_kg"),
        (ARAXResultify, "arax.resultify"),
        (ARAXFilterResults, "arax.filter_results"),
        (ARAXInfer, "arax.infer"),
        (ARAXConnect, "arax.connect"),
    ):
        _wrap(owner, "apply", _step_wrapper(span_name, 1, 2))
    _wrap(ARAXRanker, "aggregate_scores_dmk", _step_wrapper("arax.rank", 1))
    # A staticmethod: the response is its first argument
    _wrap(ResultTransformer, "transform", _step_wrapper("arax.transform_results", 0))
    _wrap(
        ARAXQueryGraphInterpreter,
        "translate_to_araxi",
        _step_wrapper("arax.interpret_query_graph", 1, after=_record_araxi),
    )
    _wrap(ARAXExpander, "expand_edge_async", _expand_edge_wrapper)
    _wrap(KPQueryCacher, "get_result", _kp_request_wrapper)
    _wrap(ARAXQuery, "query_return_stream", _stream_wrapper)
    _wrap(ARAXQuery, "asynchronous_query", _stream_thread_wrapper)
    if http_clients:
        _instrument_http_clients()
    _instrumented = True
