import os

from opentelemetry import trace

from .config import settings

# enforce only one tracer provider with this instance
_TRACER_PROVIDER = None

# Set in the parent by ProcessPoolManager before it spawns children; spawn
# copies os.environ into the child before the worker module is re-imported,
# so this is reliable even at module-import time inside a freshly spawned
# child. (Process *name* is not a usable signal here: uvicorn --workers
# children and pool children are both plain "SpawnProcess-N".)
POOL_CHILD_ENV = "SHEPHERD_PROCESS_POOL_CHILD"


def _is_pool_child() -> bool:
    """True in a spawned process-pool child."""
    return bool(os.environ.get(POOL_CHILD_ENV))


def _init_provider(service_name: str) -> None:
    """Install the global tracer provider (OTLP/grpc exporter + httpx
    instrumentation) once per process."""
    global _TRACER_PROVIDER
    if _TRACER_PROVIDER is not None:
        return
    # Import the heavy OTLP/grpc stack lazily so a process that never sets up
    # tracing (pool children, otel-disabled deployments) never pays its
    # import cost -- and never risks a grpc cold-start stall.
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter,
    )
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    from opentelemetry.sdk.resources import SERVICE_NAME, Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    _TRACER_PROVIDER = TracerProvider(
        resource=Resource.create({SERVICE_NAME: service_name})
    )
    _TRACER_PROVIDER.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(
                endpoint=f"{settings.jaeger_host}:{settings.jaeger_port}",
            )
        )
    )
    trace.set_tracer_provider(_TRACER_PROVIDER)
    HTTPXClientInstrumentor().instrument()


def setup_tracer(service_name: str):
    """Configure the global tracer provider once and return a tracer.

    When ``otel_enabled`` is false, the provider is left uninitialized: the
    default proxy provider yields non-recording spans, so instrumented code
    runs unchanged with effectively no-op spans and nothing is exported.

    Spawned process-pool children skip setup here. Most pool functions emit
    no spans of their own (the per-task span lives in the parent's
    ``run_task_lifecycle``), so re-running the OTLP/grpc init and httpx
    instrumentation on every child -- which the spawn re-import of the
    worker module would otherwise do -- would only add latency and a network
    dependency to each child's cold start for nothing. A pool function that
    does emit spans opts in with ``setup_pool_child_tracer`` below. Server
    processes spawned by ``uvicorn --workers`` are NOT pool children: each
    one initializes its own provider here, exactly like a worker container.
    """
    if not settings.otel_enabled or _is_pool_child():
        return trace.get_tracer(__name__)
    _init_provider(service_name)
    return trace.get_tracer(__name__)


def setup_pool_child_tracer(service_name: str):
    """Tracing inside a process-pool child that emits spans of its own.

    Called at the top of a pool function (idempotent, so it costs nothing
    after the child's first task). It installs the same provider a worker
    process gets, so spans the child starts -- typically under a context
    the parent extracted from its own span and passed in as a carrier --
    record and export like any other, and the httpx client spans of any
    outbound call the child makes nest underneath. The tracer a module
    obtained at import time (before this ran) is a proxy that resolves to
    this provider, so ``tracer.start_as_current_span`` works unchanged.

    Export rides the batch processor's background thread; when the pool
    recycles the child (``max_tasks_per_child``) the SDK's atexit hook
    flushes what is still queued. A child that is killed exports nothing,
    which is also the last thing anyone would learn from it.
    """
    if not settings.otel_enabled:
        return trace.get_tracer(__name__)
    _init_provider(service_name)
    return trace.get_tracer(__name__)
