import multiprocessing

from opentelemetry import trace

from .config import settings

# enforce only one tracer provider with this instance
_TRACER_PROVIDER = None


def _is_main_process() -> bool:
    """True in the main process, False in a spawned process-pool child.

    ``current_process().name`` is set during multiprocessing's spawn ``prepare()``
    step -- before the worker module is re-imported -- so this is reliable even
    at module-import time inside a freshly spawned child.
    """
    return multiprocessing.current_process().name == "MainProcess"


def setup_tracer(service_name: str):
    """Configure the global tracer provider once and return a tracer.

    When ``otel_enabled`` is false, the provider is left uninitialized: the
    default proxy provider yields non-recording spans, so instrumented code
    runs unchanged with effectively no-op spans and nothing is exported.

    Spawned process-pool children skip setup entirely. They don't emit spans
    (the per-task span lives in the parent's ``run_task_lifecycle``), so
    re-running the OTLP/grpc init and httpx instrumentation on every child --
    which the spawn re-import of the worker module would otherwise do -- only
    adds latency and a network dependency to each child's cold start for nothing.
    """
    global _TRACER_PROVIDER
    if not settings.otel_enabled or not _is_main_process():
        return trace.get_tracer(__name__)
    if _TRACER_PROVIDER is None:
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
    return trace.get_tracer(__name__)
