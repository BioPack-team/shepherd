from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from .config import settings

# enforce only one tracer provider with this instance
_TRACER_PROVIDER: TracerProvider | None = None


def setup_tracer(service_name: str):
    """Configure the global tracer provider once and return a tracer.

    When ``otel_enabled`` is false, the provider is left uninitialized: the
    default proxy provider yields non-recording spans, so instrumented code
    runs unchanged with effectively no-op spans and nothing is exported.
    """
    global _TRACER_PROVIDER
    if not settings.otel_enabled:
        return trace.get_tracer(__name__)
    if _TRACER_PROVIDER is None:
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
