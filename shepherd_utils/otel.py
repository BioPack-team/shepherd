from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

from .config import settings


def setup_tracer(service_name: str):
    provider = TracerProvider(resource=Resource.create({SERVICE_NAME: service_name}))
    trace.set_tracer_provider(provider)
    span_processor = BatchSpanProcessor(ConsoleSpanExporter())
    span_processor = BatchSpanProcessor(
        OTLPSpanExporter(
            endpoint=f"{settings.jaeger_host}:{settings.jaeger_port}",
        )
    )
    provider.add_span_processor(span_processor)
    HTTPXClientInstrumentor().instrument()
    return trace.get_tracer(__name__)
