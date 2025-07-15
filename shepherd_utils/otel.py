from opentelemetry import trace
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.export import ConsoleSpanExporter
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor


def setup_tracer(service_name: str):
    provider = TracerProvider(
        resource=Resource.create({SERVICE_NAME: service_name})
    )
    trace.set_tracer_provider(provider)
    span_processor = BatchSpanProcessor(ConsoleSpanExporter())
    # span_processor = BatchSpanProcessor(
    #     JaegerExporter(
    #         agent_host_name="translator-otel.apps.renci.org",
    #         agent_port=6831,
    #     )
    # )
    span_processor = BatchSpanProcessor(
        OTLPSpanExporter(
            endpoint="http://jaeger:4317",
        )
    )
    provider.add_span_processor(span_processor)
    HTTPXClientInstrumentor().instrument()
    return trace.get_tracer(__name__)
