#!/usr/bin/env python3
"""
OpenTelemetry trace provider and span helpers.

Usage:
    from core.observability.tracer import init_tracer, pipeline_span, traced

    init_tracer("pharma-scraper", exporter="console")

    with pipeline_span("Step 1: Scrape", country="Malaysia", run_id="abc"):
        scrape_products()

    @traced("parse_product")
    def parse_product(html):
        ...
"""

import functools
import logging
from contextlib import contextmanager
from typing import Optional

logger = logging.getLogger(__name__)

# Try importing OTel; gracefully degrade if absent
try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import (
        BatchSpanProcessor,
        ConsoleSpanExporter,
        SimpleSpanProcessor,
    )
    from opentelemetry.sdk.resources import Resource

    _OTEL_AVAILABLE = True
except ImportError:
    _OTEL_AVAILABLE = False
    logger.debug("opentelemetry not installed — tracing disabled")

_tracer: Optional[object] = None
_initialized = False


def init_tracer(service_name: str = "pharma-scraper",
                exporter: str = "console") -> Optional[object]:
    """
    Initialize the OTel trace provider.

    Args:
        service_name: Service name for spans.
        exporter: "console" (stdout), "otlp" (gRPC collector), or "none" (disabled).

    Returns:
        Tracer instance, or None if OTel unavailable.
    """
    global _tracer, _initialized

    if not _OTEL_AVAILABLE:
        logger.info("OpenTelemetry not installed — tracing disabled")
        _initialized = True
        return None

    if _initialized and _tracer is not None:
        return _tracer

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)

    if exporter == "console":
        provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
    elif exporter == "otlp":
        try:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
            provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        except ImportError:
            logger.warning("OTLP exporter not installed, falling back to console")
            provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
    elif exporter == "none":
        pass  # No exporter — spans are created but not exported
    else:
        logger.warning("Unknown exporter '%s', using console", exporter)
        provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))

    trace.set_tracer_provider(provider)
    _tracer = trace.get_tracer(service_name)
    _initialized = True
    logger.info("Tracer initialized: service=%s, exporter=%s", service_name, exporter)
    return _tracer


def get_tracer() -> Optional[object]:
    """Get the current tracer, initializing with defaults if needed."""
    global _tracer
    if not _initialized:
        init_tracer()
    return _tracer


@contextmanager
def pipeline_span(step_name: str, country: str = "", run_id: str = ""):
    """
    Context manager that creates a span for a pipeline step.

    Args:
        step_name: Name of the step (e.g. "Scrape Products").
        country: Country name attribute.
        run_id: Run ID attribute.
    """
    tracer = get_tracer()
    if tracer is None:
        yield None
        return

    with tracer.start_as_current_span(step_name) as span:
        span.set_attribute("scraper.country", country)
        span.set_attribute("scraper.run_id", run_id)
        span.set_attribute("scraper.step", step_name)
        try:
            yield span
        except Exception as exc:
            span.set_attribute("error", True)
            span.set_attribute("error.message", str(exc))
            raise


def traced(span_name: str = None):
    """
    Decorator that wraps a function in a trace span.

    Args:
        span_name: Custom span name. Defaults to function name.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            tracer = get_tracer()
            name = span_name or func.__qualname__
            if tracer is None:
                return func(*args, **kwargs)
            with tracer.start_as_current_span(name):
                return func(*args, **kwargs)
        return wrapper
    return decorator
