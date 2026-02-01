#!/usr/bin/env python3
"""
OpenTelemetry metrics: counters and histograms for scraper platform.

Usage:
    from core.observability.metrics import init_metrics, record_request, record_item

    init_metrics("pharma-scraper")
    record_request(country="Malaysia", status_code=200, duration_ms=350.5)
    record_item(country="Malaysia", table="products")
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from opentelemetry import metrics
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import (
        ConsoleMetricExporter,
        PeriodicExportingMetricReader,
    )
    from opentelemetry.sdk.resources import Resource

    _OTEL_AVAILABLE = True
except ImportError:
    _OTEL_AVAILABLE = False
    logger.debug("opentelemetry metrics not installed — metrics disabled")

_meter = None
_request_counter = None
_item_counter = None
_error_counter = None
_request_duration = None
_initialized = False


def init_metrics(service_name: str = "pharma-scraper",
                 exporter: str = "console",
                 export_interval_ms: int = 30000) -> None:
    """
    Initialize OTel meter provider and instruments.

    Args:
        service_name: Service name.
        exporter: "console" | "otlp" | "none".
        export_interval_ms: How often to export metrics.
    """
    global _meter, _request_counter, _item_counter, _error_counter
    global _request_duration, _initialized

    if not _OTEL_AVAILABLE:
        _initialized = True
        return

    if _initialized:
        return

    resource = Resource.create({"service.name": service_name})

    if exporter == "console":
        reader = PeriodicExportingMetricReader(
            ConsoleMetricExporter(),
            export_interval_millis=export_interval_ms,
        )
    elif exporter == "otlp":
        try:
            from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
            reader = PeriodicExportingMetricReader(
                OTLPMetricExporter(),
                export_interval_millis=export_interval_ms,
            )
        except ImportError:
            logger.warning("OTLP metric exporter not installed, using console")
            reader = PeriodicExportingMetricReader(
                ConsoleMetricExporter(),
                export_interval_millis=export_interval_ms,
            )
    else:
        _initialized = True
        return

    provider = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(provider)
    _meter = metrics.get_meter(service_name)

    _request_counter = _meter.create_counter(
        "scraper.http.requests",
        description="Total HTTP requests made",
        unit="1",
    )
    _item_counter = _meter.create_counter(
        "scraper.items.scraped",
        description="Total items scraped",
        unit="1",
    )
    _error_counter = _meter.create_counter(
        "scraper.errors",
        description="Total scraping errors",
        unit="1",
    )
    _request_duration = _meter.create_histogram(
        "scraper.http.duration",
        description="HTTP request duration",
        unit="ms",
    )

    _initialized = True
    logger.info("Metrics initialized: service=%s, exporter=%s", service_name, exporter)


def record_request(country: str, status_code: int = 0,
                   duration_ms: float = 0.0, method: str = "GET") -> None:
    """Record an HTTP request metric."""
    if _request_counter is not None:
        attrs = {"country": country, "status_code": str(status_code), "method": method}
        _request_counter.add(1, attrs)
    if _request_duration is not None and duration_ms > 0:
        _request_duration.record(duration_ms, {"country": country})


def record_item(country: str, table: str = "scraped_items", count: int = 1) -> None:
    """Record scraped item metric."""
    if _item_counter is not None:
        _item_counter.add(count, {"country": country, "table": table})


def record_error(country: str, error_type: str = "unknown", count: int = 1) -> None:
    """Record an error metric."""
    if _error_counter is not None:
        _error_counter.add(count, {"country": country, "error_type": error_type})
