# OpenTelemetry observability layer
"""
Provides tracing, metrics, and structured log bridging for the scraper platform.

- tracer: Trace provider, span helpers, @traced decorator
- metrics: Request/item/error counters and histograms
- log_bridge: Bridge Python logging → OTel structured logs

All modules gracefully degrade if opentelemetry is not installed.
"""

__all__ = ["tracer", "metrics", "log_bridge"]
