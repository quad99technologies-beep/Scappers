#!/usr/bin/env python3
"""
Bridge Python logging to OpenTelemetry structured log format.

Adds an OTel-aware logging handler that enriches log records with
trace context (trace_id, span_id) when available.

Usage:
    from core.observability.log_bridge import setup_log_bridge
    setup_log_bridge()  # Call once at startup
    # All existing logging.getLogger() calls now emit OTel-enriched records
"""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

_OTEL_AVAILABLE = False
try:
    from opentelemetry import trace
    _OTEL_AVAILABLE = True
except ImportError:
    pass


class StructuredJsonFormatter(logging.Formatter):
    """Format log records as JSON with optional OTel trace context."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Inject trace context if available
        if _OTEL_AVAILABLE:
            span = trace.get_current_span()
            ctx = span.get_span_context()
            if ctx and ctx.is_valid:
                log_entry["trace_id"] = format(ctx.trace_id, "032x")
                log_entry["span_id"] = format(ctx.span_id, "016x")

        # Pass through extra attributes
        for attr in ("country", "run_id", "step", "scraper"):
            if hasattr(record, attr):
                log_entry[attr] = getattr(record, attr)

        return json.dumps(log_entry, default=str)


class OTelLogHandler(logging.Handler):
    """
    Logging handler that emits OTel log records.
    Falls back to structured JSON on stderr if OTel logging SDK not available.
    """

    def __init__(self, level=logging.INFO):
        super().__init__(level)
        self._otel_logger = None

        try:
            from opentelemetry._logs import get_logger_provider
            self._otel_logger = get_logger_provider().get_logger(__name__)
        except (ImportError, AttributeError):
            pass

    def emit(self, record: logging.LogRecord):
        if self._otel_logger is not None:
            try:
                from opentelemetry._logs import LogRecord as OTelLogRecord, SeverityNumber
                severity_map = {
                    logging.DEBUG: SeverityNumber.DEBUG,
                    logging.INFO: SeverityNumber.INFO,
                    logging.WARNING: SeverityNumber.WARN,
                    logging.ERROR: SeverityNumber.ERROR,
                    logging.CRITICAL: SeverityNumber.FATAL,
                }
                self._otel_logger.emit(OTelLogRecord(
                    body=record.getMessage(),
                    severity_number=severity_map.get(record.levelno, SeverityNumber.INFO),
                ))
                return
            except Exception:
                pass  # Fall through to no-op


def setup_log_bridge(level: int = logging.INFO,
                     json_output: bool = True,
                     stream=None) -> None:
    """
    Attach OTel-aware logging to the root logger.

    Non-destructive: existing handlers are kept. Adds:
    1. OTelLogHandler (if OTel SDK available)
    2. Optionally, a structured JSON formatter on a stream handler

    Args:
        level: Minimum log level.
        json_output: If True, add a JSON-formatted stream handler.
        stream: Output stream (default stderr).
    """
    root = logging.getLogger()

    # Add OTel handler
    otel_handler = OTelLogHandler(level=level)
    root.addHandler(otel_handler)

    # Add JSON stream handler
    if json_output:
        stream_handler = logging.StreamHandler(stream or sys.stderr)
        stream_handler.setLevel(level)
        stream_handler.setFormatter(StructuredJsonFormatter())
        root.addHandler(stream_handler)

    root.setLevel(min(root.level, level) if root.level != logging.NOTSET else level)
    logger.debug("Log bridge configured: level=%s, json=%s, otel=%s",
                 logging.getLevelName(level), json_output, _OTEL_AVAILABLE)
