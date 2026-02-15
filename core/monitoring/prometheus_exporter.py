#!/usr/bin/env python3
"""
Prometheus Metrics Exporter

Exports OpenTelemetry metrics to Prometheus format for scraping.
"""

import logging
import threading
from typing import Optional
from http.server import HTTPServer, BaseHTTPRequestHandler

logger = logging.getLogger(__name__)

try:
    from prometheus_client import start_http_server, Counter, Histogram, Gauge, generate_latest, REGISTRY
    from prometheus_client.core import CollectorRegistry
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False
    logger.warning("prometheus_client not installed - Prometheus metrics disabled")


# Prometheus metrics
_metrics_initialized = False
_scraper_runs_total = None
_scraper_duration_seconds = None
_items_scraped_total = None
_active_scrapers = None
_data_quality_score = None
_step_duration_seconds = None
_http_requests_total = None
_errors_total = None


def init_prometheus_metrics(port: int = 9090):
    """Initialize Prometheus metrics and start HTTP server."""
    global _metrics_initialized, _scraper_runs_total, _scraper_duration_seconds
    global _items_scraped_total, _active_scrapers, _data_quality_score
    global _step_duration_seconds, _http_requests_total, _errors_total
    
    if not _PROMETHEUS_AVAILABLE:
        logger.warning("Prometheus client not available - metrics disabled")
        return False
    
    if _metrics_initialized:
        return True
    
    try:
        # Start Prometheus HTTP server
        start_http_server(port)
        logger.info(f"Prometheus metrics server started on port {port}")
        
        # Define metrics
        _scraper_runs_total = Counter(
            'scraper_runs_total',
            'Total scraper runs',
            ['country', 'status']
        )
        
        _scraper_duration_seconds = Histogram(
            'scraper_duration_seconds',
            'Scraper run duration in seconds',
            ['country'],
            buckets=[10, 30, 60, 120, 300, 600, 1800, 3600]
        )
        
        _items_scraped_total = Counter(
            'items_scraped_total',
            'Total items scraped',
            ['country', 'source']
        )
        
        _active_scrapers = Gauge(
            'active_scrapers',
            'Currently running scrapers',
            ['country']
        )
        
        _data_quality_score = Gauge(
            'data_quality_score',
            'Data quality score (0-100)',
            ['country']
        )
        
        _step_duration_seconds = Histogram(
            'step_duration_seconds',
            'Pipeline step duration in seconds',
            ['country', 'step_name'],
            buckets=[1, 5, 10, 30, 60, 120, 300, 600]
        )
        
        _http_requests_total = Counter(
            'http_requests_total',
            'Total HTTP requests',
            ['country', 'status_code']
        )
        
        _errors_total = Counter(
            'scraper_errors_total',
            'Total errors',
            ['country', 'error_type']
        )
        
        _metrics_initialized = True
        logger.info("Prometheus metrics initialized")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize Prometheus metrics: {e}")
        return False


def record_scraper_run(country: str, status: str):
    """Record a scraper run."""
    if _metrics_initialized and _scraper_runs_total:
        _scraper_runs_total.labels(country=country, status=status).inc()


def record_scraper_duration(country: str, duration_seconds: float):
    """Record scraper run duration."""
    if _metrics_initialized and _scraper_duration_seconds:
        _scraper_duration_seconds.labels(country=country).observe(duration_seconds)


def record_items_scraped(country: str, source: str, count: int = 1):
    """Record scraped items."""
    if _metrics_initialized and _items_scraped_total:
        _items_scraped_total.labels(country=country, source=source).inc(count)


def set_active_scrapers(country: str, count: int):
    """Set active scraper count."""
    if _metrics_initialized and _active_scrapers:
        _active_scrapers.labels(country=country).set(count)


def set_data_quality_score(country: str, score: float):
    """Set data quality score."""
    if _metrics_initialized and _data_quality_score:
        _data_quality_score.labels(country=country).set(score)


def record_step_duration(country: str, step_name: str, duration_seconds: float):
    """Record step duration."""
    if _metrics_initialized and _step_duration_seconds:
        _step_duration_seconds.labels(country=country, step_name=step_name).observe(duration_seconds)


def record_http_request(country: str, status_code: int):
    """Record HTTP request."""
    if _metrics_initialized and _http_requests_total:
        _http_requests_total.labels(country=country, status_code=str(status_code)).inc()


def record_error(country: str, error_type: str):
    """Record error."""
    if _metrics_initialized and _errors_total:
        _errors_total.labels(country=country, error_type=error_type).inc()


def get_metrics_port() -> int:
    """Get the Prometheus metrics port."""
    return 9090
