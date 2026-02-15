#!/usr/bin/env python3
"""
Unified Base Scraper Class.

Enforces Enterprise Standards:
1. Config Loading (ConfigManager)
2. Standardized Logging (Logger)
3. Proxy Management (ProxyPool)
4. Metrics & Observability (Prometheus)
5. Graceful Shutdown & Lifecycle Management
6. Database Connection (PostgresDB)
"""

import abc
import time
import signal
import sys
import logging
from typing import Optional, Any

from core.config.config_manager import ConfigManager
from core.utils.logger import get_logger
from core.network.proxy_pool import get_proxy_for_scraper, Proxy, ProxyPool
from core.db.connection import CountryDB
from core.observability.metrics import init_metrics, record_item, record_error, record_request

class BaseScraper(abc.ABC):
    """
    Abstract base class for all scrapers in the repo.
    """

    def __init__(self, scraper_name: str, run_id: Optional[str] = None):
        """
        Initialize the base scraper components.
        
        Args:
            scraper_name: Unique identifier for the scraper (e.g., "Malaysia", "Argentina")
            run_id: Optional run identifier. If None, one will be generated.
        """
        self.scraper_name = scraper_name
        
        # 1. Config Loading
        # Ensures envs are loaded from config/platform.env and config/{scraper_name}.env
        self.config = ConfigManager.load_env(scraper_name)
        
        # 2. Run ID
        from core.db.models import generate_run_id
        self.run_id = run_id or generate_run_id()
        
        # 3. Logger
        # Automatically sets up standardized format
        self.logger = get_logger(self.__class__.__name__, scraper_name=scraper_name)
        
        # 4. Metrics
        # Initialize OpenTelemetry/Prometheus metrics (safe to call multiple times)
        init_metrics(service_name=f"scraper-{scraper_name.lower()}")
        
        # 5. Database
        # Connects to PostgreSQL using unified schema
        self.db = CountryDB(scraper_name)
        self.db.connect()  # Explicit connect
        
        # 6. Proxy Management
        self.proxy_pool = ProxyPool()  # Initialize pool connection
        self.current_proxy: Optional[Proxy] = None
        
        # 7. Lifecycle State
        self._shutdown_requested = False
        self._setup_signal_handlers()
        
        self.logger.info(f"Initialized {scraper_name} scraper (RunID: {self.run_id})")

    @abc.abstractmethod
    def run(self):
        """
        Main execution logic. Must be implemented by subclasses.
        Should handle its own loop and check self._shutdown_requested.
        """
        pass

    def get_proxy(self, target_url: Optional[str] = None) -> Optional[Proxy]:
        """
        Get a proxy suitable for this scraper's country.
        Automatically logs selection.
        """
        self.current_proxy = get_proxy_for_scraper(self.scraper_name, target_url)
        if self.current_proxy:
            self.logger.info(f"Selected proxy: {self.current_proxy.id} ({self.current_proxy.country_code})")
        else:
            self.logger.warning("No suitable proxy found in pool, running direct/local")
        return self.current_proxy

    def report_proxy_result(self, success: bool, response_time_ms: float = 0, error_type: str = "unknown"):
        """
        Report proxy usage result to the pool for health tracking.
        """
        if self.current_proxy:
            if success:
                self.proxy_pool.report_success(self.current_proxy.id, response_time_ms)
            else:
                self.proxy_pool.report_failure(self.current_proxy.id, error_type)

    def record_scraped_item(self, count: int = 1, table: str = "default"):
        """
        Record scraped item metric (Prometheus).
        """
        record_item(self.scraper_name, table, count)

    def record_error(self, error_type: str, count: int = 1):
        """
        Record error metric (Prometheus) and log error.
        """
        self.logger.error(f"Error encountered: {error_type}")
        record_error(self.scraper_name, error_type, count)
        
    def record_request_metric(self, status_code: int, duration_ms: float, method: str = "GET"):
        """
        Record HTTP request metric.
        """
        record_request(self.scraper_name, status_code, duration_ms, method)

    def _setup_signal_handlers(self):
        """
        Setup SIGINT/SIGTERM handlers for graceful shutdown.
        """
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        """
        Handle shutdown signal. Sets flag and calls on_shutdown hook.
        """
        self.logger.warning(f"Shutdown signal {signum} received. Initiating graceful shutdown...")
        self._shutdown_requested = True
        self.on_shutdown()
        
        # Give a moment for cleanup
        time.sleep(1)
        
        # Ensure DB is closed
        try:
            self.db.close()
        except:
            pass
            
        self.logger.info("Shutdown complete.")
        sys.exit(0)

    def on_shutdown(self):
        """
        Hook for subclass cleanup (e.g., closing browsers).
        Override this method in subclasses.
        """
        pass

    def sleep(self, seconds: float):
        """
        Sleep that is interruptible by shutdown signal.
        """
        step = 0.5
        slept = 0.0
        while slept < seconds and not self._shutdown_requested:
            time.sleep(min(step, seconds - slept))
            slept += step
