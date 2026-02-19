#!/usr/bin/env python3
"""
Base scraper with Playwright stealth context, anti-bot init scripts,
session rotation, and shared DB/run_id management.

Includes memory leak fixes and resource monitoring from Argentina implementation.
"""

import os
import gc
import sys
import logging
import random
import time
import psutil
import signal
import atexit
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Set

# Add repo root to path for core imports (MUST be before any core imports)
_repo_root = Path(__file__).resolve().parents[3]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page, Playwright

logger = logging.getLogger(__name__)

# =============================================================================
# PERFORMANCE FIX: Resource Monitoring and Cleanup
# =============================================================================

_shutdown_requested = threading.Event()
_active_browsers = []
_browsers_lock = threading.Lock()
_tracked_chrome_pids = set()
_tracked_pids_lock = threading.Lock()

# Memory limits
MEMORY_LIMIT_MB = 2048  # 2GB hard limit
MEMORY_CHECK_INTERVAL = 50  # Check every 50 pages


def get_memory_usage_mb() -> float:
    """Get current process memory usage in MB"""
    try:
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024
    except Exception:
        return 0.0


def check_memory_limit() -> bool:
    """Check if memory usage exceeds limit. Returns True if over limit."""
    mem_mb = get_memory_usage_mb()
    if mem_mb > MEMORY_LIMIT_MB:
        logger.warning(f"[MEMORY_LIMIT] Memory usage {mem_mb:.0f}MB exceeds limit {MEMORY_LIMIT_MB}MB")
        return True
    return False


def force_cleanup():
    """Force garbage collection and cleanup"""
    gc.collect()
    for _ in range(3):
        gc.collect()


def track_chrome_pids(pids: Set[int]):
    """Track Chrome PIDs for cleanup"""
    with _tracked_pids_lock:
        _tracked_chrome_pids.update(pids)


def kill_tracked_chrome_processes():
    """Kill only tracked Chrome processes from this scraper instance"""
    killed_count = 0
    
    with _tracked_pids_lock:
        if not _tracked_chrome_pids:
            return 0
        tracked_pids = _tracked_chrome_pids.copy()
    
    for pid in tracked_pids:
        try:
            proc = psutil.Process(pid)
            proc_name = (proc.name() or '').lower()
            if 'chrome' in proc_name or 'chromium' in proc_name:
                proc.kill()
                killed_count += 1
                logger.info(f"[CLEANUP] Killed tracked Chrome process: PID {pid}")
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    
    with _tracked_pids_lock:
        _tracked_chrome_pids.clear()
    
    return killed_count


def register_browser(browser):
    """Register a browser for cleanup on shutdown"""
    with _browsers_lock:
        _active_browsers.append(browser)


def unregister_browser(browser):
    """Unregister a browser"""
    with _browsers_lock:
        if browser in _active_browsers:
            _active_browsers.remove(browser)


def close_all_browsers():
    """Close all registered browsers"""
    with _browsers_lock:
        for browser in _active_browsers[:]:
            try:
                browser.close()
            except Exception:
                pass
        _active_browsers.clear()
    kill_tracked_chrome_processes()


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.warning(f"[SHUTDOWN] Signal {signum} received, initiating graceful shutdown...")
    _shutdown_requested.set()
    close_all_browsers()
    sys.exit(0)


# Register signal handlers
try:
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
except (AttributeError, ValueError):
    pass

# Register atexit handler
atexit.register(close_all_browsers)

# Stealth init script injected into every Playwright context
_STEALTH_INIT_SCRIPT = """
// Hide webdriver property
Object.defineProperty(navigator, 'webdriver', {
    get: () => undefined,
    configurable: true
});

// Mock plugins array
Object.defineProperty(navigator, 'plugins', {
    get: () => {
        const plugins = [
            {name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format'},
            {name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: ''},
            {name: 'Native Client', filename: 'internal-nacl-plugin', description: ''}
        ];
        plugins.length = 3;
        return plugins;
    },
    configurable: true
});

// Mock languages
Object.defineProperty(navigator, 'languages', {
    get: () => ['en-US', 'en'],
    configurable: true
});

// Mock chrome runtime
window.chrome = window.chrome || {};
window.chrome.runtime = window.chrome.runtime || {};
window.chrome.loadTimes = window.chrome.loadTimes || function() {
    return { commitLoadTime: Date.now() / 1000 };
};
window.chrome.csi = window.chrome.csi || function() {
    return { startE: Date.now(), onloadT: Date.now() };
};

// Mock permissions query
if (navigator.permissions) {
    const origQuery = navigator.permissions.query.bind(navigator.permissions);
    navigator.permissions.query = (params) => {
        if (params.name === 'notifications') {
            return Promise.resolve({state: Notification.permission});
        }
        return origQuery(params);
    };
}

// Remove Playwright-specific properties
delete window.__playwright;
delete window.__pw_manual;
"""

# Random user agents pool
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]


class BaseScraper:
    """
    Playwright-based scraper with stealth context and session rotation.

    Subclasses implement scraping logic; this class manages browser lifecycle,
    anti-bot measures, and provides human-like action helpers.
    """

    def __init__(self, run_id: str, db, config: dict = None):
        """
        Args:
            run_id: Pipeline run identifier.
            db: CountryDB instance.
            config: Optional config dict (from config_loader).
        """
        self.run_id = run_id
        self.db = db
        self.config = config or {}

        # For Chrome PID tracking (used by the GUI's "Chrome Instances (tracked)" counter)
        # Defaults are derived from this file location: scripts/<ScraperName>/scrapers/base.py
        self.scraper_name = (
            self.config.get("scraper_name")
            or self.config.get("SCRAPER_NAME")
            or self._infer_scraper_name()
        )
        self.repo_root = self._infer_repo_root()
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page_count = 0
        self.max_pages_per_session = int(self.config.get("max_pages_per_session", 50))

    @staticmethod
    def _infer_scraper_name() -> str:
        try:
            # .../scripts/Malaysia/scrapers/base.py -> parents[1] == "Malaysia"
            return Path(__file__).resolve().parents[1].name
        except Exception:
            return "Unknown"

    @staticmethod
    def _infer_repo_root() -> Path:
        try:
            # .../scripts/Malaysia/scrapers/base.py -> parents[3] == repo root
            return Path(__file__).resolve().parents[3]
        except Exception:
            return Path.cwd()

    def _track_playwright_chrome_pids(self) -> None:
        """
        Best-effort: detect and persist Playwright-launched Chrome/Chromium PIDs using standardized ChromeInstanceTracker.
        """
        try:
            if not self._browser:
                return
            
            # Use standardized ChromeInstanceTracker instead of PID files
            from core.browser.chrome_pid_tracker import get_chrome_pids_from_playwright_browser
            from core.browser.chrome_instance_tracker import ChromeInstanceTracker
            from core.db.connection import CountryDB
            
            pids = get_chrome_pids_from_playwright_browser(self._browser)
            if pids and self.run_id:
                try:
                    db = CountryDB(self.scraper_name)
                    db.connect()
                    try:
                        tracker = ChromeInstanceTracker(self.scraper_name, self.run_id, db)
                        step_number = getattr(self, '_step_number', 2)
                        thread_id = getattr(self, '_thread_id', None)
                        driver_pid = list(pids)[0]
                        tracker.register(
                            step_number=step_number,
                            pid=driver_pid,
                            thread_id=thread_id,
                            browser_type="chrome",
                            user_data_dir=None,
                            child_pids=pids
                        )
                    finally:
                        db.close()
                except Exception as e:
                    logger.debug(f"ChromeInstanceTracker registration failed (non-fatal): {e}")
        except Exception:
            # Never fail scraping due to optional PID tracking.
            logger.debug("Chrome PID tracking failed (non-fatal)", exc_info=True)

    # ------------------------------------------------------------------
    # Context management
    # ------------------------------------------------------------------

    def _launch_args(self):
        return [
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--start-maximized",
        ]

    def _context_options(self) -> dict:
        """Return kwargs for browser.new_context() using standardized stealth profile and geo routing."""
        # Start with standardized stealth profile
        context_kwargs = {}
        try:
            from core.browser.stealth_profile import apply_playwright, get_stealth_init_script
            apply_playwright(context_kwargs)
        except ImportError:
            # Fallback to custom implementation if stealth_profile not available
            context_kwargs = {
                "locale": "en-US",
                "timezone_id": "Asia/Kuala_Lumpur",
                "viewport": {"width": 1366, "height": 768},
                "user_agent": random.choice(_USER_AGENTS),
                "extra_http_headers": {
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "DNT": "1",
                },
            }
        
        # Apply Geo Router for automatic VPN/proxy/timezone/locale configuration
        try:
            from core.network.geo_router import GeoRouter, get_geo_router
            router = get_geo_router()
            route_config = router.get_route(self.scraper_name)
            
            if route_config:
                # Apply geo-specific settings
                context_kwargs["timezone_id"] = route_config.timezone
                context_kwargs["locale"] = route_config.locale
                
                # Add geolocation if available
                if route_config.country_code == "MY":
                    context_kwargs["geolocation"] = {"latitude": 3.139, "longitude": 101.6869}
                    context_kwargs["permissions"] = ["geolocation"]
                
                # Get proxy from proxy pool if enabled
                try:
                    proxy = router.proxy_pool.get_proxy(
                        country_code=route_config.country_code,
                        proxy_type=route_config.proxy_type
                    )
                    if proxy and proxy.status.value == "healthy":
                        # Configure proxy for Playwright
                        proxy_server = f"{proxy.host}:{proxy.port}"
                        if proxy.username and proxy.password:
                            proxy_server = f"{proxy.username}:{proxy.password}@{proxy_server}"
                        context_kwargs["proxy"] = {
                            "server": f"http://{proxy_server}",
                            "username": proxy.username,
                            "password": proxy.password
                        }
                        logger.info(f"[GEO_ROUTER] Using proxy: {proxy.id} for {self.scraper_name}")
                except Exception as e:
                    logger.debug(f"[GEO_ROUTER] Proxy not available: {e}")
            else:
                # Fallback to Malaysia defaults if no route config
                context_kwargs["timezone_id"] = "Asia/Kuala_Lumpur"
                context_kwargs["geolocation"] = {"latitude": 3.139, "longitude": 101.6869}
                context_kwargs["permissions"] = ["geolocation"]
        except ImportError:
            # Fallback if geo router not available
            context_kwargs["timezone_id"] = "Asia/Kuala_Lumpur"
            context_kwargs["geolocation"] = {"latitude": 3.139, "longitude": 101.6869}
            context_kwargs["permissions"] = ["geolocation"]
        except Exception as e:
            logger.debug(f"[GEO_ROUTER] Geo routing failed (non-fatal): {e}")
            # Fallback to Malaysia defaults
            context_kwargs["timezone_id"] = "Asia/Kuala_Lumpur"
            context_kwargs["geolocation"] = {"latitude": 3.139, "longitude": 101.6869}
            context_kwargs["permissions"] = ["geolocation"]
        
        return context_kwargs

    def _create_context(self) -> BrowserContext:
        """Create a new stealth browser context using standardized stealth profile."""
        ctx_kwargs = self._context_options()
        ctx = self._browser.new_context(**ctx_kwargs)
        
        # Apply standardized stealth init script
        try:
            from core.browser.stealth_profile import get_stealth_init_script
            stealth_script = get_stealth_init_script()
            if stealth_script:
                ctx.add_init_script(stealth_script)
        except ImportError:
            # Fallback to custom script if not available
            ctx.add_init_script(_STEALTH_INIT_SCRIPT)
        
        self._page_count = 0
        return ctx

    @contextmanager
    def browser_session(self, headless: bool = False):
        """
        Context manager that provides a stealth Playwright page.
        Includes memory leak fixes and resource monitoring.

        Usage:
            with self.browser_session() as page:
                page.goto("https://example.com")
        """
        self._playwright = sync_playwright().start()
        try:
            self._browser = self._playwright.chromium.launch(
                headless=headless,
                args=self._launch_args(),
            )
            register_browser(self._browser)
            self._track_playwright_chrome_pids()
            self._context = self._create_context()
            page = self._context.new_page()
            self._page_count = 1
            yield page
        finally:
            if self._context:
                try:
                    self._context.close()
                except Exception:
                    pass
            if self._browser:
                try:
                    self._browser.close()
                except Exception:
                    pass
                unregister_browser(self._browser)
            if self._playwright:
                try:
                    self._playwright.stop()
                except Exception:
                    pass
            self._context = None
            self._browser = None
            self._playwright = None
            # PERFORMANCE FIX: Force cleanup after browser session
            force_cleanup()

    def new_page(self) -> Page:
        """Get a new page, rotating context if needed."""
        self._page_count += 1
        
        # PERFORMANCE FIX: Periodic garbage collection and memory check every 50 pages
        if self._page_count % MEMORY_CHECK_INTERVAL == 0:
            force_cleanup()
            mem_mb = get_memory_usage_mb()
            logger.info("[PERFORMANCE] Page %d: Memory %.1fMB", self._page_count, mem_mb)
            
            # Check memory limit
            if check_memory_limit():
                logger.warning("[MEMORY] Memory limit exceeded, forcing context rotation")
                # Force context rotation
                if self._browser:
                    old = self._context
                    self._context = self._create_context()
                    if old:
                        try:
                            old.close()
                        except Exception:
                            pass
                    # Force cleanup after rotation
                    force_cleanup()
        
        if self._page_count > self.max_pages_per_session and self._browser:
            logger.info("Session rotation: creating new context after %d pages",
                        self._page_count - 1)
            old = self._context
            self._context = self._create_context()
            if old:
                try:
                    old.close()
                except Exception:
                    pass
            # PERFORMANCE FIX: Force cleanup after rotation
            force_cleanup()
        
        return self._context.new_page()

    # ------------------------------------------------------------------
    # Human-like helpers
    # ------------------------------------------------------------------

    @staticmethod
    def pause(min_s: float = 0.3, max_s: float = 1.0):
        """Random pause to mimic human behavior."""
        time.sleep(random.uniform(min_s, max_s))

    @staticmethod
    def long_pause(min_s: float = 1.5, max_s: float = 3.5):
        """Longer pause for page loads."""
        time.sleep(random.uniform(min_s, max_s))

    @staticmethod
    def type_delay_ms() -> int:
        """Random per-character typing delay in ms."""
        return random.randint(50, 150)

    def human_type(self, page: Page, selector: str, text: str):
        """Type text character-by-character with human-like delays."""
        page.locator(selector).click()
        self.pause(0.1, 0.3)
        page.locator(selector).press_sequentially(text, delay=self.type_delay_ms())
        self.pause(0.2, 0.5)

    # ------------------------------------------------------------------
    # Wait helpers
    # ------------------------------------------------------------------

    def wait_for_cloudflare(self, page: Page, timeout_s: float = 90.0):
        """
        Wait for Cloudflare verification to complete.
        Detects challenge pages and waits for them to resolve.
        """
        start = time.time()
        check_interval = 2.0

        while time.time() - start < timeout_s:
            try:
                body_text = page.evaluate("document.body?.innerText || ''").lower()
            except Exception:
                time.sleep(check_interval)
                continue

            is_challenge = any(kw in body_text for kw in [
                "please wait", "verifying", "checking your browser",
                "just a moment", "enable javascript",
            ])

            if not is_challenge:
                logger.info("Cloudflare verification passed (%.1fs)",
                            time.time() - start)
                return True

            time.sleep(check_interval)

        logger.warning("Cloudflare verification timed out after %.0fs", timeout_s)
        return False

    def wait_for_table_stable(self, page: Page, row_selector: str,
                              checks: int = 3, interval: float = 2.0,
                              timeout: float = 60.0) -> int:
        """
        Wait for table row count to stabilize.
        Returns final row count or 0 on timeout.
        """
        start = time.time()
        stable_count = 0
        last_count = -1

        while time.time() - start < timeout:
            try:
                current = page.locator(row_selector).count()
            except Exception:
                current = 0

            if current > 0 and current == last_count:
                stable_count += 1
                if stable_count >= checks:
                    logger.info("Table stable at %d rows after %.1fs",
                                current, time.time() - start)
                    return current
            else:
                stable_count = 0
            last_count = current
            time.sleep(interval)

        logger.warning("Table stability timeout after %.0fs (last count: %d)",
                        timeout, last_count)
        return max(last_count, 0)
