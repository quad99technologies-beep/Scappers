"""
Standardized Browser Session Management.
Encapsulates Selenium driver lifecycle, page navigation state machine, and error handling.
Extracted from Canada Ontario script.
"""

import time
import re
import logging
import random
from dataclasses import dataclass
from typing import Optional, List, Any
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.common.exceptions import WebDriverException

try:
    from webdriver_manager.chrome import ChromeDriverManager
    WEBDRIVER_MANAGER_AVAILABLE = True
except ImportError:
    WEBDRIVER_MANAGER_AVAILABLE = False
    ChromeDriverManager = None

# Core imports (optional dependencies)
try:
    from core.smart_locator import SmartLocator
    from core.state_machine import NavigationStateMachine
    STATE_MACHINE_AVAILABLE = True
except ImportError:
    STATE_MACHINE_AVAILABLE = False
    SmartLocator = None
    NavigationStateMachine = None

try:
    from core.browser.browser_observer import observe_selenium, wait_until_idle
    BROWSER_OBSERVER_AVAILABLE = True
except ImportError:
    BROWSER_OBSERVER_AVAILABLE = False
    observe_selenium = lambda x: None
    wait_until_idle = lambda x, y: None

try:
    from core.browser.stealth_profile import apply_selenium
    STEALTH_PROFILE_AVAILABLE = True
except ImportError:
    STEALTH_PROFILE_AVAILABLE = False
    apply_selenium = lambda x: None

try:
    from core.browser.chrome_instance_tracker import ChromeInstanceTracker
    from core.db.postgres_connection import PostgresDB
    CHROME_TRACKING_AVAILABLE = True
except ImportError:
    CHROME_TRACKING_AVAILABLE = False
    ChromeInstanceTracker = None
    PostgresDB = None

from core.browser.human_actions import pause, type_delay
from core.config.retry_config import RetryConfig

logger = logging.getLogger(__name__)

# Constants
DEFAULT_RETRIES = 3
DEFAULT_TIMEOUT = 30
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
]


def _get_chromedriver_path() -> Optional[str]:
    if WEBDRIVER_MANAGER_AVAILABLE and ChromeDriverManager:
        try:
            return ChromeDriverManager().install()
        except Exception:
            return None
    return None


@dataclass
class BrowserSession:
    """
    Manages a Selenium browser session with automatic restart, error handling,
    and state machine integration.
    """
    driver: Optional[webdriver.Chrome] = None
    observer_state: Optional[object] = None
    locator: Optional[object] = None
    state_machine: Optional[object] = None
    uses: int = 0
    run_id: Optional[str] = None
    scraper_name: str = "Unknown"
    headless: bool = True
    enable_stealth: bool = True
    restart_every_n: int = 0
    capture_errors: bool = False
    output_dir: Optional[Path] = None

    def ensure(self) -> webdriver.Chrome:
        """Ensure the browser driver is active, creating it if necessary."""
        if self.driver is None:
            self.driver = self._build_driver()
            
            if BROWSER_OBSERVER_AVAILABLE:
                try:
                    self.observer_state = observe_selenium(self.driver)
                except Exception:
                    self.observer_state = None
            
            if STATE_MACHINE_AVAILABLE:
                try:
                    self.locator = SmartLocator(self.driver, logger=logger)
                    self.state_machine = NavigationStateMachine(self.locator, logger=logger)
                except Exception:
                    self.locator = None
                    self.state_machine = None
        return self.driver

    def _build_driver(self) -> webdriver.Chrome:
        options = webdriver.ChromeOptions()
        
        if self.enable_stealth and STEALTH_PROFILE_AVAILABLE:
            apply_selenium(options)
        
        if self.headless:
            options.add_argument("--headless=new")
            
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1600,1000")
        options.add_argument("--lang=en-US")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-infobars")
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--no-first-run")
        options.add_argument(f"--user-agent={random.choice(USER_AGENTS)}")
        
        driver_path = _get_chromedriver_path()
        if driver_path:
            service = ChromeService(driver_path)
            driver = webdriver.Chrome(service=service, options=options)
        else:
            driver = webdriver.Chrome(options=options)
            
        driver.set_page_load_timeout(DEFAULT_TIMEOUT)
        
        # Inject stealth
        try:
            driver.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
        except Exception:
            pass

        # Track PID
        if CHROME_TRACKING_AVAILABLE and self.run_id and hasattr(driver, "service") and hasattr(driver.service, "process"):
            try:
                pid = driver.service.process.pid
                if pid:
                    driver.tracked_pids = {pid} # Attach for local cleanup
                    try:
                        db = PostgresDB(self.scraper_name)
                        db.connect()
                        tracker = ChromeInstanceTracker(self.scraper_name, self.run_id, db)
                        tracker.register(step_number=1, pid=pid, browser_type='chrome')
                        db.close()
                    except Exception as e:
                        logger.warning(f"Failed to track Chrome PID {pid}: {e}")
            except Exception:
                pass
        
        return driver

    def restart(self) -> None:
        """Close and re-create the browser session."""
        self.close()
        self.ensure()

    def close(self) -> None:
        """Close the browser session and cleanup resources."""
        if self.driver:
            # Mark terminated in DB
            if CHROME_TRACKING_AVAILABLE and self.run_id and hasattr(self.driver, "tracked_pids"):
                try:
                    db = PostgresDB(self.scraper_name)
                    db.connect()
                    tracker = ChromeInstanceTracker(self.scraper_name, self.run_id, db)
                    for pid in self.driver.tracked_pids:
                        tracker.mark_terminated_by_pid(pid, "session_close")
                    db.close()
                except Exception:
                    pass

            try:
                self.driver.quit()
            except Exception:
                pass
                
        self.driver = None
        self.observer_state = None
        self.locator = None
        self.state_machine = None

    def get_page_html(self, url: str, label: str = "") -> str:
        """Navigate to a URL and return page HTML, with retries."""
        driver = self.ensure()
        last_err = None
        
        retries = DEFAULT_RETRIES
        for attempt in range(1, retries + 1):
            try:
                driver.get(url)
                
                if self.observer_state and BROWSER_OBSERVER_AVAILABLE:
                    wait_until_idle(self.observer_state, timeout=RetryConfig.NAVIGATION_TIMEOUT)
                
                pause(0.2, 0.6)
                self.uses += 1
                
                if self.restart_every_n and self.uses % self.restart_every_n == 0:
                    self.restart()
                    
                return driver.page_source
                
            except Exception as exc:
                last_err = exc
                if self.capture_errors and self.output_dir:
                    self._capture_failure(driver, label, attempt)
                
                logger.warning(f"[BROWSER] {label} attempt {attempt} failed: {exc}")
                time.sleep(RetryConfig.calculate_backoff_delay(attempt))
                
                if attempt < retries:
                    self.restart()
                    driver = self.ensure() # Ensure we have a fresh driver for next attempt
                    
        raise RuntimeError(f"Browser GET failed for {label}: {last_err}")
    
    def _capture_failure(self, driver, label, attempt):
        try:
            safe_label = re.sub(r"[^A-Za-z0-9_-]+", "_", label or "page")
            debug_dir = self.output_dir / "artifacts" / "browser_failures"
            debug_dir.mkdir(parents=True, exist_ok=True)
            
            screenshot_path = debug_dir / f"{safe_label}_attempt{attempt}.png"
            html_path = debug_dir / f"{safe_label}_attempt{attempt}.html"
            
            driver.save_screenshot(str(screenshot_path))
            with open(html_path, "w", encoding="utf-8") as fh:
                fh.write(driver.page_source or "")
            logger.info("Captured browser failure: %s", screenshot_path)
        except Exception:
            pass

    def try_search_input(self, query: str, css_selectors: List[str] = None) -> None:
        """Attempt to find a search input and type the query."""
        driver = self.ensure()
        input_elem = None
        
        selectors = css_selectors or [
            "input[name='q']", "input[name='searchTerm']", 
            "input[type='search']", "input[id*='search']"
        ]
        
        # Try finding element
        if self.locator:
            for css in selectors:
                try:
                    input_elem = self.locator.find_element(css=css, required=False, timeout=2)
                    if input_elem: break
                except Exception: continue
        
        if not input_elem:
            for css in selectors:
                try:
                    input_elem = driver.find_element(By.CSS_SELECTOR, css)
                    break
                except Exception: continue
        
        if not input_elem:
            return
            
        try:
            input_elem.clear()
            for ch in query:
                input_elem.send_keys(ch)
                time.sleep(random.uniform(0.05, 0.15))
            input_elem.send_keys(Keys.ENTER)
            pause(0.2, 0.6)
        except Exception:
            return

