import os
import json
import time
import sys
import math
import threading
from queue import Queue, Empty
from pathlib import Path
from typing import List, Dict, Optional, Set

# Add repo root for core imports (MUST be before any core imports)
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add script directory to path for config_loader import
_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from core.monitoring.audit_logger import audit_log

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, StaleElementReferenceException
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

try:
    from config_loader import load_env_file, getenv, getenv_bool, getenv_int, getenv_float, get_output_dir
    load_env_file()
    OUTPUT_DIR = get_output_dir()
    USE_CONFIG = True
except ImportError:
    OUTPUT_DIR = Path(__file__).resolve().parent
    USE_CONFIG = False
    def getenv(key: str, default: str = None) -> str:
        return os.getenv(key, default if default is not None else "")
    def getenv_bool(key: str, default: bool = False) -> bool:
        val = os.getenv(key, str(default))
        return str(val).lower() in ("true", "1", "yes", "on")
    def getenv_int(key: str, default: int = 0) -> int:
        try:
            return int(os.getenv(key, str(default)))
        except (ValueError, TypeError):
            return default
    def getenv_float(key: str, default: float = 0.0) -> float:
        try:
            return float(os.getenv(key, str(default)))
        except (ValueError, TypeError):
            return default

# Import Chrome PID tracking utilities (DB-based)
try:
    from core.browser.chrome_pid_tracker import get_chrome_pids_from_driver, terminate_scraper_pids
    from core.browser.chrome_instance_tracker import ChromeInstanceTracker
    from core.db.connection import CountryDB
except ImportError:
    get_chrome_pids_from_driver = None
    terminate_scraper_pids = None
    ChromeInstanceTracker = None
    CountryDB = None

try:
    from core.browser.chrome_manager import get_chromedriver_path as _core_get_chromedriver_path, register_chrome_driver, unregister_chrome_driver
    CORE_CHROMEDRIVER_AVAILABLE = True
except ImportError:
    CORE_CHROMEDRIVER_AVAILABLE = False
    _core_get_chromedriver_path = None
    register_chrome_driver = None
    unregister_chrome_driver = None

# Import stealth profile for anti-detection
try:
    from core.browser.stealth_profile import apply_selenium
    STEALTH_PROFILE_AVAILABLE = True
except ImportError:
    STEALTH_PROFILE_AVAILABLE = False
    def apply_selenium(options):
        pass  # Stealth profile not available, skip

# Import browser observer for idle detection
try:
    from core.browser.browser_observer import observe_selenium, wait_until_idle
    BROWSER_OBSERVER_AVAILABLE = True
except ImportError:
    BROWSER_OBSERVER_AVAILABLE = False
    def observe_selenium(driver):
        return None
    def wait_until_idle(state, timeout=10.0):
        pass  # Browser observer not available, skip

# Import human pacing
try:
    from core.browser.human_actions import pause
    HUMAN_ACTIONS_AVAILABLE = True
except ImportError:
    HUMAN_ACTIONS_AVAILABLE = False
    def pause(min_s=0.2, max_s=0.6):
        import random
        time.sleep(random.uniform(min_s, max_s))

# Import Telegram notifier for status updates
try:
    from core.utils.telegram_notifier import TelegramNotifier
    TELEGRAM_NOTIFIER_AVAILABLE = True
except ImportError:
    TELEGRAM_NOTIFIER_AVAILABLE = False
    TelegramNotifier = None

# Import state machine and smart locator for Tier 1 robustness
try:
    from smart_locator import SmartLocator
    from state_machine import NavigationStateMachine, NavigationState, StateCondition
    STATE_MACHINE_AVAILABLE = True
except ImportError:
    STATE_MACHINE_AVAILABLE = False
    SmartLocator = None
    NavigationStateMachine = None
    NavigationState = None
    StateCondition = None

BASE_URL = "https://lekovi.zdravstvo.gov.mk/drugsregister/overview"
# Multi-threading configuration
NUM_WORKERS = getenv_int("SCRIPT_01_NUM_WORKERS", 3) if USE_CONFIG else 3
MAX_RETRIES_PER_PAGE = getenv_int("SCRIPT_01_MAX_RETRIES_PER_PAGE", 3) if USE_CONFIG else 3

# Navigation retry settings
NAV_RETRIES = getenv_int("SCRIPT_01_NAV_RETRIES", 3) if USE_CONFIG else 3
NAV_RETRY_SLEEP = getenv_float("SCRIPT_01_NAV_RETRY_SLEEP", 5.0) if USE_CONFIG else 5.0
PAGE_LOAD_TIMEOUT = getenv_int("SCRIPT_01_PAGE_LOAD_TIMEOUT", 120) if USE_CONFIG else 120
WAIT_TIMEOUT = getenv_int("SCRIPT_01_WAIT_TIMEOUT", 40) if USE_CONFIG else 40

# Fix for module shadowing: Remove any conflicting 'db' module from sys.modules
# to ensure 'from db ...' resolves to the local db directory.
if "db" in sys.modules:
    del sys.modules["db"]

_driver_path = None
_driver_path_lock = None


def build_driver(headless: bool = True) -> Optional[webdriver.Chrome]:
    """Build Chrome driver with enhanced anti-bot features using core factory."""
    from core.browser.driver_factory import create_chrome_driver

    # Configure options
    extra_options = {
        "user_agent": getenv("SCRIPT_01_CHROME_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
        "page_load_timeout": PAGE_LOAD_TIMEOUT
    }

    try:
        driver = create_chrome_driver(headless=headless, extra_options=extra_options)
    except Exception as e:
        print(f"Error creating driver: {e}", flush=True)
        return None

    if not driver:
        return None

    # Apply stealth profile if available (core factory does some, but we might want extra if configured)
    # The core factory already adds some stealth options. 
    # If STEALTH_PROFILE_AVAILABLE is true, we might want to apply specific selenium stealth
    if STEALTH_PROFILE_AVAILABLE:
        try:
             # Just in case core factory missed something specific to this scraper's needs
             # But usually core factory is sufficient. 
             # For now, we rely on core factory's built-in stealth + existing CDP commands.
             pass
        except Exception:
            pass

    # Track Chrome PIDs in DB for pipeline stop cleanup
    run_id = os.environ.get("NORTH_MACEDONIA_RUN_ID", "").strip()
    if not run_id:
        run_id_file = get_output_dir() / ".current_run_id"
        if run_id_file.exists():
            try:
                run_id = run_id_file.read_text(encoding="utf-8").strip() or ""
            except Exception:
                pass
    
    if ChromeInstanceTracker and CountryDB and run_id and get_chrome_pids_from_driver:
        try:
            pids = get_chrome_pids_from_driver(driver)
            if pids:
                driver_pid = driver.service.process.pid if hasattr(driver.service, 'process') else list(pids)[0]
                db = CountryDB("NorthMacedonia")
                db.connect()
                try:
                    tracker = ChromeInstanceTracker("NorthMacedonia", run_id, db)
                    tracker.register(step_number=1, pid=driver_pid, browser_type="chrome", child_pids=pids)
                finally:
                    db.close()
        except Exception as e:
            print(f"Warning: Could not register Chrome instance: {e}", flush=True)
            
    return driver


def remove_webdriver_property(driver: webdriver.Chrome) -> None:
    """Remove webdriver property after page load for additional stealth."""
    try:
        driver.execute_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)
    except Exception:
        pass


def is_session_valid(driver: webdriver.Chrome) -> bool:
    """Check if the Chrome session is still valid and responsive."""
    try:
        _ = driver.current_url
        return True
    except Exception:
        return False


def shutdown_driver(driver: webdriver.Chrome) -> None:
    """Safely shutdown the Chrome driver."""
    if unregister_chrome_driver:
        try:
            unregister_chrome_driver(driver)
        except Exception:
            pass
    try:
        driver.quit()
    except Exception:
        pass


def restart_driver(driver: webdriver.Chrome, headless: bool = True) -> webdriver.Chrome:
    """Restart the Chrome driver."""
    shutdown_driver(driver)
    return build_driver(headless=headless)


def wait_grid_loaded(driver: webdriver.Chrome, timeout: int = None) -> None:
    """Wait for the Telerik grid to be fully loaded."""
    if timeout is None:
        timeout = WAIT_TIMEOUT
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div#grid table"))
    )
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div.t-data-grid-pager"))
    )
    # Use browser observer if available
    if BROWSER_OBSERVER_AVAILABLE:
        state = observe_selenium(driver)
        wait_until_idle(state, timeout=5.0)
    # Add human-like pause
    if HUMAN_ACTIONS_AVAILABLE:
        pause(0.2, 0.5)


def navigate_with_retries(
    driver: webdriver.Chrome,
    url: str,
    wait_fn,
    label: str,
    headless: bool = True,
    on_restart=None,
    state_machine=None,
) -> webdriver.Chrome:
    """Navigate to URL with retry logic and optional driver restart."""
    last_exc = None
    for attempt in range(1, NAV_RETRIES + 1):
        try:
            driver.get(url)
            remove_webdriver_property(driver)
            # State validation
            if STATE_MACHINE_AVAILABLE and state_machine:
                if not state_machine.transition_to(NavigationState.PAGE_LOADED, reload_on_failure=False):
                    print(f"  [WARN] {label}: Failed to validate PAGE_LOADED state", flush=True)
            wait_fn(driver)
            return driver
        except (TimeoutException, WebDriverException) as exc:
            last_exc = exc
            try:
                driver.execute_script("window.stop();")
            except Exception:
                pass
            try:
                wait_fn(driver)
                return driver
            except Exception:
                pass
            if attempt < NAV_RETRIES:
                print(f"  [WARN] {label} load failed (attempt {attempt}/{NAV_RETRIES}). Retrying in {NAV_RETRY_SLEEP}s...", flush=True)
                time.sleep(NAV_RETRY_SLEEP)

    # All retries failed, restart driver
    print(f"  [WARN] {label} load failed; restarting Chrome session.", flush=True)
    driver = restart_driver(driver, headless=headless)
    if on_restart:
        driver = on_restart(driver) or driver
    for attempt in range(1, NAV_RETRIES + 1):
        try:
            driver.get(url)
            remove_webdriver_property(driver)
            if STATE_MACHINE_AVAILABLE and state_machine:
                if not state_machine.transition_to(NavigationState.PAGE_LOADED, reload_on_failure=False):
                    print(f"  [WARN] {label}: Failed to validate PAGE_LOADED state after restart", flush=True)
            wait_fn(driver)
            return driver
        except (TimeoutException, WebDriverException) as exc:
            last_exc = exc
            try:
                driver.execute_script("window.stop();")
            except Exception:
                pass
            try:
                wait_fn(driver)
                return driver
            except Exception:
                pass
            if attempt < NAV_RETRIES:
                print(f"  [WARN] {label} load failed after restart (attempt {attempt}/{NAV_RETRIES}). Retrying in {NAV_RETRY_SLEEP}s...", flush=True)
                time.sleep(NAV_RETRY_SLEEP)

    if last_exc:
        raise last_exc
    raise RuntimeError(f"{label} load failed")


def get_rows_per_page_value(driver: webdriver.Chrome) -> Optional[str]:
    """Read the current rows-per-page selection from the grid."""
    try:
        sel_el = driver.find_element(By.CSS_SELECTOR, "select[name='rowsPerPage'], select[id^='rowsPerPage']")
        return sel_el.get_attribute("value")
    except Exception:
        return None


def set_rows_per_page(driver: webdriver.Chrome, value: str = "200") -> bool:
    """Try multiple strategies to bump page size to the desired value. Returns True when applied."""
    # Strategy 1: select element by name or id prefix
    try:
        sel_el = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "select[name='rowsPerPage'], select[id^='rowsPerPage']"))
        )
        from selenium.webdriver.support.ui import Select
        sel = Select(sel_el)
        sel.select_by_value(value)
        driver.execute_script("arguments[0].dispatchEvent(new Event('change', {bubbles: true}));", sel_el)
        pause(0.3, 0.5)
        wait_grid_loaded(driver, 20)
        if get_rows_per_page_value(driver) == value:
            return True
    except Exception:
        pass

    # Strategy 2: pager links with pageSize param (common in Telerik grids)
    try:
        pager = driver.find_element(By.CSS_SELECTOR, "div.t-data-grid-pager")
        links = pager.find_elements(By.CSS_SELECTOR, "a")
        target = None
        for a in links:
            href = (a.get_attribute("href") or "")
            if "pageSize" in href and value in href:
                target = a
                break
        if target:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", target)
            target.click()
            pause(0.3, 0.5)
            wait_grid_loaded(driver, 20)
            if get_rows_per_page_value(driver) == value:
                return True
    except Exception:
        pass

    # Strategy 3: execute script to set pageSize if grid object is exposed
    try:
        driver.execute_script(
            "if(window.t && t.grid) { try { t.grid.pageSize(%s); } catch(e){} }" % int(value)
        )
        pause(0.3, 0.5)
        wait_grid_loaded(driver, 20)
        if get_rows_per_page_value(driver) == value:
            return True
    except Exception:
        pass

    # Strategy 4: force set value and fire change
    try:
        driver.execute_script(
            """
            const sel = document.querySelector("select[name='rowsPerPage'], select[id^='rowsPerPage']");
            if (sel) {
                sel.value = arguments[0];
                sel.dispatchEvent(new Event('change', {bubbles: true}));
            }
            """,
            value,
        )
        pause(0.3, 0.5)
        wait_grid_loaded(driver, 20)
        if get_rows_per_page_value(driver) == value:
            return True
    except Exception:
        pass

    return False


def count_grid_rows(driver: webdriver.Chrome) -> int:
    """Count the number of rows in the current grid."""
    try:
        rows = driver.find_elements(By.CSS_SELECTOR, "div#grid table tbody tr")
        return len(rows)
    except Exception:
        return 0


def wait_for_correct_row_count(
    driver: webdriver.Chrome,
    expected_rows: int,
    max_wait_seconds: int = 30,
    is_last_page: bool = False
) -> bool:
    """
    Wait until the grid has the expected number of rows.

    Args:
        driver: Chrome webdriver instance
        expected_rows: Expected number of rows (e.g., 200)
        max_wait_seconds: Maximum time to wait in seconds
        is_last_page: If True, accept any count <= expected_rows

    Returns:
        True if correct row count achieved, False otherwise
    """
    deadline = time.time() + max_wait_seconds
    attempts = 0

    while time.time() < deadline:
        attempts += 1
        current_count = count_grid_rows(driver)

        # Check if we have the correct number of rows
        if is_last_page:
            # Last page can have fewer rows
            if 0 < current_count <= expected_rows:
                print(f"  [OK] Grid has {current_count} rows (last page)", flush=True)
                return True
        else:
            # Non-last pages should have exactly expected_rows
            if current_count == expected_rows:
                print(f"  [OK] Grid has {current_count} rows", flush=True)
                return True

        # Log current status every few attempts
        if attempts % 3 == 0:
            print(f"  [WAIT] Grid has {current_count} rows, expecting {expected_rows}... (attempt {attempts})", flush=True)

        # If we have wrong count, try refreshing the grid
        if attempts == 5:
            print(f"  [RETRY] Grid stuck at {current_count} rows, trying to refresh...", flush=True)
            try:
                # Try to re-trigger the page size setting
                current_page_size = get_rows_per_page_value(driver)
                if current_page_size != str(expected_rows):
                    print(f"  [WARN] Page size is {current_page_size}, should be {expected_rows}. Re-setting...", flush=True)
                    set_rows_per_page(driver, str(expected_rows))
                    wait_grid_loaded(driver, 20)
            except Exception as e:
                print(f"  [WARN] Failed to refresh grid: {e}", flush=True)

        time.sleep(1)

    # Timeout - report final count
    final_count = count_grid_rows(driver)
    print(f"  [WARN] Timeout waiting for {expected_rows} rows. Grid has {final_count} rows", flush=True)
    return False


def extract_detail_url_list_from_current_grid(driver: webdriver.Chrome, state_machine=None) -> List[str]:
    """Extract detail URLs from the current grid page."""
    wait_grid_loaded(driver, WAIT_TIMEOUT)

    # State validation
    if STATE_MACHINE_AVAILABLE and state_machine:
        grid_conditions = [
            StateCondition(element_selector="div#grid table", min_count=1, max_wait=WAIT_TIMEOUT)
        ]
        if not state_machine.transition_to(NavigationState.GRID_READY, custom_conditions=grid_conditions, reload_on_failure=False):
            print("  [WARN] Failed to validate GRID_READY state, continuing with extraction", flush=True)

    links = driver.find_elements(By.CSS_SELECTOR, "td.latinName a[href*='detaileddrug']")
    urls = []
    for a in links:
        try:
            href = a.get_attribute("href")
            if href:
                urls.append(href)
        except StaleElementReferenceException:
            continue
    return urls


def click_next_page(driver: webdriver.Chrome) -> bool:
    """Click the next-page control; returns False if no next page is available."""
    try:
        pager = driver.find_element(By.CSS_SELECTOR, "div.t-data-grid-pager")
    except Exception:
        return False
    # Scroll pager into view to make sure click succeeds
    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", pager)
    except Exception:
        pass

    selectors = [
        "a.t-arrow-next:not(.t-state-disabled)",
        "a[title*='Next']:not(.t-state-disabled)",
        "a[title*='След']:not(.t-state-disabled)",
        "a[aria-label*='Next']:not(.t-state-disabled)",
        "a[aria-label*='След']:not(.t-state-disabled)",
    ]
    next_links = []
    for sel in selectors:
        found = pager.find_elements(By.CSS_SELECTOR, sel)
        if found:
            next_links.extend(found)
    if not next_links:
        # Fallback to text-based lookup
        next_links = pager.find_elements(By.XPATH, ".//a[normalize-space(text())='>']")
    if not next_links:
        return False

    nxt = next_links[-1]
    # Ensure not disabled
    classes = (nxt.get_attribute("class") or "").lower()
    if "disabled" in classes or "t-state-disabled" in classes:
        return False

    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", nxt)
        nxt.click()
        pause(0.2, 0.4)
        wait_grid_loaded(driver, 20)
        return True
    except Exception:
        return False


def click_page(driver: webdriver.Chrome, target_page: int) -> bool:
    """Click a specific page number if available."""
    try:
        pager = driver.find_element(By.CSS_SELECTOR, "div.t-data-grid-pager")
    except Exception:
        return False
    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", pager)
    except Exception:
        pass
    candidates = pager.find_elements(
        By.CSS_SELECTOR,
        f"a[href*='drugsregister.grid.pager/{target_page}/'], a[href*='pager/{target_page}/']"
    )
    if not candidates:
        # Fallback: anchor text equals target_page
        candidates = pager.find_elements(By.XPATH, f".//a[normalize-space(text())='{target_page}']")
    if not candidates:
        return False
    link = candidates[-1]
    classes = (link.get_attribute("class") or "").lower()
    if "disabled" in classes or "t-state-disabled" in classes:
        return False
    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", link)
        link.click()
        pause(0.2, 0.4)
        wait_grid_loaded(driver, 20)
        return True
    except Exception:
        return False


def get_total_pages(driver: webdriver.Chrome, current_page: int) -> Optional[int]:
    """Try to detect total pages from pager links/text."""
    try:
        pager = driver.find_element(By.CSS_SELECTOR, "div.t-data-grid-pager")
    except Exception:
        return None
    max_num = 0
    try:
        # Look for href pattern with pager/{page}/
        links = pager.find_elements(By.CSS_SELECTOR, "a")
        import re
        for a in links:
            href = (a.get_attribute("href") or "")
            m = re.search(r'pager/(\d+)', href)
            if m:
                val = int(m.group(1))
                if val > max_num:
                    max_num = val
            txt = (a.text or "").strip()
            if txt.isdigit():
                val = int(txt)
                if val > max_num:
                    max_num = val
        if max_num > 0:
            return max_num
        # Fallback: spans containing "of N" or "X/Y"
        spans = pager.find_elements(By.XPATH, ".//span")
        for span in spans:
            txt = (span.text or "").strip()
            m = re.search(r'(\d+)\s*/\s*(\d+)', txt)
            if m:
                return int(m.group(2))
            m = re.search(r'of\s+(\d+)', txt, re.IGNORECASE)
            if m:
                return int(m.group(1))
            m = re.search(r'од\s+(\d+)', txt, re.IGNORECASE)
            if m:
                return int(m.group(1))
    except Exception:
        pass
    return None


def get_total_records(driver: webdriver.Chrome) -> Optional[int]:
    """Parse total record count from pager text like '1-10 од 4128' or '1-10 of 4128'."""
    try:
        pager = driver.find_element(By.CSS_SELECTOR, "div.t-data-grid-pager")
    except Exception:
        return None
    import re
    try:
        text = pager.text or ""
        m = re.search(r'of\s+(\d+)', text, re.IGNORECASE)
        if m:
            return int(m.group(1))
        m = re.search(r'од\s+(\d+)', text, re.IGNORECASE)
        if m:
            return int(m.group(1))
    except Exception:
        return None
    return None


# Helper functions for CSV/JSON checkpointing removed.
# All state is now managed via PostgreSQL in NorthMacedoniaRepository.


def worker_fn(
    worker_id: int,
    page_queue: Queue,
    seen_urls: Set[str],
    seen_urls_lock: threading.Lock,
    progress: Dict,
    progress_lock: threading.Lock,
    headless: bool,
    rows_per_page: str,
    total_pages: int,
    repo: Any
) -> None:
    """Worker function to process pages in parallel (DB-only)."""
    driver = None
    state_machine = None
    locator = None

    try:
        driver = build_driver(headless=headless)
        if driver is None:
            print(f"[Worker {worker_id}] Failed to create driver", flush=True)
            return
        # Brief warm-up so Chrome is fully ready before hitting the site
        time.sleep(2)

        # Initialize state machine for this worker
        if STATE_MACHINE_AVAILABLE:
            import logging
            logger = logging.getLogger(f"worker_{worker_id}")
            locator = SmartLocator(driver, logger=logger)
            state_machine = NavigationStateMachine(locator, logger=logger)

        print(f"[Worker {worker_id}] Started", flush=True)

        # Initialize session: go to base URL and set rows per page first.
        # Retry with a fresh driver if the connection is reset (HTTP 10054).
        SESSION_INIT_RETRIES = 3
        session_ok = False
        for _init_attempt in range(1, SESSION_INIT_RETRIES + 1):
            print(f"  [Worker {worker_id}] Initializing session (attempt {_init_attempt}/{SESSION_INIT_RETRIES}) with page size {rows_per_page}...", flush=True)
            try:
                driver = navigate_with_retries(
                    driver, BASE_URL, wait_grid_loaded,
                    f"worker {worker_id} init",
                    headless=headless, state_machine=state_machine
                )
                set_rows_per_page(driver, rows_per_page)
                wait_grid_loaded(driver, 30)

                # Verify page size was applied
                for _ in range(5):
                    if get_rows_per_page_value(driver) == rows_per_page:
                        break
                    pause(0.3, 0.5)
                    wait_grid_loaded(driver, 15)

                print(f"  [Worker {worker_id}] Session initialized. Page size: {get_rows_per_page_value(driver)}", flush=True)
                session_ok = True
                break
            except Exception as e:
                print(f"  [Worker {worker_id}] Session init failed (attempt {_init_attempt}): {e}", flush=True)
                if _init_attempt < SESSION_INIT_RETRIES:
                    print(f"  [Worker {worker_id}] Restarting driver and retrying in 10s...", flush=True)
                    time.sleep(10)
                    driver = restart_driver(driver, headless=headless)
                    if STATE_MACHINE_AVAILABLE and driver:
                        import logging as _logging
                        _logger = _logging.getLogger(f"worker_{worker_id}")
                        locator = SmartLocator(driver, _logger)
                        state_machine = NavigationStateMachine(locator, _logger)

        if not session_ok:
            print(f"[Worker {worker_id}] Could not initialize session after {SESSION_INIT_RETRIES} attempts — worker exiting.", flush=True)
            # Re-queue all pending pages back so other workers can handle them
            while True:
                try:
                    _p = page_queue.get_nowait()
                    page_queue.task_done()
                except Exception:
                    break
            return

        while True:
            try:
                page_num = page_queue.get(timeout=2)
            except Empty:
                break

            if page_num is None:
                page_queue.task_done()
                continue

            # Verify driver session is still alive before processing the page
            if not is_session_valid(driver):
                print(f"  [Worker {worker_id}] Dead session detected before page {page_num} — restarting driver...", flush=True)
                driver = restart_driver(driver, headless=headless)
                if driver:
                    try:
                        driver = navigate_with_retries(driver, BASE_URL, wait_grid_loaded, f"worker {worker_id} session-recover", headless=headless)
                        set_rows_per_page(driver, rows_per_page)
                        wait_grid_loaded(driver, 30)
                    except Exception as _e:
                        print(f"  [Worker {worker_id}] Session recovery failed: {_e} — skipping page {page_num}", flush=True)
                        page_queue.task_done()
                        continue

            print(f"\n[Worker {worker_id}] Processing page {page_num}", flush=True)


            success = False
            for attempt in range(1, MAX_RETRIES_PER_PAGE + 1):
                try:
                    # Navigate to page using direct pager URL with pageSize parameter
                    pager_url = f"https://lekovi.zdravstvo.gov.mk/drugsregister.grid.pager/{page_num}/grid_0?t:ac=overview&pageSize={rows_per_page}"
                    driver = navigate_with_retries(driver, pager_url, wait_grid_loaded, f"page {page_num}", headless=headless, state_machine=state_machine)

                    # Ensure page size is set - re-apply if needed
                    current_page_size = get_rows_per_page_value(driver)
                    if current_page_size != rows_per_page:
                        print(f"  [Worker {worker_id}] Page size is {current_page_size}, setting to {rows_per_page}...", flush=True)
                        set_rows_per_page(driver, rows_per_page)
                        wait_grid_loaded(driver, 30)
                        # Re-navigate to the same page after changing page size
                        driver = navigate_with_retries(driver, pager_url, wait_grid_loaded, f"page {page_num} retry", headless=headless, state_machine=state_machine)

                    # Validate row count
                    expected_row_count = int(rows_per_page) if rows_per_page.isdigit() else 200
                    is_last_page = (total_pages > 0 and page_num == total_pages)

                    print(f"  [Worker {worker_id}] Validating row count (expecting {expected_row_count})...", flush=True)

                    # Check current row count
                    actual_count = count_grid_rows(driver)

                    # If row count is very low (default 10), the page size wasn't applied - reapply it
                    if actual_count <= 10 and not is_last_page:
                        print(f"  [Worker {worker_id}] Low row count ({actual_count}), re-applying page size...", flush=True)
                        set_rows_per_page(driver, rows_per_page)
                        wait_grid_loaded(driver, 30)
                        time.sleep(2)  # Extra wait for grid refresh

                    if not wait_for_correct_row_count(driver, expected_row_count, max_wait_seconds=30, is_last_page=is_last_page):
                        actual_count = count_grid_rows(driver)

                        if not is_last_page and actual_count < 50:
                            print(f"  [Worker {worker_id}] Page {page_num} has insufficient rows ({actual_count}), retrying...", flush=True)
                            raise RuntimeError(f"Insufficient rows: {actual_count}/{expected_row_count}")

                        print(f"  [Worker {worker_id}] Proceeding with {actual_count} rows", flush=True)

                    # Add human-like pause
                    if HUMAN_ACTIONS_AVAILABLE:
                        pause(0.3, 0.8)

                    # Extract URLs
                    detail_urls = extract_detail_url_list_from_current_grid(driver, state_machine=state_machine)

                    # Filter out already seen URLs (thread-safe)
                    with seen_urls_lock:
                        new_urls = [u for u in detail_urls if u not in seen_urls]
                        seen_urls.update(new_urls)

                    if new_urls:
                        db_rows = [{"detail_url": u, "page_num": page_num, "status": "pending"} for u in new_urls]
                        repo.insert_urls(db_rows)
                        if repo and repo.run_id:
                            audit_log("INSERT_BATCH", scraper_name="NorthMacedonia", run_id=repo.run_id, details={"inserted": len(db_rows), "page": page_num, "total_found_on_page": len(detail_urls)})

                    print(f"  [Worker {worker_id}] Page {page_num}: {len(detail_urls)} URLs found, {len(new_urls)} new", flush=True)

                    # Update progress in DB (step 1)
                    repo.mark_progress(1, "collect_urls", str(page_num), "completed")

                    # Update progress
                    with progress_lock:
                        progress["done"] += 1
                        progress["new_urls"] += len(new_urls)
                        pct = round((progress["done"] / progress["total"]) * 100, 1)
                        print(f"[PROGRESS] {progress['done']}/{progress['total']} ({pct}%) pages - {progress['new_urls']} total new URLs", flush=True)

                    success = True
                    break

                except Exception as e:
                    print(f"  [Worker {worker_id}] Attempt {attempt}/{MAX_RETRIES_PER_PAGE} failed for page {page_num}: {e}", flush=True)
                    if attempt == MAX_RETRIES_PER_PAGE:
                        # Mark as failed in DB
                        repo.mark_progress(1, "collect_urls", str(page_num), "failed", error_message=str(e))
                        print(f"  [Worker {worker_id}] Page {page_num} marked as FAILED in DB", flush=True)
                    else:
                        # Restart driver on failure and reinitialize session
                        try:
                            driver = restart_driver(driver, headless=headless)
                            if STATE_MACHINE_AVAILABLE:
                                import logging
                                logger = logging.getLogger(f"worker_{worker_id}")
                                locator = SmartLocator(driver, logger=logger)
                                state_machine = NavigationStateMachine(locator, logger=logger)
                            # Reinitialize session with page size
                            driver = navigate_with_retries(driver, BASE_URL, wait_grid_loaded, f"worker {worker_id} reinit", headless=headless, state_machine=state_machine)
                            set_rows_per_page(driver, rows_per_page)
                            wait_grid_loaded(driver, 30)
                            print(f"  [Worker {worker_id}] Session reinitialized", flush=True)
                        except Exception:
                            pass
                        time.sleep(2)

            page_queue.task_done()

    finally:
        shutdown_driver(driver)
        print(f"[Worker {worker_id}] Stopped", flush=True)


def main(headless: bool = True, rows_per_page: str = "200") -> None:
    if terminate_scraper_pids:
        try:
            terminate_scraper_pids("NorthMacedonia", _repo_root, silent=True)
        except Exception:
            pass

    # Initialize Repository (DB-ONLY MODE)
    repo = None
    run_id = os.environ.get("NORTH_MACEDONIA_RUN_ID", "")
    
    # Force localized DB import
    try:
        if "db" in sys.modules:
            del sys.modules["db"]
        from core.db.connection import CountryDB
        from db.repositories import NorthMacedoniaRepository
        
        if not run_id:
             # Try to find recent run_id if not in env
             run_id_file = get_output_dir() / ".current_run_id"
             if run_id_file.exists():
                 run_id = run_id_file.read_text(encoding="utf-8").strip()
        
        if not run_id:
            print("[ERROR] No run_id found. Run Step 0 (backup and clean) first.", flush=True)
            return

        db = CountryDB("NorthMacedonia")
        repo = NorthMacedoniaRepository(db, run_id)
        repo.ensure_run_in_ledger(mode="resume")
        print(f"[DB] Connected (run_id: {run_id})\n", flush=True)
    except (ImportError, ModuleNotFoundError):
        try:
             # Fallback to absolute import
             from scripts.north_macedonia.db.repositories import NorthMacedoniaRepository
             
             if not run_id:
                  run_id_file = get_output_dir() / ".current_run_id"
                  if run_id_file.exists():
                      run_id = run_id_file.read_text(encoding="utf-8").strip()

             if not run_id:
                 print("[ERROR] No run_id found. Run Step 0 (backup and clean) first.", flush=True)
                 return

             db = CountryDB("NorthMacedonia")
             repo = NorthMacedoniaRepository(db, run_id)
             repo.ensure_run_in_ledger(mode="resume")
             print(f"[DB] Connected (run_id: {run_id}) [FALLBACK PATH]\n", flush=True)
        except Exception as e:
             raise e
    except Exception as e:
        print(f"[DB ERROR] Database connection failed: {e}", flush=True)
        print("[CRITICAL] Project-wide mandate: CSV references removed. DB is required.", flush=True)
        return

    # Initialize Telegram notifier for status updates
    telegram_notifier = None
    if TELEGRAM_NOTIFIER_AVAILABLE:
        try:
            telegram_notifier = TelegramNotifier("NorthMacedonia", rate_limit=30.0)
            if telegram_notifier.enabled:
                telegram_notifier.send_started("Collect URLs - Step 1/4")
                print("[INFO] Telegram notifications enabled", flush=True)
        except Exception as e:
            print(f"[WARN] Failed to initialize Telegram notifier: {e}", flush=True)
            telegram_notifier = None

    # Load seen URLs from DB
    seen_urls = set()
    try:
        with db.cursor() as cur:
            cur.execute("SELECT detail_url FROM nm_urls WHERE run_id = %s", (run_id,))
            seen_urls = {row[0] for row in cur.fetchall()}
        print(f"[DB] Loaded {len(seen_urls)} existing URLs from database", flush=True)
    except Exception as e:
        print(f"[DB WARN] Could not load existing URLs: {e}", flush=True)

    # MEMORY FIX: Track set for monitoring
    try:
        from core.monitoring.memory_leak_detector import track_set
        track_set("north_macedonia_seen_urls", seen_urls, max_size=100000)
    except Exception:
        pass

    # Log run_id at start
    print("\n" + "="*60, flush=True)
    print("URL COLLECTION - SELENIUM VERSION (DB-ONLY)", flush=True)
    print(f"RUN ID: {run_id}", flush=True)
    print("="*60, flush=True)

    # First, determine total pages with a single driver
    print("\n[INIT] Determining total pages...", flush=True)
    driver = build_driver(headless=headless)
    if driver is None:
        raise RuntimeError("Failed to initialize overview Chrome driver")

    # Initialize state machine and smart locator for Tier 1 robustness
    state_machine = None
    locator = None
    if STATE_MACHINE_AVAILABLE:
        import logging
        logger = logging.getLogger(__name__)
        locator = SmartLocator(driver, logger=logger)
        state_machine = NavigationStateMachine(locator, logger=logger)

    try:
        driver = navigate_with_retries(driver, BASE_URL, wait_grid_loaded, "initial page", headless=headless, state_machine=state_machine)

        initial_total_records = get_total_records(driver)
        total_records = initial_total_records

        applied = set_rows_per_page(driver, rows_per_page)
        # Ensure rows-per-page selection applied before computing total pages
        for _ in range(10):
            current_val = get_rows_per_page_value(driver)
            if current_val == rows_per_page:
                applied = True
                break
            pause(0.2, 0.4)
            wait_grid_loaded(driver, 20)

        pager_total = None
        if applied:
            print(f"[INFO] Waiting up to 30s for pager to update after rows-per-page change ({rows_per_page})...", flush=True)
            deadline = time.time() + 30
            while time.time() < deadline:
                try:
                    wait_grid_loaded(driver, 20)
                except Exception:
                    pass
                rows_now = get_rows_per_page_value(driver)
                pager_total = get_total_pages(driver, 1)
                records_now = get_total_records(driver)
                if records_now:
                    total_records = records_now
                if rows_now == rows_per_page and records_now:
                    try:
                        rows_int = int(rows_per_page)
                        pager_total = math.ceil(int(records_now) / rows_int)
                        break
                    except Exception:
                        pass
                time.sleep(3)

        # Calculate total pages
        total_pages = pager_total if pager_total else get_total_pages(driver, 1)
        if total_records and rows_per_page.isdigit():
            try:
                expected_pages = math.ceil(int(total_records) / int(rows_per_page))
                if expected_pages:
                    total_pages = expected_pages
            except Exception:
                pass
        
        # Override if env var exists
        env_override = os.environ.get("SCRIPT_01_TOTAL_PAGES", "")
        if env_override:
            try:
                total_pages = int(env_override)
            except Exception:
                pass

        print(f"[INIT] Total pages: {total_pages}, Total records: {total_records}", flush=True)

    finally:
        shutdown_driver(driver)

    if not total_pages or total_pages <= 0:
        print("[ERROR] Could not determine total pages", flush=True)
        return

    # Build list of pages to process from DB progress
    all_pages = list(range(1, total_pages + 1))
    completed_pages_keys = repo.get_completed_keys(1)
    completed_pages = set(int(k) for k in completed_pages_keys if k.isdigit())
    
    pending_pages = [p for p in all_pages if p not in completed_pages]

    print(f"\n[STATUS] Pages completed (from DB): {len(completed_pages)}", flush=True)
    print(f"[STATUS] Pages pending: {len(pending_pages)}", flush=True)
    print(f"[STATUS] Workers: {NUM_WORKERS}", flush=True)

    if not pending_pages:
        print("\n[COMPLETE] All pages already scraped in database!", flush=True)
        # Send Telegram success notification
        if telegram_notifier:
            try:
                details = f"Total URLs: {len(seen_urls)}\nAll {total_pages} pages complete"
                telegram_notifier.send_success("URL Collection Already Complete", details=details)
            except Exception:
                pass
        return

    print(f"\n[START] Processing {len(pending_pages)} pages with {NUM_WORKERS} workers...\n", flush=True)

    # Setup threading
    page_queue: Queue = Queue()
    for p in pending_pages:
        page_queue.put(p)

    seen_urls_lock = threading.Lock()
    progress_lock = threading.Lock()

    progress = {"done": 0, "total": len(pending_pages), "new_urls": 0}

    # Record started run in audit log if available
    try:
        audit_log("RUN_STARTED", scraper_name="NorthMacedonia", run_id=run_id, details={"step": "01_collect_urls", "workers": NUM_WORKERS})
    except Exception:
        pass

    # Start worker threads
    threads = []
    for i in range(NUM_WORKERS):
        t = threading.Thread(
            target=worker_fn,
            args=(i+1, page_queue, seen_urls, seen_urls_lock,
                  progress, progress_lock, headless, rows_per_page, total_pages, repo)
        )
        t.start()
        threads.append(t)
        time.sleep(0.5)  # Stagger worker starts

    # Wait for all workers to complete
    for t in threads:
        t.join()

    # Final statistics from DB
    try:
        final_count = repo.get_url_count()
        completed_count = len(repo.get_completed_keys(1))
        
        print(f"\n{'='*60}", flush=True)
        print(f"URL COLLECTION COMPLETED", flush=True)
        print(f"{'='*60}", flush=True)
        print(f"Total unique detail URLs in DB: {final_count}", flush=True)
        print(f"Completed pages: {completed_count}/{total_pages}", flush=True)

        if completed_count < total_pages:
            print(f"\n[WARNING] {total_pages - completed_count} pages did not complete successfully.", flush=True)
            if telegram_notifier:
                 telegram_notifier.send_warning("URL Collection Completed with Issues", f"Completed: {completed_count}/{total_pages}")
        else:
            print("\n[SUCCESS] All pages extracted successfully.", flush=True)
            if telegram_notifier:
                 telegram_notifier.send_success("URL Collection Completed", f"Total URLs: {final_count}")

    except Exception as e:
        print(f"[ERROR] Could not fetch final stats: {e}", flush=True)

    print("="*60 + "\n", flush=True)

    if terminate_scraper_pids:
        try:
            terminate_scraper_pids("NorthMacedonia", _repo_root, silent=True)
        except Exception:
            pass

if __name__ == "__main__":
    headless = getenv_bool("SCRIPT_01_HEADLESS", True)
    rows_per_page = getenv("SCRIPT_01_ROWS_PER_PAGE", "200")
    main(headless=headless, rows_per_page=rows_per_page)
