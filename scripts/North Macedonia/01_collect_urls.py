import os
import json
import time
import sys
import math
from pathlib import Path
from typing import List, Dict, Optional

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, StaleElementReferenceException
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

# Add script directory to path for config_loader import
_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

# Add repo root for core imports
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

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

# Import Chrome PID tracking utilities
try:
    from core.chrome_pid_tracker import get_chrome_pids_from_driver, save_chrome_pids, terminate_scraper_pids
except ImportError:
    get_chrome_pids_from_driver = None
    save_chrome_pids = None
    terminate_scraper_pids = None

try:
    from core.chrome_manager import get_chromedriver_path as _core_get_chromedriver_path, register_chrome_driver, unregister_chrome_driver
    CORE_CHROMEDRIVER_AVAILABLE = True
except ImportError:
    CORE_CHROMEDRIVER_AVAILABLE = False
    _core_get_chromedriver_path = None
    register_chrome_driver = None
    unregister_chrome_driver = None

# Import stealth profile for anti-detection
try:
    from core.stealth_profile import apply_selenium
    STEALTH_PROFILE_AVAILABLE = True
except ImportError:
    STEALTH_PROFILE_AVAILABLE = False
    def apply_selenium(options):
        pass  # Stealth profile not available, skip

# Import browser observer for idle detection
try:
    from core.browser_observer import observe_selenium, wait_until_idle
    BROWSER_OBSERVER_AVAILABLE = True
except ImportError:
    BROWSER_OBSERVER_AVAILABLE = False
    def observe_selenium(driver):
        return None
    def wait_until_idle(state, timeout=10.0):
        pass  # Browser observer not available, skip

# Import human pacing
try:
    from core.human_actions import pause
    HUMAN_ACTIONS_AVAILABLE = True
except ImportError:
    HUMAN_ACTIONS_AVAILABLE = False
    def pause(min_s=0.2, max_s=0.6):
        import random
        time.sleep(random.uniform(min_s, max_s))

# Import Telegram notifier for status updates
try:
    from core.telegram_notifier import TelegramNotifier
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
URLS_CSV = getenv("SCRIPT_01_URLS_CSV", "north_macedonia_detail_urls.csv")
CHECKPOINT_JSON = getenv("SCRIPT_01_CHECKPOINT_JSON", "mk_urls_checkpoint.json")
TOTAL_PAGES_OVERRIDE = getenv("SCRIPT_01_TOTAL_PAGES", "")

# Navigation retry settings
NAV_RETRIES = getenv_int("SCRIPT_01_NAV_RETRIES", 3) if USE_CONFIG else 3
NAV_RETRY_SLEEP = getenv_float("SCRIPT_01_NAV_RETRY_SLEEP", 5.0) if USE_CONFIG else 5.0
PAGE_LOAD_TIMEOUT = getenv_int("SCRIPT_01_PAGE_LOAD_TIMEOUT", 120) if USE_CONFIG else 120
WAIT_TIMEOUT = getenv_int("SCRIPT_01_WAIT_TIMEOUT", 40) if USE_CONFIG else 40

_driver_path = None
_driver_path_lock = None


def _get_chromedriver_path() -> Optional[str]:
    """Get ChromeDriver path with offline fallback support."""
    global _driver_path
    global _driver_path_lock
    if _driver_path_lock is None:
        import threading
        _driver_path_lock = threading.Lock()
    with _driver_path_lock:
        if _driver_path:
            return _driver_path
        # Use core module's offline-capable function if available
        if CORE_CHROMEDRIVER_AVAILABLE and _core_get_chromedriver_path:
            try:
                _driver_path = _core_get_chromedriver_path()
                return _driver_path
            except Exception:
                pass
        # Fallback to direct ChromeDriverManager
        try:
            _driver_path = ChromeDriverManager().install()
        except Exception:
            return None
        return _driver_path


def build_driver(headless: bool = True) -> Optional[webdriver.Chrome]:
    """Build Chrome driver with enhanced anti-bot features."""
    options = webdriver.ChromeOptions()

    # Apply stealth profile if available
    if STEALTH_PROFILE_AVAILABLE:
        apply_selenium(options)

    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1600,1000")
    options.add_argument("--lang=mk-MK")

    # Stability improvements to prevent Chrome crashes
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-plugins")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-translate")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-sync")
    options.add_argument("--disable-default-apps")
    options.add_argument("--mute-audio")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-hang-monitor")
    options.add_argument("--disable-prompt-on-repost")
    options.add_argument("--disable-client-side-phishing-detection")
    options.add_argument("--disable-component-update")
    options.add_argument("--disable-breakpad")
    options.add_argument("--remote-debugging-port=0")

    # Enhanced anti-detection options for bot bypass
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    options.add_experimental_option('useAutomationExtension', False)

    # Use a realistic user agent
    user_agent = getenv("SCRIPT_01_CHROME_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    options.add_argument(f"--user-agent={user_agent}")

    # Disable images/CSS for faster loads
    disable_images = getenv_bool("SCRIPT_01_DISABLE_IMAGES", True)
    disable_css = getenv_bool("SCRIPT_01_DISABLE_CSS", True)
    prefs = {
        "profile.default_content_setting_values.notifications": 2,
    }
    if disable_images:
        prefs["profile.managed_default_content_settings.images"] = 2
    if disable_css:
        prefs["profile.managed_default_content_settings.stylesheets"] = 2
    options.add_experimental_option("prefs", prefs)

    driver_path = _get_chromedriver_path()
    if not driver_path:
        return None
    try:
        service = ChromeService(driver_path)
        driver = webdriver.Chrome(service=service, options=options)
    except Exception:
        return None
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)

    # Execute CDP commands to hide webdriver property and other automation indicators
    try:
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });

                // Override the plugins property to use a custom getter
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });

                // Override the languages property
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['mk-MK', 'mk', 'en-US', 'en']
                });

                // Override permissions
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );

                // Mock chrome object
                window.chrome = {
                    runtime: {}
                };
            '''
        })
    except Exception:
        pass  # CDP commands not critical, continue without them

    # Register driver with Chrome manager for cleanup
    if register_chrome_driver:
        try:
            register_chrome_driver(driver)
        except Exception:
            pass

    # Track Chrome PIDs
    if get_chrome_pids_from_driver and save_chrome_pids:
        try:
            pids = get_chrome_pids_from_driver(driver)
            if pids:
                save_chrome_pids("NorthMacedonia", _repo_root, pids)
        except Exception:
            pass
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


def read_checkpoint() -> Dict:
    """Return last scraped page number and page-level tracking; default to page 1."""
    checkpoint_path = OUTPUT_DIR / CHECKPOINT_JSON
    default_checkpoint = {
        "page": 1,
        "total_pages": 0,
        "pages": {},  # page_num -> {status, urls_extracted, error}
        "failed_pages": []  # List of page numbers that failed extraction
    }

    if checkpoint_path.exists():
        try:
            with open(checkpoint_path, "r", encoding="utf-8") as f:
                data = json.load(f)

                # Migrate old format to new format if needed
                if isinstance(data, dict):
                    if "page" not in data:
                        data["page"] = 1
                    if "pages" not in data:
                        data["pages"] = {}
                    if "failed_pages" not in data:
                        data["failed_pages"] = []
                    if "total_pages" not in data:
                        data["total_pages"] = 0

                    # Ensure page is an int
                    data["page"] = int(data.get("page", 1))
                    return data
        except Exception:
            pass
    return default_checkpoint


def write_checkpoint(page_num: int, total_pages: int = 0, pages_info: dict = None, failed_pages: list = None) -> None:
    """
    Persist checkpoint with page-level tracking.

    Args:
        page_num: Last scraped page number
        total_pages: Total number of pages detected
        pages_info: Dict mapping page_num -> {status, urls_extracted, error}
        failed_pages: List of page numbers that failed extraction
    """
    checkpoint_path = OUTPUT_DIR / CHECKPOINT_JSON

    # Load existing checkpoint to preserve page history
    existing = read_checkpoint()

    payload = {
        "page": int(page_num),
        "total_pages": int(total_pages),
        "pages": pages_info or existing.get("pages", {}),
        "failed_pages": failed_pages or existing.get("failed_pages", [])
    }

    with open(checkpoint_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def validate_page_completeness(checkpoint: dict, start_page: int, end_page: int) -> List[int]:
    """
    Validate that all pages in range are marked as complete.
    Returns list of page numbers that need to be re-extracted.

    Args:
        checkpoint: Checkpoint dict from read_checkpoint()
        start_page: Starting page number
        end_page: Ending page number (typically last completed page)

    Returns:
        List of page numbers that are missing or incomplete
    """
    pages_to_reextract = []
    pages_info = checkpoint.get("pages", {})

    for page_num in range(start_page, end_page + 1):
        page_key = str(page_num)  # JSON keys are strings

        # Check if page info exists
        if page_key not in pages_info:
            pages_to_reextract.append(page_num)
            continue

        page_data = pages_info[page_key]
        status = page_data.get("status", "")

        # Check if page is marked as incomplete or failed
        if status != "complete":
            pages_to_reextract.append(page_num)
            continue

        # Check if page has 0 URLs (suspicious - likely failed silently)
        urls_extracted = page_data.get("urls_extracted", 0)
        if urls_extracted == 0:
            pages_to_reextract.append(page_num)

    return pages_to_reextract


def load_existing_detail_urls(path: Path) -> set:
    if not path.exists():
        return set()
    try:
        df = pd.read_csv(path, usecols=["detail_url"], dtype=str)
        return set(df["detail_url"].dropna().astype(str).tolist())
    except Exception:
        return set()


def ensure_csv_has_header(path: Path, columns: List[str]) -> None:
    if not path.exists():
        pd.DataFrame([], columns=columns).to_csv(str(path), index=False, encoding="utf-8-sig")


def append_urls(path: Path, rows: List[Dict[str, str]]) -> None:
    if not rows:
        return
    df = pd.DataFrame(rows)
    df.to_csv(str(path), mode="a", header=False, index=False, encoding="utf-8-sig")


def main(headless: bool = True, rows_per_page: str = "200") -> None:
    if terminate_scraper_pids:
        try:
            terminate_scraper_pids("NorthMacedonia", _repo_root, silent=True)
        except Exception:
            pass

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

    urls_path = OUTPUT_DIR / URLS_CSV
    ensure_csv_has_header(urls_path, ["detail_url", "page_num", "detailed_view_scraped"])

    checkpoint = read_checkpoint()
    seen_urls = set(load_existing_detail_urls(urls_path))
    page_num = int(checkpoint.get("page", 1))

    # Initialize page-level tracking
    pages_info = checkpoint.get("pages", {})
    failed_pages = checkpoint.get("failed_pages", [])

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

        initial_total_pages = get_total_pages(driver, page_num)
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
                pager_total = get_total_pages(driver, page_num)
                records_now = get_total_records(driver)
                if records_now:
                    total_records = records_now
                if rows_now == rows_per_page and pager_total and pager_total != initial_total_pages:
                    break
                if rows_now == rows_per_page and records_now:
                    try:
                        rows_int = int(rows_per_page)
                        pager_total = math.ceil(int(records_now) / rows_int)
                        break
                    except Exception:
                        pass
                time.sleep(3)
        else:
            print(f"[WARN] Could not confirm rows-per-page={rows_per_page}; continuing with current grid size.", flush=True)

        # Move to checkpoint page
        current_page = 1
        while current_page < page_num:
            ok = click_next_page(driver)
            if not ok:
                break
            current_page += 1

        total_new = 0
        # Try to read total pages from pager after rows-per-page is applied
        total_pages = pager_total if pager_total else get_total_pages(driver, page_num)
        expected_pages = None
        if total_records and rows_per_page.isdigit():
            try:
                expected_pages = math.ceil(int(total_records) / int(rows_per_page))
            except Exception:
                expected_pages = None
        if expected_pages:
            if total_pages and total_pages != expected_pages:
                print(f"[INFO] Pager pages mismatch; using computed pages from total_records: pager={total_pages}, computed={expected_pages}", flush=True)
            total_pages = expected_pages
        if TOTAL_PAGES_OVERRIDE:
            try:
                total_pages = int(TOTAL_PAGES_OVERRIDE)
            except Exception:
                pass
        current_rows = get_rows_per_page_value(driver)
        if total_pages:
            print(f"[INFO] Grid ready: rows_per_page={current_rows or 'unknown'}, total_pages={total_pages}, total_records={initial_total_records or 'unknown'}", flush=True)
        else:
            print(f"[INFO] Grid ready: rows_per_page={current_rows or 'unknown'}, total_pages=unknown (will rely on pager navigation), total_records={initial_total_records or 'unknown'}", flush=True)

        # Ensure we start from page 1 with the selected page size applied (avoid 10-row first page)
        try:
            driver = navigate_with_retries(driver, BASE_URL, wait_grid_loaded, "reload for page size", headless=headless, state_machine=state_machine)
            if get_rows_per_page_value(driver) != rows_per_page:
                set_rows_per_page(driver, rows_per_page)
                wait_grid_loaded(driver, 20)
            total_pages = get_total_pages(driver, 1) or total_pages
        except Exception:
            print("[WARN] Reload after page-size change failed; continuing with current session.", flush=True)

        # Validate page completeness if resuming
        pages_to_reextract = []
        if page_num > 1 and checkpoint.get("page", 1) > 1:
            print("\n[VALIDATION] Checking completeness of previously scraped pages...", flush=True)
            missing_pages = validate_page_completeness(checkpoint, 1, page_num - 1)
            if missing_pages:
                print(f"[VALIDATION] Found {len(missing_pages)} incomplete pages: {missing_pages[:10]}{'...' if len(missing_pages) > 10 else ''}", flush=True)
                pages_to_reextract.extend(missing_pages)
            else:
                print(f"[VALIDATION] All {page_num - 1} pages are complete", flush=True)

        # Remove duplicates and sort
        pages_to_reextract = sorted(set(pages_to_reextract))

        while True:
            # Check session health before extracting
            if not is_session_valid(driver):
                print("  [WARN] Chrome session invalid, restarting browser...", flush=True)

                # Send Telegram warning notification
                if telegram_notifier:
                    try:
                        telegram_notifier.send_warning(
                            "Chrome session restarted",
                            details=f"Restarting at page {page_num}",
                            force=True
                        )
                    except Exception:
                        pass

                driver = restart_driver(driver, headless=headless)
                if STATE_MACHINE_AVAILABLE:
                    locator = SmartLocator(driver, logger=logger)
                    state_machine = NavigationStateMachine(locator, logger=logger)
                driver = navigate_with_retries(driver, BASE_URL, wait_grid_loaded, "session restart", headless=headless, state_machine=state_machine)
                if get_rows_per_page_value(driver) != rows_per_page:
                    set_rows_per_page(driver, rows_per_page)

            # Make sure page size sticks across navigations
            if get_rows_per_page_value(driver) != rows_per_page:
                set_rows_per_page(driver, rows_per_page)
                wait_grid_loaded(driver, 20)
                total_pages = get_total_pages(driver, page_num) or total_pages
            if total_records and rows_per_page.isdigit():
                try:
                    computed_pages = math.ceil(int(total_records) / int(rows_per_page))
                    if computed_pages and computed_pages != total_pages:
                        total_pages = computed_pages
                except Exception:
                    pass
            if total_pages is None:
                total_pages = get_total_pages(driver, page_num)

            # Add human-like pause
            if HUMAN_ACTIONS_AVAILABLE:
                pause(0.3, 0.8)

            try:
                detail_urls = extract_detail_url_list_from_current_grid(driver, state_machine=state_machine)
                new_urls = [u for u in detail_urls if u not in seen_urls]

                if new_urls:
                    rows = [{"detail_url": u, "page_num": page_num, "detailed_view_scraped": "no"} for u in new_urls]
                    append_urls(urls_path, rows)
                    seen_urls.update(new_urls)
                    total_new += len(new_urls)
                    if total_pages:
                        percent = round((page_num / total_pages) * 100, 1)
                        print(f"[PROGRESS] Collecting URLs: page {page_num}/{total_pages} ({percent}%) - new {len(new_urls)} (total {total_new})", flush=True)

                        # Send Telegram status update (rate-limited)
                        if telegram_notifier:
                            try:
                                telegram_notifier.send_progress(
                                    page_num,
                                    total_pages,
                                    "Collect URLs",
                                    details=f"New URLs: {len(new_urls)} | Total: {total_new}"
                                )
                            except Exception:
                                pass
                    else:
                        print(f"[PROGRESS] Collecting URLs: page {page_num} - new {len(new_urls)} (total {total_new})", flush=True)

                # Track page status
                pages_info[str(page_num)] = {
                    "status": "complete",
                    "urls_extracted": len(detail_urls),
                    "new_urls": len(new_urls)
                }

                # Remove from failed pages if it was there
                if page_num in failed_pages:
                    failed_pages.remove(page_num)

            except Exception as e:
                print(f"  [ERROR] Failed to extract page {page_num}: {e}", flush=True)
                pages_info[str(page_num)] = {
                    "status": "failed",
                    "urls_extracted": 0,
                    "error": str(e)
                }
                if page_num not in failed_pages:
                    failed_pages.append(page_num)

                # Send Telegram error notification
                if telegram_notifier:
                    try:
                        telegram_notifier.send_error(
                            f"Failed to extract page {page_num}",
                            details=str(e)[:200],
                            force=False  # Don't spam on every error
                        )
                    except Exception:
                        pass

            # Save checkpoint after each page with page-level tracking
            write_checkpoint(page_num, total_pages or 0, pages_info, failed_pages)

            # Advance page using direct pager URL if known (preferred)
            if total_pages and page_num < total_pages:
                next_page = page_num + 1
                pager_url = f"https://lekovi.zdravstvo.gov.mk/drugsregister.grid.pager/{next_page}/grid_0?t:ac=overview"
                try:
                    driver = navigate_with_retries(driver, pager_url, wait_grid_loaded, f"page {next_page}", headless=headless, state_machine=state_machine)
                    # Ensure page size persists
                    if get_rows_per_page_value(driver) != rows_per_page:
                        set_rows_per_page(driver, rows_per_page)
                        wait_grid_loaded(driver, 20)
                        total_pages = get_total_pages(driver, next_page) or total_pages
                    page_num = next_page
                    continue
                except Exception:
                    pass

            # Fallback to click navigation
            next_ok = click_next_page(driver)
            if not next_ok:
                break
            page_num += 1

        # Re-extract pages with missing or failed data
        if pages_to_reextract or failed_pages:
            all_reextract = sorted(set(pages_to_reextract + failed_pages))
            print(f"\n\n=== RE-EXTRACTION PHASE ===", flush=True)
            print(f"Re-extracting {len(all_reextract)} pages with missing/incomplete data...", flush=True)

            for idx, p in enumerate(all_reextract, 1):
                print(f"\n--- Re-extracting Page {p} ({idx}/{len(all_reextract)}) ---", flush=True)

                # Check session health
                if not is_session_valid(driver):
                    print("  [WARN] Chrome session invalid, restarting browser...", flush=True)
                    driver = restart_driver(driver, headless=headless)
                    if STATE_MACHINE_AVAILABLE:
                        locator = SmartLocator(driver, logger=logger)
                        state_machine = NavigationStateMachine(locator, logger=logger)
                    driver = navigate_with_retries(driver, BASE_URL, wait_grid_loaded, "session restart", headless=headless, state_machine=state_machine)
                    if get_rows_per_page_value(driver) != rows_per_page:
                        set_rows_per_page(driver, rows_per_page)

                try:
                    # Navigate to missing page
                    pager_url = f"https://lekovi.zdravstvo.gov.mk/drugsregister.grid.pager/{p}/grid_0?t:ac=overview"
                    try:
                        driver = navigate_with_retries(driver, pager_url, wait_grid_loaded, f"re-extract page {p}", headless=headless, state_machine=state_machine)
                    except Exception:
                        # Fallback: navigate to base and click through
                        driver = navigate_with_retries(driver, BASE_URL, wait_grid_loaded, "base for re-extract", headless=headless, state_machine=state_machine)
                        if get_rows_per_page_value(driver) != rows_per_page:
                            set_rows_per_page(driver, rows_per_page)
                        # Click to page
                        for _ in range(p - 1):
                            if not click_next_page(driver):
                                break

                    # Ensure page size persists
                    if get_rows_per_page_value(driver) != rows_per_page:
                        set_rows_per_page(driver, rows_per_page)
                        wait_grid_loaded(driver, 20)

                    # Add human-like pause
                    if HUMAN_ACTIONS_AVAILABLE:
                        pause(0.5, 1.0)

                    # Extract URLs
                    detail_urls = extract_detail_url_list_from_current_grid(driver, state_machine=state_machine)
                    new_urls = [u for u in detail_urls if u not in seen_urls]

                    print(f"Re-extract: Found URLs: {len(detail_urls)} | New URLs: {len(new_urls)}", flush=True)

                    if new_urls:
                        rows = [{"detail_url": u, "page_num": p, "detailed_view_scraped": "no"} for u in new_urls]
                        append_urls(urls_path, rows)
                        seen_urls.update(new_urls)
                        total_new += len(new_urls)

                    # Update page status
                    pages_info[str(p)] = {
                        "status": "complete",
                        "urls_extracted": len(detail_urls),
                        "new_urls": len(new_urls),
                        "reextracted": True
                    }

                    # Remove from failed pages
                    if p in failed_pages:
                        failed_pages.remove(p)

                except Exception as e:
                    print(f"  [ERROR] Re-extraction failed for page {p}: {e}", flush=True)
                    pages_info[str(p)] = {
                        "status": "failed",
                        "urls_extracted": 0,
                        "error": str(e),
                        "reextraction_failed": True
                    }

                # Save progress after each re-extraction
                write_checkpoint(p, total_pages or 0, pages_info, failed_pages)

                # Add small delay
                if HUMAN_ACTIONS_AVAILABLE:
                    pause(0.3, 0.8)

        print(f"\n{'='*60}", flush=True)
        print(f"URL COLLECTION COMPLETED", flush=True)
        print(f"{'='*60}", flush=True)
        print(f"Total unique detail URLs: {len(seen_urls)}", flush=True)
        print(f"Total new URLs added: {total_new}", flush=True)

        # Report on pages with issues
        if failed_pages:
            print(f"\n[WARNING] {len(failed_pages)} pages failed extraction:", flush=True)
            print(f"  Pages: {failed_pages[:20]}{'...' if len(failed_pages) > 20 else ''}", flush=True)
            print(f"  You may need to manually investigate these pages", flush=True)

        # Calculate completeness
        complete_pages = sum(1 for p_info in pages_info.values() if p_info.get("status") == "complete")
        total_pages_processed = len(pages_info)
        if total_pages_processed > 0:
            completeness_pct = round((complete_pages / total_pages_processed) * 100, 1)
            print(f"\nCompleteness: {complete_pages}/{total_pages_processed} pages ({completeness_pct}%)", flush=True)

        # Log metrics if state machine is available
        if STATE_MACHINE_AVAILABLE and locator:
            metrics = locator.get_metrics()
            metrics_summary = metrics.get_summary()
            print(f"[METRICS] Locator performance: {metrics_summary}", flush=True)

        # Success message
        if not failed_pages:
            print("\n[SUCCESS] All pages extracted successfully.", flush=True)
            # Send Telegram success notification
            if telegram_notifier:
                try:
                    details = f"Total URLs: {len(seen_urls)}\nNew URLs: {total_new}\nPages: {complete_pages}/{total_pages_processed}"
                    telegram_notifier.send_success("URL Collection Completed", details=details)
                except Exception:
                    pass
        else:
            print(f"\n[INFO] Progress file retained for {len(failed_pages)} pages with issues. Run again to retry.", flush=True)
            # Send Telegram warning notification
            if telegram_notifier:
                try:
                    details = f"Total URLs: {len(seen_urls)}\nNew URLs: {total_new}\nFailed pages: {len(failed_pages)}"
                    telegram_notifier.send_warning("URL Collection Completed with Issues", details=details)
                except Exception:
                    pass

    finally:
        shutdown_driver(driver)
        if terminate_scraper_pids:
            try:
                terminate_scraper_pids("NorthMacedonia", _repo_root, silent=True)
            except Exception:
                pass


if __name__ == "__main__":
    headless = getenv_bool("SCRIPT_01_HEADLESS", True)
    rows_per_page = getenv("SCRIPT_01_ROWS_PER_PAGE", "200")
    main(headless=headless, rows_per_page=rows_per_page)
