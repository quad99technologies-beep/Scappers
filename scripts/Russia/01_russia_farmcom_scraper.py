#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Russia Farmcom VED Registry Scraper

Scrapes drug pricing data from the Russian VED (Vital and Essential Drugs) registry
at farmcom.info for a specified region.

Features:
- Resume support: Saves progress after each page, resumes from last completed page
- Deduplication: Skips already scraped item_ids
- Configurable: All settings via Russia.env.json

Author: Enterprise PDF Processing Pipeline
License: Proprietary
"""

import os
import re
import csv
import sys
import time
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs
from pathlib import Path

# Force unbuffered output for real-time console updates
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
os.environ.setdefault('PYTHONUNBUFFERED', '1')

# Add repo root to path for imports
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add scripts/Russia to path for config_loader
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

# Try to load config, fallback to defaults if not available
try:
    from config_loader import (
        load_env_file, getenv, getenv_bool, getenv_int, getenv_float,
        get_input_dir, get_output_dir
    )
    load_env_file()
    USE_CONFIG = True
except ImportError:
    USE_CONFIG = False
    def getenv(key, default=""):
        return os.getenv(key, default)
    def getenv_bool(key, default=False):
        val = os.getenv(key, str(default))
        return str(val).lower() in ("true", "1", "yes", "on")
    def getenv_int(key, default=0):
        try:
            return int(os.getenv(key, str(default)))
        except (ValueError, TypeError):
            return default
    def getenv_float(key, default=0.0):
        try:
            return float(os.getenv(key, str(default)))
        except (ValueError, TypeError):
            return default
    def get_input_dir():
        return Path(__file__).parent
    def get_output_dir():
        return Path(__file__).parent

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, WebDriverException

from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.driver_cache import DriverCacheManager
from selenium.webdriver.chrome.service import Service as ChromeService
import shutil
from selenium.webdriver.chrome.options import Options as ChromeOptions

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

# Import Chrome tracking utilities
try:
    from core.chrome_pid_tracker import get_chrome_pids_from_driver, save_chrome_pids
except ImportError:
    get_chrome_pids_from_driver = None
    save_chrome_pids = None

try:
    from core.chrome_manager import register_chrome_driver, unregister_chrome_driver
except ImportError:
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
        pass  # Human pacing not available, skip

# Configuration from env or defaults
BASE_URL = getenv("SCRIPT_01_BASE_URL", "http://farmcom.info/site/reestr") if USE_CONFIG else "http://farmcom.info/site/reestr"

# Moscow region per your annotation (reg_id value=50)
REGION_VALUE = getenv("SCRIPT_01_REGION_VALUE", "50") if USE_CONFIG else "50"

# Output
if USE_CONFIG:
    OUT_DIR = get_output_dir()
    OUT_CSV = OUT_DIR / getenv("SCRIPT_01_OUTPUT_CSV", "russia_farmcom_ved_moscow_region.csv")
    PROGRESS_FILE = OUT_DIR / "russia_scraper_progress.json"
else:
    OUT_CSV = Path(__file__).parent / "russia_farmcom_ved_moscow_region.csv"
    PROGRESS_FILE = Path(__file__).parent / "russia_scraper_progress.json"

# Safety / stability
PAGE_LOAD_TIMEOUT = getenv_int("SCRIPT_01_PAGE_LOAD_TIMEOUT", 60) if USE_CONFIG else 60
WAIT_TIMEOUT = getenv_int("SCRIPT_01_WAIT_TIMEOUT", 30) if USE_CONFIG else 30
CLICK_RETRY = getenv_int("SCRIPT_01_CLICK_RETRY", 3) if USE_CONFIG else 3
SLEEP_BETWEEN_PAGES = getenv_float("SCRIPT_01_SLEEP_BETWEEN_PAGES", 0.3) if USE_CONFIG else 0.3
NAV_RETRIES = getenv_int("SCRIPT_01_NAV_RETRIES", 3) if USE_CONFIG else 3
NAV_RETRY_SLEEP = getenv_float("SCRIPT_01_NAV_RETRY_SLEEP", 5.0) if USE_CONFIG else 5.0
NAV_RESTART_DRIVER = getenv_bool("SCRIPT_01_NAV_RESTART_DRIVER", True) if USE_CONFIG else True

# EAN fetching - disabled by default as it's very slow (clicks popup for each row)
FETCH_EAN = getenv_bool("SCRIPT_01_FETCH_EAN", False) if USE_CONFIG else False
EAN_POPUP_TIMEOUT = getenv_int("SCRIPT_01_EAN_POPUP_TIMEOUT", 3) if USE_CONFIG else 3  # Short timeout for EAN popup

# Max pages to scrape (0 = all pages, >0 = limit to N pages)
MAX_PAGES = getenv_int("SCRIPT_01_MAX_PAGES", 0) if USE_CONFIG else 0


@dataclass
class RowData:
    item_id: str
    tn: str
    inn: str
    manufacturer_country: str
    release_form: str
    ean: str
    registered_price_rub: str
    start_date_text: str


def get_chromedriver_path() -> str:
    """
    Get ChromeDriver path with offline fallback.

    Strategy:
    1. Try to use cached ChromeDriver first (works offline)
    2. If no cache, try to download (requires internet)
    3. If download fails due to network, look for system chromedriver
    4. Raise clear error if all methods fail
    """
    import glob
    from pathlib import Path

    # Get the default cache directory used by webdriver_manager
    home = Path.home()
    wdm_cache_dir = home / ".wdm" / "drivers" / "chromedriver"

    def find_cached_chromedriver():
        """Find any cached chromedriver executable"""
        if wdm_cache_dir.exists():
            # Look for chromedriver executables in cache
            patterns = [
                str(wdm_cache_dir / "**" / "chromedriver.exe"),  # Windows
                str(wdm_cache_dir / "**" / "chromedriver"),      # Linux/Mac
            ]
            for pattern in patterns:
                matches = glob.glob(pattern, recursive=True)
                if matches:
                    # Sort by modification time, newest first
                    matches.sort(key=lambda x: os.path.getmtime(x), reverse=True)
                    return matches[0]
        return None

    def find_system_chromedriver():
        """Find chromedriver in system PATH or common locations"""
        # Check if chromedriver is in PATH
        chromedriver_in_path = shutil.which("chromedriver")
        if chromedriver_in_path:
            return chromedriver_in_path

        # Common installation locations on Windows
        common_paths = [
            Path(os.environ.get("LOCALAPPDATA", "")) / "Programs" / "chromedriver.exe",
            Path(os.environ.get("PROGRAMFILES", "")) / "chromedriver" / "chromedriver.exe",
            Path(os.environ.get("PROGRAMFILES(X86)", "")) / "chromedriver" / "chromedriver.exe",
            Path("C:/chromedriver/chromedriver.exe"),
            Path("C:/WebDriver/chromedriver.exe"),
        ]

        for path in common_paths:
            if path.exists():
                return str(path)

        return None

    # Strategy 1: Try cached chromedriver first (works offline)
    cached_path = find_cached_chromedriver()
    if cached_path:
        print(f"[ChromeDriver] Using cached driver: {cached_path}")
        return cached_path

    # Strategy 2: Try to download using webdriver_manager
    try:
        print("[ChromeDriver] No cache found, attempting to download...")
        driver_path = ChromeDriverManager().install()
        print(f"[ChromeDriver] Downloaded and installed: {driver_path}")
        return driver_path
    except Exception as e:
        error_msg = str(e).lower()
        if "offline" in error_msg or "connection" in error_msg or "resolve" in error_msg or "network" in error_msg:
            print(f"[ChromeDriver] Network unavailable: {e}")
        else:
            print(f"[ChromeDriver] Download failed: {e}")

    # Strategy 3: Look for system chromedriver
    system_path = find_system_chromedriver()
    if system_path:
        print(f"[ChromeDriver] Using system driver: {system_path}")
        return system_path

    # All strategies failed
    raise RuntimeError(
        "Could not find or download ChromeDriver.\n"
        "Options to fix:\n"
        "  1. Connect to the internet and retry\n"
        "  2. Download ChromeDriver manually from https://googlechromelabs.github.io/chrome-for-testing/\n"
        "     and place it in your PATH or C:/chromedriver/\n"
        "  3. Run with internet once to cache the driver for offline use"
    )


def make_driver(headless: bool = None) -> webdriver.Chrome:
    """Build Chrome driver with config support and enhanced anti-bot features"""
    if headless is None:
        # Get from config if available
        if USE_CONFIG:
            headless = getenv_bool("SCRIPT_01_HEADLESS", True)
        else:
            headless = True
    
    opts = ChromeOptions()

    # Apply stealth profile if available
    if STEALTH_PROFILE_AVAILABLE:
        apply_selenium(opts)

    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1600,1000")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--lang=en-US")

    # Stability improvements to prevent Chrome crashes
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-plugins")
    opts.add_argument("--disable-infobars")
    opts.add_argument("--disable-popup-blocking")
    opts.add_argument("--disable-translate")  # Disable Chrome's translation prompt
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-sync")
    opts.add_argument("--disable-default-apps")
    opts.add_argument("--mute-audio")
    opts.add_argument("--no-first-run")
    opts.add_argument("--no-default-browser-check")
    opts.add_argument("--disable-hang-monitor")
    opts.add_argument("--disable-prompt-on-repost")
    opts.add_argument("--disable-client-side-phishing-detection")
    opts.add_argument("--disable-component-update")
    opts.add_argument("--disable-breakpad")  # Disable crash reporter
    opts.add_argument("--remote-debugging-port=0")  # Prevent DevTools disconnection issues

    # Enhanced anti-detection options for bot bypass
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    opts.add_experimental_option('useAutomationExtension', False)
    
    # Use a realistic user agent
    user_agent = getenv("SCRIPT_01_CHROME_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36") if USE_CONFIG else "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    opts.add_argument(f"--user-agent={user_agent}")

    # Reduce noise / speed up
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.notifications": 2,
    }
    opts.add_experimental_option("prefs", prefs)
    
    # Additional Chrome options from config
    if USE_CONFIG:
        chrome_start_max = getenv("SCRIPT_01_CHROME_START_MAXIMIZED", "")
        if chrome_start_max and not headless:
            opts.add_argument(chrome_start_max)
        
        chrome_disable_automation = getenv("SCRIPT_01_CHROME_DISABLE_AUTOMATION", "")
        if chrome_disable_automation:
            opts.add_argument(chrome_disable_automation)

    service = ChromeService(get_chromedriver_path())
    driver = webdriver.Chrome(service=service, options=opts)
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
                    get: () => ['en-US', 'en', 'ru-RU', 'ru']
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
    except Exception as e:
        # CDP commands not critical, continue without them
        pass
    
    # Register driver with Chrome manager for cleanup
    if register_chrome_driver:
        register_chrome_driver(driver)
    
    # Track Chrome PIDs so the GUI can report active instances
    if get_chrome_pids_from_driver and save_chrome_pids:
        try:
            pids = get_chrome_pids_from_driver(driver)
            if pids:
                save_chrome_pids("Russia", _repo_root, pids)
        except Exception:
            pass
    
    return driver


def wait_for_table(driver: webdriver.Chrome) -> None:
    WebDriverWait(driver, WAIT_TIMEOUT).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table.report tbody tr"))
    )
    # Use browser observer if available
    if BROWSER_OBSERVER_AVAILABLE:
        state = observe_selenium(driver)
        wait_until_idle(state, timeout=5.0)
    # Add human-like pause
    if HUMAN_ACTIONS_AVAILABLE:
        pause()

def stop_page_load(driver: webdriver.Chrome) -> None:
    try:
        driver.execute_script("window.stop();")
    except Exception:
        pass


def remove_webdriver_property(driver: webdriver.Chrome) -> None:
    """Remove webdriver property after page load for additional stealth"""
    try:
        driver.execute_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)
    except Exception:
        pass  # Not critical, continue without it


def is_session_valid(driver: webdriver.Chrome) -> bool:
    """
    Check if the Chrome session is still valid and responsive.
    Returns True if the session is healthy, False if it needs to be restarted.
    """
    try:
        # Try a simple operation to check session validity
        _ = driver.current_url
        return True
    except Exception:
        return False




def shutdown_driver(driver: webdriver.Chrome) -> None:
    if unregister_chrome_driver:
        try:
            unregister_chrome_driver(driver)
        except Exception:
            pass
    try:
        driver.quit()
    except Exception:
        pass


def restart_driver(driver: webdriver.Chrome, headless: bool | None = None) -> webdriver.Chrome:
    shutdown_driver(driver)
    return make_driver(headless=headless)


def navigate_with_retries(
    driver: webdriver.Chrome,
    url: str,
    wait_fn,
    label: str,
    headless: bool | None = None,
    on_restart=None,
    state_machine=None,
) -> webdriver.Chrome:
    last_exc = None
    for attempt in range(1, NAV_RETRIES + 1):
        try:
            driver.get(url)
            # Additional stealth: Remove webdriver property after page load
            remove_webdriver_property(driver)
            # State validation: validate PAGE_LOADED state after navigation
            if STATE_MACHINE_AVAILABLE and state_machine:
                if not state_machine.transition_to(NavigationState.PAGE_LOADED, reload_on_failure=False):
                    print(f"  [WARN] {label}: Failed to validate PAGE_LOADED state", flush=True)
            wait_fn(driver)
            return driver
        except (TimeoutException, WebDriverException) as exc:
            last_exc = exc
            stop_page_load(driver)
            try:
                wait_fn(driver)
                return driver
            except Exception:
                pass
            if attempt < NAV_RETRIES:
                print(
                    f"  [WARN] {label} load failed (attempt {attempt}/{NAV_RETRIES}). Retrying in {NAV_RETRY_SLEEP}s...",
                    flush=True,
                )
                time.sleep(NAV_RETRY_SLEEP)

    if NAV_RESTART_DRIVER:
        print(f"  [WARN] {label} load failed; restarting Chrome session.", flush=True)
        driver = restart_driver(driver, headless=headless)
        if on_restart:
            driver = on_restart(driver) or driver
        for attempt in range(1, NAV_RETRIES + 1):
            try:
                driver.get(url)
                # Additional stealth: Remove webdriver property after page load
                remove_webdriver_property(driver)
                # State validation: validate PAGE_LOADED state after navigation
                if STATE_MACHINE_AVAILABLE and state_machine:
                    if not state_machine.transition_to(NavigationState.PAGE_LOADED, reload_on_failure=False):
                        print(f"  [WARN] {label}: Failed to validate PAGE_LOADED state after restart", flush=True)
                wait_fn(driver)
                return driver
            except (TimeoutException, WebDriverException) as exc:
                last_exc = exc
                stop_page_load(driver)
                try:
                    wait_fn(driver)
                    return driver
                except Exception:
                    pass
                if attempt < NAV_RETRIES:
                    print(
                        f"  [WARN] {label} load failed after restart (attempt {attempt}/{NAV_RETRIES}). Retrying in {NAV_RETRY_SLEEP}s...",
                        flush=True,
                    )
                    time.sleep(NAV_RETRY_SLEEP)

    if last_exc:
        raise last_exc
    raise RuntimeError(f"{label} load failed")


def select_region_and_search(
    driver: webdriver.Chrome, headless: bool | None = None, state_machine=None
) -> webdriver.Chrome:
    """
    Navigate to site, select region, and perform search.

    Args:
        driver: Chrome WebDriver instance
        headless: Whether running in headless mode
        state_machine: Optional navigation state machine
    """
    last_exc = None

    for attempt in range(1, NAV_RETRIES + 1):
        try:
            driver.get(BASE_URL)
            # Additional stealth: Remove webdriver property after page load
            remove_webdriver_property(driver)

            # State validation: validate PAGE_LOADED state after navigation
            if STATE_MACHINE_AVAILABLE and state_machine:
                if not state_machine.transition_to(NavigationState.PAGE_LOADED, reload_on_failure=True):
                    print("  [WARN] Failed to validate PAGE_LOADED state", flush=True)

            # select#reg_id exists on the page
            WebDriverWait(driver, WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.ID, "reg_id"))
            )

            Select(driver.find_element(By.ID, "reg_id")).select_by_value(REGION_VALUE)

            # click Find button (input#btn_submit)
            driver.find_element(By.ID, "btn_submit").click()

            # State validation: validate TABLE_READY state before extracting data
            if STATE_MACHINE_AVAILABLE and state_machine:
                table_ready_conditions = [
                    StateCondition(element_selector="table.report tbody tr", min_count=1, max_wait=WAIT_TIMEOUT)
                ]
                if not state_machine.transition_to(NavigationState.TABLE_READY, custom_conditions=table_ready_conditions, reload_on_failure=False):
                    print("  [WARN] Failed to validate TABLE_READY state, continuing with existing wait", flush=True)

            wait_for_table(driver)
            return driver
        except (TimeoutException, WebDriverException) as exc:
            last_exc = exc
            stop_page_load(driver)
            if attempt < NAV_RETRIES:
                print(
                    f"  [WARN] Region selection failed (attempt {attempt}/{NAV_RETRIES}). Retrying in {NAV_RETRY_SLEEP}s...",
                    flush=True,
                )
                time.sleep(NAV_RETRY_SLEEP)

    if NAV_RESTART_DRIVER:
        print("  [WARN] Region selection failed; restarting Chrome session.", flush=True)
        driver = restart_driver(driver, headless=headless)
        for attempt in range(1, NAV_RETRIES + 1):
            try:
                driver.get(BASE_URL)
                # Additional stealth: Remove webdriver property after page load
                remove_webdriver_property(driver)

                # State validation: validate PAGE_LOADED state after navigation
                if STATE_MACHINE_AVAILABLE and state_machine:
                    if not state_machine.transition_to(NavigationState.PAGE_LOADED, reload_on_failure=True):
                        print("  [WARN] Failed to validate PAGE_LOADED state after restart", flush=True)

                WebDriverWait(driver, WAIT_TIMEOUT).until(
                    EC.presence_of_element_located((By.ID, "reg_id"))
                )

                Select(driver.find_element(By.ID, "reg_id")).select_by_value(REGION_VALUE)
                driver.find_element(By.ID, "btn_submit").click()

                # State validation: validate TABLE_READY state before extracting data
                if STATE_MACHINE_AVAILABLE and state_machine:
                    table_ready_conditions = [
                        StateCondition(element_selector="table.report tbody tr", min_count=1, max_wait=WAIT_TIMEOUT)
                    ]
                    if not state_machine.transition_to(NavigationState.TABLE_READY, custom_conditions=table_ready_conditions, reload_on_failure=False):
                        print("  [WARN] Failed to validate TABLE_READY state after restart, continuing with existing wait", flush=True)

                wait_for_table(driver)
                return driver
            except (TimeoutException, WebDriverException) as exc:
                last_exc = exc
                stop_page_load(driver)
                if attempt < NAV_RETRIES:
                    print(
                        f"  [WARN] Region selection failed after restart (attempt {attempt}/{NAV_RETRIES}). Retrying in {NAV_RETRY_SLEEP}s...",
                        flush=True,
                    )
                    time.sleep(NAV_RETRY_SLEEP)

    if last_exc:
        raise last_exc
    raise RuntimeError("Region selection failed")


def get_last_page(driver: webdriver.Chrome) -> int:
    """
    Reads the last page number from the pagination block, e.g. link text "[1139]".
    If not found, falls back to scanning for the max 'page=' in pagination URLs.
    """
    # First try: anchor that contains [number]
    try:
        pager = driver.find_element(By.CSS_SELECTOR, "p.paging")
        a_tags = pager.find_elements(By.CSS_SELECTOR, "a")
        for a in a_tags:
            txt = (a.text or "").strip()
            m = re.match(r"^\[(\d+)\]$", txt)
            if m:
                return int(m.group(1))
    except Exception:
        pass

    # Fallback: parse page numbers from hrefs
    max_page = 1
    try:
        pager = driver.find_element(By.CSS_SELECTOR, "p.paging")
        a_tags = pager.find_elements(By.CSS_SELECTOR, "a[href*='page=']")
        for a in a_tags:
            href = a.get_attribute("href") or ""
            qs = parse_qs(urlparse(href).query)
            if "page" in qs and qs["page"]:
                try:
                    max_page = max(max_page, int(qs["page"][0]))
                except Exception:
                    pass
    except Exception:
        pass

    return max_page


def parse_item_id_from_linkhref(linkhref: str) -> str:
    # example: frm_reestr_det.php?value=279.24&MnnName=...&item_id=31908
    if not linkhref:
        return ""
    qs = parse_qs(urlparse("http://x/?" + linkhref.split("?", 1)[-1]).query)
    return (qs.get("item_id", [""]) or [""])[0]


def extract_price_and_date(cell_text: str) -> tuple[str, str]:
    """
    cell_text example:
      "531.51 \n03/15/2010 (1907-Pr/10)"
    or:
      "69.33\n07/18/2023 7/25-4258067-OPR-ism"
    """
    lines = [ln.strip() for ln in (cell_text or "").splitlines() if ln.strip()]
    if not lines:
        return "", ""
    price = lines[0].strip()
    date_text = " ".join(lines[1:]).strip() if len(lines) > 1 else ""
    return price, date_text


def safe_click(elem):
    for _ in range(CLICK_RETRY):
        try:
            elem.click()
            return
        except StaleElementReferenceException:
            time.sleep(0.2)
    elem.click()


def click_all_barcodes(driver: webdriver.Chrome) -> None:
    """Click all barcode icons to trigger EAN insertion into package column"""
    if not FETCH_EAN:
        return

    icons = driver.find_elements(By.CSS_SELECTOR, "a.info")
    if not icons:
        return

    print(f"  Clicking {len(icons)} barcode icons...", flush=True)

    clicked = 0
    for idx, ic in enumerate(icons):
        try:
            # Scroll into view first to ensure element is visible
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", ic)

            # Use onclick attribute directly if available
            onclick = ic.get_attribute("onclick")
            if onclick:
                driver.execute_script(onclick)
                clicked += 1
            else:
                driver.execute_script("arguments[0].click();", ic)
                clicked += 1

            # Small delay every 10 clicks to let async requests process
            if (idx + 1) % 10 == 0:
                time.sleep(0.1)

        except Exception:
            pass

    print(f"  Successfully clicked {clicked} barcode icons", flush=True)


def extract_ean(text: str) -> str:
    """Extract EAN digits from package text - finds longest digit sequence of 8-14 digits"""
    if not text:
        return ""

    # Remove all whitespace to normalize
    text_compact = re.sub(r"\s+", "", text)

    # Find all sequences of 8-14 digits
    matches = re.findall(r"\d{8,14}", text_compact)

    if not matches:
        return ""

    # Return the longest match (most likely to be EAN)
    return max(matches, key=len)


def extract_rows_from_page_with_retry(driver: webdriver.Chrome, page_num: int = 0, state_machine=None) -> tuple[list[RowData], int]:
    """
    Extract rows from current page with EAN retry mechanism.

    Returns:
        tuple: (rows, missing_ean_count)
            - rows: List of extracted RowData
            - missing_ean_count: Number of rows with missing EAN after retry
    """
    wait_for_table(driver)

    # State validation: validate TABLE_READY state before extracting data
    if STATE_MACHINE_AVAILABLE and state_machine:
        table_ready_conditions = [
            StateCondition(element_selector="table.report tbody tr", min_count=1, max_wait=WAIT_TIMEOUT)
        ]
        if not state_machine.transition_to(NavigationState.TABLE_READY, custom_conditions=table_ready_conditions, reload_on_failure=False):
            print(f"  [WARN] Page {page_num}: Failed to validate TABLE_READY state, continuing with extraction", flush=True)

    # First attempt: Extract data
    rows = _extract_rows_from_table(driver, page_num)

    # Check for missing EANs (only if FETCH_EAN is enabled)
    if FETCH_EAN and rows:
        missing_count = sum(1 for r in rows if not r.ean)

        if missing_count > 0:
            print(f"  [EAN RETRY] Found {missing_count} rows with missing EAN, waiting 3 seconds and retrying...", flush=True)
            time.sleep(3)  # Wait 3 seconds as per requirement

            # Retry extraction
            rows_retry = _extract_rows_from_table(driver, page_num)

            # Check if retry helped
            missing_count_after = sum(1 for r in rows_retry if not r.ean)

            if missing_count_after < missing_count:
                print(f"  [EAN RETRY] Retry successful: reduced missing EANs from {missing_count} to {missing_count_after}", flush=True)
                rows = rows_retry
                missing_count = missing_count_after
            else:
                print(f"  [EAN RETRY] Retry did not help: still {missing_count_after} missing EANs", flush=True)
                rows = rows_retry
                missing_count = missing_count_after

            return rows, missing_count
        else:
            print(f"  [EAN] All rows have EAN data", flush=True)
            return rows, 0
    else:
        # FETCH_EAN disabled or no rows
        return rows, 0


def _extract_rows_from_table(driver: webdriver.Chrome, page_num: int = 0) -> list[RowData]:
    """
    Internal function to extract rows from the current page table.
    This is the core extraction logic that can be called multiple times for retry.
    """
    # Click all barcodes first to trigger EAN insertion into package column
    click_all_barcodes(driver)
    time.sleep(3)  # Wait for all AJAX requests and DOM mutations to complete

    rows: list[RowData] = []

    # Re-fetch rows AFTER barcode clicks (new rows inserted)
    tr_list = driver.find_elements(By.CSS_SELECTOR, "table.report tbody tr")
    total_rows = len(tr_list)

    print(f"  Processing {total_rows} rows (after EAN insertion)...", flush=True)

    for idx, tr in enumerate(tr_list, 1):
        try:
            # Only main rows have the bullet image with linkhref
            bullet_imgs = tr.find_elements(By.CSS_SELECTOR, "img.bullet[linkhref]")
            if not bullet_imgs:
                # Skip EAN-only rows (gray rows inserted after barcode click)
                continue

            linkhref = bullet_imgs[0].get_attribute("linkhref") or ""
            item_id = parse_item_id_from_linkhref(linkhref)

            tds = tr.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) < 7:
                continue

            tn = tds[2].text.strip()
            inn = tds[3].text.strip()
            manufacturer_country = tds[4].text.strip()

            release_form_full = tds[5].text.strip()
            # Remove trailing "Barcode" or EAN text from release form cell
            release_form = re.sub(r"\b(Barcode|\d{8,14})\b\s*$", "", release_form_full).strip()

            price, date_text = extract_price_and_date(tds[6].text)

            # EAN extraction: Check next sibling row (gray row with EAN after barcode click)
            ean = ""
            try:
                # Get next row after current main row
                next_tr = driver.execute_script(
                    "return arguments[0].nextElementSibling;", tr
                )
                if next_tr:
                    # Check if next row is an EAN row (no bullet image)
                    next_bullets = next_tr.find_elements(By.CSS_SELECTOR, "img.bullet[linkhref]")
                    if not next_bullets:
                        # This is an EAN-only row
                        next_tds = next_tr.find_elements(By.CSS_SELECTOR, "td")
                        if len(next_tds) >= 6:
                            next_package_text = next_tds[5].text.strip()
                            ean = extract_ean(next_package_text)
            except Exception:
                pass

            # Fallback: try extracting from current row if EAN was appended (old behavior)
            if not ean:
                ean = extract_ean(release_form_full)

            rows.append(
                RowData(
                    item_id=item_id,
                    tn=tn,
                    inn=inn,
                    manufacturer_country=manufacturer_country,
                    release_form=release_form,
                    ean=ean,
                    registered_price_rub=price,
                    start_date_text=date_text,
                )
            )
        except StaleElementReferenceException:
            # Page changed mid-loop; skip this row
            continue

    return rows


# Backward compatibility wrapper
def extract_rows_from_current_page(driver: webdriver.Chrome, page_num: int = 0, state_machine=None) -> list[RowData]:
    """
    Legacy function for backward compatibility.
    Calls the new retry-enabled function and returns only rows.
    """
    rows, _ = extract_rows_from_page_with_retry(driver, page_num, state_machine)
    return rows


def go_to_page(driver: webdriver.Chrome, page_num: int, headless: bool | None = None, state_machine=None) -> webdriver.Chrome:
    """
    Navigate to a specific page number.

    Args:
        driver: Chrome WebDriver instance
        page_num: Page number to navigate to
        headless: Whether running in headless mode
        state_machine: Optional navigation state machine
    """
    # Pagination is GET-based (?page=N) and region selection is typically held by session/cookie.
    url = f"{BASE_URL}?page={page_num}"
    return navigate_with_retries(
        driver,
        url,
        wait_for_table,
        f"page {page_num}",
        headless=headless,
        on_restart=lambda d: select_region_and_search(d, headless=headless, state_machine=state_machine),
        state_machine=state_machine,
    )


# --------------------- Progress/Resume Support ---------------------
def load_progress() -> dict:
    """Load progress from JSON file for resume support with enhanced page-level tracking."""
    default_progress = {
        "last_completed_page": 0,
        "total_pages": 0,
        "total_rows": 0,
        "pages": {},  # page_num -> {status, rows_extracted, missing_ean_count, error}
        "pages_with_missing_ean": [],  # List of page numbers with missing EAN data
        "failed_pages": []  # List of page numbers that failed extraction
    }

    if not PROGRESS_FILE.exists():
        return default_progress

    try:
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Migrate old format to new format if needed
        if "pages" not in data:
            data["pages"] = {}
        if "pages_with_missing_ean" not in data:
            data["pages_with_missing_ean"] = []
        if "failed_pages" not in data:
            data["failed_pages"] = []

        return data
    except Exception:
        return default_progress


def save_progress(last_page: int, total_pages: int, total_rows: int, pages_info: dict = None,
                 pages_with_missing_ean: list = None, failed_pages: list = None):
    """
    Save progress to JSON file after each page with enhanced page-level tracking.

    Args:
        last_page: Last successfully completed page number
        total_pages: Total number of pages to scrape
        total_rows: Total unique rows scraped so far
        pages_info: Dict mapping page_num -> {status, rows_extracted, missing_ean_count, error}
        pages_with_missing_ean: List of page numbers with missing EAN data
        failed_pages: List of page numbers that failed extraction
    """
    try:
        # Load existing progress to preserve page history
        existing = load_progress()

        payload = {
            "last_completed_page": last_page,
            "total_pages": total_pages,
            "total_rows": total_rows,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "region": REGION_VALUE,
            "output_csv": str(OUT_CSV),
            "pages": pages_info or existing.get("pages", {}),
            "pages_with_missing_ean": pages_with_missing_ean or existing.get("pages_with_missing_ean", []),
            "failed_pages": failed_pages or existing.get("failed_pages", [])
        }

        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"  Warning: Could not save progress: {e}")


def clear_progress():
    """Clear progress file (for fresh start)."""
    if PROGRESS_FILE.exists():
        try:
            PROGRESS_FILE.unlink()
        except Exception:
            pass


def validate_page_completeness(progress: dict, start_page: int, end_page: int) -> list[int]:
    """
    Validate that all pages in range are marked as complete.
    Returns list of page numbers that need to be re-extracted.

    Args:
        progress: Progress dict from load_progress()
        start_page: Starting page number
        end_page: Ending page number (typically last_completed_page)

    Returns:
        List of page numbers that are missing or incomplete
    """
    pages_to_reextract = []
    pages_info = progress.get("pages", {})

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

        # Check if page has 0 rows (suspicious - likely failed silently)
        rows_extracted = page_data.get("rows_extracted", 0)
        if rows_extracted == 0:
            pages_to_reextract.append(page_num)

    return pages_to_reextract


def load_existing_ids(csv_path: Path) -> set[str]:
    if not csv_path.exists():
        return set()
    ids = set()
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            if r.get("item_id"):
                ids.add(r["item_id"])
    return ids


def append_rows(csv_path: Path, rows: list[RowData]) -> None:
    file_exists = csv_path.exists()

    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        fieldnames = [
            "item_id",
            "TN",
            "INN",
            "Manufacturer_Country",
            "Release_Form",
            "EAN",
            "Registered_Price_RUB",
            "Start_Date_Text",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()

        for r in rows:
            writer.writerow(
                {
                    "item_id": r.item_id,
                    "TN": r.tn,
                    "INN": r.inn,
                    "Manufacturer_Country": r.manufacturer_country,
                    "Release_Form": r.release_form,
                    "EAN": r.ean,
                    "Registered_Price_RUB": r.registered_price_rub,
                    "Start_Date_Text": r.start_date_text,
                }
            )


def main(headless: bool = None, start_page: int = None, end_page: int | None = None, fresh: bool = False) -> None:
    """
    Main scraper function with resume support.

    Args:
        headless: Run Chrome in headless mode (None = use config)
        start_page: Starting page number (None = resume from last or 1)
        end_page: Ending page number (None = scrape all pages)
        fresh: If True, start from page 1 ignoring previous progress
    """
    # Ensure output directory exists
    if USE_CONFIG:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Load progress for resume support
    progress = load_progress()
    
    # Determine start page
    if fresh:
        clear_progress()
        actual_start = 1
        print("Starting fresh run (progress cleared)")
    elif start_page is not None:
        actual_start = start_page
        print(f"Starting from specified page: {actual_start}")
    elif progress["last_completed_page"] > 0:
        actual_start = progress["last_completed_page"] + 1
        print(f"Resuming from page {actual_start} (last completed: {progress['last_completed_page']})")
    else:
        actual_start = 1
        print("Starting fresh run (no previous progress found)")
    
    driver = make_driver(headless=headless)
    
    # Initialize state machine and smart locator for Tier 1 robustness
    state_machine = None
    if STATE_MACHINE_AVAILABLE:
        import logging
        logger = logging.getLogger(__name__)
        locator = SmartLocator(driver, logger=logger)
        state_machine = NavigationStateMachine(locator, logger=logger)
    
    try:
        print("Opening site, selecting region, and searching...")
        driver = select_region_and_search(driver, headless=headless, state_machine=state_machine)

        last_page = get_last_page(driver)
        if end_page is None:
            end_page = last_page
        end_page = min(end_page, last_page)

        # Apply MAX_PAGES limit from config (0 = all pages)
        if MAX_PAGES > 0:
            max_end = actual_start + MAX_PAGES - 1
            if end_page > max_end:
                print(f"[CONFIG] MAX_PAGES={MAX_PAGES}, limiting to pages {actual_start}..{max_end}")
                end_page = max_end
        
        # Check if already completed
        if actual_start > end_page:
            print(f"All pages already scraped (last page: {end_page}, last completed: {progress['last_completed_page']})")
            print(f"[PROGRESS] Scraping pages: {end_page}/{end_page} (100%) - Already completed", flush=True)
            return

        print(f"Detected last page: {last_page}. Will scrape pages {actual_start}..{end_page}.")
        
        # Calculate initial progress
        pages_done = actual_start - 1
        pages_total = end_page
        initial_percent = round((pages_done / pages_total) * 100, 1) if pages_total > 0 else 0
        print(f"[PROGRESS] Scraping pages: {pages_done}/{pages_total} ({initial_percent}%)", flush=True)

        seen_ids = load_existing_ids(OUT_CSV)
        print(f"Resume support: already have {len(seen_ids)} item_ids in {OUT_CSV}")

        # Validate page completeness if resuming
        pages_to_reextract = []
        if actual_start > 1 and progress["last_completed_page"] > 0:
            print("\n[VALIDATION] Checking completeness of previously scraped pages...")
            missing_pages = validate_page_completeness(progress, 1, progress["last_completed_page"])
            if missing_pages:
                print(f"[VALIDATION] Found {len(missing_pages)} incomplete pages: {missing_pages[:10]}{'...' if len(missing_pages) > 10 else ''}")
                pages_to_reextract.extend(missing_pages)
            else:
                print(f"[VALIDATION] All {progress['last_completed_page']} pages are complete")

        # Add pages with missing EAN to re-extract queue
        if progress.get("pages_with_missing_ean"):
            print(f"[VALIDATION] Found {len(progress['pages_with_missing_ean'])} pages with missing EAN data")
            pages_to_reextract.extend(progress["pages_with_missing_ean"])

        # Remove duplicates and sort
        pages_to_reextract = sorted(set(pages_to_reextract))

        # Initialize page-level tracking
        pages_info = progress.get("pages", {})
        pages_with_missing_ean = []
        failed_pages = []

        total_new_rows = 0

        # Main scraping loop
        for p in range(actual_start, end_page + 1):
            # Progress reporting for GUI
            percent = round((p / end_page) * 100, 1) if end_page > 0 else 0
            print(f"\n--- Page {p}/{end_page} ---")
            print(f"[PROGRESS] Scraping pages: {p}/{end_page} ({percent}%)", flush=True)

            # Check session health before navigating - restart if needed
            if not is_session_valid(driver):
                print("  [WARN] Chrome session invalid, restarting browser...", flush=True)
                shutdown_driver(driver)
                driver = make_driver(headless=headless)
                # Re-initialize state machine with new driver
                if STATE_MACHINE_AVAILABLE:
                    locator = SmartLocator(driver, logger=logger)
                    state_machine = NavigationStateMachine(locator, logger=logger)
                # Re-select region after restart
                driver = select_region_and_search(driver, headless=headless, state_machine=state_machine)

            try:
                driver = go_to_page(driver, p, headless=headless, state_machine=state_machine)

                # Add human-like pause between pages
                if HUMAN_ACTIONS_AVAILABLE:
                    pause(0.5, 1.0)

                # Check session again before extraction
                if not is_session_valid(driver):
                    print("  [WARN] Chrome session invalid after navigation, restarting...", flush=True)
                    shutdown_driver(driver)
                    driver = make_driver(headless=headless)
                    if STATE_MACHINE_AVAILABLE:
                        locator = SmartLocator(driver, logger=logger)
                        state_machine = NavigationStateMachine(locator, logger=logger)
                    driver = select_region_and_search(driver, headless=headless, state_machine=state_machine)
                    driver = go_to_page(driver, p, headless=headless, state_machine=state_machine)

                # Extract rows with EAN retry mechanism
                page_rows, missing_ean_count = extract_rows_from_page_with_retry(driver, p, state_machine=state_machine)

                # De-dup by item_id
                new_rows = [r for r in page_rows if r.item_id and r.item_id not in seen_ids]

                print(f"Found rows: {len(page_rows)} | New rows: {len(new_rows)} | Missing EAN: {missing_ean_count}")

                if new_rows:
                    append_rows(OUT_CSV, new_rows)
                    for r in new_rows:
                        seen_ids.add(r.item_id)
                    total_new_rows += len(new_rows)

                # Track page status
                pages_info[str(p)] = {
                    "status": "complete",
                    "rows_extracted": len(page_rows),
                    "missing_ean_count": missing_ean_count,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }

                # Mark page for re-extraction if EAN data is missing
                if missing_ean_count > 0 and FETCH_EAN:
                    if p not in pages_with_missing_ean:
                        pages_with_missing_ean.append(p)

            except Exception as e:
                print(f"  [ERROR] Failed to extract page {p}: {e}", flush=True)
                pages_info[str(p)] = {
                    "status": "failed",
                    "rows_extracted": 0,
                    "error": str(e),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                if p not in failed_pages:
                    failed_pages.append(p)

            # Save progress after each page (for resume support)
            save_progress(p, end_page, len(seen_ids), pages_info, pages_with_missing_ean, failed_pages)

            time.sleep(SLEEP_BETWEEN_PAGES)

        # Re-extract pages with missing EAN or failed pages
        if pages_to_reextract or failed_pages:
            all_reextract = sorted(set(pages_to_reextract + failed_pages))
            print(f"\n\n=== RE-EXTRACTION PHASE ===")
            print(f"Re-extracting {len(all_reextract)} pages with missing/incomplete data...")

            for idx, p in enumerate(all_reextract, 1):
                print(f"\n--- Re-extracting Page {p} ({idx}/{len(all_reextract)}) ---")

                # Check session health
                if not is_session_valid(driver):
                    print("  [WARN] Chrome session invalid, restarting browser...", flush=True)
                    shutdown_driver(driver)
                    driver = make_driver(headless=headless)
                    if STATE_MACHINE_AVAILABLE:
                        locator = SmartLocator(driver, logger=logger)
                        state_machine = NavigationStateMachine(locator, logger=logger)
                    driver = select_region_and_search(driver, headless=headless, state_machine=state_machine)

                try:
                    driver = go_to_page(driver, p, headless=headless, state_machine=state_machine)

                    if HUMAN_ACTIONS_AVAILABLE:
                        pause(0.5, 1.0)

                    # Extract with retry
                    page_rows, missing_ean_count = extract_rows_from_page_with_retry(driver, p, state_machine=state_machine)
                    new_rows = [r for r in page_rows if r.item_id and r.item_id not in seen_ids]

                    print(f"Re-extract: Found rows: {len(page_rows)} | New rows: {len(new_rows)} | Missing EAN: {missing_ean_count}")

                    if new_rows:
                        append_rows(OUT_CSV, new_rows)
                        for r in new_rows:
                            seen_ids.add(r.item_id)
                        total_new_rows += len(new_rows)

                    # Update page status
                    pages_info[str(p)] = {
                        "status": "complete",
                        "rows_extracted": len(page_rows),
                        "missing_ean_count": missing_ean_count,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "reextracted": True
                    }

                    # Remove from missing EAN list if now complete
                    if missing_ean_count == 0 and p in pages_with_missing_ean:
                        pages_with_missing_ean.remove(p)
                    elif missing_ean_count > 0 and p not in pages_with_missing_ean:
                        pages_with_missing_ean.append(p)

                    # Remove from failed pages
                    if p in failed_pages:
                        failed_pages.remove(p)

                except Exception as e:
                    print(f"  [ERROR] Re-extraction failed for page {p}: {e}", flush=True)
                    pages_info[str(p)] = {
                        "status": "failed",
                        "rows_extracted": 0,
                        "error": str(e),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "reextraction_failed": True
                    }

                # Save progress after each re-extraction
                save_progress(end_page, end_page, len(seen_ids), pages_info, pages_with_missing_ean, failed_pages)
                time.sleep(SLEEP_BETWEEN_PAGES)

        print(f"\n{'='*60}")
        print(f"SCRAPING COMPLETED")
        print(f"{'='*60}")
        print(f"Output file: {OUT_CSV}")
        print(f"Total new rows added: {total_new_rows}")
        print(f"Total rows in file: {len(seen_ids)}")
        print(f"Pages scraped: {end_page}")

        # Report on pages with issues
        if pages_with_missing_ean:
            print(f"\n[WARNING] {len(pages_with_missing_ean)} pages have missing EAN data:")
            print(f"  Pages: {pages_with_missing_ean[:20]}{'...' if len(pages_with_missing_ean) > 20 else ''}")
            print(f"  These pages are marked in progress file for review")

        if failed_pages:
            print(f"\n[ERROR] {len(failed_pages)} pages failed extraction:")
            print(f"  Pages: {failed_pages[:20]}{'...' if len(failed_pages) > 20 else ''}")
            print(f"  You may need to manually investigate these pages")

        # Calculate completeness
        complete_pages = sum(1 for p_info in pages_info.values() if p_info.get("status") == "complete")
        total_pages_processed = len(pages_info)
        if total_pages_processed > 0:
            completeness_pct = round((complete_pages / total_pages_processed) * 100, 1)
            print(f"\nCompleteness: {complete_pages}/{total_pages_processed} pages ({completeness_pct}%)")

        print(f"[PROGRESS] Scraping pages: {end_page}/{end_page} (100%) - Completed", flush=True)

        # Log metrics and state history if state machine is available
        if STATE_MACHINE_AVAILABLE and state_machine:
            import logging
            logger = logging.getLogger(__name__)
            metrics = locator.get_metrics()
            metrics_summary = metrics.get_summary()
            logger.info(f"[METRICS] Locator performance: {metrics_summary}")

            # Log state transitions
            state_history = state_machine.get_state_history()
            logger.info(f"[METRICS] State transitions: {len(state_history)} transitions")
            for state, timestamp, success in state_history:
                status = "SUCCESS" if success else "FAILED"
                logger.debug(f"[METRICS] State: {state.value} at {timestamp:.2f}s - {status}")

        # Clear progress file only if no issues
        if not pages_with_missing_ean and not failed_pages:
            print("\n[SUCCESS] All pages extracted successfully. Clearing progress file.")
            clear_progress()
        else:
            print("\n[INFO] Progress file retained for pages with issues. Run again to retry or use --fresh to start over.")

    finally:
        shutdown_driver(driver)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Russia Farmcom VED Scraper")
    parser.add_argument("--fresh", action="store_true", help="Start from page 1 (ignore previous progress)")
    parser.add_argument("--start", type=int, help="Start from specific page number")
    parser.add_argument("--end", type=int, help="End at specific page number")
    parser.add_argument("--visible", action="store_true", help="Show browser window (not headless)")

    args = parser.parse_args()

    main(
        headless=False if args.visible else None,
        start_page=args.start,
        end_page=args.end,
        fresh=args.fresh
    )
