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
import threading
from queue import Queue, Empty
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs
from pathlib import Path
from typing import Set, Dict, List, Optional

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

# Multi-threading configuration
NUM_WORKERS = getenv_int("SCRIPT_01_NUM_WORKERS", 3) if USE_CONFIG else 3
MAX_RETRIES_PER_PAGE = getenv_int("SCRIPT_01_MAX_RETRIES_PER_PAGE", 3) if USE_CONFIG else 3


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


def extract_rows_from_page_with_retry(driver: webdriver.Chrome, page_num: int = 0, state_machine=None) -> tuple[list[RowData], int, bool]:
    """
    Extract rows from current page with strict EAN validation.

    STRICT RULE: Row count MUST equal EAN count. If not, retry once after 5 seconds.
    If still mismatch, mark page for later retry (don't write data).

    Returns:
        tuple: (rows, missing_ean_count, is_valid)
            - rows: List of extracted RowData
            - missing_ean_count: Number of rows with missing EAN
            - is_valid: True if data is valid to write, False if needs retry later
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

    # STRICT EAN VALIDATION (only if FETCH_EAN is enabled)
    if FETCH_EAN and rows:
        row_count = len(rows)
        ean_count = sum(1 for r in rows if r.ean)
        missing_count = row_count - ean_count

        print(f"  [VALIDATION] Rows: {row_count} | With EAN: {ean_count} | Missing: {missing_count}", flush=True)

        # Check if row count == EAN count (STRICT)
        if row_count != ean_count:
            print(f"  [VALIDATION FAILED] Row count ({row_count}) != EAN count ({ean_count})", flush=True)
            print(f"  [EAN RETRY] Waiting 5 seconds and retrying...", flush=True)
            time.sleep(5)  # Wait 5 seconds as per requirement

            # Retry extraction
            rows_retry = _extract_rows_from_table(driver, page_num)
            row_count_retry = len(rows_retry)
            ean_count_retry = sum(1 for r in rows_retry if r.ean)
            missing_count_retry = row_count_retry - ean_count_retry

            print(f"  [VALIDATION RETRY] Rows: {row_count_retry} | With EAN: {ean_count_retry} | Missing: {missing_count_retry}", flush=True)

            # Check again
            if row_count_retry == ean_count_retry:
                print(f"  [VALIDATION SUCCESS] Retry successful: 100% EAN coverage achieved", flush=True)
                return rows_retry, 0, True
            else:
                print(f"  [VALIDATION FAILED] Retry did not fix issue", flush=True)
                print(f"  [ACTION] Marking page for later retry - DATA NOT WRITTEN", flush=True)
                return [], row_count_retry, False  # Return empty list, mark as invalid
        else:
            print(f"  [VALIDATION SUCCESS] 100% EAN coverage - data is valid", flush=True)
            return rows, 0, True
    else:
        # FETCH_EAN disabled or no rows
        if not rows:
            print(f"  [WARN] No rows extracted from page", flush=True)
            return rows, 0, False
        return rows, 0, True


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


# --------------------- Parallel Tab Extraction ---------------------

def open_tabs(driver: webdriver.Chrome, num_tabs: int) -> list[str]:
    """
    Open multiple browser tabs and return list of window handles.

    Args:
        driver: Chrome WebDriver instance
        num_tabs: Number of tabs to open (including the existing one)

    Returns:
        List of window handles for all tabs
    """
    handles = [driver.current_window_handle]  # First tab already exists

    # Open additional tabs
    for i in range(num_tabs - 1):
        driver.execute_script("window.open('about:blank', '_blank');")
        time.sleep(0.1)  # Small delay for tab to open

    handles = driver.window_handles
    print(f"  [TABS] Opened {len(handles)} browser tabs", flush=True)
    return handles


def navigate_tab_to_page(driver: webdriver.Chrome, handle: str, page_num: int, state_machine=None) -> bool:
    """
    Navigate a specific tab to a page URL.

    Args:
        driver: Chrome WebDriver instance
        handle: Window handle of the tab
        page_num: Page number to navigate to
        state_machine: Optional navigation state machine

    Returns:
        True if navigation succeeded, False otherwise
    """
    try:
        driver.switch_to.window(handle)
        url = f"{BASE_URL}?page={page_num}"
        driver.get(url)
        remove_webdriver_property(driver)
        return True
    except Exception as e:
        print(f"  [TAB ERROR] Failed to navigate tab to page {page_num}: {e}", flush=True)
        return False


def extract_from_tab(driver: webdriver.Chrome, handle: str, page_num: int,
                     driver_lock: threading.Lock, state_machine=None) -> tuple[int, list[RowData], int, bool]:
    """
    Extract data from a specific tab.

    Args:
        driver: Chrome WebDriver instance
        handle: Window handle of the tab
        page_num: Page number being extracted
        driver_lock: Lock for thread-safe driver access
        state_machine: Optional navigation state machine

    Returns:
        Tuple of (page_num, rows, missing_ean_count, is_valid)
    """
    try:
        with driver_lock:
            driver.switch_to.window(handle)

            # Wait for table to load
            try:
                WebDriverWait(driver, WAIT_TIMEOUT).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table.report tbody tr"))
                )
            except TimeoutException:
                print(f"  [TAB {page_num}] Timeout waiting for table", flush=True)
                return (page_num, [], 0, False)

            # Extract rows with validation
            rows, missing_ean_count, is_valid = extract_rows_from_page_with_retry(driver, page_num, state_machine)

            return (page_num, rows, missing_ean_count, is_valid)

    except Exception as e:
        print(f"  [TAB {page_num}] Extraction error: {e}", flush=True)
        return (page_num, [], 0, False)


def parallel_extract_batch(driver: webdriver.Chrome, page_numbers: list[int],
                          headless: bool = None, state_machine=None) -> list[tuple[int, list[RowData], int, bool]]:
    """
    Extract data from multiple pages in parallel using browser tabs.

    This is the TRUE parallel extraction implementation:
    1. Opens N browser tabs (one per page)
    2. Navigates all tabs to their respective pages simultaneously
    3. Extracts data from all tabs (sequentially due to Selenium limitation, but navigations are parallel)
    4. Returns combined results

    Args:
        driver: Chrome WebDriver instance
        page_numbers: List of page numbers to extract
        headless: Whether running in headless mode
        state_machine: Optional navigation state machine

    Returns:
        List of (page_num, rows, missing_ean_count, is_valid) tuples
    """
    if not page_numbers:
        return []

    num_tabs = len(page_numbers)
    results = []
    original_handle = driver.current_window_handle

    print(f"\n  [PARALLEL] Extracting {num_tabs} pages in parallel: {page_numbers}", flush=True)

    try:
        # Step 1: Open tabs
        handles = open_tabs(driver, num_tabs)

        if len(handles) < num_tabs:
            print(f"  [WARN] Only opened {len(handles)} tabs, expected {num_tabs}", flush=True)
            num_tabs = len(handles)
            page_numbers = page_numbers[:num_tabs]

        # Map page numbers to tab handles
        tab_page_map = list(zip(handles, page_numbers))

        # Step 2: Navigate ALL tabs to their pages FIRST (this is the parallel part)
        print(f"  [PARALLEL] Navigating all {num_tabs} tabs to their pages...", flush=True)
        nav_start = time.time()

        for handle, page_num in tab_page_map:
            driver.switch_to.window(handle)
            url = f"{BASE_URL}?page={page_num}"
            driver.get(url)
            remove_webdriver_property(driver)

        nav_elapsed = time.time() - nav_start
        print(f"  [PARALLEL] All tabs navigated in {nav_elapsed:.1f}s", flush=True)

        # Step 3: Wait for all tabs to load (give them time to render)
        print(f"  [PARALLEL] Waiting for all tabs to load...", flush=True)
        time.sleep(3)  # Allow all pages to start loading

        # Step 4: Extract from each tab (must be sequential due to Selenium driver limitation)
        print(f"  [PARALLEL] Extracting data from all tabs...", flush=True)
        extract_start = time.time()

        for handle, page_num in tab_page_map:
            try:
                driver.switch_to.window(handle)

                # Wait for table to be ready
                try:
                    WebDriverWait(driver, WAIT_TIMEOUT).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "table.report tbody tr"))
                    )
                except TimeoutException:
                    print(f"  [TAB {page_num}] Timeout waiting for table", flush=True)
                    results.append((page_num, [], 0, False))
                    continue

                # Add small delay to ensure page is fully rendered
                if HUMAN_ACTIONS_AVAILABLE:
                    pause(0.3, 0.6)
                else:
                    time.sleep(0.5)

                # Extract rows with strict EAN validation
                rows, missing_ean_count, is_valid = extract_rows_from_page_with_retry(driver, page_num, state_machine)
                results.append((page_num, rows, missing_ean_count, is_valid))

                if is_valid and rows:
                    print(f"  [TAB {page_num}] OK: {len(rows)} rows extracted (100% EAN)", flush=True)
                elif not is_valid:
                    print(f"  [TAB {page_num}] X Validation failed ({missing_ean_count} missing EAN)", flush=True)
                else:
                    print(f"  [TAB {page_num}] X No rows extracted", flush=True)

            except Exception as e:
                print(f"  [TAB {page_num}] ERROR: {e}", flush=True)
                results.append((page_num, [], 0, False))

        extract_elapsed = time.time() - extract_start
        total_elapsed = time.time() - nav_start
        print(f"  [PARALLEL] Extraction completed in {extract_elapsed:.1f}s (total: {total_elapsed:.1f}s)", flush=True)

    finally:
        # Close extra tabs, keep only the original
        try:
            all_handles = driver.window_handles
            for handle in all_handles:
                if handle != original_handle:
                    driver.switch_to.window(handle)
                    driver.close()
            driver.switch_to.window(original_handle)
        except Exception as e:
            print(f"  [WARN] Error closing tabs: {e}", flush=True)

    return results


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
                 pages_with_missing_ean: list = None, failed_pages: list = None, lock: threading.Lock = None):
    """
    Save progress to JSON file after each page with enhanced page-level tracking.

    Args:
        last_page: Last successfully completed page number
        total_pages: Total number of pages to scrape
        total_rows: Total unique rows scraped so far
        pages_info: Dict mapping page_num -> {status, rows_extracted, missing_ean_count, error}
        pages_with_missing_ean: List of page numbers with missing EAN data
        failed_pages: List of page numbers that failed extraction
        lock: Optional threading lock for thread-safe writes
    """
    def _write():
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

    if lock:
        with lock:
            _write()
    else:
        _write()


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


def append_rows(csv_path: Path, rows: list[RowData], lock: threading.Lock = None) -> None:
    def _write():
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

    if lock:
        with lock:
            _write()
    else:
        _write()


def worker_fn(
    worker_id: int,
    page_queue: Queue,
    seen_ids: Set[str],
    seen_ids_lock: threading.Lock,
    csv_lock: threading.Lock,
    progress_data: Dict,
    progress_lock: threading.Lock,
    progress_counter: Dict,
    counter_lock: threading.Lock,
    headless: bool,
    total_pages: int
) -> None:
    """Worker function to process pages in parallel."""
    driver = None
    state_machine = None
    locator = None

    try:
        driver = make_driver(headless=headless)
        if driver is None:
            print(f"[Worker {worker_id}] Failed to create driver", flush=True)
            return

        # Initialize state machine for this worker
        if STATE_MACHINE_AVAILABLE:
            import logging
            logger = logging.getLogger(f"worker_{worker_id}")
            locator = SmartLocator(driver, logger=logger)
            state_machine = NavigationStateMachine(locator, logger=logger)

        print(f"[Worker {worker_id}] Started", flush=True)

        # Initialize session: select region
        print(f"  [Worker {worker_id}] Initializing session...", flush=True)
        try:
            driver = select_region_and_search(driver, headless=headless, state_machine=state_machine)
            print(f"  [Worker {worker_id}] Session initialized", flush=True)
        except Exception as e:
            print(f"  [Worker {worker_id}] Failed to initialize session: {e}", flush=True)

        while True:
            try:
                page_num = page_queue.get(timeout=2)
            except Empty:
                break

            if page_num is None:
                page_queue.task_done()
                continue

            print(f"\n[Worker {worker_id}] Processing page {page_num}", flush=True)

            success = False
            for attempt in range(1, MAX_RETRIES_PER_PAGE + 1):
                try:
                    # Check session health
                    if not is_session_valid(driver):
                        print(f"  [Worker {worker_id}] Session invalid, reinitializing...", flush=True)
                        driver = restart_driver(driver, headless=headless)
                        if STATE_MACHINE_AVAILABLE:
                            import logging
                            logger = logging.getLogger(f"worker_{worker_id}")
                            locator = SmartLocator(driver, logger=logger)
                            state_machine = NavigationStateMachine(locator, logger=logger)
                        driver = select_region_and_search(driver, headless=headless, state_machine=state_machine)

                    # Navigate to page
                    driver = go_to_page(driver, page_num, headless=headless, state_machine=state_machine)

                    # Add human-like pause
                    if HUMAN_ACTIONS_AVAILABLE:
                        pause(0.3, 0.8)

                    # Extract rows with validation
                    page_rows, missing_ean_count, is_valid = extract_rows_from_page_with_retry(driver, page_num, state_machine=state_machine)

                    if is_valid and page_rows:
                        # De-dup by item_id (thread-safe)
                        with seen_ids_lock:
                            new_rows = [r for r in page_rows if r.item_id and r.item_id not in seen_ids]
                            for r in new_rows:
                                seen_ids.add(r.item_id)

                        # Write to CSV (thread-safe)
                        if new_rows:
                            append_rows(OUT_CSV, new_rows, lock=csv_lock)

                        print(f"  [Worker {worker_id}] Page {page_num}: {len(page_rows)} rows, {len(new_rows)} new", flush=True)

                        # Update progress data (thread-safe)
                        with progress_lock:
                            pages_info = progress_data.get("pages", {})
                            failed_pages = progress_data.get("failed_pages", [])
                            pages_with_missing_ean = progress_data.get("pages_with_missing_ean", [])

                            pages_info[str(page_num)] = {
                                "status": "complete",
                                "rows_extracted": len(page_rows),
                                "missing_ean_count": 0,
                                "timestamp": datetime.now(timezone.utc).isoformat()
                            }

                            if page_num in failed_pages:
                                failed_pages.remove(page_num)
                            if page_num in pages_with_missing_ean:
                                pages_with_missing_ean.remove(page_num)

                            progress_data["pages"] = pages_info
                            progress_data["failed_pages"] = failed_pages
                            progress_data["pages_with_missing_ean"] = pages_with_missing_ean

                        # Update progress counter
                        with counter_lock:
                            progress_counter["done"] += 1
                            progress_counter["new_rows"] += len(new_rows)
                            pct = round((progress_counter["done"] / progress_counter["total"]) * 100, 1)
                            print(f"[PROGRESS] {progress_counter['done']}/{progress_counter['total']} ({pct}%) pages - {progress_counter['new_rows']} new rows", flush=True)

                        # Save progress periodically (every 5 pages per worker)
                        if progress_counter["done"] % 5 == 0:
                            with progress_lock:
                                save_progress(
                                    page_num, total_pages, len(seen_ids),
                                    progress_data.get("pages", {}),
                                    progress_data.get("pages_with_missing_ean", []),
                                    progress_data.get("failed_pages", [])
                                )

                        success = True
                        break

                    elif not is_valid:
                        print(f"  [Worker {worker_id}] Page {page_num}: Validation failed ({missing_ean_count} missing EAN)", flush=True)

                        # Track as failed validation
                        with progress_lock:
                            pages_info = progress_data.get("pages", {})
                            pages_with_missing_ean = progress_data.get("pages_with_missing_ean", [])

                            pages_info[str(page_num)] = {
                                "status": "validation_failed",
                                "rows_extracted": 0,
                                "missing_ean_count": missing_ean_count,
                                "timestamp": datetime.now(timezone.utc).isoformat()
                            }

                            if page_num not in pages_with_missing_ean:
                                pages_with_missing_ean.append(page_num)

                            progress_data["pages"] = pages_info
                            progress_data["pages_with_missing_ean"] = pages_with_missing_ean

                        # Still mark as "processed" for progress
                        with counter_lock:
                            progress_counter["done"] += 1

                        success = True  # Don't retry validation failures
                        break

                    else:
                        print(f"  [Worker {worker_id}] Page {page_num}: No rows extracted", flush=True)
                        raise RuntimeError("No rows extracted")

                except Exception as e:
                    print(f"  [Worker {worker_id}] Attempt {attempt}/{MAX_RETRIES_PER_PAGE} failed for page {page_num}: {e}", flush=True)
                    if attempt == MAX_RETRIES_PER_PAGE:
                        # Mark as failed
                        with progress_lock:
                            pages_info = progress_data.get("pages", {})
                            failed_pages = progress_data.get("failed_pages", [])

                            pages_info[str(page_num)] = {
                                "status": "failed",
                                "rows_extracted": 0,
                                "error": str(e),
                                "timestamp": datetime.now(timezone.utc).isoformat()
                            }

                            if page_num not in failed_pages:
                                failed_pages.append(page_num)

                            progress_data["pages"] = pages_info
                            progress_data["failed_pages"] = failed_pages

                        with counter_lock:
                            progress_counter["done"] += 1

                        print(f"  [Worker {worker_id}] Page {page_num} marked as FAILED", flush=True)
                    else:
                        # Restart driver on failure
                        try:
                            driver = restart_driver(driver, headless=headless)
                            if STATE_MACHINE_AVAILABLE:
                                import logging
                                logger = logging.getLogger(f"worker_{worker_id}")
                                locator = SmartLocator(driver, logger=logger)
                                state_machine = NavigationStateMachine(locator, logger=logger)
                            driver = select_region_and_search(driver, headless=headless, state_machine=state_machine)
                            print(f"  [Worker {worker_id}] Session reinitialized", flush=True)
                        except Exception:
                            pass
                        time.sleep(2)

            page_queue.task_done()
            time.sleep(SLEEP_BETWEEN_PAGES)

    finally:
        shutdown_driver(driver)
        print(f"[Worker {worker_id}] Stopped", flush=True)


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
        pages_with_missing_ean = progress.get("pages_with_missing_ean", [])
        failed_pages = progress.get("failed_pages", [])

        total_new_rows = 0

        # BATCH PROCESSING: Process 5 pages at a time using parallel browser tabs
        BATCH_SIZE = 5
        total_pages = end_page - actual_start + 1

        print(f"\n{'='*80}")
        print(f"RUSSIA SCRAPER - PARALLEL TAB EXTRACTION")
        print(f"{'='*80}")
        print(f"[MODE] Extracting {BATCH_SIZE} pages simultaneously using browser tabs")
        print(f"[INFO] Total pages to scrape: {total_pages}")
        print(f"[INFO] Total batches: {(total_pages + BATCH_SIZE - 1) // BATCH_SIZE}")

        # Main scraping loop - TRUE PARALLEL EXTRACTION (5 pages at a time using browser tabs)
        for batch_start in range(actual_start, end_page + 1, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE - 1, end_page)
            batch_num = (batch_start - actual_start) // BATCH_SIZE + 1
            total_batches = (total_pages + BATCH_SIZE - 1) // BATCH_SIZE

            print(f"\n{'='*80}")
            print(f"BATCH {batch_num}/{total_batches}: Pages {batch_start} to {batch_end} [PARALLEL TAB EXTRACTION]")
            print(f"{'='*80}")

            # Progress reporting for GUI
            percent = round((batch_start / end_page) * 100, 1) if end_page > 0 else 0
            print(f"[PROGRESS] Scraping pages: {batch_start}/{end_page} ({percent}%)", flush=True)

            # Check session health before batch - restart if needed
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

            # Build list of pages for this batch
            batch_pages = list(range(batch_start, batch_end + 1))

            try:
                # TRUE PARALLEL EXTRACTION: Extract all pages in batch using multiple tabs
                batch_results = parallel_extract_batch(driver, batch_pages, headless=headless, state_machine=state_machine)

                # Process results
                batch_valid_rows = []
                batch_data = []

                for page_num, page_rows, missing_ean_count, is_valid in batch_results:
                    batch_data.append((page_num, page_rows, is_valid, missing_ean_count))

                    if is_valid and page_rows:
                        # De-dup by item_id
                        new_rows = [r for r in page_rows if r.item_id and r.item_id not in seen_ids]
                        batch_valid_rows.extend(new_rows)

                        # Track page status as complete
                        pages_info[str(page_num)] = {
                            "status": "complete",
                            "rows_extracted": len(page_rows),
                            "missing_ean_count": 0,
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        }
                    elif not is_valid:
                        # Track page status as validation failed
                        pages_info[str(page_num)] = {
                            "status": "validation_failed",
                            "rows_extracted": 0,
                            "missing_ean_count": missing_ean_count,
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        }

                        # Mark page for re-extraction
                        if page_num not in pages_with_missing_ean:
                            pages_with_missing_ean.append(page_num)
                    else:
                        pages_info[str(page_num)] = {
                            "status": "no_data",
                            "rows_extracted": 0,
                            "missing_ean_count": 0,
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        }
                        if page_num not in failed_pages:
                            failed_pages.append(page_num)

            except Exception as e:
                print(f"  [ERROR] Batch extraction failed: {e}", flush=True)
                # Mark all pages in batch as failed
                batch_data = []
                batch_valid_rows = []
                for p in batch_pages:
                    batch_data.append((p, [], False, 0))
                    pages_info[str(p)] = {
                        "status": "failed",
                        "rows_extracted": 0,
                        "error": str(e),
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    }
                    if p not in failed_pages:
                        failed_pages.append(p)

            # BATCH COMPLETE - Write all valid rows at once
            print(f"\n--- Batch {batch_num} Summary ---")
            valid_pages = sum(1 for _, _, is_valid, _ in batch_data if is_valid)
            failed_in_batch = len(batch_data) - valid_pages
            print(f"  Valid pages: {valid_pages}/{len(batch_data)}")
            print(f"  Failed pages: {failed_in_batch}")
            print(f"  Total valid rows: {len(batch_valid_rows)}")

            if batch_valid_rows:
                print(f"  [WRITE] Writing {len(batch_valid_rows)} rows to CSV...")
                append_rows(OUT_CSV, batch_valid_rows)
                for r in batch_valid_rows:
                    seen_ids.add(r.item_id)
                total_new_rows += len(batch_valid_rows)
                print(f"  [OK] Batch {batch_num} data written successfully")
            else:
                print(f"  [SKIP] No valid data to write in this batch")

            # Save progress after each batch
            save_progress(batch_end, end_page, len(seen_ids), pages_info, pages_with_missing_ean, failed_pages)
            print(f"  [SAVE] Progress saved after batch {batch_num}")

            # Small delay between batches
            time.sleep(SLEEP_BETWEEN_PAGES)

        # Re-extract pages with missing EAN or failed pages
        if pages_to_reextract or pages_with_missing_ean or failed_pages:
            print(f"\n[INFO] {len(pages_with_missing_ean)} pages with missing EAN, {len(failed_pages)} failed pages")
            print(f"[INFO] Run again to retry failed pages")

        print(f"\n{'='*80}")
        print(f"SCRAPING COMPLETED")
        print(f"{'='*80}")
        print(f"Output file: {OUT_CSV}")
        print(f"Total new rows added: {total_new_rows}")
        print(f"Total rows in file: {len(seen_ids)}")
        print(f"Total pages: {end_page}")
        print(f"Pages scraped: {actual_start} to {end_page}")

        # Calculate status breakdown
        complete_pages = sum(1 for p_info in pages_info.values() if p_info.get("status") == "complete")
        validation_failed = sum(1 for p_info in pages_info.values() if "validation_failed" in p_info.get("status", ""))
        total_pages_processed = len(pages_info)

        print(f"\n{'='*80}")
        print(f"PAGE STATUS SUMMARY")
        print(f"{'='*80}")
        print(f"OK Complete (100% EAN):        {complete_pages}/{total_pages_processed} pages")
        print(f"X Validation Failed:          {validation_failed} pages")
        print(f"X Extraction Failed:          {len(failed_pages)} pages")
        if total_pages_processed > 0:
            completeness_pct = round((complete_pages / total_pages_processed) * 100, 1)
            print(f"\nData Completeness: {completeness_pct}% ({complete_pages}/{total_pages_processed} pages)")

        # Report on pages with issues
        if pages_with_missing_ean:
            print(f"\n{'='*80}")
            print(f"WARNING: {len(pages_with_missing_ean)} PAGES MISSING EAN DATA")
            print(f"{'='*80}")
            print(f"Pages: {sorted(pages_with_missing_ean)[:30]}{'...' if len(pages_with_missing_ean) > 30 else ''}")
            print(f"\n⚠ DATA FROM THESE PAGES WAS NOT WRITTEN (100% EAN coverage required)")
            print(f"⚠ These pages need to be retried in Step 03 (Retry Failed Pages)")

        if failed_pages:
            print(f"\n{'='*80}")
            print(f"ERROR: {len(failed_pages)} PAGES FAILED EXTRACTION")
            print(f"{'='*80}")
            print(f"Pages: {sorted(failed_pages)[:30]}{'...' if len(failed_pages) > 30 else ''}")
            print(f"\n⚠ These pages need manual investigation or retry")

        # Verify EAN coverage in actual CSV file
        if OUT_CSV.exists() and FETCH_EAN:
            try:
                import pandas as pd
                df = pd.read_csv(OUT_CSV)
                total_rows_csv = len(df)
                if 'EAN' in df.columns:
                    missing_ean_csv = df['EAN'].isna().sum() + (df['EAN'] == '').sum()
                    ean_coverage_pct = round(((total_rows_csv - missing_ean_csv) / total_rows_csv * 100), 2) if total_rows_csv > 0 else 0

                    print(f"\n{'='*80}")
                    print(f"CSV FILE VALIDATION")
                    print(f"{'='*80}")
                    print(f"Total rows in CSV:            {total_rows_csv:,}")
                    print(f"Rows with EAN:                {total_rows_csv - missing_ean_csv:,}")
                    print(f"Rows missing EAN:             {missing_ean_csv:,}")
                    print(f"EAN Coverage:                 {ean_coverage_pct}%")

                    if missing_ean_csv == 0:
                        print(f"\nOK SUCCESS: 100% EAN coverage achieved!")
                    else:
                        print(f"\nX WARNING: {missing_ean_csv} rows still missing EAN")
                        print(f"  This should be 0 - retry failed pages to fix")
            except Exception as e:
                print(f"\n[INFO] Could not verify CSV EAN coverage: {e}")

        print(f"[PROGRESS] Scraping pages: {end_page}/{end_page} (100%) - Completed", flush=True)

        # Clear progress file only if no issues
        if not pages_with_missing_ean and not failed_pages:
            print("\n[SUCCESS] All pages extracted successfully. Clearing progress file.")
            clear_progress()
        else:
            print("\n[INFO] Progress file retained for pages with issues. Run again to retry or use --fresh to start over.")

    except Exception as e:
        print(f"\n[ERROR] Main function failed: {e}", flush=True)
        raise


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
