#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Russia Farmcom VED Registry Scraper - DB-Based with Full Resume Support

Features:
- DB-based storage (no CSV files)
- Page-level resume support with ru_step_progress table
- Chrome instance tracking and cleanup
- Crash recovery with automatic resume
- Comprehensive retry logic with driver restart
- Bulk insert for performance
- Progress logging every N pages

Author: Enterprise PDF Processing Pipeline
License: Proprietary
"""

import os
import re
import sys
import time
import json
import atexit
import signal
import gc
import threading
from pathlib import Path

# --- CORE PATH FIX ---
# We need to ensure local imports (like 'db') are resolved from the script's directory (scripts/Russia/db)
# and NOT from core/db or other locations.
_script_dir = Path(__file__).resolve().parent
_repo_root = _script_dir.parents[1]  # D:/quad99/Scrappers

# 1. Force script dir to be absolute index 0
if str(_script_dir) in sys.path:
    sys.path.remove(str(_script_dir))
sys.path.insert(0, str(_script_dir))

# 2. Add repo root for core imports, but AFTER script dir
if str(_repo_root) not in sys.path:
    sys.path.insert(1, str(_repo_root))

# ---------------------

from core.control.lifecycle import register_shutdown_handler
from core.parsing.price_parser import parse_price
from core.network.proxy_checker import check_vpn_connection as core_check_vpn
from core.browser.driver_factory import create_chrome_driver, restart_driver as core_restart_driver
from core.browser.chrome_manager import register_chrome_driver, unregister_chrome_driver
from datetime import datetime, timezone
from typing import Set, Dict, List, Optional
from core.monitoring.audit_logger import audit_log

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
os.environ.setdefault('PYTHONUNBUFFERED', '1')

# Clear conflicting 'db' when run in same process as other scrapers (e.g. GUI)
for mod in list(sys.modules.keys()):
    if mod == "db" or mod.startswith("db."):
        del sys.modules[mod]

# Config loader
from config_loader import (
    load_env_file, getenv, getenv_bool, getenv_int, getenv_float,
    get_input_dir, get_output_dir
)
load_env_file()

# DB imports
from core.db.connection import CountryDB
from core.db.models import generate_run_id, run_ledger_start, run_ledger_finish
# --- CRITICAL PATH FIX: Re-assert local script dir priority ---
# config_loader.py or other imports may have pushed 'core' or 'repo_root' to the top.
# We MUST ensure that 'db' imports resolve to the LOCAL scripts/Russia/db, not core/db.
_local_dir = str(Path(__file__).resolve().parent)
if _local_dir in sys.path: 
    sys.path.remove(_local_dir)
sys.path.insert(0, _local_dir)

# Check and Unload if 'db' was already loaded from the wrong place (e.g. core)
if "db" in sys.modules:
    _db_file = getattr(sys.modules["db"], "__file__", "")
    if "core" in _db_file or (_local_dir not in _db_file and "site-packages" not in _db_file):
        del sys.modules["db"]
        # Also clean up submodules
        for _k in list(sys.modules.keys()):
            if _k.startswith("db."):
                del sys.modules[_k]
# ----------------------------------------------------------------

try:
    import db
    # print(f"[DEBUG] 'db' module loaded from: {getattr(db, '__file__', 'unknown')}", flush=True)
    from db.schema import apply_russia_schema
    from db.repositories import RussiaRepository
except ImportError as e:
    print(f"[ERROR] Failed to import db.schema or db.repositories: {e}", flush=True)
    print(f"[DEBUG] sys.path[0]: {sys.path[0]}", flush=True)
    print(f"[DEBUG] db file: {getattr(sys.modules.get('db'), '__file__', 'not loaded')}", flush=True)
    raise
# Selenium imports
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, WebDriverException

from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions

# Chrome tracking
try:
    from core.browser.chrome_instance_tracker import ChromeInstanceTracker
except ImportError:
    ChromeInstanceTracker = None

from core.browser.chrome_manager import register_chrome_driver, unregister_chrome_driver

# PID tracking for pipeline stop cleanup (DB-based)
try:
    from core.browser.chrome_pid_tracker import get_chrome_pids_from_driver
except ImportError:
    get_chrome_pids_from_driver = None

# State machine
from smart_locator import SmartLocator
from state_machine import NavigationStateMachine, NavigationState, StateCondition

# Human actions
try:
    from core.browser.human_actions import pause
except ImportError:
    def pause(min_s=0.2, max_s=0.6):
        time.sleep(__import__('random').uniform(min_s, max_s))

# =============================================================================
# CONFIGURATION - All values from env, NO hardcoded defaults
# =============================================================================

BASE_URL = getenv("SCRIPT_01_BASE_URL")
REGION_VALUE = getenv("SCRIPT_01_REGION_VALUE")
HEADLESS = getenv_bool("SCRIPT_01_HEADLESS")
PAGE_LOAD_TIMEOUT = getenv_int("SCRIPT_01_PAGE_LOAD_TIMEOUT")
WAIT_TIMEOUT = getenv_int("SCRIPT_01_WAIT_TIMEOUT")
CLICK_RETRY = getenv_int("SCRIPT_01_CLICK_RETRY")
SLEEP_BETWEEN_PAGES = getenv_float("SCRIPT_01_SLEEP_BETWEEN_PAGES")
NAV_RETRIES = getenv_int("SCRIPT_01_NAV_RETRIES")
NAV_RETRY_SLEEP = getenv_float("SCRIPT_01_NAV_RETRY_SLEEP")
NAV_RESTART_DRIVER = getenv_bool("SCRIPT_01_NAV_RESTART_DRIVER")
FETCH_EAN = getenv_bool("SCRIPT_01_FETCH_EAN", False)
EAN_POPUP_TIMEOUT = getenv_int("SCRIPT_01_EAN_POPUP_TIMEOUT", 3)
# Strictly 100 EANs per full page; retry barcode click + extract up to this many times
EAN_CLICK_RETRIES = getenv_int("SCRIPT_01_EAN_CLICK_RETRIES", 5)
MAX_PAGES = getenv_int("SCRIPT_01_MAX_PAGES", 0)  # 0 = no limit, extract all pages
NUM_WORKERS = getenv_int("SCRIPT_01_NUM_WORKERS")
MAX_RETRIES_PER_PAGE = getenv_int("SCRIPT_01_MAX_RETRIES_PER_PAGE")
DB_BATCH_SIZE = getenv_int("DB_BATCH_INSERT_SIZE", 100)
PROGRESS_INTERVAL = getenv_int("DB_PROGRESS_LOG_INTERVAL", 50)
# Number of tabs to open for parallel page load (1 = sequential, 5 = 5 tabs)
MULTI_TAB_BATCH = getenv_int("SCRIPT_01_MULTI_TAB_BATCH", 10)  # Default 10 pages per batch

# Safety limits to prevent infinite loops (defensive programming)
MAX_EMPTY_BATCH_ITERATIONS = getenv_int("SCRIPT_01_MAX_EMPTY_BATCH_ITERATIONS", 10)
MAX_TOTAL_ITERATIONS = getenv_int("SCRIPT_01_MAX_TOTAL_ITERATIONS", 0)  # 0 = unlimited

# Chrome options from env
CHROME_START_MAXIMIZED = getenv("SCRIPT_01_CHROME_START_MAXIMIZED")
CHROME_DISABLE_AUTOMATION = getenv("SCRIPT_01_CHROME_DISABLE_AUTOMATION")
CHROME_NO_SANDBOX = getenv("SCRIPT_01_CHROME_NO_SANDBOX")
CHROME_DISABLE_DEV_SHM = getenv("SCRIPT_01_CHROME_DISABLE_DEV_SHM")

# VPN settings
VPN_REQUIRED = getenv_bool("VPN_REQUIRED", False)
VPN_CHECK_ENABLED = getenv_bool("VPN_CHECK_ENABLED", False)
VPN_CHECK_HOST = getenv("VPN_CHECK_HOST", "8.8.8.8")
VPN_CHECK_PORT = getenv_int("VPN_CHECK_PORT", 53)

# =============================================================================
# VPN CHECK
# =============================================================================

def check_vpn_connection() -> bool:
    """
    Check if VPN is connected (if required).
    Returns True if VPN check passes or is disabled.
    """
    if not VPN_CHECK_ENABLED:
        return True
    
    return core_check_vpn(VPN_CHECK_HOST, VPN_CHECK_PORT, required=VPN_REQUIRED)


# =============================================================================
# PERFORMANCE FIX: Resource Monitoring and Memory Limits
# =============================================================================

MEMORY_LIMIT_MB = 2048  # 2GB hard limit
MEMORY_CHECK_INTERVAL = 100  # Check every 100 pages
_operation_count = 0

# Import scraper_utils for additional utilities
try:
    from scraper_utils import (
        get_memory_usage_mb, check_memory_limit, force_cleanup,
        log_resource_usage, format_duration
    )
    _SCRAPER_UTILS_AVAILABLE = True
except ImportError:
    _SCRAPER_UTILS_AVAILABLE = False
    def get_memory_usage_mb():
        try:
            import psutil
            process = psutil.Process(os.getpid())
            return process.memory_info().rss / 1024 / 1024
        except Exception:
            return 0.0
    def check_memory_limit():
        return False
    def force_cleanup():
        gc.collect()
    def log_resource_usage(prefix="[RESOURCE]"):
        pass
    def format_duration(seconds):
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        elif minutes > 0:
            return f"{minutes}m {secs}s"
        else:
            return f"{secs}s"

# =============================================================================
# GLOBAL STATE
# =============================================================================

_shutdown_requested = False
_active_drivers: List[webdriver.Chrome] = []
_drivers_lock = threading.Lock()
_run_id: Optional[str] = None
_repo: Optional[RussiaRepository] = None

# =============================================================================
# SIGNAL HANDLERS - Crash Guard
# =============================================================================

def save_state():
    """Save progress to DB on shutdown"""
    global _shutdown_requested
    _shutdown_requested = True
    if _repo and _run_id:
        try:
            _repo.finish_run("stopped", items_scraped=0)
            print(f"[SIGNAL] Run {_run_id} marked as stopped in DB", flush=True)
        except Exception as e:
            print(f"[SIGNAL] Warning: Could not update run status: {e}", flush=True)

def perform_cleanup():
    """Cleanup all Chrome instances"""
    cleanup_all_chrome()

# Register standardized signal handler
register_shutdown_handler(cleanup_func=perform_cleanup, save_state_func=save_state)

# =============================================================================
# CHROME INSTANCE TRACKING - Enhanced with Memory Management
# =============================================================================

# Start clean
from core.browser.chrome_manager import kill_orphaned_chrome_processes
kill_orphaned_chrome_processes()

# Register cleanup
from core.browser.chrome_manager import cleanup_all_chrome_instances
cleanup_all_chrome = cleanup_all_chrome_instances
atexit.register(cleanup_all_chrome_instances)

# =============================================================================
# DRIVER MANAGEMENT
# =============================================================================

def get_chromedriver_path() -> str:
    """Get ChromeDriver path with offline fallback"""
    # (Existing implementation kept for compatibility if needed, or remove if unused)
    # Since core has get_chromedriver_path now, we should preferably use that if available.
    # But let's keep the local one as fallback or remove it if core one is imported.
    # core.browser.chrome_manager has get_chromedriver_path.
    try:
        from core.browser.chrome_manager import get_chromedriver_path as core_get_path
        return core_get_path()
    except ImportError:
        pass

    import glob
    home = Path.home()
    wdm_cache_dir = home / ".wdm" / "drivers" / "chromedriver"
    if wdm_cache_dir.exists():
        patterns = [str(wdm_cache_dir / "**" / "chromedriver.exe"), str(wdm_cache_dir / "**" / "chromedriver")]
        for p in patterns:
            matches = glob.glob(p, recursive=True)
            if matches:
                return sorted(matches, key=os.path.getmtime, reverse=True)[0]
    try:
        return ChromeDriverManager().install()
    except Exception:
        pass
    return shutil.which("chromedriver") or ""


def _create_driver() -> webdriver.Chrome:
    """Internal factory: creates driver with core factory + DB tracking."""
    # 1. Cleanup orphans — SKIP when other workers' drivers are alive
    #    (kill_orphaned_chrome_processes kills ALL chrome, including sibling workers)
    with _drivers_lock:
        has_siblings = len(_active_drivers) > 0
    if not has_siblings:
        kill_orphaned_chrome_processes()
    
    # 2. Config
    ua = getenv("SCRIPT_01_CHROME_USER_AGENT")
    extra_opts = {'page_load_timeout': PAGE_LOAD_TIMEOUT}
    if ua: extra_opts['user_agent'] = ua
    
    # 3. Create
    driver = create_chrome_driver(headless=HEADLESS, extra_options=extra_opts)
    
    # 4. Track (thread-safe)
    with _drivers_lock:
        _active_drivers.append(driver)
    register_chrome_driver(driver)
    
    # DB Logging
    if ChromeInstanceTracker and _run_id and hasattr(driver, "service"):
        try:
            pid = driver.service.process.pid
            if pid:
                pids = get_chrome_pids_from_driver(driver) if get_chrome_pids_from_driver else {pid}
                with CountryDB("Russia") as db:
                     tracker = ChromeInstanceTracker("Russia", _run_id, db)
                     tracker.register(step_number=1, pid=pid, browser_type="chrome", child_pids=pids)
        except Exception as e:
            print(f"[WARN] DB tracking failed: {e}")
            
    print(f"[DRIVER] Created new instance (Total: {len(_active_drivers)})", flush=True)
    return driver


def _restart_driver(driver: webdriver.Chrome) -> webdriver.Chrome:
    """Restart driver with DB untracking and core restart logic."""
    print("[DRIVER] Restarting Chrome...", flush=True)
    
    # Untrack in DB (thread-safe)
    with _drivers_lock:
        if driver in _active_drivers:
            _active_drivers.remove(driver)
    
    if ChromeInstanceTracker and _run_id and hasattr(driver, "service"):
        try:
            pid = driver.service.process.pid
            if pid:
                with CountryDB("Russia") as db:
                    ChromeInstanceTracker("Russia", _run_id, db).mark_terminated_by_pid(pid, "restart")
        except Exception:
            pass
            
    unregister_chrome_driver(driver)
    
    # Core restart
    return core_restart_driver(driver, _create_driver)


# =============================================================================
# NAVIGATION WITH RETRY
# =============================================================================

def wait_for_table(driver: webdriver.Chrome) -> None:
    """Wait for table to load"""
    WebDriverWait(driver, WAIT_TIMEOUT).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table.report tbody tr"))
    )
    pause()


def navigate_with_retry(driver: webdriver.Chrome, url: str, label: str) -> webdriver.Chrome:
    """Navigate with comprehensive retry logic"""
    last_exc = None
    
    for attempt in range(1, NAV_RETRIES + 1):
        if _shutdown_requested:
            raise InterruptedError("Shutdown requested")
        
        try:
            driver.get(url)
            wait_for_table(driver)
            return driver
        except (TimeoutException, WebDriverException) as exc:
            last_exc = exc
            if attempt < NAV_RETRIES:
                print(f"  [WARN] {label} failed (attempt {attempt}/{NAV_RETRIES}). Retrying in {NAV_RETRY_SLEEP}s...", flush=True)
                time.sleep(NAV_RETRY_SLEEP)
    
    # Try driver restart
    if NAV_RESTART_DRIVER:
        print(f"  [WARN] {label} failed; restarting Chrome...", flush=True)
        driver = _restart_driver(driver)
        
        for attempt in range(1, NAV_RETRIES + 1):
            try:
                driver.get(url)
                wait_for_table(driver)
                return driver
            except (TimeoutException, WebDriverException) as exc:
                last_exc = exc
                if attempt < NAV_RETRIES:
                    time.sleep(NAV_RETRY_SLEEP)
    
    raise last_exc or RuntimeError(f"{label} failed")


def select_region_and_search(driver: webdriver.Chrome) -> webdriver.Chrome:
    """Select region and search with retry"""
    for attempt in range(1, NAV_RETRIES + 1):
        if _shutdown_requested:
            raise InterruptedError("Shutdown requested")
        
        try:
            print(f"  [NAV] Loading {BASE_URL}...", flush=True)
            driver.get(BASE_URL)
            
            # Wait for page to load - check for either reg_id or error
            print(f"  [NAV] Waiting for page elements (timeout={WAIT_TIMEOUT}s)...", flush=True)
            WebDriverWait(driver, WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.ID, "reg_id"))
            )
            
            print(f"  [NAV] Page loaded, selecting region {REGION_VALUE}...", flush=True)
            reg_select = Select(driver.find_element(By.ID, "reg_id"))
            reg_select.select_by_value(REGION_VALUE)
            
            print(f"  [NAV] Clicking search button...", flush=True)
            driver.find_element(By.ID, "btn_submit").click()
            
            print(f"  [NAV] Waiting for results table...", flush=True)
            wait_for_table(driver)
            print(f"  [NAV] Region selected and results loaded", flush=True)
            return driver
            
        except TimeoutException as exc:
            print(f"  [WARN] Timeout waiting for page elements (attempt {attempt}/{NAV_RETRIES})", flush=True)
            # Take screenshot for debugging
            try:
                screenshot_path = get_output_dir() / f"debug_timeout_attempt{attempt}.png"
                driver.save_screenshot(str(screenshot_path))
                print(f"  [DEBUG] Screenshot saved: {screenshot_path}", flush=True)
            except Exception:
                pass
            
            # Print page source for debugging
            try:
                page_title = driver.title
                current_url = driver.current_url
                print(f"  [DEBUG] Page title: {page_title}", flush=True)
                print(f"  [DEBUG] Current URL: {current_url}", flush=True)
            except Exception:
                pass
                
            if attempt < NAV_RETRIES:
                time.sleep(NAV_RETRY_SLEEP)
                
        except WebDriverException as exc:
            print(f"  [WARN] WebDriver error (attempt {attempt}/{NAV_RETRIES}): {exc}", flush=True)
            if attempt < NAV_RETRIES:
                time.sleep(NAV_RETRY_SLEEP)
    
    # Restart driver and try again
    if NAV_RESTART_DRIVER:
        print(f"  [NAV] Restarting driver and retrying...", flush=True)
        driver = _restart_driver(driver)
        return select_region_and_search(driver)
    
    raise RuntimeError("Region selection failed after all retries")


# =============================================================================
# DATA EXTRACTION
# =============================================================================

# parse_price imported from core


def extract_price_and_date(cell_text: str) -> tuple[str, str]:
    """Extract price and date from cell text like '531.51 \n03/15/2010'"""
    lines = [ln.strip() for ln in (cell_text or "").splitlines() if ln.strip()]
    if not lines:
        return "", ""
    price = lines[0].strip()
    date_text = lines[1].strip() if len(lines) > 1 else ""
    return price, date_text


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


def click_all_barcodes(driver: webdriver.Chrome, is_last_page: bool = False) -> None:
    """Click all barcode icons to trigger EAN insertion into package column"""
    if not FETCH_EAN:
        return
    
    # Wait for page to be fully loaded and stable
    time.sleep(1)
    
    # Scope to table.report tbody only (matches local page 52: 100 barcode links).
    # Use a[onclick*="getEanCode"] so we only click "Штрихкод" links, not other a.info.
    icons = driver.find_elements(By.CSS_SELECTOR, "table.report tbody a[onclick*='getEanCode']")
    if not icons:
        # Fallback: legacy selector (whole-page a.info)
        icons = driver.find_elements(By.CSS_SELECTOR, "a.info")
    if not icons:
        print(f"  [WARN] No barcode icons found on this page", flush=True)
        return
    
    # Warn if we don't see expected number of barcodes (but not on last page)
    if not is_last_page and len(icons) < 90:
        print(f"  [WARN] Only found {len(icons)} barcode icons. Expected ~100. Page may not have loaded fully.", flush=True)
    
    print(f"  Clicking {len(icons)} barcode icons...", flush=True)
    
    clicked = 0
    failed = 0
    for idx, ic in enumerate(icons):
        try:
            # Scroll into view first to ensure element is visible
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", ic)
            
            # Get onclick attribute
            onclick = ic.get_attribute("onclick")
            
            if onclick:
                # Try to execute the onclick JavaScript directly
                try:
                    driver.execute_script(onclick)
                    clicked += 1
                except Exception as e:
                    # Fallback to regular click
                    driver.execute_script("arguments[0].click();", ic)
                    clicked += 1
            else:
                # No onclick, use regular click
                driver.execute_script("arguments[0].click();", ic)
                clicked += 1
            
            # Small delay every 5 clicks to let async requests process
            if (idx + 1) % 5 == 0:
                time.sleep(0.3)
                
        except Exception as e:
            error_msg = str(e).lower()
            # Check if Chrome session is invalid
            if 'invalid session id' in error_msg or 'session deleted' in error_msg or 'no such window' in error_msg:
                print(f"  [WARN] Chrome session closed during barcode clicking. Stopping.", flush=True)
                break  # Stop trying to click more barcodes
            failed += 1
            if failed <= 3:
                print(f"  [DEBUG] Barcode click {idx} failed: {e}", flush=True)
            pass
    
    print(f"  Successfully clicked {clicked}/{len(icons)} barcode icons ({failed} failed)", flush=True)
    
    # Wait for all AJAX requests to complete
    # The gray rows are inserted asynchronously after clicking barcodes
    time.sleep(5)


def extract_row_data(row, page_num: int, driver=None) -> Optional[Dict]:
    """Extract data from a table row with EAN extraction"""
    try:
        cells = row.find_elements(By.TAG_NAME, "td")
        if len(cells) < 7:
            return None
        
        # Extract item_id from first cell onclick or linkhref
        item_id = ""
        try:
            # Try onclick attribute
            onclick = cells[0].get_attribute("onclick") or ""
            match = re.search(r"showInfo\((\d+)\)", onclick)
            if match:
                item_id = match.group(1)
            
            # Try img bullet with linkhref - parse item_id from query string
            # Example: frm_reestr_det.php?value=279.24&MnnName=...&item_id=31908
            if not item_id:
                bullet_imgs = cells[0].find_elements(By.CSS_SELECTOR, "img.bullet[linkhref]")
                if bullet_imgs:
                    linkhref = bullet_imgs[0].get_attribute("linkhref") or ""
                    # Parse query string to get item_id
                    from urllib.parse import parse_qs, urlparse
                    qs = parse_qs(urlparse("http://x/?" + linkhref.split("?", 1)[-1]).query)
                    item_id = (qs.get("item_id", [""]) or [""])[0]
        except Exception:
            pass
        
        if not item_id:
            return None
        
        # Extract text from cells (index 2-6 based on old scraper)
        tn = cells[2].text.strip() if len(cells) > 2 else ""
        inn = cells[3].text.strip() if len(cells) > 3 else ""
        manufacturer_country = cells[4].text.strip() if len(cells) > 4 else ""
        
        # Release form - remove trailing "Barcode" or EAN text
        release_form_full = cells[5].text.strip() if len(cells) > 5 else ""
        release_form = re.sub(r"\b(Barcode|\d{8,14})\b\s*$", "", release_form_full).strip()
        
        # Price and date from last cell
        price, date_text = "", ""
        if len(cells) > 6:
            price, date_text = extract_price_and_date(cells[6].text)
        
        # EAN extraction: Check next sibling row (gray row with EAN after barcode click)
        ean = ""
        if FETCH_EAN and driver:
            try:
                # Get next row after current main row
                next_tr = driver.execute_script("return arguments[0].nextElementSibling;", row)
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
            
            # Fallback: try extracting from current row if EAN was appended
            if not ean:
                ean = extract_ean(release_form_full)
        
        return {
            "item_id": item_id,
            "tn": tn,
            "inn": inn,
            "manufacturer_country": manufacturer_country,
            "release_form": release_form,
            "ean": ean,
            "registered_price_rub": price,
            "start_date_text": date_text,
            "page_number": page_num,
        }
    except Exception as e:
        print(f"  [WARN] Error extracting row: {e}", flush=True)
        return None


# =============================================================================
# MAIN SCRAPING LOGIC
# =============================================================================

def extract_rows_from_table(driver: webdriver.Chrome, page_num: int = 0, last_page: int = 0) -> list[Dict]:
    """
    Internal function to extract rows from the current page table.
    This is the core extraction logic that can be called multiple times for retry.
    """
    rows: list[Dict] = []
    
    # Re-fetch rows AFTER barcode clicks (new rows inserted)
    tr_list = driver.find_elements(By.CSS_SELECTOR, "table.report tbody tr")
    total_rows = len(tr_list)
    
    # Debug: Count main rows vs gray rows
    main_rows = 0
    gray_rows = 0
    for tr in tr_list:
        bullet_imgs = tr.find_elements(By.CSS_SELECTOR, "img.bullet[linkhref]")
        if bullet_imgs:
            main_rows += 1
        else:
            gray_rows += 1
    
    is_last_page = (page_num == last_page and last_page > 0)
    
    if FETCH_EAN:
        print(f"  Processing {total_rows} rows (after EAN insertion)...", flush=True)
        print(f"  [DEBUG] Main rows: {main_rows}, Gray/EAN rows: {gray_rows}", flush=True)
        
        # If we have main rows but no gray rows, the barcode click didn't work
        # Only warn if we expect EAN rows (i.e., we have main rows to match)
        if main_rows > 0 and gray_rows == 0 and main_rows >= 10:
            print(f"  [WARN] No gray/EAN rows found after barcode click. EAN extraction may fail.", flush=True)
    
    # Warning if we don't see expected number of main rows
    # Don't warn on last page (may have fewer rows) or if page_num is 0 (retry/unknown)
    if not is_last_page and page_num > 0 and main_rows < 90:
        print(f"  [WARNING] Expected ~100 main rows, found {main_rows}. Page navigation issue?", flush=True)
    
    extracted_count = 0
    skipped_no_bullet = 0
    skipped_no_item_id = 0
    skipped_exception = 0
    
    for idx, tr in enumerate(tr_list, 1):
        try:
            # Only main rows have the bullet image with linkhref
            bullet_imgs = tr.find_elements(By.CSS_SELECTOR, "img.bullet[linkhref]")
            if not bullet_imgs:
                # Skip EAN-only rows (gray rows inserted after barcode click)
                skipped_no_bullet += 1
                continue
            
            linkhref = bullet_imgs[0].get_attribute("linkhref") or ""
            # Parse item_id from query string
            # Example: frm_reestr_det.php?value=279.24&MnnName=...&item_id=31908
            from urllib.parse import parse_qs, urlparse
            qs = parse_qs(urlparse("http://x/?" + linkhref.split("?", 1)[-1]).query)
            item_id = (qs.get("item_id", [""]) or [""])[0]
            
            if not item_id:
                skipped_no_item_id += 1
                if skipped_no_item_id <= 3:
                    print(f"  [DEBUG] Row {idx}: No item_id from linkhref: {linkhref[:50]}...", flush=True)
                continue
            
            tds = tr.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) < 7:
                if idx <= 3:
                    print(f"  [DEBUG] Row {idx}: Only {len(tds)} cells, skipping", flush=True)
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
            if FETCH_EAN:
                try:
                    # Get next row after current main row
                    next_tr = driver.execute_script("return arguments[0].nextElementSibling;", tr)
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
                
                # Fallback: try extracting from current row if EAN was appended
                if not ean:
                    ean = extract_ean(release_form_full)
            
            rows.append({
                "item_id": item_id,
                "tn": tn,
                "inn": inn,
                "manufacturer_country": manufacturer_country,
                "release_form": release_form,
                "ean": ean,
                "registered_price_rub": price,
                "start_date_text": date_text,
                "page_number": page_num,
            })
            extracted_count += 1
            
        except StaleElementReferenceException:
            # Page changed mid-loop; skip this row
            continue
        except Exception as e:
            skipped_exception += 1
            error_msg = str(e).lower()
            # Check if it's an invalid session error (Chrome closed)
            if 'invalid session id' in error_msg or 'session deleted' in error_msg or 'no such window' in error_msg:
                print(f"  [WARN] Chrome session closed. Stopping extraction.", flush=True)
                break  # Stop trying to extract more rows
            if extracted_count < 3:
                print(f"  [DEBUG] Row {idx}: Exception: {e}", flush=True)
            continue
    
    # Print extraction summary
    if main_rows > 0:
        print(f"  [DEBUG] Extraction summary: {extracted_count} extracted, {skipped_no_bullet} no bullet, {skipped_no_item_id} no item_id, {skipped_exception} exceptions", flush=True)
    
    return rows


def scrape_page_with_ean_validation(driver: webdriver.Chrome, page_num: int, repo: RussiaRepository, existing_ids: Set[str], last_page: int = 0) -> tuple[int, int, int, int, int, bool]:
    """
    Scrape a single page with strict EAN validation.
    
    MEMORY FIX: Uses DB-backed deduplication when existing_ids set grows too large.
    """
    """
    Scrape a single page with strict EAN validation.
    
    STRICT: We need exactly 100 EANs per full page (row_count == ean_count).
    If less than 100 EANs, retry barcode click + extraction up to EAN_CLICK_RETRIES (5) times.
    After 5 attempts still < 100 EANs: mark page for later retry (don't write data).
    
    Returns: (scraped_count, skipped_count, missing_ean_count, rows_found, ean_found, is_valid)
    """
    if _shutdown_requested:
        return 0, 0, 0, 0, 0, False
    
    if page_num == 1:
        print(f"  [DEBUG] FETCH_EAN = {FETCH_EAN}, EAN_CLICK_RETRIES = {EAN_CLICK_RETRIES}", flush=True)
    
    is_last_page = (page_num == last_page and last_page > 0)
    rows = []
    
    if FETCH_EAN:
        # Strictly 100 EANs: retry barcode click + extract up to EAN_CLICK_RETRIES times
        for attempt in range(1, EAN_CLICK_RETRIES + 1):
            if _shutdown_requested:
                return 0, 0, 0, 0, 0, False
            click_all_barcodes(driver, is_last_page)
            rows = extract_rows_from_table(driver, page_num, last_page)
            if not rows:
                print(f"  [EAN RETRY {attempt}/{EAN_CLICK_RETRIES}] No rows extracted", flush=True)
                if attempt < EAN_CLICK_RETRIES:
                    time.sleep(5)
                continue
            row_count = len(rows)
            ean_count = sum(1 for r in rows if r.get("ean"))
            missing_count = row_count - ean_count
            print(f"  [VALIDATION] Attempt {attempt}/{EAN_CLICK_RETRIES} | Rows: {row_count} | With EAN: {ean_count} | Missing: {missing_count}", flush=True)
            # Strict: every row must have EAN (row_count == ean_count). Full page = 100.
            if row_count == ean_count:
                print(f"  [VALIDATION SUCCESS] All {ean_count} rows have EAN", flush=True)
                break
            if attempt < EAN_CLICK_RETRIES:
                print(f"  [EAN RETRY] Need {row_count} EANs, got {ean_count}. Waiting 5s and re-clicking barcodes...", flush=True)
                time.sleep(5)
        else:
            # All retries exhausted
            row_count = len(rows)
            ean_count = sum(1 for r in rows if r.get("ean")) if rows else 0
            missing_count = row_count - ean_count if rows else 0
            print(f"  [VALIDATION FAILED] After {EAN_CLICK_RETRIES} attempts: {ean_count} EANs (need 100). DATA NOT WRITTEN", flush=True)
            repo.record_failed_page(page_num, "ved", f"EAN validation failed: {missing_count} missing after {EAN_CLICK_RETRIES} retries")
            return 0, 0, missing_count, row_count, ean_count, False
    else:
        # No EAN fetch: single extract
        rows = extract_rows_from_table(driver, page_num, last_page)
    
    # STRICT CHECK: Must have exactly 100 rows (except for last page)
    rows_found = len(rows)
    ean_found = sum(1 for r in rows if r.get("ean"))
    is_last_page = (page_num == last_page and last_page > 0)
    
    if not is_last_page and rows_found < 100:
        print(f"  [VALIDATION FAILED] Page {page_num} has only {rows_found} rows (expected 100). DATA NOT WRITTEN", flush=True)
        repo.record_failed_page(page_num, "ved", f"Row count validation failed: {rows_found} rows (expected 100)")
        return 0, 0, 0, rows_found, ean_found, False
    
        # Process extracted rows
    scraped = 0
    skipped = 0
    missing_ean_count = 0
    batch = []
    
    # STRICT: No deduplication - insert all scraped records as-is
    for data in rows:
        if _shutdown_requested:
            break
        
        # Track missing EAN
        if FETCH_EAN and not data.get("ean"):
            missing_ean_count += 1
        
        # Ensure page_number is set
        data["page_number"] = page_num
        
        batch.append(data)
        
        # Bulk insert when batch is full
        if len(batch) >= DB_BATCH_SIZE:
            repo.insert_ved_products_bulk(batch)
            scraped += len(batch)
            audit_log("INSERT_BATCH", scraper_name="Russia", run_id=_run_id, details={"inserted": len(batch), "page": page_num})
            batch = []
    
    # Insert remaining
    if batch:
        repo.insert_ved_products_bulk(batch)
        scraped += len(batch)
        audit_log("INSERT_BATCH", scraper_name="Russia", run_id=_run_id, details={"inserted": len(batch), "page": page_num})
    
    return scraped, skipped, missing_ean_count, rows_found, ean_found, True


def scrape_page(driver: webdriver.Chrome, page_num: int, repo: RussiaRepository, existing_ids: Set[str], last_page: int = 0) -> tuple[int, int, int]:
    """
    Scrape a single page with EAN extraction and validation.
    Returns: (scraped_count, skipped_count, missing_ean_count)
    No deduplication - all scraped records are inserted.
    """
    # Verify we're on the correct page by checking URL
    current_url = driver.current_url
    expected_page_param = f"page={page_num}"
    if expected_page_param not in current_url:
        print(f"  [WARN] URL mismatch! Expected page {page_num} but URL is: {current_url}")
        print(f"  [WARN] Navigating to correct page...")
        try:
            correct_url = f"{BASE_URL}?page={page_num}&reg_id={REGION_VALUE}"
            driver.get(correct_url)
            wait_for_table(driver)
            print(f"  [OK] Navigated to correct page {page_num}")
        except Exception as e:
            print(f"  [ERROR] Failed to navigate to page {page_num}: {e}")
            repo.record_failed_page(page_num, "ved", f"URL verification failed: {current_url}")
            return 0, 0, 0
    
    # Get initial DB count for this page
    initial_count = repo.get_ved_product_count_for_page(page_num)
    
    try:
        # MEMORY FIX: Pass repo for DB-backed deduplication
        scraped, skipped, missing_ean_count, rows_found, ean_found, is_valid = scrape_page_with_ean_validation(
            driver, page_num, repo, existing_ids, last_page
        )
        
        # Get final DB count for this page
        final_count = repo.get_ved_product_count_for_page(page_num)
        inserted = final_count - initial_count
        
        # Verify data integrity - if we expected to insert rows but DB count didn't change, something went wrong
        if scraped > 0 and inserted == 0:
            print(f"  [WARN] Page {page_num}: Scraped {scraped} rows but DB count unchanged. Data may have been lost!")
            # Don't mark as completed - this page needs to be retried
            repo.record_failed_page(page_num, "ved", f"Data loss detected: scraped={scraped}, inserted={inserted}")
            return scraped, skipped, missing_ean_count
        
        # Build metrics dictionary for structured storage
        metrics = {
            'rows_found': rows_found,
            'ean_found': ean_found,
            'rows_scraped': scraped,
            'rows_inserted': inserted,
            'ean_missing': missing_ean_count,
            'db_count_before': initial_count,
            'db_count_after': final_count,
        }
        
        # Build log details string for quick viewing
        log_details = (
            f"rows_found={rows_found}, "
            f"ean_found={ean_found}, "
            f"scraped={scraped}, "
            f"inserted={inserted}, "
            f"ean_missing={missing_ean_count}"
        )
        
        # Build the URL for this page
        page_url = f"{BASE_URL}?page={page_num}&reg_id={REGION_VALUE}"
        
        if is_valid:
            # Mark page as completed
            repo.mark_progress(1, "VED Scrape", f"ved_page:{page_num}", "completed", 
                             log_details=log_details, metrics=metrics, url=page_url)
        else:
            # Mark page for retry
            repo.mark_progress(1, "VED Scrape", f"ved_page:{page_num}", "ean_missing", 
                             f"Missing EAN: {missing_ean_count}", 
                             log_details=log_details, metrics=metrics, url=page_url)
        
        return scraped, skipped, missing_ean_count
        
    except Exception as e:
        # Record failed page for retry
        repo.record_failed_page(page_num, "ved", str(e))
        metrics = {
            'rows_found': 0, 'ean_found': 0, 'rows_scraped': 0,
            'rows_inserted': 0, 'ean_missing': 0,
            'db_count_before': initial_count, 'db_count_after': initial_count,
        }
        repo.mark_progress(1, "VED Scrape", f"ved_page:{page_num}", "failed", str(e), 
                         log_details=f"error={type(e).__name__}", metrics=metrics)
        raise


def get_last_page(driver: webdriver.Chrome) -> int:
    """Get total number of pages from pagination"""
    try:
        pager = driver.find_element(By.CSS_SELECTOR, "p.paging")
        links = pager.find_elements(By.TAG_NAME, "a")
        for link in reversed(links):
            text = link.text.strip()
            match = re.search(r"\[(\d+)\]", text)
            if match:
                return int(match.group(1))
    except Exception:
        pass
    
    # Fallback: scan for page= in URLs
    try:
        links = driver.find_elements(By.CSS_SELECTOR, "p.paging a")
        max_page = 1
        for link in links:
            href = link.get_attribute("href") or ""
            match = re.search(r"page=(\d+)", href)
            if match:
                max_page = max(max_page, int(match.group(1)))
        return max_page
    except Exception:
        return 1


def ensure_tabs_and_load_pages(driver: webdriver.Chrome, page_numbers: list[int]) -> list:
    """
    Ensure we have len(page_numbers) tabs, load each tab with its page URL (kick off loads),
    then wait for table in each tab. Returns list of window handles in order (tab 0 = page_numbers[0], etc.).
    """
    handles = list(driver.window_handles)
    n = len(page_numbers)
    # Open new tabs if we need more
    while len(handles) < n:
        driver.execute_script("window.open('');")
        handles = list(driver.window_handles)
    handles = handles[:n]
    base_url = f"{BASE_URL}?reg_id={REGION_VALUE}"
    # Kick off all loads (don't wait)
    for i, (h, p) in enumerate(zip(handles, page_numbers)):
        driver.switch_to.window(h)
        driver.get(f"{base_url}&page={p}")
    # Wait for table in each tab
    for i, h in enumerate(handles):
        driver.switch_to.window(h)
        try:
            wait_for_table(driver)
        except Exception as e:
            print(f"  [WARN] Tab {i+1} (page {page_numbers[i]}) table wait failed: {e}", flush=True)
    return handles


def get_resume_page_and_run_id(repo: RussiaRepository, exclude_run_id: str = None) -> tuple[int, Optional[str]]:
    """
    Find run with most completed pages and the next page to scrape.
    Returns (resume_page, best_run_id). best_run_id is None if no previous run to resume.
    """
    run_pages = repo.get_all_run_completed_pages(1, exclude_run_id)
    print(f"  [DEBUG] Found {len(run_pages)} previous runs with completed pages", flush=True)
    
    if not run_pages:
        return 1, None
    
    best_run_id = None
    max_pages = 0
    for run_id, pages in run_pages.items():
        if pages > max_pages:
            max_pages = pages
            best_run_id = run_id
    
    if not best_run_id or max_pages == 0:
        return 1, None
    
    print(f"  [DEBUG] Best run to resume from: {best_run_id} with {max_pages} pages", flush=True)
    
    completed = repo.get_completed_keys_for_run(1, best_run_id)
    pages = []
    for key in completed:
        if key.startswith("ved_page:"):
            try:
                pages.append(int(key.split(":")[1]))
            except ValueError:
                pass
    
    print(f"  [DEBUG] Page numbers: {sorted(pages)[:10]}...", flush=True)
    
    resume_page = max(pages) + 1 if pages else 1
    print(f"  [DEBUG] Will resume from page: {resume_page} using run_id: {best_run_id}", flush=True)
    
    return resume_page, best_run_id


def get_next_pages_to_scrape(repo: RussiaRepository, last_page: int, batch_size: int = 5) -> List[int]:
    """
    Get next batch of pages to scrape.
    
    Logic:
    1. Consider all pages from 1 to last_page
    2. Exclude pages already completed (in step_progress with status='completed')
    3. Exclude pages in failed_pages (will be handled by retry logic)
    4. Return up to batch_size pages (not necessarily sequential)
    
    Returns: List of page numbers to scrape
    """
    # Get completed pages
    completed_keys = repo.get_completed_keys(1)
    completed_pages = set()
    for key in completed_keys:
        if key.startswith("ved_page:"):
            try:
                completed_pages.add(int(key.split(":")[1]))
            except ValueError:
                pass
    
    # Get failed pages (pending retry)
    failed_pages_data = repo.get_failed_pages("ved")
    failed_pages = set(p['page_number'] for p in failed_pages_data)
    
    # Find pages that need scraping
    all_pages = set(range(1, last_page + 1))
    pages_to_scrape = sorted(all_pages - completed_pages - failed_pages)
    
    # Return up to batch_size pages
    return pages_to_scrape[:batch_size]


def get_existing_item_ids_all_runs(repo: RussiaRepository) -> Set[str]:
    """Get all existing item_ids from ALL runs for deduplication"""
    return repo.get_all_existing_item_ids()


# =============================================================================
# PARALLEL WORKER (each worker = 1 Chrome instance, claims pages from DB)
# =============================================================================

def worker_loop(worker_id: int, last_page: int, existing_ids: Set[str], run_id: str):
    """
    Worker thread: creates its own Chrome driver, claims pages one-by-one
    from the DB, scrapes each, and marks completion.
    """
    global _shutdown_requested

    tag = f"[W{worker_id}]"
    print(f"{tag} Starting Chrome instance...", flush=True)

    # Each worker gets its own DB connection + repo
    worker_db = CountryDB("Russia")
    worker_repo = RussiaRepository(worker_db, run_id)

    driver = None
    pages_done = 0
    total_scraped = 0
    consecutive_failures = 0
    MAX_CONSECUTIVE_FAILURES = 5

    try:
        driver = _create_driver()
        driver = select_region_and_search(driver)
        audit_log("ACTION", scraper_name="Russia", run_id=run_id,
                  details={"action": "WORKER_STARTED", "worker": worker_id, "region": REGION_VALUE})
        print(f"{tag} Chrome ready, starting page claims...", flush=True)

        while not _shutdown_requested:
            # Claim next available page atomically
            page_num = worker_repo.claim_next_page(last_page, step_number=1, source_type="ved")
            if page_num is None:
                print(f"{tag} No more pages to claim. Done.", flush=True)
                break

            print(f"{tag} Claimed page {page_num}/{last_page}", flush=True)

            # Navigate to page
            page_url = f"{BASE_URL}?page={page_num}&reg_id={REGION_VALUE}"

            for attempt in range(1, MAX_RETRIES_PER_PAGE + 1):
                try:
                    driver.get(page_url)
                    wait_for_table(driver)
                    scraped, skipped, missing_ean = scrape_page(
                        driver, page_num, worker_repo, existing_ids, last_page
                    )
                    total_scraped += scraped
                    pages_done += 1
                    consecutive_failures = 0
                    audit_log("PAGE_FETCHED", scraper_name="Russia", run_id=run_id,
                              details={"page": page_num, "scraped": scraped,
                                       "skipped": skipped, "missing_ean": missing_ean,
                                       "worker": worker_id})

                    ean_info = f", EAN missing: {missing_ean}" if (FETCH_EAN and missing_ean > 0) else ""
                    print(f"{tag} Page {page_num}: scraped {scraped}{ean_info} (total: {pages_done} pages)", flush=True)
                    break

                except (TimeoutException, WebDriverException) as e:
                    if attempt < MAX_RETRIES_PER_PAGE:
                        print(f"{tag} Page {page_num} attempt {attempt} failed: {e}. Retrying...", flush=True)
                        time.sleep(2)
                        try:
                            driver.refresh()
                            time.sleep(2)
                        except Exception:
                            pass
                    else:
                        print(f"{tag} Page {page_num} failed after {MAX_RETRIES_PER_PAGE} attempts: {e}", flush=True)
                        try:
                            worker_repo.record_failed_page(page_num, "ved", str(e))
                        except Exception as rec_err:
                            print(f"{tag} Could not record failed page {page_num}: {rec_err}", flush=True)
                        consecutive_failures += 1

                except Exception as e:
                    print(f"{tag} Page {page_num} error: {e}", flush=True)
                    try:
                        worker_repo.record_failed_page(page_num, "ved", str(e))
                    except Exception as rec_err:
                        print(f"{tag} Could not record failed page {page_num}: {rec_err}", flush=True)
                    consecutive_failures += 1
                    break

            # Restart Chrome if too many consecutive failures
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                print(f"{tag} {consecutive_failures} consecutive failures, restarting Chrome...", flush=True)
                try:
                    driver = _restart_driver(driver)
                    driver = select_region_and_search(driver)
                    consecutive_failures = 0
                except Exception as e:
                    print(f"{tag} Driver restart failed: {e}. Exiting worker.", flush=True)
                    break

            # Periodic memory check
            if pages_done > 0 and pages_done % MEMORY_CHECK_INTERVAL == 0:
                mem_mb = get_memory_usage_mb()
                print(f"{tag} Memory: {mem_mb:.1f}MB after {pages_done} pages", flush=True)
                force_cleanup()
                if check_memory_limit():
                    print(f"{tag} Memory limit hit, restarting Chrome...", flush=True)
                    driver = _restart_driver(driver)
                    driver = select_region_and_search(driver)

            # Small sleep between pages
            if SLEEP_BETWEEN_PAGES > 0:
                time.sleep(SLEEP_BETWEEN_PAGES)

    except Exception as e:
        print(f"{tag} Fatal error: {e}", flush=True)
    finally:
        # Mark Chrome instance as terminated in chrome_instances table
        if driver and ChromeInstanceTracker and run_id:
            try:
                pid = driver.service.process.pid if hasattr(driver, "service") else None
                if pid:
                    with CountryDB("Russia") as tracker_db:
                        ChromeInstanceTracker("Russia", run_id, tracker_db).mark_terminated_by_pid(
                            pid, f"worker_{worker_id}_done"
                        )
            except Exception:
                pass

        # Clean up this worker's Chrome driver
        if driver:
            try:
                with _drivers_lock:
                    if driver in _active_drivers:
                        _active_drivers.remove(driver)
                unregister_chrome_driver(driver)
                driver.quit()
            except Exception:
                pass

        # Return DB connection to pool
        try:
            worker_db.close()
        except Exception:
            pass

        audit_log("ACTION", scraper_name="Russia", run_id=run_id,
                  details={"action": "WORKER_FINISHED", "worker": worker_id,
                           "pages": pages_done, "scraped": total_scraped})
        print(f"{tag} Finished. Scraped {total_scraped} items across {pages_done} pages.", flush=True)


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def _parse_args() -> tuple[Optional[List[int]], bool, Optional[str]]:
    """
    Parse command-line arguments.
    Returns: (retry_pages, fresh_run, run_id)
    - retry_pages: List of page numbers to retry (from --start/--end), or None for full run
    - fresh_run: True if --fresh flag is present
    - run_id: Specific run_id from --run-id, or None
    """
    start = end = None
    fresh_run = False
    run_id = None
    
    args = sys.argv[1:]
    for i, a in enumerate(args):
        if a == "--start" and i + 1 < len(args):
            try:
                start = int(args[i + 1])
            except ValueError:
                pass
        elif a == "--end" and i + 1 < len(args):
            try:
                end = int(args[i + 1])
            except ValueError:
                pass
        elif a == "--fresh":
            fresh_run = True
        elif a == "--run-id" and i + 1 < len(args):
            run_id = args[i + 1]
    
    retry_pages = None
    if start is not None and end is not None and start <= end:
        retry_pages = list(range(start, end + 1))
    
    return retry_pages, fresh_run, run_id


def main():
    global _run_id, _repo
    
    retry_pages, fresh_run, specified_run_id = _parse_args()
    
    print("=" * 80)
    print("Russia VED Scraper - DB-Based with Resume Support")
    print("=" * 80)
    
    # VPN Check (optional)
    if VPN_CHECK_ENABLED:
        print("[INIT] VPN check enabled, verifying connection...", flush=True)
        if not check_vpn_connection():
            print("[FATAL] VPN connection check failed. Please connect VPN or set VPN_CHECK_ENABLED=false", flush=True)
            sys.exit(1)
    elif VPN_REQUIRED:
        print("[INIT] VPN required but check disabled. Ensure VPN is connected.", flush=True)
    else:
        print("[INIT] VPN not required, running without VPN check", flush=True)
    
    # Initialize DB
    print("[INIT] Connecting to database...", flush=True)
    db = CountryDB("Russia")
    apply_russia_schema(db)
    
    # Resolve run_id based on command-line arguments
    if fresh_run:
        # --fresh flag: Always start a new run
        _run_id = generate_run_id()
        print(f"[INIT] --fresh flag detected, starting new run: {_run_id}", flush=True)
    elif specified_run_id:
        # --run-id specified: Use the specified run_id
        _run_id = specified_run_id
        print(f"[INIT] Using specified run_id: {_run_id}", flush=True)
    else:
        # Prefer pipeline-provided run_id (RUSSIA_RUN_ID or .current_run_id) when resuming
        pipeline_run_id = (os.getenv("RUSSIA_RUN_ID") or "").strip()
        if not pipeline_run_id:
            run_id_file = get_output_dir() / ".current_run_id"
            if run_id_file.exists():
                try:
                    pipeline_run_id = run_id_file.read_text(encoding="utf-8").strip()
                except Exception:
                    pass
        
        if pipeline_run_id:
            # Use pipeline's run_id - get resume page for this specific run
            lookup_repo = RussiaRepository(db, pipeline_run_id)
            resume_page, _ = get_resume_page_and_run_id(lookup_repo, exclude_run_id=None)
            _run_id = pipeline_run_id
            print(f"[INIT] Resuming: using pipeline run_id {_run_id}", flush=True)
        else:
            # No pipeline run_id - fall back to best run from DB
            lookup_repo = RussiaRepository(db, "_lookup")
            resume_page, best_run_id = get_resume_page_and_run_id(lookup_repo, exclude_run_id=None)
            if resume_page > 1 and best_run_id:
                _run_id = best_run_id
                print(f"[INIT] Resuming: using best run from DB {_run_id}", flush=True)
            else:
                _run_id = generate_run_id()
                print(f"[INIT] Fresh run: new run_id {_run_id}", flush=True)
    
    os.environ["RUSSIA_RUN_ID"] = _run_id
    
    # Persist run_id to file so it survives pipeline restarts (e.g., from Telegram)
    run_id_file = get_output_dir() / ".current_run_id"
    try:
        run_id_file.parent.mkdir(parents=True, exist_ok=True)
        run_id_file.write_text(_run_id, encoding="utf-8")
        print(f"[INIT] Saved run_id to {run_id_file}", flush=True)
    except Exception as e:
        print(f"[INIT] Warning: could not save run_id to file: {e}", flush=True)
    
    _repo = RussiaRepository(db, _run_id)
    audit_log("RUN_STARTED", scraper_name="Russia", run_id=_run_id, details={"region": REGION_VALUE, "max_pages": MAX_PAGES})

    # Start or resume run based on whether it's a fresh run
    if fresh_run:
        print(f"[INIT] Starting fresh run (--fresh flag)", flush=True)
        _repo.start_run("fresh")
    elif specified_run_id:
        # Check if this run exists and has data
        completed = len(_repo.get_completed_keys(1))
        if completed > 0:
            print(f"[INIT] Resuming specified run with {completed} pages already completed", flush=True)
            _repo.resume_run()
        else:
            print(f"[INIT] Starting specified run (no previous data)", flush=True)
            _repo.start_run("fresh")
    else:
        # Default resume logic
        lookup_repo = RussiaRepository(db, _run_id)
        resume_page, _ = get_resume_page_and_run_id(lookup_repo, exclude_run_id=None)
        
        if resume_page > 1:
            completed = len(_repo.get_completed_keys(1))
            print(f"[INIT] Resuming run with {completed} pages already completed", flush=True)
            _repo.resume_run()
        else:
            print(f"[INIT] Starting fresh run", flush=True)
            _repo.start_run("fresh")
    
    # Initialize first driver (always needed to discover last_page)
    print("[INIT] Starting Chrome...", flush=True)
    driver = _create_driver()

    try:
        # Navigate and select region
        print(f"[NAV] Navigating to {BASE_URL}...", flush=True)
        driver = select_region_and_search(driver)
        audit_log("ACTION", scraper_name="Russia", run_id=_run_id, details={"action": "REGION_SELECTED", "region": REGION_VALUE})

        # Get total pages
        last_page = get_last_page(driver)
        if MAX_PAGES > 0:
            last_page = min(last_page, MAX_PAGES)
        print(f"[INFO] Total pages to scrape: {last_page}", flush=True)

        # Show how many pages are already done
        completed = len(_repo.get_completed_keys(1))
        print(f"[INFO] {completed} pages already completed, {last_page - completed} pages remaining", flush=True)

        # No deduplication - existing_ids kept for API compatibility
        existing_ids = set()

        global _operation_count
        initial_retry_mode = retry_pages is not None

        # =====================================================================
        # PARALLEL MODE: NUM_WORKERS > 1 and not retry mode
        # =====================================================================
        if NUM_WORKERS > 1 and not initial_retry_mode:
            print(f"\n[PARALLEL] Launching {NUM_WORKERS} Chrome workers...", flush=True)

            # Close the discovery driver — workers create their own
            try:
                # Mark terminated in chrome_instances table
                if ChromeInstanceTracker and _run_id and hasattr(driver, "service"):
                    try:
                        pid = driver.service.process.pid
                        if pid:
                            with CountryDB("Russia") as tracker_db:
                                ChromeInstanceTracker("Russia", _run_id, tracker_db).mark_terminated_by_pid(
                                    pid, "discovery_driver_closed"
                                )
                    except Exception:
                        pass
                with _drivers_lock:
                    if driver in _active_drivers:
                        _active_drivers.remove(driver)
                unregister_chrome_driver(driver)
                driver.quit()
            except Exception:
                pass
            driver = None

            # Spawn worker threads
            threads = []
            for i in range(NUM_WORKERS):
                t = threading.Thread(
                    target=worker_loop,
                    args=(i + 1, last_page, existing_ids, _run_id),
                    name=f"Worker-{i + 1}",
                    daemon=False,
                )
                t.start()
                threads.append(t)
                time.sleep(2)  # Stagger Chrome starts to avoid port conflicts

            print(f"[PARALLEL] All {NUM_WORKERS} workers running. Waiting for completion...", flush=True)

            # Wait for all workers
            for t in threads:
                t.join()

            print(f"[PARALLEL] All workers finished.", flush=True)

            # Get actual scraped count from DB (workers wrote directly to DB)
            parallel_total = _repo.get_ved_product_count()
            _repo.finish_run("completed", items_scraped=parallel_total)
            print(f"\n[COMPLETE] Scraped {parallel_total} items across {NUM_WORKERS} workers (no deduplication)", flush=True)

        # =====================================================================
        # SINGLE MODE: Legacy multi-tab batch loop (NUM_WORKERS <= 1 or retry)
        # =====================================================================
        else:
            # BATCH SIZE: number of pages per batch (from env or default 10)
            BATCH_SIZE = MULTI_TAB_BATCH if MULTI_TAB_BATCH > 0 else 10

            # Main scraping loop - dynamically get pages to scrape
            total_scraped = 0
            total_skipped = 0

            # Safety counters to prevent infinite loops (defensive programming)
            empty_batch_count = 0
            total_iteration_count = 0
            consecutive_timeout_batches = 0
            MAX_TIMEOUT_BATCHES = 3  # Restart driver after 3 consecutive batches with timeouts

            while True:
                # Check shutdown signal
                if _shutdown_requested:
                    print("[SHUTDOWN] Stopping gracefully...", flush=True)
                    break

                # Safety limit: Check total iterations
                if MAX_TOTAL_ITERATIONS > 0 and total_iteration_count >= MAX_TOTAL_ITERATIONS:
                    print(f"[SAFETY EXIT] Reached maximum total iterations ({MAX_TOTAL_ITERATIONS}), exiting to prevent infinite loop", flush=True)
                    break

                total_iteration_count += 1

                # Retry mode (--start N --end M): scrape only that range once then exit
                was_retry_batch = False
                if retry_pages is not None:
                    pages_to_scrape = retry_pages
                    retry_pages = None
                    was_retry_batch = True
                    print(f"[RETRY] Scraping only requested pages: {pages_to_scrape}", flush=True)
                else:
                    pages_to_scrape = get_next_pages_to_scrape(_repo, last_page, BATCH_SIZE)

                if not pages_to_scrape:
                    empty_batch_count += 1
                    if empty_batch_count >= MAX_EMPTY_BATCH_ITERATIONS:
                        print(f"[SAFETY EXIT] get_next_pages_to_scrape() returned empty {empty_batch_count} times, exiting to prevent infinite loop", flush=True)
                        print("[COMPLETE] All pages have been scraped!")
                        break
                    else:
                        print(f"[WARNING] get_next_pages_to_scrape() returned empty (attempt {empty_batch_count}/{MAX_EMPTY_BATCH_ITERATIONS})", flush=True)
                        time.sleep(1)  # Brief pause before retrying
                        continue

                # Reset empty counter on successful batch
                empty_batch_count = 0

                print(f"[BATCH] Loading {len(pages_to_scrape)} pages in parallel: {pages_to_scrape}", flush=True)

                batch_scraped = 0
                batch_skipped = 0

                # Open multiple tabs and load all pages simultaneously
                handles = []
                base_url = f"{BASE_URL}?reg_id={REGION_VALUE}"

                # Ensure we have enough tabs
                while len(driver.window_handles) < len(pages_to_scrape):
                    driver.execute_script("window.open('');")

                handles = driver.window_handles[:len(pages_to_scrape)]

                # Track failed page loads
                failed_loads = []

                # Load all pages simultaneously (kick off loads)
                for i, page_num in enumerate(pages_to_scrape):
                    try:
                        driver.switch_to.window(handles[i])
                        page_url = f"{BASE_URL}?page={page_num}&reg_id={REGION_VALUE}"
                        driver.get(page_url)  # Start loading (non-blocking)
                        # Small delay to prevent overwhelming browser
                        if i < len(pages_to_scrape) - 1:
                            time.sleep(0.1)
                    except TimeoutException as e:
                        print(f"  [WARN] Page {page_num} timed out during load: {e}", flush=True)
                        failed_loads.append(page_num)
                    except WebDriverException as e:
                        print(f"  [WARN] Page {page_num} failed to load: {e}", flush=True)
                        failed_loads.append(page_num)

                # Now wait for all pages to load and scrape each one
                for i, page_num in enumerate(pages_to_scrape):
                    if _shutdown_requested:
                        break

                    # Skip pages that failed to load
                    if page_num in failed_loads:
                        print(f"  [SKIP] Page {page_num} skipped due to load failure", flush=True)
                        continue

                    driver.switch_to.window(handles[i])

                    # Retry logic for page processing
                    MAX_PAGE_RETRIES = 3
                    for attempt in range(1, MAX_PAGE_RETRIES + 1):
                        try:
                            wait_for_table(driver)
                            print(f"[PAGE {page_num}/{last_page}] Scraping (tab {i+1}/{len(pages_to_scrape)})...", flush=True)

                            scraped, skipped, missing_ean = scrape_page(driver, page_num, _repo, existing_ids, last_page)
                            batch_scraped += scraped
                            batch_skipped += skipped
                            total_scraped += scraped
                            total_skipped += skipped
                            audit_log("PAGE_FETCHED", scraper_name="Russia", run_id=_run_id, details={"page": page_num, "scraped": scraped, "skipped": skipped, "missing_ean": missing_ean})
                            _operation_count += 1

                            ean_status = f", Missing EAN: {missing_ean}" if (FETCH_EAN and missing_ean > 0) else ""
                            print(f"  [OK] Page {page_num}: Scraped {scraped}{ean_status}", flush=True)
                            break  # Success, exit retry loop

                        except Exception as e:
                            error_msg = str(e)
                            if attempt < MAX_PAGE_RETRIES:
                                print(f"  [WARN] Page {page_num} failed (attempt {attempt}/{MAX_PAGE_RETRIES}): {e}. Retrying...", flush=True)
                                time.sleep(2)
                                # Try to refresh the page if it looks like a network or load error
                                if "Connection" in error_msg or "reset" in error_msg.lower() or "Timeout" in error_msg:
                                    try:
                                        print(f"  [INFO] Refreshing page {page_num}...", flush=True)
                                        driver.refresh()
                                        time.sleep(2)
                                    except Exception:
                                        pass
                            else:
                                print(f"  [ERROR] Page {page_num} failed after {MAX_PAGE_RETRIES} attempts: {e}", flush=True)
                                try:
                                    _repo.record_failed_page(page_num, "ved", f"Failed after {MAX_PAGE_RETRIES} attempts: {e}")
                                except Exception as rec_err:
                                    print(f"  [WARN] Could not record failed page {page_num}: {rec_err}", flush=True)
                                continue

                # Close extra tabs and keep only one for next batch
                while len(driver.window_handles) > 1:
                    driver.switch_to.window(driver.window_handles[-1])
                    driver.close()
                driver.switch_to.window(driver.window_handles[0])

                # Report failed loads and track consecutive timeouts
                if failed_loads:
                    print(f"[BATCH COMPLETE] Scraped {batch_scraped}, Skipped {batch_skipped}, Failed loads: {len(failed_loads)}, Total: {total_scraped}", flush=True)
                    print(f"  [INFO] Failed page loads will be retried in next run: {failed_loads}", flush=True)
                    consecutive_timeout_batches += 1

                    # Restart driver if too many consecutive timeout batches
                    if consecutive_timeout_batches >= MAX_TIMEOUT_BATCHES:
                        print(f"[DRIVER] {consecutive_timeout_batches} consecutive batches with timeouts. Restarting Chrome to recover...", flush=True)
                        driver = _restart_driver(driver)
                        consecutive_timeout_batches = 0
                        # Re-navigate to the site after restart
                        driver = select_region_and_search(driver)
                        existing_ids = _repo.get_all_existing_item_ids()
                else:
                    print(f"[BATCH COMPLETE] Scraped {batch_scraped}, Skipped {batch_skipped}, Total: {total_scraped}", flush=True)
                    consecutive_timeout_batches = 0  # Reset counter on successful batch

                if was_retry_batch:
                    print("[RETRY] Requested page(s) done.", flush=True)
                    break

                # Progress logging
                mem_mb = get_memory_usage_mb()
                completed_count = len(_repo.get_completed_keys(1))
                print(f"[PROGRESS] {completed_count}/{last_page} pages completed | Total scraped: {total_scraped} | Memory: {mem_mb:.1f}MB", flush=True)

                # MEMORY FIX: Periodic resource monitoring and cleanup
                if _operation_count % MEMORY_CHECK_INTERVAL == 0:
                    try:
                        from core.monitoring.resource_monitor import periodic_resource_check, log_resource_status
                        resource_status = periodic_resource_check("Russia", force=True)
                        if resource_status.get("warnings"):
                            for warning in resource_status["warnings"]:
                                print(f"[RESOURCE WARNING] {warning}", flush=True)
                    except Exception:
                        pass

                    force_cleanup()

                    # Check memory limit and restart Chrome if needed
                    if check_memory_limit():
                        print(f"[MEMORY] Memory limit exceeded, restarting Chrome...", flush=True)
                        driver = _restart_driver(driver)
                        driver = select_region_and_search(driver)
                        print(f"[MEMORY] Chrome restarted and session re-initialized", flush=True)

            # Mark run as complete (skip when retry mode so we do not overwrite run item count)
            if not initial_retry_mode:
                _repo.finish_run("completed", items_scraped=total_scraped)
            print(f"\n[COMPLETE] Scraped {total_scraped} items (no deduplication)", flush=True)

        # =====================================================================
        # VALIDATION REPORT (runs for both parallel and single mode)
        # =====================================================================
        print("\n" + "="*80)
        print("VED SCRAPER VALIDATION REPORT")
        print("="*80)

        # Get count from DB for this run
        db_count = _repo.get_ved_product_count()

        # Calculate total records from step progress metrics (includes previous sessions when resuming)
        completed_pages = _repo.get_completed_keys(1)
        total_rows_from_metrics = 0
        for key in completed_pages:
            if key.startswith("ved_page:"):
                try:
                    sql = """
                        SELECT COALESCE(rows_inserted, 0) FROM ru_step_progress
                        WHERE run_id = %s AND progress_key = %s
                    """
                    with _repo.db.cursor() as cur:
                        cur.execute(sql, (_run_id, key))
                        row = cur.fetchone()
                        if row:
                            total_rows_from_metrics += row[0]
                except Exception:
                    pass

        if total_rows_from_metrics > 0:
            records_scraped_display = total_rows_from_metrics
            print(f"Records scraped from website (all sessions): {records_scraped_display}")
        else:
            records_scraped_display = db_count
            print(f"Records scraped from website: {records_scraped_display}")

        print(f"Records in database (ru_ved_products): {db_count}")

        if records_scraped_display == db_count:
            print(f"[VALIDATION PASSED] Counts match: {records_scraped_display} = {db_count}")
        else:
            print(f"[VALIDATION WARNING] Count mismatch: Scraped={records_scraped_display}, DB={db_count}")
            print(f"  Difference: {abs(records_scraped_display - db_count)}")

        # Check for failed pages
        failed_pages = _repo.get_failed_pages("ved")
        if failed_pages:
            print(f"[WARNING] {len(failed_pages)} pages need retry (failed or missing EAN)")

        print("="*80)

    except Exception as e:
        print(f"\n[FATAL] Scraper failed: {e}", flush=True)
        _repo.finish_run("failed", items_scraped=0, error_message=str(e))
        raise

    finally:
        cleanup_all_chrome()


if __name__ == "__main__":
    main()
