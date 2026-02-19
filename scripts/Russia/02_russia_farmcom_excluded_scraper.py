#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Russia Farmcom Excluded List Scraper - DB-Based with Full Resume Support

Features:
- DB-based storage (no CSV files)
- Page-level resume support
- Chrome instance tracking
- Crash recovery
- Comprehensive retry logic

Author: Enterprise PDF Processing Pipeline
License: Proprietary
"""

import os
import re
import sys
import time
import atexit
import signal
import gc
import hashlib
from pathlib import Path
from typing import Set, Dict, List, Optional

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
os.environ.setdefault('PYTHONUNBUFFERED', '1')

# Add repo root and script dir to path (script dir first to avoid loading another scraper's db)
_repo_root = Path(__file__).resolve().parents[2]
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Clear conflicting 'db' when run in same process as other scrapers (e.g. GUI)
for mod in list(sys.modules.keys()):
    if mod == "db" or mod.startswith("db."):
        del sys.modules[mod]

# Config loader
from config_loader import (
    load_env_file, getenv, getenv_bool, getenv_int, getenv_float, get_output_dir
)
load_env_file()

# DB imports
from core.db.connection import CountryDB
from core.db.models import generate_run_id
from db.schema import apply_russia_schema
from db.repositories import RussiaRepository

# Selenium imports
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions

# Chrome tracking (DB-based)
from core.browser.chrome_pid_tracker import get_chrome_pids_from_driver
from core.browser.chrome_instance_tracker import ChromeInstanceTracker
from core.browser.chrome_manager import register_chrome_driver, unregister_chrome_driver

# =============================================================================
# VPN CHECK
# =============================================================================

def check_vpn_connection() -> bool:
    """Check if VPN is connected (if required)."""
    vpn_required = getenv_bool("VPN_REQUIRED", False)
    vpn_check_enabled = getenv_bool("VPN_CHECK_ENABLED", False)
    vpn_check_host = getenv("VPN_CHECK_HOST", "8.8.8.8")
    vpn_check_port = getenv_int("VPN_CHECK_PORT", 53)
    
    if not vpn_check_enabled:
        return True
    
    if not vpn_required:
        return True
    
    print(f"[VPN] Checking connection to {vpn_check_host}:{vpn_check_port}...", flush=True)
    
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((vpn_check_host, vpn_check_port))
        sock.close()
        
        if result == 0:
            print("[VPN] Connection check passed", flush=True)
            return True
        else:
            print(f"[VPN] Connection check failed", flush=True)
            return False
    except Exception as e:
        print(f"[VPN] Connection check error: {e}", flush=True)
        return False


# =============================================================================
# CONFIGURATION
# =============================================================================

BASE_URL = getenv("SCRIPT_02_BASE_URL")
REGION_VALUE = getenv("SCRIPT_02_REGION_VALUE")
HEADLESS = getenv_bool("SCRIPT_02_HEADLESS")
PAGE_LOAD_TIMEOUT = getenv_int("SCRIPT_02_PAGE_LOAD_TIMEOUT")
WAIT_TIMEOUT = getenv_int("SCRIPT_02_WAIT_TIMEOUT")
SLEEP_BETWEEN_PAGES = getenv_float("SCRIPT_02_SLEEP_BETWEEN_PAGES")
MAX_PAGES = getenv_int("SCRIPT_02_MAX_PAGES", 0)  # 0 = no limit, extract all pages
DB_BATCH_SIZE = getenv_int("DB_BATCH_INSERT_SIZE", 100)
PROGRESS_INTERVAL = getenv_int("DB_PROGRESS_LOG_INTERVAL", 50)

CHROME_NO_SANDBOX = getenv("SCRIPT_02_CHROME_NO_SANDBOX")
CHROME_DISABLE_DEV_SHM = getenv("SCRIPT_02_CHROME_DISABLE_DEV_SHM")

# Navigation retry settings
NAV_RETRIES = getenv_int("SCRIPT_02_NAV_RETRIES", 3)
NAV_RETRY_SLEEP = getenv_float("SCRIPT_02_NAV_RETRY_SLEEP", 5.0)
NAV_RESTART_DRIVER = getenv_bool("SCRIPT_02_NAV_RESTART_DRIVER", True)

# Excluded list: no EAN fetch or validation (EAN check only on VED scraper)
# Excluded list shows 50 rows per page (VED list shows 100)
EXCLUDED_ROWS_PER_PAGE = getenv_int("SCRIPT_02_EXCLUDED_ROWS_PER_PAGE", 50)
# Multi-tab: number of tabs to open per batch (1 = sequential)
MULTI_TAB_BATCH = getenv_int("SCRIPT_02_MULTI_TAB_BATCH", 10)  # Default 10 pages per batch

# =============================================================================
# GLOBAL STATE
# =============================================================================

_shutdown_requested = False
_active_drivers: List[webdriver.Chrome] = []
_run_id: Optional[str] = None
_repo: Optional[RussiaRepository] = None

def _get_run_id() -> Optional[str]:
    """Helper to get current run ID from global state or environment."""
    return _run_id or os.getenv("RUSSIA_RUN_ID")

# =============================================================================
# SIGNAL HANDLERS
# =============================================================================

def signal_handler(signum, frame):
    global _shutdown_requested
    print(f"\n[SIGNAL] Received signal {signum}, shutting down...", flush=True)
    _shutdown_requested = True
    
    if _repo and _run_id:
        try:
            _repo.finish_run("stopped")
        except Exception:
            pass
    
    cleanup_all_chrome()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# =============================================================================
# CHROME MANAGEMENT
# =============================================================================

# CHROME MANAGEMENT
# =============================================================================

from core.browser.chrome_manager import cleanup_all_chrome_instances as cleanup_all_chrome

def _create_driver() -> webdriver.Chrome:
    """Internal factory: creates driver with core factory + DB tracking."""
    # 1. Cleanup orphans
    from core.browser.chrome_manager import kill_orphaned_chrome_processes
    kill_orphaned_chrome_processes()
    
    # 2. Config
    from core.browser.driver_factory import create_chrome_driver
    ua = getenv("SCRIPT_02_CHROME_USER_AGENT")
    extra_opts = {'page_load_timeout': PAGE_LOAD_TIMEOUT}
    if ua: extra_opts['user_agent'] = ua
    
    # 3. Create
    # HEADLESS is global var in this script (imported/defined)
    driver = create_chrome_driver(headless=HEADLESS, extra_options=extra_opts)
    
    # 4. Track
    _active_drivers.append(driver)
    register_chrome_driver(driver)
    
    # DB Logging
    run_id = _get_run_id()
    if ChromeInstanceTracker and run_id and hasattr(driver, "service"):
        try:
            pid = driver.service.process.pid
            if pid:
                pids = get_chrome_pids_from_driver(driver) if get_chrome_pids_from_driver else {pid}
                with CountryDB("Russia") as db:
                     tracker = ChromeInstanceTracker("Russia", run_id, db)
                     tracker.register(step_number=2, pid=pid, browser_type="chrome", child_pids=pids)
        except Exception as e:
            print(f"[WARN] DB tracking failed: {e}")
            
    print(f"[DRIVER] Created new instance (Total: {len(_active_drivers)})", flush=True)
    return driver


def _restart_driver(driver: webdriver.Chrome) -> webdriver.Chrome:
    """Restart driver with DB untracking and core restart logic."""
    print("[DRIVER] Restarting Chrome...", flush=True)
    from core.browser.driver_factory import restart_driver as core_restart_driver
    
    # Untrack in DB
    if driver in _active_drivers:
        _active_drivers.remove(driver)
    
    run_id = _get_run_id()
    if ChromeInstanceTracker and run_id and hasattr(driver, "service"):
        try:
            pid = driver.service.process.pid
            if pid:
                with CountryDB("Russia") as db:
                    ChromeInstanceTracker("Russia", run_id, db).mark_terminated_by_pid(pid, "restart")
        except Exception:
            pass
            
    unregister_chrome_driver(driver)
    
    # Core restart
    return core_restart_driver(driver, _create_driver)


def is_tab_crash(exc: Exception) -> bool:
    msg = str(exc).lower()
    return (
        "tab crashed" in msg
        or "session deleted" in msg
        or "disconnected" in msg
        or "chrome not reachable" in msg
    )


def safe_get(driver: webdriver.Chrome, url: str, wait: bool = True) -> webdriver.Chrome:
    """Navigate with retry and crash recovery."""
    last_exc = None
    for attempt in range(1, NAV_RETRIES + 1):
        try:
            driver.get(url)
            if wait:
                wait_for_table(driver)
            return driver
        except WebDriverException as exc:
            last_exc = exc
            if is_tab_crash(exc) and NAV_RESTART_DRIVER:
                driver = _restart_driver(driver)
            if attempt < NAV_RETRIES:
                time.sleep(NAV_RETRY_SLEEP)
            else:
                raise
    if last_exc:
        raise last_exc
    return driver


def go_to_page(driver: webdriver.Chrome, page_num: int) -> webdriver.Chrome:
    sep = "&" if "?" in BASE_URL else "?"
    url = f"{BASE_URL}{sep}reg_id={REGION_VALUE}&page={page_num}"
    return safe_get(driver, url, wait=True)


# =============================================================================
# NAVIGATION
# =============================================================================

def wait_for_table(driver: webdriver.Chrome) -> None:
    WebDriverWait(driver, WAIT_TIMEOUT).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table.report tbody tr"))
    )
    time.sleep(0.5)


def ensure_tabs_and_load_pages(driver: webdriver.Chrome, page_numbers: list) -> list:
    """
    Ensure we have len(page_numbers) tabs, load each tab with its page URL,
    then wait for table in each tab. Returns list of window handles in order.
    """
    handles = list(driver.window_handles)
    n = len(page_numbers)
    while len(handles) < n:
        driver.execute_script("window.open('');")
        handles = list(driver.window_handles)
    handles = handles[:n]
    
    # Construct base URL with region parameter
    # BASE_URL already has ?vw=excl, so we add &reg_id=...
    base_url = f"{BASE_URL}&reg_id={REGION_VALUE}"
    
    for i, (h, p) in enumerate(zip(handles, page_numbers)):
        driver.switch_to.window(h)
        driver.get(f"{base_url}&page={p}")
    for i, h in enumerate(handles):
        driver.switch_to.window(h)
        try:
            wait_for_table(driver)
        except Exception as e:
            print(f"  [WARN] Tab {i+1} (page {page_numbers[i]}) table wait failed: {e}", flush=True)
    return handles


def select_region_and_search(driver: webdriver.Chrome) -> webdriver.Chrome:
    """Select region and search with retry - handles both cases: with/without region selector"""
    for attempt in range(1, NAV_RETRIES + 1):
        if _shutdown_requested:
            raise InterruptedError("Shutdown requested")
        
        try:
            # Try direct URL with region parameter first (like main scraper does)
            sep = "&" if "?" in BASE_URL else "?"
            url_with_region = f"{BASE_URL}{sep}reg_id={REGION_VALUE}"
            print(f"  [NAV] Loading {url_with_region}...", flush=True)
            driver.get(url_with_region)
            
            # Wait a bit for page to start loading
            time.sleep(2)
            
            # First, try to see if table loads directly
            print(f"  [NAV] Checking if table loads directly with region in URL...", flush=True)
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table.report tbody tr"))
                )
                print(f"  [NAV] Table loaded directly with region parameter", flush=True)
                return driver
            except TimeoutException:
                print(f"  [NAV] Table not found with direct URL, trying region selector approach...", flush=True)
            
            # If table didn't load, try navigating to base URL and using region selector
            print(f"  [NAV] Loading {BASE_URL}...", flush=True)
            driver.get(BASE_URL)
            time.sleep(2)
            
            # Try to see if table loads directly without region selection
            print(f"  [NAV] Checking if table loads directly without region selection...", flush=True)
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table.report tbody tr"))
                )
                print(f"  [NAV] Table loaded directly without region selection", flush=True)
                return driver
            except TimeoutException:
                print(f"  [NAV] Table not found directly, checking for region selector...", flush=True)
            
            # If table didn't load, try region selection approach
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
            # Print debug info
            try:
                page_title = driver.title
                current_url = driver.current_url
                print(f"  [DEBUG] Page title: {page_title}", flush=True)
                print(f"  [DEBUG] Current URL: {current_url}", flush=True)
                
                # Check what elements are actually present
                try:
                    page_source = driver.page_source
                    if "reg_id" in page_source:
                        print(f"  [DEBUG] 'reg_id' found in page source", flush=True)
                    else:
                        print(f"  [DEBUG] 'reg_id' NOT found in page source", flush=True)
                    
                    # Check for table
                    if "table.report" in page_source or "table" in page_source.lower():
                        print(f"  [DEBUG] Table element found in page source", flush=True)
                    else:
                        print(f"  [DEBUG] Table element NOT found in page source", flush=True)
                        
                    # Check for common page elements
                    if "btn_submit" in page_source:
                        print(f"  [DEBUG] 'btn_submit' found in page source", flush=True)
                    
                    # Print a snippet of page source for debugging
                    snippet = page_source[:1000] if len(page_source) > 1000 else page_source
                    print(f"  [DEBUG] Page source snippet (first 1000 chars): {snippet[:200]}...", flush=True)
                except Exception as e:
                    print(f"  [DEBUG] Could not analyze page source: {e}", flush=True)
            except Exception:
                pass
                
            if attempt < NAV_RETRIES:
                print(f"  [WARN] Retrying in {NAV_RETRY_SLEEP}s...", flush=True)
                time.sleep(NAV_RETRY_SLEEP)
                
        except WebDriverException as exc:
            print(f"  [WARN] WebDriver error (attempt {attempt}/{NAV_RETRIES}): {exc}", flush=True)
            if attempt < NAV_RETRIES:
                print(f"  [WARN] Retrying in {NAV_RETRY_SLEEP}s...", flush=True)
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

def parse_price(val: str) -> str:
    if not val:
        return ""
    nums = re.findall(r"[\d\s]+", val.replace(" ", ""))
    return nums[0] if nums else val.strip()


def extract_ean_and_release_form(cell_text: str) -> tuple[str, str]:
    """Extract EAN from release form cell and return cleaned release form."""
    lines = [ln.strip() for ln in (cell_text or "").splitlines() if ln.strip()]
    ean = ""
    release_lines = []
    for ln in lines:
        m = re.search(r"\bEAN\s*([0-9]{8,18})\b", ln, flags=re.I)
        if m and not ean:
            ean = m.group(1)
            continue
        digits = re.sub(r"\s+", "", ln)
        if not ean and re.fullmatch(r"\d{8,18}", digits):
            ean = digits
            continue
        release_lines.append(ln)
    release_form = " ".join(release_lines).strip()
    return release_form, ean


def make_row_id(*parts: str) -> str:
    """Generate unique row ID from product data components."""
    base = "|".join(p.strip().lower() for p in parts if p is not None)
    if not base:
        return ""
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def extract_price_and_date(cell_text: str) -> tuple[str, str]:
    """Extract price and date from cell text."""
    text = cell_text.strip() if cell_text else ""
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    
    price = ""
    date_text = ""
    
    for ln in lines:
        # Look for price pattern (digits with optional decimal point and spaces)
        if not price:
            # Match numbers like 1234.56 or 1234,56 (handle both . and , as decimal)
            price_match = re.search(r"[\d]+[\.,\s]?[\d]*", ln.replace(" ", ""))
            if price_match:
                price = price_match.group(0).replace(",", ".")  # Normalize to dot
        # Look for date pattern (DD.MM.YYYY)
        if not date_text:
            date_match = re.search(r"\d{2}\.\d{2}\.\d{4}", ln)
            if date_match:
                date_text = date_match.group(0)
    
    return price, date_text


def parse_item_id_from_linkhref(linkhref: str) -> str:
    """Parse item_id from linkhref attribute."""
    if not linkhref:
        return ""
    from urllib.parse import parse_qs, urlparse
    qs = parse_qs(urlparse("http://x/?" + linkhref.split("?", 1)[-1]).query)
    return (qs.get("item_id", [""]) or [""])[0]


def extract_row_data(row, page_num: int, ean_map: dict = None) -> Optional[Dict]:  # ean_map unused; no EAN fetch on excluded
    """
    Extract data from a table row.
    
    Handles two types of rows:
    1. Main rows with bullet images (7+ cells) - EAN from ean_map
    2. Gray/EAN-only rows without bullets (6+ cells) - EAN from cell text
    """
    try:
        cells = row.find_elements(By.TAG_NAME, "td")
        
        # Check for bullet image to determine row type
        bullet_imgs = row.find_elements(By.CSS_SELECTOR, "img.bullet[linkhref]")
        
        if bullet_imgs and len(cells) >= 7:
            # MAIN ROW: Has bullet image and 7+ cells
            # Extract item_id from linkhref attribute (most reliable)
            item_id = ""
            try:
                linkhref = bullet_imgs[0].get_attribute("linkhref") or ""
                item_id = parse_item_id_from_linkhref(linkhref)
            except Exception:
                pass
            
            # Fallback: try onclick attribute
            if not item_id:
                try:
                    onclick = cells[0].get_attribute("onclick") or ""
                    match = re.search(r"showInfo\((\d+)\)", onclick)
                    if match:
                        item_id = match.group(1)
                except Exception:
                    pass
            
            # Fallback: try barcode link id
            if not item_id:
                try:
                    barcode_link = row.find_element(By.CSS_SELECTOR, "a.info[id^='e']")
                    link_id = barcode_link.get_attribute("id") or ""
                    if link_id.startswith("e"):
                        item_id = link_id[1:]
                except Exception:
                    pass
            
            if not item_id:
                return None
            
            tn = cells[1].text.strip()
            inn = cells[2].text.strip()
            manufacturer_country = cells[3].text.strip()
            
            release_form_full = cells[4].text.strip()
            release_form = re.sub(r"\b(Barcode|\d{8,14})\b\s*$", "", release_form_full).strip()
            
            price, date_text = extract_price_and_date(cells[5].text)
            
            # Excluded list: no EAN fetch (column left empty for main rows)
            ean = ""
            
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
            
        elif len(cells) >= 6:
            # GRAY/EAN ROW: No bullet image, 6+ cells
            # These rows contain EAN directly in the cell text
            tn = cells[0].text.strip()
            inn = cells[1].text.strip()
            manufacturer_country = cells[2].text.strip()
            
            release_form_full = cells[3].text.strip()
            release_form, ean = extract_ean_and_release_form(release_form_full)
            
            price, date_text = extract_price_and_date(cells[4].text)
            
            # Use exclusion date if available
            exclusion_date = cells[5].text.strip()
            if exclusion_date:
                date_text = exclusion_date
            
            # Generate item_id from data components
            item_id = make_row_id(tn, inn, manufacturer_country, release_form, ean, price, date_text)
            
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
        else:
            # Not enough cells
            return None
            
    except Exception:
        return None


def scrape_page(driver: webdriver.Chrome, page_num: int, repo: RussiaRepository, existing_ids: Set[str], last_page: int = 0) -> tuple[int, int, int]:
    """Scrape one page. Only insert if page has EXCLUDED_ROWS_PER_PAGE rows (or is last page). No deduplication."""
    # Verify we're on the correct page by checking URL
    current_url = driver.current_url
    expected_page_param = f"page={page_num}"
    if expected_page_param not in current_url:
        print(f"  [WARN] URL mismatch! Expected page {page_num} but URL is: {current_url}")
        print(f"  [WARN] Navigating to correct page...")
        try:
            # Determine URL separator
            sep = "&" if "?" in current_url else "?"
            correct_url = f"{BASE_URL}{sep}vw=excl&reg_id={REGION_VALUE}&page={page_num}"
            driver.get(correct_url)
            wait_for_table(driver)
            print(f"  [OK] Navigated to correct page {page_num}")
        except Exception as e:
            print(f"  [ERROR] Failed to navigate to page {page_num}: {e}")
            repo.record_failed_page(page_num, "excluded", f"URL verification failed: {current_url}")
            return 0, 0, 0
    
    scraped = 0
    skipped = 0
    batch = []
    
    try:
        rows = driver.find_elements(By.CSS_SELECTOR, "table.report tbody tr")
        
        for row in rows:
            if _shutdown_requested:
                break
            
            data = extract_row_data(row, page_num, ean_map=None)
            if not data:
                continue
            
            # Ensure page_number is set
            data["page_number"] = page_num
            
            batch.append(data)
            # MEMORY FIX: Only track if set is reasonable size (no dedup - not used to skip)
            if len(existing_ids) < 10000:
                existing_ids.add(data["item_id"])
        
        # STRICT: Only insert if page has full rows (or is last page). Excluded list = 50 rows/page.
        is_last_page = (page_num == last_page and last_page > 0)
        rows_found = len(batch)
        if not is_last_page and rows_found < EXCLUDED_ROWS_PER_PAGE:
            print(f"  [VALIDATION FAILED] Page {page_num} has only {rows_found} rows (expected {EXCLUDED_ROWS_PER_PAGE}). DATA NOT WRITTEN", flush=True)
            repo.record_failed_page(page_num, "excluded", f"Row count validation failed: {rows_found} rows (expected {EXCLUDED_ROWS_PER_PAGE})")
            repo.mark_progress(2, "Excluded Scrape", f"excluded_page:{page_num}", "failed", f"rows={rows_found}")
            return 0, 0, 0
        
        # Insert in chunks
        for i in range(0, len(batch), DB_BATCH_SIZE):
            chunk = batch[i : i + DB_BATCH_SIZE]
            repo.insert_excluded_products_bulk(chunk)
            scraped += len(chunk)
        
        repo.mark_progress(2, "Excluded Scrape", f"excluded_page:{page_num}", "completed")
        
        return scraped, skipped, 0
        
    except Exception as e:
        repo.record_failed_page(page_num, "excluded", str(e))
        repo.mark_progress(2, "Excluded Scrape", f"excluded_page:{page_num}", "failed", str(e))
        raise


def get_last_page(driver: webdriver.Chrome) -> int:
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
    return 1


def get_resume_page(repo: RussiaRepository, current_run_id: str = None) -> int:
    """Get page to resume from - finds run with most completed pages"""
    
    # 1. Check current run first (if provided)
    if current_run_id:
        completed = repo.get_completed_keys_for_run(2, current_run_id)
        pages = []
        for key in completed:
            if key.startswith("excluded_page:"):
                try:
                    pages.append(int(key.split(":")[1]))
                except ValueError:
                    pass
        if pages:
            return max(pages) + 1

    # 2. If current run has no progress, check other runs (find best previous run)
    # Get all run_ids with their completed page counts for step 2 (excluded)
    run_pages = repo.get_all_run_completed_pages(2, current_run_id)
    
    if not run_pages:
        return 1
    
    # Find run with maximum completed pages
    best_run_id = None
    max_pages = 0
    for run_id, pages in run_pages.items():
        if pages > max_pages:
            max_pages = pages
            best_run_id = run_id
    
    if not best_run_id or max_pages == 0:
        return 1
    
    # Get the actual page numbers from the best run
    completed = repo.get_completed_keys_for_run(2, best_run_id)
    pages = []
    for key in completed:
        if key.startswith("excluded_page:"):
            try:
                pages.append(int(key.split(":")[1]))
            except ValueError:
                pass
    
    return max(pages) + 1 if pages else 1


# =============================================================================
# MAIN
# =============================================================================

def main():
    global _run_id, _repo
    
    # Parse command-line arguments
    fresh_run = "--fresh" in sys.argv
    run_id_arg = None
    for i, arg in enumerate(sys.argv):
        if arg == "--run-id" and i + 1 < len(sys.argv):
            run_id_arg = sys.argv[i + 1]
            break
    
    print("=" * 80)
    print("Russia Excluded List Scraper - DB-Based")
    print("=" * 80)
    
    # VPN Check (optional)
    if check_vpn_connection():
        print("[INIT] VPN check passed or not required", flush=True)
    else:
        print("[FATAL] VPN connection check failed. Please connect VPN or set VPN_CHECK_ENABLED=false", flush=True)
        sys.exit(1)
    
    db = CountryDB("Russia")
    apply_russia_schema(db)
    
    # Resolve run_id - prefer pipeline/env/.current_run_id so step 2 uses same run as step 1
    run_id_file = get_output_dir() / ".current_run_id"
    _existing = (os.getenv("RUSSIA_RUN_ID") or "").strip() or (run_id_file.read_text(encoding="utf-8").strip() if run_id_file.exists() else None)
    if fresh_run:
        _run_id = _existing or generate_run_id()
        print(f"[INIT] --fresh flag: run_id={_run_id}", flush=True)
    elif run_id_arg:
        # --run-id specified: Use the specified run_id
        _run_id = run_id_arg
        print(f"[INIT] Using specified run_id: {_run_id}", flush=True)
    else:
        # Default behavior: Use env or generate new
        _run_id = os.getenv("RUSSIA_RUN_ID") or generate_run_id()
        print(f"[INIT] Run ID: {_run_id}", flush=True)
    
    os.environ["RUSSIA_RUN_ID"] = _run_id
    
    # Persist run_id to file so it survives pipeline restarts (e.g., from Telegram)
    try:
        run_id_file.parent.mkdir(parents=True, exist_ok=True)
        run_id_file.write_text(_run_id, encoding="utf-8")
        print(f"[INIT] Saved run_id to {run_id_file}", flush=True)
    except Exception as e:
        print(f"[INIT] Warning: could not save run_id to file: {e}", flush=True)
    
    _repo = RussiaRepository(db, _run_id)
    
    # Use ensure_run_exists (INSERT ... ON CONFLICT DO UPDATE) instead of start_run
    # to avoid UniqueViolation when step 2 runs after step 1 (run already in run_ledger)
    _repo.ensure_run_exists(mode="resume" if not fresh_run else "fresh")
    resume_page = get_resume_page(_repo, _run_id)
    if resume_page > 1:
        print(f"[INIT] Resuming from page {resume_page}", flush=True)
    else:
        print(f"[INIT] Starting from page 1", flush=True)
    
    # MEMORY FIX: Use empty set for excluded list (no dedup needed)
    # But track it for monitoring
    existing_ids = set()
    try:
        from core.monitoring.memory_leak_detector import track_set
        track_set("russia_excluded_existing_ids", existing_ids, max_size=10000)
    except Exception:
        pass
    
    driver = _create_driver()
    
    try:
        print(f"[NAV] Navigating to {BASE_URL}...", flush=True)
        driver = select_region_and_search(driver)
        
        last_page = get_last_page(driver)
        if MAX_PAGES > 0:
            last_page = min(last_page, MAX_PAGES)
        print(f"[INFO] Total pages: {last_page}", flush=True)
        
        # Navigate to resume page
        start_page = resume_page
        # Get the current URL after region selection (should include reg_id parameter)
        current_url = driver.current_url
        base_url_with_region = current_url.split('&page=')[0].split('?page=')[0]
        if 'reg_id' not in base_url_with_region:
            # Fallback: construct URL properly
            sep = "&" if "?" in BASE_URL else "?"
            base_url_with_region = f"{BASE_URL}{sep}reg_id={REGION_VALUE}"
        
        if start_page > 1:
            print(f"[NAV] Jumping to resume page {start_page}...", flush=True)
            driver = safe_get(driver, f"{base_url_with_region}&page={start_page}")
        
        effective_multi_tab = MULTI_TAB_BATCH
        total_scraped = 0
        
        page_num = start_page
        while page_num <= last_page:
            BATCH_SIZE = effective_multi_tab if effective_multi_tab > 1 else 5
            if _shutdown_requested:
                break
            
            batch_end = min(page_num + BATCH_SIZE - 1, last_page)
            batch_pages = list(range(page_num, batch_end + 1))
            n_batch = len(batch_pages)
            print(f"[BATCH] Processing pages {page_num}-{batch_end} ({n_batch} pages, {n_batch} tabs)...", flush=True)
            
            batch_scraped = 0
            
            if effective_multi_tab > 1 and n_batch > 0:
                try:
                    handles = ensure_tabs_and_load_pages(driver, batch_pages)
                    for i in range(n_batch):
                        if _shutdown_requested:
                            break
                        batch_page = batch_pages[i]
                        driver.switch_to.window(handles[i])
                        print(f"[PAGE {batch_page}/{last_page}] Scraping (tab {i+1}/{n_batch})...", flush=True)
                        try:
                            scraped, skipped, _ = scrape_page(driver, batch_page, _repo, existing_ids, last_page)
                            batch_scraped += scraped
                            total_scraped += scraped
                            print(f"  [OK] Scraped: {scraped}, Total: {total_scraped}", flush=True)
                        except Exception as e:
                            print(f"  [ERROR] Page {batch_page}: {e}", flush=True)
                except Exception as e:
                    print(f"  [ERROR] Multi-tab batch failed: {e}", flush=True)
                    if isinstance(e, WebDriverException) and is_tab_crash(e):
                        print("  [WARN] Tab crash detected. Falling back to sequential mode.", flush=True)
                        driver = _restart_driver(driver)
                        effective_multi_tab = 1
                    else:
                        try:
                            driver.switch_to.window(driver.window_handles[0])
                        except Exception:
                            pass
                    for batch_page in batch_pages:
                        if _shutdown_requested:
                            break
                        print(f"[PAGE {batch_page}/{last_page}] Scraping...", flush=True)
                        try:
                            scraped, skipped, _ = scrape_page(driver, batch_page, _repo, existing_ids, last_page)
                            batch_scraped += scraped
                            total_scraped += scraped
                            print(f"  [OK] Scraped: {scraped}, Total: {total_scraped}", flush=True)
                            if batch_page < batch_end:
                                time.sleep(SLEEP_BETWEEN_PAGES)
                                driver = go_to_page(driver, batch_page + 1)
                        except Exception as ex:
                            print(f"  [ERROR] Page {batch_page}: {ex}", flush=True)
            else:
                for batch_page in range(page_num, batch_end + 1):
                    if _shutdown_requested:
                        break
                    print(f"[PAGE {batch_page}/{last_page}] Scraping...", flush=True)
                    try:
                        scraped, skipped, _ = scrape_page(driver, batch_page, _repo, existing_ids, last_page)
                        batch_scraped += scraped
                        total_scraped += scraped
                        print(f"  [OK] Scraped: {scraped}, Total: {total_scraped}", flush=True)
                        if batch_page < batch_end:
                            time.sleep(SLEEP_BETWEEN_PAGES)
                            driver = go_to_page(driver, batch_page + 1)
                    except Exception as e:
                        print(f"  [ERROR] Page {batch_page}: {e}", flush=True)
                        continue
            
            print(f"[BATCH COMPLETE] Pages {page_num}-{batch_end}: Scraped {batch_scraped}", flush=True)
            page_num = batch_end + 1
            
            if batch_end % PROGRESS_INTERVAL == 0:
                print(f"[PROGRESS] Page {batch_end}/{last_page} | Total: {total_scraped}", flush=True)
                gc.collect()
                
                # Proactive restart to prevent memory leaks/crashes
                if effective_multi_tab > 1:
                    print(f"[DRIVER] Periodic restart to release memory (page {batch_end})...", flush=True)
                    try:
                        driver = _restart_driver(driver)
                    except Exception as e:
                        print(f"  [WARN] Driver restart failed: {e}. Continuing with current driver.", flush=True)
            
            if page_num <= last_page and effective_multi_tab <= 1:
                print(f"[BATCH] Navigating to page {page_num} for next batch...", flush=True)
                try:
                    driver = go_to_page(driver, page_num)
                except Exception as e:
                    print(f"  [WARN] Navigation to page {page_num} failed: {e}", flush=True)
        
        _repo.finish_run("completed", items_scraped=total_scraped)
        print(f"\n[COMPLETE] Scraped {total_scraped} excluded items", flush=True)
        
        # VALIDATION REPORT
        print("\n" + "="*80)
        print("EXCLUDED SCRAPER VALIDATION REPORT")
        print("="*80)
        
        # Get count from DB
        db_count = _repo.get_excluded_product_count()
        print(f"Records scraped from website: {total_scraped}")
        print(f"Records in database (ru_excluded_products): {db_count}")
        
        if total_scraped == db_count:
            print(f"[VALIDATION PASSED] Counts match: {total_scraped} = {db_count}")
        else:
            print(f"[VALIDATION WARNING] Count mismatch: Scraped={total_scraped}, DB={db_count}")
            print(f"  Difference: {abs(total_scraped - db_count)}")
        
        # Check for failed pages
        failed_pages = _repo.get_failed_pages("excluded")
        if failed_pages:
            print(f"[WARNING] {len(failed_pages)} pages need retry")
        
        print("="*80)
        
    except Exception as e:
        print(f"\n[FATAL] {e}", flush=True)
        _repo.finish_run("failed", error_message=str(e))
        raise
    
    finally:
        cleanup_all_chrome()


if __name__ == "__main__":
    main()
