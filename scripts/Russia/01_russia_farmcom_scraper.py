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
- Deduplication via item_id
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
from queue import Queue, Empty
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Set, Dict, List, Optional

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
os.environ.setdefault('PYTHONUNBUFFERED', '1')

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

# Config loader
from config_loader import (
    load_env_file, getenv, getenv_bool, getenv_int, getenv_float,
    get_input_dir, get_output_dir
)
load_env_file()

# DB imports
from core.db.connection import CountryDB
from core.db.models import generate_run_id, run_ledger_start, run_ledger_finish
from db.schema import apply_russia_schema
from db.repositories import RussiaRepository

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
from core.chrome_pid_tracker import get_chrome_pids_from_driver, save_chrome_pids
from core.chrome_manager import register_chrome_driver, unregister_chrome_driver

# State machine
from smart_locator import SmartLocator
from state_machine import NavigationStateMachine, NavigationState, StateCondition

# Human actions
try:
    from core.human_actions import pause
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
FETCH_EAN = getenv_bool("SCRIPT_01_FETCH_EAN")
EAN_POPUP_TIMEOUT = getenv_int("SCRIPT_01_EAN_POPUP_TIMEOUT")
MAX_PAGES = getenv_int("SCRIPT_01_MAX_PAGES")
NUM_WORKERS = getenv_int("SCRIPT_01_NUM_WORKERS")
MAX_RETRIES_PER_PAGE = getenv_int("SCRIPT_01_MAX_RETRIES_PER_PAGE")
DB_BATCH_SIZE = getenv_int("DB_BATCH_INSERT_SIZE", 100)
PROGRESS_INTERVAL = getenv_int("DB_PROGRESS_LOG_INTERVAL", 50)

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
    
    if not VPN_REQUIRED:
        print("[VPN] VPN not required, skipping check", flush=True)
        return True
    
    print(f"[VPN] Checking connection to {VPN_CHECK_HOST}:{VPN_CHECK_PORT}...", flush=True)
    
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((VPN_CHECK_HOST, VPN_CHECK_PORT))
        sock.close()
        
        if result == 0:
            print("[VPN] Connection check passed", flush=True)
            return True
        else:
            print(f"[VPN] Connection check failed (error code: {result})", flush=True)
            return False
    except Exception as e:
        print(f"[VPN] Connection check error: {e}", flush=True)
        return False


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
_run_id: Optional[str] = None
_repo: Optional[RussiaRepository] = None

# =============================================================================
# SIGNAL HANDLERS - Crash Guard
# =============================================================================

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global _shutdown_requested
    print(f"\n[SIGNAL] Received signal {signum}, initiating graceful shutdown...", flush=True)
    _shutdown_requested = True
    
    # Save progress to DB
    if _repo and _run_id:
        try:
            _repo.finish_run("stopped", items_scraped=0)
            print(f"[SIGNAL] Run {_run_id} marked as stopped in DB", flush=True)
        except Exception as e:
            print(f"[SIGNAL] Warning: Could not update run status: {e}", flush=True)
    
    # Cleanup all Chrome instances
    cleanup_all_chrome()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# =============================================================================
# CHROME INSTANCE TRACKING - Enhanced with Memory Management
# =============================================================================

def cleanup_all_chrome():
    """Cleanup all tracked Chrome instances with enhanced memory management"""
    print(f"[CLEANUP] Cleaning up {len(_active_drivers)} Chrome instance(s)...", flush=True)
    
    # Get all PIDs before closing
    all_pids = set()
    for driver in _active_drivers[:]:
        try:
            if hasattr(driver, 'service') and hasattr(driver.service, 'process'):
                pid = driver.service.process.pid
                if pid:
                    all_pids.add(pid)
                    try:
                        import psutil
                        parent = psutil.Process(pid)
                        for child in parent.children(recursive=True):
                            all_pids.add(child.pid)
                    except Exception:
                        pass
        except Exception:
            pass
    
    for driver in _active_drivers[:]:
        try:
            unregister_chrome_driver(driver)
        except Exception:
            pass
        try:
            driver.quit()
        except Exception:
            pass
    _active_drivers.clear()
    
    # Kill any remaining Chrome processes
    if all_pids:
        try:
            import psutil
            for pid in all_pids:
                try:
                    proc = psutil.Process(pid)
                    proc.kill()
                except Exception:
                    pass
        except Exception:
            pass
    
    # Kill any remaining Chrome processes tracked for Russia
    try:
        from core.chrome_pid_tracker import cleanup_chrome_pids
        cleanup_chrome_pids("Russia", _repo_root)
    except Exception:
        pass
    
    # PERFORMANCE FIX: Force garbage collection
    force_cleanup()
    
    print("[CLEANUP] Chrome cleanup complete", flush=True)

atexit.register(cleanup_all_chrome)

def track_driver(driver: webdriver.Chrome):
    """Track driver for cleanup"""
    _active_drivers.append(driver)
    register_chrome_driver(driver)
    
    # Save PIDs for GUI tracking
    try:
        pids = get_chrome_pids_from_driver(driver)
        if pids:
            save_chrome_pids("Russia", _repo_root, pids)
    except Exception:
        pass

def untrack_driver(driver: webdriver.Chrome):
    """Untrack driver"""
    if driver in _active_drivers:
        _active_drivers.remove(driver)
    try:
        unregister_chrome_driver(driver)
    except Exception:
        pass

# =============================================================================
# DRIVER MANAGEMENT
# =============================================================================

def get_chromedriver_path() -> str:
    """Get ChromeDriver path with offline fallback"""
    import glob
    
    home = Path.home()
    wdm_cache_dir = home / ".wdm" / "drivers" / "chromedriver"
    
    # Try cached driver first
    if wdm_cache_dir.exists():
        patterns = [
            str(wdm_cache_dir / "**" / "chromedriver.exe"),
            str(wdm_cache_dir / "**" / "chromedriver"),
        ]
        for pattern in patterns:
            matches = glob.glob(pattern, recursive=True)
            if matches:
                matches.sort(key=lambda x: os.path.getmtime(x), reverse=True)
                return matches[0]
    
    # Try to download
    try:
        return ChromeDriverManager().install()
    except Exception:
        pass
    
    # System chromedriver
    chromedriver_in_path = shutil.which("chromedriver")
    if chromedriver_in_path:
        return chromedriver_in_path
    
    raise RuntimeError("ChromeDriver not found")


def make_driver() -> webdriver.Chrome:
    """Build Chrome driver with full anti-detection"""
    opts = ChromeOptions()
    
    if HEADLESS:
        opts.add_argument("--headless=new")
    
    # Apply options from config
    if CHROME_NO_SANDBOX:
        opts.add_argument(CHROME_NO_SANDBOX)
    if CHROME_DISABLE_DEV_SHM:
        opts.add_argument(CHROME_DISABLE_DEV_SHM)
    if CHROME_START_MAXIMIZED and not HEADLESS:
        opts.add_argument(CHROME_START_MAXIMIZED)
    if CHROME_DISABLE_AUTOMATION:
        opts.add_argument(CHROME_DISABLE_AUTOMATION)
    
    # Standard anti-detection options
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1600,1000")
    opts.add_argument("--lang=en-US")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-plugins")
    opts.add_argument("--disable-infobars")
    opts.add_argument("--disable-popup-blocking")
    opts.add_argument("--disable-translate")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-sync")
    opts.add_argument("--disable-default-apps")
    opts.add_argument("--mute-audio")
    opts.add_argument("--no-first-run")
    opts.add_argument("--no-default-browser-check")
    opts.add_argument("--remote-debugging-port=0")
    
    opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    opts.add_experimental_option('useAutomationExtension', False)
    
    # User agent from config
    user_agent = getenv("SCRIPT_01_CHROME_USER_AGENT")
    if user_agent:
        opts.add_argument(f"--user-agent={user_agent}")
    
    # Disable images for speed
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.notifications": 2,
    }
    opts.add_experimental_option("prefs", prefs)
    
    service = ChromeService(get_chromedriver_path())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    
    # CDP anti-detection
    try:
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en', 'ru-RU', 'ru'] });
                window.chrome = { runtime: {} };
            '''
        })
    except Exception:
        pass
    
    track_driver(driver)
    return driver


def restart_driver(driver: webdriver.Chrome) -> webdriver.Chrome:
    """Restart driver with cleanup"""
    print("[DRIVER] Restarting Chrome...", flush=True)
    untrack_driver(driver)
    try:
        driver.quit()
    except Exception:
        pass
    return make_driver()


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
        driver = restart_driver(driver)
        
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
        driver = restart_driver(driver)
        return select_region_and_search(driver)
    
    raise RuntimeError("Region selection failed after all retries")


# =============================================================================
# DATA EXTRACTION
# =============================================================================

def parse_price(val: str) -> str:
    """Extract numeric price from string"""
    if not val:
        return ""
    nums = re.findall(r"[\d\s]+", val.replace(" ", ""))
    return nums[0] if nums else val.strip()


def extract_price_and_date(cell_text: str) -> tuple[str, str]:
    """Extract price and date from cell text like '531.51 \n03/15/2010'"""
    lines = [ln.strip() for ln in (cell_text or "").splitlines() if ln.strip()]
    if not lines:
        return "", ""
    price = lines[0].strip()
    date_text = lines[1].strip() if len(lines) > 1 else ""
    return price, date_text


def extract_row_data(row, page_num: int) -> Optional[Dict]:
    """Extract data from a table row"""
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
        
        # Release form - remove trailing "Barcode" text
        release_form_full = cells[5].text.strip() if len(cells) > 5 else ""
        release_form = re.sub(r"\bBarcode\b\s*$", "", release_form_full).strip()
        
        # Price and date from last cell
        price, date_text = "", ""
        if len(cells) > 6:
            price, date_text = extract_price_and_date(cells[6].text)
        
        return {
            "item_id": item_id,
            "tn": tn,
            "inn": inn,
            "manufacturer_country": manufacturer_country,
            "release_form": release_form,
            "ean": "",  # Would need popup click to get
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

def scrape_page(driver: webdriver.Chrome, page_num: int, repo: RussiaRepository, existing_ids: Set[str]) -> tuple[int, int]:
    """
    Scrape a single page.
    Returns: (scraped_count, skipped_count)
    """
    if _shutdown_requested:
        return 0, 0
    
    scraped = 0
    skipped = 0
    batch = []
    
    try:
        rows = driver.find_elements(By.CSS_SELECTOR, "table.report tbody tr")
        
        # Debug: print row count on first page
        if page_num == 1:
            print(f"  [DEBUG] Found {len(rows)} rows on page 1", flush=True)
        
        for idx, row in enumerate(rows):
            if _shutdown_requested:
                break
            
            data = extract_row_data(row, page_num)
            if not data:
                # Debug first few failed rows
                if page_num == 1 and idx < 3:
                    try:
                        cells = row.find_elements(By.TAG_NAME, "td")
                        print(f"  [DEBUG] Row {idx}: {len(cells)} cells, skipping", flush=True)
                    except Exception:
                        pass
                continue
            
            # Deduplication check
            if data["item_id"] in existing_ids:
                skipped += 1
                continue
            
            batch.append(data)
            existing_ids.add(data["item_id"])
            
            # Bulk insert when batch is full
            if len(batch) >= DB_BATCH_SIZE:
                repo.insert_ved_products_bulk(batch)
                scraped += len(batch)
                batch = []
        
        # Insert remaining
        if batch:
            repo.insert_ved_products_bulk(batch)
            scraped += len(batch)
        
        # Mark page as completed
        repo.mark_progress(1, "VED Scrape", f"ved_page:{page_num}", "completed")
        
        return scraped, skipped
        
    except Exception as e:
        # Record failed page for retry
        repo.record_failed_page(page_num, "ved", str(e))
        repo.mark_progress(1, "VED Scrape", f"ved_page:{page_num}", "failed", str(e))
        raise


def get_last_page(driver: webdriver.Chrome) -> int:
    """Get total number of pages"""
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


def get_resume_page(repo: RussiaRepository) -> int:
    """Get page to resume from"""
    completed = repo.get_completed_keys(1)
    if not completed:
        return 1
    
    pages = []
    for key in completed:
        if key.startswith("ved_page:"):
            try:
                pages.append(int(key.split(":")[1]))
            except ValueError:
                pass
    
    return max(pages) + 1 if pages else 1


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main():
    global _run_id, _repo
    
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
    
    # Generate or get run_id
    _run_id = os.getenv("RUSSIA_RUN_ID") or generate_run_id()
    os.environ["RUSSIA_RUN_ID"] = _run_id
    print(f"[INIT] Run ID: {_run_id}", flush=True)
    
    # Initialize repository
    _repo = RussiaRepository(db, _run_id)
    
    # Check for resume
    resume_page = get_resume_page(_repo)
    if resume_page > 1:
        print(f"[INIT] Resuming from page {resume_page}", flush=True)
        _repo.resume_run()
    else:
        print(f"[INIT] Starting fresh run", flush=True)
        _repo.start_run("fresh")
    
    # Get existing item_ids for deduplication
    existing_ids = _repo.get_existing_item_ids()
    print(f"[INIT] {len(existing_ids)} existing items in DB", flush=True)
    
    # Initialize driver
    print("[INIT] Starting Chrome...", flush=True)
    driver = make_driver()
    
    try:
        # Navigate and select region
        print(f"[NAV] Navigating to {BASE_URL}...", flush=True)
        driver = select_region_and_search(driver)
        
        # Get total pages
        last_page = get_last_page(driver)
        if MAX_PAGES > 0:
            last_page = min(last_page, MAX_PAGES)
        print(f"[INFO] Total pages to scrape: {last_page}", flush=True)
        
        # Start from resume page
        start_page = resume_page
        if start_page > 1:
            # Navigate to resume page
            print(f"[NAV] Jumping to page {start_page}...", flush=True)
            for page in range(2, start_page + 1):
                if _shutdown_requested:
                    break
                try:
                    # Click next or navigate directly
                    next_link = driver.find_element(By.CSS_SELECTOR, "p.paging a:last-child")
                    next_link.click()
                    wait_for_table(driver)
                except Exception as e:
                    print(f"  [WARN] Navigation to page {page} failed: {e}", flush=True)
                    # Try direct URL
                    driver = navigate_with_retry(driver, f"{BASE_URL}?page={page-1}", f"Page {page}")
        
        # Main scraping loop with memory monitoring
        total_scraped = 0
        total_skipped = 0
        global _operation_count
        
        for page_num in range(start_page, last_page + 1):
            if _shutdown_requested:
                print("[SHUTDOWN] Stopping gracefully...", flush=True)
                break
            
            print(f"[PAGE {page_num}/{last_page}] Scraping...", flush=True)
            
            try:
                scraped, skipped = scrape_page(driver, page_num, _repo, existing_ids)
                total_scraped += scraped
                total_skipped += skipped
                
                print(f"  [OK] Scraped: {scraped}, Skipped: {skipped}, Total: {total_scraped}", flush=True)
                
                _operation_count += 1
                
                # Progress logging with memory monitoring
                if page_num % PROGRESS_INTERVAL == 0:
                    mem_mb = get_memory_usage_mb()
                    print(f"[PROGRESS] Page {page_num}/{last_page} | Total scraped: {total_scraped} | Memory: {mem_mb:.1f}MB", flush=True)
                    # Force GC periodically
                    force_cleanup()
                
                # PERFORMANCE FIX: Check memory limit periodically
                if _operation_count % MEMORY_CHECK_INTERVAL == 0:
                    if check_memory_limit():
                        print(f"[MEMORY] Memory limit exceeded, restarting Chrome...", flush=True)
                        # Restart driver to free memory
                        try:
                            driver.quit()
                        except Exception:
                            pass
                        driver = make_driver()
                        driver = navigate_with_retry(driver, f"{BASE_URL}?page={page_num}", f"Page {page_num}")
                        print(f"[MEMORY] Chrome restarted successfully", flush=True)
                
                # Navigate to next page
                if page_num < last_page:
                    time.sleep(SLEEP_BETWEEN_PAGES)
                    
                    try:
                        next_link = driver.find_element(By.CSS_SELECTOR, "p.paging a:last-child")
                        next_link.click()
                        wait_for_table(driver)
                    except Exception as e:
                        # Try direct navigation
                        driver = navigate_with_retry(driver, f"{BASE_URL}?page={page_num}", f"Page {page_num + 1}")
                
            except Exception as e:
                print(f"  [ERROR] Page {page_num} failed: {e}", flush=True)
                # Continue to next page (failed page recorded in DB)
                continue
        
        # Mark run as complete
        _repo.finish_run("completed", items_scraped=total_scraped)
        print(f"\n[COMPLETE] Scraped {total_scraped} items, skipped {total_skipped} duplicates", flush=True)
        
    except Exception as e:
        print(f"\n[FATAL] Scraper failed: {e}", flush=True)
        _repo.finish_run("failed", items_scraped=0, error_message=str(e))
        raise
    
    finally:
        cleanup_all_chrome()


if __name__ == "__main__":
    main()
