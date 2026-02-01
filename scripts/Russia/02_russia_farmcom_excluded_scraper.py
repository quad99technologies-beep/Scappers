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
    load_env_file, getenv, getenv_bool, getenv_int, getenv_float
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

# Chrome tracking
from core.chrome_pid_tracker import get_chrome_pids_from_driver, save_chrome_pids
from core.chrome_manager import register_chrome_driver, unregister_chrome_driver

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
MAX_PAGES = getenv_int("SCRIPT_02_MAX_PAGES")
DB_BATCH_SIZE = getenv_int("DB_BATCH_INSERT_SIZE", 100)
PROGRESS_INTERVAL = getenv_int("DB_PROGRESS_LOG_INTERVAL", 50)

CHROME_NO_SANDBOX = getenv("SCRIPT_02_CHROME_NO_SANDBOX")
CHROME_DISABLE_DEV_SHM = getenv("SCRIPT_02_CHROME_DISABLE_DEV_SHM")

# =============================================================================
# GLOBAL STATE
# =============================================================================

_shutdown_requested = False
_active_drivers: List[webdriver.Chrome] = []
_run_id: Optional[str] = None
_repo: Optional[RussiaRepository] = None

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

def cleanup_all_chrome():
    print(f"[CLEANUP] Cleaning up {len(_active_drivers)} Chrome instance(s)...", flush=True)
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
    print("[CLEANUP] Done", flush=True)

atexit.register(cleanup_all_chrome)

def track_driver(driver: webdriver.Chrome):
    _active_drivers.append(driver)
    register_chrome_driver(driver)
    try:
        pids = get_chrome_pids_from_driver(driver)
        if pids:
            save_chrome_pids("Russia", _repo_root, pids)
    except Exception:
        pass

def make_driver() -> webdriver.Chrome:
    opts = ChromeOptions()
    
    if HEADLESS:
        opts.add_argument("--headless=new")
    
    if CHROME_NO_SANDBOX:
        opts.add_argument(CHROME_NO_SANDBOX)
    if CHROME_DISABLE_DEV_SHM:
        opts.add_argument(CHROME_DISABLE_DEV_SHM)
    
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1600,1000")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-plugins")
    opts.add_argument("--disable-images")
    
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    
    prefs = {"profile.managed_default_content_settings.images": 2}
    opts.add_experimental_option("prefs", prefs)
    
    service = ChromeService(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    
    track_driver(driver)
    return driver

# =============================================================================
# NAVIGATION
# =============================================================================

def wait_for_table(driver: webdriver.Chrome) -> None:
    WebDriverWait(driver, WAIT_TIMEOUT).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table.report tbody tr"))
    )
    time.sleep(0.5)


def select_region_and_search(driver: webdriver.Chrome) -> webdriver.Chrome:
    driver.get(BASE_URL)
    WebDriverWait(driver, WAIT_TIMEOUT).until(
        EC.presence_of_element_located((By.ID, "reg_id"))
    )
    
    Select(driver.find_element(By.ID, "reg_id")).select_by_value(REGION_VALUE)
    driver.find_element(By.ID, "btn_submit").click()
    
    wait_for_table(driver)
    return driver

# =============================================================================
# DATA EXTRACTION
# =============================================================================

def parse_price(val: str) -> str:
    if not val:
        return ""
    nums = re.findall(r"[\d\s]+", val.replace(" ", ""))
    return nums[0] if nums else val.strip()


def extract_row_data(row, page_num: int) -> Optional[Dict]:
    try:
        cells = row.find_elements(By.TAG_NAME, "td")
        if len(cells) < 7:
            return None
        
        item_id = ""
        try:
            onclick = cells[0].get_attribute("onclick") or ""
            match = re.search(r"showInfo\((\d+)\)", onclick)
            if match:
                item_id = match.group(1)
        except Exception:
            pass
        
        if not item_id:
            return None
        
        return {
            "item_id": item_id,
            "tn": cells[1].text.strip(),
            "inn": cells[2].text.strip(),
            "manufacturer_country": cells[3].text.strip(),
            "release_form": cells[4].text.strip(),
            "ean": "",
            "registered_price_rub": parse_price(cells[5].text),
            "start_date_text": cells[6].text.strip(),
            "page_number": page_num,
        }
    except Exception:
        return None


def scrape_page(driver: webdriver.Chrome, page_num: int, repo: RussiaRepository, existing_ids: Set[str]) -> tuple[int, int]:
    scraped = 0
    skipped = 0
    batch = []
    
    try:
        rows = driver.find_elements(By.CSS_SELECTOR, "table.report tbody tr")
        
        for row in rows:
            if _shutdown_requested:
                break
            
            data = extract_row_data(row, page_num)
            if not data:
                continue
            
            if data["item_id"] in existing_ids:
                skipped += 1
                continue
            
            batch.append(data)
            existing_ids.add(data["item_id"])
            
            if len(batch) >= DB_BATCH_SIZE:
                repo.insert_excluded_products_bulk(batch)
                scraped += len(batch)
                batch = []
        
        if batch:
            repo.insert_excluded_products_bulk(batch)
            scraped += len(batch)
        
        repo.mark_progress(2, "Excluded Scrape", f"excluded_page:{page_num}", "completed")
        
        return scraped, skipped
        
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


def get_resume_page(repo: RussiaRepository) -> int:
    completed = repo.get_completed_keys(2)
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
    
    _run_id = os.getenv("RUSSIA_RUN_ID") or generate_run_id()
    os.environ["RUSSIA_RUN_ID"] = _run_id
    print(f"[INIT] Run ID: {_run_id}", flush=True)
    
    _repo = RussiaRepository(db, _run_id)
    
    resume_page = get_resume_page(_repo)
    if resume_page > 1:
        print(f"[INIT] Resuming from page {resume_page}", flush=True)
        _repo.resume_run()
    else:
        print(f"[INIT] Starting fresh", flush=True)
        _repo.start_run("fresh")
    
    existing_ids = set()  # Excluded list doesn't need dedup across runs
    
    driver = make_driver()
    
    try:
        print(f"[NAV] Navigating to {BASE_URL}...", flush=True)
        driver = select_region_and_search(driver)
        
        last_page = get_last_page(driver)
        if MAX_PAGES > 0:
            last_page = min(last_page, MAX_PAGES)
        print(f"[INFO] Total pages: {last_page}", flush=True)
        
        # Navigate to resume page
        start_page = resume_page
        for page in range(2, start_page + 1):
            if _shutdown_requested:
                break
            try:
                next_link = driver.find_element(By.CSS_SELECTOR, "p.paging a:last-child")
                next_link.click()
                wait_for_table(driver)
            except Exception:
                driver.get(f"{BASE_URL}?page={page-1}")
                wait_for_table(driver)
        
        total_scraped = 0
        
        for page_num in range(start_page, last_page + 1):
            if _shutdown_requested:
                break
            
            print(f"[PAGE {page_num}/{last_page}] Scraping...", flush=True)
            
            try:
                scraped, skipped = scrape_page(driver, page_num, _repo, existing_ids)
                total_scraped += scraped
                
                print(f"  [OK] Scraped: {scraped}, Total: {total_scraped}", flush=True)
                
                if page_num % PROGRESS_INTERVAL == 0:
                    print(f"[PROGRESS] Page {page_num}/{last_page} | Total: {total_scraped}", flush=True)
                    gc.collect()
                
                if page_num < last_page:
                    time.sleep(SLEEP_BETWEEN_PAGES)
                    try:
                        next_link = driver.find_element(By.CSS_SELECTOR, "p.paging a:last-child")
                        next_link.click()
                        wait_for_table(driver)
                    except Exception:
                        driver.get(f"{BASE_URL}?page={page_num}")
                        wait_for_table(driver)
                
            except Exception as e:
                print(f"  [ERROR] Page {page_num}: {e}", flush=True)
                continue
        
        _repo.finish_run("completed", items_scraped=total_scraped)
        print(f"\n[COMPLETE] Scraped {total_scraped} excluded items", flush=True)
        
    except Exception as e:
        print(f"\n[FATAL] {e}", flush=True)
        _repo.finish_run("failed", error_message=str(e))
        raise
    
    finally:
        cleanup_all_chrome()


if __name__ == "__main__":
    main()
