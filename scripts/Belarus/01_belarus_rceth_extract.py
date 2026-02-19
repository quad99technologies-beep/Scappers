# 01_belarus_rceth_extract.py
# Belarus RCETH Drug Price Registry Scraper - REFACTORED
# Target: https://www.rceth.by/Refbank/reestr_drugregpricenew
#
# Optimized version:
# 1. Restored missing scraping logic and main loop.
# 2. Integrated with BelarusRepository for data persistence.
# 3. Standardized driver management via core modules.
# 4. Improved speed by removing row-by-row AI translation (moved to post-processing).
# 5. Added resume capability via step_progress table.

import sys
import os
import re
import time
import random
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Optional, Set
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup

# Repo path configuration
_script_dir = Path(__file__).parent
_repo_root = _script_dir.resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

# Config loader
try:
    from config_loader import load_env_file, getenv, getenv_bool, get_output_dir
    load_env_file()
    USE_CONFIG = True
except ImportError:
    USE_CONFIG = False
    def getenv(k, d=""): return os.getenv(k, d)
    def getenv_bool(k, d=False): return str(os.getenv(k, str(d))).lower() in ("true", "1")
    def get_output_dir(): return _script_dir / "output"

# Core modules
from core.db.connection import CountryDB
from db.repositories import BelarusRepository
from core.db.models import generate_run_id

from core.network.tor_manager import check_tor_running, auto_start_tor_proxy, request_tor_newnym
from core.browser.driver_factory import create_firefox_driver
from core.browser.chrome_manager import kill_orphaned_chrome_processes, get_chromedriver_path
from core.browser import stealth_profile
from core.browser.human_actions import pause as jitter_sleep
from core.monitoring.resource_monitor import log_resource_status, periodic_resource_check
from core.monitoring.audit_logger import audit_log

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.common.exceptions import WebDriverException, TimeoutException

# ==================== CONFIGURATION ====================
START_URL = "https://www.rceth.by/Refbank/reestr_drugregpricenew"
BASE_URL = "https://www.rceth.by"

# Selectors
INN_INPUT_ID = "FProps_1__CritElems_0__Val"
SEARCH_XPATH = "//input[@type='submit' and (normalize-space(@value)='Поиск' or normalize-space(@value)='Search')]"
PAGE_SIZE_100_XPATH = "//*[self::a or self::button][normalize-space()='100']"
PAGINATION_LINKS_CSS = "a.rec-num[propval]"

# Constants from env or defaults
RECYCLE_DRIVER_EVERY_N = int(getenv("SCRIPT_01_RECYCLE_DRIVER_EVERY_N", "15") or "15")
USE_TOR_BROWSER = getenv_bool("SCRIPT_01_USE_TOR_BROWSER", True)
TOR_NEWNYM_ON_RECYCLE = getenv_bool("SCRIPT_01_TOR_NEWNYM_ON_RECYCLE", True)
MAX_RETRY_ATTEMPTS = int(getenv("SCRIPT_01_MAX_RETRY_ATTEMPTS", "3") or "3")
PAGINATION_TIMEOUT = int(getenv("SCRIPT_01_PAGINATION_TIMEOUT", "240") or "240")
TABLE_STABLE_SECONDS = float(getenv("SCRIPT_01_TABLE_STABLE_SECONDS", "1.5") or "1.5")

# Regex
USD_EQ_RE = re.compile(r"Equivalent price on registration date:\s*([0-9]+(?:[.,][0-9]+)?)\s*USD", re.IGNORECASE)
PRICE_CELL_RE = re.compile(r"([0-9]+(?:[.,][0-9]+)?)\s*([A-Z]{3})", re.IGNORECASE)

# ==================== UTILITY FUNCTIONS ====================

def parse_price_cell(text: str):
    if not text: return None, None
    t = " ".join(text.split())
    m = PRICE_CELL_RE.search(t)
    if not m: return None, None
    return float(m.group(1).replace(",", ".")), m.group(2).upper()

def parse_import_price_usd(text: str):
    if not text: return None, None
    m = USD_EQ_RE.search(text)
    if m:
        return float(m.group(1).replace(",", ".")), "USD"
    return None, None

def parse_strength_pack(dosage_form: str):
    """Extract strength/unit and pack size from dosage form text."""
    strength, unit, pack = "", "", "1"
    if not dosage_form: return strength, unit, pack
    
    # Simple regex for strength
    m_s = re.search(r"(\d+(?:\.\d+)?)\s*(mg|g|ml|mcg|iu)", dosage_form, re.IGNORECASE)
    if m_s:
        strength, unit = m_s.group(1), m_s.group(2).lower()
        
    # Simple regex for pack size (No10, 10 tabs, etc)
    m_p = re.search(r"[Nn]o?(\d+)(?:x(\d+))?|(\d+)\s*(?:pcs|шт)", dosage_form, re.IGNORECASE)
    if m_p:
        if m_p.group(1) and m_p.group(2):
            pack = str(int(m_p.group(1)) * int(m_p.group(2)))
        elif m_p.group(1):
            pack = m_p.group(1)
        elif m_p.group(3):
            pack = m_p.group(3)
            
    return strength, unit, pack

# ==================== BROWSER FUNCTIONS ====================

def build_driver(show_browser=None):
    if show_browser is None:
        show_browser = not getenv_bool("SCRIPT_01_HEADLESS", False)
        
    if USE_TOR_BROWSER:
        # Check Tor requirements
        tor_ok, port = check_tor_running()
        if not tor_ok:
            auto_start_tor_proxy()
            tor_ok, port = check_tor_running()
            
        driver = create_firefox_driver(headless=not show_browser, tor_config={"proxy_port": port} if tor_ok else None)
    else:
        # Chrome
        opts = webdriver.ChromeOptions()
        if not show_browser:
            opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        stealth_profile.apply_selenium(opts)
        
        # Use core chrome manager
        driver_path = get_chromedriver_path()
        if not driver_path:
            from webdriver_manager.chrome import ChromeDriverManager
            driver_path = ChromeDriverManager().install()
        service = ChromeService(driver_path)
        driver = webdriver.Chrome(service=service, options=opts)
        
    driver.set_page_load_timeout(PAGINATION_TIMEOUT)
    return driver

def wait_table_stable(driver, timeout=None):
    """Wait for results table to stop changing."""
    timeout = timeout or PAGINATION_TIMEOUT
    end = time.time() + timeout
    last_count = -1
    last_change = time.time()
    
    while time.time() < end:
        try:
            rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
            count = len(rows)
            if count > 0 and count == last_count:
                if time.time() - last_change >= TABLE_STABLE_SECONDS:
                    return True
            else:
                last_count = count
                last_change = time.time()
            time.sleep(0.3)
        except Exception:
            time.sleep(0.5)
    return False

def safe_click(driver, el):
    try:
        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", el)
        time.sleep(0.3)
        el.click()
        return True
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            return False

def translate_page_to_english(driver):
    """Aggressive JS-based text replacement for common Russian UI terms."""
    script = """
    (function() {
        var map = {'Поиск': 'Search', 'Найти': 'Find', 'МНН': 'INN', 'Торговое': 'Trade', 'Лекарственная': 'Dosage'};
        function walk(node) {
            if (node.nodeType === 3) {
                for (var k in map) {
                    if (node.textContent.includes(k)) node.textContent = node.textContent.replace(k, map[k]);
                }
            } else if (node.nodeType === 1) {
                for (var i=0; i<node.childNodes.length; i++) walk(node.childNodes[i]);
            }
        }
        walk(document.body);
    })();
    """
    try:
        driver.execute_script(script)
    except Exception as e:
        print(f"  [WARN] JS translation script failed: {e}", flush=True)

# ==================== EXTRACTION ====================

def extract_rows(html: str, inn_term: str):
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table: return []
    
    tbody = table.find("tbody") or table
    trs = tbody.find_all("tr")
    
    results = []
    for tr in trs:
        tds = tr.find_all("td")
        if len(tds) < 8: continue
        
        texts = [td.get_text(" ", strip=True) for td in tds]
        def val(i): return texts[i] if i < len(texts) else ""
        
        trade_name = val(1)
        inn = val(2) or inn_term
        dosage_form = val(3)
        atc = val(4)
        mah = val(5)
        producer = val(6)
        reg_number = val(7)
        
        p_val, p_ccy = parse_price_cell(val(8))
        imp_val, imp_ccy = parse_import_price_usd(val(9))
        
        strength, s_unit, pack = parse_strength_pack(dosage_form)
        
        details_a = tr.find("a", href=True)
        url = urljoin(BASE_URL, details_a["href"]) if details_a else ""
        
        results.append({
            "inn": inn,
            "trade_name": trade_name,
            "manufacturer": producer,
            "dosage_form": dosage_form,
            "strength": strength,
            "pack_size": pack,
            "registration_number": reg_number,
            "wholesale_price": p_val,
            "retail_price": p_val,
            "import_price": imp_val,
            "import_price_currency": imp_ccy,
            "currency": p_ccy or "BYN",
            "atc_code": atc,
            "marketing_authority": mah,
            "source_url": url,
            "scraped_at": datetime.now(timezone.utc).isoformat()
        })
    return results

# ==================== MAIN LOGIC ====================

def scrape_for_inn(driver, inn_term: str):
    driver.get(START_URL)
    wait = WebDriverWait(driver, 30)
    
    try:
        # Debug: Check if page loaded
        print(f"  [DEBUG] Loading page: {START_URL}")
        search_input = wait.until(EC.element_to_be_clickable((By.ID, INN_INPUT_ID)))
        print(f"  [DEBUG] Found search input: {INN_INPUT_ID}")
        
        search_input.clear()
        search_input.send_keys(inn_term)
        print(f"  [DEBUG] Entered search term: {inn_term}")
        
        search_btn = driver.find_element(By.XPATH, SEARCH_XPATH)
        print(f"  [DEBUG] Found search button, clicking...")
        safe_click(driver, search_btn)
        
        table_stable = wait_table_stable(driver)
        print(f"  [DEBUG] Table stable: {table_stable}")
        
        translate_page_to_english(driver)
        
        # Set 100 per page
        try:
            page_size_btn = driver.find_element(By.XPATH, PAGE_SIZE_100_XPATH)
            safe_click(driver, page_size_btn)
            wait_table_stable(driver)
            print(f"  [DEBUG] Set page size to 100")
        except Exception as e: 
            print(f"  [DEBUG] Could not set page size: {e}")
        
        all_rows = []
        # Page 1
        page_source = driver.page_source
        print(f"  [DEBUG] Page source length: {len(page_source)}")
        
        # Check if table exists
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(page_source, "lxml")
        table = soup.find("table")
        print(f"  [DEBUG] Table found: {table is not None}")
        if table:
            tbody = table.find("tbody") or table
            trs = tbody.find_all("tr")
            print(f"  [DEBUG] Number of rows found: {len(trs)}")
        
        all_rows.extend(extract_rows(page_source, inn_term))
        print(f"  [DEBUG] Extracted {len(all_rows)} rows from page 1")
        
        # Check pagination
        try:
            pages = driver.find_elements(By.CSS_SELECTOR, PAGINATION_LINKS_CSS)
            print(f"  [DEBUG] Pagination links found: {len(pages)}")
            max_page = 1
            if pages:
                vals = [int(p.get_attribute("propval")) for p in pages if p.get_attribute("propval") and p.get_attribute("propval").isdigit()]
                max_page = max(vals) if vals else 1
                print(f"  [DEBUG] Max page: {max_page}")
            
            for p_num in range(2, max_page + 1):
                # Re-find the pagination link each time (DOM may have changed)
                try:
                    p_links = driver.find_elements(By.CSS_SELECTOR, f"a.rec-num[propval='{p_num}']")
                    if p_links:
                        if safe_click(driver, p_links[0]):
                            wait_table_stable(driver)
                            page_rows = extract_rows(driver.page_source, inn_term)
                            all_rows.extend(page_rows)
                            print(f"  [DEBUG] Extracted {len(page_rows)} rows from page {p_num}")
                    else:
                        print(f"  [DEBUG] Page {p_num} link not found, stopping pagination")
                        break
                except Exception as pe:
                    print(f"  [DEBUG] Could not navigate to page {p_num}: {pe}")
                    break
        except Exception as e: 
            print(f"  [DEBUG] Pagination error: {e}")
        
        return all_rows
    except TimeoutException:
        print(f"  [TIMEOUT] Searching for {inn_term}")
        return []
    except Exception as e:
        print(f"  [ERROR] {e}")
        return []

def main():
    # 0. Initialize run
    run_id = os.environ.get("BELARUS_RUN_ID") or generate_run_id()
    print(f"[INIT] Belarus Scraper Run ID: {run_id}")
    
    # 1. Connect to DB
    db = CountryDB("Belarus")
    repo = BelarusRepository(db, run_id)
    
    # Register run in ledger
    try:
        repo.start_run(mode="fresh")
    except Exception as e:
        print(f"[WARN] Could not register run in ledger: {e}")
    
    # 2. Get INN list
    inns = repo.get_unique_inns()
    if not inns:
        print("[WARN] No INNs found in by_input_generic_names. Falling back to empty search (full scrape).")
        inns = [""] # Empty string for empty search
    
    # 3. Resume progress
    completed_inns = repo.get_completed_keys(step_number=1)
    print(f"[RECOVER] Found {len(completed_inns)} already processed items.")
    
    # 4. Scrape Loop
    driver = build_driver()
    all_extracted = []
    
    try:
        for idx, inn in enumerate(inns, 1):
            if inn in completed_inns: continue
            
            print(f"[{idx}/{len(inns)}] Processing: {inn}" if inn else "[FULL] Scoping all records")
            
            # Audit log
            audit_log("Scrape Attempt", "Belarus", run_id, {"inn": inn})
            
            # Scrape
            rows = scrape_for_inn(driver, inn)
            print(f"  -> Extracted {len(rows)} rows")
            
            # Save to DB
            if rows:
                repo.insert_rceth_data(rows)
                all_extracted.extend(rows)
            
            # Record progress
            repo.mark_progress(1, "Extract RCETH Data", progress_key=inn, status="completed")
                
            # Recycle driver
            if idx % RECYCLE_DRIVER_EVERY_N == 0:
                print("  [RECYCLE] Restarting driver to maintain stability...")
                driver.quit()
                if TOR_NEWNYM_ON_RECYCLE and USE_TOR_BROWSER: request_tor_newnym()
                driver = build_driver()
            
            jitter_sleep(1.0, 3.0)
            periodic_resource_check("Belarus")
            
    except KeyboardInterrupt:
        print("[ABORT] Stopped by user")
    except Exception as e:
        print(f"[CRITICAL] {e}")
        audit_log("Scraper Failure", "Belarus", run_id, {"error": str(e)})
    finally:
        driver.quit()
        db.close()
        print(f"[DONE] Belarus Scraper Step 1 completed. Data saved to database.")

if __name__ == "__main__":
    main()
