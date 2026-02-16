#!/usr/bin/env python3
"""
North Macedonia URL Collector - Simplified Single-Worker Version
Uses Selenium with a single browser instance for reliability.
"""

import os
import json
import time
import sys
import math
from pathlib import Path
from typing import List, Dict, Optional, Set

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, InvalidSessionIdException
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from urllib3.exceptions import MaxRetryError, NewConnectionError, ProtocolError

# Add script directory to path for config_loader import
_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

try:
    from config_loader import load_env_file, getenv, getenv_bool, getenv_int, get_output_dir
    load_env_file()
    OUTPUT_DIR = get_output_dir()
except ImportError:
    OUTPUT_DIR = Path(__file__).resolve().parent
    def getenv(key: str, default: str = None) -> str:
        return os.getenv(key, default if default is not None else "")
    def getenv_bool(key: str, default: bool = False) -> bool:
        val = getenv(key, str(default))
        return str(val).lower() in ("true", "1", "yes", "on")
    def getenv_int(key: str, default: int = 0) -> int:
        try:
            return int(getenv(key, str(default)))
        except (ValueError, TypeError):
            return default

try:
    from core.browser.chrome_pid_tracker import get_chrome_pids_from_driver, save_chrome_pids, terminate_scraper_pids
except ImportError:
    get_chrome_pids_from_driver = None
    save_chrome_pids = None
    terminate_scraper_pids = None

BASE_URL = "https://lekovi.zdravstvo.gov.mk/drugsregister/overview"
CHECKPOINT_JSON = getenv("SCRIPT_01_CHECKPOINT_JSON", "mk_urls_checkpoint.json")

_driver_path = None


def _get_chromedriver_path() -> Optional[str]:
    global _driver_path
    if _driver_path:
        return _driver_path
    try:
        _driver_path = ChromeDriverManager().install()
    except Exception:
        return None
    return _driver_path


def build_driver(headless: bool = True, proxy: str = None) -> Optional[webdriver.Chrome]:
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=mk-MK")
    
    # Add Tor proxy if provided
    if proxy:
        options.add_argument(f"--proxy-server={proxy}")
        print(f"[DRIVER] Using proxy: {proxy}", flush=True)
    
    # Disable images for faster loading
    prefs = {"profile.managed_default_content_settings.images": 2}
    options.add_experimental_option("prefs", prefs)

    driver_path = _get_chromedriver_path()
    if not driver_path:
        return None
    
    service = ChromeService(driver_path)
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(120)
    
    if get_chrome_pids_from_driver and save_chrome_pids:
        try:
            pids = get_chrome_pids_from_driver(driver)
            if pids:
                save_chrome_pids("NorthMacedonia", _repo_root, pids)
        except Exception:
            pass
    return driver


DRIVER_ERROR_TYPES = (WebDriverException, InvalidSessionIdException, MaxRetryError, NewConnectionError, ProtocolError)


def is_driver_error(exc: Exception) -> bool:
    return isinstance(exc, DRIVER_ERROR_TYPES)


def is_driver_alive(driver: webdriver.Chrome) -> bool:
    try:
        _ = driver.current_url
        return True
    except Exception:
        return False


def restart_driver(driver: webdriver.Chrome, headless: bool, proxy: str, reason: str) -> webdriver.Chrome:
    print(f"[DRIVER] Restarting driver ({reason})...", flush=True)
    try:
        driver.quit()
    except Exception:
        pass
    time.sleep(2)
    new_driver = build_driver(headless=headless, proxy=proxy)
    if new_driver is None:
        raise RuntimeError("Failed to reinitialize Chrome driver")
    return new_driver


def wait_grid_loaded(driver: webdriver.Chrome, timeout: int = 60) -> None:
    """Wait for the grid to be fully loaded."""
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "td.latinName a"))
    )


def get_total_records(driver: webdriver.Chrome) -> Optional[int]:
    """Parse total record count from pager text."""
    try:
        pager = driver.find_element(By.CSS_SELECTOR, "div.t-data-grid-pager")
        text = pager.text or ""
        import re
        # Match patterns like "1-10 of 4102" or "1-10 од 4102"
        m = re.search(r'\d+-\d+\s+(?:of|од)\s+(\d+)', text, re.IGNORECASE)
        if m:
            return int(m.group(1))
    except Exception:
        pass
    return None


def set_rows_per_page(driver: webdriver.Chrome, value: str = "200") -> bool:
    """Set rows per page using JavaScript."""
    try:
        # Try to find and set the dropdown
        driver.execute_script("""
            const sel = document.querySelector("select[name='rowsPerPage'], select[id^='rowsPerPage']");
            if (sel) {
                sel.value = arguments[0];
                sel.dispatchEvent(new Event('change', {bubbles: true}));
                return true;
            }
            return false;
        """, value)
        time.sleep(2)
        return True
    except Exception:
        return False


def extract_urls_from_page(driver: webdriver.Chrome) -> List[str]:
    """Extract all detail URLs from current page."""
    links = driver.find_elements(By.CSS_SELECTOR, "td.latinName a[href*='detaileddrug']")
    urls = []
    for a in links:
        href = a.get_attribute("href")
        if href:
            urls.append(href)
    return urls


def read_checkpoint() -> Dict:
    checkpoint_path = OUTPUT_DIR / CHECKPOINT_JSON
    if checkpoint_path.exists():
        try:
            with open(checkpoint_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"page": 1, "urls": []}


def write_checkpoint(page_num: int, urls: List[str]) -> None:
    checkpoint_path = OUTPUT_DIR / CHECKPOINT_JSON
    with open(checkpoint_path, "w", encoding="utf-8") as f:
        json.dump({"page": page_num, "urls": urls}, f, ensure_ascii=False, indent=2)


def main():
    # Log run_id at start
    run_id = os.environ.get("NORTH_MACEDONIA_RUN_ID", "")
    print("=" * 60)
    print("URL COLLECTION - SIMPLE SELENIUM VERSION")
    print(f"RUN ID: {run_id}")
    print("=" * 60)
    
    if terminate_scraper_pids:
        try:
            terminate_scraper_pids("NorthMacedonia", _repo_root, silent=True)
        except Exception:
            pass

    # Setup Tor proxy
    tor_proxy = None
    try:
        sys.path.insert(0, str(_repo_root))
        from core.network.tor_httpx import TorConfig, setup_tor
        from config_loader import getenv as config_getenv
        tor_cfg = TorConfig.from_env(config_getenv)
        tor_url = setup_tor(tor_cfg)
        if tor_url:
            # Convert socks5://host:port to socks5://host:port for Chrome
            tor_proxy = tor_url
            print(f"[TOR] Using Tor proxy: {tor_proxy}", flush=True)
        else:
            print("[TOR] Tor not available, proceeding without proxy", flush=True)
    except Exception as e:
        print(f"[TOR] Error setting up Tor: {e}", flush=True)
        tor_proxy = None

    # Database-only mode - no CSV
    checkpoint = read_checkpoint()
    seen_urls = set(checkpoint.get("urls", []))
    start_page = checkpoint.get("page", 1)

    driver = build_driver(headless=True, proxy=tor_proxy)
    if driver is None:
        raise RuntimeError("Failed to initialize Chrome driver")
    
    try:
        # Load initial page with retries
        print(f"\n[INIT] Loading base page...", flush=True)
        for attempt in range(3):
            try:
                driver.get(BASE_URL)
                wait_grid_loaded(driver, 60)
                break
            except Exception as e:
                print(f"[WARN] Page load attempt {attempt + 1} failed: {e}", flush=True)
                if is_driver_error(e) or not is_driver_alive(driver):
                    driver = restart_driver(driver, headless=True, proxy=tor_proxy, reason="base page load")
                if attempt < 2:
                    print(f"[INIT] Retrying in 10s...", flush=True)
                    time.sleep(10)
                else:
                    raise

        # Get total records
        total_records = get_total_records(driver)
        print(f"[INIT] Total records: {total_records}", flush=True)

        # Set rows per page to 200 with retries
        for attempt in range(3):
            try:
                print(f"[INIT] Setting rows per page to 200...", flush=True)
                set_rows_per_page(driver, "200")
                
                # Wait for grid to reload with 200 rows
                print(f"[INIT] Waiting for grid to reload...", flush=True)
                time.sleep(3)
                wait_grid_loaded(driver, 60)
                break
            except Exception as e:
                print(f"[WARN] Rows-per-page attempt {attempt + 1} failed: {e}", flush=True)
                if is_driver_error(e) or not is_driver_alive(driver):
                    driver = restart_driver(driver, headless=True, proxy=tor_proxy, reason="rows-per-page change")
                    try:
                        driver.get(BASE_URL)
                        wait_grid_loaded(driver, 60)
                    except Exception:
                        pass
                if attempt < 2:
                    time.sleep(5)
                else:
                    raise

        # Recalculate total pages
        total_records = get_total_records(driver) or total_records
        if total_records:
            total_pages = math.ceil(total_records / 200)
        else:
            total_pages = 21  # Fallback
        
        print(f"[INIT] Total pages: {total_pages}", flush=True)

        # Process each page
        for page_num in range(start_page, total_pages + 1):
            # Navigate to page using direct URL
            pager_url = f"https://lekovi.zdravstvo.gov.mk/drugsregister.grid.pager/{page_num}/grid_0?t:ac=overview"
            page_attempts = 0
            while page_attempts < 3:
                try:
                    driver.get(pager_url)
                    wait_grid_loaded(driver, 60)
                    
                    # Ensure rows per page is still 200
                    set_rows_per_page(driver, "200")
                    time.sleep(2)
                    wait_grid_loaded(driver, 30)
                    
                    # Extract URLs
                    urls = extract_urls_from_page(driver)
                    new_urls = [u for u in urls if u not in seen_urls]
                    
                    if new_urls:
                        seen_urls.update(new_urls)
                    
                    percent = round((page_num / total_pages) * 100, 1)
                    print(f"[PROGRESS] Page {page_num}/{total_pages} ({percent}%) - {len(new_urls)} new URLs (total {len(seen_urls)})", flush=True)
                    
                    # Save checkpoint
                    write_checkpoint(page_num + 1, list(seen_urls))
                    
                    # Small delay between pages
                    time.sleep(1)
                    break
                except Exception as e:
                    page_attempts += 1
                    print(f"[WARN] Page {page_num} attempt {page_attempts} failed: {e}", flush=True)
                    if is_driver_error(e) or not is_driver_alive(driver):
                        driver = restart_driver(driver, headless=True, proxy=tor_proxy, reason=f"page {page_num} load")
                    if page_attempts < 3:
                        time.sleep(5)
                        continue
                    print(f"[ERROR] Page {page_num} failed after retries", flush=True)
                    write_checkpoint(page_num, list(seen_urls))
                    time.sleep(5)
                    break

        print(f"\n[SUCCESS] Completed! Total unique URLs: {len(seen_urls)}")
        
        # Write to database
        if run_id:
            try:
                from core.db.connection import CountryDB
                from db.repositories import NorthMacedoniaRepository
                
                db = CountryDB("NorthMacedonia")
                repo = NorthMacedoniaRepository(db, run_id)
                repo.ensure_run_in_ledger(mode="resume")
                
                db_rows = [{"detail_url": u, "page_num": 0, "status": "pending"} for u in seen_urls]
                if db_rows:
                    batch = 500
                    inserted = 0
                    for i in range(0, len(db_rows), batch):
                        chunk = db_rows[i:i + batch]
                        try:
                            repo.insert_urls(chunk)
                            inserted += len(chunk)
                        except Exception as e:
                            print(f"[DB WARN] Batch insert error: {e}", flush=True)
                    print(f"[DB] Inserted {inserted} URLs into nm_urls", flush=True)
            except Exception as e:
                print(f"[DB] Not available: {e}", flush=True)
        
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        if terminate_scraper_pids:
            try:
                terminate_scraper_pids("NorthMacedonia", _repo_root, silent=True)
            except Exception:
                pass


if __name__ == "__main__":
    main()
