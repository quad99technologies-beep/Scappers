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
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

# Add script directory to path for config_loader import
_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

try:
    from config_loader import load_env_file, getenv, getenv_bool, get_output_dir
    load_env_file()
    OUTPUT_DIR = get_output_dir()
except ImportError:
    OUTPUT_DIR = Path(__file__).resolve().parent
    def getenv(key: str, default: str = None) -> str:
        return os.getenv(key, default if default is not None else "")
    def getenv_bool(key: str, default: bool = False) -> bool:
        val = os.getenv(key, str(default))
        return str(val).lower() in ("true", "1", "yes", "on")

try:
    from core.chrome_pid_tracker import get_chrome_pids_from_driver, save_chrome_pids, terminate_scraper_pids
except ImportError:
    get_chrome_pids_from_driver = None
    save_chrome_pids = None
    terminate_scraper_pids = None

BASE_URL = "https://lekovi.zdravstvo.gov.mk/drugsregister/overview"
URLS_CSV = getenv("SCRIPT_01_URLS_CSV", "north_macedonia_detail_urls.csv")
CHECKPOINT_JSON = getenv("SCRIPT_01_CHECKPOINT_JSON", "mk_urls_checkpoint.json")
TOTAL_PAGES_OVERRIDE = getenv("SCRIPT_01_TOTAL_PAGES", "")

_driver_path = None
_driver_path_lock = None
_repo_root = Path(__file__).resolve().parents[2]


def _get_chromedriver_path() -> Optional[str]:
    global _driver_path
    global _driver_path_lock
    if _driver_path_lock is None:
        import threading
        _driver_path_lock = threading.Lock()
    with _driver_path_lock:
        if _driver_path:
            return _driver_path
        try:
            _driver_path = ChromeDriverManager().install()
        except Exception:
            return None
        return _driver_path


def build_driver(headless: bool = True) -> Optional[webdriver.Chrome]:
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1600,1000")
    options.add_argument("--lang=mk-MK")
    # Disable images/CSS for faster loads
    disable_images = getenv_bool("SCRIPT_01_DISABLE_IMAGES", True)
    disable_css = getenv_bool("SCRIPT_01_DISABLE_CSS", True)
    prefs = {}
    if disable_images:
        prefs["profile.managed_default_content_settings.images"] = 2
    if disable_css:
        prefs["profile.managed_default_content_settings.stylesheets"] = 2
    if prefs:
        options.add_experimental_option("prefs", prefs)

    driver_path = _get_chromedriver_path()
    if not driver_path:
        return None
    try:
        service = ChromeService(driver_path)
        driver = webdriver.Chrome(service=service, options=options)
    except Exception:
        return None
    driver.set_page_load_timeout(120)
    if get_chrome_pids_from_driver and save_chrome_pids:
        try:
            pids = get_chrome_pids_from_driver(driver)
            if pids:
                save_chrome_pids("NorthMacedonia", _repo_root, pids)
        except Exception:
            pass
    return driver


def wait_grid_loaded(driver: webdriver.Chrome, timeout: int = 40) -> None:
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div#grid table"))
    )
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div.t-data-grid-pager"))
    )


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
        time.sleep(0.4)
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
            time.sleep(0.4)
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
        time.sleep(0.4)
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
        time.sleep(0.4)
        wait_grid_loaded(driver, 20)
        if get_rows_per_page_value(driver) == value:
            return True
    except Exception:
        pass

    return False


def extract_detail_url_list_from_current_grid(driver: webdriver.Chrome) -> List[str]:
    wait_grid_loaded(driver, 40)
    links = driver.find_elements(By.CSS_SELECTOR, "td.latinName a[href*='detaileddrug']")
    urls = []
    for a in links:
        href = a.get_attribute("href")
        if href:
            urls.append(href)
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
        time.sleep(0.2)
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
        time.sleep(0.2)
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
    """Return last scraped page number; default to page 1."""
    checkpoint_path = OUTPUT_DIR / CHECKPOINT_JSON
    if checkpoint_path.exists():
        try:
            with open(checkpoint_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict) and "page" in data:
                    return {"page": int(data.get("page") or 1)}
        except Exception:
            pass
    return {"page": 1}


def write_checkpoint(page_num: int) -> None:
    """Persist last scraped page number only (no huge URL lists)."""
    checkpoint_path = OUTPUT_DIR / CHECKPOINT_JSON
    with open(checkpoint_path, "w", encoding="utf-8") as f:
        json.dump({"page": page_num}, f, ensure_ascii=False, indent=2)


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

    urls_path = OUTPUT_DIR / URLS_CSV
    ensure_csv_has_header(urls_path, ["detail_url", "page_num", "detailed_view_scraped"])

    checkpoint = read_checkpoint()
    seen_urls = set(load_existing_detail_urls(urls_path))
    page_num = int(checkpoint.get("page", 1))

    driver = build_driver(headless=headless)
    if driver is None:
        raise RuntimeError("Failed to initialize overview Chrome driver")

    try:
        driver.get(BASE_URL)
        wait_grid_loaded(driver, 60)

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
            time.sleep(0.3)
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
            driver.get(BASE_URL)
            wait_grid_loaded(driver, 40)
            if get_rows_per_page_value(driver) != rows_per_page:
                set_rows_per_page(driver, rows_per_page)
                wait_grid_loaded(driver, 20)
            total_pages = get_total_pages(driver, 1) or total_pages
        except Exception:
            # Retry once after a brief pause on timeout or load issues
            try:
                time.sleep(2)
                driver.get(BASE_URL)
                wait_grid_loaded(driver, 40)
                if get_rows_per_page_value(driver) != rows_per_page:
                    set_rows_per_page(driver, rows_per_page)
                    wait_grid_loaded(driver, 20)
                total_pages = get_total_pages(driver, 1) or total_pages
            except Exception:
                print("[WARN] Reload after page-size change failed; continuing with current session.", flush=True)

        while True:
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
            detail_urls = extract_detail_url_list_from_current_grid(driver)
            new_urls = [u for u in detail_urls if u not in seen_urls]

            if new_urls:
                rows = [{"detail_url": u, "page_num": page_num, "detailed_view_scraped": "no"} for u in new_urls]
                append_urls(urls_path, rows)
                seen_urls.update(new_urls)
                total_new += len(new_urls)
                if total_pages:
                    percent = round((page_num / total_pages) * 100, 1)
                    print(f"[PROGRESS] Collecting URLs: page {page_num}/{total_pages} ({percent}%) - new {len(new_urls)} (total {total_new})", flush=True)
                else:
                    print(f"[PROGRESS] Collecting URLs: page {page_num} - new {len(new_urls)} (total {total_new})", flush=True)

            # Save checkpoint after each page (only page number)
            write_checkpoint(page_num)

            # Advance page using direct pager URL if known (preferred)
            if total_pages and page_num < total_pages:
                next_page = page_num + 1
                pager_url = f"https://lekovi.zdravstvo.gov.mk/drugsregister.grid.pager/{next_page}/grid_0?t:ac=overview"
                try:
                    driver.get(pager_url)
                    wait_grid_loaded(driver, 30)
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

        print(f"Completed URL collection. Total unique detail URLs: {len(seen_urls)}")
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
    headless = getenv_bool("SCRIPT_01_HEADLESS", True)
    rows_per_page = getenv("SCRIPT_01_ROWS_PER_PAGE", "200")
    main(headless=headless, rows_per_page=rows_per_page)
