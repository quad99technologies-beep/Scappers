#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Russia Farmcom VED Registry Scraper

Scrapes drug pricing data from the Russian VED (Vital and Essential Drugs) registry
at farmcom.info for a specified region.

Author: Enterprise PDF Processing Pipeline
License: Proprietary
"""

import os
import re
import csv
import sys
import time
from dataclasses import dataclass
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
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions

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

# Configuration from env or defaults
BASE_URL = getenv("SCRIPT_01_BASE_URL", "http://farmcom.info/site/reestr") if USE_CONFIG else "http://farmcom.info/site/reestr"

# Moscow region per your annotation (reg_id value=50)
REGION_VALUE = getenv("SCRIPT_01_REGION_VALUE", "50") if USE_CONFIG else "50"

# Output
if USE_CONFIG:
    OUT_DIR = get_output_dir()
    OUT_CSV = OUT_DIR / getenv("SCRIPT_01_OUTPUT_CSV", "russia_farmcom_ved_moscow_region.csv")
else:
    OUT_CSV = Path(__file__).parent / "russia_farmcom_ved_moscow_region.csv"

# Safety / stability
PAGE_LOAD_TIMEOUT = getenv_int("SCRIPT_01_PAGE_LOAD_TIMEOUT", 60) if USE_CONFIG else 60
WAIT_TIMEOUT = getenv_int("SCRIPT_01_WAIT_TIMEOUT", 30) if USE_CONFIG else 30
CLICK_RETRY = getenv_int("SCRIPT_01_CLICK_RETRY", 3) if USE_CONFIG else 3
SLEEP_BETWEEN_PAGES = getenv_float("SCRIPT_01_SLEEP_BETWEEN_PAGES", 0.3) if USE_CONFIG else 0.3

# EAN fetching - disabled by default as it's very slow (clicks popup for each row)
FETCH_EAN = getenv_bool("SCRIPT_01_FETCH_EAN", False) if USE_CONFIG else False
EAN_POPUP_TIMEOUT = getenv_int("SCRIPT_01_EAN_POPUP_TIMEOUT", 3) if USE_CONFIG else 3  # Short timeout for EAN popup


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


def make_driver(headless: bool = None) -> webdriver.Chrome:
    """Build Chrome driver with config support"""
    if headless is None:
        # Get from config if available
        if USE_CONFIG:
            headless = getenv_bool("SCRIPT_01_HEADLESS", True)
        else:
            headless = True
    
    opts = ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1600,1000")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--lang=en-US")

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

    service = ChromeService(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    
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


def select_region_and_search(driver: webdriver.Chrome) -> None:
    driver.get(BASE_URL)

    # select#reg_id exists on the page
    WebDriverWait(driver, WAIT_TIMEOUT).until(
        EC.presence_of_element_located((By.ID, "reg_id"))
    )

    Select(driver.find_element(By.ID, "reg_id")).select_by_value(REGION_VALUE)

    # click Find button (input#btn_submit)
    driver.find_element(By.ID, "btn_submit").click()

    wait_for_table(driver)


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


def fetch_ean_by_clicking_barcode(driver: webdriver.Chrome, row_tr) -> str:
    """
    Clicks the 'Barcode' link inside the Release form cell (if present),
    waits for div#pop-up to appear, then extracts digits.
    
    NOTE: This is SLOW - each click waits for popup. Disabled by default.
    Enable with SCRIPT_01_FETCH_EAN=true in config.
    """
    if not FETCH_EAN:
        return ""  # Skip EAN fetching for speed
    
    try:
        # Release form cell is typically the 6th visible column in the row:
        # td indices (0-based): [bullet, instr, TN, INN, Manufacturer, ReleaseForm, PriceDate]
        tds = row_tr.find_elements(By.CSS_SELECTOR, "td")
        if len(tds) < 7:
            return ""

        release_td = tds[5]
        barcode_links = release_td.find_elements(By.CSS_SELECTOR, "a.info")
        if not barcode_links:
            return ""

        # Click the "Barcode" link (calls getEanCode('item_id');return false;)
        safe_click(barcode_links[0])

        # Wait for pop-up to show some text (use short timeout)
        pop = WebDriverWait(driver, EAN_POPUP_TIMEOUT).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "div#pop-up"))
        )

        txt = (pop.text or "").strip()

        # Hide popup (optional) by clicking elsewhere
        driver.execute_script("arguments[0].style.display='none';", pop)

        # Extract EAN digits (8-14 typical, but keep flexible)
        m = re.search(r"\b(\d{8,18})\b", txt.replace(" ", ""))
        return m.group(1) if m else txt  # if digits not found, return raw popup text for debugging
    except TimeoutException:
        return ""
    except Exception:
        return ""


def extract_rows_from_current_page(driver: webdriver.Chrome, page_num: int = 0) -> list[RowData]:
    wait_for_table(driver)

    rows: list[RowData] = []
    tr_list = driver.find_elements(By.CSS_SELECTOR, "table.report tbody tr")
    total_rows = len(tr_list)
    
    print(f"  Processing {total_rows} rows...", flush=True)

    for idx, tr in enumerate(tr_list, 1):
        try:
            # Only main rows have the bullet image with linkhref
            bullet_imgs = tr.find_elements(By.CSS_SELECTOR, "img.bullet[linkhref]")
            if not bullet_imgs:
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
            # Remove trailing "Barcode" text from release form cell
            release_form = re.sub(r"\bBarcode\b\s*$", "", release_form_full).strip()

            price, date_text = extract_price_and_date(tds[6].text)

            ean = fetch_ean_by_clicking_barcode(driver, tr)

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


def go_to_page(driver: webdriver.Chrome, page_num: int) -> None:
    # Pagination is GET-based (?page=N) and region selection is typically held by session/cookie.
    url = f"{BASE_URL}?page={page_num}"
    driver.get(url)
    wait_for_table(driver)


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


def main(headless: bool = None, start_page: int = 1, end_page: int | None = None) -> None:
    # Ensure output directory exists
    if USE_CONFIG:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
    
    driver = make_driver(headless=headless)
    try:
        print("Opening site, selecting region, and searching...")
        select_region_and_search(driver)

        last_page = get_last_page(driver)
        if end_page is None:
            end_page = last_page
        end_page = min(end_page, last_page)

        print(f"Detected last page: {last_page}. Will scrape pages {start_page}..{end_page}.")
        print(f"[PROGRESS] Scraping pages: 0/{end_page} (0%)", flush=True)

        seen_ids = load_existing_ids(OUT_CSV)
        print(f"Resume support: already have {len(seen_ids)} item_ids in {OUT_CSV}")

        for p in range(start_page, end_page + 1):
            # Progress reporting for GUI
            percent = round((p / end_page) * 100, 1) if end_page > 0 else 0
            print(f"\n--- Page {p}/{end_page} ---")
            print(f"[PROGRESS] Scraping pages: {p}/{end_page} ({percent}%)", flush=True)
            
            go_to_page(driver, p)

            page_rows = extract_rows_from_current_page(driver, p)
            # De-dup by item_id
            new_rows = [r for r in page_rows if r.item_id and r.item_id not in seen_ids]

            print(f"Found rows: {len(page_rows)} | New rows: {len(new_rows)}")

            if new_rows:
                append_rows(OUT_CSV, new_rows)
                for r in new_rows:
                    seen_ids.add(r.item_id)

            time.sleep(SLEEP_BETWEEN_PAGES)

        print(f"\nDone. Output: {OUT_CSV}")
        print(f"[PROGRESS] Scraping pages: {end_page}/{end_page} (100%) - Completed", flush=True)

    finally:
        # Unregister driver from Chrome manager
        if unregister_chrome_driver:
            unregister_chrome_driver(driver)
        driver.quit()


if __name__ == "__main__":
    # Set headless=False if you want to watch it run.
    # You can also limit pages for testing:
    #   main(headless=False, start_page=1, end_page=3)
    main(headless=None, start_page=1, end_page=None)
