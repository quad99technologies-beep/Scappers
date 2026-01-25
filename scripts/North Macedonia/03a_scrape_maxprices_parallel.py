#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
North Macedonia Max Prices Scraper - PARALLEL VERSION

Extracts maximum pharmacy prices from lekovi.zdravstvo.gov.mk/maxprices/0
with multi-threaded page processing.

Features:
- Multi-threaded: Process multiple pages simultaneously (configurable workers)
- Row validation: Waits for 200 rows before extraction, skips incomplete pages
- Failed page tracking: Automatically retries failed pages
- Resume support: Saves progress after each page
- Thread-safe checkpointing and output

Author: Enterprise PDF Processing Pipeline
"""

import os
import re
import csv
import sys
import time
import json
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from queue import Queue, Empty

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

try:
    from config_loader import load_env_file, getenv, getenv_bool, getenv_int, getenv_float, get_output_dir
    load_env_file()
    OUTPUT_DIR = get_output_dir()
except Exception:
    OUTPUT_DIR = _script_dir
    def getenv(key: str, default: str = None) -> str:
        return os.getenv(key, default if default is not None else "")
    def getenv_bool(key: str, default: bool = False) -> bool:
        return str(getenv(key, str(default))).lower() in ("1", "true", "yes", "on")
    def getenv_int(key: str, default: int = 0) -> int:
        try:
            return int(getenv(key, str(default)))
        except (TypeError, ValueError):
            return default
    def getenv_float(key: str, default: float = 0.0) -> float:
        try:
            return float(getenv(key, str(default)))
        except (TypeError, ValueError):
            return default

from selenium import webdriver
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
)
from selenium.webdriver import ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

# ============================
# CONFIGURATION
# ============================
START_URL = "https://lekovi.zdravstvo.gov.mk/maxprices/0"

# Threading
NUM_WORKERS = getenv_int("SCRIPT_03A_NUM_WORKERS", 3)  # Number of parallel Chrome instances

# Selenium
HEADLESS = getenv_bool("SCRIPT_03A_HEADLESS", True)
DISABLE_IMAGES = getenv_bool("SCRIPT_03A_DISABLE_IMAGES", True)
DISABLE_CSS = getenv_bool("SCRIPT_03A_DISABLE_CSS", True)
ROWS_PER_PAGE = getenv("SCRIPT_03A_ROWS_PER_PAGE", "200")
PAGE_LOAD_TIMEOUT = getenv_int("SCRIPT_03A_PAGE_LOAD_TIMEOUT", 90)
WAIT_TIMEOUT = getenv_int("SCRIPT_03A_WAIT_TIMEOUT", 30)

# Row validation
ROW_COUNT_WAIT_TIMEOUT = getenv_int("SCRIPT_03A_ROW_COUNT_WAIT_TIMEOUT", 30)
MIN_ROWS_FOR_EXTRACTION = getenv_int("SCRIPT_03A_MIN_ROWS_FOR_EXTRACTION", 50)

# Translation
TRANSLATE_SRC = "mk"
TRANSLATE_DEST = "en"
TRANSLATE_OUTPUT_FALLBACK = True
TRANSLATE_ALL_FIELDS = True
TRANSLATE_RETRIES = 3
TRANSLATE_RETRY_SLEEP = 0.4

# Output
OUTPUT_CSV = getenv("SCRIPT_03A_OUTPUT_CSV", "maxprices_output.csv")
OUT_CSV = str(OUTPUT_DIR / OUTPUT_CSV)
CHECKPOINT_JSON = getenv("SCRIPT_03A_CHECKPOINT_JSON", "mk_maxprices_parallel_checkpoint.json")
CHECKPOINT_PATH = OUTPUT_DIR / CHECKPOINT_JSON

# Runtime controls
MAX_PAGES = getenv_int("SCRIPT_03A_MAX_PAGES", 0)  # 0 = unlimited
SLEEP_AFTER_ROW = getenv_float("SCRIPT_03A_SLEEP_AFTER_ROW", 0.5)
SLEEP_AFTER_MODAL_OPEN = getenv_float("SCRIPT_03A_SLEEP_AFTER_MODAL_OPEN", 0.2)
MAX_RETRIES_PER_PAGE = getenv_int("SCRIPT_03A_MAX_RETRIES_PER_PAGE", 3)

# Shared chromedriver path lock
_driver_path_lock = threading.Lock()
_cached_driver_path = None


# ============================
# TRANSLATION
# ============================
class OutputTranslatorFallback:
    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self._translator = None
        self._cache = {}
        if not enabled:
            return
        try:
            from deep_translator import GoogleTranslator
            self._translator = GoogleTranslator(source=TRANSLATE_SRC, target=TRANSLATE_DEST)
        except Exception:
            self._translator = None

    def to_en_if_needed(self, text: str) -> str:
        text = (text or "").strip()
        if not text or not self.enabled or self._translator is None:
            return text
        if not TRANSLATE_ALL_FIELDS and not re.search(r"[\u0400-\u04FF]", text):
            return text

        cached = self._cache.get(text)
        if cached is not None:
            return cached

        for attempt in range(TRANSLATE_RETRIES):
            try:
                out = (self._translator.translate(text) or text).strip()
                self._cache[text] = out
                return out
            except Exception:
                if attempt == TRANSLATE_RETRIES - 1:
                    break
                time.sleep(TRANSLATE_RETRY_SLEEP)
        return text


# ============================
# DATA CLASSES
# ============================
@dataclass
class BaseRow:
    atc: str
    local_pack: str
    generic: str
    manufacturer: str
    issuance: str
    price_main: str


# ============================
# SELENIUM HELPERS
# ============================
def _get_chromedriver_path() -> Optional[str]:
    global _cached_driver_path
    with _driver_path_lock:
        if _cached_driver_path:
            return _cached_driver_path
        try:
            _cached_driver_path = ChromeDriverManager().install()
            print(f"[ChromeDriver] Using cached driver: {_cached_driver_path}", flush=True)
            return _cached_driver_path
        except Exception as e:
            print(f"[ERROR] ChromeDriver installation failed: {e}", flush=True)
            return None


def make_driver(headless: bool) -> webdriver.Chrome:
    opts = ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1600,900")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--lang=en-US")

    prefs = {}
    if DISABLE_IMAGES:
        prefs["profile.managed_default_content_settings.images"] = 2
    if DISABLE_CSS:
        prefs["profile.managed_default_content_settings.stylesheets"] = 2
    opts.add_experimental_option("prefs", prefs)

    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    driver_path = _get_chromedriver_path()
    if not driver_path:
        raise RuntimeError("ChromeDriver unavailable")

    service = ChromeService(driver_path)
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)

    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        """
    })

    return driver


def shutdown_driver(driver: Optional[webdriver.Chrome]) -> None:
    if driver:
        try:
            driver.quit()
        except Exception:
            pass


def safe_text(elem) -> str:
    try:
        return (elem.text or "").strip()
    except Exception:
        return ""


def wait_table_ready(wait: WebDriverWait) -> None:
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table.table-bordered.table-condensed")))


def count_grid_rows(driver: webdriver.Chrome) -> int:
    try:
        rows = driver.find_elements(By.CSS_SELECTOR, "table.table.table-bordered.table-condensed tbody tr")
        return len(rows)
    except Exception:
        return 0


def wait_for_correct_row_count(driver: webdriver.Chrome, expected_rows: int, max_wait_seconds: int, is_last_page: bool = False) -> bool:
    """Wait until grid has expected row count"""
    deadline = time.time() + max_wait_seconds
    attempts = 0

    while time.time() < deadline:
        attempts += 1
        current_count = count_grid_rows(driver)

        if is_last_page:
            if 0 < current_count <= expected_rows:
                return True
        else:
            if current_count == expected_rows:
                return True

        if attempts % 5 == 0:
            print(f"    [WAIT] Grid has {current_count} rows, expecting {expected_rows}... (attempt {attempts})", flush=True)

        time.sleep(1)

    return False


def set_rows_per_page(driver: webdriver.Chrome, wait: WebDriverWait, value: str = "200") -> None:
    try:
        sel = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "select[name='rowsPerPage']")))
        old_tbody = driver.find_element(By.CSS_SELECTOR, "table.table.table-bordered.table-condensed tbody")
    except Exception:
        old_tbody = None

    try:
        sel = Select(sel)
        sel.select_by_value(value)
    except Exception:
        return

    if old_tbody:
        try:
            wait.until(EC.staleness_of(old_tbody))
        except Exception:
            pass

    wait_table_ready(wait)


def get_total_pages(driver: webdriver.Chrome) -> Optional[int]:
    try:
        elem = driver.find_element(By.CSS_SELECTOR, "span.t-total-pages")
        return int(elem.text.strip())
    except Exception:
        return None


def navigate_to_page(driver: webdriver.Chrome, wait: WebDriverWait, page_num: int) -> None:
    """Navigate to specific page using direct URL"""
    url = f"https://lekovi.zdravstvo.gov.mk/maxprices.grid.pager/{page_num}/grid_0?t:ac=0"
    driver.get(url)
    wait_table_ready(wait)
    time.sleep(0.5)


# ============================
# PARSING
# ============================
def parse_main_rows(driver: webdriver.Chrome, wait: WebDriverWait) -> List[Tuple[BaseRow, webdriver.remote.webelement.WebElement]]:
    wait_table_ready(wait)
    tbody = driver.find_element(By.CSS_SELECTOR, "table.table.table-bordered.table-condensed tbody")
    trs = tbody.find_elements(By.CSS_SELECTOR, "tr")

    out = []
    for tr in trs:
        try:
            atc = safe_text(tr.find_element(By.CSS_SELECTOR, "td.atcCode"))
            local_pack = safe_text(tr.find_element(By.CSS_SELECTOR, "td.nameFormStrengthPackage"))
            generic = safe_text(tr.find_element(By.CSS_SELECTOR, "td.genericName"))
            manufacturer = safe_text(tr.find_element(By.CSS_SELECTOR, "td.manufacturersNames"))
            issuance = safe_text(tr.find_element(By.CSS_SELECTOR, "td.modeOfIssuance"))
            price_main = safe_text(tr.find_element(By.CSS_SELECTOR, "td.maxPriceOriginator"))
            price_main = "" if price_main == "\u00a0" else price_main
            link = tr.find_element(By.CSS_SELECTOR, "td.priceHistory a.btn.btn-primary")
            out.append((BaseRow(atc, local_pack, generic, manufacturer, issuance, price_main), link))
        except NoSuchElementException:
            continue
        except StaleElementReferenceException:
            raise
    return out


def open_price_history_modal(driver: webdriver.Chrome, wait: WebDriverWait, link_el) -> None:
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", link_el)
    for _ in range(3):
        try:
            link_el.click()
            break
        except ElementClickInterceptedException:
            time.sleep(0.35)
        except StaleElementReferenceException:
            raise
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.modal.in, div.modal.fade.in")))


def close_modal(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
    try:
        driver.find_element(By.CSS_SELECTOR, "div.modal.in, div.modal.fade.in")
    except NoSuchElementException:
        return

    for css in [
        "div.modal.in a.btn[data-dismiss='modal']",
        "div.modal.in a.close",
        "div.modal.fade.in a.btn[data-dismiss='modal']",
        "div.modal.fade.in a.close",
    ]:
        try:
            btn = driver.find_element(By.CSS_SELECTOR, css)
            driver.execute_script("arguments[0].click();", btn)
            break
        except Exception:
            continue

    try:
        wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, "div.modal.in, div.modal.fade.in")))
    except TimeoutException:
        time.sleep(0.35)


def extract_price_history_rows(driver: webdriver.Chrome) -> List[Tuple[str, str]]:
    modal = driver.find_element(By.CSS_SELECTOR, "div.modal.in, div.modal.fade.in")
    body = modal.find_element(By.CSS_SELECTOR, "div.modal-body")

    rows: List[Tuple[str, str]] = []
    date_re = re.compile(r"^\s*\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\s*$")
    price_re = re.compile(r"^\s*[\d\s.,]+\s*$")

    for p_elem in body.find_elements(By.TAG_NAME, "p"):
        txt = (p_elem.text or "").strip()
        if "|" not in txt:
            continue
        parts = txt.split("|", 1)
        if len(parts) != 2:
            continue
        date_part, price_part = parts[0].strip(), parts[1].strip()

        if not date_re.match(date_part):
            continue
        if not price_re.match(price_part):
            continue

        rows.append((date_part, price_part))

    return rows


# ============================
# CHECKPOINTING
# ============================
def read_checkpoint() -> Dict:
    if not CHECKPOINT_PATH.exists():
        return {"completed_pages": [], "failed_pages": [], "total_pages": 0}

    try:
        with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"completed_pages": [], "failed_pages": [], "total_pages": 0}


def write_checkpoint(checkpoint: Dict, lock: threading.Lock) -> None:
    with lock:
        try:
            with open(CHECKPOINT_PATH, "w", encoding="utf-8") as f:
                json.dump(checkpoint, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"[ERROR] Failed to write checkpoint: {e}", flush=True)


def ensure_csv_header(path: Path, fieldnames: List[str]) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()


def make_row_key(row: Dict) -> str:
    return f"{row['WHO_ATC_Code']}|{row['Local_Pack_Description_MK']}|{row['Marketing_Company_MK']}|{row['Effective_Start_Date']}"


def load_existing_keys(path: Path) -> set:
    keys = set()
    if not path.exists():
        return keys
    try:
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = make_row_key(row)
                keys.add(key)
    except Exception:
        pass
    return keys


# ============================
# WORKER FUNCTION
# ============================
def worker_fn(
    worker_id: int,
    page_queue: Queue,
    output_path: Path,
    fieldnames: List[str],
    existing_keys: set,
    keys_lock: threading.Lock,
    out_lock: threading.Lock,
    checkpoint: Dict,
    checkpoint_lock: threading.Lock,
    progress: dict,
    progress_lock: threading.Lock,
) -> None:
    driver: Optional[webdriver.Chrome] = None
    xlate = OutputTranslatorFallback(enabled=TRANSLATE_OUTPUT_FALLBACK)

    try:
        driver = make_driver(headless=HEADLESS)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)

        print(f"[Worker {worker_id}] Started", flush=True)

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
                    # Navigate to page
                    navigate_to_page(driver, wait, page_num)

                    # Set rows per page
                    set_rows_per_page(driver, wait, ROWS_PER_PAGE)

                    # Validate row count
                    expected_rows = int(ROWS_PER_PAGE)
                    total_pages = checkpoint.get("total_pages", 0)
                    is_last_page = (total_pages > 0 and page_num == total_pages)

                    print(f"  [Worker {worker_id}] Validating row count (expecting {expected_rows})...", flush=True)

                    if not wait_for_correct_row_count(driver, expected_rows, ROW_COUNT_WAIT_TIMEOUT, is_last_page):
                        actual_count = count_grid_rows(driver)

                        if not is_last_page and actual_count < MIN_ROWS_FOR_EXTRACTION:
                            print(f"  [Worker {worker_id}] Page {page_num} has insufficient rows ({actual_count}), marking as failed", flush=True)
                            raise RuntimeError(f"Insufficient rows: {actual_count}/{expected_rows}")

                        print(f"  [Worker {worker_id}] Proceeding with {actual_count} rows", flush=True)

                    # Parse rows
                    rows = parse_main_rows(driver, wait)
                    print(f"  [Worker {worker_id}] Found {len(rows)} rows on page {page_num}", flush=True)

                    # Process each row
                    rows_written = 0
                    for idx, (base, link_el) in enumerate(rows, start=1):
                        hist: List[Tuple[str, str]] = []

                        try:
                            open_price_history_modal(driver, wait, link_el)
                            if SLEEP_AFTER_MODAL_OPEN > 0:
                                time.sleep(SLEEP_AFTER_MODAL_OPEN)

                            hist = extract_price_history_rows(driver)
                            close_modal(driver, wait)

                        except StaleElementReferenceException:
                            print(f"  [Worker {worker_id}] Stale element at row {idx}, re-parsing...", flush=True)
                            wait_table_ready(wait)
                            refreshed = parse_main_rows(driver, wait)
                            if idx - 1 < len(refreshed):
                                base, link_el = refreshed[idx - 1]
                                try:
                                    open_price_history_modal(driver, wait, link_el)
                                    hist = extract_price_history_rows(driver)
                                    close_modal(driver, wait)
                                except Exception:
                                    hist = []

                        except TimeoutException:
                            print(f"  [Worker {worker_id}] Modal timeout at row {idx}", flush=True)
                            close_modal(driver, wait)
                            hist = []

                        # Translate
                        local_en = xlate.to_en_if_needed(base.local_pack)
                        manuf_en = xlate.to_en_if_needed(base.manufacturer)
                        generic_en = xlate.to_en_if_needed(base.generic)
                        issuance_en = xlate.to_en_if_needed(base.issuance)

                        # Write rows
                        output_rows = []
                        if not hist:
                            row_data = {
                                "WHO_ATC_Code": base.atc,
                                "Local_Pack_Description_MK": base.local_pack,
                                "Local_Pack_Description_EN": local_en,
                                "Generic_Name": generic_en,
                                "Marketing_Company_MK": base.manufacturer,
                                "Marketing_Company_EN": manuf_en,
                                "Customized_Column_1": issuance_en,
                                "Pharmacy_Purchase_Price": base.price_main,
                                "Effective_Start_Date": "",
                            }
                            row_key = make_row_key(row_data)
                            if row_key not in existing_keys:
                                output_rows.append(row_data)
                                with keys_lock:
                                    existing_keys.add(row_key)

                        elif len(hist) == 1:
                            d, p = hist[0]
                            price_to_use = base.price_main if base.price_main else p
                            row_data = {
                                "WHO_ATC_Code": base.atc,
                                "Local_Pack_Description_MK": base.local_pack,
                                "Local_Pack_Description_EN": local_en,
                                "Generic_Name": generic_en,
                                "Marketing_Company_MK": base.manufacturer,
                                "Marketing_Company_EN": manuf_en,
                                "Customized_Column_1": issuance_en,
                                "Pharmacy_Purchase_Price": price_to_use,
                                "Effective_Start_Date": d,
                            }
                            row_key = make_row_key(row_data)
                            if row_key not in existing_keys:
                                output_rows.append(row_data)
                                with keys_lock:
                                    existing_keys.add(row_key)

                        else:
                            for d, p in hist:
                                row_data = {
                                    "WHO_ATC_Code": base.atc,
                                    "Local_Pack_Description_MK": base.local_pack,
                                    "Local_Pack_Description_EN": local_en,
                                    "Generic_Name": generic_en,
                                    "Marketing_Company_MK": base.manufacturer,
                                    "Marketing_Company_EN": manuf_en,
                                    "Customized_Column_1": issuance_en,
                                    "Pharmacy_Purchase_Price": p,
                                    "Effective_Start_Date": d,
                                }
                                row_key = make_row_key(row_data)
                                if row_key not in existing_keys:
                                    output_rows.append(row_data)
                                    with keys_lock:
                                        existing_keys.add(row_key)

                        # Write to CSV
                        if output_rows:
                            with out_lock:
                                with open(output_path, "a", newline="", encoding="utf-8") as f:
                                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                                    writer.writerows(output_rows)
                            rows_written += len(output_rows)

                        if SLEEP_AFTER_ROW > 0:
                            time.sleep(SLEEP_AFTER_ROW)

                    print(f"  [Worker {worker_id}] Page {page_num} completed ({rows_written} rows written)", flush=True)

                    # Mark page as completed
                    with checkpoint_lock:
                        if page_num not in checkpoint["completed_pages"]:
                            checkpoint["completed_pages"].append(page_num)
                        if page_num in checkpoint["failed_pages"]:
                            checkpoint["failed_pages"].remove(page_num)
                        write_checkpoint(checkpoint, threading.Lock())  # Internal lock handled

                    with progress_lock:
                        progress["done"] += 1
                        pct = round((progress["done"] / progress["total"]) * 100, 1)
                        print(f"[PROGRESS] {progress['done']}/{progress['total']} ({pct}%) pages completed", flush=True)

                    success = True
                    break

                except Exception as e:
                    print(f"  [Worker {worker_id}] Attempt {attempt}/{MAX_RETRIES_PER_PAGE} failed for page {page_num}: {e}", flush=True)
                    if attempt == MAX_RETRIES_PER_PAGE:
                        # Mark as failed
                        with checkpoint_lock:
                            if page_num not in checkpoint["failed_pages"]:
                                checkpoint["failed_pages"].append(page_num)
                            write_checkpoint(checkpoint, threading.Lock())
                        print(f"  [Worker {worker_id}] Page {page_num} marked as FAILED", flush=True)
                    else:
                        time.sleep(2)

            page_queue.task_done()

    finally:
        shutdown_driver(driver)
        print(f"[Worker {worker_id}] Stopped", flush=True)


# ============================
# MAIN
# ============================
def main():
    print("\n" + "="*60, flush=True)
    print("Max Prices Scraper - PARALLEL VERSION", flush=True)
    print("="*60, flush=True)

    # Setup CSV
    fieldnames = [
        "WHO_ATC_Code",
        "Local_Pack_Description_MK",
        "Local_Pack_Description_EN",
        "Generic_Name",
        "Marketing_Company_MK",
        "Marketing_Company_EN",
        "Customized_Column_1",
        "Pharmacy_Purchase_Price",
        "Effective_Start_Date",
    ]

    output_path = Path(OUT_CSV)
    ensure_csv_header(output_path, fieldnames)
    existing_keys = load_existing_keys(output_path)

    # Load checkpoint
    checkpoint = read_checkpoint()
    completed_pages = set(checkpoint.get("completed_pages", []))
    failed_pages = set(checkpoint.get("failed_pages", []))

    # Determine total pages
    print("\n[INIT] Determining total pages...", flush=True)
    driver = make_driver(headless=HEADLESS)
    wait = WebDriverWait(driver, WAIT_TIMEOUT)

    try:
        driver.get(START_URL)
        wait_table_ready(wait)
        set_rows_per_page(driver, wait, ROWS_PER_PAGE)
        total_pages = get_total_pages(driver) or 0
        checkpoint["total_pages"] = total_pages
        print(f"[INIT] Total pages: {total_pages}", flush=True)
    finally:
        shutdown_driver(driver)

    # Build page list
    all_pages = list(range(1, total_pages + 1)) if total_pages > 0 else []
    if MAX_PAGES > 0:
        all_pages = all_pages[:MAX_PAGES]

    pending_pages = [p for p in all_pages if p not in completed_pages]

    print(f"\n[STATUS] Pages completed: {len(completed_pages)}", flush=True)
    print(f"[STATUS] Pages failed: {len(failed_pages)}", flush=True)
    print(f"[STATUS] Pages pending: {len(pending_pages)}", flush=True)
    print(f"[STATUS] Workers: {NUM_WORKERS}", flush=True)

    if not pending_pages and not failed_pages:
        print("\n[COMPLETE] All pages already scraped!", flush=True)
        return

    # Add failed pages to retry
    pages_to_process = sorted(set(pending_pages + list(failed_pages)))

    print(f"\n[START] Processing {len(pages_to_process)} pages with {NUM_WORKERS} workers...\n", flush=True)

    # Setup threading
    page_queue = Queue()
    for p in pages_to_process:
        page_queue.put(p)

    keys_lock = threading.Lock()
    out_lock = threading.Lock()
    checkpoint_lock = threading.Lock()
    progress_lock = threading.Lock()

    progress = {"done": 0, "total": len(pages_to_process)}

    # Start workers
    threads = []
    for i in range(NUM_WORKERS):
        t = threading.Thread(
            target=worker_fn,
            args=(i+1, page_queue, output_path, fieldnames, existing_keys,
                  keys_lock, out_lock, checkpoint, checkpoint_lock, progress, progress_lock)
        )
        t.start()
        threads.append(t)

    # Wait for completion
    for t in threads:
        t.join()

    print("\n" + "="*60, flush=True)
    print("SCRAPING COMPLETE", flush=True)
    print("="*60, flush=True)
    print(f"Completed pages: {len(checkpoint['completed_pages'])}", flush=True)
    print(f"Failed pages: {len(checkpoint['failed_pages'])}", flush=True)
    if checkpoint['failed_pages']:
        print(f"Failed page numbers: {sorted(checkpoint['failed_pages'])}", flush=True)
    print("="*60 + "\n", flush=True)


if __name__ == "__main__":
    main()
