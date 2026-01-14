# 01_belarus_rceth_extract_selenium_translate_paginate_v2.py
# Python 3.10+
#
# pip install selenium pandas beautifulsoup4 lxml webdriver-manager deep-translator

import sys
import os
from pathlib import Path

# Add repo root to path for imports
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Ensure UTF-8 stdout to avoid Windows console encode errors
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# Add scripts/Belarus to path for config_loader
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

# Try to load config, fallback to defaults if not available
try:
    from config_loader import load_env_file, getenv, getenv_bool, get_input_dir, get_output_dir
    load_env_file()
    USE_CONFIG = True
except ImportError:
    USE_CONFIG = False
    # Fallback functions if config_loader not available
    def getenv(key, default=""):
        return os.getenv(key, default)
    def getenv_bool(key, default=False):
        val = os.getenv(key, str(default))
        return str(val).lower() in ("true", "1", "yes", "on")
    def get_input_dir():
        return Path(__file__).parent
    def get_output_dir():
        return Path(__file__).parent

import re
import time
import random
import json
from datetime import datetime, timezone
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException

from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService

try:
    from core.chrome_pid_tracker import get_chrome_pids_from_driver, save_chrome_pids
except ImportError:
    get_chrome_pids_from_driver = None
    save_chrome_pids = None


BASE = "https://www.rceth.by"
START_URL = "https://www.rceth.by/Refbank/reestr_lekarstvennih_sredstv"

# Use config if available, otherwise fallback to hardcoded paths
if USE_CONFIG:
    GENERIC_LIST_CSV = get_input_dir() / getenv("SCRIPT_01_GENERIC_LIST_CSV", "Generic Name.csv")
    OUT_DIR = get_output_dir()
    OUT_RAW = OUT_DIR / getenv("SCRIPT_01_OUTPUT_CSV", "belarus_rceth_raw.csv")
    PROGRESS_FILE = OUT_DIR / getenv("SCRIPT_01_PROGRESS_JSON", "belarus_rceth_progress.json")
else:
    # Fallback to hardcoded paths
    GENERIC_LIST_CSV = Path(__file__).parent / "Generic Name.csv"
    OUT_RAW = Path(__file__).parent / "belarus_rceth_raw.csv"
    PROGRESS_FILE = Path(__file__).parent / "belarus_rceth_progress.json"

# --- exact selectors from your page ---
INN_INPUT_ID = "FProps_1__CritElems_0__Val"
SEARCH_XPATH = "//input[@type='submit' and (normalize-space(@value)='Поиск' or normalize-space(@value)='Search')]"
PAGE_SIZE_100_XPATH = "//*[self::a or self::button][normalize-space()='100']"

# pagination: <a class="rec-num" propval="2">2</a>
PAGINATION_LINKS_CSS = "a.rec-num[propval]"

USD_EQ_RE = re.compile(r"Equivalent price on registration date:\s*([0-9]+(?:[.,][0-9]+)?)\s*USD", re.I)
PRICE_CELL_RE = re.compile(r"([0-9]+(?:[.,][0-9]+)?)\s*([A-Z]{3})", re.I)


# --------------------- Translation ---------------------
TRANSLATE_TO_EN = getenv_bool("SCRIPT_01_TRANSLATE_TO_EN", True) if USE_CONFIG else True

# helpful fixed phrases (offline-safe)
RU_EN_MAP = {
    "Республика Беларусь": "Republic of Belarus",
    "г.": "city ",
    "ул.": "st. ",
    "обл.": "region",
    "Минская": "Minsk",
    "Витебская": "Vitebsk",
    "Гомельская": "Gomel",
    "Брестская": "Brest",
    "Гродненская": "Grodno",
    "Могилевская": "Mogilev",
    "СООО": "LLC",
    "РУП": "RUE",
    "ОАО": "JSC",
    "ЗАО": "CJSC",
    "ООО": "LLC",
    "ИП": "IE",
}

def _has_cyrillic(s: str) -> bool:
    return bool(s) and bool(re.search(r"[А-Яа-яЁё]", s))

# translator + cache
TRANSLATOR = None
TRANSLATION_CACHE = {}

def init_translator():
    global TRANSLATOR
    if not TRANSLATE_TO_EN:
        return
    try:
        from deep_translator import GoogleTranslator
        TRANSLATOR = GoogleTranslator(source="auto", target="en")
    except Exception:
        TRANSLATOR = None
        print("[WARN] deep-translator not installed/working. Only dictionary replacement will be applied.")
        print("       To enable full RU->EN translation: pip install deep-translator")

def translate_text(text: str) -> str:
    if not text or not TRANSLATE_TO_EN:
        return text

    t = text

    # offline replacements first
    for ru, en in RU_EN_MAP.items():
        t = t.replace(ru, en)

    # if still has Cyrillic, try online translator (cached)
    if _has_cyrillic(t) and TRANSLATOR is not None:
        key = t
        if key in TRANSLATION_CACHE:
            return TRANSLATION_CACHE[key]

        # rate limit (important)
        time.sleep(random.uniform(0.25, 0.6))
        try:
            out = TRANSLATOR.translate(t)
            TRANSLATION_CACHE[key] = out
            return out
        except Exception:
            return t

    return t

def translate_row_fields(row: dict) -> dict:
    # translate text-heavy fields
    for k in [
        "trade_name",
        "inn",
        "dosage_form",
        "atc_code_or_category",
        "marketing_authorization_holder",
        "producer_raw",
        "registration_certificate_number",
        "contract_currency_info_raw",
        "max_price_registration_info_raw",
        "date_of_changes_raw",
    ]:
        v = row.get(k)
        if isinstance(v, str) and v:
            row[k] = translate_text(v)

    return row


# --------------------- Utils ---------------------
def jitter_sleep(a=0.7, b=1.6):
    time.sleep(random.uniform(a, b))

def parse_price_cell(text: str):
    if not text:
        return None, None
    t = " ".join(text.split())
    m = PRICE_CELL_RE.search(t)
    if not m:
        return None, None
    return float(m.group(1).replace(",", ".")), m.group(2).upper()

def parse_import_price_usd(contract_info_text: str):
    if not contract_info_text:
        return None, None
    t = " ".join(contract_info_text.split())
    m = USD_EQ_RE.search(t)
    if m:
        return float(m.group(1).replace(",", ".")), "USD"
    return None, None


# --------------------- Selenium ---------------------
def load_progress(progress_path: Path) -> set:
    if not progress_path.exists():
        return set()
    try:
        with open(progress_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data.get("completed_inns", []))
    except Exception:
        return set()

def save_progress(progress_path: Path, completed_inns: set):
    try:
        payload = {
            "completed_inns": sorted(completed_inns),
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        with open(progress_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def load_existing_output(output_path: Path) -> list:
    if not output_path.exists():
        return []
    try:
        df = pd.read_csv(output_path, encoding="utf-8-sig")
        return df.to_dict(orient="records")
    except Exception:
        return []

def save_output_rows(output_path: Path, rows: list):
    df = pd.DataFrame(rows)
    if not df.empty:
        key_cols = ["registration_certificate_number", "trade_name", "dosage_form", "max_selling_price", "import_price"]
        for c in key_cols:
            if c not in df.columns:
                df[c] = ""
        df = df.drop_duplicates(subset=key_cols, keep="first")
    df.to_csv(str(output_path), index=False, encoding="utf-8-sig")

def build_driver(show_browser=None):
    """Build Chrome driver with config support"""
    if show_browser is None:
        # Get from config if available
        if USE_CONFIG:
            show_browser = not getenv_bool("SCRIPT_01_HEADLESS", False)
        else:
            show_browser = True
    
    opts = ChromeOptions()
    if not show_browser:
        opts.add_argument("--headless=new")
        opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1400,900")

    # Disable images/CSS for faster loads (enabled by default for Belarus)
    disable_images = getenv_bool("SCRIPT_01_DISABLE_IMAGES", True) if USE_CONFIG else True
    disable_css = getenv_bool("SCRIPT_01_DISABLE_CSS", True) if USE_CONFIG else True
    prefs = {}
    if disable_images:
        prefs["profile.managed_default_content_settings.images"] = 2
    if disable_css:
        prefs["profile.managed_default_content_settings.stylesheets"] = 2
    if prefs:
        opts.add_experimental_option("prefs", prefs)
    
    # Additional Chrome options from config
    if USE_CONFIG:
        chrome_start_max = getenv("SCRIPT_01_CHROME_START_MAXIMIZED", "")
        if chrome_start_max and show_browser:
            opts.add_argument(chrome_start_max)
        
        chrome_disable_automation = getenv("SCRIPT_01_CHROME_DISABLE_AUTOMATION", "")
        if chrome_disable_automation:
            opts.add_argument(chrome_disable_automation)

    service = ChromeService(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)

    # Track Chrome PIDs so the GUI can report active instances
    if get_chrome_pids_from_driver and save_chrome_pids:
        try:
            pids = get_chrome_pids_from_driver(driver)
            if pids:
                save_chrome_pids("Belarus", _repo_root, pids)
        except Exception:
            pass
    return driver

def safe_click(driver, el):
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    jitter_sleep(0.2, 0.6)
    try:
        el.click()
        return True
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            return False

def wait_results_table_loaded(driver, timeout=240):
    """
    Wait for results table existence + at least 1 row.
    """
    wait = WebDriverWait(driver, timeout)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))
    wait.until(lambda d: len(d.find_elements(By.CSS_SELECTOR, "table tbody tr")) >= 1)

def wait_table_stable(driver, timeout=240, stable_seconds=1.5):
    """
    Extra-stable wait: row count stops changing for stable_seconds.
    This is what you asked: wait after each extraction/navigation.
    """
    wait_results_table_loaded(driver, timeout=timeout)

    end = time.time() + timeout
    last_count = -1
    last_change = time.time()

    while time.time() < end:
        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        count = len(rows)

        if count != last_count:
            last_count = count
            last_change = time.time()

        # stable long enough
        if time.time() - last_change >= stable_seconds:
            return True

        time.sleep(0.25)

    return False


# --------------------- HTML parsing ---------------------
def find_results_table_in_soup(soup: BeautifulSoup):
    tables = soup.find_all("table")
    if not tables:
        return None

    key_terms = ["Trade name", "INN", "Dosage form", "Maximum selling price",
                 "Торгов", "МНН", "Лекарственная", "Предельная"]

    best = None
    best_score = -1
    for t in tables:
        thead = t.find("thead")
        if not thead:
            continue
        headers = [th.get_text(" ", strip=True) for th in thead.find_all("th")]
        joined = " | ".join(headers).lower()
        score = sum(1 for term in key_terms if term.lower() in joined)
        if score > best_score:
            best_score = score
            best = t
    return best

def extract_rows_from_html(html: str, search_inn: str, page_no: int, page_url: str):
    soup = BeautifulSoup(html, "lxml")
    table = find_results_table_in_soup(soup)
    if not table:
        return []

    tbody = table.find("tbody") or table
    trs = tbody.find_all("tr")

    out = []
    for tr in trs:
        tds = tr.find_all("td")
        if not tds:
            continue

        cell_texts = [td.get_text("\n", strip=True) for td in tds]
        def safe(i): return cell_texts[i] if i < len(cell_texts) else ""

        trade_name = safe(1)
        inn = safe(2)
        dosage_form = safe(3)
        atc_cat = safe(4)
        mah = safe(5)
        producer = safe(6)
        reg_cert = safe(7)

        max_price_val, max_price_ccy = parse_price_cell(safe(8))
        import_price_usd, import_ccy = parse_import_price_usd(safe(9))
        reg_info = safe(10)
        date_changes = safe(11)

        details_url = ""
        a = tr.find("a", href=True)
        if a and "/Refbank/reestr_drugregpricenew/details/" in a["href"]:
            details_url = urljoin(BASE, a["href"])

        row = {
            "search_inn_used": search_inn,
            "page_no": page_no,
            "page_url": page_url,

            "trade_name": trade_name,
            "inn": inn,
            "dosage_form": dosage_form,
            "atc_code_or_category": atc_cat,
            "marketing_authorization_holder": mah,
            "producer_raw": producer,
            "registration_certificate_number": reg_cert,

            "max_selling_price": max_price_val,
            "max_selling_price_currency": max_price_ccy,

            "import_price": import_price_usd,
            "import_price_currency": import_ccy,

            "contract_currency_info_raw": safe(9),
            "max_price_registration_info_raw": reg_info,
            "date_of_changes_raw": date_changes,
            "details_url": details_url,

            "scraped_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        out.append(row)

    return out


# --------------------- Pagination ---------------------
def get_max_page_from_dom(driver):
    nums = []
    for el in driver.find_elements(By.CSS_SELECTOR, PAGINATION_LINKS_CSS):
        pv = el.get_attribute("propval")
        if pv and pv.isdigit():
            nums.append(int(pv))
    return max(nums) if nums else 1

def go_to_page(driver, page_number: int):
    sel = f"a.rec-num[propval='{page_number}']"
    links = driver.find_elements(By.CSS_SELECTOR, sel)
    if not links:
        return False

    old_html = driver.page_source
    for el in links:
        if el.is_displayed() and el.is_enabled():
            safe_click(driver, el)
            WebDriverWait(driver, 240).until(lambda d: d.page_source != old_html)
            wait_table_stable(driver, timeout=240, stable_seconds=1.5)
            return True
    return False

def click_page_size_100(driver):
    els = driver.find_elements(By.XPATH, PAGE_SIZE_100_XPATH)
    if not els:
        return False

    old_html = driver.page_source
    for el in els:
        if el.is_displayed() and el.is_enabled():
            safe_click(driver, el)
            WebDriverWait(driver, 240).until(lambda d: d.page_source != old_html)
            wait_table_stable(driver, timeout=240, stable_seconds=1.5)
            return True
    return False


# --------------------- Search flow ---------------------
def run_search(driver, inn_term: str):
    driver.get(START_URL)

    wait = WebDriverWait(driver, 240)
    inn_input = wait.until(EC.visibility_of_element_located((By.ID, INN_INPUT_ID)))
    wait.until(lambda d: inn_input.is_enabled())

    # type INN
    inn_input.click()
    inn_input.send_keys(Keys.CONTROL, "a")
    inn_input.send_keys(Keys.BACKSPACE)
    inn_input.send_keys(inn_term)

    jitter_sleep(0.4, 1.0)

    # click search
    search_btn = wait.until(EC.element_to_be_clickable((By.XPATH, SEARCH_XPATH)))
    safe_click(driver, search_btn)

    # wait results stable
    wait_table_stable(driver, timeout=240, stable_seconds=1.5)

    # set 100/page and wait stable again
    click_page_size_100(driver)

def scrape_for_inn(driver, inn_term: str):
    run_search(driver, inn_term)

    all_rows = []

    # page 1
    wait_table_stable(driver, timeout=240, stable_seconds=1.5)
    html = driver.page_source
    all_rows.extend(extract_rows_from_html(html, inn_term, 1, driver.current_url))
    jitter_sleep(0.8, 1.6)  # ✅ wait after extraction (your request)

    max_page = get_max_page_from_dom(driver)

    for p in range(2, max_page + 1):
        ok = go_to_page(driver, p)
        if not ok:
            break

        html = driver.page_source
        all_rows.extend(extract_rows_from_html(html, inn_term, p, driver.current_url))
        jitter_sleep(0.8, 1.6)  # ✅ wait after each extraction

    return all_rows


def main():
    init_translator()

    # Convert to string path if Path object
    csv_path = str(GENERIC_LIST_CSV) if isinstance(GENERIC_LIST_CSV, Path) else GENERIC_LIST_CSV
    inn_df = pd.read_csv(csv_path)
    if "Generic Name" in inn_df.columns:
        inns = inn_df["Generic Name"].dropna().astype(str).str.strip().tolist()
    else:
        inns = inn_df.iloc[:, 0].dropna().astype(str).str.strip().tolist()

    completed_inns = load_progress(PROGRESS_FILE)
    total_inns = len(inns)
    print(f"[INFO] Processing {total_inns} INN(s).")
    print(f"[PROGRESS] Processing INNs: 0/{total_inns} (0%)", flush=True)

    # build_driver will use config if available (None = use config)
    driver = build_driver(show_browser=None)

    all_out = load_existing_output(OUT_RAW)
    try:
        for idx, inn_term in enumerate(inns, start=1):
            if not inn_term:
                continue
            if inn_term in completed_inns:
                continue

            # Progress reporting for GUI
            percent = round((idx / total_inns) * 100, 1) if total_inns > 0 else 0
            print(f"[{idx}/{total_inns}] INN: {inn_term}")
            print(f"[PROGRESS] Processing INNs: {idx}/{total_inns} ({percent}%) - Current: {inn_term}", flush=True)
            
            try:
                rows = scrape_for_inn(driver, inn_term)
                print(f"  -> rows: {len(rows)}")

                # translate BEFORE saving
                for r in rows:
                    all_out.append(translate_row_fields(r))
                completed_inns.add(inn_term)
                save_output_rows(OUT_RAW, all_out)
                save_progress(PROGRESS_FILE, completed_inns)

            except WebDriverException as e:
                print(f"  !! WebDriver error for {inn_term}: {e}")
                print("  !! Restarting Chrome driver and retrying once...")
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = build_driver(show_browser=None)
                try:
                    rows = scrape_for_inn(driver, inn_term)
                    print(f"  -> rows: {len(rows)} (after retry)")
                    for r in rows:
                        all_out.append(translate_row_fields(r))
                    completed_inns.add(inn_term)
                    save_output_rows(OUT_RAW, all_out)
                    save_progress(PROGRESS_FILE, completed_inns)
                except Exception as retry_err:
                    print(f"  !! Retry failed for {inn_term}: {retry_err}")

            except Exception as e:
                print(f"  !! Failed {inn_term}: {e}")

            jitter_sleep(1.2, 2.4)

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    save_output_rows(OUT_RAW, all_out)
    print(f"\nSaved: {OUT_RAW} (rows={len(all_out)})")
    
    # Final progress update
    if total_inns > 0:
        print(f"[PROGRESS] Processing INNs: {total_inns}/{total_inns} (100%) - Completed", flush=True)


if __name__ == "__main__":
    main()
