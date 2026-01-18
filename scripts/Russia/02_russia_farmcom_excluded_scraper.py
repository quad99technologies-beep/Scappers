#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Russia Farmcom Excluded Drugs List Scraper

Scrapes the excluded drugs list from the Russian VED registry
at farmcom.info/site/reestr?vw=excl for a specified region.

Features:
- Resume support: Saves progress after each page, resumes from last completed page
- Deduplication: Skips already scraped item_ids
- Configurable: All settings via Russia.env.json

Author: Enterprise PDF Processing Pipeline
License: Proprietary
"""

import os
import re
import csv
import sys
import time
import json
import hashlib
from dataclasses import dataclass
from typing import Optional
from datetime import datetime, timezone
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
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, WebDriverException

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

# Configuration from env or defaults - EXCLUDED LIST URL
BASE_URL = getenv("SCRIPT_02_BASE_URL", "http://farmcom.info/site/reestr?vw=excl") if USE_CONFIG else "http://farmcom.info/site/reestr?vw=excl"

# Moscow region per your annotation (reg_id value=50)
REGION_VALUE = getenv("SCRIPT_02_REGION_VALUE", "50") if USE_CONFIG else "50"
# Some excluded list pages no longer require region selection
SKIP_REGION_SELECT = getenv_bool("SCRIPT_02_SKIP_REGION_SELECT", True) if USE_CONFIG else True

# Output
if USE_CONFIG:
    OUT_DIR = get_output_dir()
    OUT_CSV = OUT_DIR / getenv("SCRIPT_02_OUTPUT_CSV", "russia_farmcom_excluded_list.csv")
    PROGRESS_FILE = OUT_DIR / "russia_excluded_scraper_progress.json"
else:
    OUT_CSV = Path(__file__).parent / "russia_farmcom_excluded_list.csv"
    PROGRESS_FILE = Path(__file__).parent / "russia_excluded_scraper_progress.json"

# Safety / stability - reuse SCRIPT_01 settings or use SCRIPT_02 specific
PAGE_LOAD_TIMEOUT = getenv_int("SCRIPT_02_PAGE_LOAD_TIMEOUT", getenv_int("SCRIPT_01_PAGE_LOAD_TIMEOUT", 60)) if USE_CONFIG else 60
WAIT_TIMEOUT = getenv_int("SCRIPT_02_WAIT_TIMEOUT", getenv_int("SCRIPT_01_WAIT_TIMEOUT", 30)) if USE_CONFIG else 30
CLICK_RETRY = getenv_int("SCRIPT_02_CLICK_RETRY", getenv_int("SCRIPT_01_CLICK_RETRY", 3)) if USE_CONFIG else 3
SLEEP_BETWEEN_PAGES = getenv_float("SCRIPT_02_SLEEP_BETWEEN_PAGES", getenv_float("SCRIPT_01_SLEEP_BETWEEN_PAGES", 0.3)) if USE_CONFIG else 0.3
NAV_RETRIES = getenv_int("SCRIPT_02_NAV_RETRIES", getenv_int("SCRIPT_01_NAV_RETRIES", 3)) if USE_CONFIG else 3
NAV_RETRY_SLEEP = getenv_float("SCRIPT_02_NAV_RETRY_SLEEP", getenv_float("SCRIPT_01_NAV_RETRY_SLEEP", 5.0)) if USE_CONFIG else 5.0
NAV_RESTART_DRIVER = getenv_bool("SCRIPT_02_NAV_RESTART_DRIVER", getenv_bool("SCRIPT_01_NAV_RESTART_DRIVER", True)) if USE_CONFIG else True

# EAN fetching - disabled by default as it's very slow
FETCH_EAN = getenv_bool("SCRIPT_02_FETCH_EAN", False) if USE_CONFIG else False
EAN_POPUP_TIMEOUT = getenv_int("SCRIPT_02_EAN_POPUP_TIMEOUT", 3) if USE_CONFIG else 3


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
        # Get from config if available - use SCRIPT_01 headless setting
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


def stop_page_load(driver: webdriver.Chrome) -> None:
    try:
        driver.execute_script("window.stop();")
    except Exception:
        pass


def shutdown_driver(driver: webdriver.Chrome) -> None:
    if unregister_chrome_driver:
        try:
            unregister_chrome_driver(driver)
        except Exception:
            pass
    try:
        driver.quit()
    except Exception:
        pass


def restart_driver(driver: webdriver.Chrome, headless: bool | None = None) -> webdriver.Chrome:
    shutdown_driver(driver)
    return make_driver(headless=headless)


def navigate_with_retries(
    driver: webdriver.Chrome,
    url: str,
    wait_fn,
    label: str,
    headless: bool | None = None,
    on_restart=None,
) -> webdriver.Chrome:
    last_exc = None
    for attempt in range(1, NAV_RETRIES + 1):
        try:
            driver.get(url)
            wait_fn(driver)
            return driver
        except (TimeoutException, WebDriverException) as exc:
            last_exc = exc
            stop_page_load(driver)
            try:
                wait_fn(driver)
                return driver
            except Exception:
                pass
            if attempt < NAV_RETRIES:
                print(
                    f"  [WARN] {label} load failed (attempt {attempt}/{NAV_RETRIES}). Retrying in {NAV_RETRY_SLEEP}s...",
                    flush=True,
                )
                time.sleep(NAV_RETRY_SLEEP)

    if NAV_RESTART_DRIVER:
        print(f"  [WARN] {label} load failed; restarting Chrome session.", flush=True)
        driver = restart_driver(driver, headless=headless)
        if on_restart:
            driver = on_restart(driver) or driver
        for attempt in range(1, NAV_RETRIES + 1):
            try:
                driver.get(url)
                wait_fn(driver)
                return driver
            except (TimeoutException, WebDriverException) as exc:
                last_exc = exc
                stop_page_load(driver)
                try:
                    wait_fn(driver)
                    return driver
                except Exception:
                    pass
                if attempt < NAV_RETRIES:
                    print(
                        f"  [WARN] {label} load failed after restart (attempt {attempt}/{NAV_RETRIES}). Retrying in {NAV_RETRY_SLEEP}s...",
                        flush=True,
                    )
                    time.sleep(NAV_RETRY_SLEEP)

    if last_exc:
        raise last_exc
    raise RuntimeError(f"{label} load failed")

def _wait_document_ready(driver: webdriver.Chrome, timeout: int) -> None:
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
    except Exception:
        pass


def _find_visible_in_context(driver: webdriver.Chrome, by: By, value: str):
    try:
        elements = driver.find_elements(by, value)
    except Exception:
        return None
    for el in elements:
        try:
            if el.is_displayed() and el.is_enabled():
                return el
        except Exception:
            return el
    return None


def _find_element_in_frames(driver: webdriver.Chrome, by: By, value: str):
    try:
        driver.switch_to.default_content()
    except Exception:
        pass

    el = _find_visible_in_context(driver, by, value)
    if el:
        return el

    frames = []
    try:
        frames = driver.find_elements(By.TAG_NAME, "iframe")
        frames += driver.find_elements(By.TAG_NAME, "frame")
    except Exception:
        frames = []

    for frame in frames:
        try:
            driver.switch_to.default_content()
            driver.switch_to.frame(frame)
            el = _find_visible_in_context(driver, by, value)
            if el:
                return el
        except Exception:
            continue

    try:
        driver.switch_to.default_content()
    except Exception:
        pass
    return None


def _click_advanced_search(driver: webdriver.Chrome) -> bool:
    xpaths = [
        "//a[contains(normalize-space(),'Advanced queries') or contains(normalize-space(),'Advanced search')]",
        "//button[contains(normalize-space(),'Advanced queries') or contains(normalize-space(),'Advanced search')]",
        "//*[contains(@class,'advanced') and (self::a or self::button)]",
        "//*[@data-toggle='collapse' and (contains(@href,'adv') or contains(@data-target,'adv'))]",
        "//a[contains(normalize-space(),'\u0420\u0430\u0441\u0448\u0438\u0440') or contains(normalize-space(),'\u0414\u043e\u043f\u043e\u043b\u043d')]",
        "//button[contains(normalize-space(),'\u0420\u0430\u0441\u0448\u0438\u0440') or contains(normalize-space(),'\u0414\u043e\u043f\u043e\u043b\u043d')]",
    ]

    for xp in xpaths:
        el = _find_element_in_frames(driver, By.XPATH, xp)
        if not el:
            continue
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        except Exception:
            pass
        try:
            el.click()
        except Exception:
            try:
                driver.execute_script("arguments[0].click();", el)
            except Exception:
                continue
        time.sleep(0.5)
        return True
    return False


def _find_region_select(driver: webdriver.Chrome) -> Optional[webdriver.remote.webelement.WebElement]:
    locators = [
        (By.ID, "reg_id"),
        (By.NAME, "reg_id"),
        (By.CSS_SELECTOR, "select#reg_id"),
        (By.CSS_SELECTOR, "select[name='reg_id']"),
        (By.CSS_SELECTOR, "select[name*='reg']"),
    ]
    end_time = time.time() + WAIT_TIMEOUT
    while time.time() < end_time:
        for by, value in locators:
            el = _find_element_in_frames(driver, by, value)
            if el:
                return el
        time.sleep(0.5)
    return None


def _find_search_button(driver: webdriver.Chrome) -> Optional[webdriver.remote.webelement.WebElement]:
    locators = [
        (By.ID, "btn_submit"),
        (By.CSS_SELECTOR, "input#btn_submit"),
        (By.CSS_SELECTOR, "input[type='submit']"),
        (By.XPATH, "//button[contains(normalize-space(),'Find') or contains(normalize-space(),'Search')]"),
        (By.XPATH, "//input[@type='submit' and (contains(@value,'Find') or contains(@value,'Search'))]"),
        (By.XPATH, "//button[contains(normalize-space(),'\u041d\u0430\u0439\u0442\u0438') or contains(normalize-space(),'\u041f\u043e\u0438\u0441\u043a')]"),
        (By.XPATH, "//input[@type='submit' and (contains(@value,'\u041d\u0430\u0439\u0442\u0438') or contains(@value,'\u041f\u043e\u0438\u0441\u043a'))]"),
    ]
    for by, value in locators:
        el = _find_element_in_frames(driver, by, value)
        if el:
            return el
    return None


def select_region_and_search(
    driver: webdriver.Chrome, headless: bool | None = None
) -> webdriver.Chrome:
    """Navigate to excluded list and select region."""
    last_exc = None
    for attempt in range(1, NAV_RETRIES + 1):
        try:
            driver.get(BASE_URL)
            _wait_document_ready(driver, WAIT_TIMEOUT)

            if SKIP_REGION_SELECT:
                try:
                    wait_for_table(driver)
                    return driver
                except Exception:
                    pass

            region_select = _find_region_select(driver)
            if not region_select:
                _click_advanced_search(driver)
                region_select = _find_region_select(driver)
            if not region_select:
                try:
                    wait_for_table(driver)
                    return driver
                except Exception:
                    raise TimeoutException(
                        "Region select (reg_id) not found after waiting and advanced search toggle."
                    )

            Select(region_select).select_by_value(REGION_VALUE)

            search_button = _find_search_button(driver)
            if not search_button:
                raise TimeoutException("Search button not found after selecting region.")
            search_button.click()

            wait_for_table(driver)
            return driver
        except (TimeoutException, WebDriverException) as exc:
            last_exc = exc
            stop_page_load(driver)
            if attempt < NAV_RETRIES:
                print(
                    f"  [WARN] Region selection failed (attempt {attempt}/{NAV_RETRIES}). Retrying in {NAV_RETRY_SLEEP}s...",
                    flush=True,
                )
                time.sleep(NAV_RETRY_SLEEP)

    if NAV_RESTART_DRIVER:
        print("  [WARN] Region selection failed; restarting Chrome session.", flush=True)
        driver = restart_driver(driver, headless=headless)
        for attempt in range(1, NAV_RETRIES + 1):
            try:
                driver.get(BASE_URL)
                _wait_document_ready(driver, WAIT_TIMEOUT)

                if SKIP_REGION_SELECT:
                    try:
                        wait_for_table(driver)
                        return driver
                    except Exception:
                        pass

                region_select = _find_region_select(driver)
                if not region_select:
                    _click_advanced_search(driver)
                    region_select = _find_region_select(driver)
                if not region_select:
                    try:
                        wait_for_table(driver)
                        return driver
                    except Exception:
                        raise TimeoutException(
                            "Region select (reg_id) not found after waiting and advanced search toggle."
                        )

                Select(region_select).select_by_value(REGION_VALUE)

                search_button = _find_search_button(driver)
                if not search_button:
                    raise TimeoutException("Search button not found after selecting region.")
                search_button.click()

                wait_for_table(driver)
                return driver
            except (TimeoutException, WebDriverException) as exc:
                last_exc = exc
                stop_page_load(driver)
                if attempt < NAV_RETRIES:
                    print(
                        f"  [WARN] Region selection failed after restart (attempt {attempt}/{NAV_RETRIES}). Retrying in {NAV_RETRY_SLEEP}s...",
                        flush=True,
                    )
                    time.sleep(NAV_RETRY_SLEEP)

    if last_exc:
        raise last_exc
    raise RuntimeError("Region selection failed")


def get_last_page(driver: webdriver.Chrome) -> int:
    """
    Reads the last page number from the pagination block.
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
    if not linkhref:
        return ""
    qs = parse_qs(urlparse("http://x/?" + linkhref.split("?", 1)[-1]).query)
    return (qs.get("item_id", [""]) or [""])[0]


def extract_price_and_date(cell_text: str) -> tuple[str, str]:
    """Extract price and date from cell text."""
    lines = [ln.strip() for ln in (cell_text or "").splitlines() if ln.strip()]
    if not lines:
        return "", ""
    price = lines[0].strip()
    date_text = " ".join(lines[1:]).strip() if len(lines) > 1 else ""
    return price, date_text


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
    base = "|".join(p.strip().lower() for p in parts if p is not None)
    if not base:
        return ""
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def safe_click(elem):
    for _ in range(CLICK_RETRY):
        try:
            elem.click()
            return
        except StaleElementReferenceException:
            time.sleep(0.2)
    elem.click()


def fetch_ean_by_clicking_barcode(driver: webdriver.Chrome, row_tr) -> str:
    """Fetch EAN by clicking barcode link (disabled by default for speed)."""
    if not FETCH_EAN:
        return ""
    
    try:
        tds = row_tr.find_elements(By.CSS_SELECTOR, "td")
        if len(tds) < 7:
            return ""

        release_td = tds[5]
        barcode_links = release_td.find_elements(By.CSS_SELECTOR, "a.info")
        if not barcode_links:
            return ""

        safe_click(barcode_links[0])

        pop = WebDriverWait(driver, EAN_POPUP_TIMEOUT).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "div#pop-up"))
        )

        txt = (pop.text or "").strip()
        driver.execute_script("arguments[0].style.display='none';", pop)

        m = re.search(r"\b(\d{8,18})\b", txt.replace(" ", ""))
        return m.group(1) if m else txt
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
            bullet_imgs = tr.find_elements(By.CSS_SELECTOR, "img.bullet[linkhref]")
            tds = tr.find_elements(By.CSS_SELECTOR, "td")

            if bullet_imgs and len(tds) >= 7:
                linkhref = bullet_imgs[0].get_attribute("linkhref") or ""
                item_id = parse_item_id_from_linkhref(linkhref)

                tn = tds[2].text.strip()
                inn = tds[3].text.strip()
                manufacturer_country = tds[4].text.strip()

                release_form_full = tds[5].text.strip()
                release_form = re.sub(r"\bBarcode\b\s*$", "", release_form_full).strip()

                price, date_text = extract_price_and_date(tds[6].text)
                ean = fetch_ean_by_clicking_barcode(driver, tr)
            elif len(tds) >= 6:
                item_id = ""
                tn = tds[0].text.strip()
                inn = tds[1].text.strip()
                manufacturer_country = tds[2].text.strip()
                release_form_full = tds[3].text.strip()
                release_form, ean = extract_ean_and_release_form(release_form_full)
                price, date_text = extract_price_and_date(tds[4].text)
                exclusion_date = tds[5].text.strip()
                if exclusion_date:
                    date_text = exclusion_date
            else:
                continue

            if not item_id:
                item_id = make_row_id(tn, inn, manufacturer_country, release_form, ean, price, date_text)

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
            continue

    return rows


def go_to_page(driver: webdriver.Chrome, page_num: int, headless: bool | None = None) -> webdriver.Chrome:
    """Navigate to specific page - maintains vw=excl parameter."""
    url = f"http://farmcom.info/site/reestr?vw=excl&page={page_num}"
    return navigate_with_retries(
        driver,
        url,
        wait_for_table,
        f"page {page_num}",
        headless=headless,
        on_restart=lambda d: select_region_and_search(d, headless=headless),
    )


# --------------------- Progress/Resume Support ---------------------
def load_progress() -> dict:
    """Load progress from JSON file for resume support."""
    if not PROGRESS_FILE.exists():
        return {"last_completed_page": 0, "total_pages": 0, "total_rows": 0}
    try:
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    except Exception:
        return {"last_completed_page": 0, "total_pages": 0, "total_rows": 0}


def save_progress(last_page: int, total_pages: int, total_rows: int):
    """Save progress to JSON file after each page."""
    try:
        payload = {
            "last_completed_page": last_page,
            "total_pages": total_pages,
            "total_rows": total_rows,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "region": REGION_VALUE,
            "output_csv": str(OUT_CSV),
            "source": "excluded_list"
        }
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"  Warning: Could not save progress: {e}")


def clear_progress():
    """Clear progress file (for fresh start)."""
    if PROGRESS_FILE.exists():
        try:
            PROGRESS_FILE.unlink()
        except Exception:
            pass


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


def main(headless: bool = None, start_page: int = None, end_page: int | None = None, fresh: bool = False) -> None:
    """
    Main scraper function with resume support for EXCLUDED drugs list.
    
    Args:
        headless: Run Chrome in headless mode (None = use config)
        start_page: Starting page number (None = resume from last or 1)
        end_page: Ending page number (None = scrape all pages)
        fresh: If True, start from page 1 ignoring previous progress
    """
    print()
    print("=" * 80)
    print("RUSSIA FARMCOM - EXCLUDED DRUGS LIST SCRAPER")
    print(f"Source: {BASE_URL}")
    print("=" * 80)
    print()
    
    # Ensure output directory exists
    if USE_CONFIG:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Load progress for resume support
    progress = load_progress()
    
    # Determine start page
    if fresh:
        clear_progress()
        actual_start = 1
        print("Starting fresh run (progress cleared)")
    elif start_page is not None:
        actual_start = start_page
        print(f"Starting from specified page: {actual_start}")
    elif progress["last_completed_page"] > 0:
        actual_start = progress["last_completed_page"] + 1
        print(f"Resuming from page {actual_start} (last completed: {progress['last_completed_page']})")
    else:
        actual_start = 1
        print("Starting fresh run (no previous progress found)")
    
    driver = make_driver(headless=headless)
    try:
        print("Opening excluded list, selecting region, and searching...")
        driver = select_region_and_search(driver, headless=headless)

        last_page = get_last_page(driver)
        if end_page is None:
            end_page = last_page
        end_page = min(end_page, last_page)
        
        # Check if already completed
        if actual_start > end_page:
            print(f"All pages already scraped (last page: {end_page}, last completed: {progress['last_completed_page']})")
            print(f"[PROGRESS] Scraping excluded list: {end_page}/{end_page} (100%) - Already completed", flush=True)
            return

        print(f"Detected last page: {last_page}. Will scrape pages {actual_start}..{end_page}.")
        
        # Calculate initial progress
        pages_done = actual_start - 1
        pages_total = end_page
        initial_percent = round((pages_done / pages_total) * 100, 1) if pages_total > 0 else 0
        print(f"[PROGRESS] Scraping excluded list: {pages_done}/{pages_total} ({initial_percent}%)", flush=True)

        seen_ids = load_existing_ids(OUT_CSV)
        print(f"Resume support: already have {len(seen_ids)} item_ids in {OUT_CSV}")
        
        total_new_rows = 0

        for p in range(actual_start, end_page + 1):
            # Progress reporting for GUI
            percent = round((p / end_page) * 100, 1) if end_page > 0 else 0
            print(f"\n--- Page {p}/{end_page} ---")
            print(f"[PROGRESS] Scraping excluded list: {p}/{end_page} ({percent}%)", flush=True)
            
            driver = go_to_page(driver, p, headless=headless)

            page_rows = extract_rows_from_current_page(driver, p)
            # De-dup by item_id
            new_rows = [r for r in page_rows if r.item_id and r.item_id not in seen_ids]

            print(f"Found rows: {len(page_rows)} | New rows: {len(new_rows)}")

            if new_rows:
                append_rows(OUT_CSV, new_rows)
                for r in new_rows:
                    seen_ids.add(r.item_id)
                total_new_rows += len(new_rows)
            
            # Save progress after each page (for resume support)
            save_progress(p, end_page, len(seen_ids))

            time.sleep(SLEEP_BETWEEN_PAGES)

        print(f"\nDone. Output: {OUT_CSV}")
        print(f"Total new rows added: {total_new_rows}")
        print(f"Total rows in file: {len(seen_ids)}")
        print(f"[PROGRESS] Scraping excluded list: {end_page}/{end_page} (100%) - Completed", flush=True)
        
        # Clear progress file on successful completion
        clear_progress()

    finally:
        shutdown_driver(driver)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Russia Farmcom Excluded Drugs List Scraper")
    parser.add_argument("--fresh", action="store_true", help="Start from page 1 (ignore previous progress)")
    parser.add_argument("--start", type=int, help="Start from specific page number")
    parser.add_argument("--end", type=int, help="End at specific page number")
    parser.add_argument("--visible", action="store_true", help="Show browser window (not headless)")
    
    args = parser.parse_args()
    
    main(
        headless=False if args.visible else None,
        start_page=args.start,
        end_page=args.end,
        fresh=args.fresh
    )
