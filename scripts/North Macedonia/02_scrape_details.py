import os
import json
import time
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional
import threading
from queue import Queue, Empty

import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    WebDriverException,
)
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

# Add script directory to path for config_loader import
_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

try:
    from config_loader import load_env_file, getenv, getenv_bool, getenv_float, get_output_dir
    load_env_file()
    OUTPUT_DIR = get_output_dir()
except ImportError:
    OUTPUT_DIR = Path(__file__).resolve().parent
    def getenv(key: str, default: str = None) -> str:
        return os.getenv(key, default if default is not None else "")
    def getenv_bool(key: str, default: bool = False) -> bool:
        val = os.getenv(key, str(default))
        return str(val).lower() in ("true", "1", "yes", "on")
    def getenv_float(key: str, default: float = 0.0) -> float:
        try:
            return float(os.getenv(key, str(default)))
        except Exception:
            return default

try:
    from core.chrome_pid_tracker import get_chrome_pids_from_driver, save_chrome_pids, terminate_scraper_pids
except ImportError:
    get_chrome_pids_from_driver = None
    save_chrome_pids = None
    terminate_scraper_pids = None

# Translation
try:
    from googletrans import Translator  # type: ignore
    _translator = Translator()
except Exception:
    _translator = None

BASE_URL = "https://lekovi.zdravstvo.gov.mk/drugsregister/overview"
OUT_CSV = getenv("SCRIPT_02_OUTPUT_CSV", "north_macedonia_drug_register.csv")
URLS_CSV = getenv("SCRIPT_01_URLS_CSV", "north_macedonia_detail_urls.csv")
DETAIL_WORKERS = int(getenv("SCRIPT_02_DETAIL_WORKERS", "3") or "3")

_driver_path = None
_driver_path_lock = None
_repo_root = Path(__file__).resolve().parents[2]


def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def translate_mk_to_en(text: str) -> str:
    text = normalize_ws(text)
    if not text:
        return ""
    if _translator is None:
        return text
    try:
        return normalize_ws(_translator.translate(text, src="mk", dest="en").text)
    except Exception:
        return text


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
    driver.set_page_load_timeout(90)
    if get_chrome_pids_from_driver and save_chrome_pids:
        try:
            pids = get_chrome_pids_from_driver(driver)
            if pids:
                save_chrome_pids("NorthMacedonia", _repo_root, pids)
        except Exception:
            pass
    return driver


def parse_detail_page(driver: webdriver.Chrome) -> Dict[str, str]:
    WebDriverWait(driver, 40).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.row-fluid"))
    )
    rows = driver.find_elements(By.CSS_SELECTOR, "div.row-fluid")

    data = {}
    for r in rows:
        try:
            label = normalize_ws(r.find_element(By.CSS_SELECTOR, "div.span2 b").text)
            value = normalize_ws(r.find_element(By.CSS_SELECTOR, "div.span6").text)
            if label:
                data[label] = value
        except Exception:
            continue
    return data


def make_local_pack_description(formulation: str, fill_size: str, strength: str, composition: str) -> str:
    parts = [normalize_ws(formulation), normalize_ws(fill_size), normalize_ws(strength), normalize_ws(composition)]
    parts = [p for p in parts if p]
    return " ".join(parts)


def ensure_csv_has_header(path: Path, columns: List[str]) -> None:
    if not path.exists():
        pd.DataFrame([], columns=columns).to_csv(str(path), index=False, encoding="utf-8-sig")


def append_rows_to_csv(path: Path, rows: List[Dict], columns: List[str]) -> None:
    if not rows:
        return
    df = pd.DataFrame(rows)
    df = df.reindex(columns=columns)
    df.to_csv(str(path), mode="a", header=False, index=False, encoding="utf-8-sig")


def load_existing_detail_urls(path: Path) -> List[str]:
    if not path.exists():
        return []
    try:
        df = pd.read_csv(path, usecols=["detail_url"], dtype=str)
        return df["detail_url"].dropna().astype(str).tolist()
    except Exception:
        return []


def load_already_scraped_urls(path: Path) -> set:
    if not path.exists():
        return set()
    try:
        df = pd.read_csv(path, usecols=["detail_url"], dtype=str)
        return set(df["detail_url"].dropna().astype(str).tolist())
    except Exception:
        return set()


def scrape_detail_urls_worker(
    worker_id: int,
    url_queue: Queue,
    output_path: Path,
    out_columns: List[str],
    seen_urls: set,
    seen_lock: threading.Lock,
    out_lock: threading.Lock,
    headless: bool,
    sleep_between_details: float,
    progress_state: dict,
    progress_lock: threading.Lock,
    total_urls: int
) -> None:
    driver = None
    try:
        driver = build_driver(headless=headless)
        if driver is None:
            with progress_lock:
                progress_state["driver_failed"] = True
            while True:
                try:
                    _ = url_queue.get_nowait()
                except Empty:
                    break
                url_queue.task_done()
            return
        while True:
            try:
                url = url_queue.get(timeout=2)
            except Empty:
                break
            if url is None:
                url_queue.task_done()
                break

            try:
                driver.get(url)
                detail = parse_detail_page(driver)

                local_product = detail.get("??? ?? ????? (????????):", "")
                ean = detail.get("EAN ???:", "")
                generic = detail.get("????????? ???", "")
                atc = detail.get("???", "")
                formulation = detail.get("???????????? ?????", "")
                strength = detail.get("??????", "")
                packaging = detail.get("????????", "")
                composition = detail.get("??????", "")
                manufacturers = detail.get("?????????????:", "")
                eff_start = detail.get("????? ?? ???????", "")
                eff_end = detail.get("????? ?? ???????", "")
                retail_vat = detail.get("???????????? ???? ?? ???", "")
                wholesale_wo_vat = detail.get("?????????????? ???? ??? ???", "")

                local_pack_desc = make_local_pack_description(formulation, packaging, strength, composition)

                row = {
                    "Local Product Name (RAW)": local_product,
                    "Local Pack Code (RAW)": ean,
                    "Generic Name (RAW)": generic,
                    "WHO ATC Code (RAW)": atc,
                    "Formulation (RAW)": formulation,
                    "Strength Size (RAW)": strength,
                    "Fill Size (RAW)": packaging,
                    "Customized 1 - Composition (RAW)": composition,
                    "Marketing Authority / Company Name (RAW)": manufacturers,
                    "Effective Start Date (RAW)": eff_start,
                    "Effective End Date (RAW)": eff_end,
                    "Public with VAT Price (RAW)": retail_vat,
                    "Pharmacy Purchase Price (RAW)": wholesale_wo_vat,
                    "Local Pack Description (RAW)": local_pack_desc,
                    "Local Product Name (EN)": translate_mk_to_en(local_product),
                    "Local Pack Code (EN)": translate_mk_to_en(ean),
                    "Generic Name (EN)": translate_mk_to_en(generic),
                    "WHO ATC Code (EN)": translate_mk_to_en(atc),
                    "Formulation (EN)": translate_mk_to_en(formulation),
                    "Strength Size (EN)": translate_mk_to_en(strength),
                    "Fill Size (EN)": translate_mk_to_en(packaging),
                    "Customized 1 - Composition (EN)": translate_mk_to_en(composition),
                    "Marketing Authority / Company Name (EN)": translate_mk_to_en(manufacturers),
                    "Effective Start Date (EN)": translate_mk_to_en(eff_start),
                    "Effective End Date (EN)": translate_mk_to_en(eff_end),
                    "Public with VAT Price (EN)": translate_mk_to_en(retail_vat),
                    "Pharmacy Purchase Price (EN)": translate_mk_to_en(wholesale_wo_vat),
                    "Local Pack Description (EN)": translate_mk_to_en(local_pack_desc),
                    "Reimbursable Status": "PARTIALLY REIMBURSABLE",
                    "Reimbursable Rate": "80.00%",
                    "Reimbursable Notes": "",
                    "Copayment Value": "",
                    "Copayment Percent": "20.00%",
                    "Margin Rule": "650 PPP & PPI Listed",
                    "VAT Percent": "5",
                    "detail_url": url,
                    "page_num": progress_state.get("page_num_map", {}).get(url),
                }

                with out_lock:
                    append_rows_to_csv(output_path, [row], out_columns)

                with seen_lock:
                    seen_urls.add(url)

                with progress_lock:
                    progress_state["completed"] += 1
                    completed = progress_state["completed"]
                    percent = round((completed / total_urls) * 100, 1) if total_urls > 0 else 0
                    print(f"[PROGRESS] Processing packs: {completed}/{total_urls} ({percent}%) - Worker {worker_id}", flush=True)

                if sleep_between_details:
                    time.sleep(sleep_between_details)

            except (TimeoutException, WebDriverException, StaleElementReferenceException):
                pass
            finally:
                url_queue.task_done()
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def main(headless: bool = True, sleep_between_details: float = 0.1, detail_workers: int = DETAIL_WORKERS) -> None:
    if terminate_scraper_pids:
        try:
            terminate_scraper_pids("NorthMacedonia", _repo_root, silent=True)
        except Exception:
            pass

    urls_path = OUTPUT_DIR / URLS_CSV
    if not urls_path.exists():
        raise RuntimeError(f"URL list not found. Run 01_collect_urls.py first. ({urls_path})")

    output_path = OUTPUT_DIR / OUT_CSV

    # Output columns (RAW + EN)
    out_columns = [
        "Local Product Name (RAW)",
        "Local Pack Code (RAW)",
        "Generic Name (RAW)",
        "WHO ATC Code (RAW)",
        "Formulation (RAW)",
        "Strength Size (RAW)",
        "Fill Size (RAW)",
        "Customized 1 - Composition (RAW)",
        "Marketing Authority / Company Name (RAW)",
        "Effective Start Date (RAW)",
        "Effective End Date (RAW)",
        "Public with VAT Price (RAW)",
        "Pharmacy Purchase Price (RAW)",
        "Local Pack Description (RAW)",
        "Local Product Name (EN)",
        "Local Pack Code (EN)",
        "Generic Name (EN)",
        "WHO ATC Code (EN)",
        "Formulation (EN)",
        "Strength Size (EN)",
        "Fill Size (EN)",
        "Customized 1 - Composition (EN)",
        "Marketing Authority / Company Name (EN)",
        "Effective Start Date (EN)",
        "Effective End Date (EN)",
        "Public with VAT Price (EN)",
        "Pharmacy Purchase Price (EN)",
        "Local Pack Description (EN)",
        "Reimbursable Status",
        "Reimbursable Rate",
        "Reimbursable Notes",
        "Copayment Value",
        "Copayment Percent",
        "Margin Rule",
        "VAT Percent",
        "detail_url",
    ]
    ensure_csv_has_header(output_path, out_columns)

    # Load URLs with page numbers
    all_urls = []
    page_map = {}
    try:
        df_urls = pd.read_csv(urls_path, dtype=str)
        if "detail_url" in df_urls.columns:
            all_urls = df_urls["detail_url"].dropna().astype(str).tolist()
            if "page_num" in df_urls.columns:
                for _, row in df_urls.iterrows():
                    url = str(row.get("detail_url") or "").strip()
                    if url:
                        page_map[url] = row.get("page_num")
    except Exception:
        all_urls = load_existing_detail_urls(urls_path)
        # page_map stays empty
    already_scraped = load_already_scraped_urls(output_path)

    todo_urls = [u for u in all_urls if u not in already_scraped]
    total_urls = len(todo_urls)

    if total_urls == 0:
        print("No new URLs to scrape. Output already up to date.")
        return

    print(f"[PROGRESS] Processing packs: 0/{total_urls} (0%)", flush=True)

    url_queue = Queue()
    for url in todo_urls:
        url_queue.put(url)

    seen_lock = threading.Lock()
    out_lock = threading.Lock()
    progress_lock = threading.Lock()
    progress_state = {"completed": 0, "page_num_map": page_map}

    workers = []
    for worker_id in range(1, detail_workers + 1):
        t = threading.Thread(
            target=scrape_detail_urls_worker,
            args=(
                worker_id,
                url_queue,
                output_path,
                out_columns,
                already_scraped,
                seen_lock,
                out_lock,
                headless,
                sleep_between_details,
                progress_state,
                progress_lock,
                total_urls,
            ),
            daemon=True
        )
        t.start()
        workers.append(t)

    url_queue.join()
    for t in workers:
        t.join(timeout=5)

    if terminate_scraper_pids:
        try:
            terminate_scraper_pids("NorthMacedonia", _repo_root, silent=True)
        except Exception:
            pass

    print(f"Completed detail scraping. New rows written: {progress_state.get('completed', 0)}")


if __name__ == "__main__":
    headless = getenv_bool("SCRIPT_01_HEADLESS", True)
    sleep_between_details = getenv_float("SCRIPT_01_SLEEP_BETWEEN_DETAILS", 0.05)
    detail_workers = int(getenv("SCRIPT_02_DETAIL_WORKERS", "3") or "3")
    main(headless=headless, sleep_between_details=sleep_between_details, detail_workers=detail_workers)
