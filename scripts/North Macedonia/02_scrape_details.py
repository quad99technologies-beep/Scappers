import os
import re
import time
import sys
import threading
from pathlib import Path
from typing import Dict, List, Optional
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

# Optional translation (only used if values still Macedonian)
try:
    from googletrans import Translator  # type: ignore
    _translator = Translator()
except Exception:
    _translator = None


# -----------------------------
# CONFIG
# -----------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
_repo_root = Path(__file__).resolve().parents[2]

# Shared chromedriver path to avoid concurrent downloads per thread
_driver_path = None
_driver_path_lock = threading.Lock()


def _get_chromedriver_path() -> Optional[str]:
    global _driver_path
    with _driver_path_lock:
        if _driver_path:
            return _driver_path
        try:
            _driver_path = ChromeDriverManager().install()
        except Exception:
            return None
        return _driver_path

try:
    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))
    from config_loader import load_env_file, get_output_dir, getenv, getenv_bool, getenv_int, getenv_float
    load_env_file()
    OUTPUT_DIR = get_output_dir()
except Exception:
    OUTPUT_DIR = SCRIPT_DIR
    def getenv(key: str, default: str = None) -> str:
        return os.getenv(key, default if default is not None else "")
    def getenv_bool(key: str, default: bool = False) -> bool:
        val = getenv(key, str(default))
        return str(val).lower() in ("1", "true", "yes", "on")
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

try:
    from core.chrome_pid_tracker import get_chrome_pids_from_driver, save_chrome_pids, terminate_scraper_pids
except Exception:
    get_chrome_pids_from_driver = None
    save_chrome_pids = None
    terminate_scraper_pids = None

URLS_CSV = getenv("SCRIPT_01_URLS_CSV", "north_macedonia_detail_urls.csv")
OUT_CSV = getenv("SCRIPT_02_OUTPUT_CSV", "north_macedonia_drug_register.csv")

DETAIL_WORKERS = getenv_int("SCRIPT_02_DETAIL_WORKERS", 3)
HEADLESS = getenv_bool("SCRIPT_02_HEADLESS", getenv_bool("SCRIPT_01_HEADLESS", True))
SLEEP_BETWEEN_DETAILS = getenv_float(
    "SCRIPT_02_SLEEP_BETWEEN_DETAILS",
    getenv_float("SCRIPT_01_SLEEP_BETWEEN_DETAILS", 0.15),
)
DISABLE_IMAGES = getenv_bool("SCRIPT_02_DISABLE_IMAGES", True)
DISABLE_CSS = getenv_bool("SCRIPT_02_DISABLE_CSS", True)

PAGELOAD_TIMEOUT = getenv_int("SCRIPT_02_PAGELOAD_TIMEOUT", 90)
WAIT_SECONDS = getenv_int("SCRIPT_02_WAIT_SECONDS", 40)

MAX_RETRIES_PER_URL = getenv_int("SCRIPT_02_MAX_RETRIES", 3)
DUMP_FAILED_HTML = getenv_bool("SCRIPT_02_DUMP_FAILED_HTML", True)

# Reimbursement constants (as per requirement)
REIMBURSABLE_STATUS = "PARTIALLY REIMBURSABLE"
REIMBURSABLE_RATE = "80.00%"
REIMBURSABLE_NOTES = ""
COPAYMENT_VALUE = ""
COPAYMENT_PERCENT = "20.00%"
MARGIN_RULE = "650 PPP & PPI Listed"
VAT_PERCENT = "5"


# -----------------------------
# HELPERS
# -----------------------------
def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


_cyrillic_re = re.compile(r"[\u0400-\u04FF]")

def looks_cyrillic(text: str) -> bool:
    return bool(_cyrillic_re.search(text or ""))


def translate_to_en(text: str) -> str:
    """
    Translate value to English only if it looks Cyrillic.
    If page is already translated, this will usually do nothing.
    """
    text = normalize_ws(text)
    if not text:
        return ""
    if not looks_cyrillic(text):
        return text
    if _translator is None:
        return text
    try:
        return normalize_ws(_translator.translate(text, src="mk", dest="en").text)
    except Exception:
        return text


def is_invalid_session(err: Exception) -> bool:
    msg = str(err).lower()
    return "invalid session id" in msg or "session not created" in msg or "disconnected" in msg


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
    df = pd.DataFrame(rows).reindex(columns=columns)
    df.to_csv(str(path), mode="a", header=False, index=False, encoding="utf-8-sig")


def load_already_scraped_urls(path: Path) -> set:
    if not path.exists():
        return set()
    try:
        df = pd.read_csv(path, usecols=["detail_url"], dtype=str)
        return set(df["detail_url"].dropna().astype(str).tolist())
    except Exception:
        return set()


def build_driver(headless: bool = True) -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1600,1000")

    # IMPORTANT: keep English UI preference so labels are likely English if site supports it,
    # but even if not, our mapping handles Macedonian.
    options.add_argument("--lang=en-US")

    # Speed-up (optional)
    prefs = {}
    if DISABLE_IMAGES:
        prefs["profile.managed_default_content_settings.images"] = 2
    if DISABLE_CSS:
        prefs["profile.managed_default_content_settings.stylesheets"] = 2
    options.add_experimental_option("prefs", prefs)

    driver_path = _get_chromedriver_path()
    if not driver_path:
        raise RuntimeError("Failed to resolve chromedriver path")
    service = ChromeService(driver_path)
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(PAGELOAD_TIMEOUT)
    if get_chrome_pids_from_driver and save_chrome_pids:
        try:
            pids = get_chrome_pids_from_driver(driver)
            if pids:
                save_chrome_pids("NorthMacedonia", _repo_root, pids)
        except Exception:
            pass
    return driver


def parse_detail_page(driver: webdriver.Chrome) -> Dict[str, str]:
    """
    Extracts label->value for each row-fluid.
    Works for both MK and translated EN pages, even with nested <font>.
    """
    WebDriverWait(driver, WAIT_SECONDS).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.row-fluid"))
    )
    rows = driver.find_elements(By.CSS_SELECTOR, "div.row-fluid")

    data: Dict[str, str] = {}
    for r in rows:
        try:
            label_el = r.find_element(By.CSS_SELECTOR, "div.span2 b")
            value_el = r.find_element(By.CSS_SELECTOR, "div.span6")
            label = normalize_ws(label_el.text)
            value = normalize_ws(value_el.text)
            if label:
                data[label] = value
        except Exception:
            continue
    return data


def get_by_any_contains(data: Dict[str, str], *needles: str) -> str:
    """
    Robust getter: matches if ANY needle appears in the label.
    Handles punctuation changes like ":" and multiple languages.
    """
    wants = [n.strip().lower() for n in needles if n and n.strip()]
    if not wants:
        return ""
    for label, value in data.items():
        ll = (label or "").lower()
        if any(w in ll for w in wants):
            return value
    return ""


def extract_fields(detail: Dict[str, str]) -> Dict[str, str]:
    """
    Supports BOTH Macedonian labels and translated English labels.
    """
    local_product = get_by_any_contains(detail, "име на лекот (латиница)", "name of the drug (latin)")
    ean = get_by_any_contains(detail, "ean", "ean код")
    generic = get_by_any_contains(detail, "генеричко име", "generic name")
    atc = get_by_any_contains(detail, "атц", "atc")
    formulation = get_by_any_contains(detail, "фармацевтска форма", "pharmaceutical form")
    strength = get_by_any_contains(detail, "јачина", "strength", "reliability")
    packaging = get_by_any_contains(detail, "пакување", "packaging")
    composition = get_by_any_contains(detail, "состав", "composition")
    manufacturers = get_by_any_contains(detail, "производители", "manufacturers")
    eff_start = get_by_any_contains(detail, "датум на решение", "decision date", "date of solution")
    eff_end = get_by_any_contains(detail, "датум на важност", "expiration date", "date of validity")
    retail_vat = get_by_any_contains(detail, "малопродажна цена со", "retail price with vat")
    wholesale_wo_vat = get_by_any_contains(detail, "големопродажна цена", "wholesale price excluding vat", "wholesale price without vat")

    return {
        "Local Product Name": local_product,
        "Local Pack Code": ean,
        "Generic Name": generic,
        "WHO ATC Code": atc,
        "Formulation": formulation,
        "Strength Size": strength,
        "Fill Size": packaging,
        "Customized 1": composition,
        "Marketing Authority / Company Name": manufacturers,
        "Effective Start Date": eff_start,
        "Effective End Date": eff_end,
        "Public with VAT Price": retail_vat,
        "Pharmacy Purchase Price": wholesale_wo_vat,
    }


def dump_failed_page(output_dir: Path, url: str, driver: webdriver.Chrome, worker_id: int) -> None:
    if not DUMP_FAILED_HTML:
        return
    try:
        safe = re.sub(r"[^a-zA-Z0-9]+", "_", url)[-120:]
        out = output_dir / f"FAILED_detail_worker{worker_id}_{safe}.html"
        out.write_text(driver.page_source, encoding="utf-8", errors="ignore")
    except Exception:
        pass


# -----------------------------
# WORKER
# -----------------------------
def worker_fn(
    worker_id: int,
    q: Queue,
    output_path: Path,
    out_columns: List[str],
    already_scraped: set,
    seen_lock: threading.Lock,
    out_lock: threading.Lock,
    progress: dict,
    progress_lock: threading.Lock,
) -> None:
    driver: Optional[webdriver.Chrome] = None
    try:
        driver = build_driver(headless=HEADLESS)

        while True:
            try:
                url = q.get(timeout=2)
            except Empty:
                break

            if not url:
                q.task_done()
                continue

            # skip already scraped
            with seen_lock:
                if url in already_scraped:
                    q.task_done()
                    continue

            ok = False
            last_err = None

            for attempt in range(1, MAX_RETRIES_PER_URL + 1):
                try:
                    driver.get(url)

                    detail = parse_detail_page(driver)
                    fields = extract_fields(detail)

                    # Validate: at least EAN or Product Name or Generic must exist,
                    # otherwise page likely not loaded properly.
                    if not any([fields["Local Pack Code"], fields["Local Product Name"], fields["Generic Name"]]):
                        raise RuntimeError("Parsed empty critical fields; page may not be fully loaded.")

                    local_pack_desc = make_local_pack_description(
                        fields["Formulation"],
                        fields["Fill Size"],
                        fields["Strength Size"],
                        fields["Customized 1"],
                    )

                    # English-final output (translate values only if they still look Cyrillic)
                    row = {
                        "Local Product Name": translate_to_en(fields["Local Product Name"]),
                        "Local Pack Code": normalize_ws(fields["Local Pack Code"]),  # EAN stays as-is
                        "Generic Name": translate_to_en(fields["Generic Name"]),
                        "WHO ATC Code": normalize_ws(fields["WHO ATC Code"]),
                        "Formulation": translate_to_en(fields["Formulation"]),
                        "Strength Size": translate_to_en(fields["Strength Size"]),
                        "Fill Size": translate_to_en(fields["Fill Size"]),
                        "Customized 1": translate_to_en(fields["Customized 1"]),
                        "Marketing Authority / Company Name": translate_to_en(fields["Marketing Authority / Company Name"]),
                        "Effective Start Date": normalize_ws(fields["Effective Start Date"]),
                        "Effective End Date": normalize_ws(fields["Effective End Date"]),
                        "Public with VAT Price": normalize_ws(fields["Public with VAT Price"]),
                        "Pharmacy Purchase Price": normalize_ws(fields["Pharmacy Purchase Price"]),
                        "Local Pack Description": translate_to_en(local_pack_desc),

                        "Reimbursable Status": REIMBURSABLE_STATUS,
                        "Reimbursable Rate": REIMBURSABLE_RATE,
                        "Reimbursable Notes": REIMBURSABLE_NOTES,
                        "Copayment Value": COPAYMENT_VALUE,
                        "Copayment Percent": COPAYMENT_PERCENT,
                        "Margin Rule": MARGIN_RULE,
                        "VAT Percent": VAT_PERCENT,

                        "detail_url": url,
                    }

                    with out_lock:
                        append_rows_to_csv(output_path, [row], out_columns)

                    with seen_lock:
                        already_scraped.add(url)

                    with progress_lock:
                        progress["done"] += 1
                        done = progress["done"]
                        total = progress["total"]
                        progress["processed"].add(url)
                        pct = round((done / total) * 100, 1) if total else 0
                        print(f"[PROGRESS] {done}/{total} ({pct}%) - worker {worker_id}", flush=True)

                    if SLEEP_BETWEEN_DETAILS > 0:
                        time.sleep(SLEEP_BETWEEN_DETAILS)

                    ok = True
                    break

                except (TimeoutException, WebDriverException, StaleElementReferenceException, RuntimeError) as e:
                    last_err = e
                    if is_invalid_session(e):
                        try:
                            if driver:
                                driver.quit()
                        except Exception:
                            pass
                        try:
                            driver = build_driver(headless=HEADLESS)
                        except Exception:
                            driver = None
                    # small backoff then retry
                    time.sleep(0.8 * attempt)
                    continue
                except Exception as e:
                    last_err = e
                    time.sleep(0.8 * attempt)
                    continue

            if not ok:
                print(f"[WARN] Failed URL after retries (worker {worker_id}): {url} | err={last_err}", flush=True)
                if driver:
                    dump_failed_page(OUTPUT_DIR, url, driver, worker_id)

            q.task_done()

    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


# -----------------------------
# MAIN
# -----------------------------
def main() -> None:
    if terminate_scraper_pids:
        try:
            terminate_scraper_pids("NorthMacedonia", _repo_root, silent=True)
        except Exception:
            pass
    urls_path = OUTPUT_DIR / URLS_CSV
    if not urls_path.exists():
        fallback_output = _repo_root / "output" / "NorthMacedonia" / URLS_CSV
        if fallback_output.exists():
            urls_path = fallback_output
        else:
            fallback_local = Path(__file__).resolve().parent / URLS_CSV
            if fallback_local.exists():
                urls_path = fallback_local
            else:
                raise RuntimeError(f"URL list not found: {urls_path}. Run your URL-collector first.")

    df_urls = pd.read_csv(urls_path, dtype=str)
    if "detail_url" not in df_urls.columns:
        raise RuntimeError(f"'detail_url' column not found in {urls_path.name}")

    if "detailed_view_scraped" not in df_urls.columns:
        df_urls["detailed_view_scraped"] = "no"
    df_urls["detailed_view_scraped"] = df_urls["detailed_view_scraped"].astype(str).str.strip()

    all_urls = df_urls["detail_url"].dropna().astype(str).map(str.strip).tolist()
    all_urls = [u for u in all_urls if u]

    output_path = OUTPUT_DIR / OUT_CSV

    out_columns = [
        "Local Product Name",
        "Local Pack Code",
        "Generic Name",
        "WHO ATC Code",
        "Formulation",
        "Strength Size",
        "Fill Size",
        "Customized 1",
        "Marketing Authority / Company Name",
        "Effective Start Date",
        "Effective End Date",
        "Public with VAT Price",
        "Pharmacy Purchase Price",
        "Local Pack Description",
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

    already_scraped = load_already_scraped_urls(output_path)
    if already_scraped:
        df_urls.loc[df_urls["detail_url"].astype(str).isin(already_scraped), "detailed_view_scraped"] = "yes"
    status_map = {
        str(row.get("detail_url") or "").strip(): str(row.get("detailed_view_scraped") or "").strip().lower()
        for _, row in df_urls.iterrows()
    }
    todo_urls = [
        u for u in all_urls
        if u not in already_scraped and status_map.get(u, "no") != "yes"
    ]

    total = len(todo_urls)
    if total == 0:
        print("No new URLs to scrape. Output already up to date.")
        return

    print(f"[START] URLs total={len(all_urls)} | todo={total} | workers={DETAIL_WORKERS} | headless={HEADLESS}", flush=True)

    q = Queue()
    for u in todo_urls:
        q.put(u)

    seen_lock = threading.Lock()
    out_lock = threading.Lock()
    progress_lock = threading.Lock()
    progress = {"done": 0, "total": total, "processed": set()}

    threads = []
    for wid in range(1, DETAIL_WORKERS + 1):
        t = threading.Thread(
            target=worker_fn,
            args=(wid, q, output_path, out_columns, already_scraped, seen_lock, out_lock, progress, progress_lock),
            daemon=True,
        )
        t.start()
        threads.append(t)

    q.join()

    for t in threads:
        t.join(timeout=5)

    processed = progress.get("processed", set())
    if processed:
        df_urls.loc[df_urls["detail_url"].astype(str).isin(processed), "detailed_view_scraped"] = "yes"
        df_urls.to_csv(urls_path, index=False, encoding="utf-8-sig")
        print(f"[INFO] Updated detailed_view_scraped=yes for {len(processed)} URLs in {urls_path.name}", flush=True)

    print(f"[DONE] Scraped rows added: {progress['done']} | Output: {output_path}", flush=True)
    if terminate_scraper_pids:
        try:
            terminate_scraper_pids("NorthMacedonia", _repo_root, silent=True)
        except Exception:
            pass


if __name__ == "__main__":
    main()
