import csv
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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

_script_dir = Path(__file__).resolve().parent
_repo_root = Path(__file__).resolve().parents[2]
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

try:
    from core.chrome_pid_tracker import get_chrome_pids_from_driver, save_chrome_pids, terminate_scraper_pids
except Exception:
    get_chrome_pids_from_driver = None
    save_chrome_pids = None
    terminate_scraper_pids = None

# Import Telegram notifier for status updates
try:
    from core.telegram_notifier import TelegramNotifier
    TELEGRAM_NOTIFIER_AVAILABLE = True
except ImportError:
    TELEGRAM_NOTIFIER_AVAILABLE = False
    TelegramNotifier = None

# Import core chrome_manager for offline-capable chromedriver resolution
try:
    if str(_repo_root) not in sys.path:
        sys.path.insert(0, str(_repo_root))
    from core.chrome_manager import get_chromedriver_path
    CORE_CHROMEDRIVER_AVAILABLE = True
except ImportError:
    CORE_CHROMEDRIVER_AVAILABLE = False
    get_chromedriver_path = None


# ============================
# CONFIG (EDIT IF NEEDED)
# ============================
START_URL = "https://lekovi.zdravstvo.gov.mk/maxprices/0"

HEADLESS = getenv_bool("SCRIPT_03_HEADLESS", True)
DISABLE_IMAGES = getenv_bool("SCRIPT_03_DISABLE_IMAGES", True)
DISABLE_CSS = getenv_bool("SCRIPT_03_DISABLE_CSS", True)
ROWS_PER_PAGE = getenv("SCRIPT_03_ROWS_PER_PAGE", "200")
CHECKPOINT_JSON = getenv("SCRIPT_03_CHECKPOINT_JSON", "mk_maxprices_checkpoint.json")

# Translation mode (Python-based)
TRANSLATE_SRC = "mk"
TRANSLATE_DEST = "en"

# Output translation (Python-based)
TRANSLATE_OUTPUT_FALLBACK = True
TRANSLATE_ALL_FIELDS = True
TRANSLATE_RETRIES = 3
TRANSLATE_RETRY_SLEEP = 0.4

# Runtime controls
MAX_PAGES = getenv_int("SCRIPT_03_MAX_PAGES", 0)  # 0 = unlimited
SLEEP_AFTER_ROW = getenv_float("SCRIPT_03_SLEEP_AFTER_ROW", 1.0)
SLEEP_AFTER_MODAL_OPEN = getenv_float("SCRIPT_03_SLEEP_AFTER_MODAL_OPEN", 0.2)

# Selenium timeouts
PAGE_LOAD_TIMEOUT = getenv_int("SCRIPT_03_PAGE_LOAD_TIMEOUT", 90)
WAIT_TIMEOUT = getenv_int("SCRIPT_03_WAIT_TIMEOUT", 30)
# ============================


# ----------------------------
# Output path: same folder as script
# ----------------------------
OUTPUT_CSV = getenv("SCRIPT_03_OUTPUT_CSV", "maxprices_output.csv")
OUT_CSV = str(OUTPUT_DIR / OUTPUT_CSV)


class OutputTranslatorFallback:
    """
    Python-based translation for extracted values.
    """
    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self._translator = None
        self._cache = {}
        if not enabled:
            return
        try:
            from deep_translator import GoogleTranslator  # type: ignore
            self._translator = GoogleTranslator(source=TRANSLATE_SRC, target=TRANSLATE_DEST)
        except Exception:
            self._translator = None
            print(
                "[WARN] deep-translator not installed. Install:\n"
                "       pip install deep-translator\n"
                "       Output translation will be skipped.",
                file=sys.stderr,
            )

    def to_en_if_needed(self, text: str) -> str:
        text = (text or "").strip()
        if not text:
            return ""
        if not self.enabled or self._translator is None:
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


# ----------------------------
# Selenium / parsing
# ----------------------------
@dataclass
class BaseRow:
    atc: str
    local_pack: str
    generic: str
    manufacturer: str
    issuance: str
    price_main: str


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
    if prefs:
        opts.add_experimental_option("prefs", prefs)
    # Use offline-capable chromedriver resolution if available
    if CORE_CHROMEDRIVER_AVAILABLE and get_chromedriver_path:
        driver_path = get_chromedriver_path()
    else:
        driver_path = ChromeDriverManager().install()
    service = ChromeService(driver_path)
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    if get_chrome_pids_from_driver and save_chrome_pids:
        try:
            pids = get_chrome_pids_from_driver(driver)
            if pids:
                save_chrome_pids("NorthMacedonia", _repo_root, pids)
        except Exception:
            pass
    return driver


def wait_table_ready(wait: WebDriverWait) -> None:
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table.table-bordered.table-condensed")))
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table.table-bordered.table-condensed tbody")))


def safe_text(el) -> str:
    try:
        return (el.text or "").strip()
    except Exception:
        return ""


def set_rows_per_page(driver: webdriver.Chrome, wait: WebDriverWait, value: str) -> None:
    wait.until(EC.presence_of_element_located((By.ID, "rowsPerPage")))
    sel = Select(driver.find_element(By.ID, "rowsPerPage"))
    current = sel.first_selected_option.get_attribute("value")
    if current == value:
        return

    old_tbody = None
    try:
        old_tbody = driver.find_element(By.CSS_SELECTOR, "table.table.table-bordered.table-condensed tbody")
    except Exception:
        pass

    try:
        sel.select_by_value(value)
    except Exception:
        return

    if old_tbody is not None:
        try:
            wait.until(EC.staleness_of(old_tbody))
        except Exception:
            pass

    wait_table_ready(wait)


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
    # Avoid Selenium's is_displayed JS on some pages; presence is enough here.
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
    """
    FIXED: filters out header row "Date|Price" / "Датум|Цена"
    Enforces: date-like AND numeric-like.
    """
    modal = driver.find_element(By.CSS_SELECTOR, "div.modal.in, div.modal.fade.in")
    body = modal.find_element(By.CSS_SELECTOR, "div.modal-body")

    rows: List[Tuple[str, str]] = []
    date_re = re.compile(r"^\s*\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\s*$")
    price_re = re.compile(r"^\s*[\d\s.,]+\s*$")
    header_words = {"date", "price", "datum", "cena", "цена", "датум"}

    for r in body.find_elements(By.CSS_SELECTOR, "div.row"):
        cols = r.find_elements(By.CSS_SELECTOR, "div.span2")
        if len(cols) < 2:
            continue

        d = safe_text(cols[0])
        p = safe_text(cols[1])
        if not d or not p:
            continue

        dl = d.strip().lower()
        pl = p.strip().lower()
        if dl in header_words or pl in header_words:
            continue

        if not date_re.match(d):
            continue
        if not price_re.match(p):
            continue

        # ensure at least one digit in price
        if not re.search(r"\d", p):
            continue

        rows.append((d.strip(), p.strip()))

    return rows


def get_current_page_number(driver: webdriver.Chrome) -> Optional[int]:
    """Read current page from span.current inside pager."""
    try:
        el = driver.find_element(By.CSS_SELECTOR, "div.t-data-grid-pager span.current")
        t = safe_text(el)
        return int(t) if t.isdigit() else None
    except Exception:
        return None


def get_total_pages(driver: webdriver.Chrome) -> Optional[int]:
    """Compute max numeric page from pager anchors."""
    try:
        candidates = driver.find_elements(By.CSS_SELECTOR, "div.t-data-grid-pager a[id^='pager']")
        nums = []
        for el in candidates:
            t = safe_text(el)
            if t.isdigit():
                nums.append(int(t))
        spans = driver.find_elements(By.CSS_SELECTOR, "div.t-data-grid-pager span.current")
        for el in spans:
            t = safe_text(el)
            if t.isdigit():
                nums.append(int(t))
        return max(nums) if nums else None
    except Exception:
        return None


def click_next_page(driver: webdriver.Chrome, wait: WebDriverWait) -> bool:
    """Click next page using numeric link, fallback to arrow."""
    # Capture old tbody for staleness detection
    old_tbody = None
    try:
        old_tbody = driver.find_element(By.CSS_SELECTOR, "table.table.table-bordered.table-condensed tbody")
    except Exception:
        pass

    # Read current page
    current_page = get_current_page_number(driver)
    print(f"[DEBUG] Current page: {current_page}")

    # Determine target page number
    if current_page is None:
        # Fallback: try clicking page "2" if we're on page 1
        target = 2
        print(f"[DEBUG] Current page unknown, attempting to click page {target}")
    else:
        target = current_page + 1
        print(f"[DEBUG] Target next page: {target}")

    # Try to find numeric page link
    candidates = []
    try:
        xpath = f"//div[contains(@class,'t-data-grid-pager')]//a[normalize-space(text())='{target}']"
        candidates = driver.find_elements(By.XPATH, xpath)
        print(f"[DEBUG] Found {len(candidates)} candidates for page {target} via numeric link")
    except Exception as e:
        print(f"[DEBUG] Error finding numeric link: {e}")
        candidates = []

    # Filter to displayed candidates
    candidates = [c for c in candidates if c.is_displayed()]

    if candidates:
        # Try clicking numeric page link
        for i, el in enumerate(candidates):
            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                el.click()
                print(f"[DEBUG] Clicked numeric link candidate {i} (page {target}), waiting for page load...")

                if old_tbody is not None:
                    wait.until(EC.staleness_of(old_tbody))
                wait_table_ready(wait)
                print(f"[DEBUG] Successfully navigated to page {target}")
                return True
            except Exception as e:
                print(f"[DEBUG] Failed to click numeric candidate {i}: {e}")
                continue

    # Fallback: try clicking arrow button (pager_11 is the ">" button)
    print(f"[DEBUG] Numeric link failed, trying arrow button fallback")
    try:
        arrow = driver.find_element(By.CSS_SELECTOR, "a#pager_11")
        if arrow.is_displayed():
            print(f"[DEBUG] Found arrow button (pager_11), attempting click...")
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", arrow)
            arrow.click()

            if old_tbody is not None:
                wait.until(EC.staleness_of(old_tbody))
            wait_table_ready(wait)
            print(f"[DEBUG] Successfully clicked arrow button")
            return True
    except Exception as e:
        print(f"[DEBUG] Arrow button fallback failed: {e}")

    print("[DEBUG] All pagination attempts failed")
    return False


def ensure_csv_header(path: str, fieldnames: List[str]) -> None:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=fieldnames).writeheader()


def flush_file(f) -> None:
    f.flush()
    try:
        os.fsync(f.fileno())
    except Exception:
        pass


def load_existing_keys(path: str) -> set:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return set()
    keys = set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                keys.add(make_row_key(row))
    except Exception:
        return set()
    return keys


def make_row_key(row: Dict[str, str]) -> str:
    parts = [
        (row.get("WHO_ATC_Code") or "").strip(),
        (row.get("Local_Pack_Description_MK") or "").strip(),
        (row.get("Pharmacy_Purchase_Price") or "").strip(),
        (row.get("Effective_Start_Date") or "").strip(),
    ]
    return "||".join(parts)


def read_checkpoint(path: Path) -> Dict[str, int]:
    if path.exists():
        try:
            import json
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            page = int(data.get("page", 1))
            row_index = int(data.get("row_index", 0))
            return {"page": max(page, 1), "row_index": max(row_index, 0)}
        except Exception:
            pass
    return {"page": 1, "row_index": 0}


def write_checkpoint(path: Path, page: int, row_index: int) -> None:
    try:
        import json
        payload = {"page": int(page), "row_index": int(row_index)}
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


# ----------------------------
# MAIN
# ----------------------------
def run() -> None:
    if terminate_scraper_pids:
        try:
            terminate_scraper_pids("NorthMacedonia", _repo_root, silent=True)
        except Exception:
            pass

    # Initialize Telegram notifier for status updates
    telegram_notifier = None
    if TELEGRAM_NOTIFIER_AVAILABLE:
        try:
            telegram_notifier = TelegramNotifier("NorthMacedonia", rate_limit=60.0)
            if telegram_notifier.enabled:
                telegram_notifier.send_started("Scrape Max Prices - Step 3/4")
                print("[INFO] Telegram notifications enabled", flush=True)
        except Exception as e:
            print(f"[WARN] Failed to initialize Telegram notifier: {e}", flush=True)
            telegram_notifier = None

    # Initialize DB repository
    repo = None
    try:
        run_id = os.environ.get("NORTH_MACEDONIA_RUN_ID", "")
        if run_id:
            # Add repo root to path for imports
            if str(_repo_root) not in sys.path:
                sys.path.insert(0, str(_repo_root))
            from core.db.connection import CountryDB
            from db.repositories import NorthMacedoniaRepository
            db = CountryDB("NorthMacedonia")
            repo = NorthMacedoniaRepository(db, run_id)
            repo.ensure_run_in_ledger(mode="resume")
            print(f"[DB] Connected (run_id: {run_id})", flush=True)
    except Exception as e:
        print(f"[DB] Not available: {e} (CSV-only mode)", flush=True)

    driver = make_driver(HEADLESS)
    wait = WebDriverWait(driver, WAIT_TIMEOUT)
    xlate = OutputTranslatorFallback(enabled=TRANSLATE_OUTPUT_FALLBACK)

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

    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    ensure_csv_header(OUT_CSV, fieldnames)
    existing_keys = load_existing_keys(OUT_CSV)

    total_out = 0
    page_count = 0
    checkpoint_path = OUTPUT_DIR / CHECKPOINT_JSON
    checkpoint = read_checkpoint(checkpoint_path)
    start_page = checkpoint.get("page", 1)
    start_row = checkpoint.get("row_index", 0)

    try:
        print(f"[INFO] Script folder output: {OUT_CSV}")

        # --- Open START (original) ---
        print(f"[INFO] Opening original: {START_URL}")
        driver.get(START_URL)

        wait_table_ready(wait)

        print(f"[INFO] Setting rowsPerPage={ROWS_PER_PAGE}")
        set_rows_per_page(driver, wait, ROWS_PER_PAGE)
        total_pages = get_total_pages(driver)

        if start_page > 1 or start_row > 0:
            print(f"[INFO] Resuming from page {start_page}, row {start_row + 1}")
            current_page = get_current_page_number(driver) or 1
            while current_page < start_page:
                moved = click_next_page(driver, wait)
                if not moved:
                    break
                current_page = get_current_page_number(driver) or (current_page + 1)

        while True:
            page_count += 1
            if MAX_PAGES > 0 and page_count > MAX_PAGES:
                print(f"[INFO] Reached MAX_PAGES={MAX_PAGES}. Stopping.")
                break

            page_no = get_current_page_number(driver)
            page_label = page_no if page_no is not None else page_count
            print(f"\n[PAGE] Now processing page: {page_label}")

            rows = []
            for retry in range(2):
                try:
                    rows = parse_main_rows(driver, wait)
                    break
                except StaleElementReferenceException:
                    if retry == 1:
                        raise
                    print("[WARN] Stale DOM while parsing rows. Retrying...")
                    wait_table_ready(wait)

            print(f"[INFO] Rows found on page {page_label}: {len(rows)}")

            with open(OUT_CSV, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames)

                for idx, (base, link_el) in enumerate(rows, start=1):
                    if page_label == start_page and idx <= start_row:
                        continue
                    print(f"[ROW] Page {page_label} - Row {idx}/{len(rows)} | ATC={base.atc}")

                    hist: List[Tuple[str, str]] = []
                    for retry in range(2):
                        try:
                            open_price_history_modal(driver, wait, link_el)
                            if SLEEP_AFTER_MODAL_OPEN > 0:
                                time.sleep(SLEEP_AFTER_MODAL_OPEN)

                            hist = extract_price_history_rows(driver)
                            close_modal(driver, wait)
                            break

                        except StaleElementReferenceException:
                            print("[WARN] Stale element during modal. Re-finding row and retrying...")
                            wait_table_ready(wait)
                            refreshed = parse_main_rows(driver, wait)
                            if idx - 1 < len(refreshed):
                                base, link_el = refreshed[idx - 1]
                            if retry == 1:
                                raise

                        except TimeoutException:
                            print("[WARN] Modal timeout. Writing base row without effective date.")
                            close_modal(driver, wait)
                            hist = []
                            break

                    # translate fallback only if still Cyrillic
                    local_en = xlate.to_en_if_needed(base.local_pack)
                    manuf_en = xlate.to_en_if_needed(base.manufacturer)
                    generic_en = xlate.to_en_if_needed(base.generic)
                    issuance_en = xlate.to_en_if_needed(base.issuance)

                    def write_row(price: str, date: str) -> None:
                        nonlocal total_out
                        row = {
                            "WHO_ATC_Code": base.atc,
                            "Local_Pack_Description_MK": base.local_pack,
                            "Local_Pack_Description_EN": local_en,
                            "Generic_Name": generic_en,
                            "Marketing_Company_MK": base.manufacturer,
                            "Marketing_Company_EN": manuf_en,
                            "Customized_Column_1": issuance_en,
                            "Pharmacy_Purchase_Price": price,
                            "Effective_Start_Date": date,
                        }
                        row_key = make_row_key(row)
                        if row_key in existing_keys:
                            return
                        w.writerow(row)
                        existing_keys.add(row_key)
                        total_out += 1
                        flush_file(f)

                        # Also write to DB
                        if repo:
                            try:
                                repo.insert_max_price(row)
                            except Exception as e:
                                # Log but don't fail - CSV is primary
                                if total_out % 100 == 1:  # Log only occasionally
                                    print(f"[DB WARN] {e}", flush=True)

                    # same business logic: expand history rows
                    if not hist:
                        write_row(base.price_main, "")
                    elif len(hist) == 1:
                        d, p = hist[0]
                        price_to_use = base.price_main if base.price_main else p
                        write_row(price_to_use, d)
                    else:
                        for d, p in hist:
                            write_row(p, d)

                    print(f"[OK] Total rows written so far: {total_out}")
                    progress_percent = None
                    if total_pages and len(rows) > 0:
                        progress_percent = round((((page_label - 1) + (idx / len(rows))) / total_pages) * 100, 1)
                    elif len(rows) > 0:
                        progress_percent = round((idx / len(rows)) * 100, 1)
                    if progress_percent is not None:
                        print(
                            f"[PROGRESS] Max Prices: page {page_label}"
                            f"{'/' + str(total_pages) if total_pages else ''} "
                            f"row {idx}/{len(rows)} ({progress_percent}%)",
                            flush=True,
                        )
                    write_checkpoint(checkpoint_path, page_label, idx)

                    if SLEEP_AFTER_ROW > 0:
                        time.sleep(SLEEP_AFTER_ROW)

            print(f"[INFO] Going to next page from page {page_label} ...")

            # Debug: Check pagination before clicking
            current_page = get_current_page_number(driver)
            total_pages_now = get_total_pages(driver)
            print(f"[DEBUG] Current page: {current_page}, Total pages: {total_pages_now}")

            moved = click_next_page(driver, wait)
            if not moved:
                print("[WARN] click_next_page() returned False. Checking for next page elements...")
                try:
                    # Try to find next page candidates for debugging
                    next_candidates = driver.find_elements(By.XPATH, "//a[normalize-space(text())='>' or normalize-space(text())='>>']")
                    print(f"[DEBUG] Found {len(next_candidates)} next page candidates via XPath")
                    if next_candidates:
                        for i, elem in enumerate(next_candidates):
                            try:
                                print(f"[DEBUG] Candidate {i}: displayed={elem.is_displayed()}, text='{safe_text(elem)}'")
                            except:
                                pass
                except Exception as e:
                    print(f"[DEBUG] Error checking next page elements: {e}")

                print("[INFO] No next page found. Done.")

                # Send Telegram notification
                if telegram_notifier:
                    try:
                        details = f"Stopped at page {page_label}\nTotal rows: {total_out}"
                        telegram_notifier.send_warning("Max Price Pagination Stopped", details=details, force=True)
                    except Exception:
                        pass
                break

            # Send Telegram progress update (every page)
            if telegram_notifier:
                try:
                    if total_pages_now:
                        telegram_notifier.send_progress(
                            page_label,
                            total_pages_now,
                            "Scrape Max Prices",
                            details=f"Rows written: {total_out}"
                        )
                    else:
                        telegram_notifier.send_status(
                            f"Page {page_label}",
                            "Scrape Max Prices",
                            details=f"Rows written: {total_out}"
                        )
                except Exception:
                    pass

            write_checkpoint(checkpoint_path, page_label + 1, 0)

        print(f"\n[DONE] Finished. Total output rows: {total_out}")
        print(f"[DONE] Output CSV: {OUT_CSV}")

        # Send Telegram success notification
        if telegram_notifier:
            try:
                details = f"Total rows: {total_out}\nPages processed: {page_count}\nOutput: {OUTPUT_CSV}"
                telegram_notifier.send_success("Max Price Scraping Completed", details=details)
            except Exception:
                pass

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
    run()
