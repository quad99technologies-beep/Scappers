import csv
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple, Optional, Callable

# Add repo root to path for shared imports
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add script dir to path for local config_loader
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

try:
    from config_loader import load_env_file, getenv, getenv_bool, get_input_dir, get_output_dir
    load_env_file()
    USE_CONFIG = True
except ImportError:
    USE_CONFIG = False

    def getenv(key: str, default: str = "") -> str:
        return os.getenv(key, default)

    def getenv_bool(key: str, default: bool = False) -> bool:
        val = os.getenv(key, str(default))
        return str(val).lower() in ("true", "1", "yes", "on")

    def get_input_dir(subpath: str = None) -> Path:
        base = Path(__file__).resolve().parents[2] / "input" / "Taiwan"
        if subpath:
            return base / subpath
        return base

    def get_output_dir() -> Path:
        return Path(__file__).parent
try:
    from core.browser.chrome_instance_tracker import ChromeInstanceTracker
except ImportError:
    ChromeInstanceTracker = None

try:
    from core.browser.chrome_pid_tracker import get_chrome_pids_from_driver
except ImportError:
    get_chrome_pids_from_driver = None

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    ElementNotInteractableException,
    ElementClickInterceptedException,
    StaleElementReferenceException,
    InvalidSessionIdException,
    WebDriverException,
)

START_URL = "https://info.nhi.gov.tw/INAE3000/INAE3000S01"

SCRAPER_NAME = "Taiwan"

# DB imports
try:
    from core.db.connection import CountryDB
    from db.schema import apply_taiwan_schema
    from db.repositories import TaiwanRepository
    HAS_DB = True
except ImportError:
    HAS_DB = False

WAIT_TIMEOUT = 30
PAGE_LOAD_TIMEOUT = 60
SLEEP = 0.25
SEARCH_DELAY = 1.0
PAGE_DELAY = 0.8
ATC_INPUT_SELECTOR = "input[title*='ATC'], input[placeholder*='ATC']"

OUTPUT_COLUMNS = [
    "Drug Code",
    "Drug Code URL",
    "licId",
    "Name of the drug",
    "Name of drug (Chinese)",
    "Ingredient name/ingredient content",
    "Gauge quantity",
    "Single compound",
    "Price",
    "Effective date",
    "Effective start date",
    "Effective end date",
    "Pharmacists",
    "dosage form",
    "Classification of medicines",
    "Taxonomy group name",
    "ATC code",
]

ROW_FIELDS = [
    "Name of the drug",
    "Name of drug (Chinese)",
    "Ingredient name/ingredient content",
    "Gauge quantity",
    "Single compound",
    "Price",
    "Effective date",
    "Pharmacists",
    "dosage form",
    "Classification of medicines",
    "Taxonomy group name",
    "ATC code",
]
DATE_PATTERN = re.compile(r"\b\d{2,3}[./-]\d{1,2}[./-]\d{1,2}\b")


def configure_realtime_output() -> None:
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    try:
        sys.stderr.reconfigure(line_buffering=True)
    except Exception:
        pass


def prefix_progress_percent(
    processed_prefixes: int,
    total_prefixes: int,
    current_page: Optional[int] = None,
    total_pages: Optional[int] = None,
    target_page: Optional[int] = None,
) -> float:
    if total_prefixes <= 0:
        return 0.0
    prefix_fraction = 0.0
    if current_page is not None:
        denom = None
        if total_pages is not None and total_pages > 0:
            denom = total_pages
        elif target_page is not None and target_page > 0:
            denom = target_page
        if denom is not None:
            denom = max(denom, current_page)
            prefix_fraction = min(current_page / denom, 0.99)
    overall = (processed_prefixes + prefix_fraction) / total_prefixes * 100
    return round(overall, 1)


def emit_prefix_progress(
    processed_prefixes: int,
    total_prefixes: int,
    detail: str,
    current_page: Optional[int] = None,
    total_pages: Optional[int] = None,
    target_page: Optional[int] = None,
) -> None:
    pct = prefix_progress_percent(
        processed_prefixes,
        total_prefixes,
        current_page=current_page,
        total_pages=total_pages,
        target_page=target_page,
    )
    print(
        f"[PROGRESS] Collecting prefixes: {processed_prefixes}/{total_prefixes} ({pct}%) - {detail}",
        flush=True,
    )


def format_page_detail(
    prefix: str,
    prefix_position: int,
    total_prefixes: int,
    current_page: int,
    total_pages: int,
    page_rows: int,
    new_rows: int,
    seen_rows: int,
) -> str:
    if total_pages and total_pages >= current_page:
        page_part = f"page {current_page}/{total_pages}"
    elif total_pages:
        page_part = f"page {current_page} (visible {total_pages})"
    else:
        page_part = f"page {current_page}"
    return (
        f"Prefix {prefix} ({prefix_position}/{total_prefixes}) {page_part}, "
        f"rows {page_rows}, new {new_rows}, seen {seen_rows}"
    )


def ensure_driver() -> webdriver.Chrome:
    opts = webdriver.ChromeOptions()
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    if getenv_bool("SCRIPT_01_HEADLESS", False):
        opts.add_argument("--headless=new")
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    
    # Try to register with ChromeInstanceTracker (only if DB/RunID available)
    run_id = getenv("TW_RUN_ID")
    if ChromeInstanceTracker and run_id:
        try:
            from core.db.connection import CountryDB
            db = CountryDB("Taiwan") 
            db.connect()
            tracker = ChromeInstanceTracker("Taiwan", run_id, db)
            pid = driver.service.process.pid if hasattr(driver.service, 'process') else None
            if pid:
                pids = get_chrome_pids_from_driver(driver) if get_chrome_pids_from_driver else {pid}
                tracker.register(step_number=1, pid=pid, browser_type="chrome", child_pids=pids)
            db.close()
        except Exception:
            pass

    return driver


def iter_prefixes_aa_zz() -> List[str]:
    letters = "abcdefghijklmnopqrstuvwxyz"
    return [a + b for a in letters for b in letters]


def load_prefixes() -> List[str]:
    # 1. Try input table first (user uploads via Input page)
    input_table = getenv("SCRIPT_01_INPUT_TABLE", "").strip() if USE_CONFIG else ""
    if input_table and re.match(r"^[a-z_][a-z0-9_]*$", input_table):
        try:
            from core.db.connection import CountryDB
            db = CountryDB("Taiwan")
            db.connect()
            try:
                with db.cursor() as cur:
                    cur.execute(
                        "SELECT DISTINCT atc_code FROM " + input_table + " WHERE atc_code IS NOT NULL AND atc_code != '' ORDER BY atc_code"
                    )
                    prefixes = [row[0].strip().upper() for row in cur.fetchall() if row and row[0]]
                if prefixes:
                    print(f"[INPUT] Loaded {len(prefixes)} ATC prefixes from table {input_table}")
                    return prefixes
            finally:
                db.close()
        except Exception as e:
            print(f"[WARN] Could not load from input table {input_table}: {e}")
    
    # 2. Fallback to CSV file
    input_file = get_input_dir() / getenv("SCRIPT_01_ATC_PREFIX_FILE", "ATC_L3_L4_Prefixes.csv")
    if input_file.exists():
        with open(input_file, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            rows = list(reader)
        prefixes = []
        for row in rows[1:] if rows else []:
            if not row:
                continue
            prefix = row[0].strip()
            if prefix:
                prefixes.append(prefix.upper())
        if prefixes:
            return prefixes
    return [p.upper() for p in iter_prefixes_aa_zz()]


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def parse_licid(url: str) -> str:
    if not url:
        return ""
    m = re.search(r"[?&]licId=([^&]+)", url)
    return m.group(1) if m else ""


def split_effective_dates(text: str) -> Tuple[str, str]:
    if not text:
        return "", ""
    dates = DATE_PATTERN.findall(text)
    if not dates:
        return "", ""
    if len(dates) == 1:
        return dates[0], ""
    return dates[0], dates[1]


def find_atc_inputs(driver):
    return driver.find_elements(By.CSS_SELECTOR, ATC_INPUT_SELECTOR)


def get_atc_input(driver, wait):
    # Your HTML: <input title="ATC code" ...>
    locator = (By.CSS_SELECTOR, ATC_INPUT_SELECTOR)
    try:
        driver.switch_to.default_content()
    except Exception:
        pass
    el = wait.until(EC.presence_of_element_located(locator))
    for _ in range(5):
        try:
            if el.is_displayed() and el.is_enabled():
                return el
        except Exception:
            pass
        time.sleep(0.5)
        try:
            el = driver.find_element(*locator)
        except Exception:
            pass
    return wait.until(EC.element_to_be_clickable(locator))


def focus_and_clear_input(driver, el) -> None:
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    except Exception:
        pass
    try:
        el.click()
    except (ElementClickInterceptedException, ElementNotInteractableException, Exception):
        try:
            ActionChains(driver).move_to_element(el).click().perform()
        except Exception:
            try:
                driver.execute_script("arguments[0].click();", el)
            except Exception:
                pass
    try:
        el.clear()
        return
    except ElementNotInteractableException:
        pass
    try:
        el.send_keys(Keys.CONTROL, 'a')
        el.send_keys(Keys.DELETE)
        return
    except Exception:
        pass
    try:
        driver.execute_script(
            "arguments[0].value = '';"
            "arguments[0].dispatchEvent(new Event('input', {bubbles: true}));"
            "arguments[0].dispatchEvent(new Event('change', {bubbles: true}));",
            el,
        )
    except Exception:
        pass


def click_search(driver, wait):
    search_xpaths = [
        "//a[@role='button' and (normalize-space()='Search' or normalize-space()='\u67e5\u8a62')]",
        "//button[@role='button' and (normalize-space()='Search' or normalize-space()='\u67e5\u8a62')]",
        "//a[contains(@class,'btn') and contains(@class,'btn-l') and (normalize-space()='Search' or normalize-space()='\u67e5\u8a62')]",
        "//button[contains(@class,'btn') and contains(@class,'btn-l') and (normalize-space()='Search' or normalize-space()='\u67e5\u8a62')]",
    ]
    for xp in search_xpaths:
        try:
            btn = wait.until(EC.element_to_be_clickable((By.XPATH, xp)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            time.sleep(SLEEP)
            btn.click()
            return
        except TimeoutException:
            continue
        except Exception:
            continue

    # fallback any "btn btn-l" (may click Clear if Search is not found)
    btn = wait.until(EC.element_to_be_clickable(
        (By.XPATH, "//a[contains(@class,'btn') and contains(@class,'btn-l')]")
    ))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
    time.sleep(SLEEP)
    btn.click()


def open_advanced_queries(driver, wait) -> None:
    # Click the "Advanced queries" toggle before using ATC input
    try:
        if any(el.is_displayed() and el.is_enabled() for el in find_atc_inputs(driver)):
            return
    except Exception:
        pass

    xpaths = [
        "//a[@role='button' and (normalize-space()='Advanced queries' or normalize-space()='Advanced query' or normalize-space()='\u9032\u968e\u67e5\u8a62')]",
        "//button[@role='button' and (normalize-space()='Advanced queries' or normalize-space()='Advanced query' or normalize-space()='\u9032\u968e\u67e5\u8a62')]",
        "//div[.//img[contains(@src,'filter-solid')]]//a[@role='button' and (contains(normalize-space(),'Advanced') or contains(normalize-space(),'\u9032\u968e'))]",
        "//*[@aria-expanded='false' and (contains(@aria-controls,'advanced') or contains(@data-target,'advanced'))]",
        "//*[contains(@class,'advanced') and (self::a or self::button)]",
    ]

    for xp in xpaths:
        try:
            btn = wait.until(EC.element_to_be_clickable((By.XPATH, xp)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            time.sleep(SLEEP)
            try:
                btn.click()
            except Exception:
                driver.execute_script("arguments[0].click();", btn)
            time.sleep(0.5)
            try:
                wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, ATC_INPUT_SELECTOR)))
            except TimeoutException:
                pass
            return
        except TimeoutException:
            continue
        except Exception:
            continue


def get_current_page(driver) -> int:
    current = driver.find_elements(By.CSS_SELECTOR, "a.page-btn.current")
    if not current:
        return 1
    page_id = current[0].get_attribute("id") or ""
    if page_id.startswith("pageCode-"):
        try:
            return int(page_id.split("-", 1)[1])
        except ValueError:
            pass
    label = (current[0].text or "").strip()
    for token in label.split():
        if token.isdigit():
            return int(token)
    return 1


def get_total_pages(driver) -> int:
    max_page = 1
    for el in driver.find_elements(By.CSS_SELECTOR, "a.page-btn"):
        page_id = el.get_attribute("id") or ""
        if page_id.startswith("pageCode-"):
            token = page_id.split("-", 1)[1]
            if token.isdigit():
                max_page = max(max_page, int(token))
        label = (el.text or "").strip()
        if label.isdigit():
            max_page = max(max_page, int(label))
    return max_page


def click_next_page(driver, wait) -> bool:
    btns = driver.find_elements(By.XPATH, "//a[.//img[contains(@src,'btn-page-next')]]")
    if not btns:
        return False
    btn = btns[0]
    if not btn.is_displayed():
        return False
    old_page = get_current_page(driver)
    try:
        btn.click()
    except InvalidSessionIdException:
        raise
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", btn)
        except InvalidSessionIdException:
            raise
    try:
        wait.until(lambda d: get_current_page(d) != old_page)
    except TimeoutException:
        return False
    time.sleep(PAGE_DELAY)
    return True


def advance_to_page(
    driver,
    wait,
    target_page: int,
    on_advance: Optional[Callable[[int], None]] = None,
) -> bool:
    current = get_current_page(driver)
    if current >= target_page:
        if on_advance:
            on_advance(current)
        return True
    while current < target_page:
        moved = click_next_page(driver, wait)
        if not moved:
            return False
        current = get_current_page(driver)
        if on_advance:
            on_advance(current)
    return True


def has_no_results(driver) -> bool:
    markers = [
        "No data",
        "No Data",
        "NO DATA",
        "No results",
        "查無資料",
        "查无资料",
        "無資料",
        "无资料",
        "沒有資料",
        "没有资料",
    ]
    page_source = driver.page_source or ""
    for marker in markers:
        if marker in page_source:
            return True
    return False


def wait_results(driver, wait) -> bool:
    # Table from your paste: table.card-link-5
    try:
        wait.until(
            lambda d: d.find_elements(By.CSS_SELECTOR, "table.card-link-5 tbody tr")
            or has_no_results(d)
        )
        return True
    except TimeoutException:
        return False


def start_prefix_search(driver, wait, prefix: str) -> bool:
    driver.get(START_URL)

    open_advanced_queries(driver, wait)
    atc = get_atc_input(driver, wait)
    focus_and_clear_input(driver, atc)
    try:
        atc.send_keys(prefix)
    except ElementNotInteractableException:
        try:
            driver.execute_script(
                "arguments[0].value = arguments[1];"
                "arguments[0].dispatchEvent(new Event('input', {bubbles: true}));"
                "arguments[0].dispatchEvent(new Event('change', {bubbles: true}));",
                atc,
                prefix,
            )
        except Exception:
            pass
    time.sleep(SLEEP)

    click_search(driver, wait)
    if not wait_results(driver, wait):
        return False
    time.sleep(SEARCH_DELAY)
    return True


def restart_prefix_session(
    prefix: str,
    target_page: int,
    processed_prefixes: int,
    total_prefixes: int,
    prefix_position: int,
):
    emit_prefix_progress(
        processed_prefixes,
        total_prefixes,
        f"Prefix {prefix} ({prefix_position}/{total_prefixes}) browser disconnected; restarting session",
    )
    driver = ensure_driver()
    wait = WebDriverWait(driver, WAIT_TIMEOUT)
    if not start_prefix_search(driver, wait, prefix):
        return driver, wait, False
    if has_no_results(driver):
        return driver, wait, False
    if target_page > 1:
        def _on_advance(page: int) -> None:
            emit_prefix_progress(
                processed_prefixes,
                total_prefixes,
                f"Prefix {prefix} ({prefix_position}/{total_prefixes}) repositioning page {page}/{target_page}",
                current_page=page,
                target_page=target_page,
            )

        if not advance_to_page(driver, wait, target_page, on_advance=_on_advance):
            return driver, wait, False
    return driver, wait, True


def extract_drug_code_rows(driver) -> List[Dict[str, str]]:
    """
    Extract row details from S01 results table.
    First column contains <a href="https://lmspiq.fda.gov.tw/...licId=...">A000096100</a>
    """
    rows = driver.find_elements(By.CSS_SELECTOR, "table.card-link-5 tbody tr")
    out: List[Dict[str, str]] = []
    for row_idx in range(len(rows)):
        for attempt in range(3):
            try:
                rows = driver.find_elements(By.CSS_SELECTOR, "table.card-link-5 tbody tr")
                if row_idx >= len(rows):
                    break
                tr = rows[row_idx]
                cells = tr.find_elements(By.CSS_SELECTOR, "td")
                if not cells:
                    break

                code = ""
                url = ""
                try:
                    a = cells[0].find_element(By.CSS_SELECTOR, "a")
                    code = normalize_text(a.text)
                    url = normalize_text(a.get_attribute("href"))
                except Exception:
                    code = normalize_text(cells[0].text)

                if not code or not url:
                    break

                row = {
                    "Drug Code": code,
                    "Drug Code URL": url,
                    "licId": parse_licid(url),
                }

                for idx, field in enumerate(ROW_FIELDS, start=1):
                    value = ""
                    if idx < len(cells):
                        value = normalize_text(cells[idx].text)
                    row[field] = value

                effective_raw = row.get("Effective date", "")
                start_date, end_date = split_effective_dates(effective_raw)
                row["Effective start date"] = start_date
                row["Effective end date"] = end_date
                out.append(row)
                break
            except StaleElementReferenceException:
                time.sleep(0.25)
                if attempt == 2:
                    break
    return out


def main():
    configure_realtime_output()
    
    if not HAS_DB:
        print("[ERROR] Database support not available. Cannot run Taiwan collector.")
        sys.exit(1)

    # Resolve run_id
    run_id = os.environ.get("TW_RUN_ID", "").strip()
    if not run_id:
        print("[ERROR] No TW_RUN_ID. Set it or run pipeline from step 0.")
        sys.exit(1)

    # Initialize database
    db = CountryDB("Taiwan")
    db.connect()
    apply_taiwan_schema(db)
    repo = TaiwanRepository(db, run_id)
    repo.start_run(mode="resume")

    # Load seen codes and completed progress from DB
    completed_prefixes = repo.get_completed_keys(step_number=1)
    
    # Load prefixes to process
    prefixes = load_prefixes()
    total_prefixes = len(prefixes)
    processed_prefixes = len(completed_prefixes)
    
    if total_prefixes:
        emit_prefix_progress(processed_prefixes, total_prefixes, "Starting")
    else:
        print("[INFO] No prefixes found to process.", flush=True)

    driver = ensure_driver()
    wait = WebDriverWait(driver, WAIT_TIMEOUT)

    try:
        for i, prefix in enumerate(prefixes, 1):
            if prefix in completed_prefixes:
                continue
            
            # Start/Resume behavior for this prefix
            # In DB, we also track current page if needed
            start_page = 1 # Simple resume: always start prefix from page 1 unless we add page tracking
            
            prefix_position = i
            print(
                f"[ATC] Searching prefix: {prefix} "
                f"({prefix_position}/{total_prefixes})",
                flush=True,
            )
            
            repo.mark_progress(1, "collect_urls", prefix, "in_progress")

            try:
                if not start_prefix_search(driver, wait, prefix):
                    repo.mark_progress(1, "collect_urls", prefix, "completed")
                    processed_prefixes += 1
                    emit_prefix_progress(
                        processed_prefixes,
                        total_prefixes,
                        f"Prefix {prefix} results timeout; marking done",
                    )
                    continue
            except (InvalidSessionIdException, WebDriverException):
                # Browser crash
                try:
                    driver.quit()
                except Exception:
                    pass
                driver, wait, ok = restart_prefix_session(
                    prefix,
                    start_page,
                    processed_prefixes,
                    total_prefixes,
                    prefix_position,
                )
                if not ok:
                    repo.mark_progress(1, "collect_urls", prefix, "completed")
                    processed_prefixes += 1
                    emit_prefix_progress(
                        processed_prefixes,
                        total_prefixes,
                        f"Prefix {prefix} restart failed; marking done",
                    )
                    continue

            if has_no_results(driver):
                repo.mark_progress(1, "collect_urls", prefix, "completed")
                processed_prefixes += 1
                emit_prefix_progress(
                    processed_prefixes,
                    total_prefixes,
                    f"Prefix {prefix} has no results; marking done",
                )
                continue

            current_page = 1
            total_pages = get_total_pages(driver)

            while True:
                rows = extract_drug_code_rows(driver)
                
                # Transform to DB format
                db_rows = []
                for r in rows:
                    db_rows.append({
                        "drug_code": r.get("Drug Code"),
                        "drug_code_url": r.get("Drug Code URL"),
                        "lic_id": r.get("licId"),
                        "name_en": r.get("Name of the drug"),
                        "name_zh": r.get("Name of drug (Chinese)"),
                        "ingredient_content": r.get("Ingredient name/ingredient content"),
                        "gauge_quantity": r.get("Gauge quantity"),
                        "single_compound": r.get("Single compound"),
                        "price": r.get("Price"),
                        "effective_date": r.get("Effective date"),
                        "effective_start_date": r.get("Effective start date"),
                        "effective_end_date": r.get("Effective end date"),
                        "pharmacists": r.get("Pharmacists"),
                        "dosage_form": r.get("dosage form"),
                        "classification": r.get("Classification of medicines"),
                        "taxonomy_group": r.get("Taxonomy group name"),
                        "atc_code": r.get("ATC code"),
                        "page_number": current_page
                    })
                
                # Insert to DB
                repo.insert_drug_codes(db_rows)

                emit_prefix_progress(
                    processed_prefixes,
                    total_prefixes,
                    format_page_detail(
                        prefix, prefix_position, total_prefixes,
                        current_page, total_pages, len(rows), len(rows), 0
                    ),
                    current_page=current_page,
                    total_pages=total_pages,
                )

                if not click_next_page(driver, wait):
                    break
                
                current_page = get_current_page(driver)
                # Fail-safe to avoid infinite loops
                if current_page > 1000:
                    break

            repo.mark_progress(1, "collect_urls", prefix, "completed")
            processed_prefixes += 1
            emit_prefix_progress(
                processed_prefixes,
                total_prefixes,
                f"Prefix {prefix} completed",
            )

    except KeyboardInterrupt:
        print("\n[STOP] Interrupted by user. Progress saved in database.")
    except Exception as e:
        print(f"\n[FATAL] {e}", flush=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        if db:
            db.close()

    print("\n[DONE] URL Collection Complete.")


if __name__ == "__main__":
    main()
