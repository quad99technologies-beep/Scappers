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
    from core.chrome_pid_tracker import (
        get_chrome_pids_from_driver,
        save_chrome_pids,
        terminate_scraper_pids,
    )
except ImportError:
    get_chrome_pids_from_driver = None
    save_chrome_pids = None
    terminate_scraper_pids = None

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

OUTPUT_DIR = get_output_dir()
INPUT_PREFIXES = get_input_dir() / getenv("SCRIPT_01_ATC_PREFIX_FILE", "ATC_L3_L4_Prefixes.csv")
OUT_CODES = OUTPUT_DIR / getenv("SCRIPT_01_OUT_CODES", "taiwan_drug_code_urls.csv")
SEEN_CODES = OUTPUT_DIR / getenv("SCRIPT_01_SEEN_CODES", "seen_drug_codes.txt")
PROGRESS = OUTPUT_DIR / getenv("SCRIPT_01_PROGRESS_FILE", "taiwan_progress_prefix.txt")

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
    if get_chrome_pids_from_driver and save_chrome_pids:
        try:
            pids = get_chrome_pids_from_driver(driver)
            if pids:
                save_chrome_pids(SCRAPER_NAME, _repo_root, pids)
        except Exception:
            pass
    return driver


def iter_prefixes_aa_zz() -> List[str]:
    letters = "abcdefghijklmnopqrstuvwxyz"
    return [a + b for a in letters for b in letters]


def load_prefixes() -> List[str]:
    if INPUT_PREFIXES.exists():
        with open(INPUT_PREFIXES, "r", encoding="utf-8-sig") as f:
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


def read_progress() -> Dict[str, Any]:
    default_state = {"prefixes": {}, "last_prefix": ""}
    if not PROGRESS.exists():
        return default_state
    raw = PROGRESS.read_text(encoding="utf-8").strip()
    if not raw:
        return default_state
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = raw
    if isinstance(data, str):
        return {"prefixes": {data: {"page": 1, "done": False}}, "last_prefix": data}
    if isinstance(data, dict):
        if "prefixes" in data and isinstance(data["prefixes"], dict):
            data.setdefault("last_prefix", "")
            return data
        prefixes = {}
        last_prefix = ""
        for key, value in data.items():
            if key == "last_prefix" and isinstance(value, str):
                last_prefix = value
                continue
            if isinstance(value, dict):
                page = int(value.get("page", 1))
                done = bool(value.get("done", False))
                prefixes[key] = {"page": page, "done": done}
            elif isinstance(value, int):
                prefixes[key] = {"page": value, "done": False}
        return {"prefixes": prefixes, "last_prefix": last_prefix}
    return default_state


def write_progress(state: Dict[str, Any]) -> None:
    PROGRESS.write_text(json.dumps(state, ensure_ascii=True, indent=2), encoding="utf-8")


def load_seen() -> Set[str]:
    if not SEEN_CODES.exists():
        return set()
    with open(SEEN_CODES, "r", encoding="utf-8") as f:
        return set(x.strip() for x in f if x.strip())


def append_seen(code: str):
    with open(SEEN_CODES, "a", encoding="utf-8") as f:
        f.write(code + "\n")


def ensure_out_header():
    if OUT_CODES.exists():
        with open(OUT_CODES, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, [])
        if header == OUTPUT_COLUMNS:
            return
        raise RuntimeError(
            f"{OUT_CODES} has an unexpected header. "
            f"Delete it (and {SEEN_CODES}, {PROGRESS}) to regenerate with full details."
        )
    with open(OUT_CODES, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(OUTPUT_COLUMNS)


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
    if terminate_scraper_pids:
        try:
            terminate_scraper_pids(SCRAPER_NAME, _repo_root, silent=True)
        except Exception:
            pass

    ensure_out_header()
    seen = load_seen()
    progress = read_progress()
    progress_prefixes = progress.setdefault("prefixes", {})

    prefixes = load_prefixes()
    last_prefix = (progress.get("last_prefix") or "").upper()
    if last_prefix and last_prefix in prefixes:
        prefixes = prefixes[prefixes.index(last_prefix):]

    total_prefixes = len(prefixes)
    processed_prefixes = 0
    if total_prefixes:
        emit_prefix_progress(processed_prefixes, total_prefixes, "Starting")
    else:
        print("[INFO] No prefixes found to process.", flush=True)

    driver = ensure_driver()
    wait = WebDriverWait(driver, WAIT_TIMEOUT)

    try:
        for i, prefix in enumerate(prefixes, 1):
            restart_attempts = 0
            prefix_state = progress_prefixes.get(prefix) or progress_prefixes.get(prefix.lower()) or {}
            if prefix_state.get("done"):
                processed_prefixes += 1
                emit_prefix_progress(
                    processed_prefixes,
                    total_prefixes,
                    f"Skipped prefix {prefix} (already done)",
                )
                continue
            start_page = max(1, int(prefix_state.get("page", 1)))

            prefix_position = processed_prefixes + 1
            print(
                f"[ATC] Searching prefix: {prefix} "
                f"({prefix_position}/{total_prefixes}) starting at page {start_page}",
                flush=True,
            )
            progress["last_prefix"] = prefix
            write_progress(progress)

            try:
                if not start_prefix_search(driver, wait, prefix):
                    progress_prefixes[prefix] = {"page": start_page, "done": True}
                    write_progress(progress)
                    processed_prefixes += 1
                    emit_prefix_progress(
                        processed_prefixes,
                        total_prefixes,
                        f"Prefix {prefix} results timeout; marking done",
                    )
                    continue
            except (InvalidSessionIdException, WebDriverException):
                restart_attempts += 1
                if restart_attempts > 3:
                    raise
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
                    progress_prefixes[prefix] = {"page": start_page, "done": True}
                    write_progress(progress)
                    processed_prefixes += 1
                    emit_prefix_progress(
                        processed_prefixes,
                        total_prefixes,
                        f"Prefix {prefix} restart failed; marking done",
                    )
                    continue

            total_pages = get_total_pages(driver)
            emit_prefix_progress(
                processed_prefixes,
                total_prefixes,
                f"Prefix {prefix} ({prefix_position}/{total_prefixes}) start page {start_page}",
            )

            if has_no_results(driver):
                progress_prefixes[prefix] = {"page": start_page, "done": True}
                write_progress(progress)
                processed_prefixes += 1
                emit_prefix_progress(
                    processed_prefixes,
                    total_prefixes,
                    f"Prefix {prefix} has no results; marking done",
                )
                continue

            if start_page > 1:
                def _on_advance(page: int) -> None:
                    emit_prefix_progress(
                        processed_prefixes,
                        total_prefixes,
                        f"Prefix {prefix} ({prefix_position}/{total_prefixes}) advancing page {page}/{start_page}",
                        current_page=page,
                        target_page=start_page,
                    )

                if not advance_to_page(driver, wait, start_page, on_advance=_on_advance):
                    progress_prefixes[prefix] = {"page": start_page, "done": True}
                    write_progress(progress)
                    processed_prefixes += 1
                    emit_prefix_progress(
                        processed_prefixes,
                        total_prefixes,
                        f"Prefix {prefix} advance failed at page {start_page}; marking done",
                    )
                    continue

            current_page = get_current_page(driver)
            while True:
                if current_page < start_page:
                    def _on_advance(page: int) -> None:
                        emit_prefix_progress(
                            processed_prefixes,
                            total_prefixes,
                            f"Prefix {prefix} ({prefix_position}/{total_prefixes}) advancing page {page}/{start_page}",
                            current_page=page,
                            target_page=start_page,
                        )

                    if not advance_to_page(driver, wait, start_page, on_advance=_on_advance):
                        progress_prefixes[prefix] = {"page": start_page, "done": True}
                        write_progress(progress)
                        processed_prefixes += 1
                        emit_prefix_progress(
                            processed_prefixes,
                            total_prefixes,
                            f"Prefix {prefix} advance failed at page {start_page}; marking done",
                        )
                        break
                    current_page = get_current_page(driver)

                try:
                    rows = extract_drug_code_rows(driver)
                    if not rows and has_no_results(driver):
                        progress_prefixes[prefix] = {"page": current_page, "done": True}
                        write_progress(progress)
                        processed_prefixes += 1
                        emit_prefix_progress(
                            processed_prefixes,
                            total_prefixes,
                            f"Prefix {prefix} has no results; marking done",
                        )
                        break
                    page_rows = len(rows)
                    new_rows = 0
                    seen_rows = 0
                    if rows:
                        with open(OUT_CODES, "a", newline="", encoding="utf-8") as f:
                            w = csv.writer(f)
                            for row in rows:
                                code = row.get("Drug Code", "")
                                if not code or code in seen:
                                    seen_rows += 1
                                    continue
                                w.writerow([row.get(col, "") for col in OUTPUT_COLUMNS])
                                seen.add(code)
                                append_seen(code)
                                new_rows += 1

                    total_pages = max(total_pages, get_total_pages(driver))
                    emit_prefix_progress(
                        processed_prefixes,
                        total_prefixes,
                        format_page_detail(
                            prefix,
                            prefix_position,
                            total_prefixes,
                            current_page,
                            total_pages,
                            page_rows,
                            new_rows,
                            seen_rows,
                        ),
                        current_page=current_page,
                        total_pages=total_pages,
                    )

                    progress_prefixes[prefix] = {"page": current_page + 1, "done": False}
                    write_progress(progress)

                    if not click_next_page(driver, wait):
                        progress_prefixes[prefix] = {"page": current_page, "done": True}
                        write_progress(progress)
                        processed_prefixes += 1
                        emit_prefix_progress(
                            processed_prefixes,
                            total_prefixes,
                            f"Completed prefix {prefix}",
                        )
                        break
                    current_page = get_current_page(driver)
                except (InvalidSessionIdException, WebDriverException):
                    restart_attempts += 1
                    if restart_attempts > 3:
                        raise
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    driver, wait, ok = restart_prefix_session(
                        prefix,
                        current_page,
                        processed_prefixes,
                        total_prefixes,
                        prefix_position,
                    )
                    if not ok:
                        progress_prefixes[prefix] = {"page": current_page, "done": True}
                        write_progress(progress)
                        processed_prefixes += 1
                        emit_prefix_progress(
                            processed_prefixes,
                            total_prefixes,
                            f"Prefix {prefix} restart failed; marking done",
                        )
                        break
                    current_page = get_current_page(driver)

            time.sleep(SLEEP)

        print("DONE: Collected unique Drug Code URLs:", OUT_CODES, flush=True)

    finally:
        driver.quit()
        if terminate_scraper_pids:
            try:
                terminate_scraper_pids(SCRAPER_NAME, _repo_root, silent=True)
            except Exception:
                pass


if __name__ == "__main__":
    main()
