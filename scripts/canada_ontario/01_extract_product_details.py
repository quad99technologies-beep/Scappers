"""
Ontario Formulary scraper (end-to-end) with resume support

WHAT IT DOES
1) Loops q=a..z:
   https://www.formulary.health.gov.on.ca/formulary/results.xhtml?q=<q>&s=true&type=4
2) Extracts ALL rows from the results table (user assumption: no scroll/pagination needed).
3) Manufacturer resolution:
   - FIRST: resolve Manufacturer Name from LOCAL manufacturer master CSV by MFR code.
   - ONLY IF missing / blank / bad: open DIN detail.xhtml?drugId=... and extract Manufacturer name,
     then update BOTH product output + local master table.
4) PK / price-type logic:
   - Primary: if local_pack_code endswith 'PK' (last 2 letters) => PACK
   - Fallback (for Ontario screenshots): if Brand/Description contains token 'Pk'/'PK' => PACK
   - Else => UNIT
5) Reimbursable + copay (as per your final rule):
   - reimbursable_price = Amount MOH Pays (if numeric), else fallback to Drug Benefit Price
   - public_with_vat = exfactory_price * 1.08
   - copay = public_with_vat - reimbursable_price
   NOTE: Here exfactory_price is taken as "Drug Benefit Price or Unit Price" column because that is
   the only base price available on the results page.

RESUME SUPPORT:
- Tracks completed letters (q_letter) to skip already processed searches
- Saves data after each letter search
- Deduplicates products by local_pack_code
- Progress tracking with [PROGRESS] messages

OUTPUTS
- output/products.csv
- output/manufacturer_master.csv
- output/completed_letters.json (tracks which letters are done)

Deps:
  pip install requests beautifulsoup4 lxml pandas
"""

import os
import re
import time
import string
import json
import sys
import random
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Set
from pathlib import Path
from urllib.parse import urlencode
from datetime import datetime

try:
    from core.browser.browser_session import BrowserSession
    BROWSER_SESSION_AVAILABLE = True
except ImportError:
    BROWSER_SESSION_AVAILABLE = False
    
# Add script directory to path for config_loader import
_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from config_loader import (
    get_output_dir,
    get_logs_dir,
    get_run_id,
    get_run_dir,
    get_proxy_config,
    getenv,
    getenv_bool,
    getenv_int,
    getenv_float,
)
from core.utils.logger import setup_standard_logger
from core.config.retry_config import RetryConfig
from core.progress.progress_tracker import StandardProgress
from core.pipeline.pipeline_checkpoint import get_checkpoint_manager

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config_loader import (
    MAX_BAD_ROW_RATIO,
    USE_BROWSER,
    CHROME_PID_TRACKING_AVAILABLE,
    PAGE_VALIDATION_RETRIES,
)
from core.db.postgres_connection import PostgresDB

try:
    from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    Progress = None

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, WebDriverException
    from selenium.webdriver.chrome.service import Service as ChromeService
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    webdriver = None
    By = None
    Keys = None
    WebDriverWait = None
    EC = None
    TimeoutException = None
    WebDriverException = None
    ChromeService = None

try:
    from webdriver_manager.chrome import ChromeDriverManager
    WEBDRIVER_MANAGER_AVAILABLE = True
except ImportError:
    WEBDRIVER_MANAGER_AVAILABLE = False
    ChromeDriverManager = None

try:
    from smart_locator import SmartLocator
    from state_machine import NavigationStateMachine, NavigationState, StateCondition
    STATE_MACHINE_AVAILABLE = True
except ImportError:
    STATE_MACHINE_AVAILABLE = False
    SmartLocator = None
    NavigationStateMachine = None
    NavigationState = None
    StateCondition = None

try:
    from core.browser.stealth_profile import apply_selenium
    STEALTH_PROFILE_AVAILABLE = True
except ImportError:
    STEALTH_PROFILE_AVAILABLE = False
    def apply_selenium(options):
        return None

try:
    from core.browser.browser_observer import observe_selenium, wait_until_idle
    BROWSER_OBSERVER_AVAILABLE = True
except ImportError:
    BROWSER_OBSERVER_AVAILABLE = False
    def observe_selenium(driver):
        return None
    def wait_until_idle(state, timeout=RetryConfig.NAVIGATION_TIMEOUT):
        return None

try:
    from core.browser.human_actions import pause, type_delay
    HUMAN_ACTIONS_AVAILABLE = True
except ImportError:
    HUMAN_ACTIONS_AVAILABLE = False
    def pause(min_s=0.2, max_s=0.6):
        time.sleep(random.uniform(min_s, max_s))
    def type_delay():
        time.sleep(random.uniform(0.04, 0.12))

try:
    from core.browser.chrome_instance_tracker import ChromeInstanceTracker
    from core.browser.chrome_pid_tracker import get_chrome_pids_from_driver
    from core.db.postgres_connection import PostgresDB
    CHROME_INSTANCE_TRACKING_AVAILABLE = True
except ImportError:
    CHROME_INSTANCE_TRACKING_AVAILABLE = False
    def get_chrome_pids_from_driver(driver):
        return set()
    ChromeInstanceTracker = None
    PostgresDB = None

try:
    from core.browser.chrome_manager import get_chromedriver_path as _core_get_chromedriver_path
    CORE_CHROMEDRIVER_AVAILABLE = True
except ImportError:
    CORE_CHROMEDRIVER_AVAILABLE = False
    _core_get_chromedriver_path = None

BASE = "https://www.formulary.health.gov.on.ca/formulary/"
RESULTS_URL = BASE + "results.xhtml"
DETAIL_URL = BASE + "detail.xhtml"

# Use platform config for output directory
OUT_DIR = get_output_dir()
OUT_DIR.mkdir(parents=True, exist_ok=True)

PRODUCTS_CSV = str(OUT_DIR / "products.csv")
MFR_MASTER_CSV = str(OUT_DIR / "manufacturer_master.csv")
COMPLETED_LETTERS_JSON = str(OUT_DIR / "completed_letters.json")

# Tuning (be nice to the server)
SLEEP_BETWEEN_Q = 0.35
SLEEP_BETWEEN_DETAIL = 0.15
RETRIES = getenv_int("MAX_RETRIES", RetryConfig.MAX_RETRIES)
TIMEOUT = getenv_int("PAGE_LOAD_TIMEOUT", RetryConfig.PAGE_LOAD_TIMEOUT)
REQUEST_TIMEOUT = getenv_int("REQUEST_TIMEOUT", RetryConfig.CONNECTION_CHECK_TIMEOUT)

BAD_NAME_SET = {"", "n/a", "na", "none", "unknown", "-", "--"}

# Browser and anti-bot controls
USE_BROWSER = getenv_bool("USE_BROWSER", False)
USE_SEARCH_INPUT = getenv_bool("USE_SEARCH_INPUT", True)
HEADLESS = getenv_bool("HEADLESS", True)
ENABLE_STEALTH = getenv_bool("ENABLE_STEALTH", True)
ENABLE_BROWSER_OBSERVER = getenv_bool("ENABLE_BROWSER_OBSERVER", True)
HUMANIZE_TYPING = getenv_bool("HUMANIZE_TYPING", True)
HUMANIZE_MIN_DELAY = getenv_float("HUMANIZE_MIN_DELAY", 0.05)
HUMANIZE_MAX_DELAY = getenv_float("HUMANIZE_MAX_DELAY", 0.16)
RESTART_DRIVER_EVERY_N = getenv_int("RESTART_DRIVER_EVERY_N", 0)  # 0 disables
CAPTURE_BROWSER_ERRORS = getenv_bool("CAPTURE_BROWSER_ERRORS", False)

# Integrity checks
PAGE_VALIDATION_RETRIES = getenv_int("PAGE_VALIDATION_RETRIES", RetryConfig.MAX_RETRY_LOOPS)
PAGE_MIN_ROWS = getenv_int("PAGE_MIN_ROWS", 1)
MAX_BAD_ROW_RATIO = getenv_float("MAX_BAD_ROW_RATIO", 0.2)
ALLOW_EMPTY_RESULTS = getenv_bool("ALLOW_EMPTY_RESULTS", True)

# Progress/output
ENABLE_PROGRESS_BAR = getenv_bool("ENABLE_PROGRESS_BAR", True)
LOG_TO_FILE = getenv_bool("LOG_TO_FILE", False)

# DB-only mode: skip CSV writes when DB migration is active (default True)
DB_ONLY = getenv_bool("DB_ONLY", True)

# Request behavior
REQUEST_JITTER_MIN = getenv_float("REQUEST_JITTER_MIN", 0.1)
REQUEST_JITTER_MAX = getenv_float("REQUEST_JITTER_MAX", 0.6)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

PROXIES = get_proxy_config()

run_id = get_run_id()
run_dir = get_run_dir(run_id)
log_path = (run_dir / "logs" / "canada_ontario_extract.log") if LOG_TO_FILE else None
logger = setup_standard_logger("canada_ontario_extract", scraper_name="CanadaOntario", log_file=log_path)


def norm(s: str) -> str:
    return (s or "").strip()


def is_bad_name(name: Optional[str]) -> bool:
    if name is None:
        return True
    return norm(name).lower() in BAD_NAME_SET


def parse_float(s: str) -> Optional[float]:
    s = norm(s)
    if not s or s.lower() in {"n/a", "na"}:
        return None
    # keep digits and dot only
    s = re.sub(r"[^\d.]+", "", s)
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def jitter_sleep(min_s: float = None, max_s: float = None) -> None:
    lo = REQUEST_JITTER_MIN if min_s is None else min_s
    hi = REQUEST_JITTER_MAX if max_s is None else max_s
    if hi <= 0:
        return
    time.sleep(random.uniform(lo, hi))


def backoff_sleep(attempt: int) -> None:
    time.sleep(RetryConfig.calculate_backoff_delay(max(attempt - 1, 0)))


def build_headers() -> Dict[str, str]:
    return {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "User-Agent": random.choice(USER_AGENTS),
    }


def human_type(element, text: str) -> None:
    if not HUMANIZE_TYPING:
        element.send_keys(text)
        return
    for ch in text:
        element.send_keys(ch)
        time.sleep(random.uniform(HUMANIZE_MIN_DELAY, HUMANIZE_MAX_DELAY))


class ProgressReporter:
    def __init__(self, total_letters: int) -> None:
        self.total_letters = total_letters
        self.use_rich = ENABLE_PROGRESS_BAR and RICH_AVAILABLE
        self.progress = None
        self.task_letters = None
        self.task_rows = None
        if self.use_rich:
            self.progress = Progress(
                TextColumn("{task.description}"),
                BarColumn(),
                TextColumn("{task.completed}/{task.total}"),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
            )
            self.progress.start()
            self.task_letters = self.progress.add_task("Letters", total=total_letters)
            self.task_rows = self.progress.add_task("Rows", total=1)

    def start_rows(self, total_rows: int) -> None:
        if self.use_rich and self.progress is not None and self.task_rows is not None:
            self.progress.update(self.task_rows, total=max(total_rows, 1), completed=0)

    def advance_row(self, n: int = 1) -> None:
        if self.use_rich and self.progress is not None and self.task_rows is not None:
            self.progress.advance(self.task_rows, n)

    def advance_letter(self, n: int = 1) -> None:
        if self.use_rich and self.progress is not None and self.task_letters is not None:
            self.progress.advance(self.task_letters, n)

    def close(self) -> None:
        if self.use_rich and self.progress is not None:
            self.progress.stop()


def _get_chromedriver_path() -> Optional[str]:
    if CORE_CHROMEDRIVER_AVAILABLE and _core_get_chromedriver_path:
        try:
            return _core_get_chromedriver_path()
        except Exception:
            return None
    if WEBDRIVER_MANAGER_AVAILABLE and ChromeDriverManager:
        try:
            return ChromeDriverManager().install()
        except Exception:
            return None
    return None


def build_driver() -> webdriver.Chrome:
    if not SELENIUM_AVAILABLE:
        raise RuntimeError("Selenium is not available but USE_BROWSER is enabled.")
    options = webdriver.ChromeOptions()
    if ENABLE_STEALTH and STEALTH_PROFILE_AVAILABLE:
        apply_selenium(options)
    if HEADLESS:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1600,1000")
    options.add_argument("--lang=en-US")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-translate")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-sync")
    options.add_argument("--disable-default-apps")
    options.add_argument("--mute-audio")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument(f"--user-agent={random.choice(USER_AGENTS)}")
    profile_dir = run_dir / "browser_profiles" / "chrome"
    profile_dir.mkdir(parents=True, exist_ok=True)
    options.add_argument(f"--user-data-dir={profile_dir}")

    driver_path = _get_chromedriver_path()
    if driver_path:
        service = ChromeService(driver_path)
        driver = webdriver.Chrome(service=service, options=options)
    else:
        driver = webdriver.Chrome(options=options)

    driver.set_page_load_timeout(TIMEOUT)
    try:
        driver.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
    except Exception:
        pass

    if CHROME_INSTANCE_TRACKING_AVAILABLE and run_id:
        try:
            pids = get_chrome_pids_from_driver(driver)
            if pids:
                driver_pid = driver.service.process.pid if hasattr(driver.service, 'process') else list(pids)[0]
                db = PostgresDB("CanadaOntario")
                db.connect()
                try:
                    tracker = ChromeInstanceTracker("CanadaOntario", run_id, db)
                    tracker.register(step_number=1, pid=driver_pid, browser_type="chrome", child_pids=pids)
                finally:
                    db.close()
        except Exception:
            pass

    return driver


# BrowserSession imported from core

    def get_page_html(self, url: str, label: str = "") -> str:
        driver = self.ensure()
        last_err = None
        for attempt in range(1, RETRIES + 1):
            try:
                driver.get(url)
                if self.observer_state and ENABLE_BROWSER_OBSERVER and BROWSER_OBSERVER_AVAILABLE:
                    wait_until_idle(self.observer_state, timeout=RetryConfig.NAVIGATION_TIMEOUT)
                pause(0.2, 0.6)
                self.uses += 1
                if RESTART_DRIVER_EVERY_N and self.uses % RESTART_DRIVER_EVERY_N == 0:
                    self.restart()
                return driver.page_source
            except Exception as exc:
                last_err = exc
                if CAPTURE_BROWSER_ERRORS:
                    try:
                        safe_label = re.sub(r"[^A-Za-z0-9_-]+", "_", label or "page")
                        debug_dir = run_dir / "artifacts" / "browser_failures"
                        debug_dir.mkdir(parents=True, exist_ok=True)
                        screenshot_path = debug_dir / f"{safe_label}_attempt{attempt}.png"
                        html_path = debug_dir / f"{safe_label}_attempt{attempt}.html"
                        driver.save_screenshot(str(screenshot_path))
                        with open(html_path, "w", encoding="utf-8") as fh:
                            fh.write(driver.page_source or "")
                        logger.info("Captured browser failure: %s, %s", screenshot_path, html_path)
                    except Exception:
                        pass
                logger.warning(f"[BROWSER] {label} attempt {attempt} failed: {exc}")
                backoff_sleep(attempt)
                if attempt < RETRIES:
                    self.restart()
        raise RuntimeError(f"Browser GET failed for {label}: {last_err}")

    def try_search_input(self, query: str) -> None:
        if not USE_SEARCH_INPUT:
            return
        driver = self.ensure()
        input_elem = None
        if self.locator:
            for css in [
                "input[name='q']",
                "input[name='searchTerm']",
                "input[type='search']",
                "input[id*='search']",
                "input[id*='Search']",
            ]:
                try:
                    input_elem = self.locator.find_element(css=css, required=False, timeout=RetryConfig.ELEMENT_WAIT_TIMEOUT)
                    if input_elem:
                        break
                except Exception:
                    continue
        if not input_elem:
            for css in [
                "input[name='q']",
                "input[name='searchTerm']",
                "input[type='search']",
                "input[id*='search']",
                "input[id*='Search']",
            ]:
                try:
                    input_elem = driver.find_element(By.CSS_SELECTOR, css)
                    break
                except Exception:
                    continue
        if not input_elem:
            return
        try:
            input_elem.clear()
            human_type(input_elem, query)
            input_elem.send_keys(Keys.ENTER)
            pause(0.2, 0.6)
        except Exception:
            return


def safe_get(session: requests.Session, url: str, params: dict = None, timeout: int = REQUEST_TIMEOUT) -> str:
    last = None
    headers = build_headers()
    for i in range(1, RETRIES + 1):
        try:
            r = session.get(url, params=params, timeout=timeout, headers=headers, proxies=PROXIES or None)
            if r.status_code == 403:
                raise RuntimeError(f"Request blocked (HTTP 403): {url}")
            if r.status_code in (429, 500, 502, 503, 504):
                backoff_sleep(i)
                continue
            r.raise_for_status()
            jitter_sleep()
            return r.text
        except Exception as e:
            last = e
            backoff_sleep(i)
    raise RuntimeError(f"GET failed: {url} params={params} err={last}")


def load_mfr_master_from_db() -> Dict[str, str]:
    """Load manufacturer master from co_manufacturers when DB_ONLY."""
    master: Dict[str, str] = {}
    try:
        db = PostgresDB("CanadaOntario")
        db.connect()
        try:
            run_id = get_run_id()
            with db.cursor() as cur:
                cur.execute(
                    "SELECT manufacturer_code, manufacturer_name FROM co_manufacturers WHERE run_id = %s",
                    (run_id,),
                )
                for r in cur.fetchall():
                    if r[0]:
                        master[norm(r[0])] = norm(r[1] or "")
        finally:
            db.close()
    except Exception as e:
        logger.warning(f"Could not load mfr master from DB: {e}")
    return master


def load_mfr_master(path: str) -> Dict[str, str]:
    if DB_ONLY:
        return load_mfr_master_from_db()
    if not os.path.exists(path):
        return {}
    df = pd.read_csv(path, dtype=str).fillna("")
    master: Dict[str, str] = {}
    for _, r in df.iterrows():
        code = norm(r.get("mfr_code", ""))
        name = norm(r.get("manufacturer_name", ""))
        if code:
            master[code] = name
    return master


def save_mfr_master(path: str, master: Dict[str, str]) -> None:
    df = pd.DataFrame(
        [{"mfr_code": k, "manufacturer_name": v} for k, v in sorted(master.items(), key=lambda x: x[0])]
    )
    df.to_csv(path, index=False, encoding="utf-8-sig")


def load_completed_letters(cp) -> Set[str]:
    """Load set of completed letters from DB (primary) or JSON file (fallback)."""
    # Try DB first
    db_completed = load_completed_letters_from_db()
    if db_completed:
        logger.info(f"[DB] Loaded {len(db_completed)} completed letters from database")
        return db_completed
    
    # Fallback to JSON file
    path = Path(COMPLETED_LETTERS_JSON)
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return set(data.get("completed_letters", []))
        except Exception:
            return set()
    metadata = cp.get_metadata() if cp else {}
    return set(metadata.get("completed_letters", []))


def save_completed_letters(completed: Set[str]) -> None:
    """Save set of completed letters to JSON file (atomic) - legacy fallback."""
    data = {"completed_letters": sorted(list(completed))}
    path = Path(COMPLETED_LETTERS_JSON)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(".tmp")
    try:
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        temp_path.replace(path)
    except Exception:
        if temp_path.exists():
            temp_path.unlink()
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)


def save_mfr_master_to_db(mfr_master: Dict[str, str]) -> None:
    """Save manufacturer master to database."""
    if not mfr_master:
        return
    try:
        insert_manufacturers_to_db(mfr_master)
        logger.info(f"[DB] Saved {len(mfr_master)} manufacturers to co_manufacturers")
    except Exception as e:
        logger.error(f"[DB] Failed to save manufacturers: {e}")


def detect_price_type(local_pack_code: str, brand_desc: str) -> str:
    """
    Your final instruction says: PK is last 2 alphabets of Pack ID.
    Ontario screenshots also show 'Pk' inside the brand description.

    Rule:
      1) If local_pack_code endswith PK (case-insensitive) -> PACK
      2) Else if brand_desc contains token 'pk'/'Pk' (as separate token) -> PACK
      3) Else -> UNIT
    """
    code = norm(local_pack_code)
    if len(code) >= 2 and code[-2:].upper() == "PK":
        return "PACK"

    desc = " " + norm(brand_desc) + " "
    # token match: " pk " or "-pk" or "_pk" near end
    if re.search(r"(?i)(?:\bpk\b)", desc):
        return "PACK"

    return "UNIT"


def page_has_no_results(html: str) -> bool:
    return bool(re.search(r"No results found|No results were found|No matches", html, re.I))


def parse_results_rows(html: str, q_letter: str) -> Tuple[List[dict], int]:
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception as e:
        logger.error(f"BeautifulSoup parsing failed: {e}")
        return [], 0

    tbody = soup.select_one('tbody#j_id_l\\:searchResultFull_data')
    if not tbody:
        tbody = soup.find("tbody", id=re.compile(r"searchResultFull_data$"))
    if not tbody:
        return [], 0

    out: List[dict] = []
    bad_rows = 0
    for tr in tbody.select("tr"):
        tds = tr.select("td")
        if len(tds) < 9:
            bad_rows += 1
            continue

        din_a = tds[0].select_one("a[href*='detail.xhtml?drugId=']")
        local_pack_code = norm(din_a.get_text(strip=True)) if din_a else norm(tds[0].get_text(strip=True))
        din_href = din_a.get("href") if din_a else ""
        drug_id = ""
        m = re.search(r"drugId=([0-9A-Za-z]+)", din_href or "")
        if m:
            drug_id = m.group(1)

        generic = norm(tds[1].get_text(" ", strip=True))
        brand = norm(tds[2].get_text(" ", strip=True))
        mfr_code = norm(tds[3].get_text(strip=True))

        exfactory_raw = norm(tds[4].get_text(strip=True))  # "Drug Benefit Price or Unit Price"
        moh_raw = norm(tds[5].get_text(strip=True))        # "Amount MOH Pays"

        interchangeable = norm(tds[6].get_text(strip=True))
        limited_use = norm(tds[7].get_text(strip=True))
        therapeutic = norm(tds[8].get_text(" ", strip=True))

        price_type = detect_price_type(local_pack_code=local_pack_code, brand_desc=brand)

        out.append(
            {
                "q_letter": q_letter,
                "local_pack_code": local_pack_code,  # DIN/PIN/NPN on the site
                "drug_id": drug_id,
                "generic_name": generic,
                "brand_name_strength_dosage": brand,
                "mfr_code": mfr_code,

                # resolved later
                "manufacturer_name": "",

                # raw prices
                "exfactory_price_raw": exfactory_raw,
                "amount_moh_pays_raw": moh_raw,

                # flags
                "price_type": price_type,  # UNIT / PACK
                "interchangeable": interchangeable,
                "limited_use": limited_use,
                "therapeutic_notes_requirements": therapeutic,

                # derived later
                "exfactory_price": None,
                "reimbursable_price": None,
                "public_with_vat": None,
                "copay": None,
                "qa_notes": "",

                "detail_url": f"{DETAIL_URL}?drugId={drug_id}" if drug_id else "",
            }
        )

    return out, bad_rows


def validate_results_page(html: str, rows: List[dict], bad_rows: int) -> Tuple[bool, str]:
    if rows:
        bad_ratio = bad_rows / max(len(rows), 1)
        if len(rows) < PAGE_MIN_ROWS:
            return False, f"row_count_below_min ({len(rows)} < {PAGE_MIN_ROWS})"
        if bad_ratio > MAX_BAD_ROW_RATIO:
            return False, f"bad_row_ratio {bad_ratio:.2f} > {MAX_BAD_ROW_RATIO:.2f}"
        return True, "ok"

    if ALLOW_EMPTY_RESULTS and page_has_no_results(html):
        return True, "empty_ok"
    return False, "empty_without_no_results"


def fetch_results_page(session: requests.Session, q_letter: str, browser: Optional[BrowserSession]) -> Tuple[str, List[dict]]:
    last_err = None
    logger.info(f"[DEBUG] fetch_results_page start for {q_letter}")
    for attempt in range(1, PAGE_VALIDATION_RETRIES + 2):
        try:
            if USE_BROWSER and browser:
                params = {"q": q_letter, "s": "true", "type": "4"}
                url = f"{RESULTS_URL}?{urlencode(params)}"
                html = browser.get_page_html(url, label=f"results q={q_letter}")
                browser.try_search_input(q_letter)
                html = browser.driver.page_source if browser.driver else html
            else:
                logger.info(f"[DEBUG] Requesting URL: {RESULTS_URL} q={q_letter}")
                html = safe_get(session, RESULTS_URL, params={"q": q_letter, "s": "true", "type": "4"})
                logger.info(f"[DEBUG] Response received. Length: {len(html)}")

            rows, bad_rows = parse_results_rows(html, q_letter=q_letter)
            logger.info(f"[DEBUG] Parsed {len(rows)} rows, bad: {bad_rows}")
            ok, reason = validate_results_page(html, rows, bad_rows)
            if ok:
                return html, rows
            last_err = RuntimeError(f"Validation failed: {reason}")
            logger.warning(f"[Q={q_letter}] Validation failed (attempt {attempt}): {reason}")
        except Exception as exc:
            last_err = exc
            logger.warning(f"[Q={q_letter}] Fetch failed (attempt {attempt}): {exc}")
            import traceback
            logger.warning(traceback.format_exc())
        backoff_sleep(attempt)
        if USE_BROWSER and browser:
            browser.restart()
    raise RuntimeError(f"Failed to fetch valid results page for '{q_letter}': {last_err}")


def fetch_detail_page(session: requests.Session, drug_id: str, browser: Optional[BrowserSession]) -> str:
    last_err = None
    for attempt in range(1, PAGE_VALIDATION_RETRIES + 2):
        try:
            if USE_BROWSER and browser:
                url = f"{DETAIL_URL}?drugId={drug_id}"
                html = browser.get_page_html(url, label=f"detail drugId={drug_id}")
            else:
                html = safe_get(session, DETAIL_URL, params={"drugId": drug_id})
            extracted = parse_manufacturer_from_detail(html)
            if not is_bad_name(extracted):
                return html
            last_err = RuntimeError("Manufacturer not found in detail page")
            logger.warning(f"[DETAIL {drug_id}] Missing manufacturer (attempt {attempt})")
        except Exception as exc:
            last_err = exc
            logger.warning(f"[DETAIL {drug_id}] Fetch failed (attempt {attempt}): {exc}")
        backoff_sleep(attempt)
        if USE_BROWSER and browser:
            browser.restart()
    raise RuntimeError(f"Failed to fetch valid detail page for {drug_id}: {last_err}")


def parse_manufacturer_from_detail(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")

    # Typical detail table label/value pairs.
    label_td = soup.find("td", string=lambda s: isinstance(s, str) and "Manufacturer:" in s)
    if label_td:
        value_td = label_td.find_next("td")
        if value_td:
            a = value_td.select_one("a")
            if a:
                return norm(a.get_text(strip=True))
            return norm(value_td.get_text(" ", strip=True))

    # fallback regex
    m = re.search(r"Manufacturer:\s*</td>\s*<td[^>]*>\s*(?:<a[^>]*>)?([^<]+)", html, re.I)
    return norm(m.group(1)) if m else ""


def compute_prices(row: dict) -> dict:
    """
    Final calculation rule:
      reimbursable_price = Amount MOH Pays (preferred)
      public_with_vat = exfactory_price * 1.08
      copay = public_with_vat - reimbursable_price

    Here:
      exfactory_price := parsed from "Drug Benefit Price or Unit Price"
      reimbursable_price := parsed from "Amount MOH Pays" if numeric else fallback to exfactory_price
    """
    qa = []

    exf = parse_float(row.get("exfactory_price_raw", ""))
    moh = parse_float(row.get("amount_moh_pays_raw", ""))

    if exf is None:
        qa.append("exfactory_missing_or_non_numeric")

    reimb = moh if moh is not None else exf
    if reimb is None:
        qa.append("reimbursable_missing_or_non_numeric")

    public_vat = (exf * 1.08) if exf is not None else None
    copay = (public_vat - reimb) if (public_vat is not None and reimb is not None) else None

    # light QA
    if copay is not None and copay < 0:
        qa.append("copay_negative")
    if copay is not None and copay == 0:
        qa.append("copay_zero")
    if row.get("amount_moh_pays_raw", "").strip().upper() in {"N/A", "NA"}:
        qa.append("amount_moh_pays_na")

    row["exfactory_price"] = exf
    row["reimbursable_price"] = reimb
    row["public_with_vat"] = public_vat
    row["copay"] = copay
    row["qa_notes"] = ";".join(qa)

    return row


def load_existing_products(path: str) -> Tuple[pd.DataFrame, Set[str]]:
    """Load existing products and return dataframe and set of seen codes.
    When DB_ONLY, skip CSV and return empty (seen_codes come from load_seen_codes_from_db).
    """
    if DB_ONLY:
        return pd.DataFrame(), set()
    if not os.path.exists(path):
        return pd.DataFrame(), set()
    try:
        df = pd.read_csv(path, dtype=str).fillna("")
        seen = set(df["local_pack_code"].astype(str).tolist()) if "local_pack_code" in df.columns else set()
        return df, seen
    except Exception as e:
        logger.warning("Error loading existing products: %s", e)
        return pd.DataFrame(), set()


def save_products_incremental(path: str, new_rows: List[dict], existing_df: pd.DataFrame, seen_codes: Set[str]) -> Tuple[pd.DataFrame, Set[str]]:
    """Save products incrementally, merging with existing data and deduplicating"""
    if not new_rows:
        return existing_df, seen_codes
    
    # Ensure stable columns + keep numeric columns as strings in CSV (safe for Excel)
    col_order = [
        "q_letter",
        "local_pack_code",
        "drug_id",
        "generic_name",
        "brand_name_strength_dosage",
        "mfr_code",
        "manufacturer_name",
        "price_type",
        "exfactory_price_raw",
        "amount_moh_pays_raw",
        "exfactory_price",
        "reimbursable_price",
        "public_with_vat",
        "copay",
        "interchangeable",
        "limited_use",
        "therapeutic_notes_requirements",
        "qa_notes",
        "detail_url",
    ]

    def finalize(df: pd.DataFrame) -> pd.DataFrame:
        for c in col_order:
            if c not in df.columns:
                df[c] = ""
        df = df[col_order].copy()

        # Convert numeric fields to consistent string format
        for c in ["exfactory_price", "reimbursable_price", "public_with_vat", "copay"]:
            df[c] = df[c].apply(lambda x: "" if x is None or str(x).strip() == "" else f"{float(x):.4f}")
        return df

    # Create DataFrame from new rows
    new_df = pd.DataFrame(new_rows)
    new_df = finalize(new_df)

    # Merge with existing data
    if not existing_df.empty:
        existing_df = finalize(existing_df)
        # Combine and deduplicate by local_pack_code (keep first occurrence)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        before = len(combined_df)
        final_df = combined_df.drop_duplicates(subset=["local_pack_code"], keep="first")
        dropped = before - len(final_df)
        if dropped:
            logger.info("Deduped %s duplicate rows by local_pack_code", dropped)
    else:
        final_df = new_df

    # Save to CSV only when not DB-only
    if not DB_ONLY:
        final_df.to_csv(path, index=False, encoding="utf-8-sig")
    
    # Update seen codes from final dataframe
    seen_codes = set(final_df["local_pack_code"].astype(str).tolist())
    
    return final_df, seen_codes


def insert_products_to_db(rows: List[dict]):
    """Insert product rows into co_products table."""
    if not rows:
        return

    db = PostgresDB("CanadaOntario")
    db.connect()
    
    try:
        run_id = get_run_id()
        # Transform rows to match schema (includes computed price fields)
        db_rows = []
        for r in rows:
            db_rows.append((
                run_id,
                r.get("brand_name_strength_dosage", ""),
                r.get("generic_name", ""),
                r.get("manufacturer_name", ""),
                r.get("mfr_code", ""),
                r.get("local_pack_code", ""),
                r.get("exfactory_price"),
                r.get("reimbursable_price"),
                r.get("public_with_vat"),
                r.get("copay"),
                r.get("interchangeable", ""),
                r.get("limited_use", ""),  # benefit_status
                r.get("price_type", ""),
                r.get("limited_use", ""),
                r.get("therapeutic_notes_requirements", ""),
                r.get("detail_url", ""),
            ))
        
        sql_query = """
            INSERT INTO co_products 
            (run_id, product_name, generic_name, manufacturer, manufacturer_code, din, unit_price,
             reimbursable_price, public_with_vat, copay, interchangeability, benefit_status, price_type,
             limited_use, therapeutic_notes, source_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        with db.cursor() as cur:
            cur.executemany(sql_query, db_rows)
            
    finally:
        db.close()


def insert_manufacturers_to_db(mfr_master: Dict[str, str]):
    """Insert manufacturer master data into co_manufacturers table."""
    if not mfr_master:
        return

    db = PostgresDB("CanadaOntario")
    db.connect()
    
    try:
        run_id = get_run_id()
        db_rows = []
        for code, name in mfr_master.items():
            db_row = (run_id, code, name, "")  # address is empty for now
            db_rows.append(db_row)
        
        sql_query = """
            INSERT INTO co_manufacturers 
            (run_id, manufacturer_code, manufacturer_name, address)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (run_id, manufacturer_code) DO UPDATE SET
                manufacturer_name = EXCLUDED.manufacturer_name,
                address = EXCLUDED.address
        """
        
        with db.cursor() as cur:
            cur.executemany(sql_query, db_rows)
            
    finally:
        db.close()


def load_completed_letters_from_db() -> Set[str]:
    """Load completed letters from co_step_progress table."""
    completed = set()
    try:
        db = PostgresDB("CanadaOntario")
        db.connect()
        try:
            run_id = get_run_id()
            with db.cursor() as cur:
                cur.execute("""
                    SELECT progress_key FROM co_step_progress 
                    WHERE run_id = %s AND step_number = 1 
                    AND status = 'completed'
                """, (run_id,))
                rows = cur.fetchall()
                for r in rows:
                    if r[0]:
                        completed.add(r[0])
        except Exception as e:
            logger.warning(f"Error loading completed letters from DB: {e}")
        finally:
            db.close()
    except Exception as e:
        logger.warning(f"DB connection failed: {e}")
    return completed


def save_completed_letter_to_db(letter: str):
    """Mark a letter as completed in co_step_progress table."""
    try:
        db = PostgresDB("CanadaOntario")
        db.connect()
        try:
            run_id = get_run_id()
            now = datetime.now().isoformat()
            with db.cursor() as cur:
                cur.execute("""
                    INSERT INTO co_step_progress 
                    (run_id, step_number, step_name, progress_key, status, started_at, completed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id, step_number, progress_key) DO UPDATE SET
                        status = EXCLUDED.status,
                        completed_at = EXCLUDED.completed_at
                """, (run_id, 1, "Extract Product Details", letter, "completed", now, now))
        except Exception as e:
            logger.warning(f"Error saving completed letter to DB: {e}")
        finally:
            db.close()
    except Exception as e:
        logger.warning(f"DB connection failed: {e}")


def load_seen_codes_from_db() -> Set[str]:
    """Load existing DIN codes from Database."""
    seen = set()
    try:
        db = PostgresDB("CanadaOntario")
        db.connect()
        try:
            # Check if table exists
            with db.cursor() as cur:
                cur.execute("SELECT to_regclass('co_products')")
                res = cur.fetchone()
            if not res or not res[0]:
                return seen
                
            # Get all DINs from current run (or all if we want resume from any run?)
            # Usually we resume from current run or all runs?
            # For deduplication, we check global history or current run history.
            # Assuming we only care about current run or recent valid data.
            # The script logic seems to assume deduplication against valid items.
            
            # Query just the dins
            # Using execute() then fetchall() via helper or direct
            with db.cursor() as cur:
                cur.execute("SELECT din FROM co_products WHERE din IS NOT NULL")
                rows = cur.fetchall()
            
            for r in rows:
                if r[0]:
                    seen.add(str(r[0]).strip())
        except Exception as e:
            logger.warning(f"Error loading seen codes from DB: {e}")
        finally:
            db.close()
    except Exception as e:
        logger.warning(f"DB connection failed: {e}")
    return seen


def main():
    cp = get_checkpoint_manager("CanadaOntario")
    session = requests.Session()
    session.headers.update(build_headers())
    if PROXIES:
        session.proxies.update(PROXIES)
        logger.info("Proxy enabled for requests")
    logger.info("Retry config: max_retries=%s timeout=%s", RETRIES, REQUEST_TIMEOUT)
    logger.info("Jitter config: min=%s max=%s", REQUEST_JITTER_MIN, REQUEST_JITTER_MAX)
    logger.info("[DB] Database migration active - products -> co_products, manufacturers -> co_manufacturers")

    browser = None
    if USE_BROWSER:
        if not SELENIUM_AVAILABLE:
            raise RuntimeError("USE_BROWSER is enabled but Selenium is not installed.")
        browser = BrowserSession()

    # Load completed letters (resume support)
    completed_letters = load_completed_letters(cp)
    if completed_letters:
        logger.info("[RESUME] Loaded %s completed letters: %s", len(completed_letters), sorted(completed_letters))
    else:
        logger.info("[RESUME] No completed letters found")

    # Local manufacturer cache (master)
    mfr_master = load_mfr_master(MFR_MASTER_CSV)

    # Load from CSV first (legacy resume)
    existing_df, seen_codes = load_existing_products(PRODUCTS_CSV)

    # Also load from DB (modern resume)
    db_seen_codes = load_seen_codes_from_db()
    if db_seen_codes:
        logger.info(f"[RESUME] Loaded {len(db_seen_codes)} products from Database")
        seen_codes.update(db_seen_codes)
    
    logger.info(f"[RESUME] Total seen products (CSV+DB): {len(seen_codes)}")
    
    # If starting fresh (no seen codes), ensure we log that explicitly
    if not seen_codes:
         logger.info("[FRESH] No existing products found. Starting fresh scrape.")

    # Get all letters to process
    all_letters = list(string.ascii_lowercase)
    remaining_letters = [q for q in all_letters if q not in completed_letters]
    total_letters = len(all_letters)
    completed_count = len(completed_letters)

    logger.info(
        f"[RESUME] Processing {len(remaining_letters)} remaining letters out of "
        f"{total_letters} total ({completed_count} already completed)"
    )

    if not remaining_letters:
        logger.info("[DONE] All letters already processed!")
        return

    progress = ProgressReporter(total_letters=total_letters)
    letters_progress = StandardProgress(
        "canada_ontario_letters",
        total=total_letters,
        unit="letters",
        logger=logger,
        state_path=cp.checkpoint_dir / "letters_progress.json",
        log_every=1,
    )
    letters_progress.update(completed_count, message="resume", force=True)
    try:
        # Process remaining letters
        for idx, q in enumerate(remaining_letters):
            current_letter_num = completed_count + idx + 1
            logger.info(f"[Q={q}] Fetching results... (Letter {current_letter_num}/{total_letters})")

            try:
                _, rows = fetch_results_page(session, q, browser)
                logger.info(f"[Q={q}] Rows parsed: {len(rows)}")
                progress.start_rows(len(rows))
                row_progress = StandardProgress(
                    f"canada_ontario_rows_{q}",
                    total=max(len(rows), 1),
                    unit="rows",
                    logger=logger,
                    state_path=cp.checkpoint_dir / "rows_progress.json",
                    log_every=25,
                )
                row_progress.update(0, message=f"letter={q}", force=True)

                new_rows: List[dict] = []
                batch_rows: List[dict] = []
                BATCH_SIZE = 50
                new_count = 0
                skipped_count = 0

                for row in rows:
                    code = row["local_pack_code"]
                    if not code or code in seen_codes:
                        skipped_count += 1
                        progress.advance_row(1)
                        row_progress.update(skipped_count + new_count, message=f"letter={q}")
                        continue

                    # Manufacturer resolution:
                    # 1) LOCAL MASTER FIRST
                    mfr_code = row.get("mfr_code", "")
                    resolved_name = mfr_master.get(mfr_code, "") if mfr_code else ""

                    # 2) ONLY IF missing/bad -> open DIN detail page and extract Manufacturer name
                    if is_bad_name(resolved_name):
                        drug_id = row.get("drug_id", "")
                        if drug_id:
                            detail_html = fetch_detail_page(session, drug_id, browser)
                            extracted = parse_manufacturer_from_detail(detail_html)

                            if not is_bad_name(extracted):
                                resolved_name = extracted
                                if mfr_code:
                                    mfr_master[mfr_code] = extracted  # update local master

                            time.sleep(SLEEP_BETWEEN_DETAIL)

                    row["manufacturer_name"] = resolved_name if not is_bad_name(resolved_name) else ""

                    # Compute reimbursement/vat/copay
                    row = compute_prices(row)

                    new_rows.append(row)
                    batch_rows.append(row)
                    seen_codes.add(code)
                    new_count += 1
                    progress.advance_row(1)
                    row_progress.update(skipped_count + new_count, message=f"letter={q}")

                    # BATCH SAVE/INSERT
                    if len(batch_rows) >= BATCH_SIZE:
                        existing_df, seen_codes = save_products_incremental(PRODUCTS_CSV, batch_rows, existing_df, seen_codes)
                        try:
                            insert_products_to_db(batch_rows)
                            logger.info(f"[DB] Inserted batch of {len(batch_rows)} rows into co_products")
                        except Exception as e:
                            logger.error(f"[DB] Failed to insert batch: {e}")
                        batch_rows = []  # Clear batch

                row_progress.update(len(rows), message=f"letter={q} done", force=True)

                # Process remaining batch
                if batch_rows:
                    existing_df, seen_codes = save_products_incremental(PRODUCTS_CSV, batch_rows, existing_df, seen_codes)
                    try:
                        insert_products_to_db(batch_rows)
                        logger.info(f"[DB] Inserted final batch of {len(batch_rows)} rows into co_products")
                    except Exception as e:
                        logger.error(f"[DB] Failed to insert final batch: {e}")
                    batch_rows = []

                if new_rows:
                    logger.info(f"[Q={q}] Saved {new_count} new products (skipped {skipped_count} duplicates)")
                else:
                    logger.info(f"[Q={q}] No new products (all {skipped_count} were duplicates)")

                # Persist manufacturer master after each letter (DB always; CSV only when not DB_ONLY)
                save_mfr_master_to_db(mfr_master)
                if not DB_ONLY:
                    save_mfr_master(MFR_MASTER_CSV, mfr_master)

                # Mark letter as completed (DB + JSON fallback)
                completed_letters.add(q)
                save_completed_letter_to_db(q)
                save_completed_letters(completed_letters)  # Keep JSON as backup
                cp.update_metadata(
                    {
                        "completed_letters": sorted(list(completed_letters)),
                        "completed_letters_count": len(completed_letters),
                        "last_letter": q,
                    }
                )
                logger.info(f"[Q={q}] Letter completed and saved to DB")
                progress.advance_letter(1)

                letters_progress.update(len(completed_letters), message=f"letter={q}", force=True)

            except Exception as e:
                logger.error(f"[ERROR] Failed to process letter '{q}': {e}")
                logger.error(f"[ERROR] Letter '{q}' will be retried on next run")
                continue

            time.sleep(SLEEP_BETWEEN_Q)

        # Final summary
        final_df, _ = load_existing_products(PRODUCTS_CSV)
        letters_progress.update(total_letters, message="complete", force=True)

        logger.info("[DONE] All letters processed!")
        if DB_ONLY:
            try:
                db = PostgresDB("CanadaOntario")
                db.connect()
                with db.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM co_products WHERE run_id = %s", (get_run_id(),))
                    total = cur.fetchone()[0] or 0
                db.close()
                logger.info(f"[DONE] Total products in DB: {total}")
            except Exception as e:
                logger.warning(f"[DONE] Could not get product count: {e}")
        else:
            logger.info(f"[DONE] Total products: {len(final_df)}")
            logger.info(f"[DONE] Saved products: {PRODUCTS_CSV}")
        logger.info(f"[DONE] Manufacturer master: {len(mfr_master)} unique (DB)")
        if not DB_ONLY:
            logger.info(f"[DONE] Saved manufacturer master: {MFR_MASTER_CSV}")
        logger.info(f"[DONE] Completed letters: {len(completed_letters)}/{total_letters}")
        logger.info(f"[DB] Migration complete - All data saved to PostgreSQL database")
    finally:
        progress.close()
        if browser:
            browser.close()
        if CHROME_PID_TRACKING_AVAILABLE:
            repo_root = Path(__file__).resolve().parents[2]
            terminate_chrome_pids("CanadaOntario", repo_root, silent=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"FATAL ERROR in main: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
