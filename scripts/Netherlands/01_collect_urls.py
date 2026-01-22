from __future__ import annotations

import csv
import os
import re
import sys
import time
import tempfile
import shutil
import threading
import queue
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Set, Optional, Dict
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

# Add script directory to path for config_loader import
_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from config_loader import get_input_dir, get_output_dir, getenv, getenv_int, getenv_float, getenv_bool

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.common.exceptions import WebDriverException, TimeoutException, InvalidSessionIdException
import logging

# Import smart locator and state machine
from smart_locator import SmartLocator
from state_machine import NavigationStateMachine, NavigationState, StateCondition

# Import Chrome PID tracking for cleanup
try:
    from core.chrome_pid_tracker import get_chrome_pids_from_driver, save_chrome_pids, terminate_chrome_pids
    CHROME_PID_TRACKING_AVAILABLE = True
except ImportError:
    CHROME_PID_TRACKING_AVAILABLE = False
    def get_chrome_pids_from_driver(driver):
        return set()
    def save_chrome_pids(scraper_name, repo_root, pids):
        pass
    def terminate_chrome_pids(scraper_name, repo_root, silent=True):
        return 0

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    psutil = None

# Import stealth profile for anti-detection
try:
    from core.stealth_profile import apply_selenium
    STEALTH_PROFILE_AVAILABLE = True
except ImportError:
    STEALTH_PROFILE_AVAILABLE = False
    def apply_selenium(options):
        pass  # Stealth profile not available, skip

# Import browser observer for idle detection
try:
    from core.browser_observer import observe_selenium, wait_until_idle
    BROWSER_OBSERVER_AVAILABLE = True
except ImportError:
    BROWSER_OBSERVER_AVAILABLE = False
    def observe_selenium(driver):
        return None
    def wait_until_idle(state, timeout=10.0):
        pass  # Browser observer not available, skip

# Import human pacing
try:
    from core.human_actions import pause
    HUMAN_ACTIONS_AVAILABLE = True
except ImportError:
    HUMAN_ACTIONS_AVAILABLE = False
    def pause(min_s=0.2, max_s=0.6):
        pass  # Human pacing not available, skip

# Setup logger
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format='[%(levelname)s] [%(name)s] %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )


# =========================================================
# CONFIG
# =========================================================

# Base URL - can be overridden via env/config
BASE = getenv("BASE_URL", "https://www.medicijnkosten.nl")
SEARCH_URL_TEMPLATE = getenv("SEARCH_URL", f"{BASE}/zoeken?searchTerm={{kw}}")
SEARCH_URL = SEARCH_URL_TEMPLATE  # Keep for backward compatibility

# Use platform config for input/output directories
INPUT_TERMS_CSV = str(get_input_dir() / "search_terms.csv")
COLLECTED_URLS_CSV = str(get_output_dir() / "collected_urls.csv")
PACKS_CSV = str(get_output_dir() / "packs.csv")
COMPLETED_PREFIXES_CSV = str(get_output_dir() / "completed_prefixes.csv")

# Browser settings - read from env/config with fallback to defaults
HEADLESS_COLLECT = getenv_bool("HEADLESS_COLLECT", True)   # Hide browser instances
HEADLESS_SCRAPE = getenv_bool("HEADLESS_SCRAPE", True)    # Hide browser instances
PAGELOAD_TIMEOUT = getenv_int("PAGELOAD_TIMEOUT", 90)
DOM_READY_TIMEOUT = getenv_int("DOM_READY_TIMEOUT", 30)
NETWORK_RETRY_MAX = getenv_int("NETWORK_RETRY_MAX", 3)
NETWORK_RETRY_DELAY = getenv_int("NETWORK_RETRY_DELAY", 5)  # seconds

# Scroll configuration
MIN_SCROLL_LOOPS = getenv_int("MIN_SCROLL_LOOPS", 8000)
MAX_SCROLL_LOOPS = getenv_int("MAX_SCROLL_LOOPS", 15000)
MAX_STUCK_ROUNDS = getenv_int("MAX_STUCK_ROUNDS", 500)
SCROLL_WAIT_MS = getenv_int("SCROLL_WAIT_MS", 20)
SAVE_URLS_EVERY_N_NEW = getenv_int("SAVE_URLS_EVERY_N_NEW", 500)

# Behavior flags
SKIP_COMPLETED_PREFIXES = getenv_bool("SKIP_COMPLETED_PREFIXES", True)
SKIP_IF_ALREADY_SCRAPED = getenv_bool("SKIP_IF_ALREADY_SCRAPED", True)

# Retry settings
MAX_RETRIES_PER_URL = getenv_int("MAX_RETRIES_PER_URL", 1)
RETRY_BACKOFF_SECONDS = getenv_int("RETRY_BACKOFF_SECONDS", 0)  # no wait after scraping
REQUIRE_ID_FOR_SCRAPE = getenv_bool("REQUIRE_ID_FOR_SCRAPE", True)

# Financial settings
VAT_RATE = getenv_float("VAT_RATE", 0.09)
CURRENCY = getenv("CURRENCY", "EUR")

# Threading
SCRAPE_THREADS = getenv_int("SCRAPE_THREADS", 4)  # 4 threads for parallel scraping

# Dropdown toggle
INLINE_DAYS_ID = getenv("INLINE_DAYS_ID", "inline-days")
TOGGLE_TIMEOUT = getenv_int("TOGGLE_TIMEOUT", 15)
TOGGLE_SETTLE = getenv_float("TOGGLE_SETTLE", 0.35)  # small settle after change
PRICE_WAIT_TIMEOUT = getenv_int("PRICE_WAIT_TIMEOUT", 10)  # wait for correct mode price to appear


# =========================================================
# MODELS
# =========================================================
@dataclass
class PackRow:
    prefix: str
    source_url_no_id: str
    scrape_url_with_id: str

    product_group: str
    generic_name: str
    formulation: str
    strength: str
    company_name: str
    available_outside_pharmacy: str
    notes: str

    unit_price_vat: str
    pack_price_vat: str

    reimbursement_status: str
    reimbursement_message: str

    ppp_vat: str
    ppp_ex_vat: str

    local_pack_code: str

    local_pack_description: str
    local_pack_url_with_id: str
    local_pack_id: str


# =========================================================
# LOCKS
# =========================================================
_csv_lock = threading.Lock()
_update_lock = threading.Lock()
_driver_recreation_lock = threading.Lock()  # Lock to prevent simultaneous driver recreations


# =========================================================
# URL HELPERS
# =========================================================
def canonical_no_id(url: str) -> str:
    if not url:
        return ""
    try:
        u = urlparse(url)
        q = parse_qs(u.query)
        q.pop("id", None)
        flat: Dict[str, str] = {}
        for k, v in q.items():
            if v:
                flat[k] = v[0]
        return urlunparse((u.scheme, u.netloc, u.path, "", urlencode(flat), ""))
    except Exception:
        return url


def normalize_absolute(href: str) -> Optional[str]:
    if not href:
        return None
    href = href.strip()
    u = urlparse(href)
    if not u.scheme:
        return f"{BASE}{href}" if href.startswith("/") else f"{BASE}/{href}"
    return href


def is_likely_result_url(href: str) -> bool:
    h = (href or "").lower()
    if "medicijnkosten.nl" not in h:
        return False
    excluded = ["/zoeken", "/contact", "/privacy", "/about", "/login", "javascript:", "#"]
    if any(ex in h for ex in excluded):
        return False
    return ("artikel=" in h) and ("/medicijn" in h or "medicijn?" in h)


def url_with_id_for_scrape(source_url: str) -> Optional[str]:
    href = normalize_absolute(source_url)
    if not href:
        return None
    try:
        u = urlparse(href)
        q = parse_qs(u.query)
        pid = q.get("id", [None])[0]
        artikel = q.get("artikel", [None])[0]

        if pid and artikel and (u.path == "/medicijn" or "/medicijn" in u.path):
            return href

        if REQUIRE_ID_FOR_SCRAPE and not pid:
            return None

        params: Dict[str, str] = {}
        if artikel:
            params["artikel"] = artikel
        if pid:
            params["id"] = pid

        return urlunparse((u.scheme, u.netloc, "/medicijn", "", urlencode(params), ""))
    except Exception:
        return href


# =========================================================
# TEXT HELPERS
# =========================================================
def clean_single_line(text: str) -> str:
    t = (text or "").replace("\r", " ").replace("\n", " ").replace("\t", " ").replace("\xa0", " ")
    return re.sub(r"\s+", " ", t).strip()


def safe_text(el) -> str:
    try:
        return (el.text or "").strip()
    except Exception:
        return ""


def first_euro_amount(text: str) -> str:
    if not text:
        return ""
    t = clean_single_line(text)
    m = re.search(r"€\s*\d[\d\.\,]*", t)
    return m.group(0).strip() if m else ""


def euro_str_to_float(s: str) -> Optional[float]:
    if not s:
        return None
    s = s.replace("€", "").strip().replace(".", "").replace(",", ".")
    m = re.search(r"[-+]?\d+(\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None


def fmt_float(x: Optional[float]) -> str:
    if x is None:
        return ""
    return f"{x:.12f}".rstrip("0").rstrip(".")


# =========================================================
# DRIVER
# =========================================================
_driver_counter = 0


def _cleanup_old_profiles(max_age_hours: int = 24) -> None:
    try:
        tmp = tempfile.gettempdir()
        now = time.time()
        for name in os.listdir(tmp):
            if not name.startswith("chrome_profile_"):
                continue
            p = os.path.join(tmp, name)
            try:
                st = os.stat(p)
                if (now - st.st_mtime) / 3600.0 > max_age_hours:
                    shutil.rmtree(p, ignore_errors=True)
            except Exception:
                continue
    except Exception:
        pass


def make_driver(headless: bool, block_resources: bool = False, tag: str = "") -> webdriver.Chrome:
    global _driver_counter
    _cleanup_old_profiles()

    opts = Options()

    # Apply stealth profile if available
    if STEALTH_PROFILE_AVAILABLE:
        apply_selenium(opts)

    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    
    # Additional stability options to prevent connection issues
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-software-rasterizer")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--disable-backgrounding-occluded-windows")
    opts.add_argument("--disable-features=TranslateUI")
    opts.add_argument("--disable-ipc-flooding-protection")
    opts.add_argument("--enable-features=NetworkService,NetworkServiceInProcess")
    
    # User agent to avoid detection
    user_agent = getenv("CHROME_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    opts.add_argument(f"--user-agent={user_agent}")
    
    # Window size (helps with some rendering issues)
    opts.add_argument("--window-size=1920,1080")

    _driver_counter += 1
    instance_id = _driver_counter

    user_data_dir = os.path.join(
        tempfile.gettempdir(),
        f"chrome_profile_{os.getpid()}_{tag}_{instance_id}_{int(time.time() * 1000)}"
    )
    opts.add_argument(f"--user-data-dir={user_data_dir}")

    if block_resources:
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.managed_default_content_settings.cookies": 1,
        }
        opts.add_experimental_option("prefs", prefs)
    
    # Additional experimental options for stability
    opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    opts.add_experimental_option("useAutomationExtension", False)

    service = Service()
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(PAGELOAD_TIMEOUT)

    # Execute CDP commands to hide webdriver property and other automation indicators
    try:
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });

                // Override the plugins property to use a custom getter
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });

                // Override the languages property
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en', 'nl-NL', 'nl']
                });

                // Override permissions
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );

                // Mock chrome object
                window.chrome = {
                    runtime: {}
                };
            '''
        })
    except Exception:
        # CDP commands not critical, continue without them
        pass

    # Track Chrome PIDs for UI display
    try:
        from core.chrome_pid_tracker import get_chrome_pids_from_driver, save_chrome_pids
        repo_root = Path(__file__).resolve().parent.parent.parent
        scraper_name = "Netherlands"
        pids = get_chrome_pids_from_driver(driver)
        if pids:
            save_chrome_pids(scraper_name, repo_root, pids)
    except Exception:
        pass  # PID tracking not critical

    return driver


def wait_dom_ready(driver: webdriver.Chrome, timeout: int = DOM_READY_TIMEOUT) -> None:
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )
    # Use browser observer if available
    if BROWSER_OBSERVER_AVAILABLE:
        state = observe_selenium(driver)
        wait_until_idle(state, timeout=5.0)
    # Add human-like pause
    if HUMAN_ACTIONS_AVAILABLE:
        pause()


class NetworkLoadError(RuntimeError):
    pass


def _is_network_error(exc: Exception) -> bool:
    """Check if exception is a network error that should be retried.
    
    NOTE: Session errors (InvalidSessionIdException) should NOT be retried here.
    They should be handled by the worker's driver recreation logic instead.
    """
    if isinstance(exc, InvalidSessionIdException):
        # Session errors should be handled by driver recreation, not retried here
        return False
    
    msg = str(exc).lower()
    return any(
        k in msg
        for k in (
            "err_connection",
            "connection closed",
            "connection aborted",
            "connection reset",
            "name_not_resolved",
            "internet disconnected",
            "timed out",
            "timeout",
            # Note: "disconnected" without "session" is a network error
            # "invalid session id", "session deleted", "session not created" are handled above
        )
    )


def driver_get_with_retry(
    driver: webdriver.Chrome,
    url: str,
    label: str,
    max_retries: int = NETWORK_RETRY_MAX,
    base_delay: int = NETWORK_RETRY_DELAY,
) -> None:
    last_err: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            driver.get(url)
            wait_dom_ready(driver)
            return
        except (WebDriverException, TimeoutException, InvalidSessionIdException) as e:
            last_err = e
            
            # Session errors should be raised immediately - let worker handle driver recreation
            if isinstance(e, InvalidSessionIdException):
                raise  # Don't retry session errors here - worker will recreate driver
            
            if not _is_network_error(e):
                # Non-network errors (like element not found) should not be retried
                raise
            
            # Only retry network errors (not session errors)
            if attempt < max_retries:
                wait_time = base_delay * attempt
                print(f"[{label}] RETRY network error (attempt {attempt}/{max_retries}): {type(e).__name__}")
                print(f"[{label}] Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
                continue
            raise NetworkLoadError(f"Network error after {max_retries} attempts: {e}") from e
    if last_err:
        raise NetworkLoadError(f"Network error after {max_retries} attempts: {last_err}") from last_err


# =========================================================
# CSV HELPERS
# =========================================================
def ensure_csv_header(path: str, header: List[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        with _csv_lock:
            if not os.path.exists(path) or os.path.getsize(path) == 0:
                with open(path, "w", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerow(header)


def append_csv_row(path: str, row: List[str]) -> None:
    with _csv_lock:
        with open(path, "a", newline="", encoding="utf-8") as f:
            csv.writer(f, quoting=csv.QUOTE_MINIMAL).writerow(row)
            f.flush()


def update_csv_row_by_url(path: str, url_key: str, updates: Dict[str, str]) -> bool:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return False

    key_norm = canonical_no_id((url_key or "").strip())

    with _update_lock:
        try:
            rows = []
            updated = False
            with open(path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                if not fieldnames:
                    return False

                for row in reader:
                    row_url = (row.get("url", "") or "").strip()
                    row_norm = canonical_no_id(row_url)

                    if row_norm == key_norm:
                        for col, val in updates.items():
                            row[col] = val
                        updated = True
                    rows.append(row)

            if updated:
                with open(path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)

            return updated
        except Exception as e:
            print(f"[UPDATE_CSV] Error updating row for URL {url_key[:80]}...: {e}")
            return False


def load_prefixes(path: str) -> List[str]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input file not found: {path}")
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        if "prefix" not in (r.fieldnames or []):
            raise ValueError(f"{path} must contain a column named 'prefix'")
        return [row["prefix"].strip().lower() for row in r if row.get("prefix", "").strip()]


def load_completed_prefixes(path: str) -> Set[str]:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return set()
    with open(path, newline="", encoding="utf-8") as f:
        return {row["prefix"].strip().lower() for row in csv.DictReader(f) if row.get("prefix", "").strip()}


def load_existing_collected_urls(path: str) -> Set[str]:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return set()
    out: Set[str] = set()
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            u = (row.get("url", "") or "").strip()
            if u:
                out.add(canonical_no_id(u))
    return out


def load_scraped_urls(path: str) -> Set[str]:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return set()
    out: Set[str] = set()
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            u = (row.get("url", "") or "").strip()
            if not u:
                continue
            if (row.get("packs_scraped", "") or "").strip().lower() == "success":
                out.add(canonical_no_id(u))
    return out


def remove_unscraped_urls_from_collected(collected_path: str) -> int:
    if not os.path.exists(collected_path) or os.path.getsize(collected_path) == 0:
        return 0

    with _update_lock:
        rows_to_keep = []
        removed_count = 0
        try:
            with open(collected_path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                if not fieldnames:
                    return 0

                for row in reader:
                    url = (row.get("url", "") or "").strip()
                    packs_scraped = (row.get("packs_scraped", "") or "").strip().lower()
                    if url and packs_scraped == "success":
                        rows_to_keep.append(row)
                    else:
                        removed_count += 1

            if removed_count > 0:
                with open(collected_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows_to_keep)

            return removed_count
        except Exception as e:
            print(f"[CLEANUP] Error removing unscraped URLs: {e}")
            return 0


# =========================================================
# SEARCH RESULT CARD EXTRACTION
# =========================================================
def safe_find_text(parent, css: str) -> str:
    try:
        return clean_single_line(parent.find_element(By.CSS_SELECTOR, css).text)
    except Exception:
        return ""


def extract_card_fields(anchor) -> Dict[str, str]:
    return {
        "title": safe_find_text(anchor, "h3.result-title"),
        "active_substance": safe_find_text(anchor, "span.facet.active-substance"),
        "manufacturer": safe_find_text(anchor, "span.facet.manufacturer"),
        "document_type": safe_find_text(anchor, "span.document-type.byline-item"),
        "price_text": safe_find_text(anchor, "span.price.byline-item"),
        "reimbursement": safe_find_text(anchor, "span.reimbursement.byline-item"),
    }


def parse_total_results(driver: webdriver.Chrome) -> Optional[int]:
    try:
        text = driver.find_element(By.TAG_NAME, "body").text.lower()
        m = re.search(r"(\d[\d\.]*)\s+zoekresultaten", text)
        if m:
            return int(m.group(1).replace(".", ""))
    except Exception:
        pass
    return None


# =========================================================
# STEP 1: COLLECT URLS
# =========================================================
def collect_urls_for_prefix(
    driver: webdriver.Chrome,
    prefix: str,
    global_seen_no_id_urls: Set[str],
    already_collected_no_id_urls: Set[str],
    already_scraped_no_id_urls: Set[str],
) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []

    # Initialize smart locator and state machine
    locator = SmartLocator(driver, logger=logger)
    state_machine = NavigationStateMachine(locator, logger=logger)

    url = SEARCH_URL.format(kw=prefix)
    print(f"\n[{prefix}] ===== COLLECTING URLS =====")
    print(f"[{prefix}] Opening: {url}")

    driver_get_with_retry(driver, url, prefix)
    
    # Transition to PAGE_LOADED state
    if not state_machine.transition_to(NavigationState.PAGE_LOADED, reload_on_failure=True):
        print(f"[{prefix}] WARNING: Failed to reach PAGE_LOADED state")
    
    # Detect DOM changes
    locator.detect_dom_change("body", f"search_page_{prefix}")

    # Wait for anchors using smart locator
    try:
        anchors_elem = locator.find_element(css="a[href]", timeout=30.0, required=False)
        if anchors_elem is None:
            print(f"[{prefix}] WARNING: No anchors found")
    except Exception:
        # Fallback: try direct Selenium wait
        try:
            WebDriverWait(driver, 30).until(lambda d: len(d.find_elements(By.CSS_SELECTOR, "a[href]")) > 0)
        except Exception:
            print(f"[{prefix}] WARNING: No anchors found")

    expected = parse_total_results(driver)
    print(f"[{prefix}] Expected results: {expected}")

    # STEP 1: Scroll down continuously until stuck or max loops
    print(f"[{prefix}] Starting scroll...")
    stuck = 0
    last_height = 0

    for loop in range(1, MAX_SCROLL_LOOPS + 1):
        try:
            height = driver.execute_script("return document.body.scrollHeight") or 0
        except (WebDriverException, InvalidSessionIdException) as e:
            print(f"[{prefix}] ERROR: Failed to get page height (tab may have crashed): {e}")
            print(f"[{prefix}] Stopping scroll due to browser error")
            break
        
        grew = height != last_height
        
        if grew:
            stuck = 0
        else:
            stuck += 1
        
        last_height = height

        print(f"[{prefix}] Scroll loop {loop}: height={height} | stuck={stuck}")

        if loop >= MIN_SCROLL_LOOPS:
            if stuck >= MAX_STUCK_ROUNDS:
                print(f"[{prefix}] No page growth, stopping scroll")
                break

        try:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_WAIT_MS / 1000.0)
            # Add human-like pause during scrolling
            if HUMAN_ACTIONS_AVAILABLE and loop % 100 == 0:
                pause(0.1, 0.3)
        except (WebDriverException, InvalidSessionIdException) as e:
            print(f"[{prefix}] ERROR: Failed to scroll (tab may have crashed): {e}")
            print(f"[{prefix}] Stopping scroll due to browser error")
            break

    print(f"[{prefix}] Scroll complete. Extracting URLs...")
    
    # Detect DOM changes after scroll
    locator.detect_dom_change("body", f"after_scroll_{prefix}")

    # STEP 2: Extract all URLs at once after scrolling is complete
    # Use smart locator with fallback
    try:
        anchors = driver.find_elements(By.CSS_SELECTOR, "a[href]")
    except Exception:
        # Fallback: try via smart locator
        anchors_elem = locator.find_element(css="a[href]", timeout=10.0, required=False)
        anchors = [anchors_elem] if anchors_elem else []

    seen_hrefs = set()
    unique_anchors = []
    for a in anchors:
        try:
            href = a.get_attribute("href")
            if href and href not in seen_hrefs:
                seen_hrefs.add(href)
                unique_anchors.append(a)
        except Exception:
            continue

    for a in unique_anchors:
        try:
            href = a.get_attribute("href")
        except Exception:
            continue

        if not href or not is_likely_result_url(href):
            continue

        source_url_with_id = normalize_absolute(href) or ""
        if not source_url_with_id:
            continue

        source_url_no_id = canonical_no_id(source_url_with_id)
        if not source_url_no_id:
            continue

        if (
            source_url_no_id in global_seen_no_id_urls
            or source_url_no_id in already_collected_no_id_urls
            or (SKIP_IF_ALREADY_SCRAPED and source_url_no_id in already_scraped_no_id_urls)
        ):
            continue

        scrape_url = url_with_id_for_scrape(source_url_with_id)
        if REQUIRE_ID_FOR_SCRAPE and not scrape_url:
            continue

        fields = extract_card_fields(a)
        rec = {
            "prefix": prefix,
            "title": fields["title"] or "Unknown",
            "active_substance": fields["active_substance"],
            "manufacturer": fields["manufacturer"],
            "document_type": fields["document_type"],
            "price_text": fields["price_text"],
            "reimbursement": fields["reimbursement"],
            "url": source_url_no_id,          # NO-ID
            "url_with_id": scrape_url or "",  # WITH-ID
            "packs_scraped": "no",
            "error": "",
        }

        global_seen_no_id_urls.add(source_url_no_id)
        already_collected_no_id_urls.add(source_url_no_id)

        results.append(rec)
        append_csv_row(COLLECTED_URLS_CSV, [
            rec["prefix"], rec["title"], rec["active_substance"], rec["manufacturer"],
            rec["document_type"], rec["price_text"], rec["reimbursement"],
            rec["url"], rec["url_with_id"],
            rec["packs_scraped"], rec["error"]
        ])

    print(f"[{prefix}] Collection complete: {len(results)} new URLs")
    
    # Check for anomalies
    anomalies = locator.detect_anomalies(
        error_text_patterns=["error", "not found", "geen resultaten", "no results"]
    )
    if anomalies:
        logger.warning(f"[ANOMALY] {prefix}: {anomalies}")
    
    # Log metrics
    metrics = locator.get_metrics()
    metrics_summary = metrics.get_summary()
    logger.info(f"[METRICS] Collection locator performance for {prefix}: {metrics_summary}")
    
    state_history = state_machine.get_state_history()
    logger.info(f"[METRICS] Collection state transitions for {prefix}: {len(state_history)} transitions")
    
    return results


# =========================================================
# PRODUCT PAGE HELPERS
# =========================================================
def get_dd_text(driver: webdriver.Chrome, cls: str, locator: Optional[SmartLocator] = None) -> str:
    try:
        if locator:
            dd = locator.find_element(css=f"dd.{cls}", timeout=3.0, required=False)
            if dd:
                return clean_single_line(safe_text(dd))
        # Fallback to direct Selenium
        dd = driver.find_element(By.CSS_SELECTOR, f"dd.{cls}")
        return clean_single_line(safe_text(dd))
    except Exception:
        return ""


def reimbursement_banner_text(driver: webdriver.Chrome) -> str:
    texts: List[str] = []
    try:
        banners = driver.find_elements(By.CSS_SELECTOR, "dd.medicine-price div.pat-message")
        for b in banners:
            t = clean_single_line(safe_text(b))
            if t:
                texts.append(t)
    except Exception:
        pass
    return " ".join(texts).strip()


def reimbursement_status_from_banner(driver: webdriver.Chrome) -> str:
    txt = (reimbursement_banner_text(driver) or "").lower()
    if "niet vergoed" in txt or "not reimbursed" in txt:
        return "Not reimbursed"
    if "volledig vergoed" in txt or "fully reimbursed" in txt:
        return "Fully reimbursed"
    if "deels vergoed" in txt or "partially reimbursed" in txt:
        return "Partially reimbursed"
    if ("voorwaarde" in txt or "voorwaarden" in txt or "conditions" in txt) and ("vergoed" in txt or "reimbursed" in txt):
        return "Reimbursed with conditions"
    if "vergoed" in txt or "reimbursed" in txt:
        return "Reimbursed"
    return "Unknown"


def deductible_value(driver: webdriver.Chrome) -> str:
    """
    Reads the currently displayed Eigen risico value.
    """
    try:
        dts = driver.find_elements(By.CSS_SELECTOR, "dl.pat-grid-list > dt")
        for dt in dts:
            label = clean_single_line(safe_text(dt)).lower()
            if "eigen risico" in label or "deductible" in label:
                dd = dt.find_element(By.XPATH, "following-sibling::dd[1]")
                txt = clean_single_line(safe_text(dd))
                eur = first_euro_amount(txt)
                if eur:
                    return eur
                if "niets" in txt.lower() or "nothing" in txt.lower():
                    return "€ 0,00"
                return txt
    except Exception:
        pass
    return ""


def notes_text(driver: webdriver.Chrome, locator: Optional[SmartLocator] = None) -> str:
    try:
        if locator:
            dd = locator.find_element(css="dd.medicine-notes", timeout=3.0, required=False)
            if dd:
                lines = [ln.strip() for ln in safe_text(dd).splitlines() if ln.strip()]
                return "; ".join(lines)
        # Fallback to direct Selenium
        dd = driver.find_element(By.CSS_SELECTOR, "dd.medicine-notes")
        lines = [ln.strip() for ln in safe_text(dd).splitlines() if ln.strip()]
        return "; ".join(lines)
    except Exception:
        return ""


def get_local_pack_code(driver: webdriver.Chrome, locator: Optional[SmartLocator] = None) -> str:
    # RVG as fallback
    rvg = ""
    try:
        if locator:
            dd = locator.find_element(css="dd.medicine-rvg-number", timeout=3.0, required=False)
            if dd:
                rvg = clean_single_line(safe_text(dd))
        else:
            dd = driver.find_element(By.CSS_SELECTOR, "dd.medicine-rvg-number")
            rvg = clean_single_line(safe_text(dd))
    except Exception:
        rvg = ""

    # EU number preferred
    try:
        if locator:
            dts_elem = locator.find_element(css="dl.pat-grid-list > dt", timeout=3.0, required=False)
            if dts_elem:
                dts = driver.find_elements(By.CSS_SELECTOR, "dl.pat-grid-list > dt")
            else:
                dts = []
        else:
            dts = driver.find_elements(By.CSS_SELECTOR, "dl.pat-grid-list > dt")
        
        for dt in dts:
            label = clean_single_line(safe_text(dt)).lower()
            if label in ("eu number", "eu-nummer", "eu nummer"):
                dd = dt.find_element(By.XPATH, "following-sibling::dd[1]")
                eu = clean_single_line(safe_text(dd))
                if eu:
                    return eu
    except Exception:
        pass

    return rvg or ""


# =========================================================
# ✅ FIX: pat-select / pat-depends toggle + read
# =========================================================
def _dispatch_inline_days_events(driver: webdriver.Chrome, value: str) -> None:
    driver.execute_script(
        """
        const id = arguments[0];
        const val = arguments[1];
        const s = document.getElementById(id);
        if (!s) return;
        s.value = val;
        s.dispatchEvent(new Event('input', {bubbles:true}));
        s.dispatchEvent(new Event('change', {bubbles:true}));
        // Patternslib listens to pat-update in many setups:
        document.body.dispatchEvent(new Event('pat-update', {bubbles:true}));
        """,
        INLINE_DAYS_ID,
        value,
    )


def select_inline_days(driver: webdriver.Chrome, value: str, timeout: int = TOGGLE_TIMEOUT) -> bool:
    """
    Stale-safe select of #inline-days value (piece/package).
    Uses BOTH Selenium Select and JS dispatch (needed for pat-select).
    """
    end = time.time() + timeout
    while time.time() < end:
        try:
            sel_el = driver.find_element(By.ID, INLINE_DAYS_ID)

            # Try normal Select first
            try:
                Select(sel_el).select_by_value(value)
            except Exception:
                pass

            # Always dispatch JS events (pat-select needs it)
            _dispatch_inline_days_events(driver, value)

            # Wait until DOM shows the select value as expected
            WebDriverWait(driver, 5).until(
                lambda d: d.execute_script(
                    "var s=document.getElementById(arguments[0]); return s && s.value;",
                    INLINE_DAYS_ID
                ) == value
            )
            time.sleep(TOGGLE_SETTLE)
            return True
        except Exception:
            time.sleep(0.2)
            continue
    return False


def _visible_mode_price(driver: webdriver.Chrome, mode: str) -> str:
    """
    Returns the visible price for inline-days={mode} from pat-depends spans.
    IMPORTANT: We do not rely on '.visible' class only; we also check is_displayed().
    """
    spans = driver.find_elements(By.CSS_SELECTOR, f'span.pat-depends[data-pat-depends="inline-days={mode}"]')
    for sp in spans:
        try:
            if not sp.is_displayed():
                continue
            txt = clean_single_line(safe_text(sp))
            eur = first_euro_amount(txt)
            if eur:
                return eur
        except Exception:
            continue
    return ""


def read_prices_piece_and_package(driver: webdriver.Chrome) -> Dict[str, str]:
    """
    ✅ Reads the “Gemiddelde prijs per …” values from pat-depends spans.
    - package => pack_price_vat
    - piece   => unit_price_vat

    Handles pages where only one mode exists.
    """
    out = {"piece": "", "package": ""}

    # dropdown missing => only one displayed price
    if not driver.find_elements(By.ID, INLINE_DAYS_ID):
        # try whatever is currently visible as "package"
        out["package"] = _visible_mode_price(driver, "package") or _visible_mode_price(driver, "piece")
        return out

    # PACKAGE
    if select_inline_days(driver, "package"):
        try:
            WebDriverWait(driver, PRICE_WAIT_TIMEOUT).until(lambda d: _visible_mode_price(d, "package") != "")
        except Exception:
            pass
        out["package"] = _visible_mode_price(driver, "package")

    # PIECE
    if select_inline_days(driver, "piece"):
        try:
            WebDriverWait(driver, PRICE_WAIT_TIMEOUT).until(lambda d: _visible_mode_price(d, "piece") != "")
        except Exception:
            pass
        out["piece"] = _visible_mode_price(driver, "piece")

    # restore package for stable downstream fields
    select_inline_days(driver, "package")

    # if piece equals package, keep piece blank (meaning site doesn’t differentiate)
    if out["piece"] and out["package"] and out["piece"] == out["package"]:
        out["piece"] = ""

    return out


# =========================================================
# SCRAPE PRODUCT PAGE
# =========================================================
def scrape_product_to_pack(
    driver: webdriver.Chrome,
    prefix: str,
    source_url_no_id: str,
    scrape_url_with_id: str
) -> PackRow:
    # Initialize smart locator and state machine
    locator = SmartLocator(driver, logger=logger)
    state_machine = NavigationStateMachine(locator, logger=logger)
    
    driver_get_with_retry(driver, scrape_url_with_id, prefix)
    
    # Transition to PAGE_LOADED state
    if not state_machine.transition_to(NavigationState.PAGE_LOADED, reload_on_failure=True):
        print(f"[{prefix}] WARNING: Failed to reach PAGE_LOADED state for {scrape_url_with_id}")

    if "pagenotfound" in (driver.current_url or "").lower():
        raise Exception(f"Page not found: {driver.current_url}")
    
    # Detect DOM changes
    locator.detect_dom_change("body", f"detail_page_{prefix}")

    # Use smart locator to find h1 with fallback
    try:
        h1_elem = locator.find_element(css="h1", timeout=5.0, required=False)
        if h1_elem:
            product_group = clean_single_line(safe_text(h1_elem))
        else:
            product_group = clean_single_line(driver.title or "")
    except Exception:
        # Fallback to direct Selenium
        try:
            product_group = clean_single_line(safe_text(driver.find_element(By.TAG_NAME, "h1")))
        except Exception:
            product_group = clean_single_line(driver.title or "")
    
    # Add human pacing after page load
    pause()

    generic_name = get_dd_text(driver, "medicine-active-substance", locator=locator)
    formulation = get_dd_text(driver, "medicine-method", locator=locator)
    strength = get_dd_text(driver, "medicine-strength", locator=locator)
    company_name = get_dd_text(driver, "medicine-manufacturer", locator=locator)
    available = get_dd_text(driver, "available-outside-farmacy", locator=locator)
    notes = notes_text(driver, locator=locator)

    # ✅ FIXED: price comes from pat-depends after correct toggle
    prices = read_prices_piece_and_package(driver)
    unit_price_vat = prices.get("piece", "")      # per stuk
    pack_price_vat = prices.get("package", "")    # per verpakking

    reimb_status = reimbursement_status_from_banner(driver)
    reimb_message = reimbursement_banner_text(driver)

    # PPP VAT uses Eigen risico (package mode restored)
    ppp_vat = deductible_value(driver)
    ppp_vat_float = euro_str_to_float(ppp_vat)
    ppp_ex_vat = fmt_float(ppp_vat_float / (1.0 + VAT_RATE) if ppp_vat_float is not None else None)

    local_pack_code = get_local_pack_code(driver, locator=locator)

    pid = parse_qs(urlparse(scrape_url_with_id).query).get("id", [""])[0]
    
    # Check for anomalies
    anomalies = locator.detect_anomalies(
        error_text_patterns=["error", "not found", "geen resultaten", "no results"]
    )
    if anomalies:
        logger.warning(f"[ANOMALY] {prefix} {source_url_no_id}: {anomalies}")

    return PackRow(
        prefix=prefix,
        source_url_no_id=source_url_no_id,
        scrape_url_with_id=scrape_url_with_id,

        product_group=product_group,
        generic_name=generic_name,
        formulation=formulation,
        strength=strength,
        company_name=company_name,
        available_outside_pharmacy=available,
        notes=notes,

        unit_price_vat=unit_price_vat,
        pack_price_vat=pack_price_vat,

        reimbursement_status=reimb_status,
        reimbursement_message=reimb_message,

        ppp_vat=ppp_vat,
        ppp_ex_vat=ppp_ex_vat,

        local_pack_code=local_pack_code,

        local_pack_description=product_group,
        local_pack_url_with_id=scrape_url_with_id,
        local_pack_id=pid,
    )


def mark_scraped(source_url_no_id: str) -> None:
    update_csv_row_by_url(COLLECTED_URLS_CSV, source_url_no_id, {"packs_scraped": "success", "error": ""})


def mark_failed(source_url_no_id: str, err: str) -> None:
    update_csv_row_by_url(COLLECTED_URLS_CSV, source_url_no_id, {"packs_scraped": "failed", "error": (err or "")[:300]})


def mark_prefix_completed(prefix: str) -> None:
    # Normalize prefix before storing (lowercase and stripped)
    prefix_normalized = prefix.strip().lower() if prefix else ""
    append_csv_row(COMPLETED_PREFIXES_CSV, [prefix_normalized, datetime.now().isoformat(timespec="seconds")])


def reset_completed_prefixes() -> None:
    """Reset completed_prefixes.csv file (clear all entries but keep header)."""
    ensure_csv_header(COMPLETED_PREFIXES_CSV, ["prefix", "ts"])
    print("[RESET] Cleared completed_prefixes.csv - ready for second pass")


# =========================================================
# WORKER (queue-based, thread-safe)
# =========================================================
def _force_kill_chrome_processes(driver) -> None:
    """Force kill Chrome processes associated with a driver if quit() fails."""
    if not CHROME_PID_TRACKING_AVAILABLE or not PSUTIL_AVAILABLE:
        return
    
    try:
        pids = get_chrome_pids_from_driver(driver)
        if not pids:
            # Try to get PIDs from service if available
            try:
                if hasattr(driver, 'service') and hasattr(driver.service, 'process'):
                    chromedriver_pid = driver.service.process.pid
                    if chromedriver_pid:
                        pids.add(chromedriver_pid)
                        # Get all child processes
                        if PSUTIL_AVAILABLE:
                            try:
                                parent = psutil.Process(chromedriver_pid)
                                for child in parent.children(recursive=True):
                                    pids.add(child.pid)
                            except (psutil.NoSuchProcess, psutil.AccessDenied):
                                pass
            except Exception:
                pass
        
        for pid in pids:
            try:
                proc = psutil.Process(pid)
                # Try graceful termination first
                proc.terminate()
                try:
                    proc.wait(timeout=2)  # Wait up to 2 seconds
                except psutil.TimeoutExpired:
                    # Force kill if terminate didn't work
                    proc.kill()
                    try:
                        proc.wait(timeout=2)  # Wait for kill to complete
                    except psutil.TimeoutExpired:
                        pass  # Process might be stuck, but we tried
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass  # Process already gone
    except Exception:
        pass  # Don't fail if cleanup fails


def _cleanup_driver_safely(driver) -> None:
    """Safely cleanup a driver, ensuring all Chrome processes are killed."""
    if driver is None:
        return
    
    try:
        # First try normal quit
        driver.quit()
        # Give it a moment to clean up - increased wait time
        time.sleep(1.0)
    except Exception as e:
        # If quit fails, force kill Chrome processes
        _force_kill_chrome_processes(driver)
        # Give processes time to terminate - increased wait time
        time.sleep(1.0)


def scrape_worker(
    prefix: str,
    work_queue: queue.Queue,
    scraped_no_id_urls: Set[str],
    thread_id: int,
    stats: Dict[str, int]
) -> None:
    """Worker that pulls items from queue until empty."""
    driver = None
    driver_creation_count = 0
    MAX_DRIVER_RECREATIONS = 10  # Limit driver recreations per thread
    
    try:
        driver = make_driver(headless=HEADLESS_SCRAPE, block_resources=True, tag=f"scrape_t{thread_id}")
        driver_creation_count = 1
        local_done = 0
        local_skipped = 0
        local_failed = 0
        
        while True:
            try:
                # Get next item (blocking)
                r = work_queue.get(timeout=5)
            except queue.Empty:
                # Queue empty for 5 seconds - likely done, but check one more time
                try:
                    r = work_queue.get_nowait()
                except queue.Empty:
                    # Definitely empty, exit thread
                    break
            
            if r is None:  # Sentinel to stop
                work_queue.task_done()
                break
            
            source_url_no_id = canonical_no_id((r.get("url", "") or "").strip())
            scrape_url_with_id = (r.get("url_with_id", "") or "").strip()

            if not source_url_no_id:
                work_queue.task_done()
                continue

            # Fast check with minimal lock time
            should_skip = False
            if SKIP_IF_ALREADY_SCRAPED:
                with _update_lock:
                    should_skip = source_url_no_id in scraped_no_id_urls
            
            if should_skip:
                local_skipped += 1
                local_done += 1
                work_queue.task_done()
                if local_done % 10 == 0:
                    print(f"[{prefix}][T{thread_id}] Done: {local_done} | Skipped: {local_skipped} | Failed: {local_failed}")
                continue

            if REQUIRE_ID_FOR_SCRAPE and not scrape_url_with_id:
                mark_failed(source_url_no_id, "No id in url_with_id (strict mode)")
                local_failed += 1
                local_done += 1
                work_queue.task_done()
                continue

            ok = False
            last_err = ""
            
            # Ensure driver exists (should already exist from thread start)
            if driver is None:
                if driver_creation_count >= MAX_DRIVER_RECREATIONS:
                    last_err = f"Maximum driver recreations ({MAX_DRIVER_RECREATIONS}) exceeded for thread {thread_id}"
                    print(f"[{prefix}][T{thread_id}] ERROR: {last_err}")
                    mark_failed(source_url_no_id, last_err)
                    local_failed += 1
                    local_done += 1
                    work_queue.task_done()
                    continue
                
                # Use lock to prevent simultaneous driver creations
                with _driver_recreation_lock:
                    print(f"[{prefix}][T{thread_id}] WARNING: Driver is None, creating new driver (count: {driver_creation_count + 1})...")
                    driver = make_driver(headless=HEADLESS_SCRAPE, block_resources=True, tag=f"scrape_t{thread_id}")
                    driver_creation_count += 1
                    print(f"[{prefix}][T{thread_id}] Driver created (total drivers created: {driver_creation_count})")
            
            for attempt in range(1, MAX_RETRIES_PER_URL + 1):
                try:
                    # Don't validate driver proactively - let the actual scrape attempt fail if driver is invalid
                    # This avoids unnecessary driver recreations
                    
                    row = scrape_product_to_pack(driver, prefix, source_url_no_id, scrape_url_with_id)

                    append_csv_row(PACKS_CSV, [
                        row.prefix,
                        row.source_url_no_id,        # NO-ID
                        row.scrape_url_with_id,      # WITH-ID

                        row.product_group,
                        row.generic_name,
                        row.formulation,
                        row.strength,
                        row.company_name,
                        row.available_outside_pharmacy,
                        row.notes,

                        row.unit_price_vat,          # ✅ per stuk
                        row.pack_price_vat,          # ✅ per verpakking

                        row.reimbursement_status,
                        row.reimbursement_message,

                        row.ppp_vat,
                        str(VAT_RATE),
                        CURRENCY,
                        row.ppp_ex_vat,

                        row.local_pack_code,

                        row.local_pack_description,
                        row.local_pack_url_with_id,
                        row.local_pack_id,
                    ])

                    mark_scraped(source_url_no_id)
                    with _update_lock:
                        scraped_no_id_urls.add(source_url_no_id)

                    ok = True
                    break

                except (InvalidSessionIdException, WebDriverException) as e:
                    # Browser session crashed - recreate driver for next attempt
                    error_msg = str(e).lower()
                    is_session_error = (
                        "invalid session id" in error_msg or 
                        "session deleted" in error_msg or
                        "session not created" in error_msg or
                        "disconnected" in error_msg or
                        isinstance(e, InvalidSessionIdException)
                    )
                    
                    if is_session_error and attempt < MAX_RETRIES_PER_URL:
                        if driver_creation_count >= MAX_DRIVER_RECREATIONS:
                            # Max recreations reached
                            last_err = f"{type(e).__name__}: {e} (Max driver recreations reached: {MAX_DRIVER_RECREATIONS})"
                            print(f"[{prefix}][T{thread_id}] ERROR: {last_err}")
                            break  # Exit retry loop
                        
                        # Use lock to prevent simultaneous driver recreations across threads
                        with _driver_recreation_lock:
                            print(f"[{prefix}][T{thread_id}] Browser session crashed (driver #{driver_creation_count}), recreating for retry {attempt + 1}/{MAX_RETRIES_PER_URL}...")
                            old_driver = driver
                            # Clean up old driver BEFORE creating new one
                            _cleanup_driver_safely(old_driver)
                            driver = None  # Clear reference
                            # Staggered delay to prevent all threads from creating drivers simultaneously
                            # Each thread waits slightly different amount based on thread_id
                            stagger_delay = 0.5 + (thread_id * 0.2)  # 0.5s base + 0.2s per thread
                            time.sleep(stagger_delay)  # Staggered delay for better cleanup
                            driver = make_driver(headless=HEADLESS_SCRAPE, block_resources=True, tag=f"scrape_t{thread_id}")
                            driver_creation_count += 1
                            print(f"[{prefix}][T{thread_id}] Driver recreated (total drivers created: {driver_creation_count})")
                        
                        last_err = f"{type(e).__name__}: {e}"
                        if RETRY_BACKOFF_SECONDS > 0:
                            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
                        continue
                    else:
                        # Session error on last attempt, or not a session error
                        last_err = f"{type(e).__name__}: {e}"
                        if RETRY_BACKOFF_SECONDS > 0:
                            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
                
                except Exception as e:
                    last_err = f"{type(e).__name__}: {e}"
                    if RETRY_BACKOFF_SECONDS > 0:
                        time.sleep(RETRY_BACKOFF_SECONDS * attempt)

            if not ok:
                mark_failed(source_url_no_id, last_err)
                local_failed += 1

            local_done += 1
            work_queue.task_done()
            
            if local_done % 10 == 0:
                print(f"[{prefix}][T{thread_id}] Done: {local_done} | Skipped: {local_skipped} | Failed: {local_failed}")

        # Update global stats
        with _update_lock:
            stats['done'] += local_done
            stats['skipped'] += local_skipped
            stats['failed'] += local_failed
        
        print(f"[{prefix}][T{thread_id}] [OK] FINISHED | Done: {local_done} | Skipped: {local_skipped} | Failed: {local_failed} | Drivers created: {driver_creation_count}")

    except Exception as e:
        print(f"[{prefix}][T{thread_id}] [ERROR] THREAD ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if driver:
            _cleanup_driver_safely(driver)


# =========================================================
# COLLECTION PASS
# =========================================================
def run_collection_pass(pass_number: int) -> None:
    """Run one complete pass through all prefixes."""
    print("\n" + "=" * 60)
    print(f"PASS {pass_number}: COLLECTING URLs AND SCRAPING DATA")
    print("=" * 60 + "\n")
    
    # Reload state for this pass
    scraped_no_id_urls = load_scraped_urls(COLLECTED_URLS_CSV)
    completed_prefixes = load_completed_prefixes(COMPLETED_PREFIXES_CSV)
    collected_no_id_urls = load_existing_collected_urls(COLLECTED_URLS_CSV)
    
    print(f"[PASS {pass_number}] Loaded {len(scraped_no_id_urls)} scraped URLs")
    print(f"[PASS {pass_number}] Loaded {len(collected_no_id_urls)} collected URLs")
    print(f"[PASS {pass_number}] Loaded {len(completed_prefixes)} completed prefixes")
    
    prefixes = load_prefixes(INPUT_TERMS_CSV)
    print(f"[PASS {pass_number}] Loaded {len(prefixes)} prefixes from {INPUT_TERMS_CSV}")
    
    global_seen_no_id_urls: Set[str] = set()
    collect_driver = make_driver(headless=HEADLESS_COLLECT, block_resources=False, tag=f"collect_pass{pass_number}")
    skipped_count = 0
    total_prefixes = len(prefixes)
    processed_prefixes = 0
    
    try:
        if total_prefixes > 0:
            print(f"[PROGRESS] Pass {pass_number} - Collecting URLs: 0/{total_prefixes} prefixes (0.0%)", flush=True)
        
        for prefix_idx, prefix in enumerate(prefixes, 1):
            prefix_normalized = prefix.strip().lower() if prefix else ""
            
            print(f"\n[{prefix}] Pass {pass_number} - Checking prefix")
            
            # Check if prefix should be skipped
            if not SKIP_COMPLETED_PREFIXES:
                pass  # Process anyway
            elif prefix_normalized in completed_prefixes:
                skipped_count += 1
                processed_prefixes += 1
                print(f"[{prefix}] [SKIP] Skipping (already completed)")
                percent = (processed_prefixes / total_prefixes * 100) if total_prefixes > 0 else 0.0
                print(f"[PROGRESS] Pass {pass_number} - Collecting URLs: {processed_prefixes}/{total_prefixes} prefixes ({percent:.1f}%)", flush=True)
                continue
            
            try:
                collected_recs = collect_urls_for_prefix(
                    driver=collect_driver,
                    prefix=prefix,
                    global_seen_no_id_urls=global_seen_no_id_urls,
                    already_collected_no_id_urls=collected_no_id_urls,
                    already_scraped_no_id_urls=scraped_no_id_urls,
                )
            except NetworkLoadError as e:
                print(f"[{prefix}] ERROR: {e}")
                old_driver = collect_driver
                _cleanup_driver_safely(old_driver)
                collect_driver = None
                time.sleep(0.5)
                collect_driver = make_driver(headless=HEADLESS_COLLECT, block_resources=False, tag=f"collect_pass{pass_number}")
                continue
            except WebDriverException as e:
                if _is_network_error(e):
                    print(f"[{prefix}] ERROR: {e}")
                    old_driver = collect_driver
                    _cleanup_driver_safely(old_driver)
                    collect_driver = None
                    time.sleep(0.5)
                    collect_driver = make_driver(headless=HEADLESS_COLLECT, block_resources=False, tag=f"collect_pass{pass_number}")
                    continue
                raise
            
            print(f"\n[{prefix}] ===== STEP 2: SCRAPE ({SCRAPE_THREADS} threads) =====")
            if not collected_recs:
                mark_prefix_completed(prefix)
                completed_prefixes.add(prefix_normalized)
                processed_prefixes += 1
                print(f"[{prefix}] [OK] COMPLETED (no new URLs)\n")
                percent = (processed_prefixes / total_prefixes * 100) if total_prefixes > 0 else 0.0
                print(f"[PROGRESS] Pass {pass_number} - Collecting URLs: {processed_prefixes}/{total_prefixes} prefixes ({percent:.1f}%)", flush=True)
                continue
            
            # Scrape collected URLs
            work_queue = queue.Queue()
            stats = {'done': 0, 'skipped': 0, 'failed': 0}
            
            for rec in collected_recs:
                work_queue.put(rec)
            
            threads = []
            for thread_id in range(SCRAPE_THREADS):
                t = threading.Thread(
                    target=scrape_worker,
                    args=(prefix, work_queue, scraped_no_id_urls, thread_id, stats),
                    daemon=False,
                    name=f"ScrapeWorker-{thread_id}"
                )
                t.start()
                threads.append(t)
                print(f"[{prefix}] Started thread {thread_id}")
            
            work_queue.join()
            
            for _ in range(SCRAPE_THREADS):
                try:
                    work_queue.put_nowait(None)
                except queue.Full:
                    pass
            
            for t in threads:
                t.join(timeout=60)
                if t.is_alive():
                    print(f"[{prefix}] WARNING: Thread {t.name} did not finish in time")
            
            print(f"[{prefix}] Thread stats: Done={stats['done']}, Skipped={stats['skipped']}, Failed={stats['failed']}")
            
            mark_prefix_completed(prefix)
            completed_prefixes.add(prefix_normalized)
            processed_prefixes += 1
            print(f"[{prefix}] [OK] COMPLETED (scraped_total={len(scraped_no_id_urls)})\n")
            percent = (processed_prefixes / total_prefixes * 100) if total_prefixes > 0 else 0.0
            print(f"[PROGRESS] Pass {pass_number} - Collecting URLs: {processed_prefixes}/{total_prefixes} prefixes ({percent:.1f}%)", flush=True)
    
    finally:
        if 'collect_driver' in locals() and collect_driver:
            _cleanup_driver_safely(collect_driver)
    
    print(f"\n[PASS {pass_number}] COMPLETE")
    if SKIP_COMPLETED_PREFIXES:
        print(f"[PASS {pass_number}] Skipped {skipped_count} already-completed prefix(es)")


# =========================================================
# MAIN
# =========================================================
def main() -> None:
    ensure_csv_header(COLLECTED_URLS_CSV, [
        "prefix", "title", "active_substance", "manufacturer", "document_type",
        "price_text", "reimbursement", "url", "url_with_id",
        "packs_scraped", "error"
    ])

    ensure_csv_header(PACKS_CSV, [
        "prefix",
        "source_url",    # NO-ID
        "scrape_url",    # WITH-ID

        "product_group",
        "generic_name",
        "formulation",
        "strength_size",
        "company_name",
        "available_outside_pharmacy",
        "notes",

        "unit_price_vat",
        "pack_price_vat",

        "reimbursement_status",
        "reimbursement_message",

        "ppp_vat",
        "vat_rate",
        "currency",
        "ppp_ex_vat",

        "local_pack_code",

        "local_pack_description",
        "local_pack_url",
        "local_pack_id",
    ])

    ensure_csv_header(COMPLETED_PREFIXES_CSV, ["prefix", "ts"])

    print("[CLEANUP] Removing unscraped URLs from collected_urls.csv (keeping only packs_scraped='success')...")
    removed_count = remove_unscraped_urls_from_collected(COLLECTED_URLS_CSV)
    if removed_count > 0:
        print(f"[CLEANUP] Removed {removed_count} unscraped URLs (session IDs expired)")
    else:
        print("[CLEANUP] No unscraped URLs to remove")

    # PASS 1: First complete run
    run_collection_pass(1)
    
    # Reset completed_prefixes for second pass
    print("\n" + "=" * 60)
    print("RESETTING COMPLETED_PREFIXES FOR SECOND PASS")
    print("=" * 60)
    reset_completed_prefixes()
    
    # PASS 2: Second complete run (to catch any missed URLs/data)
    run_collection_pass(2)

    # Cleanup
    if CHROME_PID_TRACKING_AVAILABLE:
        try:
            repo_root = Path(__file__).resolve().parent.parent.parent
            terminated = terminate_chrome_pids("Netherlands", repo_root, silent=True)
            if terminated > 0:
                print(f"[CLEANUP] Terminated {terminated} orphaned Chrome process(es)")
        except Exception:
            pass  # Don't fail if cleanup fails

    print("\n" + "=" * 60)
    print("ALL PASSES COMPLETE - SCRAPING FINISHED")
    print("=" * 60)
    print(f"Collected URLs: {COLLECTED_URLS_CSV}")
    print(f"Packs: {PACKS_CSV}")
    print(f"Completed Prefixes: {COMPLETED_PREFIXES_CSV}")


if __name__ == "__main__":
    main()
