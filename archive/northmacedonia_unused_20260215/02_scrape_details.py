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

# Add repo root for core imports
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Shared chromedriver path to avoid concurrent downloads per thread
_driver_path = None
_driver_path_lock = threading.Lock()

# Try to import core chrome_manager for offline-capable chromedriver resolution
try:
    from core.browser.chrome_manager import get_chromedriver_path as _core_get_chromedriver_path, register_chrome_driver, unregister_chrome_driver
    CORE_CHROMEDRIVER_AVAILABLE = True
except ImportError:
    CORE_CHROMEDRIVER_AVAILABLE = False
    _core_get_chromedriver_path = None
    register_chrome_driver = None
    unregister_chrome_driver = None


def _get_chromedriver_path() -> Optional[str]:
    """Get ChromeDriver path with offline fallback support."""
    global _driver_path
    with _driver_path_lock:
        if _driver_path:
            return _driver_path
        # Use core module's offline-capable function if available
        if CORE_CHROMEDRIVER_AVAILABLE and _core_get_chromedriver_path:
            try:
                _driver_path = _core_get_chromedriver_path()
                return _driver_path
            except Exception:
                pass
        # Fallback to direct ChromeDriverManager
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
    USE_CONFIG = True
except Exception:
    OUTPUT_DIR = SCRIPT_DIR
    USE_CONFIG = False
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

# Import Chrome PID tracking utilities
try:
    from core.browser.chrome_pid_tracker import get_chrome_pids_from_driver, save_chrome_pids, terminate_scraper_pids
except Exception:
    get_chrome_pids_from_driver = None
    save_chrome_pids = None
    terminate_scraper_pids = None

# Import stealth profile for anti-detection
try:
    from core.browser.stealth_profile import apply_selenium
    STEALTH_PROFILE_AVAILABLE = True
except ImportError:
    STEALTH_PROFILE_AVAILABLE = False
    def apply_selenium(options):
        pass  # Stealth profile not available, skip

# Import browser observer for idle detection
try:
    from core.browser.browser_observer import observe_selenium, wait_until_idle
    BROWSER_OBSERVER_AVAILABLE = True
except ImportError:
    BROWSER_OBSERVER_AVAILABLE = False
    def observe_selenium(driver):
        return None
    def wait_until_idle(state, timeout=10.0):
        pass  # Browser observer not available, skip

# Import human pacing
try:
    from core.browser.human_actions import pause
    HUMAN_ACTIONS_AVAILABLE = True
except ImportError:
    HUMAN_ACTIONS_AVAILABLE = False
    def pause(min_s=0.2, max_s=0.6):
        import random
        time.sleep(random.uniform(min_s, max_s))

# Import Telegram notifier for status updates
try:
    from core.utils.telegram_notifier import TelegramNotifier
    TELEGRAM_NOTIFIER_AVAILABLE = True
except ImportError:
    TELEGRAM_NOTIFIER_AVAILABLE = False
    TelegramNotifier = None

# Import state machine and smart locator for Tier 1 robustness
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

# Navigation retry settings
NAV_RETRIES = getenv_int("SCRIPT_02_NAV_RETRIES", 3) if USE_CONFIG else 3
NAV_RETRY_SLEEP = getenv_float("SCRIPT_02_NAV_RETRY_SLEEP", 3.0) if USE_CONFIG else 3.0

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
    """Build Chrome driver with enhanced anti-bot features."""
    options = webdriver.ChromeOptions()

    # Apply stealth profile if available
    if STEALTH_PROFILE_AVAILABLE:
        apply_selenium(options)

    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1600,1000")

    # IMPORTANT: keep English UI preference so labels are likely English if site supports it,
    # but even if not, our mapping handles Macedonian.
    options.add_argument("--lang=en-US")

    # Stability improvements to prevent Chrome crashes
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-plugins")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-translate")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-sync")
    options.add_argument("--disable-default-apps")
    options.add_argument("--mute-audio")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-hang-monitor")
    options.add_argument("--disable-prompt-on-repost")
    options.add_argument("--disable-client-side-phishing-detection")
    options.add_argument("--disable-component-update")
    options.add_argument("--disable-breakpad")
    options.add_argument("--remote-debugging-port=0")

    # Enhanced anti-detection options for bot bypass
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    options.add_experimental_option('useAutomationExtension', False)

    # Use a realistic user agent
    user_agent = getenv("SCRIPT_02_CHROME_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    options.add_argument(f"--user-agent={user_agent}")

    # Speed-up (optional)
    prefs = {
        "profile.default_content_setting_values.notifications": 2,
    }
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
                    get: () => ['en-US', 'en', 'mk-MK', 'mk']
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
        pass  # CDP commands not critical, continue without them

    # Register driver with Chrome manager for cleanup
    if register_chrome_driver:
        try:
            register_chrome_driver(driver)
        except Exception:
            pass

    # Track Chrome PIDs
    if get_chrome_pids_from_driver and save_chrome_pids:
        try:
            pids = get_chrome_pids_from_driver(driver)
            if pids:
                save_chrome_pids("NorthMacedonia", _repo_root, pids)
        except Exception:
            pass
    return driver


def remove_webdriver_property(driver: webdriver.Chrome) -> None:
    """Remove webdriver property after page load for additional stealth."""
    try:
        driver.execute_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)
    except Exception:
        pass


def is_session_valid(driver: webdriver.Chrome) -> bool:
    """Check if the Chrome session is still valid and responsive."""
    try:
        _ = driver.current_url
        return True
    except Exception:
        return False


def shutdown_driver(driver: webdriver.Chrome) -> None:
    """Safely shutdown the Chrome driver."""
    if unregister_chrome_driver:
        try:
            unregister_chrome_driver(driver)
        except Exception:
            pass
    try:
        driver.quit()
    except Exception:
        pass


def parse_detail_page(driver: webdriver.Chrome) -> Dict[str, str]:
    """
    Extracts label->value for each row-fluid.
    Works for both MK and translated EN pages, even with nested <font>.
    """
    WebDriverWait(driver, WAIT_SECONDS).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.row-fluid"))
    )

    # Use browser observer if available
    if BROWSER_OBSERVER_AVAILABLE:
        state = observe_selenium(driver)
        wait_until_idle(state, timeout=5.0)

    # Add human-like pause
    if HUMAN_ACTIONS_AVAILABLE:
        pause(0.1, 0.3)

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
    telegram_notifier=None,
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
                    # Check session health before navigating
                    if not is_session_valid(driver):
                        shutdown_driver(driver)
                        driver = build_driver(headless=HEADLESS)

                    driver.get(url)
                    remove_webdriver_property(driver)

                    # Add human-like pause after navigation
                    if HUMAN_ACTIONS_AVAILABLE:
                        pause(0.2, 0.5)

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

                        # Send Telegram notification (rate-limited)
                        if telegram_notifier and done % 50 == 0:  # Send every 50 items
                            try:
                                telegram_notifier.send_progress(
                                    done,
                                    total,
                                    "Scrape Details",
                                    details=f"Workers: {DETAIL_WORKERS}"
                                )
                            except Exception:
                                pass

                    if SLEEP_BETWEEN_DETAILS > 0:
                        # Use human pacing if available, otherwise use configured sleep
                        if HUMAN_ACTIONS_AVAILABLE:
                            pause(SLEEP_BETWEEN_DETAILS * 0.5, SLEEP_BETWEEN_DETAILS * 1.5)
                        else:
                            time.sleep(SLEEP_BETWEEN_DETAILS)

                    ok = True
                    break

                except (TimeoutException, WebDriverException, StaleElementReferenceException, RuntimeError) as e:
                    last_err = e
                    if is_invalid_session(e):
                        shutdown_driver(driver)
                        try:
                            driver = build_driver(headless=HEADLESS)
                        except Exception:
                            driver = None
                    # small backoff then retry
                    time.sleep(NAV_RETRY_SLEEP * attempt)
                    continue
                except Exception as e:
                    last_err = e
                    time.sleep(NAV_RETRY_SLEEP * attempt)
                    continue

            if not ok:
                print(f"[WARN] Failed URL after retries (worker {worker_id}): {url} | err={last_err}", flush=True)
                if driver:
                    dump_failed_page(OUTPUT_DIR, url, driver, worker_id)

            q.task_done()

    finally:
        if driver:
            shutdown_driver(driver)


# -----------------------------
# MAIN
# -----------------------------
def main() -> None:
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
                telegram_notifier.send_started("Scrape Details - Step 2/4")
                print("[INFO] Telegram notifications enabled", flush=True)
        except Exception as e:
            print(f"[WARN] Failed to initialize Telegram notifier: {e}", flush=True)
            telegram_notifier = None

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

    # CRITICAL: Resume state is determined ONLY by what's in the output CSV
    # The detailed_view_scraped column in the URL list is IGNORED for skip decisions
    already_scraped = load_already_scraped_urls(output_path)

    # Build todo list: scrape anything NOT in output file
    todo_urls = [u for u in all_urls if u not in already_scraped]

    total = len(todo_urls)

    # Startup diagnostic
    print(f"\n{'='*60}", flush=True)
    print(f"[STARTUP] Detail Scraper Configuration", flush=True)
    print(f"{'='*60}", flush=True)
    print(f"URL List Path:        {urls_path}", flush=True)
    print(f"Output CSV Path:      {output_path}", flush=True)
    print(f"Total URLs in list:   {len(all_urls)}", flush=True)
    print(f"Already scraped:      {len(already_scraped)} (from output CSV)", flush=True)
    print(f"URLs to scrape:       {total}", flush=True)
    print(f"Workers:              {DETAIL_WORKERS}", flush=True)
    print(f"Headless:             {HEADLESS}", flush=True)
    print(f"{'='*60}\n", flush=True)

    # Safety check: warn if output is empty but URL list says everything is done
    if total == 0:
        if len(already_scraped) == 0 and output_path.exists():
            # Output file exists but is empty (header only), yet we have no work
            # This shouldn't happen unless URL list is also empty
            if len(all_urls) > 0:
                print(f"[WARN] Safety check failed!", flush=True)
                print(f"[WARN] - Output CSV exists but has no data (header only)", flush=True)
                print(f"[WARN] - URL list has {len(all_urls)} URLs", flush=True)
                print(f"[WARN] - But all URLs are marked as already scraped", flush=True)
                print(f"[WARN] This indicates a possible mismatch.", flush=True)
                print(f"[WARN] Suggestion: Delete {output_path.name} and rerun to force re-scrape.", flush=True)

        print("\n[INFO] No new URLs to scrape. Output already up to date.")
        return

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
            args=(wid, q, output_path, out_columns, already_scraped, seen_lock, out_lock, progress, progress_lock, telegram_notifier),
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

    # Send Telegram completion notification
    if telegram_notifier:
        try:
            failed_count = total - progress['done']
            if failed_count > 0:
                details = f"Scraped: {progress['done']}/{total}\nFailed: {failed_count}\nWorkers: {DETAIL_WORKERS}"
                telegram_notifier.send_warning("Detail Scraping Completed with Failures", details=details)
            else:
                details = f"Scraped: {progress['done']}/{total}\nWorkers: {DETAIL_WORKERS}"
                telegram_notifier.send_success("Detail Scraping Completed", details=details)
        except Exception:
            pass

    if terminate_scraper_pids:
        try:
            terminate_scraper_pids("NorthMacedonia", _repo_root, silent=True)
        except Exception:
            pass


if __name__ == "__main__":
    main()
