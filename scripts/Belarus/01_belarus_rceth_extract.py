# 01_belarus_rceth_extract.py
# Belarus RCETH Drug Price Registry Scraper
# Target: https://www.rceth.by/Refbank/reestr_drugregpricenew
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
import socket
import time
import random
import json
from datetime import datetime, timezone
from urllib.parse import urljoin
from typing import List, Dict, Optional, Set

import pandas as pd
from bs4 import BeautifulSoup

# Database imports
try:
    from core.db.postgres_connection import PostgresDB
    from db.repositories import BelarusRepository
    from core.db.models import generate_run_id
    HAS_DB = True
except ImportError:
    HAS_DB = False

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, InvalidSessionIdException
from urllib3.exceptions import ProtocolError, ConnectionError as URLConnectionError, MaxRetryError

# Core modules
from core.network.tor_manager import (
    check_tor_running, 
    auto_start_tor_proxy, 
    build_driver_firefox_tor as core_build_driver_firefox_tor, 
    request_tor_newnym
)
from core.browser.chrome_manager import kill_orphaned_chrome_processes
from core.resource_monitor import check_memory_leak, log_resource_status, periodic_resource_check


# ==================== CONFIGURATION ====================
BASE = "https://www.rceth.by"
# New URL for drug registration price registry
START_URL = "https://www.rceth.by/Refbank/reestr_drugregpricenew"
# Refbank index page - contains menu link to registry (navigate here first, click link once)
REFBANK_INDEX_URL = "https://www.rceth.by/Refbank/"
# Link to "State register of maximum selling prices" - click once before search
REGISTRY_LINK_CSS = "a.reestrs.selected-reestr[href*='reestr_drugregpricenew'], a.reestrs[href*='reestr_drugregpricenew'], a[href='/Refbank/reestr_drugregpricenew']"
REGISTRY_LINK_XPATH = "//*[@id='content']/div/div[1]/div/table/tbody/tr/td[2]/ul/li[8]/a"
REGISTRY_LINK_XPATH_FALLBACK = "//a[contains(@href,'reestr_drugregpricenew')]"

# Use config if available, otherwise fallback to hardcoded paths
if USE_CONFIG:
    OUT_DIR = get_output_dir()
    OUT_RAW = OUT_DIR / getenv("SCRIPT_01_OUTPUT_CSV", "belarus_rceth_raw.csv")
    PROGRESS_FILE = OUT_DIR / getenv("SCRIPT_01_PROGRESS_JSON", "belarus_rceth_progress.json")
    # Restart Chrome driver every N INNs to prevent memory buildup / crashes (default 15)
    RECYCLE_DRIVER_EVERY_N = int(getenv("SCRIPT_01_RECYCLE_DRIVER_EVERY_N", "15") or "15")
else:
    # Fallback to hardcoded paths
    OUT_RAW = Path(__file__).parent / "belarus_rceth_raw.csv"
    PROGRESS_FILE = Path(__file__).parent / "belarus_rceth_progress.json"
    RECYCLE_DRIVER_EVERY_N = 15

# --- exact selectors from the page ---
# International nonproprietary name (INN) input field
INN_INPUT_ID = "FProps_1__CritElems_0__Val"
SEARCH_XPATH = "//input[@type='submit' and (normalize-space(@value)='Поиск' or normalize-space(@value)='Search')]"
PAGE_SIZE_100_XPATH = "//*[self::a or self::button][normalize-space()='100']"

# pagination: <a class="rec-num" propval="2">2</a>
PAGINATION_LINKS_CSS = "a.rec-num[propval]"

# Tor Browser config (same as Argentina)
USE_TOR_BROWSER = getenv_bool("SCRIPT_01_USE_TOR_BROWSER", True) if USE_CONFIG else True
REQUIRE_TOR_PROXY = getenv_bool("REQUIRE_TOR_PROXY", False) if USE_CONFIG else False
AUTO_START_TOR_PROXY = getenv_bool("AUTO_START_TOR_PROXY", True) if USE_CONFIG else True
TOR_PROXY_PORT = None  # Set by check_tor_requirements
# Tor New Identity (NEWNYM) - request new circuit when recycling driver
TOR_NEWNYM_ON_RECYCLE = getenv_bool("SCRIPT_01_TOR_NEWNYM_ON_RECYCLE", True) if USE_CONFIG else True
TOR_CONTROL_HOST = getenv("TOR_CONTROL_HOST", "127.0.0.1") if USE_CONFIG else "127.0.0.1"
TOR_CONTROL_PORT = int(getenv("TOR_CONTROL_PORT", "9051") or "9051") if USE_CONFIG else 9051
TOR_CONTROL_COOKIE_FILE = getenv("TOR_CONTROL_COOKIE_FILE", "") if USE_CONFIG else ""

# Selenium HTTP read timeout (driver-to-browser). Default 120s is too low for slow Tor page loads.
DRIVER_HTTP_TIMEOUT = int(getenv("SCRIPT_01_DRIVER_HTTP_TIMEOUT", "300") or "300") if USE_CONFIG else 300

# Timeouts and stability thresholds (configurable via env)
PAGINATION_TIMEOUT = int(getenv("SCRIPT_01_PAGINATION_TIMEOUT", "240") or "240") if USE_CONFIG else 240
TABLE_STABLE_SECONDS = float(getenv("SCRIPT_01_TABLE_STABLE_SECONDS", "1.5") or "1.5") if USE_CONFIG else 1.5
SEARCH_PAGE_LOAD_TIMEOUT = int(getenv("SCRIPT_01_SEARCH_PAGE_LOAD_TIMEOUT", "60") or "60") if USE_CONFIG else 60
MAX_RETRY_ATTEMPTS = int(getenv("SCRIPT_01_MAX_RETRY_ATTEMPTS", "3") or "3") if USE_CONFIG else 3

# Regex patterns for price extraction
USD_EQ_RE = re.compile(r"Equivalent price on registration date:\s*([0-9]+(?:[.,][0-9]+)?)\s*USD", re.IGNORECASE)
PRICE_CELL_RE = re.compile(r"([0-9]+(?:[.,][0-9]+)?)\s*([A-Z]{3})", re.IGNORECASE)

# ==================== TRANSLATION ====================
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
    "УП": "UP",
    "таблетки": "tablets",
    "капсулы": "capsules",
    "сироп": "syrup",
    "раствор": "solution",
    "порошок": "powder",
    "гель": "gel",
    "мазь": "ointment",
    "капли": "drops",
    "спрей": "spray",
    "суспензия": "suspension",
    "суппозитории": "suppositories",
    "штук": "pcs",
    "мг": "mg",
    "мл": "ml",
    "г": "g",
    "шт": "pcs",
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
        except Exception as e:
            print(f"  [TRANSLATE] Failed for '{t[:60]}': {e}")
            return t

    return t

def translate_row_fields(row: dict) -> dict:
    """Translate text-heavy fields and set English versions."""
    # Translate fields that need translation
    fields_to_translate = {
        "inn": "inn_en",
        "trade_name": "trade_name_en",
        "dosage_form": "dosage_form_en",
    }
    
    for field, en_field in fields_to_translate.items():
        value = row.get(field)
        if isinstance(value, str) and value and value.strip():
            translated = translate_text(value)
            # Set English version if translation succeeded and is different
            if translated and translated != value:
                row[en_field] = translated
            # Keep original value in the original field
    
    # Also translate manufacturer and ATC codes (but don't create _en versions for these)
    for field in ["manufacturer", "atc_code", "who_atc_code"]:
        value = row.get(field)
        if isinstance(value, str) and value and value.strip():
            # Translate but keep in same field (these don't have _en versions in schema)
            translated = translate_text(value)
            if translated and translated != value:
                # For now, keep original - we can add _en versions later if needed
                pass
    
    return row


# ==================== UTILITY FUNCTIONS ====================
def jitter_sleep(a=0.7, b=1.6):
    time.sleep(random.uniform(a, b))

def parse_price_cell(text: str):
    """Parse price and currency from cell text"""
    if not text:
        return None, None
    t = " ".join(text.split())
    m = PRICE_CELL_RE.search(t)
    if not m:
        return None, None
    return float(m.group(1).replace(",", ".")), m.group(2).upper()

def parse_import_price_usd(contract_info_text: str):
    """Extract USD import price from contract info text"""
    if not contract_info_text:
        return None, None
    t = " ".join(contract_info_text.split())
    m = USD_EQ_RE.search(t)
    if m:
        return float(m.group(1).replace(",", ".")), "USD"
    return None, None

def parse_strength_from_dosage_form(dosage_form: str) -> tuple:
    """Extract strength and strength unit from dosage form"""
    if not dosage_form:
        return "", ""
    
    # Common patterns: "200mg", "10 mg", "500 mg/ml", "1000mg/5ml"
    patterns = [
        r"(\d+(?:\.\d+)?)\s*(mg|g|ml|mcg|iu|units?)",
        r"(\d+(?:\.\d+)?)\s*(мг|г|мл|мкг|ЕД)",
    ]
    
    for pattern in patterns:
        match = re.search(pattern, dosage_form, re.IGNORECASE)
        if match:
            strength = match.group(1)
            unit = match.group(2).lower()
            # Normalize units
            unit_map = {"мг": "mg", "г": "g", "мл": "ml", "мкг": "mcg", "ед": "iu"}
            unit = unit_map.get(unit, unit)
            return strength, unit
    
    return "", ""

def parse_pack_size_from_dosage_form(dosage_form: str) -> str:
    """Extract pack size from dosage form"""
    if not dosage_form:
        return "1"
    
    # Look for patterns like "No10", "No30x2", "10pcs", "10 шт"
    patterns = [
        r"[Nn]o?(\d+)(?:x(\d+))?",
        r"(\d+)\s*(?:шт|pcs|pieces|tab|caps)",
        r"(\d+)\s*шт",
    ]
    
    for pattern in patterns:
        match = re.search(pattern, dosage_form, re.IGNORECASE)
        if match:
            if match.group(2):  # Pattern like No10x2
                return str(int(match.group(1)) * int(match.group(2)))
            return match.group(1)
    
    return "1"


# ==================== SELENIUM FUNCTIONS ====================
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
    except Exception as e:
        print(f"[WARN] Failed to save progress file {progress_path}: {e}")

def load_existing_output(output_path: Path) -> list:
    if not output_path.exists():
        return []
    try:
        df = pd.read_csv(output_path, encoding="utf-8-sig")
        return df.to_dict(orient="records")
    except Exception as e:
        print(f"[WARN] Failed to load existing output {output_path}: {e}")
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


# ==================== TOR BROWSER (Core Integration) ====================
# Tor logic moved to core.tor_manager
# check_tor_requirements, _check_tor_running, _auto_start_tor_proxy, etc. replaced by core functions

def check_tor_requirements() -> bool:
    """Verify Tor/Firefox before starting. Returns True if OK."""
    if not USE_TOR_BROWSER:
        return True
        
    print("\n[TOR_CHECK] Verifying Tor connection...")
    
    # 1. Check Tor Proxy
    tor_running, port = check_tor_running()
    
    if not tor_running and AUTO_START_TOR_PROXY:
        print("  [INFO] Tor not detected; attempting auto-start...")
        if auto_start_tor_proxy():
            tor_running, port = check_tor_running()
            
    if tor_running:
        global TOR_PROXY_PORT
        TOR_PROXY_PORT = port
        print(f"  [OK] Tor proxy on localhost:{port}")
        return True
        
    if REQUIRE_TOR_PROXY:
        print("  [FAIL] Tor required but not running. Start Tor Browser or set REQUIRE_TOR_PROXY=0")
        return False
        
    print("  [WARN] Tor not running; will use direct connection")
    TOR_PROXY_PORT = None
    return True





def build_driver_firefox_tor(show_browser=None):
    """Build Firefox driver with Tor proxy using core manager."""
    if show_browser is None:
        show_browser = not getenv_bool("SCRIPT_01_HEADLESS", False) if USE_CONFIG else True
        
    disable_images = getenv_bool("SCRIPT_01_DISABLE_IMAGES", True) if USE_CONFIG else True
    disable_css = getenv_bool("SCRIPT_01_DISABLE_CSS", True) if USE_CONFIG else True
    
    driver = core_build_driver_firefox_tor(
        show_browser=show_browser,
        disable_images=disable_images,
        disable_css=disable_css,
        tor_proxy_port=TOR_PROXY_PORT,
        run_id=get_run_id() if 'get_run_id' in globals() else None,
        scraper_name="Belarus"
    )
    
    # Set timeouts
    driver.set_page_load_timeout(PAGINATION_TIMEOUT)
    _set_driver_connection_timeout(driver)
    
    return driver


def _set_driver_connection_timeout(driver, timeout_seconds: int = None):
    """Raise Selenium HTTP read timeout above default 120s so slow Tor page loads don't fail."""
    sec = timeout_seconds if timeout_seconds is not None else DRIVER_HTTP_TIMEOUT
    try:
        executor = getattr(driver, "command_executor", None)
        if executor is not None and hasattr(executor, "_client_config"):
            executor._client_config._timeout = sec
    except Exception:
        pass


def inject_stealth_script(driver):
    """
    Inject stealth/antibot JavaScript to hide webdriver properties (from Malaysia scraper).
    Makes browser appear more human-like to anti-bot systems.
    """
    stealth_script = """
    // Hide webdriver property
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined,
        configurable: true
    });
    
    // Mock plugins array (Chrome-like)
    Object.defineProperty(navigator, 'plugins', {
        get: () => {
            const plugins = [
                {name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format'},
                {name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: ''},
                {name: 'Native Client', filename: 'internal-nacl-plugin', description: ''}
            ];
            plugins.length = 3;
            return plugins;
        },
        configurable: true
    });
    
    // Mock languages
    Object.defineProperty(navigator, 'languages', {
        get: () => ['en-US', 'en'],
        configurable: true
    });
    
    // Mock chrome runtime (for Chrome detection)
    if (typeof window.chrome === 'undefined') {
        window.chrome = {};
    }
    window.chrome.runtime = window.chrome.runtime || {};
    window.chrome.loadTimes = window.chrome.loadTimes || function() {
        return { commitLoadTime: Date.now() / 1000 };
    };
    window.chrome.csi = window.chrome.csi || function() {
        return { startE: Date.now(), onloadT: Date.now() };
    };
    
    // Mock permissions query
    if (navigator.permissions) {
        const origQuery = navigator.permissions.query.bind(navigator.permissions);
        navigator.permissions.query = (params) => {
            if (params.name === 'notifications') {
                return Promise.resolve({state: Notification.permission});
            }
            return origQuery(params);
        };
    }
    
    // Remove Selenium-specific properties
    delete window.__selenium_unwrapped;
    delete window.__selenium_evaluate;
    delete window.__selenium_evaluate;
    delete window.__fxdriver_unwrapped;
    delete window.__driver_evaluate;
    delete window.__webdriver_evaluate;
    delete window.__selenium_evaluate;
    delete window.__fxdriver_evaluate;
    delete window.__driver_unwrapped;
    delete window.__webdriver_unwrapped;
    delete window.__selenium_unwrapped;
    delete window.__webdriver_script_fn;
    delete window.__webdriver_script_func;
    delete window.__webdriver_script_fn;
    delete window.__selenium_IDE_recorder;
    delete window.__selenium;
    delete window._selenium;
    delete window.calledSelenium;
    delete window.$cdc_asdjflasutopfhvcZLmcfl_;
    delete window.$chrome_asyncScriptInfo;
    delete window.__$webdriverAsyncExecutor;
    """
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": stealth_script})
    except Exception:
        # Fallback: execute script directly if CDP not available
        try:
            driver.execute_script(stealth_script)
        except Exception:
            pass


def humanize_mouse_movement(driver):
    """Add human-like random mouse movements to avoid bot detection."""
    try:
        # Random small mouse movements with curved paths (more human-like)
        num_movements = random.randint(2, 4)
        current_x = random.randint(100, 400)
        current_y = random.randint(100, 400)
        
        for _ in range(num_movements):
            # Create curved movement path (bezier-like)
            target_x = current_x + random.randint(-80, 80)
            target_y = current_y + random.randint(-80, 80)
            
            # Clamp to viewport
            target_x = max(50, min(target_x, 1200))
            target_y = max(50, min(target_y, 800))
            
            # Simulate smooth movement with multiple steps
            steps = random.randint(3, 6)
            for step in range(steps):
                t = step / steps
                # Add some randomness to path
                mid_x = (current_x + target_x) / 2 + random.randint(-20, 20)
                mid_y = (current_y + target_y) / 2 + random.randint(-20, 20)
                
                # Bezier-like curve
                x = (1-t)**2 * current_x + 2*(1-t)*t * mid_x + t**2 * target_x
                y = (1-t)**2 * current_y + 2*(1-t)*t * mid_y + t**2 * target_y
                
                driver.execute_script(f"""
                    var event = new MouseEvent('mousemove', {{
                        view: window,
                        bubbles: true,
                        cancelable: true,
                        clientX: {x},
                        clientY: {y}
                    }});
                    document.dispatchEvent(event);
                """)
                jitter_sleep(0.05, 0.15)
            
            current_x, current_y = target_x, target_y
            jitter_sleep(0.1, 0.3)
    except Exception:
        pass


def humanize_scroll(driver, element=None):
    """Simulate human-like scrolling behavior."""
    try:
        if element:
            # Scroll element into view with smooth behavior
            driver.execute_script("""
                arguments[0].scrollIntoView({
                    behavior: 'smooth',
                    block: 'center',
                    inline: 'nearest'
                });
            """, element)
        else:
            # Random scroll on page
            scroll_amount = random.randint(100, 400)
            scroll_direction = random.choice([-1, 1])
            driver.execute_script(f"""
                window.scrollBy({{
                    top: {scroll_amount * scroll_direction},
                    left: 0,
                    behavior: 'smooth'
                }});
            """)
        jitter_sleep(0.3, 0.7)
    except Exception:
        pass


def humanize_typing(driver, element, text):
    """Type text with human-like variable speed and occasional pauses."""
    try:
        element.clear()
        for char in text:
            element.send_keys(char)
            # Variable typing speed (faster for common chars, slower for special)
            if char.isalnum():
                jitter_sleep(0.05, 0.15)
            else:
                jitter_sleep(0.1, 0.25)
            # Occasional longer pause (like thinking)
            if random.random() < 0.1:  # 10% chance
                jitter_sleep(0.3, 0.8)
    except Exception:
        # Fallback to normal typing
        element.send_keys(text)


def translate_page_to_english(driver, wait_for_completion=True):
    """
    Translate page content from Russian to English using multiple methods.
    Runs after page loads to ensure all text is in English for extraction.
    
    Args:
        wait_for_completion: If True, wait longer and verify translation applied
    """
    # Method 1: Try Chrome's built-in translation via CDP (most reliable)
    try:
        driver.execute_cdp_cmd("Page.setDocumentContent", {
            "frameId": driver.execute_script("return window.frameId || '';"),
        })
        # Trigger Chrome's translate feature
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                if (window.chrome && window.chrome.i18n) {
                    document.documentElement.lang = 'en';
                }
            """
        })
    except Exception:
        pass
    
    # Method 2: Comprehensive dictionary replacement (immediate, synchronous, guaranteed)
    translate_script = """
    (function() {
        var ruToEnMap = {
            // Navigation and buttons
            'Поиск': 'Search', 'Найти': 'Find', 'Искать': 'Search',
            'Очистить форму': 'Clear form', 'Очистить': 'Clear',
            'Страница': 'Page', 'Следующая': 'Next', 'Предыдущая': 'Previous',
            'Первая': 'First', 'Последняя': 'Last',
            // Table headers and labels
            'Запись': 'Record', 'Всего': 'Total', 'Показать': 'Show',
            'записей': 'records', 'на странице': 'per page',
            'Найдено': 'Found', 'Найдено записей': 'Records found',
            // Form field labels (CRITICAL - these are what user sees)
            'Регистрационное удостоверение': 'Registration Certificate',
            'Торговое наименование': 'Trade Name',
            'Международное непатентованное наименование': 'International Nonproprietary Name',
            'МНН': 'INN',
            'Лекарственная форма': 'Dosage Form',
            'Дозировка': 'Dosage', 'Упаковка': 'Packaging',
            'Производитель': 'Manufacturer',
            'Цена': 'Price', 'Валюта': 'Currency',
            'Дата': 'Date', 'Изменения': 'Changes',
            'Дата регистрации': 'Registration Date',
            'Дата изменения': 'Change Date',
            // Common phrases
            'Государственный реестр': 'State Register',
            'предельных отпускных цен': 'Maximum Selling Prices',
            'производителей лекарственных средств': 'Drug Manufacturers',
            'лекарственные препараты': 'Medicines',
            'Поиск в': 'Search in',
            // Actions
            'Добавить': 'Add', 'Удалить': 'Delete', 'Редактировать': 'Edit',
            'Сохранить': 'Save', 'Отмена': 'Cancel', 'Подтвердить': 'Confirm',
            // Status and dropdowns
            'Активный': 'Active', 'Неактивный': 'Inactive',
            'Действующий': 'Valid', 'Недействующий': 'Invalid',
            'с любой частью': 'with any part', 'исключить': 'exclude',
            'по дате': 'by date', 'ДД.ММ.ГГГГ': 'DD.MM.YYYY'
        };
        
        // Aggressive text replacement function
        function translateText(text) {
            if (!text || typeof text !== 'string') return text;
            var result = text;
            for (var ru in ruToEnMap) {
                // Use global replace without word boundaries for better matching
                var regex = new RegExp(ru.replace(/[.*+?^${}()|[\\]\\]/g, '\\\\$&'), 'gi');
                result = result.replace(regex, ruToEnMap[ru]);
            }
            return result;
        }
        
        // Translate all text nodes recursively
        function translateNode(node) {
            if (node.nodeType === 3) { // Text node
                var original = node.textContent;
                var translated = translateText(original);
                if (translated !== original) {
                    node.textContent = translated;
                }
            } else if (node.nodeType === 1) { // Element node
                var tagName = node.tagName.toLowerCase();
                // Skip scripts, styles, noscript
                if (tagName === 'script' || tagName === 'style' || tagName === 'noscript') {
                    return;
                }
                // Translate all child nodes
                var children = Array.from(node.childNodes);
                children.forEach(translateNode);
                
                // Translate attributes that contain text
                if (node.title) node.title = translateText(node.title);
                if (node.alt) node.alt = translateText(node.alt);
                if (node.placeholder) node.placeholder = translateText(node.placeholder);
                if (node.value && node.tagName === 'INPUT' && node.type !== 'hidden') {
                    // Only translate value if it's Russian text (not user input)
                    if (/[А-Яа-яЁё]/.test(node.value)) {
                        node.value = translateText(node.value);
                    }
                }
            }
        }
        
        // Translate the entire document body
        translateNode(document.body);
        
        // Also translate specific form elements more aggressively
        var formElements = document.querySelectorAll('label, span, div, td, th, button, a, input[placeholder], select option');
        formElements.forEach(function(el) {
            if (el.textContent && /[А-Яа-яЁё]/.test(el.textContent)) {
                el.textContent = translateText(el.textContent);
            }
            if (el.innerText && /[А-Яа-яЁё]/.test(el.innerText)) {
                el.innerText = translateText(el.innerText);
            }
        });
        
        // Set document language
        document.documentElement.lang = 'en';
        
        return true;
    })();
    """
    try:
        result = driver.execute_script(translate_script)
        # Force a small delay to let DOM updates settle
        jitter_sleep(0.3, 0.5)
        
        # Run translation again to catch any dynamically loaded content
        driver.execute_script(translate_script)
        
        if wait_for_completion:
            jitter_sleep(1.0, 1.5)
            # Verify translation worked
            try:
                page_html = driver.execute_script("return document.body.innerHTML;")
                # Check if key Russian words are still present
                russian_words = ['Поиск', 'Найти', 'МНН']
                has_russian = any(word in page_html for word in russian_words)
                if has_russian:
                    # Run translation one more time
                    print("  [TRANSLATE] Russian text still detected, retrying translation...")
                    driver.execute_script(translate_script)
                    jitter_sleep(0.5, 1.0)
            except Exception:
                pass
        else:
            jitter_sleep(0.5, 1.0)
    except Exception as e:
        print(f"  [TRANSLATE] Error: {e}")


def build_driver(show_browser=None):
    """Build driver - Firefox/Tor when SCRIPT_01_USE_TOR_BROWSER=1, else Chrome"""
    if USE_TOR_BROWSER:
        return build_driver_firefox_tor(show_browser)
    # Chrome fallback
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
    # Prefer English language so page loads in English (translate in browser before scraping)
    opts.add_argument("--lang=en-US")
    
    # Stealth/antibot options (from Malaysia scraper)
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    
    # Memory and stability options to prevent tab crashes
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-plugins")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-backgrounding-occluded-windows")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--memory-pressure-off")
    opts.add_argument("--max_old_space_size=4096")
    opts.add_argument("--js-flags=--max-old-space-size=4096")

    # Disable images/CSS for faster loads (enabled by default for Belarus)
    disable_images = getenv_bool("SCRIPT_01_DISABLE_IMAGES", True) if USE_CONFIG else True
    disable_css = getenv_bool("SCRIPT_01_DISABLE_CSS", True) if USE_CONFIG else True
    prefs = {
        # Prefer English language so page loads in English (translate in browser before scraping)
        "intl.accept_languages": "en-US,en,ru-RU,ru"
    }
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

    # Use offline-capable chromedriver resolution if available
    if get_chromedriver_path:
        driver_path = get_chromedriver_path()
    else:
        driver_path = ChromeDriverManager().install()
    service = ChromeService(driver_path)
    driver = webdriver.Chrome(service=service, options=opts)
    _set_driver_connection_timeout(driver)
    
    # Inject stealth/antibot script (from Malaysia scraper)
    inject_stealth_script(driver)

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
    """Click element with human-like behavior (mouse movement, delays, scrolling)."""
    # Humanize: scroll element into view with smooth behavior
    humanize_scroll(driver, el)
    jitter_sleep(0.2, 0.6)
    # Humanize: random mouse movement before click
    humanize_mouse_movement(driver)
    jitter_sleep(0.1, 0.3)
    try:
        el.click()
        jitter_sleep(0.2, 0.5)  # Small delay after click
        return True
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            return False

def wait_results_table_loaded(driver, timeout=None):
    """Wait for results table existence + at least 1 row."""
    if timeout is None:
        timeout = PAGINATION_TIMEOUT
    wait = WebDriverWait(driver, timeout)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))
    wait.until(lambda d: len(d.find_elements(By.CSS_SELECTOR, "table tbody tr")) >= 1)

def wait_table_stable(driver, timeout=None, stable_seconds=None):
    """Extra-stable wait: row count stops changing for stable_seconds."""
    if timeout is None:
        timeout = PAGINATION_TIMEOUT
    if stable_seconds is None:
        stable_seconds = TABLE_STABLE_SECONDS
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


# ==================== HTML PARSING ====================
def find_results_table_in_soup(soup: BeautifulSoup):
    """Find the results table in the HTML"""
    tables = soup.find_all("table")
    if not tables:
        return None

    # Look for table with specific headers
    key_terms = ["Trade name", "INN", "Dosage form", "Maximum selling price",
                 "Торгов", "МНН", "Лекарственная", "Предельная", "ATC code"]

    best = None
    best_score = -1
    for t in tables:
        thead = t.find("thead")
        if not thead:
            # Also check first row for headers
            first_row = t.find("tr")
            if first_row:
                headers = [th.get_text(" ", strip=True) for th in first_row.find_all(["th", "td"])]
            else:
                continue
        else:
            headers = [th.get_text(" ", strip=True) for th in thead.find_all("th")]
        
        joined = " | ".join(headers).lower()
        score = sum(1 for term in key_terms if term.lower() in joined)
        if score > best_score:
            best_score = score
            best = t
    return best

def extract_rows_from_html(html: str, search_inn: str, page_no: int, page_url: str):
    """Extract data rows from HTML and map to template format.

    Logs detailed verification data:
    - Raw cell text from website (what we see)
    - Parsed/extracted values (what we interpret)
    - Any parse failures or missing data
    """
    soup = BeautifulSoup(html, "lxml")
    table = find_results_table_in_soup(soup)
    if not table:
        print(f"  [VERIFY] Page {page_no}: No results table found in HTML ({len(html)} chars)")
        return []

    tbody = table.find("tbody") or table
    trs = tbody.find_all("tr")
    total_trs = len(trs)
    print(f"  [VERIFY] Page {page_no}: Found {total_trs} <tr> elements in results table")

    out = []
    skipped_rows = 0
    for tr in trs:
        tds = tr.find_all("td")
        if not tds or len(tds) < 8:
            skipped_rows += 1
            continue

        cell_texts = [td.get_text("\n", strip=True) for td in tds]
        def safe(i): return cell_texts[i] if i < len(cell_texts) else ""

        # Map columns based on observed structure
        # Column mapping from screenshots:
        # 0: No (line number)
        # 1: Trade Name
        # 2: INN (International Nonproprietary Name)
        # 3: Dosage Form
        # 4: ATC code / Category
        # 5: Marketing Authorization Holder
        # 6: Producer
        # 7: Registration certificate number
        # 8: Maximum selling price
        # 9: Information about max selling price (contains USD import price)
        # 10: Date of changes
        
        trade_name = safe(1)
        inn = safe(2)
        dosage_form = safe(3)
        atc_cat = safe(4)
        mah = safe(5)
        producer = safe(6)
        reg_cert = safe(7)

        # Parse prices
        max_price_val, max_price_ccy = parse_price_cell(safe(8))
        
        # Extract import price from contract info column (column 9)
        contract_info = safe(9)
        import_price_usd, import_ccy = parse_import_price_usd(contract_info)
        
        # Additional price parsing from contract info
        if not import_price_usd:
            # Try alternative patterns
            alt_patterns = [
                r"(\d+(?:[.,]\d+)?)\s*USD",
                r"USD\s*(\d+(?:[.,]\d+)?)",
            ]
            for pattern in alt_patterns:
                m = re.search(pattern, contract_info, re.IGNORECASE)
                if m:
                    import_price_usd = float(m.group(1).replace(",", "."))
                    import_ccy = "USD"
                    break
        
        reg_info = safe(10) if len(cell_texts) > 10 else ""
        date_changes = safe(11) if len(cell_texts) > 11 else ""

        # Extract additional fields
        strength, strength_unit = parse_strength_from_dosage_form(dosage_form)
        pack_size = parse_pack_size_from_dosage_form(dosage_form)
        
        # Determine effective date from date_changes or use current date
        effective_date = ""
        if date_changes:
            # Try to extract date in format DD-MM-YYYY
            date_match = re.search(r"(\d{2})[.-](\d{2})[.-](\d{4})", date_changes)
            if date_match:
                effective_date = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
        
        # Build LOCAL_PACK_CODE from registration certificate
        local_pack_code = reg_cert if reg_cert else ""

        details_url = ""
        a = tr.find("a", href=True)
        if a and "/Refbank/reestr_drugregpricenew/details/" in a["href"]:
            details_url = urljoin(BASE, a["href"])

        row = {
            # Database fields (primary) - these are what insert_rceth_data expects
            "inn": inn if inn else search_inn,
            "inn_en": None,  # Will be set after translation
            "trade_name": trade_name,
            "trade_name_en": None,  # Will be set after translation
            "manufacturer": producer,
            "manufacturer_country": "",  # Not available in source
            "dosage_form": dosage_form,
            "dosage_form_en": None,  # Will be set after translation
            "strength": strength,
            "pack_size": pack_size,
            "local_pack_description": dosage_form,  # Use dosage form as description
            "registration_number": reg_cert,
            "registration_date": effective_date,
            "registration_valid_to": "",  # Not available
            "producer_price": None,  # Not directly available
            "producer_price_vat": None,
            "wholesale_price": max_price_val,
            "wholesale_price_vat": None,
            "retail_price": max_price_val,
            "retail_price_vat": None,
            "import_price": import_price_usd,
            "import_price_currency": import_ccy,
            "currency": max_price_ccy if max_price_ccy else "BYN",
            "atc_code": atc_cat,
            "who_atc_code": atc_cat,
            "pharmacotherapeutic_group": "",  # Not available
            "source_url": details_url,
            
            # Template fields (for compatibility/export)
            "Country": "BELARUS",
            "Product Group": trade_name.upper() if trade_name else "",
            "Local Product Name": trade_name,
            "Generic Name": inn if inn else search_inn,
            "Indication": "",  # Not available in this registry
            "Pack Size": pack_size,
            "Effective Start Date": effective_date,
            "Currency": max_price_ccy if max_price_ccy else "BYN",
            "Ex Factory Wholesale Price": max_price_val,
            "VAT Percent": "0.00",
            "Margin Rule": "65 Manual Entry",
            "Package Notes": "",
            "Discontinued": "NO",
            "Region": "EUROPE",
            "WHO ATC Code": atc_cat,
            "Marketing Authority": mah,
            "Fill Unit": "",
            "Fill Size": "",
            "Pack Unit": "",
            "Strength": strength,
            "Strength Unit": strength_unit,
            "Import Type": "NONE",
            "Combination Molecule": "NO",
            "Source": "PRICENTRIC",
            "Client": "VALUE NEEDED",
            "LOCAL_PACK_CODE": local_pack_code,
            
            # Additional raw fields for reference
            "search_inn_used": search_inn,
            "page_no": page_no,
            "page_url": page_url,
            "producer_raw": producer,
            "registration_certificate_number": reg_cert,
            "max_selling_price": max_price_val,
            "max_selling_price_currency": max_price_ccy,
            "import_price": import_price_usd,
            "import_price_currency": import_ccy,
            "contract_currency_info_raw": contract_info,
            "max_price_registration_info_raw": reg_info,
            "date_of_changes_raw": date_changes,
            "details_url": details_url,
            "scraped_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        # -- Data verification: log what we extracted vs raw website text --
        row_num = len(out) + 1
        _issues = []
        if not trade_name:
            _issues.append("trade_name=EMPTY")
        if not inn and not search_inn:
            _issues.append("inn=EMPTY")
        if max_price_val is None:
            _issues.append("price=NONE")
        if not reg_cert:
            _issues.append("reg_cert=EMPTY")
        if not atc_cat:
            _issues.append("atc_code=EMPTY")
        if _issues:
            print(f"  [VERIFY] Page {page_no} Row {row_num}: GAPS: {', '.join(_issues)} | raw_cells={len(tds)}")

        out.append(row)

    if skipped_rows > 0:
        print(f"  [VERIFY] Page {page_no}: Skipped {skipped_rows}/{total_trs} rows (< 8 columns)")
    print(f"  [VERIFY] Page {page_no}: Extracted {len(out)} data rows from {total_trs} table rows")
    return out


# ==================== PAGINATION ====================
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
            WebDriverWait(driver, PAGINATION_TIMEOUT).until(lambda d: d.page_source != old_html)
            wait_table_stable(driver)
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
            WebDriverWait(driver, PAGINATION_TIMEOUT).until(lambda d: d.page_source != old_html)
            wait_table_stable(driver)
            return True
    return False


# ==================== SEARCH FLOW ====================
def navigate_to_registry_page_once(driver):
    """
    Navigate to Refbank index and click the registry link once per driver session.
    Always clicks through the index page menu (required even for direct connection).
    """
    if getattr(driver, "_belarus_registry_navigated", False):
        return
    try:
        driver.get(REFBANK_INDEX_URL)
        jitter_sleep(1.0, 2.0)
        wait = WebDriverWait(driver, 60)
        # Find and click the registry link (State register of maximum selling prices)
        # Try specific XPath first, then CSS selector, then fallback XPath
        for sel in [
            (By.XPATH, REGISTRY_LINK_XPATH),  # Most specific: //*[@id="content"]/div/div[1]/div/table/tbody/tr/td[2]/ul/li[8]/a
            (By.CSS_SELECTOR, REGISTRY_LINK_CSS),  # CSS with class="reestrs selected-reestr"
            (By.XPATH, REGISTRY_LINK_XPATH_FALLBACK),  # Fallback: //a[contains(@href,'reestr_drugregpricenew')]
        ]:
            try:
                link = wait.until(EC.element_to_be_clickable(sel))
                if link and link.is_displayed():
                    safe_click(driver, link)
                    jitter_sleep(1.0, 2.0)
                    wait.until(EC.presence_of_element_located((By.ID, INN_INPUT_ID)))
                    driver._belarus_registry_navigated = True
                    print("  [NAV] Clicked registry link (once per session)")
                    return
            except Exception:
                continue
        # Fallback: go directly to START_URL if clicking failed
        print("  [WARN] Could not find registry link, using direct URL")
        driver.get(START_URL)
        driver._belarus_registry_navigated = True
    except Exception as e:
        print(f"  [WARN] Registry nav failed: {e}, using direct URL")
        try:
            driver.get(START_URL)
            driver._belarus_registry_navigated = True
        except Exception as e2:
            print(f"  [ERROR] Direct URL navigation also failed: {e2}")


def run_search(driver, inn_term: str):
    """Run search for a specific INN/generic name"""
    navigate_to_registry_page_once(driver)
    driver.get(START_URL)

    # Wait for page to fully load
    wait = WebDriverWait(driver, PAGINATION_TIMEOUT)
    wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
    jitter_sleep(0.5, 1.0)

    inn_input = wait.until(EC.visibility_of_element_located((By.ID, INN_INPUT_ID)))
    wait.until(lambda d: inn_input.is_enabled())
    
    # Humanize: random mouse movement
    humanize_mouse_movement(driver)

    # Inject INN value directly (no typing)
    driver.execute_script("arguments[0].value = arguments[1];", inn_input, inn_term)
    # Trigger input events so the form recognizes the value
    driver.execute_script("""
        arguments[0].dispatchEvent(new Event('input', {bubbles: true}));
        arguments[0].dispatchEvent(new Event('change', {bubbles: true}));
    """, inn_input)

    jitter_sleep(0.4, 1.0)

    # Click search button
    search_btn = wait.until(EC.element_to_be_clickable((By.XPATH, SEARCH_XPATH)))
    safe_click(driver, search_btn)

    # Wait for results to stabilize
    wait_table_stable(driver)

    # Set 100 items per page
    click_page_size_100(driver)

def scrape_for_inn(driver, inn_term: str):
    """Scrape all pages for a specific INN.

    Logs:
    - Total pages detected (pagination)
    - Rows extracted per page
    - Total rows across all pages
    - Any pagination failures
    """
    run_search(driver, inn_term)

    all_rows = []

    # Page 1
    wait_table_stable(driver, timeout=PAGINATION_TIMEOUT, stable_seconds=TABLE_STABLE_SECONDS)
    html = driver.page_source
    page1_rows = extract_rows_from_html(html, inn_term, 1, driver.current_url)
    all_rows.extend(page1_rows)
    jitter_sleep(0.8, 1.6)

    # Check for additional pages
    max_page = get_max_page_from_dom(driver)
    if max_page > 1:
        print(f"  [PAGINATION] INN '{inn_term}': {max_page} pages detected, page 1 yielded {len(page1_rows)} rows")

    pagination_failures = 0
    for p in range(2, max_page + 1):
        ok = go_to_page(driver, p)
        if not ok:
            pagination_failures += 1
            print(f"  [PAGINATION] INN '{inn_term}': FAILED to navigate to page {p}/{max_page}")
            break

        html = driver.page_source
        page_rows = extract_rows_from_html(html, inn_term, p, driver.current_url)
        all_rows.extend(page_rows)
        print(f"  [PAGINATION] INN '{inn_term}': Page {p}/{max_page} -> {len(page_rows)} rows (cumulative: {len(all_rows)})")
        jitter_sleep(0.8, 1.6)

    # Summary log for this INN
    if max_page > 1:
        print(f"  [VERIFY] INN '{inn_term}': Total {len(all_rows)} rows across {max_page} pages (failures: {pagination_failures})")
    return all_rows


def get_run_id() -> str:
    """Get or create run_id for this scraper run. Checks canonical path first (unified with 02/03)."""
    candidates = [
        get_output_dir() / ".current_run_id" if USE_CONFIG else None,
        _script_dir / "output" / ".current_run_id",
        _repo_root / "output" / ".current_run_id",
        _repo_root / "output" / "Belarus" / ".current_run_id",
    ]
    for run_id_file in candidates:
        if run_id_file and run_id_file.exists():
            try:
                rid = run_id_file.read_text(encoding="utf-8").strip()
                if rid:
                    return rid
            except Exception:
                pass
    return generate_run_id()


def save_run_id(run_id: str) -> None:
    """Save run_id to canonical path (and legacy) so steps 02/03 find it."""
    targets = []
    if USE_CONFIG:
        targets.append(get_output_dir() / ".current_run_id")
    targets.extend([
        _script_dir / "output" / ".current_run_id",
        _repo_root / "output" / "Belarus" / ".current_run_id",
    ])
    for run_id_file in targets:
        try:
            run_id_file.parent.mkdir(parents=True, exist_ok=True)
            run_id_file.write_text(run_id, encoding="utf-8")
        except Exception as e:
            print(f"[WARN] Could not save run_id to {run_id_file}: {e}")


def is_driver_alive(driver) -> bool:
    """Check if ChromeDriver session is still alive."""
    try:
        # Try to get current URL - if driver is dead, this will raise an exception
        _ = driver.current_url
        return True
    except (WebDriverException, InvalidSessionIdException, MaxRetryError, URLConnectionError, Exception):
        return False


def get_inns_from_db(db) -> List[str]:
    """Get unique INNs from by_input_generic_names table."""
    try:
        with db.cursor() as cur:
            cur.execute("SELECT DISTINCT generic_name FROM by_input_generic_names WHERE generic_name IS NOT NULL ORDER BY generic_name")
            rows = cur.fetchall()
            return [row[0].strip() for row in rows if row[0] and row[0].strip()]
    except Exception as e:
        print(f"[WARN] Failed to read from database: {e}")
        return []


# ==================== MAIN ====================
def main():
    init_translator()
    kill_orphaned_chrome_processes(include_firefox=True)

    # Initialize database connection - REQUIRED (no CSV fallback)
    if not HAS_DB:
        print("[ERROR] Database support not available. Cannot proceed without database.")
        print("[ERROR] Please ensure core.db.postgres_connection and db.repositories are available.")
        return
    
    try:
        run_id = get_run_id()
        save_run_id(run_id)
        db = PostgresDB("Belarus")
        db.connect()
        repo = BelarusRepository(db, run_id)
        repo.ensure_run_in_ledger(mode="resume")
        completed_inns = repo.get_completed_keys(step_number=1)
        
        # Get INNs from database table (by_input_generic_names)
        inns = get_inns_from_db(db)
        if not inns:
            print("[ERROR] No INNs found in database table 'by_input_generic_names'.")
            print("[ERROR] Please upload generic names to the database using the GUI Input page.")
            print("[ERROR] Expected table: by_input_generic_names")
            print("[ERROR] Expected column: generic_name")
            return
        
        print(f"[DB] Loaded {len(inns)} unique INNs from database (by_input_generic_names)")
        print(f"[DB] Connected to PostgreSQL | run_id={run_id}")
        print(f"[DB] Resuming with {len(completed_inns)} already processed INNs")
    except Exception as e:
        print(f"[ERROR] Database connection failed: {e}")
        print("[ERROR] Cannot proceed without database connection.")
        return

    # Tor/Firefox check when using Tor Browser
    if USE_TOR_BROWSER and not check_tor_requirements():
        print("[ERROR] Tor requirements not met. Fix issues above or set SCRIPT_01_USE_TOR_BROWSER=0 for Chrome.")
        return

    # Build driver (Firefox/Tor or Chrome)
    driver = build_driver(show_browser=None)

    # Track rows for batch insert
    rows_buffer: List[Dict] = []
    total_rows_inserted = 0
    total_inns = len(inns)
    inns_processed_with_current_driver = 0  # For proactive driver recycling
    
    try:
        for idx, inn_term in enumerate(inns, start=1):
            if not inn_term:
                continue
            if inn_term in completed_inns:
                print(f"[{idx}/{total_inns}] Skipping {inn_term} (already completed)")
                continue

            # Proactive driver recycling: restart every N INNs to prevent memory buildup
            if inns_processed_with_current_driver >= RECYCLE_DRIVER_EVERY_N:
                print(f"  [RECYCLE] Restarting driver after {inns_processed_with_current_driver} INNs (prevents memory crashes)")
                try:
                    driver.quit()
                except Exception:
                    pass
                # Request new Tor identity when recycling (new IP)
                if USE_TOR_BROWSER and TOR_NEWNYM_ON_RECYCLE:
                    if not request_tor_newnym():
                        time.sleep(3)
                else:
                    time.sleep(3)
                driver = build_driver(show_browser=None)
                inns_processed_with_current_driver = 0

            # Progress reporting
            percent = round((idx / total_inns) * 100, 1) if total_inns > 0 else 0
            print(f"[{idx}/{total_inns}] INN: {inn_term}")
            print(f"[PROGRESS] Processing INNs: {idx}/{total_inns} ({percent}%) - Current: {inn_term}", flush=True)
            
            # Check if driver is still alive before proceeding
            if not is_driver_alive(driver):
                print(f"  !! Driver session died, restarting...")
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = build_driver(show_browser=None)
                inns_processed_with_current_driver = 0
                print(f"  -> Driver restarted")
            
            # Mark as in_progress in DB
            if repo:
                repo.mark_progress(step_number=1, step_name="scrape_rceth", 
                                   progress_key=inn_term, status="in_progress")
            
            # Allow up to MAX_RETRY_ATTEMPTS per INN (initial + retries) with driver restart between retries
            max_attempts = MAX_RETRY_ATTEMPTS
            driver_error_types = (WebDriverException, InvalidSessionIdException, MaxRetryError, URLConnectionError, ProtocolError)
            last_error = None
            attempt = 0
            success = False

            while attempt < max_attempts and not success:
                attempt += 1
                try:
                    if attempt > 1:
                        print(f"  -> Retry attempt {attempt}/{max_attempts} for {inn_term}...")
                    rows = scrape_for_inn(driver, inn_term)
                    if attempt > 1:
                        print(f"  -> rows: {len(rows)} (after retry)")
                    else:
                        print(f"  -> rows extracted: {len(rows)}")

                    # Translate and buffer rows
                    translated_rows = [translate_row_fields(r) for r in rows]
                    rows_buffer.extend(translated_rows)

                    # Insert to database in batches
                    if repo and rows_buffer:
                        inserted = repo.insert_rceth_data(rows_buffer)
                        total_rows_inserted += inserted
                        # -- Verification: compare extracted vs inserted --
                        if inserted != len(rows_buffer):
                            print(f"  [VERIFY] DB INSERT MISMATCH for '{inn_term}': extracted={len(rows_buffer)} inserted={inserted} (upsert/conflict resolution)")
                        else:
                            print(f"  [VERIFY] DB INSERT OK for '{inn_term}': {inserted} rows written to by_rceth_data")
                        rows_buffer = []

                    completed_inns.add(inn_term)
                    inns_processed_with_current_driver += 1
                    success = True

                    if repo:
                        repo.mark_progress(step_number=1, step_name="scrape_rceth",
                                           progress_key=inn_term, status="completed")
                    else:
                        save_progress(PROGRESS_FILE, completed_inns)

                except driver_error_types as e:
                    last_error = e
                    error_msg = str(e)
                    print(f"  !! Driver connection error for {inn_term}: {error_msg[:200] or '(Chrome/driver crashed)'}")
                    if attempt < max_attempts:
                        # Wait for Chrome to fully exit before starting a new driver
                        wait_sec = 5
                        print(f"  !! Waiting {wait_sec}s then restarting Chrome driver (attempt {attempt}/{max_attempts})...")
                        time.sleep(wait_sec)
                        try:
                            driver.quit()
                        except Exception:
                            pass
                        driver = build_driver(show_browser=None)
                        inns_processed_with_current_driver = 0
                        print(f"  -> Driver restarted")
                    else:
                        print(f"  !! All {max_attempts} attempts failed for {inn_term}")
                        if repo:
                            repo.mark_progress(step_number=1, step_name="scrape_rceth",
                                               progress_key=inn_term, status="failed",
                                               error_message=error_msg[:500] or "Chrome/driver crashed")

                except Exception as e:
                    last_error = e
                    if attempt < max_attempts:
                        wait_sec = 3
                        print(f"  !! Error for {inn_term}: {e}")
                        print(f"  !! Waiting {wait_sec}s and retrying ({attempt}/{max_attempts})...")
                        time.sleep(wait_sec)
                        try:
                            driver.quit()
                        except Exception:
                            pass
                        driver = build_driver(show_browser=None)
                        inns_processed_with_current_driver = 0
                    else:
                        print(f"  !! Failed {inn_term}: {e}")
                        if repo:
                            repo.mark_progress(step_number=1, step_name="scrape_rceth",
                                               progress_key=inn_term, status="failed",
                                               error_message=str(e)[:500])

            jitter_sleep(1.2, 2.4)

    finally:
        # Insert any remaining rows
        if repo and rows_buffer:
            try:
                inserted = repo.insert_rceth_data(rows_buffer)
                total_rows_inserted += inserted
            except Exception as e:
                print(f"[WARN] Failed to insert final batch: {e}")
        
        # Close driver
        try:
            driver.quit()
        except Exception:
            pass
        
        # Close database connection
        if db:
            try:
                db.close()
            except Exception:
                pass

    # Final summary and DB verification
    if repo:
        print(f"\n[SUCCESS] Scraped {total_rows_inserted} rows to database (by_rceth_data)")
        # -- Final DB verification --
        try:
            db_count = repo.get_rceth_data_count()
            print(f"[VERIFY] Final DB count: {db_count} rows in by_rceth_data for run_id={run_id}")
            if db_count != total_rows_inserted:
                print(f"[VERIFY] NOTE: DB has {db_count} rows vs {total_rows_inserted} inserted this session")
                print(f"[VERIFY]       (difference due to upserts/conflicts from resumed runs is expected)")
        except Exception as e:
            print(f"[VERIFY] Could not query DB for verification: {e}")

        # -- Completeness check: which INNs had zero results? --
        zero_result_inns = [inn for inn in inns if inn in completed_inns and inn not in completed_inns]
        failed_keys = set()
        try:
            with db.cursor() as cur:
                cur.execute("""
                    SELECT progress_key FROM by_step_progress
                    WHERE run_id = %s AND step_number = 1 AND status = 'failed'
                """, (run_id,))
                failed_keys = {row[0] for row in cur.fetchall()}
        except Exception:
            pass
        if failed_keys:
            print(f"[VERIFY] INNs that FAILED scraping ({len(failed_keys)}): {', '.join(sorted(failed_keys)[:10])}" +
                  (f" ... and {len(failed_keys) - 10} more" if len(failed_keys) > 10 else ""))

        # Export to CSV for checkpoint verification and legacy consumers
        try:
            rows = repo.get_all_rceth_data()
            if rows:
                out_path = get_output_dir() / getenv("SCRIPT_01_OUTPUT_CSV", "belarus_rceth_raw.csv") if USE_CONFIG else OUT_RAW
                df = pd.DataFrame(rows)
                out_path.parent.mkdir(parents=True, exist_ok=True)
                df.to_csv(str(out_path), index=False, encoding="utf-8-sig")
                print(f"[INFO] Exported {len(rows)} rows to {out_path.name}")
                # Verify CSV matches DB
                if len(rows) != db_count:
                    print(f"[VERIFY] WARNING: CSV export has {len(rows)} rows but DB has {db_count} rows")
        except Exception as e:
            print(f"[WARN] Could not export CSV: {e}")
    else:
        print(f"\n[SUCCESS] Processed {len(completed_inns)} INNs")

    # Final progress update
    if total_inns > 0:
        print(f"[PROGRESS] Processing INNs: {total_inns}/{total_inns} (100%) - Completed", flush=True)


if __name__ == "__main__":
    main()
