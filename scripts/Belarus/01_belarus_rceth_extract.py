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

# Core modules - CORRECTED IMPORTS
from core.network.proxy_checker import check_tor_running
from core.network.tor_manager import auto_start_tor_proxy
from core.browser.driver_factory import create_firefox_driver
from core.network.ip_rotation import tor_signal_newnym as request_tor_newnym # Alias for compatibility

from core.browser.chrome_manager import kill_orphaned_chrome_processes, get_chromedriver_path
from selenium.webdriver.chrome.service import Service as ChromeService
from core.monitoring.resource_monitor import check_memory_leak, log_resource_status, periodic_resource_check

# Chrome PID tracking (optional - for GUI to report active instances)
try:
    from core.browser.chrome_pid_tracker import get_chrome_pids_from_driver
    from core.browser.chrome_instance_tracker import ChromeInstanceTracker
    from core.db.postgres_connection import PostgresDB
except ImportError:
    get_chrome_pids_from_driver = None
    ChromeInstanceTracker = None
    PostgresDB = None
from core.browser import stealth_profile


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
    
    # 1. Check Tor Proxy - use updated core function
    tor_running, port = check_tor_running()
    
    if not tor_running and AUTO_START_TOR_PROXY:
        print("  [INFO] Tor not detected; attempting auto-start...")
        # auto_start_tor_proxy from core
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
        
    # Create config for core driver factory
    tor_config = {"enabled": False}
    if TOR_PROXY_PORT:
        tor_config = {"enabled": True, "port": TOR_PROXY_PORT}
        
    # Use standardized driver factory
    try:
        driver = create_firefox_driver(
            headless=not show_browser,
            tor_config=tor_config
        )
    except Exception as e:
        print(f"[ERROR] Failed to create Firefox driver: {e}")
        # Try fallback to Chrome if Firefox fails and tor not strictly required? 
        # But this function name implies Firefox Tor. Let's just raise or return None.
        raise
    
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
    stealth_script = stealth_profile.get_stealth_init_script()
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
        driver = build_driver_firefox_tor(show_browser=show_browser)
        # Apply local comprehensive stealth script (overrides basic one in core)
        inject_stealth_script(driver)
        return driver
    # Chrome fallback
    if show_browser is None:
        # Get from config if available
        if USE_CONFIG:
            show_browser = not getenv_bool("SCRIPT_01_HEADLESS", False)
        else:
            show_browser = True
    
    opts = webdriver.ChromeOptions()
    if not show_browser:
        opts.add_argument("--headless=new")
        opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1400,900")
    
    # Apply standardized stealth profile (User agent, Automation flags, Lang, etc)
    stealth_profile.apply_selenium(opts)
    
    # Overwrite lang if needed (stealth profile sets en-US,en)
    
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
    driver_path = get_chromedriver_path()
    if not driver_path:
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            driver_path = ChromeDriverManager().install()
        except ImportError:
            raise RuntimeError("ChromeDriver not found. Install webdriver-manager: pip install webdriver-manager")
    service = ChromeService(driver_path)
    driver = webdriver.Chrome(service=service, options=opts)
    _set_driver_connection_timeout(driver)
    
    # Inject stealth/antibot script (from Malaysia scraper)
    inject_stealth_script(driver)
    
    return driver
