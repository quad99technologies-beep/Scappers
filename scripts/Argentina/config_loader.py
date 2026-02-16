"""
Configuration Loader for Argentina Scraper (Facade for Core ConfigManager)

This module provides centralized config and path management for Argentina scraper.
It acts as a facade, delegating all logic to core.config.config_manager.ConfigManager.
"""
import os
import sys
from pathlib import Path

# Ensure core is in path
_script_dir = Path(__file__).resolve().parent
_repo_root = _script_dir.parents[1]  # scripts/Argentina -> scripts -> Scrappers (Repo Root)
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.config.config_manager import ConfigManager, get_env_bool as _get_bool, get_env_int as _get_int, get_env_float as _get_float

SCRAPER_ID = "Argentina"

# Initialize Environment
ConfigManager.ensure_dirs()
ConfigManager.load_env(SCRAPER_ID)

# --- Path Accessors ---

def get_repo_root() -> Path:
    return ConfigManager.get_app_root()

def get_base_dir() -> Path:
    return ConfigManager.get_app_root()

def get_input_dir(subpath: str = None) -> Path:
    base = ConfigManager.get_input_dir(SCRAPER_ID)
    if subpath:
        return base / subpath
    return base

def get_output_dir(subpath: str = None) -> Path:
    base = ConfigManager.get_output_dir(SCRAPER_ID)
    if subpath:
        return base / subpath
    return base

def get_backup_dir() -> Path:
    return ConfigManager.get_backups_dir(SCRAPER_ID)

def get_logs_dir() -> Path:
    return ConfigManager.get_logs_dir()

def get_central_output_dir() -> Path:
    return ConfigManager.get_exports_dir(SCRAPER_ID)

# --- Environment Accessors ---

def getenv(key: str, default: str = None) -> str:
    # Use ConfigManager's get_env_value which searches OS env then loaded env
    val = ConfigManager.get_env_value(SCRAPER_ID, key, default)
    return val if val is not None else ""

def getenv_int(key: str, default: int = 0) -> int:
    return _get_int(SCRAPER_ID, key, default)

def getenv_float(key: str, default: float = 0.0) -> float:
    return _get_float(SCRAPER_ID, key, default)

def getenv_bool(key: str, default: bool = False) -> bool:
    return _get_bool(SCRAPER_ID, key, default)

# --- Configuration Constants ---

ALFABETA_USER = getenv("ALFABETA_USER", "")
ALFABETA_PASS = getenv("ALFABETA_PASS", "")
HEADLESS = getenv_bool("HEADLESS", True)
MAX_ROWS = getenv_int("MAX_ROWS", 0)

# Proxy
PROXY_1 = getenv("PROXY_1", "")
PROXY_2 = getenv("PROXY_2", "")
PROXY_3 = getenv("PROXY_3", "")

# API
SCRAPINGDOG_API_KEY = getenv("SCRAPINGDOG_API_KEY", "")
USE_API_STEPS = getenv_bool("USE_API_STEPS", False)
RESET_API_PENDING_BEFORE_SELENIUM = getenv_bool("RESET_API_PENDING_BEFORE_SELENIUM", False)

# Account Rotation
ACCOUNT_ROTATION_SEARCH_LIMIT = getenv_int("ACCOUNT_ROTATION_SEARCH_LIMIT", 50)
ACCOUNT_ROTATION_SEARCH_LIMIT_API = getenv_int("ACCOUNT_ROTATION_SEARCH_LIMIT_API", 50)
SELENIUM_ROTATION_LIMIT = getenv_int("SELENIUM_ROTATION_LIMIT", 50)

# Rate Limiting
RATE_LIMIT_PRODUCTS = getenv_int("RATE_LIMIT_PRODUCTS", 1)
RATE_LIMIT_SECONDS = getenv_float("RATE_LIMIT_SECONDS", 10.0)
DUPLICATE_RATE_LIMIT_SECONDS = getenv_float("DUPLICATE_RATE_LIMIT_SECONDS", 10.0)
SELENIUM_FALLBACK_RATE_LIMIT_SECONDS = getenv_float("SELENIUM_FALLBACK_RATE_LIMIT_SECONDS", 60.0)
SKIP_REPEAT_SELENIUM_TO_API = getenv_bool("SKIP_REPEAT_SELENIUM_TO_API", True)

# Threading
DEFAULT_THREADS = getenv_int("DEFAULT_THREADS", 2)
MIN_THREADS = getenv_int("MIN_THREADS", 1)
MAX_THREADS = getenv_int("MAX_THREADS", 2)
API_THREADS = getenv_int("API_THREADS", 5)
SELENIUM_THREADS = getenv_int("SELENIUM_THREADS", 4)
SELENIUM_SINGLE_ATTEMPT = getenv_bool("SELENIUM_SINGLE_ATTEMPT", False)

# Loop vs Retry
SELENIUM_MAX_LOOPS = getenv_int("SELENIUM_MAX_LOOPS", 0)
SELENIUM_ROUNDS = getenv_int("SELENIUM_ROUNDS", 3)
ROUND_PAUSE_SECONDS = getenv_int("ROUND_PAUSE_SECONDS", 60)
SELENIUM_MAX_RUNS = getenv_int("SELENIUM_MAX_RUNS", SELENIUM_ROUNDS)
if SELENIUM_MAX_LOOPS <= 0:
    SELENIUM_MAX_LOOPS = SELENIUM_MAX_RUNS

SELENIUM_STEP3_MAX_ATTEMPTS = getenv_int("SELENIUM_STEP3_MAX_ATTEMPTS", 5)

# No Data Retry
NO_DATA_MAX_ROUNDS = getenv_int("NO_DATA_MAX_ROUNDS", 1)
SKIP_NO_DATA_STEP = getenv_bool("SKIP_NO_DATA_STEP", True)

# Browser / Health
SELENIUM_PRODUCTS_PER_RESTART = getenv_int("SELENIUM_PRODUCTS_PER_RESTART", 100)
SLOW_PAGE_RESTART_ENABLED = getenv_bool("SLOW_PAGE_RESTART_ENABLED", True)
SLOW_PAGE_MEDIAN_WINDOW = getenv_int("SLOW_PAGE_MEDIAN_WINDOW", 15)
SLOW_PAGE_MIN_SAMPLES = getenv_int("SLOW_PAGE_MIN_SAMPLES", 8)
SLOW_PAGE_MEDIAN_THRESHOLD_SECONDS = getenv_float("SLOW_PAGE_MEDIAN_THRESHOLD_SECONDS", 60.0)

# File names
PREPARED_URLS_FILE = getenv("PREPARED_URLS_FILE", "Productlist_with_urls.csv")
OUTPUT_PRODUCTS_CSV = getenv("OUTPUT_PRODUCTS_CSV", "alfabeta_products_by_product.csv")
OUTPUT_TRANSLATED_CSV = getenv("OUTPUT_TRANSLATED_CSV", "alfabeta_products_all_dict_en.csv")
OUTPUT_MISSING_CSV = getenv("OUTPUT_MISSING_CSV", "missing_cells.csv")
OUTPUT_PROGRESS_CSV = getenv("OUTPUT_PROGRESS_CSV", "alfabeta_progress.csv")
OUTPUT_ERRORS_CSV = getenv("OUTPUT_ERRORS_CSV", "alfabeta_errors.csv")
OUTPUT_REPORT_PREFIX = getenv("OUTPUT_REPORT_PREFIX", "alfabeta_Report_")
OUTPUT_PCID_MISSING = getenv("OUTPUT_PCID_MISSING", "pcid_MISSING.xlsx")

# URLs
HUB_URL = getenv("HUB_URL", "https://www.alfabeta.net/precio/srv")
PRODUCTS_URL = getenv("PRODUCTS_URL", "https://www.alfabeta.net/precio")
SCRAPINGDOG_URL = getenv("SCRAPINGDOG_URL", "https://api.scrapingdog.com/scrape")

# Pauses
REQUEST_PAUSE_BASE = getenv_float("REQUEST_PAUSE_BASE", 0.20)
REQUEST_PAUSE_JITTER_MIN = getenv_float("REQUEST_PAUSE_JITTER_MIN", 0.05)
REQUEST_PAUSE_JITTER_MAX = getenv_float("REQUEST_PAUSE_JITTER_MAX", 0.20)

# Timeouts
WAIT_SHORT = getenv_int("WAIT_SHORT", 5)
WAIT_LONG = getenv_int("WAIT_LONG", 20)
WAIT_ALERT = getenv_int("WAIT_ALERT", 2)
WAIT_SEARCH_FORM = getenv_int("WAIT_SEARCH_FORM", 10)
WAIT_SEARCH_RESULTS = getenv_int("WAIT_SEARCH_RESULTS", 30)
WAIT_PAGE_LOAD = getenv_int("WAIT_PAGE_LOAD", 30)
API_REQUEST_TIMEOUT = getenv_int("API_REQUEST_TIMEOUT", 30)
QUEUE_GET_TIMEOUT = getenv_int("QUEUE_GET_TIMEOUT", 2)
PAUSE_BETWEEN_OPERATIONS = getenv_float("PAUSE_BETWEEN_OPERATIONS", 0.6)
PAUSE_RETRY = getenv_int("PAUSE_RETRY", 10)
PAUSE_CPU_THROTTLE = getenv_float("PAUSE_CPU_THROTTLE", 0.5)
PAUSE_HTML_LOAD = getenv_int("PAUSE_HTML_LOAD", 1)
PAUSE_SHORT = getenv_float("PAUSE_SHORT", 0.2)
PAUSE_MEDIUM = getenv_float("PAUSE_MEDIUM", 0.4)
PAUSE_AFTER_ALERT = getenv_int("PAUSE_AFTER_ALERT", 1)

# Driver
PAGE_LOAD_TIMEOUT = getenv_int("PAGE_LOAD_TIMEOUT", 90)
IMPLICIT_WAIT = getenv_int("IMPLICIT_WAIT", 2)

# Tor / Network
TOR_CONTROL_HOST = getenv("TOR_CONTROL_HOST", "127.0.0.1")
TOR_CONTROL_PORT = getenv_int("TOR_CONTROL_PORT", 0)
TOR_CONTROL_PASSWORD = getenv("TOR_CONTROL_PASSWORD", "")
TOR_CONTROL_COOKIE_FILE = getenv("TOR_CONTROL_COOKIE_FILE", "")
TOR_NEWNYM_ENABLED = getenv_bool("TOR_NEWNYM_ENABLED", False)
TOR_NEWNYM_INTERVAL_SECONDS = getenv_int("TOR_NEWNYM_INTERVAL_SECONDS", 180)
TOR_SOCKS_PORT = getenv_int("TOR_SOCKS_PORT", 0)
TOR_NEWNYM_COOLDOWN_SECONDS = getenv_int("TOR_NEWNYM_COOLDOWN_SECONDS", 10)
REQUIRE_TOR_PROXY = getenv_bool("REQUIRE_TOR_PROXY", False)
AUTO_START_TOR_PROXY = getenv_bool("AUTO_START_TOR_PROXY", True)
TOR_PROXY_PORT = TOR_SOCKS_PORT

# Surfshark
SURFSHARK_RECONNECT_CMD = getenv("SURFSHARK_RECONNECT_CMD", "")
SURFSHARK_ROTATE_INTERVAL_SECONDS = getenv_int("SURFSHARK_ROTATE_INTERVAL_SECONDS", 600)
SURFSHARK_IP_CHANGE_TIMEOUT_SECONDS = getenv_int("SURFSHARK_IP_CHANGE_TIMEOUT_SECONDS", 120)

# Round Robin
SELENIUM_ROUND_ROBIN_RETRY = getenv_bool("SELENIUM_ROUND_ROBIN_RETRY", False)
SELENIUM_MAX_ATTEMPTS_PER_PRODUCT = getenv_int("SELENIUM_MAX_ATTEMPTS_PER_PRODUCT", 5)

# Runtime
MAX_BROWSER_RUNTIME_SECONDS = getenv_int("MAX_BROWSER_RUNTIME_SECONDS", 480)

# Retries
MAX_RETRIES_SUBMIT = getenv_int("MAX_RETRIES_SUBMIT", 4)
MAX_RETRIES_TIMEOUT = getenv_int("MAX_RETRIES_TIMEOUT", 2)
MAX_RETRIES_AUTH = getenv_int("MAX_RETRIES_AUTH", 3)

# CPU
CPU_THROTTLE_HIGH = getenv_int("CPU_THROTTLE_HIGH", 90)
CPU_THROTTLE_MEDIUM = getenv_int("CPU_THROTTLE_MEDIUM", 70)

# Output
EXCLUDE_PRICE = getenv_bool("EXCLUDE_PRICE", False)
DATE_FORMAT = getenv("DATE_FORMAT", "%d%m%Y")

# OpenAI
OPENAI_API_KEY = getenv("OPENAI_API_KEY", "")

# Functions
def get_accounts() -> list:
    """Get list of accounts from environment variables."""
    accounts = []
    max_accounts = 20
    for account_num in range(1, max_accounts + 1):
        user_key = f"ALFABETA_USER_{account_num}"
        pass_key = f"ALFABETA_PASS_{account_num}"
        user = getenv(user_key, "")
        pwd = getenv(pass_key, "")
        if user and pwd:
            accounts.append((user, pwd))
    
    # Fallback to single account
    if not accounts and ALFABETA_USER and ALFABETA_PASS:
        accounts.append((ALFABETA_USER, ALFABETA_PASS))
    
    return accounts

def get_proxy_list() -> list:
    proxies = []
    for i in range(1, 10):
        proxy = getenv(f"PROXY_{i}", "")
        if proxy:
            proxies.append(proxy)
    return proxies

def validate_config() -> list:
    issues = []
    accounts = get_accounts()
    if not accounts:
        issues.append("No AlfaBeta credentials.")
    return issues
