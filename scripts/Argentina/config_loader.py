"""
Configuration Loader for Argentina Scraper (Platform Config Integration)

This module provides centralized config and path management for Argentina scraper.
Integrates with platform_config.py to read from config/Argentina.env.json.

Precedence (highest to lowest):
1. Runtime overrides
2. Environment variables (OS-level)
3. Platform config (config/Argentina.env.json)
4. Hardcoded defaults
"""
import os
import sys
from pathlib import Path

# Add repo root to path for platform_config import
# Now: scripts/Argentina/config_loader.py -> parents[2] = repo root
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))


def get_repo_root() -> Path:
    """Get repository root directory (parent of scraper directories)."""
    return _repo_root


def get_central_output_dir() -> Path:
    """Get central exports directory for final reports - uses Documents/ScraperPlatform/output/exports/Argentina/"""
    if _PLATFORM_CONFIG_AVAILABLE:
        pm = get_path_manager()
        exports_dir = pm.get_exports_dir(SCRAPER_ID)  # Scraper-specific exports
        exports_dir.mkdir(parents=True, exist_ok=True)
        return exports_dir
    else:
        # Fallback: use repo root output
        repo_root = get_repo_root()
        central_output = repo_root / "output"
        central_output.mkdir(parents=True, exist_ok=True)
        return central_output

# Try to import platform_config (preferred)
try:
    from platform_config import PathManager, ConfigResolver, get_path_manager, get_config_resolver
    _PLATFORM_CONFIG_AVAILABLE = True
except ImportError:
    _PLATFORM_CONFIG_AVAILABLE = False
    PathManager = None
    ConfigResolver = None

# Scraper ID for this scraper
SCRAPER_ID = "Argentina"


def getenv(key: str, default: str = None) -> str:
    """
    Get environment variable with fallback to default.
    Integrates with platform_config if available.
    Checks both 'config' and 'secrets' sections.

    Args:
        key: Environment variable name
        default: Default value if not found

    Returns:
        Environment variable value or default (always as string)
    """
    # First check environment variables (highest precedence)
    env_val = os.getenv(key)
    if env_val is not None:
        return env_val
    
    if _PLATFORM_CONFIG_AVAILABLE:
        cr = get_config_resolver()
        # First check config section
        val = cr.get(SCRAPER_ID, key, None)
        if val is not None:
            # Convert to string in case JSON config returns boolean/int/float
            return str(val)
        
        # Then check secrets section
        secret_val = cr.get_secret_value(SCRAPER_ID, key, "")
        if secret_val:
            return secret_val
    
    # Return default if nothing found
    return default if default is not None else ""


def getenv_int(key: str, default: int = 0) -> int:
    """Get environment variable as integer."""
    try:
        val = getenv(key, str(default))
        return int(val)
    except (ValueError, TypeError):
        return default


def getenv_float(key: str, default: float = 0.0) -> float:
    """Get environment variable as float."""
    try:
        val = getenv(key, str(default))
        return float(val)
    except (ValueError, TypeError):
        return default


def getenv_bool(key: str, default: bool = False) -> bool:
    """Get environment variable as boolean."""
    val = getenv(key, "")
    
    # Handle case where val might already be a boolean (from JSON config)
    if isinstance(val, bool):
        return val
    
    # Convert to string and process
    val_str = str(val).strip().lower()
    if val_str in ("1", "true", "yes", "on"):
        return True
    elif val_str in ("0", "false", "no", "off", ""):
        return False
    return default


def get_base_dir() -> Path:
    """
    Get base directory for Argentina scraper.

    With platform_config: Returns platform root
    Legacy mode: Returns parent of scripts folder
    """
    if _PLATFORM_CONFIG_AVAILABLE:
        pm = get_path_manager()
        return pm.get_platform_root()
    else:
        # Legacy: relative to script location
        return Path(__file__).resolve().parents[1]


def get_input_dir(subpath: str = None) -> Path:
    """
    Get input directory - uses Documents/ScraperPlatform/input/Argentina/

    Args:
        subpath: Optional subdirectory under input/
    """
    if _PLATFORM_CONFIG_AVAILABLE:
        pm = get_path_manager()
        base = pm.get_input_dir(SCRAPER_ID)  # Scraper-specific input
        base.mkdir(parents=True, exist_ok=True)
    else:
        base = get_base_dir() / "Input"  # Note: Argentina uses capital I

    if subpath:
        return base / subpath
    return base


def get_output_dir(subpath: str = None) -> Path:
    """
    Get output directory - uses Documents/ScraperPlatform/output/Argentina/
    
    Scraper-specific output directory for organized file management.

    Args:
        subpath: Optional subdirectory under output/
    """
    # First check if OUTPUT_DIR is explicitly set (absolute path or environment variable)
    output_dir_str = getenv("OUTPUT_DIR", "")
    if output_dir_str and Path(output_dir_str).is_absolute():
        base = Path(output_dir_str)
    else:
        # Use scraper-specific platform output directory
        if _PLATFORM_CONFIG_AVAILABLE:
            pm = get_path_manager()
            base = pm.get_output_dir(SCRAPER_ID)  # Scraper-specific output
            base.mkdir(parents=True, exist_ok=True)
        else:
            # Fallback: use repo root output (legacy)
            repo_root = get_repo_root()
            base = repo_root / "output"
            base.mkdir(parents=True, exist_ok=True)

    if subpath:
        result = base / subpath
        result.mkdir(parents=True, exist_ok=True)
        return result
    return base


def get_backup_dir() -> Path:
    """Get backup directory - uses Documents/ScraperPlatform/output/backups/Argentina/"""
    if _PLATFORM_CONFIG_AVAILABLE:
        pm = get_path_manager()
        backup_dir = pm.get_backups_dir(SCRAPER_ID)  # Scraper-specific backups
        backup_dir.mkdir(parents=True, exist_ok=True)
        return backup_dir
    else:
        return get_base_dir() / "backups"


def get_logs_dir() -> Path:
    """Get logs directory."""
    if _PLATFORM_CONFIG_AVAILABLE:
        pm = get_path_manager()
        return pm.get_logs_dir()
    else:
        return get_base_dir() / "logs"


# Configuration values (commonly used in Argentina scripts)
ALFABETA_USER = getenv("ALFABETA_USER", "")
ALFABETA_PASS = getenv("ALFABETA_PASS", "")
HEADLESS = getenv_bool("HEADLESS", True)
MAX_ROWS = getenv_int("MAX_ROWS", 0)  # Default to 0 (unlimited)

# Proxy configuration
PROXY_1 = getenv("PROXY_1", "")
PROXY_2 = getenv("PROXY_2", "")
PROXY_3 = getenv("PROXY_3", "")

# ScrapingDog API configuration
SCRAPINGDOG_API_KEY = getenv("SCRAPINGDOG_API_KEY", "")

# Account rotation configuration
ACCOUNT_ROTATION_SEARCH_LIMIT = getenv_int("ACCOUNT_ROTATION_SEARCH_LIMIT", 50)  # For Selenium products
ACCOUNT_ROTATION_SEARCH_LIMIT_API = getenv_int("ACCOUNT_ROTATION_SEARCH_LIMIT_API", 50)  # For API products
SELENIUM_ROTATION_LIMIT = getenv_int("SELENIUM_ROTATION_LIMIT", 50)  # Rotate account every N products for Selenium workers

# Rate limiting configuration
RATE_LIMIT_PRODUCTS = getenv_int("RATE_LIMIT_PRODUCTS", 1)
RATE_LIMIT_SECONDS = getenv_float("RATE_LIMIT_SECONDS", 10.0)
DUPLICATE_RATE_LIMIT_SECONDS = getenv_float("DUPLICATE_RATE_LIMIT_SECONDS", 10.0)  # 10 seconds per product for Selenium
SELENIUM_FALLBACK_RATE_LIMIT_SECONDS = getenv_float("SELENIUM_FALLBACK_RATE_LIMIT_SECONDS", 60.0)  # 60 seconds (1 minute) per product for Selenium fallback when API returns null
SKIP_REPEAT_SELENIUM_TO_API = getenv_bool("SKIP_REPEAT_SELENIUM_TO_API", True)

# Threading configuration
DEFAULT_THREADS = getenv_int("DEFAULT_THREADS", 2)
MIN_THREADS = getenv_int("MIN_THREADS", 1)
MAX_THREADS = getenv_int("MAX_THREADS", 2)
API_THREADS = getenv_int("API_THREADS", 5)  # Number of threads for API processing
SELENIUM_THREADS = getenv_int("SELENIUM_THREADS", 4)  # Number of threads for Selenium processing
SELENIUM_SINGLE_ATTEMPT = getenv_bool("SELENIUM_SINGLE_ATTEMPT", False)  # If true, no retries/requeue in Selenium

# File names
DICTIONARY_FILE = getenv("DICTIONARY_FILE", "Dictionary.csv")
PCID_MAPPING_FILE = getenv("PCID_MAPPING_FILE", "pcid_Mapping.csv")
PRODUCTLIST_FILE = getenv("PRODUCTLIST_FILE", "Productlist.csv")
PROXY_LIST_FILE = getenv("PROXY_LIST_FILE", "ProxyList.txt")
IGNORE_LIST_FILE = getenv("IGNORE_LIST_FILE", "ignore_list.csv")

# Output file names
OUTPUT_PRODUCTS_CSV = getenv("OUTPUT_PRODUCTS_CSV", "alfabeta_products_by_product.csv")
OUTPUT_TRANSLATED_CSV = getenv("OUTPUT_TRANSLATED_CSV", "alfabeta_products_all_dict_en.csv")
OUTPUT_MISSING_CSV = getenv("OUTPUT_MISSING_CSV", "missing_cells.csv")
OUTPUT_PROGRESS_CSV = getenv("OUTPUT_PROGRESS_CSV", "alfabeta_progress.csv")
OUTPUT_ERRORS_CSV = getenv("OUTPUT_ERRORS_CSV", "alfabeta_errors.csv")
OUTPUT_REPORT_PREFIX = getenv("OUTPUT_REPORT_PREFIX", "alfabeta_Report_")
OUTPUT_PCID_MISSING = getenv("OUTPUT_PCID_MISSING", "pcid_MISSING.xlsx")

# AlfaBeta URLs
HUB_URL = getenv("HUB_URL", "https://www.alfabeta.net/precio/srv")
PRODUCTS_URL = getenv("PRODUCTS_URL", "https://www.alfabeta.net/precio")

# ScrapingDog API URL
SCRAPINGDOG_URL = getenv("SCRAPINGDOG_URL", "https://api.scrapingdog.com/scrape")

# Request pause configuration
REQUEST_PAUSE_BASE = getenv_float("REQUEST_PAUSE_BASE", 0.20)
REQUEST_PAUSE_JITTER_MIN = getenv_float("REQUEST_PAUSE_JITTER_MIN", 0.05)
REQUEST_PAUSE_JITTER_MAX = getenv_float("REQUEST_PAUSE_JITTER_MAX", 0.20)

# Timeout configuration
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

# Driver configuration
PAGE_LOAD_TIMEOUT = getenv_int("PAGE_LOAD_TIMEOUT", 90)
IMPLICIT_WAIT = getenv_int("IMPLICIT_WAIT", 2)

# Retry configuration
MAX_RETRIES_SUBMIT = getenv_int("MAX_RETRIES_SUBMIT", 4)
MAX_RETRIES_TIMEOUT = getenv_int("MAX_RETRIES_TIMEOUT", 2)
MAX_RETRIES_AUTH = getenv_int("MAX_RETRIES_AUTH", 3)

# CPU throttling thresholds
CPU_THROTTLE_HIGH = getenv_int("CPU_THROTTLE_HIGH", 90)
CPU_THROTTLE_MEDIUM = getenv_int("CPU_THROTTLE_MEDIUM", 70)

# File names
PREPARED_URLS_FILE = getenv("PREPARED_URLS_FILE", "Productlist_with_urls.csv")

# Output configuration
EXCLUDE_PRICE = getenv_bool("EXCLUDE_PRICE", False)
DATE_FORMAT = getenv("DATE_FORMAT", "%d%m%Y")

# Translation configuration
TARGET_COLUMNS = getenv("TARGET_COLUMNS", "").split(",") if getenv("TARGET_COLUMNS", "") else [
    "active_ingredient", "therapeutic_class", "description",
    "SIFAR_detail", "IOMA_detail", "IOMA_AF", "IOMA_OS", "import_status",
]

# OpenAI configuration
OPENAI_API_KEY = getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_TEMPERATURE = getenv_float("OPENAI_TEMPERATURE", 0.0)

# Helper function to get proxy list
def get_proxy_list() -> list:
    """Get list of proxies from environment variables (config/Argentina.env.json)."""
    proxies = []
    # Get proxies from environment variables (loaded from config/Argentina.env.json)
    for i in range(1, 10):  # Support up to 9 proxies
        proxy = getenv(f"PROXY_{i}", "")
        if proxy:
            proxies.append(proxy)
    
    return proxies

# Helper function to load accounts
def get_accounts() -> list:
    """Get list of accounts from environment variables."""
    accounts = []
    account_num = 1
    while True:
        user_key = f"ALFABETA_USER_{account_num}"
        pass_key = f"ALFABETA_PASS_{account_num}"
        user = getenv(user_key, "")
        pwd = getenv(pass_key, "")
        if user and pwd:
            accounts.append((user, pwd))
            account_num += 1
        else:
            break
    
    # Fallback to single account
    if not accounts and ALFABETA_USER and ALFABETA_PASS:
        accounts.append((ALFABETA_USER, ALFABETA_PASS))
    
    return accounts


def parse_proxy_url(proxy_url: str) -> dict:
    """Parse proxy URL to extract components"""
    from urllib.parse import urlparse
    # Format: https://user:pass@host:port
    parsed = urlparse(proxy_url)
    return {
        "host": parsed.hostname,
        "port": parsed.port,
        "username": parsed.username,
        "password": parsed.password,
        "scheme": parsed.scheme or "http"
    }

# Diagnostic function
if __name__ == "__main__":
    print("=" * 60)
    print("Argentina Config Loader - Diagnostic")
    print("=" * 60)
    print(f"Platform Config Available: {_PLATFORM_CONFIG_AVAILABLE}")
    print(f"Scraper ID: {SCRAPER_ID}")
    print()
    print("Paths:")
    print(f"  Base Dir: {get_base_dir()}")
    print(f"  Input Dir: {get_input_dir()}")
    print(f"  Output Dir: {get_output_dir()}")
    print(f"  Backup Dir: {get_backup_dir()}")
    print(f"  Logs Dir: {get_logs_dir()}")
    print()
    print("Config Values:")
    print(f"  Headless: {HEADLESS}")
    print(f"  Max Rows: {MAX_ROWS}")
    print(f"  AlfaBeta User Set: {'Yes' if ALFABETA_USER else 'No'}")
    print(f"  AlfaBeta Pass Set: {'Yes' if ALFABETA_PASS else 'No'}")
    print(f"  Proxy 1 Set: {'Yes' if PROXY_1 else 'No'}")
    print(f"  Proxy 2 Set: {'Yes' if PROXY_2 else 'No'}")
    print(f"  Proxy 3 Set: {'Yes' if PROXY_3 else 'No'}")
