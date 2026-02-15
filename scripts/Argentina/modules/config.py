"""
Argentina Scraper Configuration
Centralized configuration loading and constants
"""

from pathlib import Path
import os

# Import from config_loader
from config_loader import (
    get_input_dir, get_output_dir, get_accounts,
    ALFABETA_USER, ALFABETA_PASS, HEADLESS, HUB_URL, PRODUCTS_URL,
    SELENIUM_ROTATION_LIMIT, SELENIUM_THREADS, SELENIUM_SINGLE_ATTEMPT,
    SELENIUM_MAX_LOOPS,
    SELENIUM_PRODUCTS_PER_RESTART,
    DUPLICATE_RATE_LIMIT_SECONDS,
    SKIP_REPEAT_SELENIUM_TO_API,
    USE_API_STEPS,
    REQUEST_PAUSE_BASE, REQUEST_PAUSE_JITTER_MIN, REQUEST_PAUSE_JITTER_MAX,
    WAIT_ALERT, WAIT_SEARCH_FORM, WAIT_SEARCH_RESULTS, WAIT_PAGE_LOAD,
    PAGE_LOAD_TIMEOUT, MAX_RETRIES_TIMEOUT, CPU_THROTTLE_HIGH, PAUSE_CPU_THROTTLE,
    QUEUE_GET_TIMEOUT,
    PREPARED_URLS_FILE,
    OUTPUT_PRODUCTS_CSV, OUTPUT_PROGRESS_CSV, OUTPUT_ERRORS_CSV,
    SLOW_PAGE_RESTART_ENABLED, SLOW_PAGE_MEDIAN_WINDOW, SLOW_PAGE_MIN_SAMPLES,
    SLOW_PAGE_MEDIAN_THRESHOLD_SECONDS,
    TOR_CONTROL_HOST, TOR_CONTROL_PORT, TOR_CONTROL_PASSWORD, TOR_CONTROL_COOKIE_FILE,
    TOR_NEWNYM_ENABLED, TOR_NEWNYM_INTERVAL_SECONDS, TOR_SOCKS_PORT,
    TOR_NEWNYM_COOLDOWN_SECONDS,
    SURFSHARK_RECONNECT_CMD, SURFSHARK_ROTATE_INTERVAL_SECONDS, SURFSHARK_IP_CHANGE_TIMEOUT_SECONDS,
    MAX_BROWSER_RUNTIME_SECONDS,
    REQUIRE_TOR_PROXY, AUTO_START_TOR_PROXY,
    SELENIUM_ROUND_ROBIN_RETRY, SELENIUM_MAX_ATTEMPTS_PER_PRODUCT
)

# Paths
REPO_ROOT = Path(__file__).resolve().parents[3]
INPUT_DIR = get_input_dir()
OUTPUT_DIR = get_output_dir()
PREPARED_URLS_FILE_PATH = OUTPUT_DIR / PREPARED_URLS_FILE
OUT_CSV = OUTPUT_DIR / OUTPUT_PRODUCTS_CSV
PROGRESS = OUTPUT_DIR / OUTPUT_PROGRESS_CSV
ERRORS = OUTPUT_DIR / OUTPUT_ERRORS_CSV
DEBUG_ERR = OUTPUT_DIR / "debug" / "error"
DEBUG_NF = OUTPUT_DIR / "debug" / "not_found"
ARTIFACTS_DIR = OUTPUT_DIR / "artifacts"

# Ensure directories exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
for d in [DEBUG_ERR, DEBUG_NF]:
    d.mkdir(parents=True, exist_ok=True)

# Request pause jitter
REQUEST_PAUSE_JITTER = (REQUEST_PAUSE_JITTER_MIN, REQUEST_PAUSE_JITTER_MAX)

# Memory limits
MEMORY_LIMIT_MB = 2048  # 2GB hard limit
MEMORY_SOFT_LIMIT_MB = 1536  # 1.5GB soft limit
MEMORY_CHECK_INTERVAL = 5  # Check every 5 products

# Tor proxy configuration
TOR_PROXY_PORT = int(TOR_SOCKS_PORT) if TOR_SOCKS_PORT else 0

# Fatal driver error substrings
FATAL_DRIVER_SUBSTRINGS = (
    "tab crashed",
    "invalid session id",
    "disconnected",
    "cannot determine loading status",
    "firefox not reachable",
    "session deleted",
    "target window already closed",
    "connection refused",
    "connection reset",
)

# Load accounts at module import
ACCOUNTS = get_accounts()
if not ACCOUNTS:
    raise RuntimeError("No accounts found! Please configure ALFABETA_USER and ALFABETA_PASS in environment")
