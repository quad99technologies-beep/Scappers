

import os, re, csv, json, time, random, argparse, logging, threading, tempfile
from pathlib import Path
from datetime import datetime
from queue import Queue, Empty
from typing import Optional, Tuple, List, Dict, Any
from collections import Counter
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    requests = None
    REQUESTS_AVAILABLE = False

try:
    from bs4 import BeautifulSoup
    BEAUTIFULSOUP_AVAILABLE = True
except ImportError:
    BeautifulSoup = None
    BEAUTIFULSOUP_AVAILABLE = False

try:
    import psutil  # optional
except Exception:
    psutil = None

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.alert import Alert
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

# ====== CONFIG ======
from config_loader import (
    get_input_dir, get_output_dir, get_proxy_list, get_accounts, parse_proxy_url,
    ALFABETA_USER, ALFABETA_PASS, HEADLESS, HUB_URL, PRODUCTS_URL,
    SCRAPINGDOG_API_KEY, SCRAPINGDOG_URL, ACCOUNT_ROTATION_SEARCH_LIMIT, ACCOUNT_ROTATION_SEARCH_LIMIT_API, SELENIUM_ROTATION_LIMIT,
    RATE_LIMIT_PRODUCTS, RATE_LIMIT_SECONDS, DUPLICATE_RATE_LIMIT_SECONDS, SELENIUM_FALLBACK_RATE_LIMIT_SECONDS,
    REQUEST_PAUSE_BASE, REQUEST_PAUSE_JITTER_MIN, REQUEST_PAUSE_JITTER_MAX,
    WAIT_ALERT, WAIT_SEARCH_FORM, WAIT_SEARCH_RESULTS, WAIT_PAGE_LOAD,
    API_REQUEST_TIMEOUT, QUEUE_GET_TIMEOUT, PAUSE_RETRY, PAUSE_CPU_THROTTLE, PAUSE_HTML_LOAD,
    PAGE_LOAD_TIMEOUT, MAX_RETRIES_TIMEOUT, MAX_RETRIES_AUTH, CPU_THROTTLE_HIGH, CPU_THROTTLE_MEDIUM,
    DEFAULT_THREADS, MIN_THREADS, MAX_THREADS, API_THREADS, SELENIUM_THREADS,
    PRODUCTLIST_FILE, PREPARED_URLS_FILE, IGNORE_LIST_FILE, OUTPUT_PRODUCTS_CSV, OUTPUT_PROGRESS_CSV, OUTPUT_ERRORS_CSV
)

# ====== LOGGING ======
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("alfabeta")

# Load accounts at startup
ACCOUNTS = get_accounts()
if not ACCOUNTS:
    raise RuntimeError("No accounts found! Please configure ALFABETA_USER and ALFABETA_PASS in environment")

# Get paths from config_loader
INPUT_DIR = get_input_dir()
OUTPUT_DIR = get_output_dir()
# Try to use prepared URLs file first, fallback to Productlist.csv
PREPARED_URLS_FILE_PATH = OUTPUT_DIR / PREPARED_URLS_FILE
INPUT_FILE = PREPARED_URLS_FILE_PATH if PREPARED_URLS_FILE_PATH.exists() else (INPUT_DIR / PRODUCTLIST_FILE)
OUT_CSV = OUTPUT_DIR / OUTPUT_PRODUCTS_CSV
PROGRESS = OUTPUT_DIR / OUTPUT_PROGRESS_CSV
ERRORS = OUTPUT_DIR / OUTPUT_ERRORS_CSV
DEBUG_ERR = OUTPUT_DIR / "debug" / "error"
DEBUG_NF = OUTPUT_DIR / "debug" / "not_found"

# Create debug directories
for d in [DEBUG_ERR, DEBUG_NF]:
    d.mkdir(parents=True, exist_ok=True)

# Request pause jitter tuple
REQUEST_PAUSE_JITTER = (REQUEST_PAUSE_JITTER_MIN, REQUEST_PAUSE_JITTER_MAX)

OUT_FIELDS = [
    "input_company", "input_product_name",
    "company", "product_name",
    "active_ingredient", "therapeutic_class",
    "description", "price_ars", "date", "scraped_at",
    # five coverage fields (priority)
    "SIFAR_detail", "PAMI_AF", "IOMA_detail", "IOMA_AF", "IOMA_OS",
    # extras
    "import_status", "coverage_json"
]

# ====== LOCKS ======
CSV_LOCK = threading.Lock()
PROGRESS_LOCK = threading.Lock()
ERROR_LOCK = threading.Lock()
PROXY_LOCK = threading.Lock()
RATE_LIMIT_LOCK = threading.Lock()

# ====== RATE LIMITING ======
_rate_limit_batch_start = None
_rate_limit_count = 0
_duplicate_rate_limit_per_thread = {}  # thread_id -> last_process_time
_selenium_fallback_rate_limit_per_thread = {}  # thread_id -> last_process_time

def rate_limit_wait():
    """Wait if needed to respect rate limit: 1 product every 10 seconds"""
    global _rate_limit_batch_start, _rate_limit_count
    with RATE_LIMIT_LOCK:
        now = time.time()
        if _rate_limit_batch_start is None:
            _rate_limit_batch_start = now
            _rate_limit_count = 0
        
        _rate_limit_count += 1
        
        if _rate_limit_count >= RATE_LIMIT_PRODUCTS:
            elapsed = now - _rate_limit_batch_start
            if elapsed < RATE_LIMIT_SECONDS:
                wait_time = RATE_LIMIT_SECONDS - elapsed
                log.info(f"Rate limit: processed {_rate_limit_count} products, waiting {wait_time:.2f}s")
                time.sleep(wait_time)
            # Reset for next batch
            _rate_limit_batch_start = time.time()
            _rate_limit_count = 0

def duplicate_rate_limit_wait(thread_id: int):
    """Wait if needed to respect rate limit for duplicates: 1 product per 30 seconds per thread (Selenium)"""
    global _duplicate_rate_limit_per_thread
    with RATE_LIMIT_LOCK:
        now = time.time()
        last_time = _duplicate_rate_limit_per_thread.get(thread_id, 0)
        time_since_last = now - last_time
        
        if time_since_last < DUPLICATE_RATE_LIMIT_SECONDS:
            wait_time = DUPLICATE_RATE_LIMIT_SECONDS - time_since_last
            log.info(f"[DUPLICATE_RATE_LIMIT] Thread {thread_id}: waiting {wait_time:.2f}s (1 product per {DUPLICATE_RATE_LIMIT_SECONDS}s)")
            time.sleep(wait_time)
        
        _duplicate_rate_limit_per_thread[thread_id] = time.time()

def selenium_fallback_rate_limit_wait(thread_id: int):
    """Wait if needed to respect rate limit for Selenium fallback: 1 product per 60 seconds (1 minute) per thread"""
    global _selenium_fallback_rate_limit_per_thread
    with RATE_LIMIT_LOCK:
        now = time.time()
        last_time = _selenium_fallback_rate_limit_per_thread.get(thread_id, 0)
        time_since_last = now - last_time
        
        if time_since_last < SELENIUM_FALLBACK_RATE_LIMIT_SECONDS:
            wait_time = SELENIUM_FALLBACK_RATE_LIMIT_SECONDS - time_since_last
            log.info(f"[SELENIUM_FALLBACK_RATE_LIMIT] Thread {thread_id}: waiting {wait_time:.2f}s (1 product per {SELENIUM_FALLBACK_RATE_LIMIT_SECONDS}s)")
            time.sleep(wait_time)
        
        _selenium_fallback_rate_limit_per_thread[thread_id] = time.time()

# ====== PROXY ======
def get_random_proxy() -> Optional[str]:
    """Get a random proxy from the list"""
    proxies = get_proxy_list()
    return random.choice(proxies) if proxies else None

# ====== UTILS ======

def ts() -> str:
    return datetime.now().isoformat(timespec="seconds")

def normalize_ws(s: Optional[str]) -> Optional[str]:
    if s is None: return None
    return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()

def strip_accents(s: str) -> str:
    import unicodedata as _u
    return "".join(ch for ch in _u.normalize("NFKD", s or "") if not _u.combining(ch))

def nk(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", strip_accents((s or "").strip())).lower()

def sanitize_product_name_for_url(product_name: str) -> str:
    """
    Sanitize product name for URL construction based on AlfaBeta URL patterns:
    - Remove special characters like +, /, etc. (keep only alphanumeric, spaces, and hyphens)
    - Replace spaces with hyphens
    - Convert to lowercase
    - Handle multiple consecutive hyphens (preserve double hyphens from + between words)
    - Format as productname.html
    
    Examples:
    - "+50" -> "50.html"
    - "BIOCLAVID 500/125" -> "bioclavid-500125.html"
    - "FILTRES FLUID COLORS FPS 50+" -> "filtres-fluid-colors-fps-50.html"
    - "3 TC + AZT ELEA" -> "3-tc--azt-elea.html"
    - "AEROTINA JARABE PEDIATRICO C/DOSIFICADOR" -> "aerotina-jarabe-pediatrico-cdosificador.html"
    """
    if not product_name:
        return ""
    
    # Remove accents and normalize
    sanitized = strip_accents(product_name)
    
    # Handle + character:
    # - If + is between words with spaces (like "TC + AZT"), replace with double space to create double hyphen
    # - If + is at start/end or attached (like "+50" or "50+"), just remove it
    sanitized = re.sub(r'\s+\+\s+', '  ', sanitized)  # "+" between spaces -> double space
    sanitized = re.sub(r'\+', '', sanitized)  # Remove remaining + characters
    
    # Remove special characters (keep only alphanumeric, spaces, and hyphens)
    # This removes /, @, #, etc. but keeps spaces and hyphens
    sanitized = re.sub(r'[^a-zA-Z0-9\s-]', '', sanitized)
    
    # Replace spaces with hyphens, preserving multiple spaces as multiple hyphens
    # First replace double spaces with a placeholder, then single spaces, then restore double hyphens
    sanitized = re.sub(r'  ', ' __DOUBLE__ ', sanitized)  # Preserve double spaces
    sanitized = re.sub(r'\s+', '-', sanitized)  # Replace all spaces (including single) with single hyphen
    sanitized = re.sub(r'__DOUBLE__', '-', sanitized)  # Restore double hyphens
    
    # Remove more than 2 consecutive hyphens (keep double hyphens, remove triple+)
    sanitized = re.sub(r'-{3,}', '--', sanitized)
    
    # Convert to lowercase
    sanitized = sanitized.lower()
    
    # Remove leading/trailing hyphens
    sanitized = sanitized.strip('-')
    
    # Format as productname.html
    if sanitized:
        return f"{sanitized}.html"
    return ""

def construct_product_url(product_name: str, base_url: str = None) -> str:
    """
    Construct product URL from product name.
    Format: https://www.alfabeta.net/precio/productname.html
    """
    if base_url is None:
        base_url = PRODUCTS_URL
    
    # Ensure base_url doesn't end with /
    base_url = base_url.rstrip('/')
    
    # Sanitize product name
    sanitized = sanitize_product_name_for_url(product_name)
    
    if not sanitized:
        return ""
    
    # Construct full URL
    return f"{base_url}/{sanitized}"

def human_pause():
    time.sleep(REQUEST_PAUSE_BASE + random.uniform(*REQUEST_PAUSE_JITTER))

def ar_money_to_float(s: str) -> Optional[float]:
    if not s: return None
    t = re.sub(r"[^\d\.,]", "", s.strip())
    if not t: return None
    # AR: dot thousands, comma decimals
    t = t.replace(".", "").replace(",", ".")
    try: return float(t)
    except ValueError: return None

def parse_date(s: str) -> Optional[str]:
    """Accepts '(24/07/25)' or '24/07/25' or '24-07-2025' → '2025-07-24'"""
    s = (s or "").strip()
    m = re.search(r"\((\d{2})/(\d{2})/(\d{2})\)", s) or re.search(r"\b(\d{2})/(\d{2})/(\d{2})\b", s)
    if m:
        d,mn,y = map(int, m.groups()); y += 2000
        try: return datetime(y,mn,d).date().isoformat()
        except: return None
    m = re.search(r"\b(\d{2})-(\d{2})-(\d{4})\b", s)
    if m:
        d,mn,y = map(int, m.groups())
        try: return datetime(y,mn,d).date().isoformat()
        except: return None
    return None

# ====== CSV IO ======

def ensure_headers():
    if not OUT_CSV.exists():
        with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=OUT_FIELDS).writeheader()
    if not PROGRESS.exists():
        with open(PROGRESS, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["input_company","input_product_name","timestamp","records_found"])
    if not ERRORS.exists():
        with open(ERRORS, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["input_company","input_product_name","timestamp","error_message"])

def load_progress_set() -> set:
    """Load products from progress file (alfabeta_progress.csv)."""
    done = set()
    if PROGRESS.exists():
        try:
            with open(PROGRESS, encoding="utf-8") as f:
                r = csv.DictReader(f)
                for row in r:
                    company = (row.get("input_company") or "").strip()
                    product = (row.get("input_product_name") or "").strip()
                    if company and product:
                        done.add((nk(company), nk(product)))
        except Exception as e:
            log.warning(f"[PROGRESS] Failed to load progress file: {e}")
    return done

def load_output_set() -> set:
    """Load products from output file (alfabeta_products_by_product.csv).
    Returns a set of normalized (company, product) tuples that already have data."""
    done = set()
    if OUT_CSV.exists():
        try:
            with open(OUT_CSV, encoding="utf-8-sig") as f:
                r = csv.DictReader(f)
                for row in r:
                    company = (row.get("input_company") or "").strip()
                    product = (row.get("input_product_name") or "").strip()
                    if company and product:
                        done.add((nk(company), nk(product)))
        except Exception as e:
            log.warning(f"[OUTPUT] Failed to load output file: {e}")
    return done

def load_ignore_list() -> set:
    """Load ignore list from input/Argentina/IGNORE_LIST_FILE (Company, Product format).
    Returns a set of normalized (company, product) tuples to skip."""
    ignore_set = set()
    ignore_file = INPUT_DIR / IGNORE_LIST_FILE
    if ignore_file.exists():
        try:
            with open(ignore_file, encoding="utf-8-sig") as f:
                r = csv.DictReader(f)
                headers = {nk(h): h for h in (r.fieldnames or [])}
                pcol = headers.get(nk("Product")) or headers.get("product") or "Product"
                ccol = headers.get(nk("Company")) or headers.get("company") or "Company"
                for row in r:
                    prod = (row.get(pcol) or "").strip()
                    comp = (row.get(ccol) or "").strip()
                    if prod and comp:
                        ignore_set.add((nk(comp), nk(prod)))
            log.info(f"[IGNORE_LIST] Loaded {len(ignore_set)} combinations from {IGNORE_LIST_FILE}")
        except Exception as e:
            log.warning(f"[IGNORE_LIST] Failed to load {IGNORE_LIST_FILE}: {e}")
    else:
        log.info(f"[IGNORE_LIST] No {IGNORE_LIST_FILE} found in {INPUT_DIR} (optional file)")
    return ignore_set

def append_progress(company: str, product: str, count: int):
    with PROGRESS_LOCK, open(PROGRESS, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([company, product, ts(), count])

def append_error(company: str, product: str, msg: str):
    with ERROR_LOCK, open(ERRORS, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([company, product, ts(), msg[:5000]])

def append_rows(rows: List[Dict[str, Any]]):
    if not rows: return
    with CSV_LOCK, open(OUT_CSV, "a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=OUT_FIELDS, extrasaction="ignore").writerows(rows)

def save_debug(driver, folder: Path, tag: str):
    try:
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        png = folder / f"{tag}_{stamp}.png"
        html = folder / f"{tag}_{stamp}.html"
        driver.save_screenshot(str(png))
        html.write_text(driver.page_source, encoding="utf-8")
    except Exception as e:
        log.warning(f"Could not save debug for {tag}: {e}")

def update_prepared_urls_source(company: str, product: str, new_source: str = "selenium"):
    """Update the Source column in Productlist_with_urls.csv to selenium when API returns null.
    Thread-safe: uses CSV_LOCK to prevent race conditions when multiple threads update the file.
    """
    if not PREPARED_URLS_FILE_PATH.exists():
        return  # File doesn't exist, skip update
    
    try:
        # Use lock for both read and write to make operation atomic
        with CSV_LOCK:
            # Read all rows
            rows = []
            with open(PREPARED_URLS_FILE_PATH, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                if not fieldnames:
                    return
                
                updated = False
                for row in reader:
                    # Normalize for comparison
                    row_company = (row.get("Company") or "").strip()
                    row_product = (row.get("Product") or "").strip()
                    
                    # Update source if match found
                    if nk(row_company) == nk(company) and nk(row_product) == nk(product):
                        if row.get("Source", "").lower() != new_source.lower():
                            row["Source"] = new_source
                            updated = True
                            log.info(f"[CSV_UPDATE] Updated source to '{new_source}' for {company} | {product}")
                    
                    rows.append(row)
            
            # Write back all rows only if update was made
            if updated:
                with open(PREPARED_URLS_FILE_PATH, "w", encoding="utf-8", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)
    except Exception as e:
        log.warning(f"[CSV_UPDATE] Failed to update source for {company} | {product}: {e}")

def is_captcha_page(driver) -> bool:
    """Check if current page is a captcha page."""
    try:
        page_source_lower = driver.page_source.lower()
        url_lower = driver.current_url.lower()
        
        # Check for common captcha indicators
        captcha_indicators = [
            "captcha",
            "recaptcha",
            "cloudflare",
            "challenge",
            "verify you are human",
            "access denied",
            "checking your browser"
        ]
        
        for indicator in captcha_indicators:
            if indicator in page_source_lower or indicator in url_lower:
                return True
        
        return False
    except Exception:
        return False

# ====== DRIVER / LOGIN ======

def setup_driver(headless=True, proxy_url: Optional[str] = None):
    opts = webdriver.ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    # cache mitigations
    opts.add_argument("--incognito")
    opts.add_argument("--disable-application-cache")
    opts.add_argument("--disk-cache-size=0")
    opts.add_argument(f"--disk-cache-dir={tempfile.mkdtemp(prefix='alfabeta-cache-')}")
    opts.add_argument(f"--user-data-dir={tempfile.mkdtemp(prefix='alfabeta-profile-')}")
    # stability
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--lang=es-AR")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Configure proxy if provided
    if proxy_url:
        try:
            proxy_info = parse_proxy_url(proxy_url)
            log.info(f"Using proxy: {proxy_info['host']}:{proxy_info['port']}")
            
            # For authenticated proxies, use Chrome extension
            if proxy_info['username'] and proxy_info['password']:
                import zipfile
                
                # Create a Chrome extension for proxy authentication
                manifest_json = """{
                    "version": "1.0.0",
                    "manifest_version": 2,
                    "name": "Chrome Proxy",
                    "permissions": [
                        "proxy",
                        "tabs",
                        "unlimitedStorage",
                        "storage",
                        "<all_urls>",
                        "webRequest",
                        "webRequestBlocking"
                    ],
                    "background": {
                        "scripts": ["background.js"]
                    },
                    "minimum_chrome_version":"22.0.0"
                }"""
                
                background_js = f"""
                var config = {{
                    mode: "fixed_servers",
                    rules: {{
                        singleProxy: {{
                            scheme: "{proxy_info['scheme']}",
                            host: "{proxy_info['host']}",
                            port: parseInt({proxy_info['port']})
                        }},
                        bypassList: ["localhost"]
                    }}
                }};
                chrome.proxy.settings.set({{value: config, scope: "regular"}}, function() {{}});
                function callbackFn(details) {{
                    return {{
                        authCredentials: {{
                            username: "{proxy_info['username']}",
                            password: "{proxy_info['password']}"
                        }}
                    }};
                }}
                chrome.webRequest.onAuthRequired.addListener(
                    callbackFn,
                    {{urls: ["<all_urls>"]}},
                    ['blocking']
                );
                """
                
                pluginfile = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')
                with zipfile.ZipFile(pluginfile.name, 'w') as zip_file:
                    zip_file.writestr("manifest.json", manifest_json)
                    zip_file.writestr("background.js", background_js)
                opts.add_extension(pluginfile.name)
            else:
                # Non-authenticated proxy
                proxy_server = f"{proxy_info['host']}:{proxy_info['port']}"
                opts.add_argument(f"--proxy-server={proxy_info['scheme']}://{proxy_server}")
        except Exception as e:
            log.warning(f"Failed to configure proxy {proxy_url}: {e}")
    
    drv = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    drv.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    
    # Register Chrome instance for cleanup tracking
    try:
        from core.chrome_manager import register_chrome_driver
        register_chrome_driver(drv)
    except ImportError:
        pass  # Chrome manager not available, continue without registration
    
    return drv

def is_login_page(driver) -> bool:
    try:
        return bool(driver.find_elements(By.ID, "usuario")) and bool(driver.find_elements(By.ID, "clave"))
    except Exception:
        return False

def do_login(driver, username: Optional[str] = None, password: Optional[str] = None):
    """Login with provided credentials or use defaults"""
    u = username or ALFABETA_USER
    p = password or ALFABETA_PASS
    try:
        # Track login URL
        login_url = driver.current_url
        log.info(f"[LOGIN_URL] Attempting login at: {login_url} (username: {u})")
        
        user = driver.find_element(By.ID, "usuario")
        pwd  = driver.find_element(By.ID, "clave")
        user.clear(); user.send_keys(u)
        pwd.clear();  pwd.send_keys(p)
        try:
            driver.find_element(By.XPATH, "//input[@value='Enviar']").click()
        except Exception:
            pwd.send_keys(Keys.ENTER)
        try:
            WebDriverWait(driver, WAIT_ALERT).until(EC.alert_is_present())
            Alert(driver).accept()
        except Exception:
            pass
        WebDriverWait(driver, WAIT_PAGE_LOAD).until(lambda d: not is_login_page(d))
        
        # Log successful login and final URL
        final_url = driver.current_url
        log.info(f"[LOGIN_SUCCESS] Login successful, redirected to: {final_url}")
    except Exception as e:
        login_url = driver.current_url if 'driver' in locals() else "unknown"
        log.error(f"[LOGIN_FAILED] Login failed at URL: {login_url} - {e}")
        raise RuntimeError(f"Login failed: {e}")

def go_hub_authenticated(driver, username: Optional[str] = None, password: Optional[str] = None):
    """Authenticate and navigate to hub with provided credentials"""
    log.info(f"[AUTH] Navigating to HUB_URL: {HUB_URL}")
    for attempt in range(MAX_RETRIES_AUTH):
        driver.get(HUB_URL)
        current_url = driver.current_url
        log.debug(f"[AUTH] Attempt {attempt + 1}/3 - Current URL: {current_url}")
        if is_login_page(driver):
            log.info(f"[AUTH] Login page detected at: {current_url}")
            do_login(driver, username, password)
            driver.get(HUB_URL)
            current_url = driver.current_url
            log.info(f"[AUTH] After login, navigated to: {current_url}")
        if not is_login_page(driver):
            log.info(f"[AUTH] Successfully authenticated, final URL: {driver.current_url}")
            return
    raise RuntimeError(f"Could not get authenticated access to HUB. Last URL: {driver.current_url}")

def guard_auth_and(func):
    def wrapper(driver, *a, username=None, password=None, **kw):
        if is_login_page(driver):
            go_hub_authenticated(driver, username, password)
        try:
            out = func(driver, *a, username=username, password=password, **kw)
        except Exception:
            if is_login_page(driver):
                go_hub_authenticated(driver, username, password)
                out = func(driver, *a, username=username, password=password, **kw)
            else:
                raise
        if is_login_page(driver):
            go_hub_authenticated(driver, username, password)
        return out
    return wrapper

# ====== SEARCH / RESULTS ======

@guard_auth_and
def search_in_products(driver, product_term: str, username=None, password=None):
    log.info(f"[SEARCH] Searching for product: {product_term}")
    go_hub_authenticated(driver, username, password)
    form = WebDriverWait(driver, WAIT_SEARCH_FORM).until(EC.presence_of_element_located((By.CSS_SELECTOR, "form#srvPr")))
    box = form.find_element(By.NAME, "patron")
    box.clear(); box.send_keys(product_term); box.send_keys(Keys.ENTER)
    log.debug(f"[SEARCH] Search submitted, waiting for results...")
    WebDriverWait(driver, WAIT_SEARCH_RESULTS).until(lambda d: d.find_elements(By.CSS_SELECTOR, "a.rprod, form[name^='pr']"))
    log.debug(f"[SEARCH] Search results loaded")

def enumerate_pairs(driver) -> List[Dict[str, Any]]:
    out = []
    for a in driver.find_elements(By.CSS_SELECTOR, "a.rprod"):
        prod_txt = normalize_ws(a.text) or ""
        href = a.get_attribute("href") or ""
        m = re.search(r"document\.(pr\d+)\.submit", href)
        pr_form = m.group(1) if m else None
        comp_txt = ""
        try:
            rlab = a.find_element(By.XPATH, "following-sibling::a[contains(@class,'rlab')][1]")
            comp_txt = normalize_ws(rlab.text) or ""
        except NoSuchElementException:
            pass
        out.append({"prod": prod_txt, "comp": comp_txt, "pr_form": pr_form})
    return out

@guard_auth_and
def open_exact_pair(driver, product: str, company: str, username=None, password=None) -> bool:
    rows = enumerate_pairs(driver)
    matches = [r for r in rows if nk(r["prod"]) == nk(product) and nk(r["comp"]) == nk(company)]
    if not matches: return False
    pr = matches[0]["pr_form"]
    if not pr: return False
    driver.execute_script(f"if (document.{pr}) document.{pr}.submit();")
    WebDriverWait(driver, WAIT_PAGE_LOAD).until(
        lambda d: "presentacion" in d.page_source.lower() or d.find_elements(By.CSS_SELECTOR, "tr.lproducto span.tproducto")
    )
    return True

# ====== SCRAPINGDOG API FOR SINGLE PRODUCTS ======

def parse_html_content(html_content: str, in_company: str, in_product: str) -> List[Dict[str, Any]]:
    """
    Parse HTML content from ScrapingDog API response and extract product rows.
    Uses BeautifulSoup if available, otherwise falls back to Selenium.
    """
    if BEAUTIFULSOUP_AVAILABLE:
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            return parse_html_with_bs4(soup, in_company, in_product)
        except Exception as e:
            log.warning(f"[API] BeautifulSoup parsing failed: {e}, falling back to Selenium")
    
    # Fallback: Use Selenium to load HTML
    try:
        driver = setup_driver(headless=True)
        # Write HTML to temporary file and load it
        with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8') as f:
            f.write(html_content)
            temp_file = f.name
        
        try:
            # Load HTML file using file:// protocol
            file_url = f"file:///{temp_file.replace(chr(92), '/')}"  # Replace backslashes for Windows
            driver.get(file_url)
            # Wait a bit for page to load
            time.sleep(PAUSE_HTML_LOAD)
            rows = extract_rows_from_driver(driver, in_company, in_product)
        finally:
            # Clean up temp file
            try:
                os.unlink(temp_file)
            except Exception:
                pass
        
        driver.quit()
        return rows
    except Exception as e:
        log.error(f"[API] Failed to parse HTML with Selenium: {e}")
        return []

def parse_html_with_bs4(soup, in_company: str, in_product: str) -> List[Dict[str, Any]]:
    """
    Parse HTML using BeautifulSoup and extract product information.
    This is a simplified version - you may need to adjust selectors based on actual HTML structure.
    """
    rows: List[Dict[str, Any]] = []
    
    try:
        # Extract header/meta information
        active_elem = soup.select_one("tr.sproducto td.textoe i")
        active = normalize_ws(active_elem.get_text()) if active_elem else None
        
        therap_elem = soup.select_one("tr.sproducto td.textor i")
        therap = normalize_ws(therap_elem.get_text()) if therap_elem else None
        
        comp_elem = soup.select_one("tr.lproducto td.textor .defecto") or soup.select_one("td.textoe b")
        comp = normalize_ws(comp_elem.get_text()) if comp_elem else None
        
        pname_elem = soup.select_one("tr.lproducto span.tproducto")
        pname = normalize_ws(pname_elem.get_text()) if pname_elem else None
        
        # Extract presentation rows
        pres_tables = soup.select("td.dproducto > table.presentacion")
        
        for p in pres_tables:
            desc_elem = p.select_one("td.tddesc")
            desc = normalize_ws(desc_elem.get_text()) if desc_elem else None
            
            price_elem = p.select_one("td.tdprecio")
            price = normalize_ws(price_elem.get_text()) if price_elem else None
            
            datev_elem = p.select_one("td.tdfecha")
            datev = normalize_ws(datev_elem.get_text()) if datev_elem else None
            
            import_elem = p.select_one("td.import")
            import_status = normalize_ws(import_elem.get_text()) if import_elem else None
            
            # Parse coverage (simplified - you may need to enhance this)
            cov = {}
            try:
                cob_table = p.select_one("table.coberturas")
                if cob_table:
                    # Basic coverage parsing - you may need to enhance this
                    for tr in cob_table.select("tr"):
                        payer_elem = tr.select_one("td.obrasn")
                        if payer_elem:
                            payer_text = normalize_ws(payer_elem.get_text())
                            if payer_text:
                                current_payer = strip_accents(payer_text).upper()
                                cov.setdefault(current_payer, {})
                                
                                detail_elem = tr.select_one("td.obrasd")
                                if detail_elem:
                                    detail = normalize_ws(detail_elem.get_text())
                                    if detail:
                                        cov[current_payer]["detail"] = detail
            except Exception as e:
                log.debug(f"[API] Coverage parsing error: {e}")
            
            rows.append({
                "input_company": in_company,
                "input_product_name": in_product,
                "company": comp,
                "product_name": pname,
                "active_ingredient": active,
                "therapeutic_class": therap,
                "description": desc,
                "price_ars": ar_money_to_float(price or ""),
                "date": parse_date(datev or ""),
                "scraped_at": ts(),
                "SIFAR_detail": (cov.get("SIFAR") or {}).get("detail"),
                "PAMI_AF": (cov.get("PAMI") or {}).get("AF"),
                "IOMA_detail": (cov.get("IOMA") or {}).get("detail"),
                "IOMA_AF": (cov.get("IOMA") or {}).get("AF"),
                "IOMA_OS": (cov.get("IOMA") or {}).get("OS"),
                "import_status": import_status,
                "coverage_json": json.dumps(cov, ensure_ascii=False)
            })
        
        # Fallback if no presentation rows found
        if not rows:
            rows.append({
                "input_company": in_company,
                "input_product_name": in_product,
                "company": comp,
                "product_name": pname,
                "active_ingredient": active,
                "therapeutic_class": therap,
                "description": None,
                "price_ars": None,
                "date": None,
                "scraped_at": ts(),
                "SIFAR_detail": None, "PAMI_AF": None, "IOMA_detail": None, "IOMA_AF": None, "IOMA_OS": None,
                "import_status": None,
                "coverage_json": "{}"
            })
    except Exception as e:
        log.error(f"[API] Error parsing HTML with BeautifulSoup: {e}")
    
    return rows

def extract_rows_from_driver(driver, in_company: str, in_product: str) -> List[Dict[str, Any]]:
    """
    Extract rows from Selenium driver without authentication guard.
    This is used when loading HTML directly from API.
    """
    # Header/meta from the product page
    active = get_text_safe(driver, "tr.sproducto td.textoe i")
    therap = get_text_safe(driver, "tr.sproducto td.textor i")
    comp   = get_text_safe(driver, "tr.lproducto td.textor .defecto") or \
             get_text_safe(driver, "td.textoe b")
    pname  = get_text_safe(driver, "tr.lproducto span.tproducto")

    rows: List[Dict[str, Any]] = []
    pres = driver.find_elements(By.CSS_SELECTOR, "td.dproducto > table.presentacion")
    for p in pres:
        desc  = get_text_safe(p, "td.tddesc")
        price = get_text_safe(p, "td.tdprecio")
        datev = get_text_safe(p, "td.tdfecha")

        import_status = get_text_safe(p, "td.import")
        cov = collect_coverage(p)

        rows.append({
            "input_company": in_company,
            "input_product_name": in_product,
            "company": comp,
            "product_name": pname,
            "active_ingredient": active,
            "therapeutic_class": therap,
            "description": desc,
            "price_ars": ar_money_to_float(price or ""),
            "date": parse_date(datev or ""),
            "scraped_at": ts(),
            "SIFAR_detail": (cov.get("SIFAR") or {}).get("detail"),
            "PAMI_AF":      (cov.get("PAMI")  or {}).get("AF"),
            "IOMA_detail":  (cov.get("IOMA")  or {}).get("detail"),
            "IOMA_AF":      (cov.get("IOMA")  or {}).get("AF"),
            "IOMA_OS":      (cov.get("IOMA")  or {}).get("OS"),
            "import_status": import_status,
            "coverage_json": json.dumps(cov, ensure_ascii=False)
        })

    # Fallback if no presentation rows found
    if not rows:
        rows.append({
            "input_company": in_company,
            "input_product_name": in_product,
            "company": comp,
            "product_name": pname,
            "active_ingredient": active,
            "therapeutic_class": therap,
            "description": None,
            "price_ars": None,
            "date": None,
            "scraped_at": ts(),
            "SIFAR_detail": None, "PAMI_AF": None, "IOMA_detail": None, "IOMA_AF": None, "IOMA_OS": None,
            "import_status": None,
            "coverage_json": "{}"
        })
    return rows

def scrape_single_product_api(product_name: str, company: str) -> List[Dict[str, Any]]:
    """
    Scrape a single product using scrapingdog API.
    Constructs URL directly from product name: productname.html
    """
    # Construct URL from product name
    product_url = construct_product_url(product_name)
    if not product_url:
        log.warning(f"[API] Could not construct URL for product: {product_name}")
        return []
    
    return scrape_single_product_api_with_url(product_url, product_name, company)

def scrape_single_product_api_with_url(product_url: str, product_name: str, company: str) -> List[Dict[str, Any]]:
    """
    Scrape a single product using scrapingdog API with a prepared URL.
    """
    if not REQUESTS_AVAILABLE:
        log.error("[API] requests library not available. Install with: pip install requests")
        return []
    
    if not SCRAPINGDOG_API_KEY:
        log.warning("[API] SCRAPINGDOG_API_KEY not configured")
        return []
    
    if not product_url:
        log.warning(f"[API] No URL provided for product: {product_name}")
        return []
    
    log.info(f"[API] Using URL for {product_name}: {product_url}")
    
    try:
        params = {
            "api_key": SCRAPINGDOG_API_KEY,
            "url": product_url,
            "dynamic": "true"
        }
        response = requests.get(SCRAPINGDOG_URL, params=params, timeout=API_REQUEST_TIMEOUT)
        
        if response.status_code == 200:
            html_content = response.text
            log.info(f"[API] Successfully fetched HTML for {product_name}")
            # Parse HTML content
            rows = parse_html_content(html_content, company, product_name)
            return rows
        else:
            log.warning(f"[API] API request failed with status {response.status_code} for {product_name}")
            return []
    except Exception as e:
        log.error(f"[API] Error fetching product {product_name}: {e}")
        return []

# ====== PRODUCT PAGE PARSING ======

def get_text_safe(root, css):
    try:
        el = root.find_element(By.CSS_SELECTOR, css)
        txt = el.get_attribute("innerText")
        if not txt:
            txt = el.get_attribute("innerHTML")
        return normalize_ws(txt)
    except Exception:
        return None

def collect_coverage(pres_el) -> Dict[str, Any]:
    """Robust coverage parser: normalizes payer keys and reads innerHTML to catch AF/OS in <b> tags."""
    cov: Dict[str, Any] = {}
    try:
        cob = pres_el.find_element(By.CSS_SELECTOR, "table.coberturas")
    except Exception:
        return cov

    current_payer = None
    for tr in cob.find_elements(By.CSS_SELECTOR, "tr"):
        # Payer name (fallback to innerHTML)
        try:
            payer_el = tr.find_element(By.CSS_SELECTOR, "td.obrasn")
            payer_text = normalize_ws(payer_el.get_attribute("innerText")) or normalize_ws(payer_el.get_attribute("innerHTML"))
            if payer_text:
                current_payer = strip_accents(payer_text).upper()
                cov.setdefault(current_payer, {})
        except Exception:
            pass

        # Detail/description
        try:
            detail = normalize_ws(tr.find_element(By.CSS_SELECTOR, "td.obrasd").get_attribute("innerText"))
            if current_payer and detail:
                cov[current_payer]["detail"] = detail
        except Exception:
            pass

        # Amounts: check both left/right amount cells, use innerText first
        for sel in ("td.importesi", "td.importesd"):
            try:
                txt = tr.find_element(By.CSS_SELECTOR, sel).get_attribute("innerText")
                if not txt:
                    txt = tr.find_element(By.CSS_SELECTOR, sel).get_attribute("innerHTML")
                    txt = re.sub(r'<[^>]*>', '', txt)
                for tag, amt in re.findall(r"(AF|OS)[^<]*?[\$]?([\d\.,]+)", txt or "", flags=re.I):
                    val = ar_money_to_float(amt)
                    if val is not None and current_payer:
                        cov[current_payer][tag.upper()] = val
            except Exception:
                pass
    return cov

@guard_auth_and
def extract_rows(driver, in_company, in_product, username=None, password=None):
    # Header/meta from the product page
    active = get_text_safe(driver, "tr.sproducto td.textoe i")           # active_ingredient
    therap = get_text_safe(driver, "tr.sproducto td.textor i")           # therapeutic_class
    comp   = get_text_safe(driver, "tr.lproducto td.textor .defecto") or \
             get_text_safe(driver, "td.textoe b")                        # company
    pname  = get_text_safe(driver, "tr.lproducto span.tproducto")        # product_name

    rows: List[Dict[str, Any]] = []
    pres = driver.find_elements(By.CSS_SELECTOR, "td.dproducto > table.presentacion")
    for p in pres:
        desc  = get_text_safe(p, "td.tddesc")
        price = get_text_safe(p, "td.tdprecio")
        datev = get_text_safe(p, "td.tdfecha")

        import_status = get_text_safe(p, "td.import")  # may be None
        cov = collect_coverage(p)

        rows.append({
            "input_company": in_company,
            "input_product_name": in_product,
            "company": comp,
            "product_name": pname,
            "active_ingredient": active,
            "therapeutic_class": therap,
            "description": desc,
            "price_ars": ar_money_to_float(price or ""),
            "date": parse_date(datev or ""),
            "scraped_at": ts(),

            # Five priority coverage fields
            "SIFAR_detail": (cov.get("SIFAR") or {}).get("detail"),
            "PAMI_AF":      (cov.get("PAMI")  or {}).get("AF"),
            "IOMA_detail":  (cov.get("IOMA")  or {}).get("detail"),
            "IOMA_AF":      (cov.get("IOMA")  or {}).get("AF"),
            "IOMA_OS":      (cov.get("IOMA")  or {}).get("OS"),

            "import_status": import_status,
            "coverage_json": json.dumps(cov, ensure_ascii=False)
        })

    # Fallback if no presentation rows found
    if not rows:
        rows.append({
            "input_company": in_company,
            "input_product_name": in_product,
            "company": comp,
            "product_name": pname,
            "active_ingredient": active,
            "therapeutic_class": therap,
            "description": None,
            "price_ars": None,
            "date": None,
            "scraped_at": ts(),
            "SIFAR_detail": None, "PAMI_AF": None, "IOMA_detail": None, "IOMA_AF": None, "IOMA_OS": None,
            "import_status": None,
            "coverage_json": "{}"
        })
    return rows

# ====== WORKER ======

def rotate_account(driver, current_account_idx: int, proxy: Optional[str], headless: bool) -> Tuple[Any, int, str, str]:
    """Close current driver, switch to next account, create new driver and login.
    Returns: (new_driver, new_account_idx, username, password)
    """
    # Close old driver
    try:
        driver.quit()
    except Exception:
        pass
    
    # Switch to next account (round-robin)
    next_account_idx = (current_account_idx + 1) % len(ACCOUNTS)
    username, password = ACCOUNTS[next_account_idx]
    
    log.info(f"[ACCOUNT_ROTATION] Switching from account {current_account_idx + 1} to account {next_account_idx + 1} (username: {username})")
    log.info(f"[ACCOUNT_ROTATION] Will use HUB_URL: {HUB_URL}")
    
    # Create new driver
    new_driver = setup_driver(headless=headless, proxy_url=proxy)
    
    # Login with new account
    go_hub_authenticated(new_driver, username, password)
    
    # Log final URL after account rotation
    final_url = new_driver.current_url
    log.info(f"[ACCOUNT_ROTATION] Account rotation complete, final URL: {final_url}")
    
    return new_driver, next_account_idx, username, password

# ====== API WORKER (5 threads) ======

def api_worker(api_queue: Queue, selenium_queue: Queue, args, skip_set: set):
    """API worker: processes single products via API, adds null results to Selenium queue"""
    thread_id = threading.get_ident()
    log.info(f"[API_WORKER] Thread {thread_id} started")
    
    while True:
        try:
            item = api_queue.get(timeout=QUEUE_GET_TIMEOUT)
            # Format: (product, company, is_duplicate, url)
            if len(item) == 4:
                in_product, in_company, is_duplicate, product_url = item
            else:
                in_product, in_company, is_duplicate, product_url = item[0], item[1], False, construct_product_url(item[0])
        except Empty:
            break
        
        try:
            if (nk(in_company), nk(in_product)) in skip_set:
                log.debug(f"[API_WORKER] [SKIPPED] {in_company} | {in_product} (already processed)")
                continue
            
            log.info(f"[API_WORKER] Processing: {in_company} | {in_product}")
            
            # Use API to scrape
            rows = scrape_single_product_api_with_url(product_url, in_product, in_company)
            
            if rows:
                # API succeeded - save results
                append_rows(rows)
                append_progress(in_company, in_product, len(rows))
                log.info(f"[API_WORKER] [SUCCESS] {in_company} | {in_product} → {len(rows)} rows")
            else:
                # API returned null - update CSV source to selenium and add to Selenium queue
                log.warning(f"[API_WORKER] [NULL] API returned null for {in_company} | {in_product}, updating source to selenium and adding to Selenium queue")
                update_prepared_urls_source(in_company, in_product, "selenium")
                selenium_queue.put((in_product, in_company, product_url))
            
            # Apply API rate limit
            rate_limit_wait()
            
        except Exception as e:
            log.error(f"[API_WORKER] [ERROR] {in_company} | {in_product}: {e}")
            append_error(in_company, in_product, f"API Worker Error: {e}")
            rate_limit_wait()
        finally:
            api_queue.task_done()
    
    log.info(f"[API_WORKER] Thread {thread_id} finished")

# ====== SELENIUM WORKER ======

def selenium_worker(selenium_queue: Queue, args, skip_set: set):
    """Selenium worker: processes duplicates + API null results with 1 minute interval"""
    proxy = get_random_proxy()
    thread_id = threading.get_ident()
    
    # Initialize with first account
    account_idx = 0
    if not ACCOUNTS:
        raise RuntimeError("No accounts available")
    username, password = ACCOUNTS[account_idx]
    log.info(f"[SELENIUM_WORKER] Thread {thread_id} started with account {account_idx + 1}/{len(ACCOUNTS)} (username: {username})")
    
    driver = setup_driver(headless=args.headless, proxy_url=proxy)
    go_hub_authenticated(driver, username, password)
    
    search_count = 0
    
    while True:
        search_attempted = False
        try:
            item = selenium_queue.get(timeout=QUEUE_GET_TIMEOUT)
            # Format: (product, company, url) - from API null results
            # OR: (product, company, is_duplicate, url) - from duplicate products
            if len(item) == 4:
                in_product, in_company, is_duplicate, product_url = item
            elif len(item) == 3:
                in_product, in_company, product_url = item
                is_duplicate = False  # API null result
            else:
                in_product, in_company = item[0], item[1]
                product_url = construct_product_url(in_product)
                is_duplicate = True
        except Empty:
            break
        
        try:
            if psutil:
                try:
                    if psutil.cpu_percent(interval=0.1) > CPU_THROTTLE_HIGH:
                        time.sleep(PAUSE_CPU_THROTTLE)
                except Exception:
                    pass
            
            if (nk(in_company), nk(in_product)) in skip_set:
                log.debug(f"[SELENIUM_WORKER] [SKIPPED] {in_company} | {in_product} (already processed)")
                selenium_queue.task_done()
                continue
            
            product_type = "API_NULL" if len(item) == 3 else "DUPLICATE"
            log.info(f"[SELENIUM_WORKER] [SEARCH_START] [{product_type}] {in_company} | {in_product} (search #{search_count + 1})")
            search_attempted = True
            
            # Apply 1 minute rate limit per product
            selenium_fallback_rate_limit_wait(thread_id)
            
            # Check if we need to rotate account
            if search_count >= SELENIUM_ROTATION_LIMIT:
                driver, account_idx, username, password = rotate_account(driver, account_idx, proxy, args.headless)
                search_count = 0
            
            # Check for captcha and rotate account if detected
            if is_captcha_page(driver):
                log.warning(f"[SELENIUM_WORKER] [CAPTCHA_DETECTED] Captcha detected for {in_company} | {in_product}, rotating account")
                driver, account_idx, username, password = rotate_account(driver, account_idx, proxy, args.headless)
                search_count = 0  # Reset search count after rotation
            
            # Retry logic for TimeoutException
            max_retries = MAX_RETRIES_TIMEOUT
            retry_count = 0
            success = False
            
            while retry_count <= max_retries and not success:
                try:
                    if retry_count > 0:
                        log.info(f"[SELENIUM_WORKER] [RETRY {retry_count}/{max_retries}] {in_company} | {in_product}")
                        time.sleep(PAUSE_RETRY)
                        try:
                            go_hub_authenticated(driver, username, password)
                        except Exception:
                            pass
                    
                    search_in_products(driver, in_product, username=username, password=password)
                    
                    # Check for captcha after search and rotate if detected
                    if is_captcha_page(driver):
                        log.warning(f"[SELENIUM_WORKER] [CAPTCHA_DETECTED] Captcha detected after search for {in_company} | {in_product}, rotating account")
                        driver, account_idx, username, password = rotate_account(driver, account_idx, proxy, args.headless)
                        search_count = 0
                        # Retry the search with new account
                        search_in_products(driver, in_product, username=username, password=password)
                    
                    if not open_exact_pair(driver, in_product, in_company, username=username, password=password):
                        save_debug(driver, DEBUG_NF, f"{in_company}_{in_product}")
                        append_progress(in_company, in_product, 0)
                        log.info(f"[SELENIUM_WORKER] [NOT_FOUND] {in_company} | {in_product}")
                        success = True
                        search_count += 1
                        break
                    
                    # Check for captcha after opening product page
                    if is_captcha_page(driver):
                        log.warning(f"[SELENIUM_WORKER] [CAPTCHA_DETECTED] Captcha detected on product page for {in_company} | {in_product}, rotating account")
                        driver, account_idx, username, password = rotate_account(driver, account_idx, proxy, args.headless)
                        search_count = 0
                        # Retry the entire flow with new account
                        search_in_products(driver, in_product, username=username, password=password)
                        if not open_exact_pair(driver, in_product, in_company, username=username, password=password):
                            save_debug(driver, DEBUG_NF, f"{in_company}_{in_product}")
                            append_progress(in_company, in_product, 0)
                            log.info(f"[SELENIUM_WORKER] [NOT_FOUND] {in_company} | {in_product}")
                            success = True
                            search_count += 1
                            break
                    
                    rows = extract_rows(driver, in_company, in_product, username=username, password=password)
                    if rows:
                        append_rows(rows)
                        append_progress(in_company, in_product, len(rows))
                        log.info(f"[SELENIUM_WORKER] [SUCCESS] {in_company} | {in_product} → {len(rows)} rows")
                    else:
                        save_debug(driver, DEBUG_NF, f"{in_company}_{in_product}")
                        append_progress(in_company, in_product, 0)
                        log.info(f"[SELENIUM_WORKER] [NOT_FOUND] (0 rows) {in_company} | {in_product}")
                    success = True
                    search_count += 1
                    
                except TimeoutException as te:
                    retry_count += 1
                    if retry_count > max_retries:
                        log.error(f"[SELENIUM_WORKER] [TIMEOUT] {in_company} | {in_product} - All {max_retries} retries exhausted")
                        search_count += 1
                        raise
                    log.warning(f"[SELENIUM_WORKER] [TIMEOUT] {in_company} | {in_product} - Retry {retry_count}/{max_retries}")
                except Exception as e:
                    raise
                    
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            append_error(in_company, in_product, msg)
            save_debug(driver, DEBUG_ERR, f"{in_company}_{in_product}")
            log.error(f"[SELENIUM_WORKER] [ERROR] {in_company} | {in_product}: {msg}")
            search_count += 1
        finally:
            selenium_queue.task_done()
            if search_attempted:
                human_pause()
    
    try:
        driver.quit()
    except Exception:
        pass
    log.info(f"[SELENIUM_WORKER] Thread {thread_id} finished")

# ====== LEGACY WORKER (for backward compatibility, can be removed later) ======

def worker(q: Queue, args, skip_set: set):
    proxy = get_random_proxy()
    thread_id = threading.get_ident()
    
    # Initialize with first account
    account_idx = 0
    if not ACCOUNTS:
        raise RuntimeError("No accounts available")
    username, password = ACCOUNTS[account_idx]
    log.info(f"[WORKER] Initializing with account {account_idx + 1}/{len(ACCOUNTS)} (username: {username})")
    log.info(f"[WORKER] Will use HUB_URL: {HUB_URL}")
    
    driver = setup_driver(headless=args.headless, proxy_url=proxy)
    go_hub_authenticated(driver, username, password)
    
    # Log initial URL after authentication
    initial_url = driver.current_url
    log.info(f"[WORKER] Initial authentication complete, URL: {initial_url}")
    
    search_count = 0
    
    while True:
        search_attempted = False
        try:
            item = q.get(timeout=QUEUE_GET_TIMEOUT)
            # Handle formats:
            # - (product, company, is_duplicate, url) - new format with prepared URLs
            # - (product, company, is_duplicate) - old format
            # - (product, company) - legacy format
            if len(item) == 4:
                in_product, in_company, is_duplicate, product_url = item
            elif len(item) == 3:
                in_product, in_company, is_duplicate = item
                product_url = construct_product_url(in_product)  # Construct URL if not provided
            else:
                in_product, in_company = item
                is_duplicate = True  # Default to duplicate for backward compatibility
                product_url = construct_product_url(in_product)  # Construct URL if not provided
        except Empty:
            break
        try:
            if psutil:
                try:
                    if psutil.cpu_percent(interval=0.1) > CPU_THROTTLE_HIGH:
                        time.sleep(PAUSE_CPU_THROTTLE)
                except Exception:
                    pass

            if (nk(in_company), nk(in_product)) in skip_set:
                log.debug(f"[SKIPPED] {in_company} | {in_product} (already processed)")
                q.task_done()
                # Don't apply rate limit for skipped products
                continue
            
            product_type = "DUPLICATE" if is_duplicate else "SINGLE"
            log.info(f"[SEARCH_START] [{product_type}] {in_company} | {in_product} (search #{search_count + 1})")
            search_attempted = True
            
            # Check if we need to rotate account
            # Use different limits: 200 for Selenium (duplicates), 100 for API (singles)
            rotation_limit = ACCOUNT_ROTATION_SEARCH_LIMIT if is_duplicate else ACCOUNT_ROTATION_SEARCH_LIMIT_API
            if search_count >= rotation_limit:
                driver, account_idx, username, password = rotate_account(driver, account_idx, proxy, args.headless)
                search_count = 0
            
            # Check for captcha and rotate account if detected
            if is_captcha_page(driver):
                log.warning(f"[WORKER] [CAPTCHA_DETECTED] Captcha detected for {in_company} | {in_product}, rotating account")
                driver, account_idx, username, password = rotate_account(driver, account_idx, proxy, args.headless)
                search_count = 0  # Reset search count after rotation

            # Handle single products with API
            if not is_duplicate:
                try:
                    # For single products: use prepared URL and API
                    log.info(f"[API] Processing single product via API: {in_product}")
                    log.info(f"[API] Using URL: {product_url}")
                    
                    # Use API to scrape directly (use prepared URL)
                    rows = scrape_single_product_api_with_url(product_url, in_product, in_company)
                    
                    if rows:
                        append_rows(rows)
                        append_progress(in_company, in_product, len(rows))
                        log.info(f"[SUCCESS] [API] {in_company} | {in_product} → {len(rows)}")
                    else:
                        # Fallback to Selenium if API returns null/empty
                        log.warning(f"[API] API returned no results (null), updating source to selenium and falling back to Selenium for {in_company} | {in_product}")
                        update_prepared_urls_source(in_company, in_product, "selenium")
                        # Apply rate limit: 1 minute per product per thread for Selenium fallback
                        selenium_fallback_rate_limit_wait(thread_id)
                        search_in_products(driver, in_product, username=username, password=password)
                        
                        # Check for captcha after search and rotate if detected
                        if is_captcha_page(driver):
                            log.warning(f"[WORKER] [CAPTCHA_DETECTED] Captcha detected after search for {in_company} | {in_product}, rotating account")
                            driver, account_idx, username, password = rotate_account(driver, account_idx, proxy, args.headless)
                            search_count = 0
                            # Retry the search with new account
                            search_in_products(driver, in_product, username=username, password=password)
                        if not open_exact_pair(driver, in_product, in_company, username=username, password=password):
                            save_debug(driver, DEBUG_NF, f"{in_company}_{in_product}")
                            append_progress(in_company, in_product, 0)
                            log.info(f"[NOT_FOUND] {in_company} | {in_product}")
                        else:
                            # Check for captcha after opening product page
                            if is_captcha_page(driver):
                                log.warning(f"[WORKER] [CAPTCHA_DETECTED] Captcha detected on product page for {in_company} | {in_product}, rotating account")
                                driver, account_idx, username, password = rotate_account(driver, account_idx, proxy, args.headless)
                                search_count = 0
                                # Retry the entire flow with new account
                                search_in_products(driver, in_product, username=username, password=password)
                                if not open_exact_pair(driver, in_product, in_company, username=username, password=password):
                                    save_debug(driver, DEBUG_NF, f"{in_company}_{in_product}")
                                    append_progress(in_company, in_product, 0)
                                    log.info(f"[NOT_FOUND] {in_company} | {in_product}")
                                else:
                                    rows = extract_rows(driver, in_company, in_product, username=username, password=password)
                                    if rows:
                                        append_rows(rows)
                                        append_progress(in_company, in_product, len(rows))
                                        log.info(f"[SUCCESS] [SELENIUM_FALLBACK] {in_company} | {in_product} → {len(rows)}")
                                    else:
                                        save_debug(driver, DEBUG_NF, f"{in_company}_{in_product}")
                                        append_progress(in_company, in_product, 0)
                                        log.info(f"[NOT_FOUND] (0 rows) {in_company} | {in_product}")
                            else:
                                rows = extract_rows(driver, in_company, in_product, username=username, password=password)
                                if rows:
                                    append_rows(rows)
                                    append_progress(in_company, in_product, len(rows))
                                    log.info(f"[SUCCESS] [SELENIUM_FALLBACK] {in_company} | {in_product} → {len(rows)}")
                                else:
                                    save_debug(driver, DEBUG_NF, f"{in_company}_{in_product}")
                                    append_progress(in_company, in_product, 0)
                                    log.info(f"[NOT_FOUND] (0 rows) {in_company} | {in_product}")
                    
                    search_count += 1
                    rate_limit_wait()  # API rate limit
                    continue
                except Exception as e:
                    log.error(f"[API] Error processing single product {in_company} | {in_product}: {e}")
                    append_error(in_company, in_product, f"API Error: {e}")
                    search_count += 1
                    rate_limit_wait()
                    continue

            # For duplicates: apply 1 per minute rate limit per thread
            duplicate_rate_limit_wait(thread_id)

            # Retry logic for TimeoutException (duplicates only)
            max_retries = MAX_RETRIES_TIMEOUT
            retry_count = 0
            success = False
            last_exception = None
            
            while retry_count <= max_retries and not success:
                try:
                    if retry_count > 0:
                        log.info(f"[RETRY {retry_count}/{max_retries}] {in_company} | {in_product}")
                        time.sleep(PAUSE_RETRY)  # Wait before retry (align with rate limit)
                        # Try to refresh/re-authenticate on retry
                        try:
                            go_hub_authenticated(driver, username, password)
                        except Exception:
                            pass  # Continue even if re-auth fails
                    
                    search_in_products(driver, in_product, username=username, password=password)
                    
                    # Check for captcha after search and rotate if detected
                    if is_captcha_page(driver):
                        log.warning(f"[WORKER] [CAPTCHA_DETECTED] Captcha detected after search for {in_company} | {in_product}, rotating account")
                        driver, account_idx, username, password = rotate_account(driver, account_idx, proxy, args.headless)
                        search_count = 0
                        # Retry the search with new account
                        search_in_products(driver, in_product, username=username, password=password)
                    
                    if not open_exact_pair(driver, in_product, in_company, username=username, password=password):
                        save_debug(driver, DEBUG_NF, f"{in_company}_{in_product}")
                        append_progress(in_company, in_product, 0)
                        log.info(f"[NOT_FOUND] {in_company} | {in_product}")
                        success = True
                        search_count += 1
                        break
                    
                    # Check for captcha after opening product page
                    if is_captcha_page(driver):
                        log.warning(f"[WORKER] [CAPTCHA_DETECTED] Captcha detected on product page for {in_company} | {in_product}, rotating account")
                        driver, account_idx, username, password = rotate_account(driver, account_idx, proxy, args.headless)
                        search_count = 0
                        # Retry the entire flow with new account
                        search_in_products(driver, in_product, username=username, password=password)
                        if not open_exact_pair(driver, in_product, in_company, username=username, password=password):
                            save_debug(driver, DEBUG_NF, f"{in_company}_{in_product}")
                            append_progress(in_company, in_product, 0)
                            log.info(f"[NOT_FOUND] {in_company} | {in_product}")
                            success = True
                            search_count += 1
                            break

                    rows = extract_rows(driver, in_company, in_product, username=username, password=password)
                    if rows:
                        append_rows(rows)
                        append_progress(in_company, in_product, len(rows))
                        log.info(f"[SUCCESS] {in_company} | {in_product} → {len(rows)}")
                    else:
                        save_debug(driver, DEBUG_NF, f"{in_company}_{in_product}")
                        append_progress(in_company, in_product, 0)
                        log.info(f"[NOT_FOUND] (0 rows) {in_company} | {in_product}")
                    success = True
                    search_count += 1
                    
                except TimeoutException as te:
                    last_exception = te
                    retry_count += 1
                    if retry_count > max_retries:
                        log.error(f"[TIMEOUT] {in_company} | {in_product} - All {max_retries} retries exhausted")
                        search_count += 1
                        raise  # Re-raise if max retries exceeded
                    log.warning(f"[TIMEOUT] {in_company} | {in_product} - Retry {retry_count}/{max_retries}")
                except Exception as e:
                    # For non-timeout exceptions, don't retry
                    raise
                    
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            append_error(in_company, in_product, msg)
            save_debug(driver, DEBUG_ERR, f"{in_company}_{in_product}")
            log.error(f"[ERROR] {in_company} | {in_product}: {msg}")
            # Increment search count even on error (after all retries exhausted)
            search_count += 1
        finally:
            q.task_done()
            # Rate limiting is handled earlier in the code for both singles and duplicates
            # Just add a small pause if search was attempted
            if search_attempted:
                human_pause()
    try:
        driver.quit()
    except Exception:
        pass

# ====== MAIN ======

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--threads", type=int, default=DEFAULT_THREADS)
    ap.add_argument("--min-threads", type=int, default=MIN_THREADS)
    ap.add_argument("--max-threads", type=int, default=MAX_THREADS)
    ap.add_argument("--max-rows", type=int, default=0, help="Maximum number of rows to process (0 = unlimited)")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--headless", dest="headless", action="store_true", default=False)
    g.add_argument("--no-headless", dest="headless", action="store_false")
    args = ap.parse_args()
    args.max_threads = max(1, int(args.max_threads))

    ensure_headers()
    skip_set = load_progress_set()
    ignore_set = load_ignore_list()

    # load targets - check if input file exists
    input_file_path = INPUT_FILE
    if not input_file_path.exists():
        # Try case-insensitive search
        input_dir = input_file_path.parent
        found_file = None
        if input_dir.exists():
            for file in input_dir.iterdir():
                if file.is_file() and file.name.lower() == input_file_path.name.lower():
                    found_file = file
                    break
        
        if found_file:
            log.warning(f"Input file found with different casing: {found_file}")
            log.warning(f"Using: {found_file}")
            input_file_path = found_file
        else:
            # Provide helpful error message
            error_msg = f"Input file not found: {INPUT_FILE}\n"
            error_msg += f"Please run script 01 (getProdList.py) first to generate Productlist.csv\n"
            if input_dir.exists():
                files = list(input_dir.glob("*.csv"))
                if files:
                    error_msg += f"\nFound CSV files in {input_dir}:\n"
                    for f in files[:5]:  # Show first 5
                        error_msg += f"  - {f.name}\n"
                    if len(files) > 5:
                        error_msg += f"  ... and {len(files) - 5} more\n"
                else:
                    error_msg += f"\nNo CSV files found in {input_dir}\n"
            else:
                error_msg += f"\nInput directory does not exist: {input_dir}\n"
                error_msg += f"Please create the directory and add Productlist.csv\n"
            raise FileNotFoundError(error_msg)
    
    targets: List[Tuple[str, str, bool, str]] = []  # (product, company, is_duplicate, url)
    all_products: List[Tuple[str, str]] = []
    
    # Check if we're using prepared URLs file
    using_prepared = PREPARED_URLS_FILE_PATH.exists() and input_file_path == PREPARED_URLS_FILE_PATH
    
    # First pass: load all products
    with open(input_file_path, encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        headers = {nk(h): h for h in (r.fieldnames or [])}
        pcol = headers.get(nk("Product")) or headers.get("product") or "Product"
        ccol = headers.get(nk("Company")) or headers.get("company") or "Company"
        
        if using_prepared:
            # Use prepared URLs file with Source, URL, IsDuplicate columns
            source_col = headers.get(nk("Source")) or headers.get("source") or "Source"
            url_col = headers.get(nk("URL")) or headers.get("url") or "URL"
            dup_col = headers.get(nk("IsDuplicate")) or headers.get("isduplicate") or "IsDuplicate"
            
            log.info("[INPUT] Using prepared URLs file with source and URL information")
            
            for row in r:
                prod = (row.get(pcol) or "").strip()
                comp = (row.get(ccol) or "").strip()
                source = (row.get(source_col) or "").strip().lower()
                url = (row.get(url_col) or "").strip()
                is_dup_str = (row.get(dup_col) or "").strip().lower()
                is_duplicate = is_dup_str == "true"
                
                # If source contains "selenium", use Selenium instead of API
                if "selenium" in source:
                    is_duplicate = True
                
                if prod and comp:
                    all_products.append((prod, comp))
                    # Use the prepared URL, or construct if missing
                    if not url:
                        url = construct_product_url(prod)
                    targets.append((prod, comp, is_duplicate, url))
            
            # Count by source
            api_count = sum(1 for t in targets if not t[2])  # not is_duplicate
            selenium_count = sum(1 for t in targets if t[2])  # is_duplicate
            
            log.info(f"[FILTER] Using prepared URLs: {len(targets)} products")
            log.info(f"[FILTER] API source (singles): {api_count}")
            log.info(f"[FILTER] Selenium source (duplicates + selenium-marked): {selenium_count}")
        else:
            # Fallback: use original Productlist.csv and determine duplicates
            log.info("[INPUT] Using original Productlist.csv, determining duplicates and constructing URLs")
            
            for row in r:
                prod = (row.get(pcol) or "").strip()
                comp = (row.get(ccol) or "").strip()
                if prod and comp:
                    all_products.append((prod, comp))
            
            # Count product occurrences (by product name only, case-insensitive)
            product_counts = Counter(nk(prod) for prod, _ in all_products)
            
            # Separate into singles and duplicates
            duplicate_products = {prod for prod, count in product_counts.items() if count > 1}
            single_products = {prod for prod, count in product_counts.items() if count == 1}
            
            # Separate targets: (product, company, is_duplicate, url)
            for prod, comp in all_products:
                prod_norm = nk(prod)
                is_duplicate = prod_norm in duplicate_products
                url = construct_product_url(prod)
                targets.append((prod, comp, is_duplicate, url))
            
            log.info(f"[FILTER] Single products: {len(single_products)} ({sum(1 for t in targets if not t[2])} occurrences) - will use API")
            log.info(f"[FILTER] Duplicate products: {len(duplicate_products)} ({sum(1 for t in targets if t[2])} occurrences) - will use Selenium (1 per minute per thread)")
    
    # Apply ignore list filter
    if ignore_set:
        original_count = len(targets)
        targets = [t for t in targets if (nk(t[1]), nk(t[0])) not in ignore_set]  # (company, product)
        ignored_count = original_count - len(targets)
        if ignored_count > 0:
            log.info(f"[FILTER] Ignored {ignored_count} products from {IGNORE_LIST_FILE}")
    
    # Apply max-rows limit if specified
    if args.max_rows > 0 and len(targets) > args.max_rows:
        targets = targets[:args.max_rows]
        log.info(f"Max rows limit applied: {args.max_rows} targets")
    
    log.info(f"[FILTER] Total targets: {len(targets)} (from {len(all_products)} total products)")

    # Separate targets into API queue (singles) and Selenium queue (duplicates)
    api_queue = Queue()
    selenium_queue = Queue()
    
    api_targets = []
    selenium_targets = []
    
    for t in targets:
        product, company, is_duplicate, url = t
        if is_duplicate:
            selenium_targets.append(t)
            selenium_queue.put(t)
        else:
            api_targets.append(t)
            api_queue.put(t)
    
    log.info(f"[QUEUE] API queue: {len(api_targets)} products (singles)")
    log.info(f"[QUEUE] Selenium queue: {len(selenium_targets)} products (duplicates)")
    
    # Use configured thread counts
    api_thread_count = API_THREADS
    selenium_thread_count = SELENIUM_THREADS
    
    log.info(f"[PARALLEL] Starting API workers: {api_thread_count} threads")
    log.info(f"[PARALLEL] Starting Selenium workers: {selenium_thread_count} threads (1 minute per product)")
    
    # Start API workers
    api_threads = [threading.Thread(target=api_worker, args=(api_queue, selenium_queue, args, skip_set), daemon=True) 
                   for _ in range(api_thread_count)]
    
    # Start Selenium workers
    selenium_threads = [threading.Thread(target=selenium_worker, args=(selenium_queue, args, skip_set), daemon=True) 
                        for _ in range(selenium_thread_count)]
    
    # Start all threads
    for t in api_threads:
        t.start()
    for t in selenium_threads:
        t.start()
    
    # Wait for all threads to complete
    for t in api_threads:
        t.join()
    for t in selenium_threads:
        t.join()
    
    log.info("All done.")

if __name__ == "__main__":
    main()
