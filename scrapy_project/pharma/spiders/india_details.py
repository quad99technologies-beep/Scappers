#!/usr/bin/env python3
"""
India NPPA Scrapy Spider -- parallel work-queue variant.

Each spider instance (worker) does:
1. Warm-up GET to searchMedicine (establish cookies)
2. GET formulationListNew -- build formulation ID map
3. Claim a batch of pending formulations from formulation_status table
4. For each claimed formulation:
   a. GET formulationDataTableNew -- list of SKUs
   b. For each SKU: GET skuMrpNew, otherBrandPriceNew, medDtlsNew
   c. Write all data to PostgreSQL
5. Repeat step 3 until no pending formulations remain

Business logic is IDENTICAL to the original 02_get_details.py.
Coordination: formulation_status table acts as a work queue.
Workers claim batches atomically so no formulation is scraped twice.
"""

import hashlib
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import scrapy
from scrapy import Request, Spider
from scrapy.http import TextResponse

# Ensure repo root on path for core imports
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.postgres_connection import PostgresDB
from core.db.models import apply_common_schema, run_ledger_finish
from core.db.schema_registry import SchemaRegistry
from core.db.upsert import upsert_items

logger = logging.getLogger(__name__)

# Selenium imports (optional - only used if USE_SELENIUM_DROPDOWN is enabled)
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

# --- Constants (same as original 02_get_details.py) ---
SEARCH_URL = "https://nppaipdms.gov.in/NPPA/PharmaSahiDaam/searchMedicine"
REST_BASE = "https://nppaipdms.gov.in/NPPA/rest"
API_FORMULATION_LIST = f"{REST_BASE}/formulationListNew"
API_FORMULATION_TABLE = f"{REST_BASE}/formulationDataTableNew"
API_SKU_MRP = f"{REST_BASE}/skuMrpNew"
API_OTHER_BRANDS = f"{REST_BASE}/otherBrandPriceNew"
API_MED_DTLS = f"{REST_BASE}/medDtlsNew"

# India: claim batch size must be India-specific (avoid global CLAIM_BATCH_SIZE causing massive pre-claiming).
# Default is 1 to prevent "claiming like crazy" before finishing the first batch.
CLAIM_BATCH_SIZE = int(os.getenv("INDIA_CLAIM_BATCH", "1"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
LOOKUP_RETRIES = int(os.getenv("INDIA_LOOKUP_RETRIES", "3"))
MIN_API_MAP_SIZE = int(os.getenv("INDIA_MIN_API_MAP_SIZE", "1000"))
# Disabled by default; enable explicitly with INDIA_PREFILTER_QUEUE=true
PREFILTER_QUEUE = os.getenv("INDIA_PREFILTER_QUEUE", "false").lower() in ("true", "1", "yes")

# Requeue backoff: prevents immediate re-claim thrash on transient mismatches/errors.
# Uses `claimed_at` as "available_at" when status='pending'.
REQUEUE_BACKOFF_BASE_MINUTES = int(os.getenv("INDIA_REQUEUE_BACKOFF_BASE_MINUTES", "2"))
REQUEUE_BACKOFF_MAX_MINUTES = int(os.getenv("INDIA_REQUEUE_BACKOFF_MAX_MINUTES", "60"))

# Claim heartbeat: keep claimed_at fresh during long-running formulations so stale recovery
# doesn't steal active work. Throttled per formulation.
CLAIM_TOUCH_INTERVAL_SECONDS = int(os.getenv("INDIA_CLAIM_TOUCH_INTERVAL_SECONDS", "60"))

# Completion timeout: if items are stuck pending/in_progress for too long near completion,
# mark them as failed to allow run to complete (prevents infinite waiting on stuck items)
COMPLETION_TIMEOUT_MINUTES = int(os.getenv("INDIA_COMPLETION_TIMEOUT_MINUTES", "30"))
COMPLETION_THRESHOLD_PCT = float(os.getenv("INDIA_COMPLETION_THRESHOLD_PCT", "99.5"))

# Circuit breaker: pause scraping when NPPA API is consistently failing
# (prevents wasting requests and getting rate-limited on a down server)
CIRCUIT_BREAKER_THRESHOLD = int(os.getenv("INDIA_CIRCUIT_BREAKER_THRESHOLD", "10"))  # consecutive failures
CIRCUIT_BREAKER_COOLDOWN_SECONDS = int(os.getenv("INDIA_CIRCUIT_BREAKER_COOLDOWN", "120"))  # wait before retry

# Throttling: avoid hammering NPPA and crashing the platform (tune via env if needed)
INDIA_DOWNLOAD_DELAY = float(os.getenv("INDIA_DOWNLOAD_DELAY", "1.0"))
INDIA_CONCURRENT_REQUESTS = int(os.getenv("INDIA_CONCURRENT_REQUESTS", "2"))
AUTOTHROTTLE_ENABLED = os.getenv("INDIA_AUTOTHROTTLE", "true").lower() in ("true", "1", "yes")

# Selenium dropdown interaction (Step 2): use browser automation to interact with dropdown
USE_SELENIUM_DROPDOWN = os.getenv("INDIA_USE_SELENIUM_DROPDOWN", "false").lower() in ("true", "1", "yes")
SELENIUM_HEADLESS = os.getenv("INDIA_SELENIUM_HEADLESS", "true").lower() in ("true", "1", "yes")
SELENIUM_DROPDOWN_TIMEOUT = int(os.getenv("INDIA_SELENIUM_DROPDOWN_TIMEOUT", "30"))


# --- Utility functions (identical to original) ---

def _sanitize_api_value(value: Any) -> str:
    if value is None:
        return ""
    s = str(value)
    s = s.replace(" ", "_").replace("\r\n", "_").replace("\n", "_").replace("\r", "_")
    if s == "undefined":
        return ""
    return s


def compute_fhttf(params: Dict[str, Any]) -> str:
    """Compute the MD5 auth token required by NPPA REST endpoints."""
    entries = sorted(
        ((k.lower(), k, _sanitize_api_value(params.get(k))) for k in params if k != "fhttf"),
        key=lambda x: x[0],
    )
    acc = "".join(v for _, __, v in entries)
    return hashlib.md5(acc.encode("utf-8")).hexdigest()


def normalize_name(value: str) -> str:
    return " ".join((value or "").strip().upper().split())


def _dedupe_words(s: str) -> str:
    """Remove duplicate consecutive words (e.g. 'GEL GEL' -> 'GEL')."""
    words = s.split()
    out = []
    for w in words:
        if not out or out[-1] != w:
            out.append(w)
    return " ".join(out)


_STRENGTH_TOKENS = {
    "MG", "MCG", "UG", "G", "GM", "KG",
    "ML", "L",
    "IU", "MIU",
    "%", "%W/W", "%W/V", "%V/V",
    "MG/ML", "MCG/ML", "UG/ML", "G/ML",
    "MG/G", "MCG/G", "UG/G", "G/G",
}

_DOSAGE_SUFFIX_PHRASES = [
    ("EYE", "DROPS"),
    ("EAR", "DROPS"),
    ("NASAL", "SPRAY"),
    ("ORAL", "SOLUTION"),
    ("ORAL", "SUSPENSION"),
    ("ORAL", "DROPS"),
    ("INTRAVENOUS", "INJECTION"),
    ("IV", "INJECTION"),
    ("IM", "INJECTION"),
    ("INJECTION",),
    ("TABLET", "SR"),
    ("TABLET", "ER"),
    ("TABLET", "XR"),
    ("TABLET", "CR"),
    ("TABLET", "DR"),
    ("TABLET",),
    ("CAPSULE", "SR"),
    ("CAPSULE", "ER"),
    ("CAPSULE", "XR"),
    ("CAPSULE", "CR"),
    ("CAPSULE", "DR"),
    ("CAPSULE",),
    ("CREAM",),
    ("GEL",),
    ("OINTMENT",),
    ("LOTION",),
    ("SYRUP",),
    ("SUSPENSION",),
    ("SOLUTION",),
    ("DROPS",),
    ("SPRAY",),
    ("SHAMPOO",),
    ("SOAP",),
    ("POWDER",),
    ("GRANULES",),
    ("SACHET",),
    ("TUBE",),
    ("PATCH",),
    ("AEROSOL",),
    ("INHALER",),
    ("INHALATION",),
    ("NEBULISER",),
    ("NEBULIZER",),
    ("MOUTHWASH",),
    ("RINSE",),
]


def _strip_dosage_suffix(s: str) -> Optional[str]:
    """
    Heuristic: strip trailing dosage-form phrases like 'TABLET', 'INJECTION', 'EYE DROPS'.
    Keeps ingredient names while removing form words that NPPA formulation list often omits.
    """
    toks = (s or "").split()
    if len(toks) < 2:
        return None
    for phrase in sorted(_DOSAGE_SUFFIX_PHRASES, key=len, reverse=True):
        n = len(phrase)
        if len(toks) >= n and tuple(toks[-n:]) == phrase:
            out = " ".join(toks[:-n]).strip()
            if out and out != s and len(out.split()) >= 2:
                return out
    return None


def _looks_strengthy(token: str) -> bool:
    t = (token or "").strip().upper().strip(",;:()[]{}")
    if not t:
        return False
    if t in _STRENGTH_TOKENS:
        return True
    if any(ch.isdigit() for ch in t):
        return True
    if "/" in t or "%" in t:
        return True
    return False


def _strip_strength_suffix(s: str) -> Optional[str]:
    """
    Heuristic: strip trailing strength/pack tokens like '10 MG', '0.5/8 MG', '1 %', '90 MIU'.
    This helps map input strings that include strength to NPPA formulation names that often don't.
    """
    toks = (s or "").split()
    if len(toks) < 2:
        return None
    i = len(toks)
    removed = 0
    while i > 1 and _looks_strengthy(toks[i - 1]):
        i -= 1
        removed += 1
        if removed >= 8:
            break
    out = " ".join(toks[:i]).strip()
    if out and out != s and len(out.split()) >= 2:
        return out
    return None


def _exact_match_variants(formulation: str) -> List[str]:
    """Return normalized key and display variants for exact-match (search box) lookup."""
    key = normalize_name(formulation)
    # Normalize " - " and " – " (dash) to space so "AMPHOTERICIN B - LIPID" can match API "AMPHOTERICIN B LIPID"
    key = key.replace(" - ", " ").replace(" – ", " ").replace("  ", " ").strip()
    key_deduped = _dedupe_words(key)
    variants = [
        key,
        key_deduped,
        key.replace(" + ", " ").replace("  ", " ").strip(),
        key.replace(" + ", " AND "),
        key.replace(" AND ", " + "),
        _dedupe_words(key.replace(" + ", " ")),
    ]
    # Also try stripping strength suffixes (e.g. "... TABLET 10 MG" -> "... TABLET")
    stripped = []
    for v in variants:
        sv = _strip_strength_suffix(v)
        if sv:
            stripped.append(sv)
            sd = _dedupe_words(sv)
            if sd and sd != sv:
                stripped.append(sd)
            dv = _strip_dosage_suffix(sv)
            if dv:
                stripped.append(dv)
                dd = _dedupe_words(dv)
                if dd and dd != dv:
                    stripped.append(dd)
        dv0 = _strip_dosage_suffix(v)
        if dv0:
            stripped.append(dv0)
            dd0 = _dedupe_words(dv0)
            if dd0 and dd0 != dv0:
                stripped.append(dd0)
    variants.extend(stripped)
    # Dedupe so we don't repeat same string
    seen = set()
    out = []
    for v in variants:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _lookup_formulation_id(api_map: Dict[str, str], formulation: str) -> Optional[str]:
    """Look up formulation ID by exact match only (dropdown / search box semantics).

    Uses generic name → normalized key and a small set of display variants
    (+/AND, duplicate words). No substring or fuzzy match: we only resolve
    when the API list contains an exact match (as if user selected from dropdown).
    """
    for v in _exact_match_variants(formulation):
        if v and api_map.get(v):
            return api_map[v]
    return None


def _search_box_exact_match(
    api_map: Dict[str, str], formulation: str
) -> Optional[tuple]:
    """
    Check formulation in search box (formulation list): exact match only.
    Returns (formulation_id, matched_api_name) if found, else None.
    Call this after claim; only then call detail APIs.
    """
    for v in _exact_match_variants(formulation):
        if v and v in api_map:
            return (api_map[v], v)
    return None


def _selenium_dropdown_search(
    formulation: str, headless: bool = True, timeout: int = 30
) -> Optional[Tuple[str, str]]:
    """
    Use Selenium to interact with NPPA search dropdown:
    1. Go to website
    2. Enter formulation name
    3. Wait for dropdown
    4. Scroll down in dropdown
    5. Click on exact match
    6. Extract formulation ID from API link
    
    Returns (formulation_id, matched_name) if found, else None.
    """
    if not SELENIUM_AVAILABLE:
        logger.warning("Selenium not available - install selenium package to use dropdown search")
        return None
    
    driver = None
    try:
        # Create Chrome driver
        options = Options()
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(timeout)
        wait = WebDriverWait(driver, timeout)
        
        logger.info(f"[SELENIUM] Navigating to {SEARCH_URL}")
        driver.get(SEARCH_URL)
        
        # Wait for page to load
        time.sleep(2)
        
        # Find search input box (typically has id or name related to formulation/search)
        # Common selectors: #formulationName, #searchFormulation, input[name*="formulation"], etc.
        search_selectors = [
            "#formulationName",
            "#searchFormulation", 
            "input[name*='formulation']",
            "input[name*='Formulation']",
            "input[type='text'][placeholder*='formulation']",
            "input[type='text'][placeholder*='Formulation']",
            "#formulation",
            ".formulation-input",
        ]
        
        search_input = None
        for selector in search_selectors:
            try:
                search_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                logger.info(f"[SELENIUM] Found search input with selector: {selector}")
                break
            except TimeoutException:
                continue
        
        if not search_input:
            logger.error("[SELENIUM] Could not find search input box")
            return None
        
        # Clear and enter formulation name
        logger.info(f"[SELENIUM] Entering formulation: {formulation}")
        search_input.clear()
        search_input.send_keys(formulation)
        time.sleep(1)  # Wait for dropdown to appear
        
        # Wait for dropdown to appear
        dropdown_selectors = [
            ".ui-autocomplete",
            ".dropdown-menu",
            ".autocomplete",
            "#ui-id-1",  # jQuery UI autocomplete
            "ul[role='listbox']",
            "ul.ui-autocomplete",
            ".formulation-dropdown",
        ]
        
        dropdown = None
        for selector in dropdown_selectors:
            try:
                dropdown = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                logger.info(f"[SELENIUM] Found dropdown with selector: {selector}")
                break
            except TimeoutException:
                continue
        
        if not dropdown:
            logger.warning("[SELENIUM] Dropdown did not appear, trying to find options directly")
            # Try finding dropdown items directly
            option_selectors = [
                "li.ui-menu-item",
                ".ui-menu-item",
                "li[role='option']",
                ".dropdown-item",
                "li a",
            ]
            for opt_selector in option_selectors:
                try:
                    options = driver.find_elements(By.CSS_SELECTOR, opt_selector)
                    if options:
                        dropdown = options[0].find_element(By.XPATH, "./..")  # Get parent
                        logger.info(f"[SELENIUM] Found dropdown options with selector: {opt_selector}")
                        break
                except:
                    continue
        
        if not dropdown:
            logger.error("[SELENIUM] Could not find dropdown")
            return None
        
        # Scroll down in dropdown to see all options
        logger.info("[SELENIUM] Scrolling dropdown to see all options")
        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", dropdown)
        time.sleep(0.5)
        
        # Find exact match in dropdown options
        option_elements = dropdown.find_elements(By.CSS_SELECTOR, "li, .dropdown-item, [role='option']")
        if not option_elements:
            # Try alternative selectors
            option_elements = driver.find_elements(By.CSS_SELECTOR, "li.ui-menu-item, .ui-menu-item, li[role='option']")
        
        logger.info(f"[SELENIUM] Found {len(option_elements)} dropdown options")
        
        matched_option = None
        matched_text = None
        
        # Normalize formulation for comparison
        formulation_normalized = normalize_name(formulation)
        formulation_variants = _exact_match_variants(formulation)
        
        for option in option_elements:
            try:
                option_text = option.text.strip()
                if not option_text:
                    # Try getting text from child elements
                    try:
                        option_text = option.find_element(By.TAG_NAME, "a").text.strip()
                    except:
                        continue
                
                option_normalized = normalize_name(option_text)
                
                # Check for exact match
                if option_normalized in formulation_variants or option_normalized == formulation_normalized:
                    matched_option = option
                    matched_text = option_text
                    logger.info(f"[SELENIUM] Found exact match: '{matched_text}'")
                    break
            except Exception as e:
                logger.debug(f"[SELENIUM] Error checking option: {e}")
                continue
        
        if not matched_option:
            logger.warning(f"[SELENIUM] No exact match found for '{formulation}' in dropdown")
            return None
        
        # Scroll to make option visible
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", matched_option)
        time.sleep(0.3)
        
        # Click on exact match
        logger.info(f"[SELENIUM] Clicking on exact match: '{matched_text}'")
        try:
            matched_option.click()
        except:
            # Try JavaScript click if regular click fails
            driver.execute_script("arguments[0].click();", matched_option)
        
        time.sleep(1)  # Wait for page to update
        
        # Extract formulation ID from the page
        # The ID might be in:
        # - A hidden input field
        # - Data attribute on an element
        # - URL parameter
        # - JavaScript variable
        
        fid = None
        
        # Try to get from hidden input or data attribute
        fid_selectors = [
            "#formulationId",
            "input[name='formulationId']",
            "[data-formulation-id]",
            "[data-id]",
        ]
        
        for selector in fid_selectors:
            try:
                element = driver.find_element(By.CSS_SELECTOR, selector)
                fid = element.get_attribute("value") or element.get_attribute("data-formulation-id") or element.get_attribute("data-id")
                if fid:
                    logger.info(f"[SELENIUM] Found formulation ID from selector {selector}: {fid}")
                    break
            except:
                continue
        
        # Try to extract from URL if page navigated
        if not fid:
            current_url = driver.current_url
            import re
            match = re.search(r'formulationId[=:](\d+)', current_url)
            if match:
                fid = match.group(1)
                logger.info(f"[SELENIUM] Found formulation ID from URL: {fid}")
        
        # Try to extract from JavaScript variable
        if not fid:
            try:
                fid = driver.execute_script("return window.formulationId || document.formulationId || arguments[0].formulationId;", driver)
                if fid:
                    logger.info(f"[SELENIUM] Found formulation ID from JS variable: {fid}")
            except:
                pass
        
        if not fid:
            logger.warning("[SELENIUM] Could not extract formulation ID after clicking dropdown option")
            return None
        
        logger.info(f"[SELENIUM] Successfully found formulation ID: {fid} for '{matched_text}'")
        return (str(fid), matched_text)
        
    except Exception as e:
        logger.error(f"[SELENIUM] Error in dropdown search: {e}", exc_info=True)
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass


def sget(record: Dict[str, Any], key: str) -> str:
    value = record.get(key, "")
    return "" if value is None else str(value)


def safe_json(response) -> Any:
    """Parse JSON from response, handling non-UTF-8 bytes (e.g. 0xa0 from NPPA)."""
    try:
        return response.json()
    except (UnicodeDecodeError, ValueError):
        pass

    try:
        text = response.body.decode("latin-1", errors="replace")
        text = text.strip()
        if not text:
            return None
        return json.loads(text)
    except Exception:
        return None


def build_api_url(endpoint: str, params: Dict[str, Any]) -> str:
    """Build URL with fhttf token appended to query params."""
    params = dict(params)
    params["fhttf"] = compute_fhttf(params)
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{endpoint}?{qs}"


class IndiaNPPASpider(Spider):
    """
    Scrapy spider for India NPPA medicine details.

    Custom settings override global to match NPPA's rate limits.
    Scrapy handles retry (429, 500-504) and autothrottle automatically.
    """

    name = "india_details"
    country_name = "India"
    allowed_domains = ["nppaipdms.gov.in"]

    custom_settings = {
        # Throttling: avoid hammering NPPA (reduces platform crash / 429 risk)
        "DOWNLOAD_DELAY": INDIA_DOWNLOAD_DELAY,
        "CONCURRENT_REQUESTS": INDIA_CONCURRENT_REQUESTS,
        "CONCURRENT_REQUESTS_PER_DOMAIN": INDIA_CONCURRENT_REQUESTS,
        "AUTOTHROTTLE_ENABLED": AUTOTHROTTLE_ENABLED,
        "AUTOTHROTTLE_START_DELAY": 1.0,
        "AUTOTHROTTLE_MAX_DELAY": 10.0,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 1.5,
        "AUTOTHROTTLE_DEBUG": False,
        "RETRY_TIMES": MAX_RETRIES,
        "RETRY_HTTP_CODES": [429, 500, 502, 503, 504, 599],  # 599 = connection timeout
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        },
        # Disable the generic pipeline -- this spider writes directly to DB
        "ITEM_PIPELINES": {},
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
            "pharma.middlewares.OTelDownloaderMiddleware": 50,
            "pharma.middlewares.RandomUserAgentMiddleware": 90,
            "pharma.middlewares.BrowserHeadersMiddleware": 95,
            "pharma.middlewares.HumanizeDownloaderMiddleware": None,  # DISABLED for API
            "pharma.middlewares.AntiBotDownloaderMiddleware": 110,
            "pharma.middlewares.ProxyRotationMiddleware": 120,
            "pharma.middlewares.PlatformFetcherMiddleware": 130,
        },
    }

    def __init__(self, run_id=None, worker_id=1, limit=None, platform_run_id=None, **kwargs):
        super().__init__(**kwargs)

        self.run_id = run_id
        self.worker_id = int(worker_id)
        self.limit = int(limit) if limit else None
        self.formulation_map: Dict[str, str] = {}  # search box: normalized name -> formulationId
        self.formulation_id_to_name: Dict[str, str] = {}  # fid -> API name (for verify before get API)
        self.platform_run_id = platform_run_id or os.getenv("PLATFORM_RUN_ID") or None

        # DB handle
        self.db: Optional[PostgresDB] = None

        # Stats
        self.stats_medicines = 0
        self.stats_substitutes = 0
        self.stats_errors = 0
        self.stats_completed = 0
        self.stats_zero = 0
        
        # PERFORMANCE FIX: Track last performance log time
        self._last_perf_log = 0
        self._perf_log_interval = 50  # Log every 50 formulations

        # Track pending detail requests per formulation for deferred completion
        # key=formulation, value={"pending": int, "sku_count": int}
        self._pending_details: Dict[str, Dict] = {}
        # Number of formulations in current batch still awaiting detail completion
        self._batch_pending_formulations = 0
        # Platform entity mapping: hidden_id -> entity_id
        self._entity_ids: Dict[str, int] = {}
        self._platform_ready = False

        # PERFORMANCE: DB write buffer — accumulate detail writes per formulation,
        # flush once when formulation completes (1 commit vs N*3 commits).
        self._write_buffer: Dict[str, Dict] = {}
        self._last_claim_touch: Dict[str, float] = {}
        # formulation -> {"stmts": [(sql, params)], "brand_rows": [row_dict]}
        
        # Completion timeout tracking: track when we first detected stuck items
        self._stuck_items_detected_at: Optional[float] = None

        # Circuit breaker: track consecutive API failures to detect server outages
        self._consecutive_failures = 0
        self._circuit_breaker_open = False
        self._circuit_breaker_until: float = 0.0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start_requests(self):
        """Step 1: Warm-up GET to establish cookies, then fetch formulation list."""
        yield Request(
            url=SEARCH_URL,
            callback=self._after_warmup,
            dont_filter=True,
            meta={"handle_httpstatus_list": [200, 302, 599]},  # Handle 599 so errback is called
            errback=self._warmup_error,
        )

    def _after_warmup(self, response: TextResponse):
        """Step 2: Fetch formulation list to build ID map."""
        self.logger.info("[W%d] Session established. Fetching formulation list...", self.worker_id)
        yield Request(
            url=API_FORMULATION_LIST,
            callback=self._parse_formulation_list,
            headers={"Referer": SEARCH_URL},
            dont_filter=True,
            meta={"source": "api", "entity_type": "formulation_list", "handle_httpstatus_list": [200, 599]},
            errback=self._formulation_list_error,
        )

    def _parse_formulation_list(self, response: TextResponse):
        """Step 3: Parse formulation map, then start claiming work."""
        # Handle HTTP 599 (connection timeout) - should have been retried, but if it reaches here, close spider
        if response.status == 599:
            self.logger.error("[W%d] HTTP 599 (connection timeout) for formulation list after retries", self.worker_id)
            raise scrapy.exceptions.CloseSpider("formulation_list_timeout_599")
        
        items = safe_json(response)
        if not isinstance(items, list) or not items:
            self.logger.error("[W%d] Formulation list response was not JSON list (status=%s)", self.worker_id, response.status)
            raise scrapy.exceptions.CloseSpider("formulation_list_invalid")
        if isinstance(items, list):
            for row in items:
                if not isinstance(row, dict):
                    continue
                name = normalize_name(row.get("formulationName", ""))
                name = name.replace(" - ", " ").replace(" – ", " ").replace("  ", " ").strip()
                fid = str(row.get("formulationId", "")).strip()
                if name and fid:
                    self.formulation_map[name] = fid
                    self.formulation_id_to_name[fid] = name

        self.logger.info("[W%d] Loaded %d formulations from API map (search box)", self.worker_id, len(self.formulation_map))

        try:
            # Initialize DB connection
            self._init_db()
            self._ensure_platform()

            # Get total formulations count for progress reporting
            cur = self.db.execute("SELECT COUNT(*) FROM in_formulation_status WHERE run_id = %s", (self.run_id,))
            self.total_formulations = cur.fetchone()[0] or 0

            # Prefilter pending queue once (reduce claim-thrash when inputs don't match NPPA list)
            if PREFILTER_QUEUE and self.worker_id == 1 and len(self.formulation_map) >= MIN_API_MAP_SIZE:
                try:
                    self._prefilter_pending_queue()
                except Exception as exc:
                    self.logger.warning("[W%d] Prefilter queue failed: %s", self.worker_id, exc)

            # Claim first batch and start processing
            yield from self._claim_and_process()
        except Exception as exc:
            self.logger.exception("[W%d] Fatal error in formulation list processing: %s", self.worker_id, exc)
            raise

    def _claim_and_process(self):
        """Claim a batch, then for each: check in search box (exact match), only then get API details."""
        while True:
            # Circuit breaker: wait if NPPA API is consistently failing
            if not self._check_circuit_breaker():
                self.logger.info(
                    "[W%d] Circuit breaker open — waiting %d seconds before next claim",
                    self.worker_id, CIRCUIT_BREAKER_COOLDOWN_SECONDS,
                )
                time.sleep(CIRCUIT_BREAKER_COOLDOWN_SECONDS)
                continue

            try:
                batch = self._claim_batch()
            except Exception as exc:
                self.logger.exception("[W%d] Failed to claim batch: %s", self.worker_id, exc)
                return

            if not batch:
                # With backoff requeues, there may be pending rows that are not yet claimable.
                # Keep the spider alive and poll until the queue is truly empty.
                try:
                    # Check for pending/in_progress formulations
                    cur = self.db.execute(
                        "SELECT COUNT(*) FROM in_formulation_status "
                        "WHERE run_id = %s AND status IN ('pending', 'in_progress')",
                        (self.run_id,),
                    )
                    remaining = int(cur.fetchone()[0] or 0)
                    
                    # Also check if all formulations are in terminal states (true completion)
                    cur = self.db.execute(
                        "SELECT COUNT(*) FROM in_formulation_status "
                        "WHERE run_id = %s AND status NOT IN ('completed', 'zero_records', 'failed', 'blocked', 'blocked_captcha')",
                        (self.run_id,),
                    )
                    non_terminal = int(cur.fetchone()[0] or 0)
                    
                    # Get total count for logging
                    cur = self.db.execute(
                        "SELECT status, COUNT(*) FROM in_formulation_status "
                        "WHERE run_id = %s GROUP BY status",
                        (self.run_id,),
                    )
                    status_counts = dict(cur.fetchall())
                    total = sum(status_counts.values())
                    completed = status_counts.get('completed', 0)
                    zero_rec = status_counts.get('zero_records', 0)
                    failed = status_counts.get('failed', 0)
                    done = completed + zero_rec + failed
                    
                except Exception:
                    self.logger.info("[W%d] No more claimable formulations. Done.", self.worker_id)
                    return

                # True completion: all formulations are in terminal states
                if non_terminal <= 0:
                    self.logger.info(
                        "[W%d] ✅ List completed! All formulations processed: %d/%d done "
                        "(completed=%d, zero_records=%d, failed=%d). Exiting.",
                        self.worker_id, done, total, completed, zero_rec, failed
                    )
                    print(f"[DB] W{self.worker_id} | COMPLETE | All {total} formulations processed | completed={completed} zero={zero_rec} failed={failed}", flush=True)
                    return

                # No pending/in_progress left (but may have non-terminal states from other workers)
                if remaining <= 0:
                    self.logger.info(
                        "[W%d] No more pending/in_progress formulations (remaining=%d, non-terminal=%d). "
                        "Waiting for other workers to finish...",
                        self.worker_id, remaining, non_terminal
                    )
                    # Still wait a bit in case other workers are finishing
                    poll_s = float(os.getenv("INDIA_QUEUE_POLL_SECONDS", "5"))
                    try:
                        from twisted.internet import reactor
                        from twisted.internet.task import deferLater
                        yield deferLater(reactor, poll_s, lambda: None)
                    except Exception:
                        return
                    continue

                # Has remaining but not claimable (backoff/requeue)
                # Check if we're near completion and items are stuck
                completion_pct = (done / total * 100) if total > 0 else 0
                stuck_count = self._check_and_recover_stuck_items(completion_pct, total, done)
                
                poll_s = float(os.getenv("INDIA_QUEUE_POLL_SECONDS", "5"))
                if stuck_count > 0:
                    self.logger.info(
                        "[W%d] Recovered %d stuck item(s) near completion. Retrying claim...",
                        self.worker_id, stuck_count
                    )
                    # Immediately retry claim after recovering stuck items
                    continue
                
                self.logger.info(
                    "[W%d] No claimable formulations right now (remaining=%d, non-terminal=%d, done=%d/%d, %.1f%%). Waiting %.1fs",
                    self.worker_id, remaining, non_terminal, done, total, completion_pct, poll_s,
                )
                try:
                    from twisted.internet import reactor
                    from twisted.internet.task import deferLater
                    yield deferLater(reactor, poll_s, lambda: None)
                except Exception:
                    return
                continue

            self.logger.info("[W%d] Claimed %d formulations", self.worker_id, len(batch))
            print(f"[DB] W{self.worker_id} | CLAIM | {len(batch)} formulations from queue", flush=True)

            # Step 2: for each claimed formulation, check in search box (exact match in list) before get API
            # Use Selenium dropdown interaction if enabled, otherwise use API map matching
            actionable: List[tuple[str, str]] = []
            for formulation in batch:
                fid = None
                matched_api_name = None
                
                if USE_SELENIUM_DROPDOWN:
                    # Use Selenium to interact with dropdown: go to website, enter, wait for dropdown,
                    # scroll down, click exact match, get API link
                    try:
                        self.logger.info("[W%d] Using Selenium dropdown search for '%s'", self.worker_id, formulation)
                        match = _selenium_dropdown_search(
                            formulation,
                            headless=SELENIUM_HEADLESS,
                            timeout=SELENIUM_DROPDOWN_TIMEOUT
                        )
                        if match:
                            fid, matched_api_name = match
                            self.logger.info(
                                "[W%d] Selenium found match: '%s' -> fid %s (matched: '%s')",
                                self.worker_id, formulation, fid, matched_api_name
                            )
                        else:
                            self.logger.warning("[W%d] Selenium dropdown search found no match for '%s'", self.worker_id, formulation)
                    except Exception as exc:
                        self.logger.warning("[W%d] Selenium dropdown search failed for '%s': %s", self.worker_id, formulation, exc)
                        # Fall back to API map matching if Selenium fails
                        try:
                            match = _search_box_exact_match(self.formulation_map, formulation)
                            if match:
                                fid, matched_api_name = match
                                self.logger.info("[W%d] Fallback to API map: found '%s' -> fid %s", self.worker_id, formulation, fid)
                        except Exception as fallback_exc:
                            self.logger.warning("[W%d] Fallback API map check also failed: %s", self.worker_id, fallback_exc)
                else:
                    # Use API map matching (original approach)
                    try:
                        match = _search_box_exact_match(self.formulation_map, formulation)
                        if match:
                            fid, matched_api_name = match
                    except Exception as exc:
                        self.logger.warning("[W%d] Search-box check failed for '%s': %s", self.worker_id, formulation, exc)
                        self._requeue_as_pending_for_retry(formulation)
                        continue
                
                if not fid:
                    # Not in formulation list — retry once in case API list was still loading
                    try:
                        cur = self.db.execute(
                            "SELECT attempts FROM in_formulation_status WHERE formulation = %s AND run_id = %s",
                            (formulation, self.run_id),
                        )
                        row = cur.fetchone()
                        attempts = (row[0] or 0)
                    except Exception as exc:
                        self.logger.warning("[W%d] DB read failed for '%s': %s", self.worker_id, formulation, exc)
                        self._requeue_as_pending_for_retry(formulation)
                        continue
                    if attempts < max(1, LOOKUP_RETRIES) - 1:
                        self._requeue_as_pending_for_retry(formulation)
                        self.logger.debug(
                            "[W%d] '%s' not found, requeuing for retry (attempts=%d)",
                            self.worker_id, formulation, attempts,
                        )
                        continue
                    self.logger.warning("[W%d] '%s' not found after retry, skipping", self.worker_id, formulation)
                    self._mark_formulation(formulation, "zero_records")
                    self.stats_zero += 1
                    continue
                
                # Store matched name for verification
                if matched_api_name:
                    self.formulation_id_to_name[fid] = matched_api_name
                
                actionable.append((formulation, fid))

            if not actionable:
                continue

            # Set batch counter — next batch claimed only when all formulations' details complete
            self._batch_pending_formulations = len(actionable)

            for i, (formulation, fid) in enumerate(actionable):
                # Verify: fid matches this formulation (don't call API with wrong id)
                # Skip verification if using Selenium (already verified during dropdown interaction)
                if not USE_SELENIUM_DROPDOWN:
                    api_name = self.formulation_id_to_name.get(fid)
                    if api_name is None or api_name not in _exact_match_variants(formulation):
                        self.logger.warning(
                            "[W%d] Skipping '%s': fid %s not exact match in search box (api_name=%s)",
                            self.worker_id, formulation, fid, api_name,
                        )
                        self._mark_formulation(formulation, "zero_records")
                        self.stats_zero += 1
                        self._batch_pending_formulations -= 1
                        continue
                
                # Search box exact match done (via API map or Selenium); now get API details
                # (no next claim until this one completes)
                self.logger.info("[W%d] Search match: '%s' -> fid %s, fetching API", self.worker_id, formulation, fid)
                params = {"formulationId": fid, "strengthId": "0", "dosageId": "0"}
                url = build_api_url(API_FORMULATION_TABLE, params)
                platform_url_id = self._register_platform_url(
                    url=url,
                    source="api",
                    entity_type="formulation_table",
                    metadata={"formulation": formulation, "formulation_id": fid},
                )

                yield Request(
                    url=url,
                    callback=self._parse_formulation_table,
                    headers={"Referer": SEARCH_URL},
                    meta={
                        "formulation": formulation,
                        "formulation_id": fid,
                        "_platform_url_id": platform_url_id,
                        "source": "api",
                        "entity_type": "formulation_table",
                        "handle_httpstatus_list": [200, 599],  # Handle 599 so errback is called
                    },
                    dont_filter=True,
                    errback=self._formulation_error,
                )

            return

    def _parse_formulation_table(self, response: TextResponse):
        """Step 4: Parse SKU list for a formulation, store MAIN rows, then fetch details."""
        formulation = response.meta["formulation"]
        self._touch_claim(formulation)
        
        # Handle HTTP 599 (connection timeout) - should have been retried, but if it reaches here, mark as failed
        if response.status == 599:
            self.logger.error(
                "[W%d] HTTP 599 (connection timeout) for '%s' after retries",
                self.worker_id, formulation
            )
            self._mark_formulation(formulation, "failed", error=f"HTTP 599: connection timeout")
            self.stats_errors += 1
            self._batch_pending_formulations -= 1
            if self._batch_pending_formulations <= 0:
                yield from self._claim_and_process()
            return
        
        table_data = safe_json(response)

        if table_data is None:
            retry_n = int(response.meta.get("json_retry", 0))
            if retry_n < MAX_RETRIES:
                self.logger.warning(
                    "[W%d] Non-JSON/empty response for '%s' (status=%s). Retrying %d/%d",
                    self.worker_id,
                    formulation,
                    response.status,
                    retry_n + 1,
                    MAX_RETRIES,
                )
                meta = dict(response.meta)
                meta["json_retry"] = retry_n + 1
                yield response.request.replace(dont_filter=True, meta=meta)
                return

            self.logger.error(
                "[W%d] Non-JSON/empty response for '%s' after retries (status=%s)",
                self.worker_id,
                formulation,
                response.status,
            )
            self._mark_formulation(formulation, "failed", error="Non-JSON/empty response")
            self.stats_errors += 1
            self._batch_pending_formulations -= 1
            if self._batch_pending_formulations <= 0:
                yield from self._claim_and_process()
            return

        if not isinstance(table_data, list):
            self.logger.error("[W%d] Unexpected response for '%s'", self.worker_id, formulation)
            self._mark_formulation(formulation, "failed", error="Unexpected response type")
            self.stats_errors += 1
            self._batch_pending_formulations -= 1
            if self._batch_pending_formulations <= 0:
                yield from self._claim_and_process()
            return

        if not table_data:
            self._mark_formulation(formulation, "zero_records")
            self.stats_zero += 1
            self._batch_pending_formulations -= 1
            if self._batch_pending_formulations <= 0:
                yield from self._claim_and_process()
            return

        # Truncate if needed
        max_rows = int(os.getenv("MAX_MEDICINES_PER_FORMULATION", "5000"))
        if len(table_data) > max_rows:
            self.logger.warning("[W%d] Truncating '%s' from %d to %d",
                                self.worker_id, formulation, len(table_data), max_rows)
            table_data = table_data[:max_rows]

        # Store MAIN SKU rows in DB
        sku_rows = []
        for row in table_data:
            if not isinstance(row, dict):
                continue
            hid = sget(row, "hiddenId").strip()
            if not hid:
                continue
            sku_rows.append({
                "run_id": self.run_id,
                "formulation": formulation,
                "hidden_id": hid,
                "sku_name": sget(row, "skuName"),
                "company": sget(row, "company"),
                "composition": sget(row, "composition"),
                "pack_size": sget(row, "packSize"),
                "dosage_form": sget(row, "dosageForm"),
                "schedule_status": sget(row, "scheduleStatus"),
                "ceiling_price": sget(row, "ceilingPrice"),
                "mrp": sget(row, "mrp"),
                "mrp_per_unit": sget(row, "mrpPerUnit"),
                "year_month": sget(row, "yearMonth"),
            })

        if sku_rows:
            upsert_items(self.db, "in_sku_main", sku_rows,
                         conflict_columns=["hidden_id", "run_id"])
            print(f"[DB] W{self.worker_id} | UPSERT | in_sku_main +{len(sku_rows)} rows ({formulation})", flush=True)
            self._upsert_platform_entities(sku_rows, response.meta.get("_platform_url_id"))

        # Count detail requests to track completion
        detail_count = 0
        detail_requests = []
        for row in table_data:
            if not isinstance(row, dict):
                continue
            hid = sget(row, "hiddenId").strip()
            if not hid:
                continue

            params_hid = {"hiddenId": hid}

            # skuMrpNew ? priority=1 ensures details run before next formulation table (priority=0)
            sku_mrp_url = build_api_url(API_SKU_MRP, params_hid)
            sku_mrp_url_id = self._register_platform_url(
                url=sku_mrp_url,
                source="api",
                entity_type="sku_mrp",
                metadata={"hidden_id": hid},
            )
            detail_requests.append(Request(
                url=sku_mrp_url,
                callback=self._parse_sku_mrp,
                errback=self._detail_error,
                headers={"Referer": SEARCH_URL},
                meta={"hidden_id": hid, "formulation": formulation, "api": "skuMrp",
                      "_platform_url_id": sku_mrp_url_id, "source": "api", "entity_type": "sku_mrp",
                      "handle_httpstatus_list": [200, 599]},  # Handle 599 so errback is called
                dont_filter=True,
                priority=1,
            ))

            # otherBrandPriceNew
            other_url = build_api_url(API_OTHER_BRANDS, params_hid)
            other_url_id = self._register_platform_url(
                url=other_url,
                source="api",
                entity_type="other_brands",
                metadata={"hidden_id": hid},
            )
            detail_requests.append(Request(
                url=other_url,
                callback=self._parse_other_brands,
                errback=self._detail_error,
                headers={"Referer": SEARCH_URL},
                meta={"hidden_id": hid, "formulation": formulation, "api": "otherBrands",
                      "_platform_url_id": other_url_id, "source": "api", "entity_type": "other_brands",
                      "handle_httpstatus_list": [200, 599]},  # Handle 599 so errback is called
                dont_filter=True,
                priority=1,
            ))

            # medDtlsNew
            med_url = build_api_url(API_MED_DTLS, params_hid)
            med_url_id = self._register_platform_url(
                url=med_url,
                source="api",
                entity_type="med_details",
                metadata={"hidden_id": hid},
            )
            detail_requests.append(Request(
                url=med_url,
                callback=self._parse_med_details,
                errback=self._detail_error,
                headers={"Referer": SEARCH_URL},
                meta={"hidden_id": hid, "formulation": formulation, "api": "medDtls",
                      "_platform_url_id": med_url_id, "source": "api", "entity_type": "med_details",
                      "handle_httpstatus_list": [200, 599]},  # Handle 599 so errback is called
                dont_filter=True,
                priority=1,
            ))
            detail_count += 3

        # Register pending details — formulation marked complete only when all finish
        self.stats_medicines += len(sku_rows)
        if detail_count > 0:
            self._pending_details[formulation] = {
                "pending": detail_count,
                "sku_count": len(sku_rows),
            }
            for req in detail_requests:
                yield req
        else:
            # No detail requests (no valid hidden_ids) — mark complete now
            self.stats_completed += 1
            self._mark_formulation(formulation, "completed", medicines=len(sku_rows))
            self._batch_pending_formulations -= 1
            # PERFORMANCE FIX: Log performance stats periodically
            self._log_performance_if_needed()
            if self._batch_pending_formulations <= 0:
                yield from self._claim_and_process()

    def _parse_sku_mrp(self, response: TextResponse):
        """Store skuMrpNew JSON payload (buffered — committed on formulation completion)."""
        hid = response.meta["hidden_id"]
        formulation = response.meta["formulation"]
        self._touch_claim(formulation)
        
        # Handle HTTP 599 (connection timeout) - should go to errback, but safety check
        if response.status == 599:
            self.logger.warning("[W%d] HTTP 599 in sku_mrp callback (should have gone to errback)", self.worker_id)
            # Let errback handle it via _detail_error
            return
        
        payload = safe_json(response)
        self._reset_circuit_breaker()  # Successful response — reset circuit breaker
        # PERFORMANCE: Buffer instead of immediate commit (use DO NOTHING for safety)
        self._buffer_stmt(formulation,
            "INSERT INTO in_sku_mrp (run_id, hidden_id, payload_json) VALUES (%s, %s, %s) "
            "ON CONFLICT DO NOTHING",
            (self.run_id, hid, json.dumps(payload, ensure_ascii=False)),
        )
        self._upsert_platform_attributes(hid, {"sku_mrp": payload})
        if self._detail_done(formulation):
            yield from self._claim_and_process()

    def _parse_other_brands(self, response: TextResponse):
        """Store otherBrandPriceNew as brand_alternatives rows (buffered)."""
        hid = response.meta["hidden_id"]
        formulation = response.meta["formulation"]
        self._touch_claim(formulation)
        
        # Handle HTTP 599 (connection timeout) - should go to errback, but safety check
        if response.status == 599:
            self.logger.warning("[W%d] HTTP 599 in other_brands callback (should have gone to errback)", self.worker_id)
            # Let errback handle it via _detail_error
            return
        
        payload = safe_json(response)
        self._reset_circuit_breaker()  # Successful response — reset circuit breaker

        if not isinstance(payload, list):
            payload = [payload] if isinstance(payload, dict) else []

        brand_rows = []
        for other in payload:
            if not isinstance(other, dict):
                continue
            brand_rows.append({
                "run_id": self.run_id,
                "hidden_id": hid,
                "formulation": formulation,  # Store for fast queries without JOIN
                "brand_name": sget(other, "brandName"),
                "company": sget(other, "company"),
                "pack_size": sget(other, "packSize"),
                "brand_mrp": sget(other, "brandMrp"),
                "mrp_per_unit": sget(other, "mrpPerUnit"),
                "year_month": sget(other, "yearMonth"),
            })

        if brand_rows:
            # PERFORMANCE: Buffer instead of immediate bulk_insert + commit
            self._buffer_brand_rows(formulation, brand_rows)
            self.stats_substitutes += len(brand_rows)

        self._upsert_platform_attributes(hid, {"other_brands": payload})

        if self._detail_done(formulation):
            yield from self._claim_and_process()

    def _parse_med_details(self, response: TextResponse):
        """Store medDtlsNew JSON payload (buffered — committed on formulation completion)."""
        hid = response.meta["hidden_id"]
        formulation = response.meta["formulation"]
        self._touch_claim(formulation)
        
        # Handle HTTP 599 (connection timeout) - should go to errback, but safety check
        if response.status == 599:
            self.logger.warning("[W%d] HTTP 599 in med_details callback (should have gone to errback)", self.worker_id)
            # Let errback handle it via _detail_error
            return
        
        payload = safe_json(response)
        self._reset_circuit_breaker()  # Successful response — reset circuit breaker
        # PERFORMANCE: Buffer instead of immediate commit (use DO NOTHING for safety)
        self._buffer_stmt(formulation,
            "INSERT INTO in_med_details (run_id, hidden_id, payload_json) VALUES (%s, %s, %s) "
            "ON CONFLICT DO NOTHING",
            (self.run_id, hid, json.dumps(payload, ensure_ascii=False)),
        )
        self._upsert_platform_attributes(hid, {"med_details": payload})
        if self._detail_done(formulation):
            yield from self._claim_and_process()

    def _detail_done(self, formulation: str):
        """Decrement pending counter for a formulation; flush DB writes + mark complete when all details done."""
        info = self._pending_details.get(formulation)
        if not info:
            return False  # already completed or not tracked
        info["pending"] -= 1
        if info["pending"] <= 0:
            # PERFORMANCE: Flush all buffered detail writes in one commit
            self._flush_writes(formulation)
            self.stats_completed += 1
            self._mark_formulation(formulation, "completed", medicines=info["sku_count"])
            del self._pending_details[formulation]
            self._batch_pending_formulations -= 1
            if self._batch_pending_formulations <= 0:
                return True  # all formulations in batch done — claim next
        return False

    def _detail_error(self, failure):
        """Handle failure for detail API requests (skuMrp, otherBrands, medDtls).

        Enhanced with failure type logging and circuit breaker tracking.
        """
        meta = failure.request.meta
        api = meta.get("api", "unknown")
        hid = meta.get("hidden_id", "unknown")
        formulation = meta.get("formulation", "unknown")
        self._touch_claim(formulation)

        # Enhanced error context: include failure type and HTTP status if available
        fail_type = failure.type.__name__ if failure.type else "Unknown"
        fail_msg = str(failure.value)
        http_status = getattr(failure.value, 'response', None)
        status_info = f" (HTTP {http_status.status})" if http_status and hasattr(http_status, 'status') else ""

        self.logger.warning(
            "[W%d] Detail API '%s' failed for hid=%s formulation='%s' [%s%s]: %s",
            self.worker_id, api, hid[:30], formulation, fail_type, status_info, fail_msg,
        )

        # Circuit breaker: track consecutive failures
        self._consecutive_failures += 1
        if self._consecutive_failures >= CIRCUIT_BREAKER_THRESHOLD and not self._circuit_breaker_open:
            self._circuit_breaker_open = True
            self._circuit_breaker_until = time.monotonic() + CIRCUIT_BREAKER_COOLDOWN_SECONDS
            self.logger.error(
                "[W%d] ⚡ CIRCUIT BREAKER OPEN — %d consecutive API failures. "
                "Pausing for %d seconds before retrying.",
                self.worker_id, self._consecutive_failures, CIRCUIT_BREAKER_COOLDOWN_SECONDS,
            )

        if self._detail_done(formulation):
            yield from self._claim_and_process()

    def _warmup_error(self, failure):
        """Handle warmup request failure."""
        msg = str(failure.value)
        self.logger.error("[W%d] Warmup request failed: %s", self.worker_id, msg)
        # Close spider if we can't establish session
        raise scrapy.exceptions.CloseSpider(f"warmup_failed: {msg}")

    def _formulation_list_error(self, failure):
        """Handle formulation list request failure."""
        msg = str(failure.value)
        self.logger.error("[W%d] Formulation list request failed: %s", self.worker_id, msg)
        # Close spider if we can't get formulation list
        raise scrapy.exceptions.CloseSpider(f"formulation_list_failed: {msg}")

    def _formulation_error(self, failure):
        """Handle request failure for a formulation."""
        formulation = failure.request.meta.get("formulation", "unknown")
        self._touch_claim(formulation)
        msg = str(failure.value)
        self.stats_errors += 1
        # Flush any buffered writes before marking failed
        self._flush_writes(formulation)
        self._mark_formulation(formulation, "failed", error=msg)
        self.logger.error("[W%d] Formulation '%s' failed: %s", self.worker_id, formulation, msg)

        self._batch_pending_formulations -= 1
        if self._batch_pending_formulations <= 0:
            yield from self._claim_and_process()

    # ------------------------------------------------------------------
    # DB Helpers
    # ------------------------------------------------------------------

    def _init_db(self):
        """Open DB connection (schemas already applied by run_scrapy_india.py)."""
        self.db = PostgresDB("India")
        self.db.connect()
        self.logger.info("[W%d] DB connected to PostgreSQL, run_id=%s", self.worker_id, self.run_id)

    def _reconnect_db(self):
        """Reconnect to DB after connection loss (network failure, timeout, etc.)."""
        self.logger.warning("[W%d] Reconnecting to PostgreSQL...", self.worker_id)
        try:
            if self.db:
                try:
                    self.db.close()
                except Exception:
                    pass
        except Exception:
            pass
        self.db = PostgresDB("India")
        self.db.connect()
        self.logger.info("[W%d] DB reconnected successfully", self.worker_id)

    def _check_circuit_breaker(self) -> bool:
        """Check if circuit breaker is active. Returns True if requests should proceed.

        When too many consecutive API failures occur, the circuit breaker opens
        and pauses requests for CIRCUIT_BREAKER_COOLDOWN_SECONDS to avoid
        hammering a down server.
        """
        if not self._circuit_breaker_open:
            return True

        now = time.monotonic()
        if now >= self._circuit_breaker_until:
            self.logger.info(
                "[W%d] ⚡ Circuit breaker CLOSED — cooldown expired, resuming requests",
                self.worker_id,
            )
            self._circuit_breaker_open = False
            self._consecutive_failures = 0
            return True

        remaining = int(self._circuit_breaker_until - now)
        self.logger.debug(
            "[W%d] Circuit breaker still open, %d seconds remaining",
            self.worker_id, remaining,
        )
        return False

    def _reset_circuit_breaker(self):
        """Reset circuit breaker on successful API response."""
        if self._consecutive_failures > 0:
            self._consecutive_failures = 0
        if self._circuit_breaker_open:
            self._circuit_breaker_open = False
            self.logger.info("[W%d] ⚡ Circuit breaker CLOSED after successful response", self.worker_id)

    # ------------------------------------------------------------------
    # PERFORMANCE: Buffered DB writes — commit once per formulation
    # ------------------------------------------------------------------

    def _buffer_stmt(self, formulation: str, sql: str, params: tuple):
        """Buffer an individual SQL statement for later batch commit."""
        buf = self._write_buffer.setdefault(formulation, {"stmts": [], "brand_rows": []})
        buf["stmts"].append((sql, params))

    def _buffer_brand_rows(self, formulation: str, rows: List[Dict[str, Any]]):
        """Buffer brand_alternatives rows for later batch commit."""
        buf = self._write_buffer.setdefault(formulation, {"stmts": [], "brand_rows": []})
        buf["brand_rows"].extend(rows)

    def _flush_writes(self, formulation: str, _retry: int = 0):
        """Flush all buffered writes for a formulation in a single atomic commit.

        Uses a savepoint so that partial failures don't leave inconsistent data.
        Uses execute_values for ~5-10x faster brand_alternatives batch inserts.
        Includes DB reconnection on connection failure (max 2 retries).
        """
        buf = self._write_buffer.pop(formulation, None)
        if not buf:
            return

        try:
            conn = self.db._conn
        except Exception:
            if _retry < 2:
                self._reconnect_db()
                # Put buffer back so retry can use it
                self._write_buffer[formulation] = buf
                return self._flush_writes(formulation, _retry=_retry + 1)
            raise

        cur = conn.cursor()
        try:
            # Use savepoint for atomic all-or-nothing per formulation
            cur.execute("SAVEPOINT flush_formulation")

            # Execute individual statements (sku_mrp, med_details)
            for sql, params in buf["stmts"]:
                cur.execute(sql, params)

            # Bulk upsert brand_alternatives (avoid duplicates on retry)
            brand_rows = buf["brand_rows"]
            if brand_rows:
                columns = list(brand_rows[0].keys())
                col_str = ", ".join(columns)
                rows_data = [tuple(r.get(c) for c in columns) for r in brand_rows]

                # Use execute_values for 5-10x faster inserts (single statement)
                try:
                    from psycopg2.extras import execute_values as _ev
                    _ev(
                        cur,
                        f"INSERT INTO in_brand_alternatives ({col_str}) VALUES %s "
                        f"ON CONFLICT (hidden_id, brand_name, pack_size, run_id) DO NOTHING",
                        rows_data,
                        page_size=500,
                    )
                except ImportError:
                    # Fallback: executemany (slower but functional)
                    placeholders = ", ".join(["%s"] * len(columns))
                    insert_sql = (
                        f"INSERT INTO in_brand_alternatives ({col_str}) VALUES ({placeholders}) "
                        f"ON CONFLICT (hidden_id, brand_name, pack_size, run_id) DO NOTHING"
                    )
                    cur.executemany(insert_sql, rows_data)

            cur.execute("RELEASE SAVEPOINT flush_formulation")
            conn.commit()
        except Exception as exc:
            try:
                cur.execute("ROLLBACK TO SAVEPOINT flush_formulation")
                conn.commit()  # Commit the rollback to release the savepoint
            except Exception:
                conn.rollback()

            # Retry on connection-level errors (network drop, etc.)
            import psycopg2
            if isinstance(exc, (psycopg2.OperationalError, psycopg2.InterfaceError)) and _retry < 2:
                self.logger.warning(
                    "[W%d] DB connection lost during flush for '%s', reconnecting (attempt %d/2)",
                    self.worker_id, formulation, _retry + 1,
                )
                self._reconnect_db()
                self._write_buffer[formulation] = buf
                return self._flush_writes(formulation, _retry=_retry + 1)
            raise
        finally:
            cur.close()

    def _ensure_platform(self):
        if self._platform_ready:
            return
        try:
            from services.db import ensure_platform_schema
            ensure_platform_schema()
            self._platform_ready = True
        except Exception:
            self._platform_ready = False

    def _register_platform_url(self, url: str, source: Optional[str],
                               entity_type: Optional[str], metadata: Optional[Dict]):
        if not self._platform_ready:
            return None
        try:
            from services.db import upsert_url
            return upsert_url(url, self.country_name, source=source, entity_type=entity_type, metadata=metadata)
        except Exception:
            return None

    def _upsert_platform_entities(self, sku_rows: List[Dict[str, Any]], source_url_id: Optional[int]):
        if not self._platform_ready:
            return
        try:
            from services.db import insert_entity, insert_attribute
        except Exception:
            return

        for row in sku_rows:
            hid = row.get("hidden_id") or ""
            if not hid:
                continue
            try:
                entity_id = insert_entity(
                    entity_type="sku",
                    country=self.country_name,
                    source_url_id=source_url_id,
                    run_id=self.platform_run_id,
                    external_id=hid,
                    data=row,
                )
                self._entity_ids[hid] = entity_id
                for name, value in row.items():
                    if name == "run_id":
                        continue
                    insert_attribute(entity_id, name, value, source="scrape")
            except Exception:
                continue

    def _upsert_platform_attributes(self, hidden_id: str, attrs: Dict[str, Any]):
        if not self._platform_ready:
            return
        entity_id = self._entity_ids.get(hidden_id)
        if not entity_id:
            return
        try:
            from services.db import insert_attribute
            for name, value in attrs.items():
                insert_attribute(entity_id, name, value, source="scrape")
        except Exception:
            return

    def _prefilter_pending_queue(self):
        """Mark pending formulations not present in API map as zero_records (once per run)."""
        cur = self.db.execute(
            "SELECT formulation FROM in_formulation_status "
            "WHERE run_id = %s AND status = 'pending'",
            (self.run_id,),
        )
        rows = [r[0] for r in cur.fetchall() if r and r[0]]
        if not rows:
            return
        to_zero: List[str] = []
        for formulation in rows:
            if not _search_box_exact_match(self.formulation_map, formulation):
                to_zero.append(formulation)
        if not to_zero:
            return
        chunk_size = 500
        for i in range(0, len(to_zero), chunk_size):
            chunk = to_zero[i:i + chunk_size]
            self.db.execute(
                "UPDATE in_formulation_status "
                "SET status='zero_records', error_message='not_in_api_map', "
                "updated_at=CURRENT_TIMESTAMP "
                "WHERE run_id=%s AND status='pending' AND formulation = ANY(%s)",
                (self.run_id, chunk),
            )
        self.db.commit()
        self.logger.info(
            "[W%d] Prefiltered %d/%d pending formulations not in API map",
            self.worker_id, len(to_zero), len(rows),
        )

    def _check_and_recover_stuck_items(self, completion_pct: float, total: int, done: int) -> int:
        """
        Check for stuck items near completion and recover them.
        
        When we're very close to completion (e.g., 99.5%+), items stuck in pending/in_progress
        for too long may be preventing completion. This method:
        1. Checks if we're above completion threshold
        2. Finds items stuck for longer than COMPLETION_TIMEOUT_MINUTES
        3. Recovers them (resets claimed_at for pending, marks as failed if exceeded retries)
        
        Returns: number of items recovered
        """
        if completion_pct < COMPLETION_THRESHOLD_PCT:
            self._stuck_items_detected_at = None
            return 0
        
        now = time.monotonic()
        if self._stuck_items_detected_at is None:
            self._stuck_items_detected_at = now
        
        # Only recover if stuck items have been waiting for COMPLETION_TIMEOUT_MINUTES
        elapsed_minutes = (now - self._stuck_items_detected_at) / 60.0
        if elapsed_minutes < COMPLETION_TIMEOUT_MINUTES:
            return 0
        
        # Find stuck pending items (with future claimed_at due to backoff)
        cur = self.db.execute("""
            SELECT formulation, attempts, claimed_at
            FROM in_formulation_status
            WHERE run_id = %s AND status = 'pending'
              AND claimed_at > CURRENT_TIMESTAMP
              AND updated_at < CURRENT_TIMESTAMP - (%s * INTERVAL '1 minute')
            ORDER BY updated_at ASC
            LIMIT 10
        """, (self.run_id, COMPLETION_TIMEOUT_MINUTES))
        stuck_pending = cur.fetchall()
        
        # Find stuck in_progress items (claimed but not updated recently)
        cur = self.db.execute("""
            SELECT formulation, attempts, claimed_at
            FROM in_formulation_status
            WHERE run_id = %s AND status = 'in_progress'
              AND claimed_at < CURRENT_TIMESTAMP - (%s * INTERVAL '1 minute')
            ORDER BY claimed_at ASC
            LIMIT 10
        """, (self.run_id, COMPLETION_TIMEOUT_MINUTES))
        stuck_in_progress = cur.fetchall()
        
        recovered = 0
        
        # Recover stuck pending items: reset claimed_at to make them claimable
        for formulation, attempts, claimed_at in stuck_pending:
            if attempts >= max(1, LOOKUP_RETRIES):
                # Exceeded retries - mark as zero_records
                self.logger.warning(
                    "[W%d] Marking stuck pending item '%s' as zero_records (attempts=%d, exceeded retries)",
                    self.worker_id, formulation, attempts
                )
                self._mark_formulation(formulation, "zero_records")
                self.stats_zero += 1
                recovered += 1
            else:
                # Reset claimed_at to make it claimable immediately
                self.logger.info(
                    "[W%d] Resetting claimed_at for stuck pending item '%s' (attempts=%d)",
                    self.worker_id, formulation, attempts
                )
                self.db.execute("""
                    UPDATE in_formulation_status
                    SET claimed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                    WHERE formulation = %s AND run_id = %s
                """, (formulation, self.run_id))
                self.db.commit()
                recovered += 1
        
        # Recover stuck in_progress items: reset to pending
        for formulation, attempts, claimed_at in stuck_in_progress:
            self.logger.info(
                "[W%d] Resetting stuck in_progress item '%s' to pending (attempts=%d)",
                self.worker_id, formulation, attempts
            )
            self.db.execute("""
                UPDATE in_formulation_status
                SET status = 'pending', claimed_by = NULL,
                    claimed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                WHERE formulation = %s AND run_id = %s
            """, (formulation, self.run_id))
            self.db.commit()
            recovered += 1
        
        if recovered > 0:
            self.logger.info(
                "[W%d] Recovered %d stuck item(s) near completion (%.1f%% done, waited %.1f min)",
                self.worker_id, recovered, completion_pct, elapsed_minutes
            )
            # Reset detection timer after recovery
            self._stuck_items_detected_at = None
        
        return recovered

    def _claim_batch(self) -> List[str]:
        """Atomically claim a batch of pending formulations for this worker.

        Uses SELECT FOR UPDATE SKIP LOCKED to atomically transition rows
        from 'pending' to 'in_progress', preventing double-scraping.
        """
        # Use advisory lock or FOR UPDATE SKIP LOCKED for PostgreSQL
        # First, select and lock pending formulations
        cur = self.db.execute("""
            WITH claimed AS (
                SELECT formulation FROM in_formulation_status
                WHERE status = 'pending' AND run_id = %s
                  AND (claimed_at IS NULL OR claimed_at <= CURRENT_TIMESTAMP)
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE in_formulation_status fs
            SET status = 'in_progress', worker_id = %s,
                claimed_by = %s, claimed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            FROM claimed c
            WHERE fs.formulation = c.formulation AND fs.run_id = %s
            RETURNING fs.formulation
        """, (self.run_id, CLAIM_BATCH_SIZE, self.worker_id, self.worker_id, self.run_id))

        claimed = [row[0] for row in cur.fetchall()]
        self.db.commit()
        return claimed

    def _requeue_as_pending_for_retry(self, formulation: str) -> bool:
        """Requeue formulation as pending so it can be retried. Returns True if requeued, False if max retries exceeded."""
        # First check current attempts
        cur = self.db.execute(
            "SELECT attempts FROM in_formulation_status WHERE formulation = %s AND run_id = %s",
            (formulation, self.run_id)
        )
        row = cur.fetchone()
        current_attempts = (row[0] or 0) if row else 0

        # Check if max retries exceeded
        if current_attempts >= MAX_RETRIES:
            self.logger.warning(
                "[W%d] '%s' exceeded max retries (%d/%d), marking as zero_records",
                self.worker_id, formulation, current_attempts, MAX_RETRIES
            )
            self._mark_formulation(formulation, "zero_records", error=f"max_retries_exceeded ({current_attempts})")
            self.stats_zero += 1
            return False

        # Calculate simple backoff: 10s, 30s, 60s (capped at 1 minute)
        backoff_seconds = min(60, 10 * (2 ** current_attempts))

        self.logger.info(
            "[W%d] Requeuing '%s' for retry (attempt %d/%d, backoff %ds)",
            self.worker_id, formulation, current_attempts + 1, MAX_RETRIES, backoff_seconds
        )

        self.db.execute("""
            UPDATE in_formulation_status
            SET status = 'pending', claimed_by = NULL,
                claimed_at = CURRENT_TIMESTAMP + (%s * INTERVAL '1 second'),
                attempts = attempts + 1, updated_at = CURRENT_TIMESTAMP
            WHERE formulation = %s AND run_id = %s
        """, (backoff_seconds, formulation, self.run_id))
        self.db.commit()
        return True

    def _mark_formulation(self, formulation: str, status: str,
                          medicines: int = 0, substitutes: int = 0,
                          error: Optional[str] = None):
        """Update formulation_status table."""
        self.db.execute("""
            UPDATE in_formulation_status
            SET status = %s, medicines_count = %s, substitutes_count = %s,
                error_message = %s, attempts = attempts + 1, updated_at = CURRENT_TIMESTAMP
            WHERE formulation = %s AND run_id = %s
        """, (status, medicines, substitutes, error, formulation, self.run_id))
        self.db.commit()
        self._emit_progress(formulation, status, medicines)

    def _touch_claim(self, formulation: str):
        """Refresh claimed_at for in_progress rows (lease heartbeat), throttled per formulation."""
        if not formulation or not self.db:
            return
        now = time.monotonic()
        last = self._last_claim_touch.get(formulation, 0.0)
        if CLAIM_TOUCH_INTERVAL_SECONDS > 0 and (now - last) < CLAIM_TOUCH_INTERVAL_SECONDS:
            return
        try:
            self.db.execute(
                "UPDATE in_formulation_status "
                "SET claimed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP "
                "WHERE run_id = %s AND formulation = %s AND status = 'in_progress' AND claimed_by = %s",
                (self.run_id, formulation, self.worker_id),
            )
            self.db.commit()
            self._last_claim_touch[formulation] = now
        except Exception:
            return

    def _emit_progress(self, formulation: str, status: str, medicines: int):
        """Print [DB] activity line for GUI consumption.

        Note: [PROGRESS] lines are emitted by ProgressReporter in run_scrapy_india.py
        which aggregates all workers into a single status line.
        """
        if status == "zero_records":
            return  # Skip noisy zero-record lines
        tag = "OK" if status == "completed" else status.upper()
        med_info = f" ({medicines} medicines)" if medicines > 0 else ""
        print(f"[DB] W{self.worker_id} | {tag} | {formulation}{med_info} | sku_main={self.stats_medicines} brands={self.stats_substitutes}", flush=True)

    # ------------------------------------------------------------------
    # Spider close
    # ------------------------------------------------------------------

    def _log_performance_if_needed(self):
        """PERFORMANCE FIX: Log memory and performance stats periodically"""
        if self.stats_completed - self._last_perf_log >= self._perf_log_interval:
            self._last_perf_log = self.stats_completed
            try:
                import os
                import psutil
                proc = psutil.Process(os.getpid())
                mem_mb = proc.memory_info().rss / 1024 / 1024
                self.logger.info(
                    "[W%d] [PERFORMANCE] Memory: %.1fMB | Completed: %d | Medicines: %d | Errors: %d",
                    self.worker_id, mem_mb, self.stats_completed, 
                    self.stats_medicines, self.stats_errors
                )
            except Exception:
                pass  # psutil not available, skip silently

    def closed(self, reason):
        """Finalize: flush remaining writes, close DB."""
        if self.db:
            # Flush any remaining buffered writes
            for formulation in list(self._write_buffer.keys()):
                try:
                    self._flush_writes(formulation)
                except Exception as exc:
                    self.logger.warning("[W%d] Failed to flush writes for '%s': %s",
                                        self.worker_id, formulation, exc)
            
            # Log final completion status from DB
            try:
                cur = self.db.execute(
                    "SELECT status, COUNT(*) FROM in_formulation_status "
                    "WHERE run_id = %s GROUP BY status",
                    (self.run_id,),
                )
                status_counts = dict(cur.fetchall())
                total = sum(status_counts.values())
                completed = status_counts.get('completed', 0)
                zero_rec = status_counts.get('zero_records', 0)
                failed = status_counts.get('failed', 0)
                pending = status_counts.get('pending', 0)
                in_progress = status_counts.get('in_progress', 0)
                done = completed + zero_rec + failed
                pct = round((done / total * 100), 1) if total > 0 else 0
                
                self.logger.info(
                    "[W%d] 📊 Final status: %d/%d formulations done (%.1f%%) | "
                    "completed=%d, zero_records=%d, failed=%d, pending=%d, in_progress=%d",
                    self.worker_id, done, total, pct, completed, zero_rec, failed, pending, in_progress
                )
                
                if done == total:
                    self.logger.info("[W%d] ✅ All formulations completed successfully!", self.worker_id)
                    print(f"[DB] W{self.worker_id} | FINAL | All {total} formulations completed | completed={completed} zero={zero_rec} failed={failed}", flush=True)
            except Exception as exc:
                self.logger.debug("Could not fetch final status: %s", exc)
            
            self.db.close()

        self.logger.info(
            "[W%d] Spider closed: %s | medicines=%d, substitutes=%d, errors=%d, completed=%d, zero=%d",
            self.worker_id, reason, self.stats_medicines, self.stats_substitutes,
            self.stats_errors, self.stats_completed, self.stats_zero,
        )
