#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Argentina - API Scraper (Backup, DB-only)
Processes products selected from ar_product_index where:
  total_records == 0 and loop_count >= SELENIUM_MAX_RUNS
"""

import csv
import re
import json
import time
import logging
import threading
import argparse
from pathlib import Path
from datetime import datetime
from queue import Queue, Empty
from typing import Optional, Tuple, List, Dict, Any

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

# ====== CONFIG ======
from config_loader import (
    get_input_dir, get_output_dir, get_proxy_list,
    ALFABETA_USER, ALFABETA_PASS, HEADLESS, HUB_URL, PRODUCTS_URL,
    SCRAPINGDOG_API_KEY, SCRAPINGDOG_URL,
    RATE_LIMIT_PRODUCTS, RATE_LIMIT_SECONDS,
    REQUEST_PAUSE_BASE, REQUEST_PAUSE_JITTER_MIN, REQUEST_PAUSE_JITTER_MAX,
    API_REQUEST_TIMEOUT, QUEUE_GET_TIMEOUT, PAUSE_HTML_LOAD,
    API_THREADS,
    IGNORE_LIST_FILE,
    SELENIUM_MAX_RUNS,
    OUTPUT_PRODUCTS_CSV, OUTPUT_ERRORS_CSV,
    API_INPUT_CSV
)

from scraper_utils import (
    ensure_headers, combine_skip_sets,
    append_rows, append_error,
    nk, ts, strip_accents, OUT_FIELDS,
    CSV_LOCK, ERROR_LOCK
)
from core.db.connection import CountryDB
from db.repositories import ArgentinaRepository
from db.schema import apply_argentina_schema
from core.db.models import generate_run_id

# ====== PATHS ======
INPUT_DIR = get_input_dir()
OUTPUT_DIR = get_output_dir()
API_INPUT_FILE_PATH = OUTPUT_DIR / API_INPUT_CSV
OUT_CSV = OUTPUT_DIR / OUTPUT_PRODUCTS_CSV
ERRORS = OUTPUT_DIR / OUTPUT_ERRORS_CSV
LOG_DIR = OUTPUT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"argentina_api_{datetime.now():%Y%m%d_%H%M%S}.log"

# DB setup
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
_RUN_ID_FILE = OUTPUT_DIR / ".current_run_id"

def _get_run_id() -> str:
    rid = os.environ.get("ARGENTINA_RUN_ID")
    if rid:
        return rid
    if _RUN_ID_FILE.exists():
        try:
            txt = _RUN_ID_FILE.read_text(encoding="utf-8").strip()
            if txt:
                return txt
        except Exception:
            pass
    rid = generate_run_id()
    os.environ["ARGENTINA_RUN_ID"] = rid
    _RUN_ID_FILE.write_text(rid, encoding="utf-8")
    return rid

_DB = CountryDB("Argentina")
apply_argentina_schema(_DB)
_RUN_ID = _get_run_id()
_REPO = ArgentinaRepository(_DB, _RUN_ID)

# ====== LOGGING ======
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("api_scraper")
log.info("[LOG] Writing API log to %s", LOG_FILE)

# Request pause jitter tuple
REQUEST_PAUSE_JITTER = (REQUEST_PAUSE_JITTER_MIN, REQUEST_PAUSE_JITTER_MAX)

# ====== RATE LIMITING ======
RATE_LIMIT_LOCK = threading.Lock()
_rate_limit_batch_start = None
_rate_limit_count = 0

# ====== SKIP SET LOCK ======
_skip_lock = threading.Lock()  # Lock for updating skip_set during runtime

# ====== PROGRESS TRACKING =====
_progress_lock = threading.Lock()
_api_products_completed = 0
_api_total_products = 0

def rate_limit_wait():
    """No rate limiting - process immediately"""
    # Rate limiting disabled - process API calls immediately
    pass

# ====== UTILITY FUNCTIONS ======

def normalize_ws(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()

def ar_money_to_float(s: str) -> Optional[float]:
    if not s:
        return None
    t = re.sub(r"[^\d\.,]", "", s.strip())
    if not t:
        return None
    if "," in t and "." in t:
        # AR format: dot thousands, comma decimals
        t = t.replace(".", "").replace(",", ".")
    elif "," in t:
        # Decimal comma
        t = t.replace(",", ".")
    try:
        return float(t)
    except ValueError:
        return None

def parse_date(s: str) -> Optional[str]:
    """Accepts '(24/07/25)' or '24/07/25' or '24-07-2025' → '2025-07-24'"""
    s = (s or "").strip()
    m = re.search(r"\((\d{2})/(\d{2})/(\d{2})\)", s) or re.search(r"\b(\d{2})/(\d{2})/(\d{2})\b", s)
    if m:
        d, mn, y = map(int, m.groups())
        y += 2000
        try:
            return datetime(y, mn, d).date().isoformat()
        except:
            return None
    m = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", s)
    if m:
        y, mn, d = map(int, m.groups())
        try:
            return datetime(y, mn, d).date().isoformat()
        except:
            return None
    m = re.search(r"\b(\d{2})-(\d{2})-(\d{4})\b", s)
    if m:
        d, mn, y = map(int, m.groups())
        try:
            return datetime(y, mn, d).date().isoformat()
        except:
            return None
    return None

def sanitize_product_name_for_url(product_name: str) -> str:
    """Sanitize product name for URL construction."""
    if not product_name:
        return ""
    
    sanitized = strip_accents(product_name)
    sanitized = re.sub(r'\s+\+\s+', '  ', sanitized)  # "+" between spaces -> double space
    sanitized = re.sub(r'\+', '', sanitized)  # Remove remaining + characters
    sanitized = re.sub(r'[^a-zA-Z0-9\s-]', '', sanitized)  # Remove special chars
    sanitized = re.sub(r'  ', ' __DOUBLE__ ', sanitized)  # Preserve double spaces
    sanitized = re.sub(r'\s+', '-', sanitized)  # Replace spaces with hyphens
    sanitized = re.sub(r'__DOUBLE__', '-', sanitized)  # Restore double hyphens
    sanitized = re.sub(r'-{3,}', '--', sanitized)  # Remove triple+ hyphens
    sanitized = sanitized.lower()
    sanitized = sanitized.strip('-')
    
    if sanitized:
        return f"{sanitized}.html"
    return ""

def construct_product_url(product_name: str, base_url: str = None) -> str:
    """Construct product URL from product name."""
    if base_url is None:
        base_url = PRODUCTS_URL
    
    base_url = base_url.rstrip('/')
    sanitized = sanitize_product_name_for_url(product_name)
    
    if not sanitized:
        return ""
    
    return f"{base_url}/{sanitized}"

def extract_json_ld_rows(html: str, in_company: str, in_product: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not html:
        return rows
    for match in re.finditer(r"<script[^>]*type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>", html, flags=re.I | re.S):
        payload = match.group(1).strip()
        if not payload:
            continue
        try:
            data = json.loads(payload)
        except Exception:
            continue
        if isinstance(data, dict) and isinstance(data.get("@graph"), list):
            items = data.get("@graph", [])
        elif isinstance(data, list):
            items = data
        else:
            items = [data]
        for item in items:
            if not isinstance(item, dict):
                continue
            type_val = item.get("@type") or item.get("type")
            if isinstance(type_val, list):
                type_str = " ".join(str(t) for t in type_val)
            else:
                type_str = str(type_val or "")
            if "product" not in type_str.lower():
                continue

            product_name = item.get("name") or in_product
            brand = item.get("brand")
            if isinstance(brand, dict):
                company = brand.get("name")
            elif isinstance(brand, str):
                company = brand
            else:
                company = None
            if not company:
                company = in_company

            active = None
            therap = None
            for prop in item.get("additionalProperty") or []:
                if not isinstance(prop, dict):
                    continue
                prop_name = prop.get("name") or ""
                prop_val = prop.get("value")
                key = nk(prop_name)
                if prop_val is None:
                    continue
                if not active and ("monodroga" in key or "principio" in key or "droga" in key):
                    active = str(prop_val)
                if not therap and ("accion terapeutica" in key or ("accion" in key and "terapeutica" in key)):
                    therap = str(prop_val)

            offers = item.get("offers") or []
            if isinstance(offers, dict):
                offers = [offers]
            if not offers:
                rows.append({
                    "input_company": in_company,
                    "input_product_name": in_product,
                    "company": company,
                    "product_name": product_name,
                    "active_ingredient": active,
                    "therapeutic_class": therap,
                    "description": item.get("description"),
                    "price_ars": None,
                    "date": None,
                    "scraped_at": ts(),
                    "SIFAR_detail": None,
                    "PAMI_AF": None,
                    "IOMA_detail": None,
                    "IOMA_AF": None,
                    "IOMA_OS": None,
                    "import_status": None,
                    "coverage_json": "{}"
                })
            else:
                for offer in offers:
                    if not isinstance(offer, dict):
                        continue
                    desc = offer.get("name")
                    price_val = offer.get("price")
                    date_val = offer.get("priceValidUntil") or ""
                    rows.append({
                        "input_company": in_company,
                        "input_product_name": in_product,
                        "company": company,
                        "product_name": product_name,
                        "active_ingredient": active,
                        "therapeutic_class": therap,
                        "description": desc,
                        "price_ars": ar_money_to_float(str(price_val)) if price_val is not None else None,
                        "date": parse_date(str(date_val)),
                        "scraped_at": ts(),
                        "SIFAR_detail": None,
                        "PAMI_AF": None,
                        "IOMA_detail": None,
                        "IOMA_AF": None,
                        "IOMA_OS": None,
                        "import_status": None,
                        "coverage_json": "{}"
                    })
            if rows:
                return rows
    return rows

# ====== API SCRAPING ======

def parse_html_with_bs4(soup, in_company: str, in_product: str) -> List[Dict[str, Any]]:
    """Parse HTML using BeautifulSoup and extract product information."""
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
            
            # Parse coverage
            cov = {}
            try:
                cob_table = p.select_one("table.coberturas")
                if cob_table:
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
                "coverage_json": json.dumps(cov, ensure_ascii=False) if cov else "{}"
            })
        
        # Don't create fallback row - if no presentation rows found, return empty list
        # This will trigger moving to Selenium without recording anything
    except Exception as e:
        log.error(f"[API] Error parsing HTML with BeautifulSoup: {e}")
    
    return rows

def parse_html_content(html_content: str, in_company: str, in_product: str) -> List[Dict[str, Any]]:
    """Parse HTML content from ScrapingDog API response and extract product rows."""
    rows: List[Dict[str, Any]] = []
    if BEAUTIFULSOUP_AVAILABLE:
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            rows = parse_html_with_bs4(soup, in_company, in_product)
        except Exception as e:
            log.warning(f"[API] BeautifulSoup parsing failed: {e}")
            rows = []

    if rows:
        return rows

    json_rows = extract_json_ld_rows(html_content, in_company, in_product)
    if json_rows:
        return json_rows

    if not BEAUTIFULSOUP_AVAILABLE:
        log.warning("[API] BeautifulSoup not available, cannot parse HTML")
    return []

def scrape_single_product_api_with_url(product_url: str, product_name: str, company: str) -> List[Dict[str, Any]]:
    """Scrape a single product using scrapingdog API with a prepared URL."""
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
            rows = parse_html_content(html_content, company, product_name)
            return rows
        else:
            log.warning(f"[API] Failed to fetch {product_name}: HTTP {response.status_code}")
            return []
    except requests.exceptions.Timeout as e:
        log.warning(f"[API] Timeout fetching {product_name} (timeout={API_REQUEST_TIMEOUT}s): {e}")
        log.info(f"[API] Will retry {product_name} with Selenium")
        return []
    except requests.exceptions.RequestException as e:
        log.error(f"[API] Request error fetching {product_name}: {e}")
        return []
    except Exception as e:
        log.error(f"[API] Unexpected error fetching {product_name}: {e}")
        return []

# ====== API WORKER ======

def api_worker(api_queue: Queue, args, skip_set: set):
    """API worker: processes products via API, updates source to selenium if null"""
    global _api_products_completed, _api_total_products
    thread_id = threading.get_ident()
    log.info(f"[API_WORKER] Thread {thread_id} started")
    
    while True:
        try:
            item = api_queue.get(timeout=QUEUE_GET_TIMEOUT)
            # Format: (product, company, url)
            if len(item) == 3:
                in_product, in_company, product_url = item
            else:
                in_product, in_company = item[0], item[1]
                product_url = construct_product_url(in_product)
        except Empty:
            break
        
        # Defensive skip check with lock (runtime update protection)
        key = (nk(in_company), nk(in_product))
        with _skip_lock:
            if key in skip_set:
                log.info(f"[SKIP-RUNTIME] {in_company} | {in_product}")
                api_queue.task_done()
                continue
        
        try:
            
            log.info(f"[API_WORKER] Processing: {in_company} | {in_product}")
            
            # Use API to scrape
            rows = scrape_single_product_api_with_url(product_url, in_product, in_company)
            
            # Filter rows to only include those with actual values (at least price_ars or other meaningful data)
            rows_with_values = []
            for row in rows:
                # Check if row has actual values: price_ars, or other meaningful fields
                has_price = row.get("price_ars") is not None
                has_description = row.get("description") and row.get("description").strip()
                has_coverage = row.get("coverage_json") and row.get("coverage_json") != "{}"
                has_import_status = row.get("import_status") and row.get("import_status").strip()
                
                # Only include rows with at least one meaningful value
                if has_price or has_description or has_coverage or has_import_status:
                    rows_with_values.append(row)
            
            if rows_with_values:
                # API succeeded with actual values - save results
                saved = append_rows(rows_with_values, source="api")
                if saved:
                    try:
                        _REPO.mark_api_result(in_company, in_product, total_records=len(rows_with_values), status="completed")
                    except Exception as e:
                        log.warning(f"[DB] Failed to mark API result for {in_company} | {in_product}: {e}")
                    # Update skip_set to prevent reprocessing in same run (only for SUCCESS cases)
                    with _skip_lock:
                        skip_set.add(key)
                    log.info(f"[API_WORKER] [SUCCESS] {in_company} | {in_product} -> {len(rows_with_values)} rows with values")
                else:
                    log.warning(f"[API_WORKER] [DB_FAIL] {in_company} | {in_product} -> insert failed, will retry later")
                    try:
                        _REPO.mark_api_result(in_company, in_product, total_records=0, status="failed", error_message="db_insert_failed")
                    except Exception as e:
                        log.warning(f"[DB] Failed to mark API db-failure for {in_company} | {in_product}: {e}")
            else:
                # API returned no values (null, no price, connection lost, not found) - keep in API for retry
                log.warning(f"[API_WORKER] [NO_VALUES] API returned no values for {in_company} | {in_product}, keeping in API (not recording)")
                try:
                    _REPO.mark_api_result(in_company, in_product, total_records=0, status="failed")
                except Exception as e:
                    log.warning(f"[DB] Failed to mark API failure for {in_company} | {in_product}: {e}")
            
            # Update progress counter
            with _progress_lock:
                _api_products_completed += 1
                completed = _api_products_completed
                total = _api_total_products
                if total > 0 and completed % 10 == 0:  # Update every 10 products
                    percent = round((completed / total) * 100, 1)
                    print(f"[PROGRESS] API scraping: {completed}/{total} ({percent}%)", flush=True)
            
            # Apply API rate limit
            rate_limit_wait()
            
        except Exception as e:
            # Connection lost, timeout, or other errors - keep in API for retry
            log.warning(f"[API_WORKER] [ERROR] {in_company} | {in_product}: {e} - keeping in API (not recording)")
            
            # Update progress counter even on error
            with _progress_lock:
                _api_products_completed += 1
                completed = _api_products_completed
                total = _api_total_products
                if total > 0 and completed % 10 == 0:  # Update every 10 products
                    percent = round((completed / total) * 100, 1)
                    print(f"[PROGRESS] API scraping: {completed}/{total} ({percent}%)", flush=True)
            
            rate_limit_wait()
        finally:
            api_queue.task_done()
    
    log.info(f"[API_WORKER] Thread {thread_id} finished")

# ====== MAIN ======

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--threads", type=int, default=API_THREADS, help="Number of API worker threads")
    ap.add_argument("--max-rows", type=int, default=0, help="Maximum number of rows to process (0 = unlimited)")
    args = ap.parse_args()
    
    ensure_headers()
    skip_set = combine_skip_sets()
    log.info(f"[SKIP_SET] Loaded skip_set size = {len(skip_set)}")
    
    # Show skip set file information
    from config_loader import IGNORE_LIST_FILE
    ignore_file = INPUT_DIR / IGNORE_LIST_FILE
    
    log.info(f"[SKIP_SET] Files used for skip set:")
    log.info(f"[SKIP_SET]   - Products file: {OUT_CSV.name} (exists: {OUT_CSV.exists()})")
    log.info(f"[SKIP_SET]   - Ignore list: {ignore_file.name} (exists: {ignore_file.exists()})")
    
    # Load API targets from DB (total_records=0 and loop_count >= SELENIUM_MAX_RUNS)
    api_targets: List[Tuple[str, str, str]] = []  # (product, company, url)
    seen_pairs = set()
    skipped_count = 0

    try:
        with _DB.cursor(dict_cursor=True) as cur:
            cur.execute(
                """
                SELECT product, company, url
                  FROM ar_product_index
                 WHERE run_id = %s
                   AND COALESCE(total_records,0) = 0
                   AND COALESCE(loop_count,0) >= %s
                """,
                (_RUN_ID, int(SELENIUM_MAX_RUNS)),
            )
            rows = cur.fetchall()
    except Exception as e:
        log.error(f"[INPUT] Failed to load API targets from DB: {e}")
        return

    if not rows:
        log.info("[INPUT] No API targets found in DB")
        return

    for row in rows:
        prod = (row.get("product") or "").strip() if isinstance(row, dict) else (row[0] or "").strip()
        comp = (row.get("company") or "").strip() if isinstance(row, dict) else (row[1] or "").strip()
        url = (row.get("url") or "").strip() if isinstance(row, dict) else (row[2] or "").strip()

        if prod and comp:
            key = (nk(comp), nk(prod))
            if key in seen_pairs:
                skipped_count += 1
                continue
            if key in skip_set:
                skipped_count += 1
                continue
            seen_pairs.add(key)
            if not url:
                url = construct_product_url(prod)
            api_targets.append((prod, comp, url))
    unique_combinations = len(api_targets)
    log.info(f"[FILTER] Found {len(api_targets) + skipped_count} products in API input file")
    log.info(f"[FILTER] - Skipped (in ignore_list/products or duplicates): {skipped_count}")
    log.info(f"[FILTER] - Unique combinations to process: {unique_combinations}")
    
    # Apply max-rows limit if specified
    if args.max_rows > 0 and len(api_targets) > args.max_rows:
        api_targets = api_targets[:args.max_rows]
        log.info(f"Max rows limit applied: {args.max_rows} targets")
        unique_combinations = len(api_targets)
    
    # Show summary and wait 10 seconds before starting
    print(f"\n{'='*80}")
    print(f"API SCRAPER READY TO START")
    print(f"{'='*80}")
    print(f"Total unique combinations to process: {unique_combinations}")
    print(f"Threads: {args.threads}")
    print(f"{'='*80}")
    print(f"Skip set information:")
    print(f"  - Products file: {OUT_CSV.name} (exists: {OUT_CSV.exists()})")
    print(f"  - Ignore list: {ignore_file.name} (exists: {ignore_file.exists()})")
    print(f"{'='*80}")
    print(f"Waiting 10 seconds before starting... (Press Ctrl+C to cancel)")
    print(f"{'='*80}\n")
    
    # Wait 10 seconds with countdown
    try:
        for remaining in range(10, 0, -1):
            print(f"\r[API] Starting in {remaining} seconds...", end="", flush=True)
            time.sleep(1)
        print("\r[API] Starting API workers now...                    ")
        log.info("[API] Auto-start after 10 second wait")
    except KeyboardInterrupt:
        print("\n[API] Cancelled by user, exiting...")
        log.warning("[API] Cancelled by user, exiting...")
        return
    
    # Create API queue
    api_queue = Queue()
    for target in api_targets:
        api_queue.put(target)
    
    total_api_products = len(api_targets)
    # Set total for progress tracking
    global _api_total_products, _api_products_completed
    _api_total_products = total_api_products
    _api_products_completed = 0
    
    log.info(f"[QUEUE] API queue: {total_api_products} products")
    log.info(f"[PARALLEL] Starting API workers: {args.threads} threads")
    print(f"[PROGRESS] API scraping: 0/{total_api_products} (0%)", flush=True)
    
    # Start API workers
    # Note: Using daemon=False ensures threads complete before script exits
    # The pipeline runner waits for this script to exit before starting Selenium
    api_threads = [threading.Thread(target=api_worker, args=(api_queue, args, skip_set), daemon=False) 
                   for _ in range(args.threads)]
    
    # Start all threads
    for t in api_threads:
        t.start()
    
    # Wait for all threads to complete - wait for queue to be empty first
    log.info("[MAIN] Waiting for API queue to be processed...")
    
    # Wait for queue to be empty (with periodic checks)
    max_wait_time = 3600  # Maximum 1 hour total wait
    check_interval = 5  # Check every 5 seconds
    elapsed = 0
    
    while not api_queue.empty() and elapsed < max_wait_time:
        time.sleep(check_interval)
        elapsed += check_interval
        if elapsed % 30 == 0:  # Log every 30 seconds
            remaining = api_queue.qsize()
            log.info(f"[MAIN] Queue status: {remaining} products remaining in queue")
    
    if not api_queue.empty():
        log.warning(f"[MAIN] Queue still has {api_queue.qsize()} items after {elapsed}s, waiting for threads to finish...")
    
    # Wait for queue to be fully processed (all task_done calls)
    api_queue.join()  # Blocks until all items in queue have been processed
    
    # Now wait for all threads to complete
    log.info("[MAIN] Waiting for all API threads to finish...")
    for i, t in enumerate(api_threads):
        t.join(timeout=30)  # Wait up to 30 seconds per thread
        if t.is_alive():
            log.warning(f"[MAIN] API thread {i+1} ({t.ident}) still alive after timeout")
        else:
            log.info(f"[MAIN] API thread {i+1} ({t.ident}) completed")
    
    # Final progress update
    if _api_total_products > 0:
        print(f"[PROGRESS] API scraping: {_api_products_completed}/{_api_total_products} (100%)", flush=True)
    
    log.info("[MAIN] API scraping complete.")
    log.info("=" * 80)
    log.info("[MAIN] API thread is completed. Moving to Selenium step.")
    log.info("=" * 80)

    # Count summary
    try:
        with _DB.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ar_product_index WHERE run_id = %s", (_RUN_ID,))
            total = cur.fetchone()[0]
            cur.execute(
                "SELECT COUNT(*) FROM ar_product_index WHERE run_id = %s AND COALESCE(total_records,0) > 0",
                (_RUN_ID,),
            )
            scraped = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM ar_products WHERE run_id = %s", (_RUN_ID,))
            products = cur.fetchone()[0]
        log.info("[COUNT] product_index=%s scraped_with_records=%s products_rows=%s", total, scraped, products)
        print(f"[COUNT] product_index={total} scraped_with_records={scraped} products_rows={products}", flush=True)
    except Exception as e:
        log.warning(f"[COUNT] Failed to load DB counts: {e}")

if __name__ == "__main__":
    main()


