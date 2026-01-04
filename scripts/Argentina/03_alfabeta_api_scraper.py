#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Argentina - API Scraper
Processes products marked as "api" in prepared URLs file.
If API returns null, updates source to "selenium" in CSV.
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
    PRODUCTLIST_FILE, PREPARED_URLS_FILE, IGNORE_LIST_FILE,
    OUTPUT_PRODUCTS_CSV, OUTPUT_PROGRESS_CSV, OUTPUT_ERRORS_CSV
)

from scraper_utils import (
    ensure_headers, combine_skip_sets,
    append_rows, append_progress, append_error, update_prepared_urls_source,
    nk, ts, strip_accents, OUT_FIELDS,
    CSV_LOCK, PROGRESS_LOCK, ERROR_LOCK
)

# ====== LOGGING ======
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("api_scraper")

# ====== PATHS ======
INPUT_DIR = get_input_dir()
OUTPUT_DIR = get_output_dir()
PREPARED_URLS_FILE_PATH = OUTPUT_DIR / PREPARED_URLS_FILE
OUT_CSV = OUTPUT_DIR / OUTPUT_PRODUCTS_CSV
PROGRESS = OUTPUT_DIR / OUTPUT_PROGRESS_CSV
ERRORS = OUTPUT_DIR / OUTPUT_ERRORS_CSV

# Request pause jitter tuple
REQUEST_PAUSE_JITTER = (REQUEST_PAUSE_JITTER_MIN, REQUEST_PAUSE_JITTER_MAX)

# ====== RATE LIMITING ======
RATE_LIMIT_LOCK = threading.Lock()
_rate_limit_batch_start = None
_rate_limit_count = 0

def rate_limit_wait():
    """Wait if needed to respect rate limit: 1 product every N seconds"""
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
    # AR: dot thousands, comma decimals
    t = t.replace(".", "").replace(",", ".")
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
    if BEAUTIFULSOUP_AVAILABLE:
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            return parse_html_with_bs4(soup, in_company, in_product)
        except Exception as e:
            log.warning(f"[API] BeautifulSoup parsing failed: {e}")
            return []
    
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
        
        try:
            if (nk(in_company), nk(in_product)) in skip_set:
                log.debug(f"[API_WORKER] [SKIPPED] {in_company} | {in_product} (already processed)")
                continue
            
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
                append_rows(rows_with_values)
                append_progress(in_company, in_product, len(rows_with_values))
                log.info(f"[API_WORKER] [SUCCESS] {in_company} | {in_product} → {len(rows_with_values)} rows with values")
            else:
                # API returned no values (null, no price, connection lost, not found) - move to selenium without recording
                log.warning(f"[API_WORKER] [NO_VALUES] API returned no values for {in_company} | {in_product}, moving to selenium (not recording)")
                update_prepared_urls_source(in_company, in_product, "selenium")
            
            # Apply API rate limit
            rate_limit_wait()
            
        except Exception as e:
            # Connection lost, timeout, or other errors - move to selenium without recording
            log.warning(f"[API_WORKER] [ERROR] {in_company} | {in_product}: {e} - moving to selenium (not recording)")
            update_prepared_urls_source(in_company, in_product, "selenium")
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
    
    # Load prepared URLs file
    if not PREPARED_URLS_FILE_PATH.exists():
        log.error(f"Prepared URLs file not found: {PREPARED_URLS_FILE_PATH}")
        log.error("Please run script 02 (prepare_urls.py) first to generate Productlist_with_urls.csv")
        return
    
    log.info(f"[INPUT] Reading prepared URLs from: {PREPARED_URLS_FILE_PATH}")
    
    # Load products marked as "api"
    api_targets: List[Tuple[str, str, str]] = []  # (product, company, url)
    
    # Try multiple encodings to handle different file encodings
    encoding_attempts = ["utf-8-sig", "utf-8", "latin-1", "windows-1252", "cp1252"]
    f = None
    for encoding in encoding_attempts:
        try:
            f = open(PREPARED_URLS_FILE_PATH, encoding=encoding)
            r = csv.DictReader(f)
            headers = {nk(h): h for h in (r.fieldnames or [])}
            break  # Success, exit encoding loop
        except UnicodeDecodeError:
            if f:
                f.close()
            continue  # Try next encoding
        except Exception as e:
            if f:
                f.close()
            log.error(f"[INPUT] Failed to read prepared URLs file: {e}")
            return
    
    if f is None:
        log.error(f"[INPUT] Failed to read prepared URLs file with any encoding")
        return
    
    try:
        pcol = headers.get(nk("Product")) or headers.get("product") or "Product"
        ccol = headers.get(nk("Company")) or headers.get("company") or "Company"
        source_col = headers.get(nk("Source")) or headers.get("source") or "Source"
        url_col = headers.get(nk("URL")) or headers.get("url") or "URL"
        
        for row in r:
            prod = (row.get(pcol) or "").strip()
            comp = (row.get(ccol) or "").strip()
            source = (row.get(source_col) or "").strip().lower()
            url = (row.get(url_col) or "").strip()
            
            # Only process products marked as "api"
            if source == "api" and prod and comp:
                # Use the prepared URL, or construct if missing
                if not url:
                    url = construct_product_url(prod)
                api_targets.append((prod, comp, url))
    finally:
        if f:
            f.close()
    
    log.info(f"[FILTER] Found {len(api_targets)} products marked as 'api'")
    
    # Apply max-rows limit if specified
    if args.max_rows > 0 and len(api_targets) > args.max_rows:
        api_targets = api_targets[:args.max_rows]
        log.info(f"Max rows limit applied: {args.max_rows} targets")
    
    # Create API queue
    api_queue = Queue()
    for target in api_targets:
        api_queue.put(target)
    
    log.info(f"[QUEUE] API queue: {len(api_targets)} products")
    log.info(f"[PARALLEL] Starting API workers: {args.threads} threads")
    
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
    
    log.info("[MAIN] API scraping complete.")
    log.info("=" * 80)
    log.info("[MAIN] API thread is completed. Moving to Selenium step.")
    log.info("=" * 80)

if __name__ == "__main__":
    main()

