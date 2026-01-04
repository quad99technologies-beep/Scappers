#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Argentina - Selenium Scraper
Processes products marked as "selenium" in prepared URLs file.
Rotates accounts every 50 searches or when captcha is detected.
"""

import csv
import re
import json
import time
import random
import logging
import argparse
import tempfile
import threading
import signal
import sys
import atexit
from pathlib import Path
from datetime import datetime
from queue import Queue, Empty
from typing import Optional, Tuple, List, Dict, Any

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
    SELENIUM_ROTATION_LIMIT,
    DUPLICATE_RATE_LIMIT_SECONDS,
    REQUEST_PAUSE_BASE, REQUEST_PAUSE_JITTER_MIN, REQUEST_PAUSE_JITTER_MAX,
    WAIT_ALERT, WAIT_SEARCH_FORM, WAIT_SEARCH_RESULTS, WAIT_PAGE_LOAD,
    PAGE_LOAD_TIMEOUT, MAX_RETRIES_TIMEOUT, CPU_THROTTLE_HIGH, PAUSE_CPU_THROTTLE,
    QUEUE_GET_TIMEOUT,
    PRODUCTLIST_FILE, PREPARED_URLS_FILE,
    OUTPUT_PRODUCTS_CSV, OUTPUT_PROGRESS_CSV, OUTPUT_ERRORS_CSV
)

from scraper_utils import (
    ensure_headers, combine_skip_sets,
    append_rows, append_progress, append_error,
    nk, ts, strip_accents, OUT_FIELDS
)

# ====== LOGGING ======
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("selenium_scraper")

# ====== SHUTDOWN HANDLING ======
_shutdown_requested = threading.Event()
_active_drivers = []
_drivers_lock = threading.Lock()

def signal_handler(signum, frame):
    """Handle shutdown signals (Ctrl+C, SIGTERM, etc.)"""
    log.warning(f"[SHUTDOWN] Shutdown signal received ({signum}), closing all Chrome sessions...")
    _shutdown_requested.set()
    close_all_drivers()
    sys.exit(0)

def register_driver(driver):
    """Register a driver for cleanup on shutdown"""
    with _drivers_lock:
        _active_drivers.append(driver)

def unregister_driver(driver):
    """Unregister a driver"""
    with _drivers_lock:
        if driver in _active_drivers:
            _active_drivers.remove(driver)

def close_all_drivers():
    """Close all registered Chrome drivers and kill any remaining Chrome processes"""
    with _drivers_lock:
        log.info(f"[SHUTDOWN] Closing {len(_active_drivers)} Chrome session(s)...")
        for driver in _active_drivers[:]:  # Copy list to avoid modification during iteration
            try:
                driver.quit()
            except Exception as e:
                log.warning(f"[SHUTDOWN] Error closing driver: {e}")
            try:
                # Force close if quit() didn't work
                driver.close()
            except Exception:
                pass
        _active_drivers.clear()
        
        # Force kill any remaining Chrome processes
        kill_chrome_processes()
        
        log.info("[SHUTDOWN] All Chrome sessions closed")

def kill_chrome_processes():
    """Kill any remaining Chrome/ChromeDriver processes"""
    killed_count = 0
    
    # Method 1: Use psutil if available
    try:
        if psutil:
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    proc_name = (proc.info.get('name') or '').lower()
                    cmdline = ' '.join(proc.info.get('cmdline') or [])
                    
                    # Kill ChromeDriver processes
                    if 'chromedriver' in proc_name:
                        try:
                            proc.kill()
                            killed_count += 1
                            log.info(f"[SHUTDOWN] Killed ChromeDriver process: PID {proc.info['pid']}")
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
                    # Kill Chrome browser processes (but not all Chrome processes, just ones with specific flags)
                    elif 'chrome' in proc_name and ('--remote-debugging-port' in cmdline or '--test-type' in cmdline or '--user-data-dir' in cmdline):
                        try:
                            proc.kill()
                            killed_count += 1
                            log.info(f"[SHUTDOWN] Killed Chrome process: PID {proc.info['pid']}")
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass
    except Exception as e:
        log.warning(f"[SHUTDOWN] Error killing Chrome processes with psutil: {e}")
    
    # Method 2: Use Windows taskkill as fallback (more aggressive)
    try:
        import subprocess
        import platform
        if platform.system() == 'Windows':
            # Kill chromedriver.exe
            try:
                subprocess.run(['taskkill', '/F', '/IM', 'chromedriver.exe'], 
                             capture_output=True, timeout=5)
                log.info("[SHUTDOWN] Attempted to kill chromedriver.exe via taskkill")
            except Exception:
                pass
            
            # Kill chrome.exe processes that might be orphaned
            # Note: This is more aggressive and might kill other Chrome instances
            # We'll only do this if we detected Chrome processes via psutil
            if killed_count > 0:
                try:
                    # Kill Chrome processes with specific flags (headless, remote debugging)
                    subprocess.run(['taskkill', '/F', '/FI', 'WINDOWTITLE eq chrome*'], 
                                 capture_output=True, timeout=5)
                except Exception:
                    pass
    except Exception as e:
        log.warning(f"[SHUTDOWN] Error killing Chrome processes with taskkill: {e}")
    
    if killed_count > 0:
        log.info(f"[SHUTDOWN] Killed {killed_count} Chrome/ChromeDriver process(es)")

# Register signal handlers
try:
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
except (AttributeError, ValueError):
    # Windows may not support all signals
    pass

# Register atexit handler to ensure cleanup on any exit
atexit.register(close_all_drivers)

# ====== PATHS ======
INPUT_DIR = get_input_dir()
OUTPUT_DIR = get_output_dir()
PREPARED_URLS_FILE_PATH = OUTPUT_DIR / PREPARED_URLS_FILE
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

# Load accounts at startup
ACCOUNTS = get_accounts()
if not ACCOUNTS:
    raise RuntimeError("No accounts found! Please configure ALFABETA_USER and ALFABETA_PASS in environment")

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

def human_pause():
    time.sleep(REQUEST_PAUSE_BASE + random.uniform(*REQUEST_PAUSE_JITTER))

def get_random_proxy() -> Optional[str]:
    """Get a random proxy from the list"""
    proxies = get_proxy_list()
    return random.choice(proxies) if proxies else None

def save_debug(driver, folder: Path, tag: str):
    try:
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        png = folder / f"{tag}_{stamp}.png"
        html = folder / f"{tag}_{stamp}.html"
        driver.save_screenshot(str(png))
        html.write_text(driver.page_source, encoding="utf-8")
    except Exception as e:
        log.warning(f"Could not save debug for {tag}: {e}")

# ====== DRIVER / LOGIN ======

def setup_driver(headless=False):
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
    
    # Check if shutdown was requested before creating new driver
    if _shutdown_requested.is_set():
        raise RuntimeError("Shutdown requested, cannot create new Chrome session")
    
    drv = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    drv.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    
    # Register driver for cleanup on shutdown
    register_driver(drv)
    
    # Register Chrome instance for cleanup tracking
    try:
        from core.chrome_manager import register_chrome_driver
        register_chrome_driver(drv)
    except ImportError:
        pass  # Chrome manager not available, continue without registration
    
    return drv

def is_login_page(driver) -> bool:
    """Check if current page is a login page"""
    try:
        return bool(driver.find_elements(By.ID, "usuario")) and bool(driver.find_elements(By.ID, "clave"))
    except Exception:
        return False

def wait_for_user_resume():
    """Wait for user to press Enter key after changing VPN location"""
    log.warning("[CAPTCHA_PAUSE] Session closed.")
    log.info("[CAPTCHA_PAUSE] Please change your VPN location and press ENTER to resume...")
    try:
        input()  # Wait for Enter key press
        log.info("[CAPTCHA_PAUSE] Resuming with new session...")
    except (EOFError, KeyboardInterrupt):
        log.warning("[CAPTCHA_PAUSE] Input interrupted, exiting...")
        _shutdown_requested.set()
        raise

# ====== SEARCH / RESULTS ======

def search_in_products(driver, product_term: str):
    """Navigate to products page and search for product term"""
    log.info(f"[SEARCH] Searching for product: {product_term}")
    driver.get(PRODUCTS_URL)
    
    # Check for login page after navigation
    if is_login_page(driver):
        raise RuntimeError("Login page detected after navigating to products URL")
    
    try:
        form = WebDriverWait(driver, WAIT_SEARCH_FORM).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "form#srvPr"))
        )
    except TimeoutException:
        # Check again for login page in case it appeared during wait
        if is_login_page(driver):
            raise RuntimeError("Login page detected while waiting for search form")
        log.error(f"[SEARCH] Form not found after {WAIT_SEARCH_FORM}s. Current URL: {driver.current_url}")
        log.error(f"[SEARCH] Page source snippet: {driver.page_source[:500]}")
        raise
    box = form.find_element(By.NAME, "patron")
    box.clear()
    box.send_keys(product_term)
    box.send_keys(Keys.ENTER)
    log.debug(f"[SEARCH] Search submitted, waiting for results...")
    try:
        WebDriverWait(driver, WAIT_SEARCH_RESULTS).until(
            lambda d: d.find_elements(By.CSS_SELECTOR, "a.rprod, form[name^='pr']")
        )
    except TimeoutException:
        log.error(f"[SEARCH] Search results not found after {WAIT_SEARCH_RESULTS}s. Current URL: {driver.current_url}")
        raise
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

def open_exact_pair(driver, product: str, company: str) -> bool:
    """Open exact product-company pair from search results"""
    rows = enumerate_pairs(driver)
    matches = [r for r in rows if nk(r["prod"]) == nk(product) and nk(r["comp"]) == nk(company)]
    if not matches:
        return False
    pr = matches[0]["pr_form"]
    if not pr:
        return False
    driver.execute_script(f"if (document.{pr}) document.{pr}.submit();")
    WebDriverWait(driver, WAIT_PAGE_LOAD).until(
        lambda d: "presentacion" in d.page_source.lower() or d.find_elements(By.CSS_SELECTOR, "tr.lproducto span.tproducto")
    )
    return True

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

def extract_rows(driver, in_company, in_product):
    # Header/meta from the product page
    active = get_text_safe(driver, "tr.sproducto td.textoe i")
    therap = get_text_safe(driver, "tr.sproducto td.textor i")
    comp = get_text_safe(driver, "tr.lproducto td.textor .defecto") or \
           get_text_safe(driver, "td.textoe b")
    pname = get_text_safe(driver, "tr.lproducto span.tproducto")

    rows: List[Dict[str, Any]] = []
    pres = driver.find_elements(By.CSS_SELECTOR, "td.dproducto > table.presentacion")
    for p in pres:
        desc = get_text_safe(p, "td.tddesc")
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
    return rows

# ====== CAPTCHA DETECTION ======

def is_captcha_page(driver) -> bool:
    """Check if current page is a captcha page."""
    try:
        page_source_lower = driver.page_source.lower()
        url_lower = driver.current_url.lower()
        
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

# ====== RATE LIMITING ======

_duplicate_rate_limit_per_thread = {}  # thread_id -> last_process_time

def duplicate_rate_limit_wait(thread_id: int):
    """Wait if needed to respect rate limit for duplicates: 1 product per 30 seconds per thread (Selenium)"""
    global _duplicate_rate_limit_per_thread
    now = time.time()
    last_time = _duplicate_rate_limit_per_thread.get(thread_id, 0)
    time_since_last = now - last_time
    
    if time_since_last < DUPLICATE_RATE_LIMIT_SECONDS:
        wait_time = DUPLICATE_RATE_LIMIT_SECONDS - time_since_last
        log.info(f"[DUPLICATE_RATE_LIMIT] Thread {thread_id}: waiting {wait_time:.2f}s (1 product per {DUPLICATE_RATE_LIMIT_SECONDS}s)")
        time.sleep(wait_time)
    
    _duplicate_rate_limit_per_thread[thread_id] = time.time()

# ====== MAIN ======

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-rows", type=int, default=0, help="Maximum number of rows to process (0 = unlimited)")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--headless", dest="headless", action="store_true", default=False)
    g.add_argument("--no-headless", dest="headless", action="store_false")
    args = ap.parse_args()

    ensure_headers()
    skip_set = combine_skip_sets()
    
    # Load prepared URLs file
    if not PREPARED_URLS_FILE_PATH.exists():
        log.error(f"Prepared URLs file not found: {PREPARED_URLS_FILE_PATH}")
        log.error("Please run script 02 (prepare_urls.py) first to generate Productlist_with_urls.csv")
        return
    
    log.info(f"[INPUT] Reading prepared URLs from: {PREPARED_URLS_FILE_PATH}")
    
    # Load products marked as "selenium"
    selenium_targets: List[Tuple[str, str, bool]] = []  # (product, company, is_duplicate)
    
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
        dup_col = headers.get(nk("IsDuplicate")) or headers.get("isduplicate") or "IsDuplicate"
        
        for row in r:
            prod = (row.get(pcol) or "").strip()
            comp = (row.get(ccol) or "").strip()
            source = (row.get(source_col) or "").strip().lower()
            is_dup_str = (row.get(dup_col) or "").strip().lower()
            is_duplicate = is_dup_str == "true"
            
            # Only process products marked as "selenium"
            if source == "selenium" and prod and comp:
                selenium_targets.append((prod, comp, is_duplicate))
    finally:
        if f:
            f.close()
    
    # Sort: duplicates (is_duplicate=True) first, then non-duplicates
    selenium_targets.sort(key=lambda x: (not x[2], x[0], x[1]))  # False (non-duplicates) comes after True (duplicates)
    
    duplicate_count = sum(1 for _, _, is_dup in selenium_targets if is_dup)
    non_duplicate_count = len(selenium_targets) - duplicate_count
    
    log.info(f"[FILTER] Found {len(selenium_targets)} products marked as 'selenium'")
    log.info(f"[FILTER] - Duplicates (priority): {duplicate_count}")
    log.info(f"[FILTER] - Non-duplicates: {non_duplicate_count}")
    
    # Apply max-rows limit if specified
    if args.max_rows > 0 and len(selenium_targets) > args.max_rows:
        selenium_targets = selenium_targets[:args.max_rows]
        log.info(f"Max rows limit applied: {args.max_rows} targets")
    
    # Use 3 threads (no proxy, no authentication)
    num_threads = 3
    log.info(f"[SELENIUM] Using {num_threads} threads (no proxy, no authentication)")
    
    # Create queue and add all products
    selenium_queue = Queue()
    for target in selenium_targets:
        selenium_queue.put(target)
    
    log.info(f"[QUEUE] Added {selenium_queue.qsize()} products to queue")
    
    # Create and start worker threads
    threads = []
    for thread_idx in range(num_threads):
        thread = threading.Thread(
            target=selenium_worker,
            args=(selenium_queue, args, skip_set),
            name=f"SeleniumWorker-{thread_idx + 1}",
            daemon=False
        )
        threads.append(thread)
        thread.start()
        log.info(f"[SELENIUM] Started thread {thread_idx + 1}/{num_threads} (no proxy, no authentication)")
    
    # Wait for all threads to complete
    log.info("[SELENIUM] Waiting for all worker threads to complete...")
    try:
        for i, thread in enumerate(threads):
            thread.join()
            log.info(f"[SELENIUM] Thread {i + 1}/{num_threads} completed")
    except KeyboardInterrupt:
        log.warning("[SELENIUM] Interrupted, shutting down...")
        _shutdown_requested.set()
        close_all_drivers()
        raise
    finally:
        # Ensure all drivers are closed
        close_all_drivers()
    
    log.info("[SELENIUM] All threads completed")

# ====== SELENIUM WORKER ======

def selenium_worker(selenium_queue: Queue, args, skip_set: set):
    """Selenium worker: processes products from queue (no proxy, no authentication)"""
    thread_id = threading.get_ident()
    log.info(f"[SELENIUM_WORKER] Thread {thread_id} started (no proxy, no authentication)")
    
    driver = None
    try:
        # Check if shutdown was requested before initializing
        if _shutdown_requested.is_set():
            log.warning(f"[SELENIUM_WORKER] Shutdown requested, thread {thread_id} exiting before initialization")
            return
        
        # Initialize driver (no proxy, no authentication)
        driver = setup_driver(headless=args.headless)
        
        while True:
            # Check for shutdown before processing next item
            if _shutdown_requested.is_set():
                log.warning(f"[SELENIUM_WORKER] Shutdown requested, thread {thread_id} exiting")
                break
            
            try:
                item = selenium_queue.get(timeout=QUEUE_GET_TIMEOUT)
                # Format: (product, company, is_duplicate)
                in_product, in_company, is_duplicate = item
            except Empty:
                break
            
            search_attempted = False
            try:
                # Check skip set
                if (nk(in_company), nk(in_product)) in skip_set:
                    log.debug(f"[SELENIUM_WORKER] [SKIPPED] {in_company} | {in_product} (already processed)")
                    selenium_queue.task_done()
                    continue
                
                product_type = "DUPLICATE" if is_duplicate else "NON-DUPLICATE"
                log.info(f"[SELENIUM_WORKER] [SEARCH_START] [{product_type}] {in_company} | {in_product}")
                search_attempted = True
                
                # Apply rate limit
                duplicate_rate_limit_wait(thread_id)
                
                # Check for captcha before processing
                if driver and is_captcha_page(driver):
                    log.warning(f"[SELENIUM_WORKER] [CAPTCHA_DETECTED] Captcha detected for {in_company} | {in_product}")
                    unregister_driver(driver)
                    driver.quit()
                    driver = None
                    wait_for_user_resume()
                    if _shutdown_requested.is_set():
                        break
                    # Create new driver after user resumes
                    driver = setup_driver(headless=args.headless)
                
                # Retry logic for TimeoutException
                max_retries = MAX_RETRIES_TIMEOUT
                retry_count = 0
                success = False
                
                while retry_count <= max_retries and not success:
                    try:
                        if retry_count > 0:
                            log.info(f"[SELENIUM_WORKER] [RETRY {retry_count}/{max_retries}] {in_company} | {in_product}")
                            time.sleep(10)  # PAUSE_RETRY
                        
                        try:
                            search_in_products(driver, in_product)
                        except RuntimeError as e:
                            if "Login page detected" in str(e):
                                log.warning(f"[SELENIUM_WORKER] [LOGIN_REQUIRED] Login page detected during search for {in_company} | {in_product}")
                                unregister_driver(driver)
                                driver.quit()
                                driver = None
                                wait_for_user_resume()
                                if _shutdown_requested.is_set():
                                    break
                                driver = setup_driver(headless=args.headless)
                                continue  # Retry the search
                            else:
                                raise
                        
                        # Check for login page or captcha after search
                        if is_login_page(driver):
                            log.warning(f"[SELENIUM_WORKER] [LOGIN_REQUIRED] Login page detected after search for {in_company} | {in_product}")
                            unregister_driver(driver)
                            driver.quit()
                            driver = None
                            wait_for_user_resume()
                            if _shutdown_requested.is_set():
                                break
                            driver = setup_driver(headless=args.headless)
                            search_in_products(driver, in_product)
                        elif is_captcha_page(driver):
                            log.warning(f"[SELENIUM_WORKER] [CAPTCHA_DETECTED] Captcha detected after search for {in_company} | {in_product}")
                            unregister_driver(driver)
                            driver.quit()
                            driver = None
                            wait_for_user_resume()
                            if _shutdown_requested.is_set():
                                break
                            driver = setup_driver(headless=args.headless)
                            search_in_products(driver, in_product)
                        
                        if not open_exact_pair(driver, in_product, in_company):
                            save_debug(driver, DEBUG_NF, f"{in_company}_{in_product}")
                            append_progress(in_company, in_product, 0)
                            log.info(f"[SELENIUM_WORKER] [NOT_FOUND] {in_company} | {in_product}")
                            success = True
                            break
                        
                        # Check for login page or captcha after opening product page
                        if is_login_page(driver):
                            log.warning(f"[SELENIUM_WORKER] [LOGIN_REQUIRED] Login page detected on product page for {in_company} | {in_product}")
                            unregister_driver(driver)
                            driver.quit()
                            driver = None
                            wait_for_user_resume()
                            if _shutdown_requested.is_set():
                                break
                            driver = setup_driver(headless=args.headless)
                            search_in_products(driver, in_product)
                            if not open_exact_pair(driver, in_product, in_company):
                                save_debug(driver, DEBUG_NF, f"{in_company}_{in_product}")
                                append_progress(in_company, in_product, 0)
                                log.info(f"[SELENIUM_WORKER] [NOT_FOUND] {in_company} | {in_product}")
                                success = True
                                break
                        elif is_captcha_page(driver):
                            log.warning(f"[SELENIUM_WORKER] [CAPTCHA_DETECTED] Captcha detected on product page for {in_company} | {in_product}")
                            unregister_driver(driver)
                            driver.quit()
                            driver = None
                            wait_for_user_resume()
                            if _shutdown_requested.is_set():
                                break
                            driver = setup_driver(headless=args.headless)
                            search_in_products(driver, in_product)
                            if not open_exact_pair(driver, in_product, in_company):
                                save_debug(driver, DEBUG_NF, f"{in_company}_{in_product}")
                                append_progress(in_company, in_product, 0)
                                log.info(f"[SELENIUM_WORKER] [NOT_FOUND] {in_company} | {in_product}")
                                success = True
                                break
                        
                        rows = extract_rows(driver, in_company, in_product)
                        if rows:
                            append_rows(rows)
                            append_progress(in_company, in_product, len(rows))
                            log.info(f"[SELENIUM_WORKER] [SUCCESS] {in_company} | {in_product} → {len(rows)} rows")
                        else:
                            save_debug(driver, DEBUG_NF, f"{in_company}_{in_product}")
                            append_progress(in_company, in_product, 0)
                            log.info(f"[SELENIUM_WORKER] [NOT_FOUND] (0 rows) {in_company} | {in_product}")
                        success = True
                        
                    except TimeoutException as te:
                        retry_count += 1
                        if retry_count > max_retries:
                            log.error(f"[SELENIUM_WORKER] [TIMEOUT] {in_company} | {in_product} - All {max_retries} retries exhausted")
                            raise
                        log.warning(f"[SELENIUM_WORKER] [TIMEOUT] {in_company} | {in_product} - Retry {retry_count}/{max_retries}")
                    except Exception as e:
                        raise
                        
            except Exception as e:
                msg = f"{type(e).__name__}: {e}"
                append_error(in_company, in_product, msg)
                save_debug(driver, DEBUG_ERR, f"{in_company}_{in_product}")
                log.error(f"[SELENIUM_WORKER] [ERROR] {in_company} | {in_product}: {msg}")
            finally:
                selenium_queue.task_done()
                if search_attempted:
                    human_pause()
    
    finally:
        # Clean up driver
        if driver:
            try:
                unregister_driver(driver)
                try:
                    driver.quit()
                except Exception:
                    try:
                        driver.close()
                    except Exception:
                        pass
            except Exception as e:
                log.warning(f"[SELENIUM_WORKER] Error closing driver: {e}")
    
    log.info(f"[SELENIUM_WORKER] Thread {thread_id} finished")

if __name__ == "__main__":
    main()

