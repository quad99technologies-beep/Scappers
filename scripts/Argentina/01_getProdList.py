#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AlfaBeta – PRODUCTS index dump (srvPr)
Resilient end-to-end scraper that:
 - logs in
 - submits a blank 'patron' on form#srvPr to list all products
 - paginates through results, extracting (Product, Company)
 - writes to ar_product_index (DB-only)
 - captures screenshots and page source on critical failure
"""

import sys
import os
import time
import logging
import socket
import subprocess
from pathlib import Path
from typing import List, Tuple, Set, Optional

# Add project root to sys.path to allow 'core' imports
project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path: 
    sys.path.insert(0, str(project_root))

# Add script dir to sys.path to allow local imports
script_dir = Path(__file__).resolve().parent
if str(script_dir) not in sys.path:
    sys.path.insert(0, str(script_dir))

from core.monitoring.audit_logger import audit_log
import threading

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
    WebDriverException,
    UnexpectedAlertPresentException,
)
from webdriver_manager.firefox import GeckoDriverManager

# ====== CONFIG ======
from config_loader import (
    get_input_dir, get_output_dir,
    ALFABETA_USER as USERNAME, ALFABETA_PASS as PASSWORD,
    HEADLESS, PRODUCTS_URL, HUB_URL,
    WAIT_SHORT, WAIT_LONG, WAIT_ALERT, PAUSE_BETWEEN_OPERATIONS,
    PAGE_LOAD_TIMEOUT, IMPLICIT_WAIT, MAX_RETRIES_SUBMIT,
    PAUSE_SHORT, PAUSE_MEDIUM, PAUSE_AFTER_ALERT,
    REQUIRE_TOR_PROXY, AUTO_START_TOR_PROXY
)

# Try to import requests for VPN check
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    requests = None
    REQUESTS_AVAILABLE = False

# Use config values (aliased for backward compatibility)
PAUSE = PAUSE_BETWEEN_OPERATIONS

from core.db.connection import CountryDB

# Ensure Argentina directory is at the front of sys.path to prioritize local 'db' package
# This fixes conflict with core/db which might be in sys.path
sys.path = [p for p in sys.path if not Path(p).name == 'core']
_script_dir = Path(__file__).resolve().parent
if str(_script_dir) in sys.path:
    sys.path.remove(str(_script_dir))
sys.path.insert(0, str(_script_dir))

# Force re-import of db module if it was incorrectly loaded from core/db
if 'db' in sys.modules:
    del sys.modules['db']

try:
    from db.repositories import ArgentinaRepository
except ImportError:
    from scripts.Argentina.db.repositories import ArgentinaRepository

try:
    from db.schema import apply_argentina_schema
except ImportError:
    from scripts.Argentina.db.schema import apply_argentina_schema
from core.db.models import generate_run_id
import os

OUTPUT_DIR = get_output_dir()
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Failure artifacts dir - store in output folder
ARTIFACTS_DIR = OUTPUT_DIR / "artifacts"
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

# DB setup
_RUN_ID_FILE = OUTPUT_DIR / ".current_run_id"

def _get_run_id() -> str:
    run_id = os.environ.get("ARGENTINA_RUN_ID")
    if run_id:
        return run_id
    if _RUN_ID_FILE.exists():
        try:
            txt = _RUN_ID_FILE.read_text(encoding="utf-8").strip()
            if txt:
                return txt
        except Exception:
            pass
    run_id = generate_run_id()
    os.environ["ARGENTINA_RUN_ID"] = run_id
    _RUN_ID_FILE.write_text(run_id, encoding="utf-8")
    return run_id

_DB = CountryDB("Argentina")
apply_argentina_schema(_DB)
_RUN_ID = _get_run_id()
_REPO = ArgentinaRepository(_DB, _RUN_ID)

from core.browser.driver_factory import create_firefox_driver
from core.network.tor_manager import ensure_tor_proxy_running, is_port_open

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("alfabeta-products-dump")

# ====== TOR CONFIGURATION ======
# Global variable to store detected Tor port (9050 for Tor service, 9150 for Tor Browser)
TOR_PROXY_PORT = 9050  # Default to Tor service port

def check_tor_requirements():
    """
    Check Tor requirements using core utilities where possible.
    Returns True if all requirements are met, False otherwise.
    """
    print("\n" + "=" * 80)
    print("[TOR_CHECK] Verifying Tor connection...")
    print("=" * 80)
    log.info("[TOR_CHECK] Verifying Tor connection...")
    
    # Check ports 9050 and 9150
    ports_to_check = [9150, 9050]
    found_port = None
    
    for port in ports_to_check:
        if is_port_open("127.0.0.1", port):
            found_port = port
            break
            
    if found_port:
         print(f"  [OK] Tor proxy found on port {found_port}")
         global TOR_PROXY_PORT
         TOR_PROXY_PORT = found_port
         return True
         
    if AUTO_START_TOR_PROXY:
        print("  [INFO] Tor proxy not detected; attempting auto-start...")
        # ensure_tor_proxy_running attempts auto-start on 9050/9051
        ensure_tor_proxy_running(socks_port=9050, control_port=9051)
        if is_port_open("127.0.0.1", 9050):
             print(f"  [OK] Tor proxy auto-started on port 9050")
             TOR_PROXY_PORT = 9050
             return True

    if REQUIRE_TOR_PROXY:
        print("  [FAIL] Tor proxy not found and REQUIRE_TOR_PROXY=true")
        return False
        
    print("  [WARN] Tor not found, continuing without Tor (direct connection)")
    TOR_PROXY_PORT = None
    return True


# ====== DRIVER ======
def setup_driver(headless: bool = HEADLESS):
    tor_config = {}
    if TOR_PROXY_PORT:
        tor_config = {"enabled": True, "port": TOR_PROXY_PORT}
        
    # Extra prefs to enable images/css as required by this scraper
    extra_prefs = {
        "permissions.default.image": 1,
        "permissions.default.stylesheet": 1,
        "intl.accept_languages": "es-ES,es,en-US,en",
        "general.useragent.override": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
    }
    
    return create_firefox_driver(headless=headless, tor_config=tor_config, extra_prefs=extra_prefs)

# ====== PAGE HELPERS ======
def is_login_page(d):
    try:
        return bool(d.find_elements(By.ID, "usuario") and d.find_elements(By.ID, "clave"))
    except Exception:
        return False

def ensure_logged_in(d):
    # If the current page has login inputs, perform login
    if not is_login_page(d):
        return
    log.info("Logging in…")
    user = WebDriverWait(d, WAIT_LONG).until(EC.presence_of_element_located((By.ID, "usuario")))
    pwd  = WebDriverWait(d, WAIT_LONG).until(EC.presence_of_element_located((By.ID, "clave")))
    user.clear(); user.send_keys(USERNAME)
    pwd.clear();  pwd.send_keys(PASSWORD)

    # prefer clickable submit button if present
    try:
        submit_sel = "input[type='button'][value='Enviar'], input[value='Enviar'], button[value='Enviar']"
        WebDriverWait(d, WAIT_SHORT).until(EC.element_to_be_clickable((By.CSS_SELECTOR, submit_sel))).click()
    except Exception:
        # fallback - try JS function, then ENTER
        try:
            d.execute_script("if (typeof enviar === 'function') enviar();")
        except Exception:
            pwd.send_keys(Keys.ENTER)

    # accept alert if present
    try:
        WebDriverWait(d, WAIT_ALERT).until(EC.alert_is_present())
        d.switch_to.alert.accept()
    except TimeoutException:
        pass

def open_hub(d):
    d.get(HUB_URL)
    ensure_logged_in(d)
    # ensure we're on the hub URL
    if d.current_url != HUB_URL:
        d.get(HUB_URL)

def open_products_page(d):
    """Navigate to the products search page (srvPr form)"""
    d.get(PRODUCTS_URL)
    ensure_logged_in(d)
    
    # Handle any alerts that might be present
    try:
        WebDriverWait(d, WAIT_ALERT).until(EC.alert_is_present())
        alert = d.switch_to.alert
        alert.accept()
        log.debug("Dismissed alert on products page")
    except TimeoutException:
        pass  # No alert present, continue
    
    # Wait for the form to be present
    try:
        WebDriverWait(d, WAIT_LONG).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "form#srvPr"))
        )
    except TimeoutException:
        log.warning("Products form not found, trying to reload page")
        d.get(PRODUCTS_URL)
        ensure_logged_in(d)
        # Handle alert again after reload
        try:
            WebDriverWait(d, WAIT_ALERT).until(EC.alert_is_present())
            d.switch_to.alert.accept()
        except TimeoutException:
            pass

def navigate_to_products_page(d, max_nav_retries=1):
    nav_timeout = PAGE_LOAD_TIMEOUT
    if TOR_PROXY_PORT:
        nav_timeout = max(nav_timeout, 120)
    navigation_timeout = nav_timeout + 10
    last_error = None

    for attempt in range(1, max_nav_retries + 1):
        try:
            d.set_page_load_timeout(nav_timeout)
            log.info(f"[NAVIGATE] Attempt {attempt}/{max_nav_retries} to {PRODUCTS_URL} (timeout: {nav_timeout}s)")
            start = time.time()
            navigation_complete = threading.Event()
            navigation_error = [None]

            def do_nav():
                try:
                    d.get(PRODUCTS_URL)
                except Exception as e:
                    navigation_error[0] = e
                finally:
                    navigation_complete.set()

            nav_thread = threading.Thread(target=do_nav, daemon=True)
            nav_thread.start()

            if navigation_complete.wait(timeout=navigation_timeout):
                if navigation_error[0]:
                    raise navigation_error[0]
                elapsed = time.time() - start
                log.info(f"[NAVIGATE] driver.get() completed in {elapsed:.2f}s. Current URL: {d.current_url}")
                d.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
                return

            elapsed = time.time() - start
            log.warning(f"[NAVIGATE] Navigation hung after {elapsed:.2f}s (timeout: {navigation_timeout}s)")
            try:
                d.execute_script("window.stop();")
            except Exception:
                pass
            last_error = TimeoutException(f"Navigation hung after {navigation_timeout}s")
        except TimeoutException as e:
            last_error = e
            log.warning(f"[NAVIGATE] Navigation timed out on attempt {attempt}/{max_nav_retries}: {e}")
        except Exception as e:
            last_error = e
            log.warning(f"[NAVIGATE] Navigation error on attempt {attempt}/{max_nav_retries}: {e}")

        if attempt < max_nav_retries:
            wait_time = 60
            log.info(f"[NAVIGATE] Waiting {wait_time}s before retry...")
            time.sleep(wait_time)
        else:
            break

    safe_screenshot_and_source(d, name_prefix="nav_failure")
    raise last_error or TimeoutException(f"Failed to navigate to {PRODUCTS_URL} after {max_nav_retries} attempts")

# ====== UTIL ======
def dismiss_alert_if_present(d):
    """Dismiss any alert that might be present"""
    try:
        WebDriverWait(d, WAIT_ALERT).until(EC.alert_is_present())
        alert = d.switch_to.alert
        alert_text = alert.text
        alert.accept()
        log.debug(f"Dismissed alert: {alert_text}")
        return True
    except TimeoutException:
        return False
    except Exception as e:
        log.debug(f"No alert or error dismissing: {e}")
        return False

def wait_for_page_ready(d, timeout=WAIT_LONG):
    """Wait until the document reports readyState == complete."""
    try:
        WebDriverWait(d, timeout).until(
            lambda drv: drv.execute_script("return document.readyState") == "complete"
        )
    except TimeoutException:
        log.debug("Timed out waiting for document.readyState == complete")

def scroll_to_bottom(d, pause=0.5, max_rounds=20):
    """Scroll to bottom to trigger lazy loading; stop when height stabilizes."""
    last_height = 0
    stable_rounds = 0
    for _ in range(max_rounds):
        height = d.execute_script("return document.body.scrollHeight")
        if height == last_height:
            stable_rounds += 1
            if stable_rounds >= 2:
                break
        else:
            stable_rounds = 0
            last_height = height
        d.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause)

def wait_for_products_render(d, timeout=20, stable_checks=3):
    """Wait for product links count to stabilize after scrolling."""
    end_time = time.time() + timeout
    last_count = -1
    stable = 0
    while time.time() < end_time:
        try:
            count = len(d.find_elements(By.CSS_SELECTOR, "table.estandar td a.rprod"))
        except Exception:
            count = -1
        if count == last_count and count > 0:
            stable += 1
            if stable >= stable_checks:
                return count
        else:
            stable = 0
            last_count = count
        time.sleep(0.5)
    return last_count

def safe_screenshot_and_source(d, name_prefix="failure"):
    ts = int(time.time())
    screenshot_path = ARTIFACTS_DIR / f"{name_prefix}-{ts}.png"
    html_path = ARTIFACTS_DIR / f"{name_prefix}-{ts}.html"
    
    # Dismiss any alerts before taking screenshot
    dismiss_alert_if_present(d)
    
    try:
        d.save_screenshot(str(screenshot_path))
    except UnexpectedAlertPresentException:
        # Alert appeared during screenshot - dismiss and retry
        dismiss_alert_if_present(d)
        try:
            d.save_screenshot(str(screenshot_path))
        except Exception:
            log.exception("Could not save screenshot after alert dismissal")
    except Exception:
        log.exception("Could not save screenshot")
    try:
        with open(html_path, "w", encoding="utf-8") as fh:
            fh.write(d.page_source)
    except Exception:
        log.exception("Could not dump page source")
    log.info(f"Saved artifacts: {screenshot_path} , {html_path}")
    return screenshot_path, html_path

def clean(s: Optional[str]) -> str:
    return " ".join((s or "").split()).strip()

# ====== FORM SUBMIT ======
def submit_blank_products(d):
    """
    Submit blank 'patron' on form#srvPr. Single attempt with 2-minute wait for results.
    """
    log.info("submit_blank_products: submitting blank search")
    
    form = WebDriverWait(d, WAIT_LONG).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "form#srvPr"))
    )
    
    # Find the input field using the exact selector
    try:
        box = form.find_element(By.CSS_SELECTOR, "input.entrada[name='patron'], input[name='patron']")
    except Exception:
        raise RuntimeError("Input box not found in form")

    # Find the submit button
    try:
        submit_btn = form.find_element(By.CSS_SELECTOR, "input.mfsubmit[value='Buscar'], input.mfsubmit, input[type='submit'][value='Buscar']")
    except Exception:
        submit_btn = None
        
    # Submit the form using JavaScript (avoids page load timeout issues)
    log.debug("Submitting form via JavaScript (bypassing validation)")
    try:
        # Use JavaScript to set empty value and submit - this doesn't wait for page load
        d.execute_script("""
            var form = arguments[0];
            var input = form.querySelector('input[name="patron"]');
            if (input) {
                input.value = '';  // Empty value
                // Bypass validation and submit directly
                if (form.onsubmit) {
                    form.onsubmit = null;  // Remove validation
                }
                form.submit();
            }
        """, form)
        # Rate limiting: pause after form submission to avoid overwhelming server
        time.sleep(PAUSE_AFTER_ALERT)
        log.debug("Form submitted via JavaScript")
    except Exception as e:
        log.warning(f"JavaScript submission failed: {e}, trying fallback with minimal input")
        # Fallback: Try with minimal input (two spaces)
        try:
            d.execute_script("""
                var form = arguments[0];
                var input = form.querySelector('input[name="patron"]');
                if (input) {
                    input.value = '  ';  // Two spaces as fallback
                    // Bypass validation and submit directly
                    if (form.onsubmit) {
                        form.onsubmit = null;  // Remove validation
                    }
                    form.submit();
                }
            """, form)
            # Rate limiting: pause after form submission
            time.sleep(PAUSE_AFTER_ALERT)
            log.debug("Used fallback method with minimal input via JavaScript")
        except Exception as e2:
            log.error(f"Fallback submission also failed: {e2}")
            raise RuntimeError(f"Failed to submit form: {e}, {e2}")

    # Ensure we're still logged in after navigation
    ensure_logged_in(d)
    
    # Wait for results table to appear and be fully loaded
    log.info("Waiting for results table to load...")
    table_found = False
    try:
        # Wait for the results table to be present (up to 120 seconds)
        WebDriverWait(d, 120).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table.estandar"))
        )
        log.info("Results table found")
        table_found = True
        
        # Wait for document to be fully loaded
        wait_for_page_ready(d, timeout=30)
        log.info("Document ready state: complete")
        
    except TimeoutException:
        log.warning("Timeout waiting for results table - checking current page state")
        # Check if we're still on the form page
        try:
            form = d.find_elements(By.CSS_SELECTOR, "form#srvPr")
            if form:
                log.error("Still on form page - form submission may have failed")
                raise RuntimeError("Form submission failed - still on form page")
        except Exception:
            pass
    
    # Check if table was found
    if not table_found:
        try:
            # Try one more time to find the table
            table = d.find_elements(By.CSS_SELECTOR, "table.estandar")
            if table:
                log.info("Results table found on retry")
                table_found = True
        except Exception:
            pass
    
    if not table_found:
        raise RuntimeError("Results table not found after form submission")
    
    # Now wait for products to appear (give it up to 2 minutes since large result sets take time)
    log.info("Waiting for products to appear in table (this may take up to 2 minutes for large result sets)...")
    products_found = False
    
    # Poll for products every 5 seconds for up to 120 seconds
    max_wait_time = 120
    poll_interval = 5
    elapsed = 0
    
    while elapsed < max_wait_time:
        try:
            prods = d.find_elements(By.CSS_SELECTOR, "table.estandar td a.rprod")
            if prods and len(prods) > 0:
                log.info(f"Product links detected: {len(prods)} products found")
                products_found = True
                break
            else:
                # Check if table has any content at all
                table_cells = d.find_elements(By.CSS_SELECTOR, "table.estandar td")
                if table_cells:
                    log.debug(f"Table has {len(table_cells)} cells but no product links yet...")
        except Exception as e:
            log.debug(f"Error checking for products: {e}")
        
        if elapsed % 30 == 0 and elapsed > 0:  # Log every 30 seconds
            log.info(f"Still waiting for products... ({elapsed}s elapsed)")
        
        time.sleep(poll_interval)
        elapsed += poll_interval
    
    if not products_found:
        # Final check - maybe products are there but selector is different
        log.warning("No products found with standard selector - checking page content...")
        try:
            # Check for any links in the table
            all_links = d.find_elements(By.CSS_SELECTOR, "table.estandar a")
            log.info(f"Found {len(all_links)} total links in table")
            
            # Try alternative selectors
            prods_alt1 = d.find_elements(By.CSS_SELECTOR, "table.estandar a.rprod")
            prods_alt2 = d.find_elements(By.CSS_SELECTOR, "table.estandar td a")
            
            if prods_alt1 or prods_alt2:
                log.warning("Products may be present but not detected with primary selector - proceeding anyway")
                log.info("Will attempt extraction on next step")
                return
            else:
                log.error("No products detected even with alternative selectors")
                raise RuntimeError("No products found after waiting 2 minutes - form submission may have failed")
        except RuntimeError:
            raise
        except Exception as e:
            log.error(f"Error in final check: {e}")
            raise RuntimeError(f"Failed to detect products: {e}")
    else:
        # Scroll to trigger any lazy loading and wait for render
        log.info("Scrolling to trigger full render...")
        scroll_to_bottom(d, pause=0.6, max_rounds=25)
        wait_for_products_render(d, timeout=20)
        
        # Final verification
        prods = d.find_elements(By.CSS_SELECTOR, "table.estandar td a.rprod")
        labs = d.find_elements(By.CSS_SELECTOR, "table.estandar td a.rlab")
        log.info(f"submit_blank_products: {len(prods)} product(s) and {len(labs)} company/ies detected - content fully loaded")
        return

# ====== PAGINATION & EXTRACTION ======
def go_next(d) -> bool:
    candidates = [
        "a#siguiente", "a[rel='next']",
        "a[title*='Siguiente']", "a[title*='siguiente']",
        "a.paginacion_siguiente",
        # last two are generic; keep in case of English label
        "a[title*='Next']", "a:contains('Next')"
    ]
    for sel in candidates:
        try:
            els = d.find_elements(By.CSS_SELECTOR, sel)
            if not els:
                continue
            for el in els:
                try:
                    if not (el.is_displayed() and el.is_enabled()):
                        continue
                    try:
                        el.click()
                    except Exception:
                        try:
                            d.execute_script("arguments[0].click();", el)
                        except Exception:
                            log.warning("next click failed on element; continuing to next candidate")
                            continue

                    # wait for the table to refresh (either table present & changed or element becomes stale)
                    WebDriverWait(d, WAIT_LONG).until(
                        lambda drv: drv.find_elements(By.CSS_SELECTOR, "table.estandar")
                    )
                    return True
                except StaleElementReferenceException:
                    # element vanished — page likely navigated; treat as success
                    return True
                except Exception:
                    continue
        except Exception:
            continue
    return False

def extract_products_page(d) -> List[Tuple[str, str]]:
    """
    Extract (Product, Company) pairs from <table class="estandar">:
    sequence appears as  ... <a class="rprod">NAME</a> • <a class="rlab">LAB</a> <br> ...
    Waits for content to be fully loaded before extracting.
    """
    # Wait for table to be present and content to load
    try:
        WebDriverWait(d, WAIT_LONG).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table.estandar"))
        )
        # Wait for document ready state
        wait_for_page_ready(d, timeout=10)
    except TimeoutException:
        log.warning("Timeout waiting for table to load, continuing anyway")

    # Scroll to trigger any lazy loading and wait for content to stabilize
    try:
        scroll_to_bottom(d, pause=0.4, max_rounds=20)
        wait_for_products_render(d, timeout=15)
    except Exception:
        log.debug("Scrolling/waiting for render failed; continuing with extraction")
    
    rows: List[Tuple[str, str, Optional[str]]] = []
    try:
        prods = d.find_elements(By.CSS_SELECTOR, "table.estandar td a.rprod")
        labs  = d.find_elements(By.CSS_SELECTOR, "table.estandar td a.rlab")
        if len(prods) == len(labs) and len(prods) > 0:
            for p, l in zip(prods, labs):
                pname = clean(p.get_attribute("innerText"))
                cname = clean(l.get_attribute("innerText"))
                if pname or cname:
                    href = p.get_attribute("href")
                    # Ensure relative URLs are made absolute if needed, or just store as is if absolute
                    # Usually get_attribute("href") returns absolute URL
                    rows.append((pname, cname, href))
            return rows
    except Exception:
        log.debug("Primary extraction method failed or found mismatched counts; falling back")

    # Fallback: pair within each TD block by nearest anchors
    try:
        prod_nodes = d.find_elements(By.CSS_SELECTOR, "table.estandar td")
        for td in prod_nodes:
            anchors = td.find_elements(By.CSS_SELECTOR, "a.rprod, a.rlab")
            pending_prod: Optional[str] = None
            pending_url: Optional[str] = None
            for a in anchors:
                cls = a.get_attribute("class") or ""
                text = clean(a.get_attribute("innerText"))
                curr_href = a.get_attribute("href")
                if not text:
                    continue
                if "rprod" in cls:
                    if pending_prod:
                        rows.append((pending_prod, "", pending_url))  # flush previous without lab
                    pending_prod = text
                    pending_url = curr_href
                elif "rlab" in cls:
                    if pending_prod:
                        rows.append((pending_prod, text, pending_url))
                        pending_prod = None
                        pending_url = None
            if pending_prod:
                rows.append((pending_prod, "", pending_url))

    except Exception:
        log.exception("Fallback extraction failed")

    return rows

# ====== MAIN ======
def main():
    log.info("===== Starting AlfaBeta Products Scraper =====")
    
    # Check Tor connection before starting (Tor is optional unless REQUIRE_TOR_PROXY=true)
    audit_log("RUN_STARTED", scraper_name="Argentina", run_id=_RUN_ID, details={"script": "01_products"})
    if not check_tor_requirements():
        print("\n" + "=" * 80)
        print("[STARTUP] [FAIL] Tor connection check failed!")
        print("[STARTUP] Please start Tor (Tor Browser or Tor service) and try again.")
        print("=" * 80 + "\n")
        log.error("[STARTUP] Tor connection check failed!")
        log.error("[STARTUP] Please start Tor (Tor Browser or Tor service) and try again.")
        return 1
    
    print("\n" + "=" * 80)
    if TOR_PROXY_PORT:
        print("[STARTUP] [OK] Tor connection verified. Starting scraper...")
        print("[STARTUP] Using Tor proxy for all requests")
        log.info("[STARTUP] Tor connection verified. Starting scraper...")
        log.info("[STARTUP] Using Tor proxy for all requests")
    else:
        print("[STARTUP] [OK] Starting scraper WITHOUT Tor (direct connection)...")
        print("[STARTUP] Tip: set REQUIRE_TOR_PROXY=true to enforce Tor usage")
        log.info("[STARTUP] Starting scraper without Tor (direct connection)")
    print("=" * 80 + "\n")
    
    # Ensure run_id is in run_ledger (in case step 0 was skipped or backup failed)
    _REPO.ensure_run_in_ledger(mode="resume")
    
    # Force headless mode for product list extraction
    d = setup_driver(headless=True)
    try:
        # Go directly to alfabeta.net/precio/
        log.info(f"Navigating to {PRODUCTS_URL}")
        navigate_to_products_page(d)
        ensure_logged_in(d)
        
        # Wait for page to load (wait for form#srvPr)
        log.info("Waiting for page to load...")
        try:
            WebDriverWait(d, WAIT_LONG).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "form#srvPr"))
            )
            log.info("Page loaded successfully")
        except TimeoutException:
            log.warning("Form not found after initial load, continuing anyway")
        
        # Handle any alerts
        dismiss_alert_if_present(d)
        
        # Refresh page
        log.info("Refreshing page...")
        d.refresh()
        ensure_logged_in(d)
        
        # Wait for page to load again after refresh
        log.info("Waiting for page to load after refresh...")
        try:
            WebDriverWait(d, WAIT_LONG).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "form#srvPr"))
            )
            log.info("Page loaded successfully after refresh")
        except TimeoutException:
            log.warning("Form not found after refresh, continuing anyway")
        
        # Handle any alerts after refresh
        dismiss_alert_if_present(d)
        
        # Now submit blank search to load all products
        log.info("Submitting blank search to load all products...")
        submit_blank_products(d)
        audit_log("ACTION", scraper_name="Argentina", run_id=_RUN_ID, details={"action": "SUBMIT_BLANK_SEARCH"})

        acc: Set[Tuple[str, str, Optional[str]]] = set()
        page = 1
        # Estimate total pages (will update as we go)
        estimated_total = 100  # Initial estimate, will adjust
        consecutive_errors = 0
        max_consecutive_errors = 3  # Exit after 3 consecutive extraction failures

        while True:
            try:
                pairs = extract_products_page(d)
                consecutive_errors = 0  # Reset on success
                for row in pairs:
                    acc.add(row)
                log.info(f"Page {page}: +{len(pairs)}  (unique total: {len(acc)})")
                audit_log("PAGE_FETCHED", scraper_name="Argentina", run_id=_RUN_ID, details={"page": page, "items_on_page": len(pairs), "total_unique": len(acc)})

                # Output progress (estimate total pages, update as we discover more)
                if page % 10 == 0 or len(pairs) == 0:  # Update every 10 pages or when no more products
                    # Estimate: if we're still getting products, there might be more
                    if len(pairs) > 0:
                        estimated_total = max(estimated_total, page + 10)  # Extend estimate
                    percent = round((page / estimated_total) * 100, 1) if estimated_total > 0 else 0
                    print(f"[PROGRESS] Extracting products: Page {page} (unique: {len(acc)})", flush=True)
            except Exception:
                consecutive_errors += 1
                log.exception(f"Error extracting page (attempt {consecutive_errors}/{max_consecutive_errors}); saving artifact")
                safe_screenshot_and_source(d, name_prefix=f"extract_page_{page}")
                if consecutive_errors >= max_consecutive_errors:
                    log.error(f"Too many consecutive extraction errors ({consecutive_errors}), aborting pagination")
                    break

            # attempt to go next
            try:
                if not go_next(d):
                    log.info("No 'next' link found - finished paging")
                    # Final progress update
                    print(f"[PROGRESS] Extracting products: Page {page}/{page} (100%) - {len(acc)} unique products", flush=True)
                    break
            except Exception:
                log.exception("Error clicking next; aborting pagination and saving artifact")
                safe_screenshot_and_source(d, name_prefix=f"go_next_error_{page}")
                # Final progress update
                print(f"[PROGRESS] Extracting products: Page {page}/{page} (100%) - {len(acc)} unique products", flush=True)
                break
            page += 1

        # after loop end

        rows = [
            {
                "product": prod,
                "company": comp,
                "url": url,
                "loop_count": 0,
                "total_records": 0,
                "status": "pending",
            }
            for prod, comp, url in sorted(acc, key=lambda x: (x[0].lower(), x[1].lower()))
        ]
        inserted = _REPO.upsert_product_index(rows)
        audit_log("INSERT_COMPLETE", scraper_name="Argentina", run_id=_RUN_ID, details={"inserted": inserted, "total_attempted": len(rows)})
        db_count = _REPO.get_product_index_count()
        log.info(f"[DB] Upserted {inserted} product index rows into ar_product_index (run_id={_RUN_ID})")
        print(f"[DB] Product index ready: {db_count} rows", flush=True)
        # Count cross-checks
        extracted_count = len(rows)
        if db_count < extracted_count:
            log.error(
                "[COUNT_MISMATCH] Extracted=%s, DB=%s (run_id=%s) - CRITICAL: DB missing rows",
                extracted_count,
                db_count,
                _RUN_ID,
            )
            raise RuntimeError(
                f"Count mismatch: extracted={extracted_count} > db={db_count} (run_id={_RUN_ID}). Upsert failed?"
            )
        elif db_count > extracted_count:
            log.warning(
                "[COUNT_MISMATCH] Extracted=%s, DB=%s (run_id=%s) - DB has extra rows; likely from prior attempt or site changes.",
                extracted_count,
                db_count,
                _RUN_ID,
            )
        else:
            log.info("[COUNT_OK] website/extracted=%s db_inserted=%s", extracted_count, db_count)
        return 0
    except Exception:
        log.exception("Fatal error during scraping; capturing artifacts")
        try:
            safe_screenshot_and_source(d, name_prefix="fatal")
        except Exception:
            pass
        return 1
    finally:
        try:
            d.quit()
        except Exception:
            pass
        log.info("Done.")
        
        # Write metrics for pipeline runner
        try:
            metrics_file = os.environ.get("PIPELINE_METRICS_FILE")
            if metrics_file:
                import json
                metrics = {
                    "rows_processed": extracted_count if 'extracted_count' in locals() else 0,
                    "rows_inserted": inserted if 'inserted' in locals() else 0,
                    "rows_read": 0  # This step doesn't read from DB
                }
                with open(metrics_file, "w", encoding="utf-8") as f:
                    json.dump(metrics, f)
                log.info(f"[METRICS] Wrote metrics to {metrics_file}: {metrics}")
        except Exception as e:
            log.warning(f"[METRICS] Failed to write metrics: {e}")

    return 0

if __name__ == "__main__":
    import sys
    exit_code = main()
    sys.exit(exit_code if exit_code is not None else 0)
