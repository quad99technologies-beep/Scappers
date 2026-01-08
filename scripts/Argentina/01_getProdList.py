#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AlfaBeta – PRODUCTS index dump (srvPr)
Resilient end-to-end scraper that:
 - logs in
 - submits a blank 'patron' on form#srvPr to list all products
 - paginates through results, extracting (Product, Company)
 - saves Productlist.csv
 - captures screenshots and page source on critical failure
"""

import os
import csv
import time
import logging
from pathlib import Path
from typing import List, Tuple, Set, Optional

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
    WebDriverException,
    UnexpectedAlertPresentException,
)
from webdriver_manager.chrome import ChromeDriverManager

# ====== CONFIG ======
from config_loader import (
    get_input_dir, get_output_dir,
    ALFABETA_USER as USERNAME, ALFABETA_PASS as PASSWORD,
    HEADLESS, PRODUCTS_URL, HUB_URL,
    PRODUCTLIST_FILE,
    WAIT_SHORT, WAIT_LONG, WAIT_ALERT, PAUSE_BETWEEN_OPERATIONS,
    PAGE_LOAD_TIMEOUT, IMPLICIT_WAIT, MAX_RETRIES_SUBMIT,
    PAUSE_SHORT, PAUSE_MEDIUM, PAUSE_AFTER_ALERT
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

INPUT_DIR = get_input_dir()
INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PRODUCTS = INPUT_DIR / PRODUCTLIST_FILE

OUTPUT_DIR = get_output_dir()
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Failure artifacts dir - store in output folder
ARTIFACTS_DIR = OUTPUT_DIR / "artifacts"
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("alfabeta-products-dump")

# ====== VPN CHECK ======
def get_vpn_info() -> dict:
    """Get detailed VPN connection information."""
    vpn_info = {
        "connected": False,
        "provider": "Unknown",
        "server": "Unknown",
        "country": "Unknown",
        "city": "Unknown",
        "ip": "Unknown",
        "method": "Unknown"
    }
    
    try:
        import subprocess
        import platform
        
        # Method 1: Check Proton VPN CLI status (Linux/Mac)
        if platform.system() != "Windows":
            try:
                result = subprocess.run(
                    ["protonvpn-cli", "status"],
                    capture_output=True,
                    timeout=10,
                    text=True
                )
                if result.returncode == 0:
                    output = result.stdout
                    output_lower = output.lower()
                    if "connected" in output_lower or "active" in output_lower:
                        vpn_info["connected"] = True
                        vpn_info["provider"] = "Proton VPN"
                        vpn_info["method"] = "Proton VPN CLI"
                        
                        # Parse server information from output
                        lines = output.split('\n')
                        for line in lines:
                            line_lower = line.lower()
                            if 'server' in line_lower and ':' in line:
                                server = line.split(':', 1)[1].strip()
                                if server:
                                    vpn_info["server"] = server
                            elif 'country' in line_lower and ':' in line:
                                country = line.split(':', 1)[1].strip()
                                if country:
                                    vpn_info["country"] = country
                            elif 'city' in line_lower and ':' in line:
                                city = line.split(':', 1)[1].strip()
                                if city:
                                    vpn_info["city"] = city
                            elif 'ip' in line_lower and ':' in line and 'server' not in line_lower:
                                ip = line.split(':', 1)[1].strip()
                                if ip and '.' in ip:
                                    vpn_info["ip"] = ip
                        
                        return vpn_info
            except (FileNotFoundError, subprocess.TimeoutExpired):
                pass
        
        # Method 2: Check via IP geolocation (works for any VPN)
        if REQUESTS_AVAILABLE:
            try:
                ip_check_services = [
                    ("https://ipapi.co/json/", ["ip", "country_name", "city", "org"]),
                    ("https://api.ipify.org?format=json", ["ip"]),
                    ("https://api.myip.com", ["ip", "country"])
                ]
                
                for service_url, fields in ip_check_services:
                    try:
                        response = requests.get(service_url, timeout=10)
                        if response.status_code == 200:
                            ip_info = response.json()
                            
                            vpn_info["connected"] = True
                            vpn_info["method"] = "IP Geolocation"
                            
                            vpn_info["ip"] = ip_info.get("ip") or ip_info.get("query") or "Unknown"
                            vpn_info["country"] = ip_info.get("country_name") or ip_info.get("country") or "Unknown"
                            vpn_info["city"] = ip_info.get("city") or "Unknown"
                            
                            org = ip_info.get("org") or ip_info.get("isp") or ""
                            if "proton" in org.lower():
                                vpn_info["provider"] = "Proton VPN"
                                if "#" in org:
                                    vpn_info["server"] = org.split("#")[-1].strip()
                            else:
                                vpn_info["provider"] = org or "VPN Service"
                            
                            return vpn_info
                    except Exception:
                        continue
            except Exception:
                pass  # If IP check fails, continue to return default vpn_info
        
        return vpn_info
    except Exception as e:
        log.warning(f"[VPN_INFO] Error getting VPN info: {e}")
        return vpn_info

def check_vpn_connection() -> bool:
    """Check if VPN is connected and working. Displays VPN connection details."""
    print("\n" + "=" * 80)
    print("[VPN_CHECK] Verifying VPN connection...")
    print("=" * 80)
    log.info("[VPN_CHECK] Verifying VPN connection...")
    
    try:
        import subprocess
        import platform
        
        # Method 1: Check Proton VPN CLI status (Linux/Mac)
        if platform.system() != "Windows":
            try:
                result = subprocess.run(
                    ["protonvpn-cli", "status"],
                    capture_output=True,
                    timeout=10,
                    text=True
                )
                if result.returncode == 0:
                    output = result.stdout
                    output_lower = output.lower()
                    if "connected" in output_lower or "active" in output_lower:
                        print("\n[VPN_STATUS] [OK] VPN CONNECTED")
                        print("-" * 80)
                        lines = output.split('\n')
                        for line in lines:
                            if line.strip() and ':' in line:
                                print(f"  {line.strip()}")
                        print("-" * 80)
                        log.info("[VPN_CHECK] [OK] VPN is connected (Proton VPN CLI)")
                        return True
                    else:
                        print("\n[VPN_STATUS] [FAIL] VPN NOT CONNECTED")
                        print("-" * 80)
                        print("  Please connect Proton VPN before running the scraper")
                        print("-" * 80)
                        log.error("[VPN_CHECK] [FAIL] VPN is not connected (Proton VPN CLI)")
                        return False
            except (FileNotFoundError, subprocess.TimeoutExpired):
                pass
        
        # Method 2: Check IP address and location
        if not REQUESTS_AVAILABLE:
            log.warning("[VPN_CHECK] requests library not available, skipping IP check")
            try:
                print("\n[VPN_CHECK] Cannot verify VPN automatically")
                response = input("[VPN_CHECK] Is your VPN connected? (yes/no): ").strip().lower()
                if response in ["yes", "y"]:
                    print("[VPN_CHECK] [OK] Proceeding with user confirmation")
                    log.info("[VPN_CHECK] Proceeding with user confirmation")
                    return True
                else:
                    print("[VPN_CHECK] [FAIL] VPN connection not confirmed. Exiting.")
                    log.error("[VPN_CHECK] VPN connection not confirmed. Exiting.")
                    return False
            except (EOFError, KeyboardInterrupt):
                log.error("[VPN_CHECK] Input interrupted. Exiting.")
                return False
        
        # Get VPN info via IP geolocation
        vpn_info = get_vpn_info()
        
        if vpn_info["connected"]:
            print("\n[VPN_STATUS] [OK] VPN CONNECTED")
            print("-" * 80)
            print(f"  Provider: {vpn_info['provider']}")
            if vpn_info['server'] != "Unknown":
                print(f"  Server: {vpn_info['server']}")
            print(f"  IP Address: {vpn_info['ip']}")
            print(f"  Location: {vpn_info['city']}, {vpn_info['country']}")
            print(f"  Detection Method: {vpn_info['method']}")
            print("-" * 80)
            
            log.info(f"[VPN_CHECK] [OK] VPN Connected - Provider: {vpn_info['provider']}, Server: {vpn_info['server']}, IP: {vpn_info['ip']}, Location: {vpn_info['city']}, {vpn_info['country']}")
            
            if vpn_info['ip'] and vpn_info['ip'] not in ["127.0.0.1", "localhost", "::1", "Unknown"]:
                return True
            else:
                print("[VPN_CHECK] [FAIL] VPN connection failed (no valid external IP)")
                log.error("[VPN_CHECK] [FAIL] VPN connection failed (no valid external IP)")
                return False
        else:
            print("\n[VPN_STATUS] [FAIL] VPN NOT CONNECTED")
            print("-" * 80)
            print("  Could not detect VPN connection")
            print("  Please connect your VPN (Proton VPN) and try again")
            print("-" * 80)
            log.error("[VPN_CHECK] [FAIL] VPN connection not detected")
            return False
            
    except Exception as e:
        log.error(f"[VPN_CHECK] VPN check failed: {e}")
        log.warning("[VPN_CHECK] Cannot verify VPN connection. Please ensure VPN is connected before proceeding.")
        try:
            response = input("[VPN_CHECK] Is your VPN connected? (yes/no): ").strip().lower()
            if response in ["yes", "y"]:
                log.info("[VPN_CHECK] Proceeding with user confirmation")
                return True
            else:
                log.error("[VPN_CHECK] VPN connection not confirmed. Exiting.")
                return False
        except (EOFError, KeyboardInterrupt):
            log.error("[VPN_CHECK] Input interrupted. Exiting.")
            return False

# ====== DRIVER ======
def setup_driver(headless: bool = HEADLESS):
    opts = webdriver.ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-popup-blocking")
    
    # Note: No proxy configuration - using VPN only
    log.info("[DRIVER] No proxy configured - using VPN only")
    
    # prefer Spanish content, disable translate
    prefs = {
        "translate": {"enabled": False},
        "intl.accept_languages": "es-ES,es,en-US,en"
    }
    opts.add_experimental_option("prefs", prefs)
    opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    opts.add_experimental_option("useAutomationExtension", False)

    # Create driver
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    except Exception as e:
        log.exception("Failed to start ChromeDriver")
        raise

    # Be generous on page load for slow pages
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    # small implicit wait to reduce flakiness, rely mostly on explicit waits
    driver.implicitly_wait(IMPLICIT_WAIT)
    
    # Register Chrome instance for cleanup tracking
    try:
        from core.chrome_manager import register_chrome_driver
        register_chrome_driver(driver)
    except ImportError:
        pass  # Chrome manager not available, continue without registration
    
    return driver

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
    time.sleep(PAUSE_AFTER_ALERT)

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

# ====== FORM SUBMIT (robust) ======
def submit_blank_products(d, max_retries: int = MAX_RETRIES_SUBMIT):
    """
    Submit blank 'patron' on form#srvPr. Retries with JS-click fallback and waits for results.
    Raises RuntimeError on repeated failure.
    """
    for attempt in range(1, max_retries + 1):
        log.info(f"submit_blank_products: attempt {attempt}/{max_retries}")
        try:
            form = WebDriverWait(d, WAIT_LONG).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "form#srvPr"))
            )
            # Find the input field using the exact selector
            try:
                box = form.find_element(By.CSS_SELECTOR, "input.entrada[name='patron'], input[name='patron']")
            except Exception:
                box = None

            if box is None:
                log.warning("Input box not found, skipping this attempt")
                continue
            
            # Find the submit button
            try:
                submit_btn = form.find_element(By.CSS_SELECTOR, "input.mfsubmit[value='Buscar'], input.mfsubmit, input[type='submit'][value='Buscar']")
            except Exception:
                submit_btn = None
                
            # Clear the input field and leave it empty
            log.debug("Clearing input field and submitting with empty search")
            box.clear()
            time.sleep(PAUSE_SHORT)
            
            # Try to submit with empty input first
            try:
                # Method 1: Click the submit button directly
                if submit_btn:
                    log.debug("Clicking submit button (Buscar)")
                    submit_btn.click()
                    time.sleep(PAUSE_AFTER_ALERT)
                else:
                    # Method 2: Use JavaScript to bypass validation and submit
                    log.debug("Submitting via JavaScript (bypassing validation)")
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
                    time.sleep(PAUSE_AFTER_ALERT)
            except Exception as e:
                log.warning(f"Primary submission method failed: {e}, trying fallback")
                # Fallback: Try with minimal input if empty doesn't work
                try:
                    box.clear()
                    box.send_keys("  ")  # Two spaces as fallback
                    time.sleep(PAUSE_SHORT)
                    if submit_btn:
                        submit_btn.click()
                    else:
                        d.execute_script("arguments[0].submit();", form)
                    log.debug("Used fallback method with minimal input")
                    time.sleep(PAUSE_AFTER_ALERT)
                except Exception as e2:
                    log.warning(f"All submission methods failed: {e2}")
                    raise

            # After triggering submit, wait for results (same approach as script 03)
            try:
                WebDriverWait(d, WAIT_LONG).until(
                    lambda drv: drv.find_elements(By.CSS_SELECTOR, "a.rprod, form[name^='pr']")
                )
                log.debug("Results page loaded")
            except TimeoutException:
                log.warning("Timeout waiting for results page")
                # Check if we're still on the same page
                try:
                    if form.is_displayed():
                        log.warning("Form still visible, submission may have failed")
                except StaleElementReferenceException:
                    log.debug("Form went stale, page may have navigated")
            except Exception as e_wait:
                log.debug(f"Post-submit wait encountered: {e_wait}")

            # small pause; then ensure logged in (to catch redirects to login)
            time.sleep(PAUSE)
            ensure_logged_in(d)

            # Confirm we have at least product anchors; if so, success
            try:
                prods = d.find_elements(By.CSS_SELECTOR, "table.estandar td a.rprod")
                if prods:
                    log.info("submit_blank_products: results detected")
                    return
            except Exception:
                pass

            log.warning("submit_blank_products: no product anchors detected after submit - will retry")
        except TimeoutException as te:
            log.warning(f"Timeout while locating form (attempt {attempt}): {te}")
        except StaleElementReferenceException:
            log.warning("Form went stale while submitting; retrying")
        except WebDriverException as e:
            log.exception(f"WebDriverException during submit attempt {attempt}: {e}")
        except Exception:
            log.exception("Unexpected error in submit_blank_products")

        # capture an artifact occasionally
        try:
            safe_screenshot_and_source(d, name_prefix=f"submit_attempt_{attempt}")
        except Exception:
            pass

        time.sleep(PAUSE_AFTER_ALERT + attempt * 0.5)

    raise RuntimeError("submit_blank_products failed after retries")

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
                    time.sleep(PAUSE)
                    return True
                except StaleElementReferenceException:
                    # element vanished — page likely navigated; treat as success
                    time.sleep(PAUSE)
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
    """
    time.sleep(PAUSE_MEDIUM)
    rows: List[Tuple[str, str]] = []
    try:
        prods = d.find_elements(By.CSS_SELECTOR, "table.estandar td a.rprod")
        labs  = d.find_elements(By.CSS_SELECTOR, "table.estandar td a.rlab")
        if len(prods) == len(labs) and len(prods) > 0:
            for p, l in zip(prods, labs):
                pname = clean(p.get_attribute("innerText"))
                cname = clean(l.get_attribute("innerText"))
                if pname or cname:
                    rows.append((pname, cname))
            return rows
    except Exception:
        log.debug("Primary extraction method failed or found mismatched counts; falling back")

    # Fallback: pair within each TD block by nearest anchors
    try:
        prod_nodes = d.find_elements(By.CSS_SELECTOR, "table.estandar td")
        for td in prod_nodes:
            anchors = td.find_elements(By.CSS_SELECTOR, "a.rprod, a.rlab")
            pending_prod: Optional[str] = None
            for a in anchors:
                cls = a.get_attribute("class") or ""
                text = clean(a.get_attribute("innerText"))
                if not text:
                    continue
                if "rprod" in cls:
                    if pending_prod:
                        rows.append((pending_prod, ""))  # flush previous without lab
                    pending_prod = text
                elif "rlab" in cls:
                    if pending_prod:
                        rows.append((pending_prod, text))
                        pending_prod = None
            if pending_prod:
                rows.append((pending_prod, ""))
    except Exception:
        log.exception("Fallback extraction failed")

    return rows

# ====== MAIN ======
def main():
    log.info("===== Starting AlfaBeta Products Scraper =====")
    
    # Check VPN connection before starting
    if not check_vpn_connection():
        print("\n" + "=" * 80)
        print("[STARTUP] [FAIL] VPN connection check failed!")
        print("[STARTUP] Please connect your VPN (Proton VPN) and try again.")
        print("=" * 80 + "\n")
        log.error("[STARTUP] VPN connection check failed!")
        log.error("[STARTUP] Please connect your VPN (Proton VPN) and try again.")
        return 1
    
    print("\n" + "=" * 80)
    print("[STARTUP] [OK] VPN connection verified. Starting scraper...")
    print("[STARTUP] Note: Proxies are NOT used - using VPN only")
    print("=" * 80 + "\n")
    log.info("[STARTUP] VPN connection verified. Starting scraper...")
    log.info("[STARTUP] Note: Proxies are NOT used - using VPN only")
    
    d = setup_driver(headless=HEADLESS)
    try:
        # Go directly to alfabeta.net/precio/
        log.info(f"Navigating to {PRODUCTS_URL}")
        d.get(PRODUCTS_URL)
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
        time.sleep(PAUSE)
        
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
        time.sleep(PAUSE)
        
        # Now submit blank search to load all products
        log.info("Submitting blank search to load all products...")
        submit_blank_products(d)

        acc: Set[Tuple[str, str]] = set()
        page = 1
        while True:
            try:
                pairs = extract_products_page(d)
                for row in pairs:
                    acc.add(row)
                log.info(f"Page {page}: +{len(pairs)}  (unique total: {len(acc)})")
            except Exception:
                log.exception("Error extracting page; saving artifact and continuing")
                safe_screenshot_and_source(d, name_prefix=f"extract_page_{page}")

            # attempt to go next
            try:
                if not go_next(d):
                    log.info("No 'next' link found - finished paging")
                    break
            except Exception:
                log.exception("Error clicking next; aborting pagination and saving artifact")
                safe_screenshot_and_source(d, name_prefix=f"go_next_error_{page}")
                break
            page += 1

        # write CSV
        with open(OUT_PRODUCTS, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Product", "Company"])
            for prod, comp in sorted(acc, key=lambda x: (x[0].lower(), x[1].lower())):
                w.writerow([prod, comp])

        log.info(f"Saved {len(acc)} rows → {OUT_PRODUCTS}")
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

if __name__ == "__main__":
    import sys
    exit_code = main()
    sys.exit(exit_code if exit_code is not None else 0)
