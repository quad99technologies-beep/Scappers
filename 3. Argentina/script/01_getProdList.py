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
import random
from pathlib import Path
from typing import List, Tuple, Set, Optional
from urllib.parse import urlparse

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
# Load .env file if it exists
def load_env_file():
    """Load environment variables from .env file (platform root first, then local)"""
    # Try platform root first (repository root, 2 levels up from script/)
    script_dir = Path(__file__).resolve().parent
    platform_root_env = script_dir.parents[1] / ".env"
    env_file = None
    
    if platform_root_env.exists():
        env_file = platform_root_env
    else:
        # Fallback: try scraper root
        scraper_root_env = script_dir.parent / ".env"
        if scraper_root_env.exists():
            env_file = scraper_root_env
        else:
            # Fallback: current directory
            local_env = Path(".env")
            if local_env.exists():
                env_file = local_env
    
    if env_file:
        try:
            with open(env_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, value = line.split("=", 1)
                        # Only set if not already in environment
                        if key.strip() not in os.environ:
                            os.environ[key.strip()] = value.strip()
        except Exception:
            pass  # Silently fail if .env can't be read

load_env_file()  # Load .env at startup

HUB_URL  = "https://www.alfabeta.net/precio/"
PRODUCTS_URL = "https://www.alfabeta.net/precio"  # Products search page (form#srvPr)
USERNAME = os.getenv("ALFABETA_USER", "your_email@example.com")
PASSWORD = os.getenv("ALFABETA_PASS", "your_password")

# Control headless via env var "HEADLESS" = "1" or "0"
HEADLESS   = os.getenv("HEADLESS", "0") == "1"
WAIT_SHORT = 5
WAIT_LONG  = 20
PAUSE      = 0.6

INPUT_DIR = Path("./Input")
INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PRODUCTS = INPUT_DIR / "Productlist.csv"
PROXY_LIST = INPUT_DIR / "ProxyList.txt"

# Failure artifacts dir
ARTIFACTS_DIR = Path("./artifacts")
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("alfabeta-products-dump")

# ====== PROXY ======
def load_proxies() -> List[str]:
    """Load proxy URLs from ProxyList.txt or .env file"""
    proxies = []
    
    # First try platform root .env file
    script_dir = Path(__file__).resolve().parent
    platform_root_env = script_dir.parents[1] / ".env"
    env_file = None
    
    if platform_root_env.exists():
        env_file = platform_root_env
    else:
        scraper_root_env = script_dir.parent / ".env"
        if scraper_root_env.exists():
            env_file = scraper_root_env
        else:
            env_file = Path(".env")
    
    if env_file and env_file.exists():
        try:
            with open(env_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, value = line.split("=", 1)
                        if key.startswith("PROXY_") and value:
                            proxies.append(value.strip())
        except Exception as e:
            log.debug(f"Could not read .env file: {e}")
    
    # Fallback to ProxyList.txt if .env has no proxies
    if not proxies and PROXY_LIST.exists():
        with open(PROXY_LIST, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    proxies.append(line)
    
    return proxies

def get_random_proxy() -> Optional[str]:
    """Get a random proxy from the list"""
    proxies = load_proxies()
    return random.choice(proxies) if proxies else None

def parse_proxy_url(proxy_url: str) -> dict:
    """Parse proxy URL to extract components"""
    # Format: https://user:pass@host:port
    parsed = urlparse(proxy_url)
    return {
        "host": parsed.hostname,
        "port": parsed.port,
        "username": parsed.username,
        "password": parsed.password,
        "scheme": parsed.scheme or "http"
    }

# ====== DRIVER ======
def setup_driver(headless: bool = HEADLESS, proxy_url: Optional[str] = None):
    opts = webdriver.ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-popup-blocking")
    
    # Configure proxy if provided
    if proxy_url:
        try:
            proxy_info = parse_proxy_url(proxy_url)
            log.info(f"Using proxy: {proxy_info['host']}:{proxy_info['port']}")
            
            # For authenticated proxies, use Chrome extension
            if proxy_info['username'] and proxy_info['password']:
                import tempfile
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
    driver.set_page_load_timeout(90)
    # small implicit wait to reduce flakiness, rely mostly on explicit waits
    driver.implicitly_wait(2)
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
        WebDriverWait(d, 2).until(EC.alert_is_present())
        d.switch_to.alert.accept()
    except TimeoutException:
        pass
    time.sleep(1)

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
        WebDriverWait(d, 2).until(EC.alert_is_present())
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
            WebDriverWait(d, 2).until(EC.alert_is_present())
            d.switch_to.alert.accept()
        except TimeoutException:
            pass

# ====== UTIL ======
def dismiss_alert_if_present(d):
    """Dismiss any alert that might be present"""
    try:
        WebDriverWait(d, 1).until(EC.alert_is_present())
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
def submit_blank_products(d, max_retries: int = 4):
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
            # re-find the input inside the form each attempt (avoid stale refs)
            try:
                box = form.find_element(By.CSS_SELECTOR, "input[name='patron']")
            except Exception:
                box = None

            if box is None:
                log.warning("Input box not found, skipping this attempt")
                continue
                
            # Submit form - bypass validation by using JavaScript directly
            # The form has onsubmit="return validaParamPr()" which blocks blank submissions
            # Solution: Use JavaScript to set minimal input and submit directly
            try:
                box.clear()
                # Set minimal input (2 spaces) to satisfy validation, then submit via JS
                d.execute_script("""
                    var form = arguments[0];
                    var input = form.querySelector('input[name="patron"]');
                    if (input) {
                        input.value = '  ';  // Two spaces to satisfy "2 characters" requirement
                        // Bypass validation and submit directly
                        form.onsubmit = null;  // Remove validation
                        form.submit();
                    }
                """, form)
                log.debug("Submitted form via JavaScript (bypassed validation)")
                time.sleep(1)  # Wait for submission to process
                
            except Exception as e:
                log.warning(f"JavaScript submission failed: {e}, trying alternative method")
                # Fallback: Try clicking submit button
                try:
                    submit_btn = form.find_element(By.CSS_SELECTOR, "input.mfsubmit, input[type='submit']")
                    # Set input value first
                    box.clear()
                    box.send_keys("  ")
                    time.sleep(0.2)
                    # Click submit button
                    submit_btn.click()
                    log.debug("Clicked submit button")
                except Exception:
                    # Last resort: Try direct form submit
                    try:
                        box.clear()
                        box.send_keys("  ")
                        d.execute_script("arguments[0].submit();", form)
                        log.debug("Used direct form.submit()")
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

        time.sleep(1 + attempt * 0.5)

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
    time.sleep(0.4)
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
    proxy = get_random_proxy()
    d = setup_driver(headless=HEADLESS, proxy_url=proxy)
    try:
        open_hub(d)
        open_products_page(d)  # Navigate to products page before submitting
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
    except Exception:
        log.exception("Fatal error during scraping; capturing artifacts")
        try:
            safe_screenshot_and_source(d, name_prefix="fatal")
        except Exception:
            pass
        raise
    finally:
        try:
            d.quit()
        except Exception:
            pass
        log.info("Done.")

if __name__ == "__main__":
    main()
