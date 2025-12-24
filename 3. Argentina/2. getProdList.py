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
)
from webdriver_manager.chrome import ChromeDriverManager

# ====== CONFIG ======
HUB_URL  = "https://www.alfabeta.net/precio/"
USERNAME = os.getenv("ALFABETA_USER", "your_email@example.com")
PASSWORD = os.getenv("ALFABETA_PASS", "your_password")

# Control headless via env var "HEADLESS" = "1" or "0"
HEADLESS   = os.getenv("HEADLESS", "0") == "1"
WAIT_SHORT = 5
WAIT_LONG  = 20
PAUSE      = 0.6

INPUT_DIR = Path("./input")
INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PRODUCTS = INPUT_DIR / "Productlist.csv"

# Failure artifacts dir
ARTIFACTS_DIR = Path("./artifacts")
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("alfabeta-products-dump")

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

# ====== UTIL ======
def safe_screenshot_and_source(d, name_prefix="failure"):
    ts = int(time.time())
    screenshot_path = ARTIFACTS_DIR / f"{name_prefix}-{ts}.png"
    html_path = ARTIFACTS_DIR / f"{name_prefix}-{ts}.html"
    try:
        d.save_screenshot(str(screenshot_path))
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

            if box is not None:
                try:
                    box.clear()
                except Exception:
                    pass

            # Try clicking submit button if exists
            try:
                submit_btn = form.find_element(By.CSS_SELECTOR, "input[type='submit'], .mfsubmit")
                # wait clickable
                WebDriverWait(d, WAIT_SHORT).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "form#srvPr input[type='submit'], form#srvPr .mfsubmit"))
                )
                try:
                    submit_btn.click()
                except Exception:
                    log.warning("Native click failed; trying JS click")
                    d.execute_script("arguments[0].click();", submit_btn)
            except NoSuchElementException:
                # fallback: send ENTER to the box
                if box is not None:
                    try:
                        box.send_keys(Keys.ENTER)
                    except Exception:
                        log.warning("box.send_keys(Keys.ENTER) failed; trying JS form.submit()")
                        try:
                            d.execute_script("arguments[0].submit();", form)
                        except Exception:
                            try:
                                d.execute_script("if (typeof enviar === 'function') enviar();")
                            except Exception:
                                raise

            # After triggering submit, wait for either results table or for form to go stale (navigation)
            try:
                WebDriverWait(d, WAIT_LONG).until(
                    lambda drv: drv.find_elements(By.CSS_SELECTOR, "table.estandar") or not form.is_displayed()
                )
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
    d = setup_driver(headless=HEADLESS)
    try:
        open_hub(d)
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
