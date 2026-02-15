#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Argentina - Step 3b: Selenium Company Search Scraper

This step runs AFTER the regular Selenium product search (Step 3) and BEFORE the API step (Step 4).
It targets products that still have total_records=0 after product search.

Strategy:
1. Search by COMPANY name in "Índice de Laboratorios" field
2. Click on the exact company match to see all products for that company
3. Find and click on the exact product from the company's product list
4. Extract the product data

This is useful when:
- The product name search returns too many results or no exact match
- The company name is more unique than the product name
"""

import os
import sys
import re
import time
import logging
import argparse
import threading
import signal
import gc
import json
from pathlib import Path
from datetime import datetime
from queue import Queue, Empty
from typing import Optional, Tuple, List, Dict, Any

try:
    import psutil
except Exception:
    psutil = None

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

# Import from existing selenium worker
from config_loader import (
    get_output_dir,
    PRODUCTS_URL,
    SELENIUM_MAX_LOOPS,
    SELENIUM_THREADS,
    SELENIUM_SINGLE_ATTEMPT,
    SELENIUM_ROUND_ROBIN_RETRY,
    ROUND_PAUSE_SECONDS,
    USE_API_STEPS,
)

from core.db.connection import CountryDB
from core.network.ip_rotation import get_public_ip_via_socks
from db.schema import apply_argentina_schema
from db.repositories import ArgentinaRepository

# Selenium imports
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    WebDriverException,
    InvalidSessionIdException,
    NoSuchElementException,
    StaleElementReferenceException,
)
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService

try:
    from webdriver_manager.firefox import GeckoDriverManager
    WDM_AVAILABLE = True
except ImportError:
    WDM_AVAILABLE = False

# Import utility functions from the main selenium worker
from scraper_utils import (
    nk, ts, strip_accents, OUT_FIELDS,
    append_progress, append_error, append_rows,
    update_prepared_urls_source,
)

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Timeouts
WAIT_SEARCH_FORM = 30
WAIT_SEARCH_RESULTS = 60
WAIT_PAGE_LOAD = 45
RATE_LIMIT_MIN = 1.5
RATE_LIMIT_MAX = 3.0

# Globals
_shutdown_requested = threading.Event()
_REPO: Optional[ArgentinaRepository] = None
_DB: Optional[CountryDB] = None

# Tor IP guard (per-product IP tracking)
_current_tor_ip: Optional[str] = None
_product_last_ip: Dict[Tuple[str, str], str] = {}


def _get_run_id(output_dir: Path) -> str:
    """Get run_id from env or .current_run_id file."""
    rid = os.environ.get("ARGENTINA_RUN_ID", "").strip()
    if rid:
        return rid
    run_id_file = output_dir / ".current_run_id"
    if run_id_file.exists():
        txt = run_id_file.read_text(encoding="utf-8").strip()
        if txt:
            os.environ["ARGENTINA_RUN_ID"] = txt
            return txt
    raise RuntimeError("ARGENTINA_RUN_ID not set and .current_run_id missing.")


def rate_limit_pause():
    """Random pause for rate limiting."""
    time.sleep(random.uniform(RATE_LIMIT_MIN, RATE_LIMIT_MAX))


import random


def normalize_ws(s: str) -> str:
    """Normalize whitespace in string."""
    if not s:
        return ""
    return " ".join(s.split())


def check_tor_running(host="127.0.0.1", timeout=2):
    """Check if Tor SOCKS5 proxy is running."""
    import socket
    for port in [9050, 9150]:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                log.info(f"[TOR_CHECK] Tor proxy is running on {host}:{port}")
                return True, port
        except Exception:
            continue
    log.warning(f"[TOR_CHECK] Tor proxy is not running")
    return False, None


def create_firefox_driver(headless: bool = True) -> webdriver.Firefox:
    """Create a Firefox WebDriver instance with Tor proxy support."""
    options = FirefoxOptions()
    if headless:
        options.add_argument("--headless")
    
    # Create Firefox profile with Tor proxy configuration
    profile = webdriver.FirefoxProfile()
    
    # Disable notifications and popups
    profile.set_preference("dom.webnotifications.enabled", False)
    profile.set_preference("dom.push.enabled", False)
    profile.set_preference("permissions.default.desktop-notification", 2)

    # Disable images, CSS, and fonts for speed
    profile.set_preference("permissions.default.image", 2)
    profile.set_preference("permissions.default.stylesheet", 2)
    profile.set_preference("browser.display.use_document_fonts", 0)
    profile.set_preference("gfx.downloadable_fonts.enabled", False)

    # Check for Tor proxy and configure if available
    tor_running, tor_port = check_tor_running()
    if tor_running and tor_port:
        # Configure SOCKS5 proxy for Tor
        profile.set_preference("network.proxy.type", 1)  # Manual proxy configuration
        profile.set_preference("network.proxy.socks", "127.0.0.1")
        profile.set_preference("network.proxy.socks_port", tor_port)
        profile.set_preference("network.proxy.socks_version", 5)
        profile.set_preference("network.proxy.socks_remote_dns", True)  # Route DNS through Tor
        log.info(f"[TOR_CONFIG] Using Tor proxy on port {tor_port}")
    else:
        log.warning("[TOR_CONFIG] Tor proxy not available, using direct connection")
    
    options.profile = profile
    options.page_load_strategy = "eager"
    
    if WDM_AVAILABLE:
        service = FirefoxService(GeckoDriverManager().install())
        driver = webdriver.Firefox(service=service, options=options)
    else:
        driver = webdriver.Firefox(options=options)
    
    driver.set_page_load_timeout(120)
    driver.implicitly_wait(10)
    return driver


def navigate_to_products_page(driver) -> bool:
    """Navigate to the products search page."""
    try:
        driver.get(PRODUCTS_URL or "https://www.alfabeta.net/precio")
        WebDriverWait(driver, WAIT_SEARCH_FORM).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "form#srvPr"))
        )
        return True
    except Exception as e:
        log.error(f"[NAVIGATE] Failed to navigate to products page: {e}")
        return False


def search_company_on_page(driver, company_term: str) -> bool:
    """
    Search for company using the "Índice de Laboratorios" search form.
    
    Same pattern as product search in 03_alfabeta_selenium_worker.py but uses form#srvLab.
    After search, the page shows company links that we need to click to get products.
    """
    log.info(f"[COMPANY_SEARCH] Searching for company: {company_term}")
    
    # Wait for search form to be present
    log.info(f"[COMPANY_SEARCH] Waiting for search form (timeout: {WAIT_SEARCH_FORM}s)...")
    try:
        form = WebDriverWait(driver, WAIT_SEARCH_FORM).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "form#srvLab"))
        )
        log.info("[COMPANY_SEARCH] Search form found")
    except TimeoutException:
        # Form not found, try navigating again
        log.warning("[COMPANY_SEARCH] Search form not found, navigating to products page...")
        navigate_to_products_page(driver)
        form = WebDriverWait(driver, WAIT_SEARCH_FORM).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "form#srvLab"))
        )
    
    log.info(f"[COMPANY_SEARCH] Entering search term: {company_term}")
    box = form.find_element(By.NAME, "patron")
    box.clear()
    time.sleep(0.3)  # Small delay to ensure form is ready
    box.send_keys(company_term)
    time.sleep(0.2)  # Small delay before submitting
    box.send_keys(Keys.ENTER)
    
    # Rate limiting: pause after form submission
    rate_limit_pause()
    log.info(f"[COMPANY_SEARCH] Search submitted, waiting for results (timeout: {WAIT_SEARCH_RESULTS}s)...")
    
    try:
        # Wait for search results to appear - company search results show "coincidencias"
        # and contain links in the results area
        # First, wait for page to have actual content (not empty HTML)
        def page_has_content(d):
            src = d.page_source
            # Check if page has actual content, not just empty HTML
            has_content = len(src) > 1000 and ("alfabeta" in src.lower() or "laboratorio" in src.lower())
            if not has_content and len(src) < 500:
                log.debug(f"[COMPANY_SEARCH] Page source length: {len(src)}")
            return has_content
        
        WebDriverWait(driver, WAIT_SEARCH_RESULTS).until(page_has_content)
        log.info("[COMPANY_SEARCH] Page content loaded")
        
        # Now check for search results indicators
        page_src = driver.page_source.lower()
        
        # Check for coincidencias (matches found)
        if "coincidencias" in page_src:
            log.info("[COMPANY_SEARCH] Search results loaded successfully (found 'coincidencias')")
            # Check for zero results
            if "0 coincidencias" in page_src:
                log.warning(f"[COMPANY_SEARCH] No results found for company: {company_term}")
                return False
            return True
        
        # Alternative: check for "resultado de su busqueda" (search result)
        if "resultado de su busqueda" in page_src or "resultado de su búsqueda" in page_src:
            log.info("[COMPANY_SEARCH] Search results loaded (found 'resultado de su busqueda')")
            return True
        
        # Alternative: check if we have company links in the results
        # Sometimes the page loads without the "coincidencias" text
        try:
            results_area = None
            for selector in ["#home1", "#centroc", "div#centro"]:
                try:
                    results_area = driver.find_element(By.CSS_SELECTOR, selector)
                    break
                except:
                    continue
            
            if results_area:
                links = results_area.find_elements(By.TAG_NAME, "a")
                # Filter to potential company links
                company_links = [l for l in links if l.text.strip() and 
                                 l.text.strip().lower() not in ["principal", "precios", "vademecum", "productos", "buscar"]]
                if company_links:
                    log.info(f"[COMPANY_SEARCH] Found {len(company_links)} potential company links in results")
                    return True
        except Exception as e:
            log.debug(f"[COMPANY_SEARCH] Error checking for links: {e}")
        
        # Page loaded but no results found
        log.warning(f"[COMPANY_SEARCH] Page loaded but no search results found")
        log.warning(f"[COMPANY_SEARCH] Page source snippet: {driver.page_source[:500]}")
        return False
        
    except TimeoutException:
        log.error(f"[COMPANY_SEARCH] Search results not found after {WAIT_SEARCH_RESULTS}s")
        log.error(f"[COMPANY_SEARCH] Current URL: {driver.current_url}")
        log.error(f"[COMPANY_SEARCH] Page title: {driver.title}")
        log.error(f"[COMPANY_SEARCH] Page source length: {len(driver.page_source)}")
        log.error(f"[COMPANY_SEARCH] Page source: {driver.page_source[:1000]}")
        return False


def get_matching_company_links(driver, company_name: str) -> List:
    """
    Get all company links that match the given company name.
    Returns a list of (index, link_text) tuples for matching links.
    
    When searching for a company like "Abbvie", the results may show multiple
    entries with the same name (e.g., "Abbvie" appearing twice). We need to
    try each one to find the product.
    """
    log.info(f"[COMPANY_LINKS] Finding all links matching: {company_name}")
    
    # Find all links in the results area
    results_area = None
    for selector in ["#home1", "#centroc", "div#centro"]:
        try:
            results_area = driver.find_element(By.CSS_SELECTOR, selector)
            break
        except:
            continue
    
    if not results_area:
        log.warning("[COMPANY_LINKS] Could not find results area")
        results_area = driver  # Fall back to searching entire page
    
    # Find all links in the results area
    all_links = results_area.find_elements(By.TAG_NAME, "a")
    
    # Filter to links that could be company names (not navigation, not images)
    matching_links = []
    nk_company = nk(company_name)
    
    for idx, link in enumerate(all_links):
        try:
            link_text = normalize_ws(link.text).strip()
            href = link.get_attribute("href") or ""
            # Skip empty links, navigation links, image links
            if not link_text:
                continue
            if "javascript:" in href and "submit" not in href:
                continue
            if link_text.lower() in ["principal", "precios", "vademecum", "productos", "buscar"]:
                continue
            
            # Check if this link matches our company name
            if nk(link_text) == nk_company:
                matching_links.append((idx, link_text))
                log.info(f"[COMPANY_LINKS] Found matching link [{len(matching_links)}]: '{link_text}' at index {idx}")
        except StaleElementReferenceException:
            continue
    
    log.info(f"[COMPANY_LINKS] Found {len(matching_links)} matching company links for '{company_name}'")
    return matching_links


def click_company_link_by_index(driver, company_name: str, link_index: int) -> bool:
    """
    Click on a specific company link by its index in the results.
    
    This is used when there are multiple companies with the same name.
    Returns True if the company link was clicked and products page loaded.
    """
    log.info(f"[COMPANY_CLICK] Clicking company link at index {link_index} for: {company_name}")
    
    try:
        # Find all links in the results area again (page may have refreshed)
        results_area = None
        for selector in ["#home1", "#centroc", "div#centro"]:
            try:
                results_area = driver.find_element(By.CSS_SELECTOR, selector)
                break
            except:
                continue
        
        if not results_area:
            results_area = driver
        
        all_links = results_area.find_elements(By.TAG_NAME, "a")
        
        if link_index >= len(all_links):
            log.error(f"[COMPANY_CLICK] Link index {link_index} out of range (only {len(all_links)} links)")
            return False
        
        link = all_links[link_index]
        link_text = normalize_ws(link.text).strip()
        
        log.info(f"[COMPANY_CLICK] Clicking link: '{link_text}'")
        
        # Click the link
        try:
            link.click()
        except:
            # Try JavaScript click if regular click fails
            driver.execute_script("arguments[0].click();", link)
        
        rate_limit_pause()
        
        # Wait for company products page to load
        # The company page shows products in format "PRODUCT • Company" as links
        # These can be a.rprod OR regular <a> tags with product names
        log.info("[COMPANY_CLICK] Waiting for company products page...")
        
        def products_loaded(d):
            # Check for a.rprod links (standard product links)
            rprod_links = d.find_elements(By.CSS_SELECTOR, "a.rprod")
            if rprod_links:
                return True
            
            # Check for "coincidencias" text indicating search results loaded
            if "coincidencias" in d.page_source.lower():
                # Find links in the results area that could be products
                for selector in ["#home1", "#centroc"]:
                    try:
                        area = d.find_element(By.CSS_SELECTOR, selector)
                        links = area.find_elements(By.TAG_NAME, "a")
                        # Filter to links that look like product links (have text, not navigation)
                        product_links = [l for l in links if l.text.strip() and 
                                        l.text.strip().lower() not in ["principal", "precios", "vademecum", "productos", "buscar"]]
                        if len(product_links) > 3:  # At least a few product links
                            return True
                    except:
                        continue
            return False
        
        WebDriverWait(driver, WAIT_PAGE_LOAD).until(products_loaded)
        
        # Count products found
        prod_links = driver.find_elements(By.CSS_SELECTOR, "a.rprod")
        if not prod_links:
            # Try finding links in the results area
            for selector in ["#home1", "#centroc"]:
                try:
                    area = driver.find_element(By.CSS_SELECTOR, selector)
                    all_links = area.find_elements(By.TAG_NAME, "a")
                    prod_links = [l for l in all_links if l.text.strip() and 
                                 l.text.strip().lower() not in ["principal", "precios", "vademecum", "productos", "buscar"]]
                    break
                except:
                    continue
        
        log.info(f"[COMPANY_CLICK] Company products page loaded with {len(prod_links)} products")
        return True
        
    except TimeoutException:
        log.error("[COMPANY_CLICK] Timeout waiting for products page after clicking company")
        # Log page state for debugging
        log.error(f"[COMPANY_CLICK] Current URL: {driver.current_url}")
        log.error(f"[COMPANY_CLICK] Page has 'coincidencias': {'coincidencias' in driver.page_source.lower()}")
        return False
    except Exception as e:
        log.error(f"[COMPANY_CLICK] Error clicking company link: {e}")
        return False


def click_company_link(driver, company_name: str) -> bool:
    """
    Click on the exact company link from search results.
    
    After searching for company in "Índice de Laboratorios", the results page shows:
    - "X coincidencias para 'TERM' en el índice de Laboratorios"
    - A list of company names as plain links (e.g., just "Grimberg")
    
    NOTE: There may be multiple companies with the same name (e.g., "Abbvie" x2).
    This function clicks the FIRST matching link. Use click_company_link_by_index
    for subsequent attempts.
    
    Returns True if company was found and clicked.
    """
    log.info(f"[COMPANY_CLICK] Looking for company link: {company_name}")
    
    try:
        # Get all matching company links
        matching_links = get_matching_company_links(driver, company_name)
        
        if not matching_links:
            # Log available links for debugging
            log.warning(f"[COMPANY_CLICK] No exact match for '{company_name}'")
            
            # Find all links for debugging
            results_area = None
            for selector in ["#home1", "#centroc", "div#centro"]:
                try:
                    results_area = driver.find_element(By.CSS_SELECTOR, selector)
                    break
                except:
                    continue
            
            if results_area:
                all_links = results_area.find_elements(By.TAG_NAME, "a")
                log.warning(f"[COMPANY_CLICK] Available links:")
                for i, link in enumerate(all_links[:15]):
                    try:
                        link_text = normalize_ws(link.text).strip()
                        if link_text:
                            log.warning(f"  [{i}] '{link_text}' (normalized: '{nk(link_text)}')")
                    except:
                        pass
            
            return False
        
        # Click the first matching link
        first_idx, first_text = matching_links[0]
        return click_company_link_by_index(driver, company_name, first_idx)
        
    except Exception as e:
        log.error(f"[COMPANY_CLICK] Error clicking company: {e}")
        import traceback
        log.error(traceback.format_exc())
        return False


def find_and_click_product(driver, product_name: str, company_name: str) -> bool:
    """
    Find and click on the exact product from the company's product list.
    
    The company page can show products in two formats:
    1. a.rprod links (standard product search results)
    2. Regular <a> tags with "PRODUCT • Company" format
    
    Returns True if product was found and clicked.
    """
    log.info(f"[PRODUCT_CLICK] Looking for product: {product_name}")
    
    try:
        # First try a.rprod links (standard format)
        product_links = driver.find_elements(By.CSS_SELECTOR, "a.rprod")
        
        # If no a.rprod links, try finding links in the results area
        if not product_links:
            log.info("[PRODUCT_CLICK] No a.rprod links, searching in results area...")
            for selector in ["#home1", "#centroc", "div#centro"]:
                try:
                    area = driver.find_element(By.CSS_SELECTOR, selector)
                    all_links = area.find_elements(By.TAG_NAME, "a")
                    # Filter to links that could be products
                    product_links = [l for l in all_links if l.text.strip() and 
                                    l.text.strip().lower() not in ["principal", "precios", "vademecum", "productos", "buscar",
                                                                    "manual farmacéutico digital", "suscripciones", 
                                                                    "catálogo de precios", "altas y bajas", "estadísticas"]]
                    if product_links:
                        log.info(f"[PRODUCT_CLICK] Found {len(product_links)} potential product links in {selector}")
                        break
                except:
                    continue
        
        if not product_links:
            log.warning(f"[PRODUCT_CLICK] No product links found on company page")
            return False
        
        log.info(f"[PRODUCT_CLICK] Searching through {len(product_links)} links for '{product_name}'")
        
        # Find exact match
        nk_product = nk(product_name)
        nk_company = nk(company_name)
        
        for link in product_links:
            try:
                link_text = normalize_ws(link.text)
                
                # Handle "PRODUCT • Company" format - extract just the product name
                if "•" in link_text:
                    parts = link_text.split("•")
                    link_product = parts[0].strip()
                    link_company = parts[1].strip() if len(parts) > 1 else ""
                else:
                    link_product = link_text
                    link_company = ""
                
                # Check if product matches
                if nk(link_product) == nk_product:
                    # If company is in the link text, verify it matches
                    if link_company and nk(link_company) != nk_company:
                        continue  # Wrong company
                    
                    # Also check for company in sibling element (a.rlab)
                    if not link_company:
                        try:
                            rlab_elements = link.find_elements(By.XPATH, "following-sibling::a[contains(@class,'rlab')][1]")
                            if rlab_elements:
                                comp_text = normalize_ws(rlab_elements[0].text)
                                if nk(comp_text) != nk_company:
                                    continue  # Wrong company
                        except:
                            pass  # Company verification failed, but product matches
                    
                    log.info(f"[PRODUCT_CLICK] Found exact match: '{link_product}' (full text: '{link_text}')")
                    
                    # Get the form reference from href
                    href = link.get_attribute("href") or ""
                    m = re.search(r"document\.(pr\d+)\.submit", href)
                    if m:
                        pr_form = m.group(1)
                        log.info(f"[PRODUCT_CLICK] Submitting form: {pr_form}")
                        driver.execute_script(f"if (document.{pr_form}) document.{pr_form}.submit();")
                    else:
                        log.info(f"[PRODUCT_CLICK] Clicking link directly")
                        try:
                            link.click()
                        except:
                            driver.execute_script("arguments[0].click();", link)
                    
                    rate_limit_pause()
                    
                    # Wait for product detail page to load
                    WebDriverWait(driver, WAIT_PAGE_LOAD).until(
                        lambda d: d.find_elements(By.CSS_SELECTOR, "tr.lproducto span.tproducto") or
                                  d.find_elements(By.CSS_SELECTOR, "td.dproducto > table.presentacion")
                    )
                    log.info("[PRODUCT_CLICK] Product detail page loaded")
                    return True
            except StaleElementReferenceException:
                continue
        
        # Log available products for debugging
        log.warning(f"[PRODUCT_CLICK] No exact match for '{product_name}' (normalized: '{nk_product}')")
        log.warning(f"[PRODUCT_CLICK] Available products:")
        for i, link in enumerate(product_links[:15]):
            try:
                link_text = normalize_ws(link.text)
                # Extract product name from "PRODUCT • Company" format
                if "•" in link_text:
                    link_product = link_text.split("•")[0].strip()
                else:
                    link_product = link_text
                log.warning(f"  [{i+1}] '{link_product}' (normalized: '{nk(link_product)}') | full: '{link_text}'")
            except:
                pass
        if len(product_links) > 15:
            log.warning(f"  ... and {len(product_links) - 15} more")
        
        return False
        
    except TimeoutException:
        log.error(f"[PRODUCT_CLICK] Timeout waiting for product detail page")
        return False
    except Exception as e:
        log.error(f"[PRODUCT_CLICK] Error clicking product: {e}")
        import traceback
        log.error(traceback.format_exc())
        return False


def get_text_safe(root, css):
    """Safely get text from element, checking presence before fetching values."""
    try:
        elements = root.find_elements(By.CSS_SELECTOR, css)
        if not elements:
            return None
        el = elements[0]
        try:
            _ = el.is_displayed()
        except StaleElementReferenceException:
            return None
        text = el.text
        return normalize_ws(text) if text else None
    except Exception:
        return None


def ar_money_to_float(s: str):
    """Convert Argentine money format to float: '$ 1.234,56' -> 1234.56"""
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


def parse_date(s: str):
    """Parse date: '(24/07/25)' or '24/07/25' -> '2025-07-24'"""
    from datetime import datetime
    s = (s or "").strip()
    m = re.search(r"\((\d{2})/(\d{2})/(\d{2})\)", s) or re.search(r"\b(\d{2})/(\d{2})/(\d{2})\b", s)
    if m:
        d, mn, y = map(int, m.groups())
        y += 2000
        try:
            return datetime(y, mn, d).date().isoformat()
        except:
            return None
    return None


def collect_coverage(pres_el) -> Dict[str, Any]:
    """Extract coverage info (SIFAR, PAMI, IOMA) from presentation element."""
    cov: Dict[str, Any] = {}
    try:
        cob_elements = pres_el.find_elements(By.CSS_SELECTOR, "table.coberturas")
        if not cob_elements:
            return cov
        
        cob = cob_elements[0]
        
        # Get all rows in coverage table
        rows = cob.find_elements(By.CSS_SELECTOR, "tr")
        for row in rows:
            try:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 2:
                    payer_text = normalize_ws(cells[0].text or "").upper()
                    detail_text = normalize_ws(cells[1].text or "")
                    
                    # Normalize payer names
                    if "SIFAR" in payer_text:
                        cov["SIFAR"] = {"detail": detail_text}
                    elif "PAMI" in payer_text:
                        pami_data = {"detail": detail_text}
                        # Try to extract AF/OS values
                        try:
                            af_elem = cells[1].find_elements(By.CSS_SELECTOR, "b")
                            if af_elem:
                                af_text = normalize_ws(af_elem[0].text or "")
                                if af_text:
                                    pami_data["AF"] = af_text
                        except:
                            pass
                        cov["PAMI"] = pami_data
                    elif "IOMA" in payer_text:
                        ioma_data = {"detail": detail_text}
                        try:
                            af_elem = cells[1].find_elements(By.CSS_SELECTOR, "b")
                            if af_elem:
                                af_text = normalize_ws(af_elem[0].text or "")
                                if af_text:
                                    ioma_data["AF"] = af_text
                        except:
                            pass
                        cov["IOMA"] = ioma_data
            except:
                continue
    except Exception as e:
        log.debug(f"[COVERAGE] Error extracting coverage: {e}")
    
    return cov


def extract_product_rows(driver, input_company: str, input_product: str) -> List[Dict]:
    """
    Extract product data from the product detail page.
    Uses same CSS selectors as main selenium worker (03_alfabeta_selenium_worker.py).
    """
    rows = []
    
    try:
        # Header/meta from the product page - same selectors as main worker
        active = get_text_safe(driver, "tr.sproducto td.textoe i")
        therap = get_text_safe(driver, "tr.sproducto td.textor i")
        comp = get_text_safe(driver, "tr.lproducto td.textor .defecto") or \
               get_text_safe(driver, "td.textoe b")
        pname = get_text_safe(driver, "tr.lproducto span.tproducto")
        
        log.info(f"[EXTRACT] Product: {pname}, Company: {comp}, Active: {active}, Therapeutic: {therap}")
        
        # Get presentations (multiple rows per product)
        presentations = driver.find_elements(By.CSS_SELECTOR, "td.dproducto > table.presentacion")
        log.info(f"[EXTRACT] Found {len(presentations)} presentation table(s)")
        
        for idx, pres in enumerate(presentations):
            try:
                # Verify element is still accessible
                try:
                    _ = pres.is_displayed()
                except StaleElementReferenceException:
                    log.warning(f"[EXTRACT] Presentation {idx} is stale, skipping...")
                    continue
                
                # Extract data using same selectors as main worker
                desc = get_text_safe(pres, "td.tddesc")
                price = get_text_safe(pres, "td.tdprecio")
                datev = get_text_safe(pres, "td.tdfecha")
                import_status = get_text_safe(pres, "td.import")
                cov = collect_coverage(pres)
                
                ts_now = time.strftime("%Y-%m-%d %H:%M:%S")
                
                row = {
                    "input_company": input_company,
                    "input_product_name": input_product,
                    "company": comp,
                    "product_name": pname,
                    "active_ingredient": active,
                    "therapeutic_class": therap,
                    "description": desc,
                    "price_ars": ar_money_to_float(price or ""),
                    "date": parse_date(datev or ""),
                    "scraped_at": ts_now,
                    "SIFAR_detail": (cov.get("SIFAR") or {}).get("detail"),
                    "PAMI_AF": (cov.get("PAMI") or {}).get("AF"),
                    "PAMI_OS": (cov.get("PAMI") or {}).get("OS"),
                    "IOMA_detail": (cov.get("IOMA") or {}).get("detail"),
                    "IOMA_AF": (cov.get("IOMA") or {}).get("AF"),
                    "IOMA_OS": (cov.get("IOMA") or {}).get("OS"),
                    "import_status": import_status,
                    "coverage_json": json.dumps(cov, ensure_ascii=False) if cov else "{}",
                    "source": "selenium_company",  # Mark as company search source
                }
                rows.append(row)
                log.debug(f"[EXTRACT] Row {idx}: {desc} | {price} | {datev}")
                
            except Exception as e:
                log.warning(f"[EXTRACT] Error extracting presentation {idx}: {e}")
                continue
        
        if rows:
            log.info(f"[EXTRACT] Extracted {len(rows)} presentations for {input_company} | {input_product}")
        elif pname or comp:
            # Create fallback row if we have product info but no presentations
            log.info(f"[EXTRACT] No presentations found, creating fallback row for {input_company} | {input_product}")
            rows.append({
                "input_company": input_company,
                "input_product_name": input_product,
                "company": comp,
                "product_name": pname,
                "active_ingredient": active,
                "therapeutic_class": therap,
                "description": None,
                "price_ars": None,
                "date": None,
                "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "SIFAR_detail": None, "PAMI_AF": None, "PAMI_OS": None,
                "IOMA_detail": None, "IOMA_AF": None, "IOMA_OS": None,
                "import_status": None,
                "coverage_json": "{}",
                "source": "selenium_company",
            })
        else:
            log.warning(f"[EXTRACT] No data found for {input_company} | {input_product}")
        
    except Exception as e:
        log.error(f"[EXTRACT] Error extracting product data: {e}")
        import traceback
        log.error(traceback.format_exc())
    
    return rows


def get_zero_record_products(repo: ArgentinaRepository, limit: int = 1000) -> List[Tuple[str, str]]:
    """
    Get products with total_records=0 that haven't been scraped by company search yet.
    """
    with repo.db.cursor() as cur:
        cur.execute(
            """
            SELECT company, product
            FROM ar_product_index
            WHERE run_id = %s
              AND total_records = 0
              AND (scrape_source IS NULL OR scrape_source NOT LIKE '%%company%%')
              AND url IS NOT NULL AND url <> ''
            ORDER BY company, product
            LIMIT %s
            """,
            (repo.run_id, limit),
        )
        return [(r[0], r[1]) for r in cur.fetchall()]


def update_scrape_source(repo: ArgentinaRepository, company: str, product: str, source: str, records: int):
    """Update the scrape_source and total_records for a product."""
    with repo.db.cursor() as cur:
        cur.execute(
            """
            UPDATE ar_product_index
            SET scrape_source = %s,
                total_records = %s,
                status = CASE WHEN %s > 0 THEN 'completed' ELSE status END,
                updated_at = CURRENT_TIMESTAMP
            WHERE run_id = %s AND company = %s AND product = %s
            """,
            (source, records, records, repo.run_id, company, product),
        )
    try:
        repo.db.commit()
    except:
        pass


def mark_company_search_attempted(repo: ArgentinaRepository, company: str, product: str):
    """Mark that company search was attempted for this product (even if it failed)."""
    with repo.db.cursor() as cur:
        # Append '_company_attempted' to scrape_source if not already there
        cur.execute(
            """
            UPDATE ar_product_index
            SET scrape_source = CASE 
                    WHEN scrape_source IS NULL THEN 'company_attempted'
                    WHEN scrape_source LIKE '%%company%%' THEN scrape_source
                    ELSE scrape_source || '_company_attempted'
                END,
                updated_at = CURRENT_TIMESTAMP
            WHERE run_id = %s AND company = %s AND product = %s
            """,
            (repo.run_id, company, product),
        )
    try:
        repo.db.commit()
    except:
        pass


def process_product_company_search(driver, company: str, product: str, repo: ArgentinaRepository) -> int:
    """
    Process a single product using company search strategy.
    
    Handles the case where multiple companies have the same name by trying each one
    until the product is found.
    
    Returns number of rows extracted.
    """
    log.info(f"[PROCESS] Starting company search for: {company} | {product}")
    
    try:
        # Step 1: Navigate to products page
        if not navigate_to_products_page(driver):
            log.error(f"[PROCESS] Failed to navigate to products page")
            mark_company_search_attempted(repo, company, product)  # Mark as attempted
            return 0
        
        # Step 2: Search by company name
        if not search_company_on_page(driver, company):
            log.warning(f"[PROCESS] Company search failed for: {company}")
            mark_company_search_attempted(repo, company, product)  # Mark as attempted
            return 0
        
        # Step 3: Get all matching company links (there may be multiple with same name)
        matching_links = get_matching_company_links(driver, company)
        
        if not matching_links:
            log.warning(f"[PROCESS] No matching company links found for: {company}")
            mark_company_search_attempted(repo, company, product)  # Mark as attempted
            return 0
        
        log.info(f"[PROCESS] Found {len(matching_links)} company link(s) for '{company}'")
        
        # Step 4: Try each matching company link until we find the product
        for attempt, (link_idx, link_text) in enumerate(matching_links, 1):
            log.info(f"[PROCESS] Trying company link {attempt}/{len(matching_links)}: '{link_text}' (index {link_idx})")
            
            # If not the first attempt, we need to go back and search again
            if attempt > 1:
                log.info(f"[PROCESS] Navigating back to search for next company match...")
                if not navigate_to_products_page(driver):
                    log.error(f"[PROCESS] Failed to navigate back to products page")
                    continue
                if not search_company_on_page(driver, company):
                    log.warning(f"[PROCESS] Company search failed on retry")
                    continue
                # Re-get the matching links (page refreshed)
                new_matching_links = get_matching_company_links(driver, company)
                if attempt - 1 >= len(new_matching_links):
                    log.warning(f"[PROCESS] Link index out of range after refresh")
                    continue
                link_idx, link_text = new_matching_links[attempt - 1]
            
            # Click on this company link
            if not click_company_link_by_index(driver, company, link_idx):
                log.warning(f"[PROCESS] Failed to click company link at index {link_idx}")
                continue
            
            # Try to find the product under this company
            if find_and_click_product(driver, product, company):
                # Found the product! Extract data
                rows = extract_product_rows(driver, company, product)
                
                if rows:
                    # Save to database
                    saved = append_rows(rows, source="selenium_company")
                    if saved:
                        log.info(f"[PROCESS] Saved {len(rows)} rows for {company} | {product}")
                        # Update tracking - success!
                        update_scrape_source(repo, company, product, "selenium_company", len(rows))
                        append_progress(company, product, len(rows), source="selenium_company")
                        return len(rows)
            else:
                log.info(f"[PROCESS] Product '{product}' not found under company link {attempt}, trying next...")
        
        # Exhausted all company links without finding the product
        log.warning(f"[PROCESS] Product '{product}' not found under any of {len(matching_links)} company link(s) for '{company}'")
        mark_company_search_attempted(repo, company, product)  # Mark as attempted (failed)
        return 0
        
    except Exception as e:
        log.error(f"[PROCESS] Error processing {company} | {product}: {e}")
        append_error(company, product, f"Company search error: {e}")
        mark_company_search_attempted(repo, company, product)  # Mark as attempted (error)
        return 0


def main():
    global _REPO, _DB
    
    parser = argparse.ArgumentParser(description="Argentina Selenium Company Search Scraper")
    parser.add_argument("--headless", action="store_true", dest="headless", help="Run in headless mode (default)")
    parser.add_argument("--no-headless", action="store_false", dest="headless", help="Run with visible browser")
    parser.set_defaults(headless=True)
    parser.add_argument("--limit", type=int, default=500, help="Max products to process")
    args = parser.parse_args()
    
    output_dir = get_output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)
    
    run_id = _get_run_id(output_dir)
    log.info(f"[INIT] Starting company search scraper for run_id: {run_id}")
    
    # Initialize database
    _DB = CountryDB("Argentina")
    apply_argentina_schema(_DB)
    _REPO = ArgentinaRepository(_DB, run_id)
    
    # Get products with zero records
    products = get_zero_record_products(_REPO, limit=args.limit)
    
    if not products:
        log.info("[INIT] No products with zero records found. Nothing to do.")
        print("[COMPANY_SEARCH] No products need company search (all have data or already tried).")
        return
    
    log.info(f"[INIT] Found {len(products)} products with zero records to process")
    print(f"[COMPANY_SEARCH] Processing {len(products)} products with company search strategy...")
    
    # Group by company for efficiency
    from collections import defaultdict
    company_products = defaultdict(list)
    for company, product in products:
        company_products[company].append(product)
    
    log.info(f"[INIT] Products grouped into {len(company_products)} companies")
    
    # Initialize Tor IP for per-product IP guard
    global _current_tor_ip
    tor_running, tor_port = check_tor_running()
    if tor_running and tor_port:
        try:
            _current_tor_ip = get_public_ip_via_socks("127.0.0.1", tor_port)
            if _current_tor_ip:
                log.info(f"[TOR_IP_GUARD] Initial Tor exit IP: {_current_tor_ip}")
                try:
                    _REPO.snapshot_scrape_stats('session_start', _current_tor_ip)
                except Exception:
                    pass
        except Exception:
            pass

    # Create driver
    driver = None
    total_scraped = 0
    total_processed = 0
    ip_skipped = 0

    try:
        browser_mode = "HEADLESS" if args.headless else "VISIBLE"
        log.info(f"[BROWSER] Running in {browser_mode} mode")
        print(f"[BROWSER] Browser will be {'hidden' if args.headless else 'visible'} during company search")
        driver = create_firefox_driver(headless=args.headless)

        for company, product_list in company_products.items():
            if _shutdown_requested.is_set():
                break

            log.info(f"[BATCH] Processing company: {company} ({len(product_list)} products)")

            for product in product_list:
                if _shutdown_requested.is_set():
                    break

                # Tor IP guard: skip product if current Tor IP is same as last attempt
                if _current_tor_ip:
                    ip_key = (nk(company), nk(product))
                    last_ip = _product_last_ip.get(ip_key)
                    if last_ip and _current_tor_ip == last_ip:
                        log.info(f"[TOR_IP_GUARD] Skipping {product} | {company} — same Tor IP ({_current_tor_ip}), will retry after rotation")
                        ip_skipped += 1
                        continue

                total_processed += 1
                # Record the Tor IP used for this product
                if _current_tor_ip:
                    ip_key = (nk(company), nk(product))
                    _product_last_ip[ip_key] = _current_tor_ip
                rows = process_product_company_search(driver, company, product, _REPO)
                if rows > 0:
                    total_scraped += rows

                # Progress update with percentage
                if total_processed % 10 == 0 or total_processed == len(products):
                    percent = (total_processed / len(products) * 100) if len(products) > 0 else 0
                    log.info(f"[PROGRESS] Processed {total_processed}/{len(products)} ({percent:.1f}%), scraped {total_scraped} rows")
                    print(f"[PROGRESS] Company Search: {total_processed}/{len(products)} ({percent:.1f}%) - {total_scraped} rows scraped", flush=True)

                    # Update scrape stats snapshot
                    try:
                        _REPO.snapshot_scrape_stats('company_search_progress', _current_tor_ip)
                    except Exception:
                        pass
    
    except KeyboardInterrupt:
        log.warning("[SHUTDOWN] Keyboard interrupt received")
        _shutdown_requested.set()
    except Exception as e:
        log.error(f"[ERROR] Unexpected error: {e}")
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass
    
    # Summary
    print("=" * 80)
    print(f"Company Search Complete")
    print(f"  Products processed: {total_processed}")
    print(f"  Rows scraped: {total_scraped}")
    print(f"  IP guard skips: {ip_skipped}")
    print("=" * 80)
    log.info(f"[DONE] Company search complete. Processed: {total_processed}, Scraped: {total_scraped}, IP skips: {ip_skipped}")

    # Final stats snapshot at session end
    try:
        _REPO.snapshot_scrape_stats('session_end', _current_tor_ip)
        log.info("[STATS] Final scrape stats snapshot captured")
    except Exception as e:
        log.warning(f"[STATS] Failed to capture final stats snapshot: {e}")


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    log.warning(f"[SIGNAL] Received signal {signum}, initiating shutdown...")
    _shutdown_requested.set()


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    main()
