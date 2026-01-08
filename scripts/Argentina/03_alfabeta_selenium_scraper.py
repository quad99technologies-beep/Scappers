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

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    requests = None
    REQUESTS_AVAILABLE = False

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
    get_input_dir, get_output_dir, get_accounts,
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
    nk, ts, strip_accents, OUT_FIELDS, update_prepared_urls_source
)

# ====== LOGGING ======
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("selenium_scraper")

# ====== SHUTDOWN HANDLING ======
_shutdown_requested = threading.Event()
_active_drivers = []
_drivers_lock = threading.Lock()
_captcha_login_detected = threading.Event()  # Flag to track if any thread detected captcha/login
_skip_lock = threading.Lock()  # Lock for updating skip_set during runtime

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
        driver_count = len(_active_drivers)
        log.info(f"[SHUTDOWN] Closing {driver_count} Chrome session(s)...")
        for driver in _active_drivers[:]:  # Copy list to avoid modification during iteration
            try:
                driver.quit()
            except Exception as e:
                # Only log if it's not a "session not found" type error (expected after quit)
                error_msg = str(e).lower()
                if "session" not in error_msg and "connection" not in error_msg and "target window" not in error_msg:
                    log.warning(f"[SHUTDOWN] Error closing driver: {e}")
            # Don't call driver.close() after quit() - it causes noisy connection errors
        _active_drivers.clear()
        
        # Only kill Chrome processes if we had active drivers (less aggressive)
        if driver_count > 0:
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

def get_vpn_info() -> dict:
    """Get detailed VPN connection information.
    Returns dict with VPN details: server, country, ip, etc.
    """
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
                pass  # Try next method
        
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
                            
                            # Extract IP
                            vpn_info["ip"] = ip_info.get("ip") or ip_info.get("query") or "Unknown"
                            
                            # Extract country
                            vpn_info["country"] = ip_info.get("country_name") or ip_info.get("country") or "Unknown"
                            
                            # Extract city
                            vpn_info["city"] = ip_info.get("city") or "Unknown"
                            
                            # Extract provider/server from org
                            org = ip_info.get("org") or ip_info.get("isp") or ""
                            if "proton" in org.lower():
                                vpn_info["provider"] = "Proton VPN"
                                # Try to extract server name from org
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
    """Check if VPN (Proton VPN) is connected and working.
    Returns True if VPN is connected, False otherwise.
    Displays VPN connection details in console.
    """
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
                        # Display VPN details
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
                pass  # Try next method
        
        # Method 2: Check IP address and location (verify VPN is working)
        if not REQUESTS_AVAILABLE:
            log.warning("[VPN_CHECK] requests library not available, skipping IP check")
            # Ask user to confirm VPN is connected
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
        try:
            vpn_info = get_vpn_info()
            
            if vpn_info["connected"]:
                # Display VPN details
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
                
                # Verify we have a valid IP (not localhost/private)
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
            log.warning(f"[VPN_CHECK] IP check failed: {e}, assuming VPN is connected")
            return True  # Assume connected if check fails
        
    except Exception as e:
        log.error(f"[VPN_CHECK] VPN check failed: {e}")
        log.warning("[VPN_CHECK] Cannot verify VPN connection. Please ensure VPN is connected before proceeding.")
        # Ask user to confirm
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
    
    # Set page load strategy to "eager" to avoid hanging on slow-loading resources
    # We'll add explicit waits after navigation to ensure content is loaded
    opts.set_capability("pageLoadStrategy", "eager")
    
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
    log.info(f"[SEARCH] Starting search for product: {product_term}")
    log.info(f"[SEARCH] Navigating to: {PRODUCTS_URL}")
    
    # Navigation watchdog: if driver.get() hangs, we'll detect it
    navigation_start = time.time()
    navigation_timeout = PAGE_LOAD_TIMEOUT + 10  # Give extra time beyond page load timeout
    
    try:
        log.info(f"[SEARCH] Calling driver.get() with URL: {PRODUCTS_URL}")
        log.info(f"[SEARCH] Navigation timeout: {navigation_timeout}s")
        
        # Use threading to detect if navigation hangs
        navigation_complete = threading.Event()
        navigation_error = [None]
        
        def do_navigation():
            try:
                driver.get(PRODUCTS_URL)
                navigation_complete.set()
            except Exception as e:
                navigation_error[0] = e
                navigation_complete.set()
        
        nav_thread = threading.Thread(target=do_navigation, daemon=True)
        nav_thread.start()
        
        # Wait for navigation with timeout
        if navigation_complete.wait(timeout=navigation_timeout):
            if navigation_error[0]:
                raise navigation_error[0]
            elapsed = time.time() - navigation_start
            log.info(f"[SEARCH] driver.get() completed in {elapsed:.2f}s. Current URL: {driver.current_url}")
        else:
            # Navigation hung - log and raise
            elapsed = time.time() - navigation_start
            log.error(f"[SEARCH] Navigation hung after {elapsed:.2f}s (timeout: {navigation_timeout}s)")
            log.error(f"[SEARCH] Target URL: {PRODUCTS_URL}")
            try:
                log.error(f"[SEARCH] Driver current URL: {driver.current_url}")
                log.error(f"[SEARCH] Driver page title: {driver.title}")
            except:
                log.error("[SEARCH] Could not get driver info - driver may be unresponsive")
            raise TimeoutException(f"Navigation to {PRODUCTS_URL} hung after {elapsed:.2f}s")
        
        log.info(f"[SEARCH] Page title: {driver.title}")
        
        # Print first 300 chars of page HTML for debugging
        try:
            page_source_preview = driver.page_source[:300]
            log.info(f"[SEARCH] Page HTML preview (first 300 chars): {page_source_preview}")
        except Exception as e:
            log.warning(f"[SEARCH] Could not get page source preview: {e}")
        
        # Wait for page to stabilize and JavaScript to execute
        time.sleep(2)
        log.info(f"[SEARCH] After 2s wait. Current URL: {driver.current_url}")
        
        # Wait for document ready state to ensure JavaScript has executed
        try:
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            log.info("[SEARCH] Document ready state: complete")
        except Exception as e:
            log.warning(f"[SEARCH] Document ready state check failed: {e}, continuing anyway")
    except TimeoutException as te:
        log.error(f"[SEARCH] Navigation timeout: {te}")
        raise
    except Exception as e:
        log.error(f"[SEARCH] Failed to navigate to {PRODUCTS_URL}: {e}")
        log.error(f"[SEARCH] Exception type: {type(e).__name__}")
        import traceback
        log.error(f"[SEARCH] Traceback: {traceback.format_exc()}")
        if driver:
            try:
                log.error(f"[SEARCH] Driver current URL: {driver.current_url}")
                log.error(f"[SEARCH] Driver page title: {driver.title}")
            except:
                log.error("[SEARCH] Could not get driver info")
        raise
    
    # Check for login page after navigation
    if is_login_page(driver):
        log.error("[SEARCH] Login page detected after navigating to products URL")
        raise RuntimeError("Login page detected after navigating to products URL")
    
    log.info(f"[SEARCH] Waiting for search form (timeout: {WAIT_SEARCH_FORM}s)...")
    try:
        form = WebDriverWait(driver, WAIT_SEARCH_FORM).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "form#srvPr"))
        )
        log.info("[SEARCH] Search form found")
    except TimeoutException:
        # Check again for login page in case it appeared during wait
        if is_login_page(driver):
            log.error("[SEARCH] Login page detected while waiting for search form")
            raise RuntimeError("Login page detected while waiting for search form")
        log.error(f"[SEARCH] Form not found after {WAIT_SEARCH_FORM}s. Current URL: {driver.current_url}")
        log.error(f"[SEARCH] Page title: {driver.title}")
        log.error(f"[SEARCH] Page source snippet: {driver.page_source[:500]}")
        raise
    
    log.info(f"[SEARCH] Entering search term: {product_term}")
    box = form.find_element(By.NAME, "patron")
    box.clear()
    box.send_keys(product_term)
    box.send_keys(Keys.ENTER)
    log.info(f"[SEARCH] Search submitted, waiting for results (timeout: {WAIT_SEARCH_RESULTS}s)...")
    
    try:
        # Wait for search results to appear
        WebDriverWait(driver, WAIT_SEARCH_RESULTS).until(
            lambda d: d.find_elements(By.CSS_SELECTOR, "a.rprod, form[name^='pr']")
        )
        log.info("[SEARCH] Search results loaded successfully")
        
        # Additional wait to ensure all dynamic content is rendered
        time.sleep(2)
        log.info("[SEARCH] Waiting 2s for dynamic content to fully render...")
        
        # Verify results are still present and page is stable
        results = driver.find_elements(By.CSS_SELECTOR, "a.rprod, form[name^='pr']")
        if results:
            log.info(f"[SEARCH] Confirmed {len(results)} result elements are present")
        else:
            log.warning("[SEARCH] Results disappeared after wait, may need more time")
    except TimeoutException:
        log.error(f"[SEARCH] Search results not found after {WAIT_SEARCH_RESULTS}s. Current URL: {driver.current_url}")
        log.error(f"[SEARCH] Page title: {driver.title}")
        raise

def enumerate_pairs(driver) -> List[Dict[str, Any]]:
    out = []
    for a in driver.find_elements(By.CSS_SELECTOR, "a.rprod"):
        # Check if element is still present and accessible before fetching values
        try:
            # Verify element is still attached to DOM
            _ = a.is_displayed()
            prod_txt = normalize_ws(a.text) or ""
            href = a.get_attribute("href") or ""
        except Exception:
            # Element may be stale or not accessible, skip it
            continue
        
        m = re.search(r"document\.(pr\d+)\.submit", href)
        pr_form = m.group(1) if m else None
        comp_txt = ""
        
        # Check for company label before fetching
        rlab_elements = a.find_elements(By.XPATH, "following-sibling::a[contains(@class,'rlab')][1]")
        if rlab_elements:
            try:
                rlab = rlab_elements[0]
                _ = rlab.is_displayed()  # Check presence before fetching
                comp_txt = normalize_ws(rlab.text) or ""
            except Exception:
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
    
    # Wait for product page to load
    WebDriverWait(driver, WAIT_PAGE_LOAD).until(
        lambda d: "presentacion" in d.page_source.lower() or d.find_elements(By.CSS_SELECTOR, "tr.lproducto span.tproducto")
    )
    
    # Additional wait to ensure all content is fully loaded
    time.sleep(2)
    log.info("[OPEN_PAIR] Waiting 2s for product page content to fully load...")
    
    # Wait for document ready state
    try:
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        log.info("[OPEN_PAIR] Document ready state: complete")
    except Exception as e:
        log.warning(f"[OPEN_PAIR] Document ready state check failed: {e}, continuing anyway")
    
    # Verify key elements are present
    try:
        pres_elements = driver.find_elements(By.CSS_SELECTOR, "td.dproducto > table.presentacion")
        if pres_elements:
            log.info(f"[OPEN_PAIR] Confirmed {len(pres_elements)} presentation table(s) are present")
        else:
            log.warning("[OPEN_PAIR] No presentation tables found, content may not be fully loaded")
    except Exception as e:
        log.warning(f"[OPEN_PAIR] Could not verify presentation tables: {e}")
    
    return True

# ====== PRODUCT PAGE PARSING ======

def get_text_safe(root, css):
    """Safely get text from element, checking presence before fetching values"""
    try:
        # Check if element exists before fetching
        elements = root.find_elements(By.CSS_SELECTOR, css)
        if not elements:
            return None
        
        el = elements[0]
        # Verify element is still attached and accessible
        _ = el.is_displayed()
        
        # Now fetch values
        txt = el.get_attribute("innerText")
        if not txt:
            txt = el.get_attribute("innerHTML")
        return normalize_ws(txt)
    except Exception:
        return None

def collect_coverage(pres_el) -> Dict[str, Any]:
    """Robust coverage parser: normalizes payer keys and reads innerHTML to catch AF/OS in <b> tags.
    Checks element presence before fetching values."""
    cov: Dict[str, Any] = {}
    
    # Check if coverage table exists before accessing
    cob_elements = pres_el.find_elements(By.CSS_SELECTOR, "table.coberturas")
    if not cob_elements:
        return cov
    
    try:
        cob = cob_elements[0]
        _ = cob.is_displayed()  # Verify element is accessible
    except Exception:
        return cov

    current_payer = None
    for tr in cob.find_elements(By.CSS_SELECTOR, "tr"):
        # Payer name (fallback to innerHTML) - check presence before fetching
        payer_elements = tr.find_elements(By.CSS_SELECTOR, "td.obrasn")
        if payer_elements:
            try:
                payer_el = payer_elements[0]
                _ = payer_el.is_displayed()  # Check presence before fetching
                payer_text = normalize_ws(payer_el.get_attribute("innerText")) or normalize_ws(payer_el.get_attribute("innerHTML"))
                if payer_text:
                    current_payer = strip_accents(payer_text).upper()
                    cov.setdefault(current_payer, {})
            except Exception:
                pass

        # Detail/description - check presence before fetching
        detail_elements = tr.find_elements(By.CSS_SELECTOR, "td.obrasd")
        if detail_elements:
            try:
                detail_el = detail_elements[0]
                _ = detail_el.is_displayed()  # Check presence before fetching
                detail = normalize_ws(detail_el.get_attribute("innerText"))
                if current_payer and detail:
                    cov[current_payer]["detail"] = detail
            except Exception:
                pass

        # Amounts: check both left/right amount cells, use innerText first
        for sel in ("td.importesi", "td.importesd"):
            amount_elements = tr.find_elements(By.CSS_SELECTOR, sel)
            if amount_elements:
                try:
                    amount_el = amount_elements[0]
                    _ = amount_el.is_displayed()  # Check presence before fetching
                    txt = amount_el.get_attribute("innerText")
                    if not txt:
                        txt = amount_el.get_attribute("innerHTML")
                        txt = re.sub(r'<[^>]*>', '', txt)
                    for tag, amt in re.findall(r"(AF|OS)[^<]*?[\$]?([\d\.,]+)", txt or "", flags=re.I):
                        val = ar_money_to_float(amt)
                        if val is not None and current_payer:
                            cov[current_payer][tag.upper()] = val
                except Exception:
                    pass
    return cov

def extract_rows(driver, in_company, in_product):
    # Ensure page is fully loaded before parsing
    try:
        # Wait for document ready state
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        # Additional small wait for any remaining dynamic content
        time.sleep(1)
    except Exception as e:
        log.warning(f"[EXTRACT] Document ready state check failed: {e}, continuing anyway")
    
    # Header/meta from the product page - get_text_safe now checks presence before fetching
    active = get_text_safe(driver, "tr.sproducto td.textoe i")
    therap = get_text_safe(driver, "tr.sproducto td.textor i")
    comp = get_text_safe(driver, "tr.lproducto td.textor .defecto") or \
           get_text_safe(driver, "td.textoe b")
    pname = get_text_safe(driver, "tr.lproducto span.tproducto")

    rows: List[Dict[str, Any]] = []
    # Check if presentation elements exist before iterating
    pres = driver.find_elements(By.CSS_SELECTOR, "td.dproducto > table.presentacion")
    for p in pres:
        # Verify element is still accessible before processing
        try:
            _ = p.is_displayed()
        except Exception:
            # Element may be stale, skip it
            continue
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

# ====== VPN ROTATION ======

_vpn_rotation_lock = threading.Lock()
_last_vpn_change_time = {}  # thread_id -> timestamp
_vpn_change_interval = 600  # 10 minutes in seconds

def rotate_vpn(thread_id: int, reason: str = "scheduled"):
    """Rotate Proton VPN connection.
    Tries multiple methods:
    1. Proton VPN CLI (Linux/Mac)
    2. Windows COM automation (if Proton VPN app is running)
    3. Manual prompt (fallback)
    Displays VPN rotation details in console.
    """
    with _vpn_rotation_lock:
        now = time.time()
        last_change = _last_vpn_change_time.get(thread_id, 0)
        
        # Check if enough time has passed since last change (unless forced)
        if reason != "forced" and (now - last_change) < 60:  # Minimum 60 seconds between changes
            log.info(f"[VPN] Skipping VPN rotation (too soon since last change)")
            return
        
        print("\n" + "=" * 80)
        print(f"[VPN_ROTATION] Rotating VPN Connection")
        print("=" * 80)
        print(f"  Reason: {reason}")
        print(f"  Thread ID: {thread_id}")
        print("-" * 80)
        log.warning(f"[VPN] Rotating VPN connection (reason: {reason})...")
        _last_vpn_change_time[thread_id] = now
        
        # Get current VPN info before rotation
        old_vpn_info = get_vpn_info()
        if old_vpn_info["connected"]:
            print(f"  Current VPN: {old_vpn_info['server']} ({old_vpn_info['country']})")
            print(f"  Current IP: {old_vpn_info['ip']}")
            print("-" * 80)
        
        # Try Proton VPN CLI first (Linux/Mac)
        try:
            import subprocess
            import platform
            
            if platform.system() != "Windows":
                # Try protonvpn-cli
                print("  Attempting to connect via Proton VPN CLI...")
                result = subprocess.run(
                    ["protonvpn-cli", "connect", "--random"],
                    capture_output=True,
                    timeout=30,
                    text=True
                )
                if result.returncode == 0:
                    print("  [OK] VPN rotation successful via Proton VPN CLI")
                    time.sleep(5)  # Wait for connection to stabilize
                    
                    # Get new VPN info
                    new_vpn_info = get_vpn_info()
                    if new_vpn_info["connected"]:
                        print("\n[VPN_STATUS] [OK] NEW VPN CONNECTED")
                        print("-" * 80)
                        print(f"  Provider: {new_vpn_info['provider']}")
                        if new_vpn_info['server'] != "Unknown":
                            print(f"  Server: {new_vpn_info['server']}")
                        print(f"  IP Address: {new_vpn_info['ip']}")
                        print(f"  Location: {new_vpn_info['city']}, {new_vpn_info['country']}")
                        print("-" * 80)
                        log.info(f"[VPN] Successfully rotated VPN - New Server: {new_vpn_info['server']}, IP: {new_vpn_info['ip']}, Location: {new_vpn_info['city']}, {new_vpn_info['country']}")
                    else:
                        log.info("[VPN] Successfully rotated VPN using protonvpn-cli")
                    print("=" * 80 + "\n")
                    return
                else:
                    print(f"  [FAIL] Proton VPN CLI failed: {result.stderr}")
        except Exception as e:
            log.debug(f"[VPN] Proton VPN CLI not available: {e}")
        
        # Try Windows automation methods
        try:
            import platform
            import subprocess
            import os
            
            if platform.system() == "Windows":
                print("  Attempting Windows VPN automation...")
                
                # Method 1: Try Proton VPN CLI in common installation paths
                protonvpn_paths = [
                    os.path.expanduser(r"~\AppData\Local\Programs\ProtonVPN\protonvpn-cli.exe"),
                    r"C:\Program Files\ProtonVPN\protonvpn-cli.exe",
                    r"C:\Program Files (x86)\ProtonVPN\protonvpn-cli.exe",
                ]
                
                for cli_path in protonvpn_paths:
                    if os.path.exists(cli_path):
                        try:
                            print(f"  Found Proton VPN CLI at: {cli_path}")
                            result = subprocess.run(
                                [cli_path, "connect", "--random"],
                                capture_output=True,
                                timeout=30,
                                text=True
                            )
                            if result.returncode == 0:
                                print("  [OK] VPN rotation successful via Proton VPN CLI")
                                time.sleep(5)  # Wait for connection to stabilize
                                
                                # Get new VPN info
                                new_vpn_info = get_vpn_info()
                                if new_vpn_info["connected"]:
                                    print("\n[VPN_STATUS] [OK] NEW VPN CONNECTED")
                                    print("-" * 80)
                                    print(f"  Provider: {new_vpn_info['provider']}")
                                    if new_vpn_info['server'] != "Unknown":
                                        print(f"  Server: {new_vpn_info['server']}")
                                    print(f"  IP Address: {new_vpn_info['ip']}")
                                    print(f"  Location: {new_vpn_info['city']}, {new_vpn_info['country']}")
                                    print("-" * 80)
                                    log.info(f"[VPN] Successfully rotated VPN - New Server: {new_vpn_info['server']}, IP: {new_vpn_info['ip']}, Location: {new_vpn_info['city']}, {new_vpn_info['country']}")
                                else:
                                    log.info("[VPN] Successfully rotated VPN using Proton VPN CLI")
                                print("=" * 80 + "\n")
                                return
                            else:
                                print(f"  [FAIL] Proton VPN CLI failed: {result.stderr}")
                        except Exception as e:
                            log.debug(f"[VPN] Failed to use Proton VPN CLI at {cli_path}: {e}")
                            continue
                
                # Method 2: Try PowerShell to interact with Proton VPN (if available)
                try:
                    # Check if Proton VPN process is running
                    result = subprocess.run(
                        ["powershell", "-Command", "Get-Process -Name 'ProtonVPN*' -ErrorAction SilentlyContinue | Select-Object -First 1"],
                        capture_output=True,
                        timeout=5,
                        text=True
                    )
                    if result.returncode == 0 and result.stdout.strip():
                        print("  Proton VPN app is running")
                        print("  Note: Automatic rotation via PowerShell is not available")
                        print("  Please use the Proton VPN app to change servers")
                    else:
                        print("  Proton VPN app is not running")
                except Exception as e:
                    log.debug(f"[VPN] PowerShell check failed: {e}")
                
                # Method 3: Try COM automation (requires pywin32)
                try:
                    import win32com.client
                    print("  Attempting COM automation...")
                    # Proton VPN COM interface is not publicly documented
                    # This is a placeholder for future implementation
                    log.debug("[VPN] COM automation not implemented (Proton VPN COM interface not documented)")
                except ImportError:
                    log.debug("[VPN] pywin32 not available for COM automation")
                except Exception as e:
                    log.debug(f"[VPN] COM automation failed: {e}")
                    
        except Exception as e:
            log.debug(f"[VPN] Windows automation methods failed: {e}")
        
        # Fallback: Manual prompt
        print("  Manual VPN rotation required")
        print("-" * 80)
        print("  Please change your Proton VPN location manually")
        print("  After changing VPN, press ENTER to continue...")
        print("=" * 80)
        log.warning("[VPN] Please change your Proton VPN location manually")
        log.warning(f"[VPN] Reason: {reason}")
        log.warning("[VPN] Press ENTER after changing VPN to continue...")
        try:
            input()
            print("  Waiting for VPN connection to stabilize...")
            time.sleep(3)  # Wait for connection to stabilize
            
            # Get new VPN info after manual change
            new_vpn_info = get_vpn_info()
            if new_vpn_info["connected"]:
                print("\n[VPN_STATUS] [OK] NEW VPN CONNECTED")
                print("-" * 80)
                print(f"  Provider: {new_vpn_info['provider']}")
                if new_vpn_info['server'] != "Unknown":
                    print(f"  Server: {new_vpn_info['server']}")
                print(f"  IP Address: {new_vpn_info['ip']}")
                print(f"  Location: {new_vpn_info['city']}, {new_vpn_info['country']}")
                print("-" * 80)
                log.info(f"[VPN] VPN changed manually - New Server: {new_vpn_info['server']}, IP: {new_vpn_info['ip']}, Location: {new_vpn_info['city']}, {new_vpn_info['country']}")
            else:
                log.info("[VPN] VPN changed manually (could not verify new connection)")
            print("=" * 80 + "\n")
        except (EOFError, KeyboardInterrupt):
            log.warning("[VPN] Input interrupted")
            raise

# GUI automation function removed - it was clicking on other applications
# Users should manually change VPN when prompted

def check_and_rotate_vpn_if_needed(thread_id: int):
    """Check if VPN rotation is needed (every 10 minutes) and rotate if necessary.
    On first check for a thread, initializes the timestamp and does NOT rotate.
    """
    with _vpn_rotation_lock:
        now = time.time()
        
        # If this is the first check for this thread, initialize timestamp and skip rotation
        if thread_id not in _last_vpn_change_time:
            _last_vpn_change_time[thread_id] = now
            log.debug(f"[VPN] Initialized VPN rotation timer for thread {thread_id} (no rotation on first check)")
            return  # Don't rotate on first check
        
        last_change = _last_vpn_change_time[thread_id]
        time_since_last = now - last_change
        
        if time_since_last >= _vpn_change_interval:
            elapsed_minutes = int(time_since_last / 60)
            rotate_vpn(thread_id, reason=f"scheduled ({elapsed_minutes} minutes elapsed)")
        else:
            # Show time remaining until next rotation
            remaining_seconds = _vpn_change_interval - time_since_last
            remaining_minutes = int(remaining_seconds / 60)
            if remaining_minutes > 0:
                log.debug(f"[VPN] Next rotation in ~{remaining_minutes} minutes (Thread {thread_id})")

# ====== CAPTCHA DETECTION ======

def is_captcha_page(driver) -> bool:
    """Check if current page is a captcha page.
    Skips check if driver is on about:blank to avoid hanging.
    """
    try:
        # Skip captcha check on about:blank pages (can hang on page_source access)
        current_url = driver.current_url.lower()
        if current_url.startswith("about:") or current_url == "data:":
            return False
        
        page_source_lower = driver.page_source.lower()
        url_lower = current_url
        
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
    """Wait if needed to respect rate limit for duplicates: 1 product per 10 seconds per thread (Selenium)"""
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
    g.add_argument("--headless", dest="headless", action="store_true", help="Run browser in headless mode")
    g.add_argument("--no-headless", dest="headless", action="store_false", help="Run browser in visible mode (default)")
    # Default to visible browser (headless=False)
    ap.set_defaults(headless=False)
    args = ap.parse_args()
    
    # Ensure visible browser by default (unless --headless is explicitly passed)
    if args.headless is None:
        args.headless = False
    
    # Log browser mode
    browser_mode = "HEADLESS" if args.headless else "VISIBLE"
    log.info(f"[BROWSER] Running in {browser_mode} mode")
    
    # Check VPN connection before starting
    if not check_vpn_connection():
        print("\n" + "=" * 80)
        print("[STARTUP] [FAIL] VPN connection check failed!")
        print("[STARTUP] Please connect your VPN (Proton VPN) and try again.")
        print("=" * 80 + "\n")
        log.error("[STARTUP] VPN connection check failed!")
        log.error("[STARTUP] Please connect your VPN (Proton VPN) and try again.")
        return 1  # Exit with error code
    
    print("\n" + "=" * 80)
    print("[STARTUP] [OK] VPN connection verified. Starting scraper...")
    print("[STARTUP] Note: Proxies are NOT used - using VPN only")
    print("[STARTUP] VPN will rotate every 10 minutes or on captcha/login detection")
    print("=" * 80 + "\n")
    log.info("[STARTUP] VPN connection verified. Starting scraper...")
    log.info("[STARTUP] Note: Proxies are NOT used - using VPN only")

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
        
        skipped_count = 0
        seen_pairs = set()  # Track seen pairs to deduplicate within the same file
        for row in r:
            prod = (row.get(pcol) or "").strip()
            comp = (row.get(ccol) or "").strip()
            source = (row.get(source_col) or "").strip().lower()
            is_dup_str = (row.get(dup_col) or "").strip().lower()
            is_duplicate = is_dup_str == "true"
            
            # Only process products marked as "selenium"
            if source == "selenium" and prod and comp:
                # Normalize for comparison
                key = (nk(comp), nk(prod))
                
                # Check if already seen in this file (deduplicate)
                if key in seen_pairs:
                    skipped_count += 1
                    log.debug(f"[SKIP] Skipping duplicate {comp} | {prod} (already in file)")
                    continue
                
                # Check if combination is in skip sets (ignore_list, progress, or products)
                if key in skip_set:
                    skipped_count += 1
                    log.debug(f"[SKIP] Skipping {comp} | {prod} (in ignore_list, progress, or products)")
                    continue
                
                # Add to seen pairs and targets
                seen_pairs.add(key)
                selenium_targets.append((prod, comp, is_duplicate))
    finally:
        if f:
            f.close()
    
    # Sort: duplicates (is_duplicate=True) first, then non-duplicates
    selenium_targets.sort(key=lambda x: (not x[2], x[0], x[1]))  # False (non-duplicates) comes after True (duplicates)
    
    duplicate_count = sum(1 for _, _, is_dup in selenium_targets if is_dup)
    non_duplicate_count = len(selenium_targets) - duplicate_count
    
    log.info(f"[FILTER] Found {len(selenium_targets) + skipped_count} products marked as 'selenium'")
    log.info(f"[FILTER] - Skipped (in ignore_list/progress/products): {skipped_count}")
    log.info(f"[FILTER] - To process: {len(selenium_targets)}")
    log.info(f"[FILTER]   - Duplicates (priority): {duplicate_count}")
    log.info(f"[FILTER]   - Non-duplicates: {non_duplicate_count}")
    
    # Apply max-rows limit if specified
    if args.max_rows > 0 and len(selenium_targets) > args.max_rows:
        selenium_targets = selenium_targets[:args.max_rows]
        log.info(f"Max rows limit applied: {args.max_rows} targets")
    
    # Use 1 thread when VPN rotation is enabled (to avoid conflicts with manual VPN changes)
    # Multiple threads can cause issues with VPN stability and manual rotation prompts
    num_threads = 1
    log.info(f"[SELENIUM] Using {num_threads} thread(s) (no proxy, no authentication)")
    log.info(f"[SELENIUM] Note: Single thread mode recommended for VPN rotation workflow")
    
    # Create queue and add all products
    selenium_queue = Queue()
    for target in selenium_targets:
        selenium_queue.put(target)
    
    log.info(f"[QUEUE] Added {selenium_queue.qsize()} products to queue")
    
    # Main processing loop: restart threads if captcha/login detected
    while not selenium_queue.empty() and not _shutdown_requested.is_set():
        # Reset captcha/login flag for this iteration
        _captcha_login_detected.clear()
        
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
        
        # Wait for all threads to complete (with timeout to check shutdown)
        log.info("[SELENIUM] Waiting for all worker threads to complete...")
        try:
            for i, thread in enumerate(threads):
                # Use timeout to periodically check for shutdown
                while thread.is_alive():
                    thread.join(timeout=1.0)  # Check every second
                    if _shutdown_requested.is_set():
                        log.warning(f"[SELENIUM] Shutdown requested, waiting for thread {i + 1}/{num_threads} to exit...")
                        # Give thread a moment to exit, then break
                        thread.join(timeout=2.0)
                        break
                if thread.is_alive():
                    log.warning(f"[SELENIUM] Thread {i + 1}/{num_threads} still alive after shutdown request")
                else:
                    log.info(f"[SELENIUM] Thread {i + 1}/{num_threads} completed")
        except KeyboardInterrupt:
            log.warning("[SELENIUM] Interrupted, shutting down...")
            _shutdown_requested.set()
            # Wait for threads to exit before closing drivers
            for i, thread in enumerate(threads):
                if thread.is_alive():
                    log.warning(f"[SELENIUM] Waiting for thread {i + 1}/{num_threads} to exit...")
                    thread.join(timeout=2.0)
            close_all_drivers()
            raise
        
        # Check for shutdown before continuing
        if _shutdown_requested.is_set():
            log.warning("[SELENIUM] Shutdown requested, exiting main loop...")
            # Ensure all threads have exited
            for i, thread in enumerate(threads):
                if thread.is_alive():
                    log.warning(f"[SELENIUM] Waiting for thread {i + 1}/{num_threads} to exit...")
                    thread.join(timeout=2.0)
                    if thread.is_alive():
                        log.warning(f"[SELENIUM] Thread {i + 1}/{num_threads} did not exit in time")
            break
        
        # Check if captcha/login was detected
        if _captcha_login_detected.is_set():
            log.warning("[SELENIUM] Captcha or login detected - all threads have exited")
            log.info(f"[SELENIUM] Remaining items in queue: {selenium_queue.qsize()}")
            log.warning("[SELENIUM] Please resolve the captcha/login issue and press ENTER to continue...")
            log.warning("[SELENIUM] (Press Ctrl+C to stop the pipeline)")
            try:
                # Simple input with periodic shutdown check
                import sys
                import time
                start_time = time.time()
                timeout = 300  # 5 minutes max wait
                
                # For Windows, we can't easily interrupt input(), so we'll use a timeout
                # For Unix, we'll check periodically
                if sys.platform == 'win32':
                    # Windows: try to read with timeout using threading
                    input_received = threading.Event()
                    def read_input():
                        try:
                            input()
                            input_received.set()
                        except:
                            pass
                    
                    input_thread = threading.Thread(target=read_input, daemon=True)
                    input_thread.start()
                    
                    # Wait for input or shutdown, checking every second
                    while not input_received.is_set() and not _shutdown_requested.is_set():
                        if (time.time() - start_time) > timeout:
                            log.warning("[SELENIUM] Input timeout, continuing...")
                            break
                        time.sleep(0.5)
                    
                    if _shutdown_requested.is_set():
                        raise KeyboardInterrupt
                else:
                    # Unix: use select for non-blocking input
                    import select
                    while not _shutdown_requested.is_set():
                        if (time.time() - start_time) > timeout:
                            log.warning("[SELENIUM] Input timeout, continuing...")
                            break
                        ready, _, _ = select.select([sys.stdin], [], [], 1.0)
                        if ready:
                            sys.stdin.readline()  # Consume the input
                            break
                    if _shutdown_requested.is_set():
                        raise KeyboardInterrupt
                
                log.info("[SELENIUM] Resuming with new threads...")
            except (EOFError, KeyboardInterrupt):
                log.warning("[SELENIUM] Input interrupted or shutdown requested, exiting...")
                _shutdown_requested.set()
                break
        else:
            # No captcha/login detected, processing completed normally
            break
    
    # Final check: ensure all threads are closed before exiting
    if _shutdown_requested.is_set():
        log.warning("[SELENIUM] Shutdown requested, ensuring all threads are closed...")
        # Get any remaining threads (in case we're in the middle of a loop)
        all_threads = [t for t in threading.enumerate() if t.name.startswith("SeleniumWorker-")]
        for thread in all_threads:
            if thread.is_alive():
                log.warning(f"[SELENIUM] Waiting for thread {thread.name} to exit...")
                thread.join(timeout=2.0)
                if thread.is_alive():
                    log.warning(f"[SELENIUM] Thread {thread.name} did not exit in time")
    
    # Ensure all drivers are closed
    close_all_drivers()
    
    if selenium_queue.empty():
        log.info("[SELENIUM] All products processed successfully")
        return 0
    else:
        log.warning(f"[SELENIUM] Processing stopped. Remaining items in queue: {selenium_queue.qsize()}")
        return 1

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
        log.info(f"[SELENIUM_WORKER] Initializing Chrome driver (headless={args.headless})...")
        log.info(f"[SELENIUM_WORKER] Target URL will be: {PRODUCTS_URL}")
        driver = setup_driver(headless=args.headless)
        log.info(f"[SELENIUM_WORKER] Chrome driver initialized successfully")
        
        # Test navigation to ensure driver is working (quick test)
        try:
            log.info(f"[SELENIUM_WORKER] Testing driver navigation with about:blank...")
            driver.get("about:blank")
            log.info(f"[SELENIUM_WORKER] Driver navigation test successful. Current URL: {driver.current_url}")
        except Exception as e:
            log.error(f"[SELENIUM_WORKER] Driver navigation test failed: {e}")
            raise
        
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
                # Defensive skip check with lock (runtime update protection)
                with _skip_lock:
                    key = (nk(in_company), nk(in_product))
                    if key in skip_set:
                        log.info(f"[SKIP-RUNTIME] {in_company} | {in_product}")
                        selenium_queue.task_done()
                        continue
                
                product_type = "DUPLICATE" if is_duplicate else "NON-DUPLICATE"
                log.info(f"[SELENIUM_WORKER] [SEARCH_START] [{product_type}] {in_company} | {in_product}")
                search_attempted = True
                
                # Check for shutdown before rate limiting
                if _shutdown_requested.is_set():
                    log.warning(f"[SELENIUM_WORKER] Shutdown requested, thread {thread_id} exiting")
                    selenium_queue.task_done()
                    break
                
                # Apply rate limit
                duplicate_rate_limit_wait(thread_id)
                
                # Check for shutdown after rate limiting
                if _shutdown_requested.is_set():
                    log.warning(f"[SELENIUM_WORKER] Shutdown requested, thread {thread_id} exiting")
                    selenium_queue.task_done()
                    break
                
                # Check if VPN rotation is needed (every 10 minutes)
                try:
                    check_and_rotate_vpn_if_needed(thread_id)
                except Exception as e:
                    log.warning(f"[SELENIUM_WORKER] VPN rotation check failed: {e}")
                
                # Check for captcha before processing (skip if on about:blank to avoid hanging)
                if driver and driver.current_url and not driver.current_url.startswith("about:") and is_captcha_page(driver):
                    log.warning(f"[SELENIUM_WORKER] [CAPTCHA_DETECTED] Captcha detected for {in_company} | {in_product}")
                    # Rotate VPN on captcha detection
                    try:
                        rotate_vpn(thread_id, reason="forced (captcha detected)")
                    except Exception as e:
                        log.warning(f"[SELENIUM_WORKER] VPN rotation failed: {e}")
                    log.warning(f"[SELENIUM_WORKER] [THREAD_EXIT] Thread {thread_id} exiting due to captcha detection")
                    _captcha_login_detected.set()  # Signal that captcha/login was detected
                    unregister_driver(driver)
                    driver.quit()
                    driver = None
                    # Check skip_set before requeuing - don't requeue if already processed
                    key = (nk(in_company), nk(in_product))
                    with _skip_lock:
                        if key not in skip_set:
                            # Put item back in queue to retry after user resolves captcha
                            selenium_queue.put(item)
                        else:
                            log.info(f"[SKIP-RUNTIME] Not requeuing {in_company} | {in_product} (already in skip_set)")
                    selenium_queue.task_done()
                    return  # Exit thread
                
                # Retry logic for TimeoutException
                max_retries = MAX_RETRIES_TIMEOUT
                retry_count = 0
                success = False
                
                while retry_count <= max_retries and not success:
                    # Check for shutdown at start of each retry
                    if _shutdown_requested.is_set():
                        log.warning(f"[SELENIUM_WORKER] Shutdown requested, thread {thread_id} exiting")
                        selenium_queue.task_done()
                        break
                    
                    try:
                        if retry_count > 0:
                            log.info(f"[SELENIUM_WORKER] [RETRY {retry_count}/{max_retries}] {in_company} | {in_product}")
                            # Check shutdown during sleep
                            for _ in range(10):
                                if _shutdown_requested.is_set():
                                    break
                                time.sleep(1)
                            if _shutdown_requested.is_set():
                                log.warning(f"[SELENIUM_WORKER] Shutdown requested, thread {thread_id} exiting")
                                selenium_queue.task_done()
                                break
                        
                        # Check shutdown before search
                        if _shutdown_requested.is_set():
                            log.warning(f"[SELENIUM_WORKER] Shutdown requested, thread {thread_id} exiting")
                            selenium_queue.task_done()
                            break
                        
                        # Log before attempting search
                        log.info(f"[SELENIUM_WORKER] [NAVIGATE] Starting navigation to products page for {in_company} | {in_product}")
                        log.info(f"[SELENIUM_WORKER] [NAVIGATE] Driver current URL before navigation: {driver.current_url}")
                        try:
                            search_in_products(driver, in_product)
                            log.info(f"[SELENIUM_WORKER] [NAVIGATE] Successfully navigated and searched for {in_company} | {in_product}")
                            log.info(f"[SELENIUM_WORKER] [NAVIGATE] Driver current URL after navigation: {driver.current_url}")
                        except RuntimeError as e:
                            if "Login page detected" in str(e):
                                log.warning(f"[SELENIUM_WORKER] [LOGIN_REQUIRED] Login page detected during search for {in_company} | {in_product}")
                                # Rotate VPN on login detection
                                try:
                                    rotate_vpn(thread_id, reason="forced (login detected)")
                                except Exception as vpn_e:
                                    log.warning(f"[SELENIUM_WORKER] VPN rotation failed: {vpn_e}")
                                log.warning(f"[SELENIUM_WORKER] [THREAD_EXIT] Thread {thread_id} exiting due to login requirement")
                                _captcha_login_detected.set()  # Signal that captcha/login was detected
                                unregister_driver(driver)
                                driver.quit()
                                driver = None
                                # Check skip_set before requeuing - don't requeue if already processed
                                key = (nk(in_company), nk(in_product))
                                with _skip_lock:
                                    if key not in skip_set:
                                        # Put item back in queue to retry after user resolves login
                                        selenium_queue.put(item)
                                    else:
                                        log.info(f"[SKIP-RUNTIME] Not requeuing {in_company} | {in_product} (already in skip_set)")
                                selenium_queue.task_done()
                                return  # Exit thread
                            else:
                                raise
                        
                        # Check for login page or captcha after search
                        if is_login_page(driver):
                            log.warning(f"[SELENIUM_WORKER] [LOGIN_REQUIRED] Login page detected after search for {in_company} | {in_product}")
                            # Rotate VPN on login detection
                            try:
                                rotate_vpn(thread_id, reason="forced (login detected)")
                            except Exception as e:
                                log.warning(f"[SELENIUM_WORKER] VPN rotation failed: {e}")
                            log.warning(f"[SELENIUM_WORKER] [THREAD_EXIT] Thread {thread_id} exiting due to login requirement")
                            _captcha_login_detected.set()  # Signal that captcha/login was detected
                            unregister_driver(driver)
                            driver.quit()
                            driver = None
                            # Check skip_set before requeuing - don't requeue if already processed
                            key = (nk(in_company), nk(in_product))
                            with _skip_lock:
                                if key not in skip_set:
                                    # Put item back in queue to retry after user resolves login
                                    selenium_queue.put(item)
                                else:
                                    log.info(f"[SKIP-RUNTIME] Not requeuing {in_company} | {in_product} (already in skip_set)")
                            selenium_queue.task_done()
                            return  # Exit thread
                        elif driver.current_url and not driver.current_url.startswith("about:") and is_captcha_page(driver):
                            log.warning(f"[SELENIUM_WORKER] [CAPTCHA_DETECTED] Captcha detected after search for {in_company} | {in_product}")
                            # Rotate VPN on captcha detection
                            try:
                                rotate_vpn(thread_id, reason="forced (captcha detected)")
                            except Exception as e:
                                log.warning(f"[SELENIUM_WORKER] VPN rotation failed: {e}")
                            log.warning(f"[SELENIUM_WORKER] [THREAD_EXIT] Thread {thread_id} exiting due to captcha detection")
                            _captcha_login_detected.set()  # Signal that captcha/login was detected
                            unregister_driver(driver)
                            driver.quit()
                            driver = None
                            # Check skip_set before requeuing - don't requeue if already processed
                            key = (nk(in_company), nk(in_product))
                            with _skip_lock:
                                if key not in skip_set:
                                    # Put item back in queue to retry after user resolves captcha
                                    selenium_queue.put(item)
                                else:
                                    log.info(f"[SKIP-RUNTIME] Not requeuing {in_company} | {in_product} (already in skip_set)")
                            selenium_queue.task_done()
                            return  # Exit thread
                        
                        if not open_exact_pair(driver, in_product, in_company):
                            save_debug(driver, DEBUG_NF, f"{in_company}_{in_product}")
                            # Product not found - mark for API fallback
                            log.warning(f"[SELENIUM_WORKER] [NOT_FOUND] Product not found for {in_company} | {in_product}, marking for API fallback")
                            update_prepared_urls_source(in_company, in_product, "api")
                            # Don't add to skip_set - let API try it
                            # Don't record progress for not found
                            success = True
                            break
                        
                        # Check for login page or captcha after opening product page
                        if is_login_page(driver):
                            log.warning(f"[SELENIUM_WORKER] [LOGIN_REQUIRED] Login page detected on product page for {in_company} | {in_product}")
                            # Rotate VPN on login detection
                            try:
                                rotate_vpn(thread_id, reason="forced (login detected)")
                            except Exception as e:
                                log.warning(f"[SELENIUM_WORKER] VPN rotation failed: {e}")
                            log.warning(f"[SELENIUM_WORKER] [THREAD_EXIT] Thread {thread_id} exiting due to login requirement")
                            _captcha_login_detected.set()  # Signal that captcha/login was detected
                            unregister_driver(driver)
                            driver.quit()
                            driver = None
                            # Check skip_set before requeuing - don't requeue if already processed
                            key = (nk(in_company), nk(in_product))
                            with _skip_lock:
                                if key not in skip_set:
                                    # Put item back in queue to retry after user resolves login
                                    selenium_queue.put(item)
                                else:
                                    log.info(f"[SKIP-RUNTIME] Not requeuing {in_company} | {in_product} (already in skip_set)")
                            selenium_queue.task_done()
                            return  # Exit thread
                        elif driver.current_url and not driver.current_url.startswith("about:") and is_captcha_page(driver):
                            log.warning(f"[SELENIUM_WORKER] [CAPTCHA_DETECTED] Captcha detected on product page for {in_company} | {in_product}")
                            # Rotate VPN on captcha detection
                            try:
                                rotate_vpn(thread_id, reason="forced (captcha detected)")
                            except Exception as e:
                                log.warning(f"[SELENIUM_WORKER] VPN rotation failed: {e}")
                            log.warning(f"[SELENIUM_WORKER] [THREAD_EXIT] Thread {thread_id} exiting due to captcha detection")
                            _captcha_login_detected.set()  # Signal that captcha/login was detected
                            unregister_driver(driver)
                            driver.quit()
                            driver = None
                            # Check skip_set before requeuing - don't requeue if already processed
                            key = (nk(in_company), nk(in_product))
                            with _skip_lock:
                                if key not in skip_set:
                                    # Put item back in queue to retry after user resolves captcha
                                    selenium_queue.put(item)
                                else:
                                    log.info(f"[SKIP-RUNTIME] Not requeuing {in_company} | {in_product} (already in skip_set)")
                            selenium_queue.task_done()
                            return  # Exit thread
                        
                        # Check shutdown before extracting rows
                        if _shutdown_requested.is_set():
                            log.warning(f"[SELENIUM_WORKER] Shutdown requested, thread {thread_id} exiting")
                            selenium_queue.task_done()
                            break
                        
                        rows = extract_rows(driver, in_company, in_product)
                        
                        # Check if rows have meaningful data (not blank)
                        rows_with_values = []
                        for row in rows:
                            # Check if row has actual values: price_ars, description, coverage, etc.
                            has_price = row.get("price_ars") is not None
                            has_description = row.get("description") and row.get("description").strip()
                            has_coverage = row.get("coverage_json") and row.get("coverage_json") != "{}"
                            has_import_status = row.get("import_status") and row.get("import_status").strip()
                            has_product_name = row.get("product_name") and row.get("product_name").strip()
                            
                            # Only include rows with at least one meaningful value
                            if has_price or has_description or has_coverage or has_import_status or has_product_name:
                                rows_with_values.append(row)
                        
                        if rows_with_values:
                            # Selenium succeeded with actual values - save results
                            append_rows(rows_with_values)
                            append_progress(in_company, in_product, len(rows_with_values))
                            # Update skip_set to prevent reprocessing in same run
                            with _skip_lock:
                                skip_set.add((nk(in_company), nk(in_product)))
                            log.info(f"[SELENIUM_WORKER] [SUCCESS] {in_company} | {in_product} → {len(rows_with_values)} rows with values")
                        else:
                            # Selenium returned blank results - mark for API fallback
                            log.warning(f"[SELENIUM_WORKER] [BLANK_RESULT] Selenium returned blank result for {in_company} | {in_product}, marking for API fallback")
                            update_prepared_urls_source(in_company, in_product, "api")
                            # Don't add to skip_set - let API try it
                            # Don't record progress for blank results
                            save_debug(driver, DEBUG_NF, f"{in_company}_{in_product}")
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
                import traceback
                log.error(f"[SELENIUM_WORKER] [ERROR] Traceback: {traceback.format_exc()}")
            finally:
                selenium_queue.task_done()
                if search_attempted:
                    human_pause()
    
    finally:
        # Clean up driver on shutdown or normal exit
        if driver:
            try:
                log.info(f"[SELENIUM_WORKER] Thread {thread_id} cleaning up driver...")
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
        
        if _shutdown_requested.is_set():
            log.warning(f"[SELENIUM_WORKER] Thread {thread_id} finished (shutdown requested)")
        else:
            log.info(f"[SELENIUM_WORKER] Thread {thread_id} finished")

if __name__ == "__main__":
    import sys
    exit_code = main()
    sys.exit(exit_code if exit_code is not None else 0)

