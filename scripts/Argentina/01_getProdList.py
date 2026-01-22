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
import socket
from pathlib import Path
from typing import List, Tuple, Set, Optional

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

# ====== TOR CONFIGURATION ======
# Global variable to store detected Tor port (9050 for Tor service, 9150 for Tor Browser)
TOR_PROXY_PORT = 9050  # Default to Tor service port

def check_tor_running(host="127.0.0.1", timeout=2):
    """
    Check if Tor SOCKS5 proxy is running and accepting connections.
    Checks both port 9050 (Tor service) and 9150 (Tor Browser).
    
    Returns:
        Tuple of (is_running: bool, port: int) - port is 9050 or 9150 if running, None otherwise
    """
    # Tor Browser uses port 9150, Tor service uses port 9050
    ports_to_check = [9150, 9050]  # Check Tor Browser port first
    
    for port in ports_to_check:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                port_name = "Tor Browser" if port == 9150 else "Tor service"
                log.info(f"[TOR_CHECK] {port_name} proxy is running on {host}:{port}")
                return True, port
        except Exception as e:
            log.debug(f"[TOR_CHECK] Error checking port {port}: {e}")
            continue
    
    log.warning(f"[TOR_CHECK] Tor proxy is not running on {host}:9050 or {host}:9150")
    return False, None

def find_firefox_binary():
    """
    Find Firefox binary in common locations on Windows.
    Checks for:
    1. Regular Firefox installation
    2. Tor Browser (which includes Firefox)
    3. Environment variable FIREFOX_BINARY
    """
    import shutil
    
    # Check environment variable first
    firefox_bin = os.getenv("FIREFOX_BINARY", "")
    if firefox_bin and Path(firefox_bin).exists():
        log.info(f"[FIREFOX] Using Firefox binary from FIREFOX_BINARY env: {firefox_bin}")
        return str(Path(firefox_bin).resolve())
    
    # Common Firefox installation paths on Windows
    userprofile = os.environ.get("USERPROFILE", "")
    possible_paths = [
        # Regular Firefox
        Path(os.environ.get("ProgramFiles", "C:\\Program Files")) / "Mozilla Firefox" / "firefox.exe",
        Path(os.environ.get("ProgramFiles(x86)", "C:\\Program Files (x86)")) / "Mozilla Firefox" / "firefox.exe",
        Path(os.environ.get("LOCALAPPDATA", "")) / "Mozilla Firefox" / "firefox.exe",
        # Tor Browser (includes Firefox) - Standard locations
        Path(os.environ.get("ProgramFiles", "C:\\Program Files")) / "Tor Browser" / "Browser" / "firefox.exe",
        Path(os.environ.get("ProgramFiles(x86)", "C:\\Program Files (x86)")) / "Tor Browser" / "Browser" / "firefox.exe",
        Path(os.environ.get("LOCALAPPDATA", "")) / "Tor Browser" / "Browser" / "firefox.exe",
        # Common user installation locations
        Path(userprofile) / "AppData" / "Local" / "Mozilla Firefox" / "firefox.exe",
        Path(userprofile) / "AppData" / "Local" / "Tor Browser" / "Browser" / "firefox.exe",
        # Desktop location (common for portable installations)
        Path(userprofile) / "Desktop" / "Tor Browser" / "Browser" / "firefox.exe",
        Path(userprofile) / "OneDrive" / "Desktop" / "Tor Browser" / "Browser" / "firefox.exe",
        # Downloads folder (common for portable installations)
        Path(userprofile) / "Downloads" / "Tor Browser" / "Browser" / "firefox.exe",
        Path(userprofile) / "OneDrive" / "Downloads" / "Tor Browser" / "Browser" / "firefox.exe",
    ]
    
    for path in possible_paths:
        if path.exists():
            log.info(f"[FIREFOX] Found Firefox binary: {path}")
            return str(path.resolve())
    
    # Last resort: try to find firefox.exe in PATH
    firefox_path = shutil.which("firefox")
    if firefox_path:
        log.info(f"[FIREFOX] Found Firefox in PATH: {firefox_path}")
        return firefox_path
    
    return None

def check_tor_requirements():
    """
    Check Tor requirements before starting the scraper.
    Returns True if all requirements are met, False otherwise.
    """
    print("\n" + "=" * 80)
    print("[TOR_CHECK] Verifying Tor connection...")
    print("=" * 80)
    log.info("[TOR_CHECK] Verifying Tor connection...")
    
    all_ok = True
    
    # Check 1: Firefox/Tor Browser installation
    print("\n[TOR_CHECK] 1. Checking Firefox/Tor Browser installation...")
    firefox_binary = find_firefox_binary()
    if firefox_binary:
        print(f"  [OK] Firefox/Tor Browser found: {firefox_binary}")
        log.info(f"[TOR_CHECK] Firefox/Tor Browser found: {firefox_binary}")
    else:
        print("  [FAIL] Firefox/Tor Browser not found")
        print("  [INFO] Please install Firefox or Tor Browser")
        print("  [INFO] Firefox: https://www.mozilla.org/firefox/")
        print("  [INFO] Tor Browser: https://www.torproject.org/download/")
        print("  [INFO] Or set FIREFOX_BINARY environment variable")
        log.error("[TOR_CHECK] Firefox/Tor Browser not found")
        all_ok = False
    
    # Check 2: Tor service running
    print("\n[TOR_CHECK] 2. Checking Tor proxy service...")
    tor_running, tor_port = check_tor_running()
    if tor_running:
        port_name = "Tor Browser" if tor_port == 9150 else "Tor service"
        print(f"  [OK] {port_name} proxy is running on localhost:{tor_port}")
        log.info(f"[TOR_CHECK] {port_name} proxy is running on port {tor_port}")
        # Store the detected port for later use
        global TOR_PROXY_PORT
        TOR_PROXY_PORT = tor_port
    else:
        print("  [FAIL] Tor proxy is not running on localhost:9050 or localhost:9150")
        print("  [INFO] Please start Tor before running the scraper:")
        print("  [INFO]   Option 1: Start Tor Browser (uses port 9150)")
        print("  [INFO]   Option 2: Start Tor service separately (uses port 9050)")
        print("  [INFO]   The scraper will automatically detect which port Tor is using")
        log.error("[TOR_CHECK] Tor proxy is not running")
        all_ok = False
    
    # Summary
    print("\n" + "=" * 80)
    if all_ok:
        print("[TOR_CHECK] [OK] Tor connection verified. Starting scraper...")
        log.info("[TOR_CHECK] Tor connection verified. Starting scraper...")
    else:
        print("[TOR_CHECK] [FAIL] Tor requirements not met. Please fix the issues above.")
        log.error("[TOR_CHECK] Tor requirements check failed")
    print("=" * 80 + "\n")
    
    return all_ok

# ====== VPN CHECK (kept for backward compatibility, but not used) ======
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
    opts = webdriver.FirefoxOptions()
    if headless:
        opts.add_argument("--headless")
    
    # Create a temporary profile for isolation
    profile = webdriver.FirefoxProfile()
    profile.set_preference("browser.cache.disk.enable", False)
    profile.set_preference("browser.cache.memory.enable", False)
    profile.set_preference("browser.cache.offline.enable", False)
    profile.set_preference("network.http.use-cache", False)
    
    # Enable images and CSS (needed for full content rendering on this site)
    profile.set_preference("permissions.default.image", 1)
    profile.set_preference("permissions.default.stylesheet", 1)
    
    # Language preference
    profile.set_preference("intl.accept_languages", "es-ES,es,en-US,en")
    
    # Disable notifications and popups
    profile.set_preference("dom.webnotifications.enabled", False)
    profile.set_preference("dom.push.enabled", False)
    
    # User agent
    profile.set_preference("general.useragent.override", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0")
    
    # Configure Tor SOCKS5 proxy (uses detected port: 9050 for Tor service, 9150 for Tor Browser)
    # Note: Tor must be running separately (either Tor service or Tor Browser)
    profile.set_preference("network.proxy.type", 1)  # Manual proxy configuration
    profile.set_preference("network.proxy.socks", "127.0.0.1")
    profile.set_preference("network.proxy.socks_port", TOR_PROXY_PORT)
    profile.set_preference("network.proxy.socks_version", 5)
    profile.set_preference("network.proxy.socks_remote_dns", True)  # Route DNS through Tor
    log.info(f"[TOR_CONFIG] Using Tor proxy on port {TOR_PROXY_PORT} ({'Tor Browser' if TOR_PROXY_PORT == 9150 else 'Tor service'})")
    
    # Update preferences
    opts.profile = profile
    
    # Set page load strategy to "eager" to avoid hanging on slow-loading resources
    opts.set_capability("pageLoadStrategy", "eager")
    
    # Find and set Firefox binary path
    firefox_binary = find_firefox_binary()
    if firefox_binary:
        # In Selenium 4, use binary_location instead of FirefoxBinary
        opts.binary_location = firefox_binary
        log.info(f"[FIREFOX] Using Firefox binary: {firefox_binary}")
    else:
        log.error("[FIREFOX] Firefox binary not found in common locations")
        log.error("[FIREFOX] Please install Firefox or set FIREFOX_BINARY environment variable")
        log.error("[FIREFOX] Example: set FIREFOX_BINARY=C:\\Program Files\\Mozilla Firefox\\firefox.exe")
        raise RuntimeError(
            "Firefox binary not found. Please:\n"
            "1. Install Firefox from https://www.mozilla.org/firefox/\n"
            "2. Or install Tor Browser (includes Firefox)\n"
            "3. Or set FIREFOX_BINARY environment variable to Firefox executable path"
        )

    # Create driver - try local geckodriver first, then fall back to GeckoDriverManager
    local_geckodriver = get_input_dir() / "geckodriver.exe"
    try:
        if local_geckodriver.exists():
            log.info(f"[GECKODRIVER] Using local geckodriver: {local_geckodriver}")
            driver = webdriver.Firefox(service=Service(str(local_geckodriver)), options=opts)
        else:
            log.info("[GECKODRIVER] Local geckodriver not found, using GeckoDriverManager")
            driver = webdriver.Firefox(service=Service(GeckoDriverManager().install()), options=opts)
    except Exception as e:
        log.exception("Failed to start FirefoxDriver")
        raise

    # Be generous on page load for slow pages
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    # small implicit wait to reduce flakiness, rely mostly on explicit waits
    driver.implicitly_wait(IMPLICIT_WAIT)
    
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
    
    # Check Tor connection before starting
    if not check_tor_requirements():
        print("\n" + "=" * 80)
        print("[STARTUP] [FAIL] Tor connection check failed!")
        print("[STARTUP] Please start Tor (Tor Browser or Tor service) and try again.")
        print("=" * 80 + "\n")
        log.error("[STARTUP] Tor connection check failed!")
        log.error("[STARTUP] Please start Tor (Tor Browser or Tor service) and try again.")
        return 1
    
    print("\n" + "=" * 80)
    print("[STARTUP] [OK] Tor connection verified. Starting scraper...")
    print("[STARTUP] Using Tor proxy for all requests")
    print("=" * 80 + "\n")
    log.info("[STARTUP] Tor connection verified. Starting scraper...")
    log.info("[STARTUP] Using Tor proxy for all requests")
    
    # Force headless mode for product list extraction
    d = setup_driver(headless=True)
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

        acc: Set[Tuple[str, str]] = set()
        page = 1
        # Estimate total pages (will update as we go)
        estimated_total = 100  # Initial estimate, will adjust
        
        while True:
            try:
                pairs = extract_products_page(d)
                for row in pairs:
                    acc.add(row)
                log.info(f"Page {page}: +{len(pairs)}  (unique total: {len(acc)})")
                
                # Output progress (estimate total pages, update as we discover more)
                if page % 10 == 0 or len(pairs) == 0:  # Update every 10 pages or when no more products
                    # Estimate: if we're still getting products, there might be more
                    if len(pairs) > 0:
                        estimated_total = max(estimated_total, page + 10)  # Extend estimate
                    percent = round((page / estimated_total) * 100, 1) if estimated_total > 0 else 0
                    print(f"[PROGRESS] Extracting products: Page {page} (unique: {len(acc)})", flush=True)
            except Exception:
                log.exception("Error extracting page; saving artifact and continuing")
                safe_screenshot_and_source(d, name_prefix=f"extract_page_{page}")

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
