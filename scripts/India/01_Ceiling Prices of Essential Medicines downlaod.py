#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
India NPPA Pharma Sahi Daam Scraper - Step 01: Download Ceiling Prices

Downloads the complete ceiling prices Excel file from NPPA website.
"""

import os
import sys
import time
from pathlib import Path
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Add repo root to path for core imports
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add scripts/India to path for local imports
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

# Import platform components
from config_loader import (
    get_output_dir, get_download_dir,
    getenv_bool, getenv_int, load_env_file, SCRAPER_ID
)

# Import Chrome manager for proper cleanup
try:
    from core.chrome_manager import register_chrome_driver, cleanup_all_chrome_instances
    _CHROME_MANAGER_AVAILABLE = True
except ImportError:
    _CHROME_MANAGER_AVAILABLE = False
    def register_chrome_driver(driver): pass
    def cleanup_all_chrome_instances(silent=False): pass

# Load environment configuration
load_env_file()

URL = "https://nppaipdms.gov.in/NPPA/PharmaSahiDaam/searchMedicine"


def _latest_file(dir_path: Path) -> Path:
    """Get the most recently modified file in a directory."""
    files = [p for p in dir_path.glob("*") if p.is_file()]
    if not files:
        return None
    return max(files, key=lambda p: p.stat().st_mtime)


def wait_for_download_complete(download_dir: Path, timeout_sec: int = 180) -> Path:
    """
    Wait for Chrome downloads to finish (.crdownload disappears).
    Returns the newest file in download_dir once complete.
    """
    end = time.time() + timeout_sec

    while time.time() < end:
        if list(download_dir.glob("*.crdownload")):
            time.sleep(1)
            continue

        latest = _latest_file(download_dir)
        if latest:
            # small grace period for flush
            time.sleep(1)
            if not list(download_dir.glob("*.crdownload")):
                return latest

        time.sleep(1)

    raise TimeoutError(f"Download did not complete within {timeout_sec} seconds.")


def build_driver(download_dir: Path, headless: bool = None) -> webdriver.Chrome:
    """Build Chrome WebDriver with proper configuration and registration."""
    download_dir.mkdir(parents=True, exist_ok=True)

    # Use config headless setting if not explicitly passed
    if headless is None:
        headless = getenv_bool("HEADLESS", False)

    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")

    options.add_argument("--window-size=1400,900")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option("prefs", prefs)
    options.add_experimental_option("excludeSwitches", ["enable-logging"])

    driver = webdriver.Chrome(options=options)
    
    # Register with Chrome manager for cleanup on exit
    if _CHROME_MANAGER_AVAILABLE:
        register_chrome_driver(driver)
    
    return driver


def main():
    print("=" * 60)
    print("India NPPA Scraper - Step 01: Download Ceiling Prices")
    print("=" * 60)
    
    download_dir = get_download_dir()
    output_dir = get_output_dir()
    wait_seconds = getenv_int("WAIT_SECONDS", 60)
    
    print(f"[CONFIG] Download dir: {download_dir}")
    print(f"[CONFIG] Output dir: {output_dir}")
    
    driver = None
    try:
        driver = build_driver(download_dir)
        wait = WebDriverWait(driver, wait_seconds)

        print("[INFO] Opening NPPA Pharma Sahi Daam website...")
        driver.get(URL)

        # ----------------------------
        # Step 1 + Step 2:
        # Click the radio input (caseFlag) with value "2" for Ceiling Prices
        # ----------------------------
        print("[INFO] Selecting 'Ceiling Prices of Essential Medicines' option...")
        radio_val_2 = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[name="caseFlag"][value="2"]'))
        )
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", radio_val_2)
        driver.execute_script("arguments[0].click();", radio_val_2)

        # small pause to allow JS (getBrandScheduleCmb()) to run
        time.sleep(1)

        # ----------------------------
        # Step 3:
        # Click GO button
        # ----------------------------
        print("[INFO] Clicking GO button...")
        go_btn = wait.until(EC.element_to_be_clickable((By.ID, "gobtn")))
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", go_btn)
        go_btn.click()

        # ----------------------------
        # Step 4:
        # Click Excel export button (DataTables)
        # ----------------------------
        print("[INFO] Waiting for results and Excel button...")
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.dt-buttons")))

        excel_btn = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.dt-button.buttons-excel[title="Excel"]'))
        )
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", excel_btn)
        
        print("[INFO] Clicking Excel export button...")
        excel_btn.click()

        # Wait for the file to download
        print("[INFO] Waiting for download to complete...")
        downloaded = wait_for_download_complete(download_dir, timeout_sec=180)

        # Move to output directory with deterministic name
        target = output_dir / f"ceiling_prices_{datetime.now():%Y%m%d_%H%M%S}{downloaded.suffix}"
        
        # If same file name exists, add a counter
        if target.exists():
            i = 1
            while True:
                cand = target.with_name(f"{target.stem}_{i}{target.suffix}")
                if not cand.exists():
                    target = cand
                    break
                i += 1

        downloaded.rename(target)
        print(f"[OK] Saved Excel: {target}")
        
        # Also save a copy with standard name for pipeline
        standard_target = output_dir / f"ceiling_prices{downloaded.suffix}"
        if standard_target.exists():
            standard_target.unlink()
        import shutil
        shutil.copy2(target, standard_target)
        print(f"[OK] Standard output: {standard_target}")

        print("\n" + "=" * 60)
        print("Ceiling prices download complete!")
        print("=" * 60)

    except Exception as e:
        print(f"[FATAL] Script failed: {e}")
        raise
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass
        # Cleanup any remaining Chrome instances
        if _CHROME_MANAGER_AVAILABLE:
            cleanup_all_chrome_instances(silent=True)


if __name__ == "__main__":
    main()
