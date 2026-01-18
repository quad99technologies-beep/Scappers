#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
India NPPA Pharma Sahi Daam Scraper - Step 02: Get Medicine Details

Searches for formulations, downloads Excel exports, and extracts detailed
medicine information including substitutes/available brands.

Features:
- Loads unique formulations from ceiling_prices.xlsx (Step 01 output)
- Resume support: skips fully completed formulations
- Handles partial scrapes without duplicating data
- Generates final summary report
"""

import os
import re
import time
import csv
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional, Set

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

# Add repo root to path for core imports
_repo_root = Path(__file__).resolve().parents[2]
import sys
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add scripts/India to path for local imports
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

# Import platform components
from config_loader import (
    get_output_dir, get_input_dir, get_download_dir,
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

# -----------------------------
# Checkpoint/Resume System for Formulations
# -----------------------------
class FormulationCheckpoint:
    """Manages checkpoint/resume for formulation-level processing."""
    
    def __init__(self, output_dir: Path):
        self.checkpoint_dir = output_dir / ".checkpoints"
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_file = self.checkpoint_dir / "formulation_progress.json"
        self._data = None
    
    def _load(self) -> Dict:
        """Load checkpoint data."""
        if self._data is not None:
            return self._data
        
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    self._data = json.load(f)
            except Exception as e:
                print(f"[WARN] Failed to load formulation checkpoint: {e}")
                self._data = self._default_data()
        else:
            self._data = self._default_data()
        return self._data
    
    def _default_data(self) -> Dict:
        return {
            "completed_formulations": [],
            "in_progress": None,
            "last_updated": None,
            "stats": {
                "total_processed": 0,
                "total_medicines": 0,
                "total_substitutes": 0,
                "errors": 0
            }
        }
    
    def _save(self):
        """Save checkpoint data atomically."""
        data = self._load()
        data["last_updated"] = datetime.now().isoformat()
        
        try:
            temp_file = self.checkpoint_file.with_suffix('.tmp')
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            temp_file.replace(self.checkpoint_file)
        except Exception as e:
            print(f"[ERROR] Failed to save formulation checkpoint: {e}")
    
    def is_completed(self, formulation: str) -> bool:
        """Check if a formulation has been fully completed."""
        data = self._load()
        return formulation.strip().upper() in [f.upper() for f in data["completed_formulations"]]
    
    def mark_completed(self, formulation: str, medicines_count: int = 0, substitutes_count: int = 0):
        """Mark a formulation as fully completed."""
        data = self._load()
        formulation_upper = formulation.strip().upper()
        
        if formulation_upper not in [f.upper() for f in data["completed_formulations"]]:
            data["completed_formulations"].append(formulation)
            data["stats"]["total_processed"] += 1
            data["stats"]["total_medicines"] += medicines_count
            data["stats"]["total_substitutes"] += substitutes_count
        
        # Clear in_progress if it matches
        if data["in_progress"] and data["in_progress"].upper() == formulation_upper:
            data["in_progress"] = None
        
        self._save()
        print(f"[CHECKPOINT] Marked formulation '{formulation}' as completed")
    
    def mark_in_progress(self, formulation: str):
        """Mark a formulation as currently being processed."""
        data = self._load()
        data["in_progress"] = formulation
        self._save()
    
    def mark_error(self, formulation: str):
        """Record an error for a formulation."""
        data = self._load()
        data["stats"]["errors"] += 1
        self._save()
    
    def get_completed_count(self) -> int:
        """Get count of completed formulations."""
        return len(self._load()["completed_formulations"])
    
    def get_stats(self) -> Dict:
        """Get processing statistics."""
        return self._load()["stats"]
    
    def clear(self):
        """Clear all checkpoint data for fresh start."""
        self._data = self._default_data()
        self._save()
        print("[CHECKPOINT] Cleared formulation checkpoint data")


# -----------------------------
# Load formulations from ceiling prices
# -----------------------------
def load_formulations_from_ceiling_prices(output_dir: Path) -> List[str]:
    """
    Load unique formulations from ceiling_prices.xlsx (Step 01 output).
    Falls back to input CSV if ceiling prices not available.
    """
    ceiling_prices_file = output_dir / "ceiling_prices.xlsx"
    
    if ceiling_prices_file.exists():
        try:
            print(f"[INFO] Loading formulations from ceiling prices: {ceiling_prices_file}")
            
            # The NPPA Excel has a title row at row 0, actual headers at row 1
            # Try reading with header=1 first (skip title row)
            try:
                df = pd.read_excel(ceiling_prices_file, header=1)
            except Exception:
                df = pd.read_excel(ceiling_prices_file)
            
            # Look for formulation column - common names
            formulation_cols = [
                'Formulation', 'formulation', 'FORMULATION',
                'Medicine Formulation', 'medicine_formulation',
                'Generic Name', 'generic_name', 'GENERIC_NAME',
                'Salt', 'salt', 'SALT',
                'Drug Name', 'drug_name'
            ]
            
            formulation_col = None
            for col in formulation_cols:
                if col in df.columns:
                    formulation_col = col
                    break
            
            # If no exact match, look for column containing 'formulation' or 'generic'
            if formulation_col is None:
                for col in df.columns:
                    col_lower = str(col).lower()
                    if 'formulation' in col_lower or 'generic' in col_lower or 'salt' in col_lower:
                        formulation_col = col
                        break
            
            if formulation_col is None:
                # If still not found, try reading without header skip
                df_alt = pd.read_excel(ceiling_prices_file)
                for col in df_alt.columns:
                    col_lower = str(col).lower()
                    if 'formulation' in col_lower:
                        df = df_alt
                        formulation_col = col
                        break
            
            if formulation_col is None:
                print(f"[WARN] Could not find formulation column in ceiling prices.")
                print(f"[WARN] Available columns: {[str(c) for c in df.columns]}")
                return load_formulations_from_input()
            
            print(f"[INFO] Using column '{formulation_col}' for formulations")
            
            # Extract unique formulations - use the full formulation name as-is
            # The NPPA website expects the exact formulation name
            formulations = df[formulation_col].dropna().astype(str).str.strip()
            unique_formulations = sorted(set(formulations))
            
            # Filter out empty strings and numeric values
            unique_formulations = [f for f in unique_formulations if f and not f.replace('.', '').isdigit()]
            
            print(f"[OK] Loaded {len(unique_formulations)} unique formulations from ceiling prices")
            
            # Show sample
            if unique_formulations:
                sample = unique_formulations[:5]
                safe_sample = [s.encode('ascii', 'replace').decode('ascii') for s in sample]
                print(f"[INFO] Sample formulations: {safe_sample}")
            
            return unique_formulations
            
        except Exception as e:
            print(f"[ERROR] Failed to load ceiling prices: {e}")
            import traceback
            traceback.print_exc()
    
    # Fallback to input CSV
    return load_formulations_from_input()


def load_formulations_from_input() -> List[str]:
    """Load formulations from input CSV file if it exists."""
    input_file = get_input_dir() / "formulations.csv"
    if input_file.exists():
        try:
            df = pd.read_csv(input_file)
            # Look for column named 'formulation', 'Formulation', 'name', 'Name', or first column
            for col in ['formulation', 'Formulation', 'name', 'Name', 'generic_name', 'Generic_Name']:
                if col in df.columns:
                    return df[col].dropna().str.strip().tolist()
            # Use first column if no known column found
            return df.iloc[:, 0].dropna().str.strip().tolist()
        except Exception as e:
            print(f"[WARN] Failed to load formulations from {input_file}: {e}")
    return []


# Default formulations (used if no input file)
DEFAULT_FORMULATIONS = [
    "ABACAVIR",
]


# -----------------------------
# Small utilities
# -----------------------------
def slugify(s: str, max_len: int = 120) -> str:
    s = (s or "").strip()
    s = re.sub(r"[\\/:*?\"<>|]+", "_", s)  # windows illegal
    s = re.sub(r"\s+", " ", s).strip()
    if len(s) > max_len:
        s = s[:max_len].rstrip()
    return s


def click_js(driver, el):
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    driver.execute_script("arguments[0].click();", el)


def latest_file(folder: Path) -> Optional[Path]:
    files = [p for p in folder.glob("*") if p.is_file()]
    return max(files, key=lambda p: p.stat().st_mtime) if files else None


def wait_for_download_complete(folder: Path, timeout_sec: int = 180) -> Path:
    end = time.time() + timeout_sec
    while time.time() < end:
        if list(folder.glob("*.crdownload")):
            time.sleep(1)
            continue

        f = latest_file(folder)
        if f:
            time.sleep(1)  # flush grace
            if not list(folder.glob("*.crdownload")):
                return f
        time.sleep(1)
    raise TimeoutError("Download did not complete in time.")


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


# -----------------------------
# Page-specific robust waits
# -----------------------------
def wait_results_loaded(driver, wait: WebDriverWait):
    """
    Results table is injected into #pharmaDiv. Wait until table with medicine data appears.
    """
    def _ready(d):
        # Check for any table with Medicine Name links
        tables = d.find_elements(By.CSS_SELECTOR, "table")
        for table in tables:
            links = table.find_elements(By.CSS_SELECTOR, "a[data-toggle='modal']")
            if links:
                return True
        # Also check for DataTables
        if d.find_elements(By.CSS_SELECTOR, "#myDatatable"):
            return True
        if d.find_elements(By.CSS_SELECTOR, ".dataTables_wrapper"):
            return True
        return False

    wait.until(_ready)
    time.sleep(1.5)


def dismiss_alert_if_present(driver):
    """Dismiss any alert dialog that may be present."""
    try:
        from selenium.webdriver.common.alert import Alert
        alert = Alert(driver)
        alert_text = alert.text
        alert.accept()
        print(f"[WARN] Dismissed alert: {alert_text}")
        return True
    except:
        return False


def pick_autocomplete_exact_match(driver, wait: WebDriverWait, search_term: str, timeout: int = 10) -> bool:
    """
    Wait for autocomplete dropdown and click on the item that exactly matches the search term.
    If no exact match found, click the first item.
    
    Returns:
        True if autocomplete selection was successful, False otherwise
    """
    # Try multiple possible autocomplete selectors
    autocomplete_selectors = [
        ".autocomplete-suggestions div",  # NPPA uses this
        "ul.ui-autocomplete li.ui-menu-item",
        "ul.ui-autocomplete li",
        ".ui-autocomplete .ui-menu-item",
        ".ui-autocomplete-results li",
        "ul.ui-menu li.ui-menu-item",
        ".ui-menu-item",
        "[role='option']",
        ".dropdown-menu li",
        ".typeahead li",
    ]
    
    items = None
    end_time = time.time() + timeout
    
    safe_search = search_term.encode('ascii', 'replace').decode('ascii')
    print(f"[DEBUG] Looking for autocomplete dropdown for '{safe_search}'...")
    
    while time.time() < end_time:
        # Check for alert first
        dismiss_alert_if_present(driver)
        
        for selector in autocomplete_selectors:
            try:
                found = driver.find_elements(By.CSS_SELECTOR, selector)
                if found:
                    visible = [el for el in found if el.is_displayed()]
                    if visible:
                        items = visible
                        print(f"[DEBUG] Found {len(items)} items with selector: {selector}")
                        break
            except Exception as e:
                pass
        if items:
            break
        time.sleep(0.3)
    
    if not items:
        print(f"[WARN] No autocomplete dropdown found for '{safe_search}'.")
        return False
    
    # Look for exact match (case-insensitive)
    search_upper = search_term.strip().upper()
    for item in items:
        try:
            item_text = item.text.strip().upper()
            # Only print first few items to avoid log spam
            if items.index(item) < 5:
                safe_text = item.text.strip().encode('ascii', 'replace').decode('ascii')
                print(f"[DEBUG] Checking item: '{safe_text}'")
            if item_text == search_upper:
                safe_text = item.text.strip().encode('ascii', 'replace').decode('ascii')
                print(f"[OK] Found exact match: {safe_text}")
                click_js(driver, item)
                time.sleep(0.5)
                return True
            # Also check if item starts with search term (for partial matches)
            if item_text.startswith(search_upper):
                safe_text = item.text.strip().encode('ascii', 'replace').decode('ascii')
                print(f"[OK] Found partial match: {safe_text}")
                click_js(driver, item)
                time.sleep(0.5)
                return True
        except Exception as e:
            continue
    
    # If no exact/partial match, click the first item
    if items:
        try:
            safe_text = items[0].text.strip().encode('ascii', 'replace').decode('ascii')
            print(f"[INFO] No exact match for '{safe_search}', clicking first item: {safe_text}")
        except:
            print(f"[INFO] No exact match for '{safe_search}', clicking first item")
        click_js(driver, items[0])
        time.sleep(0.5)
        return True
    
    return False


def set_datatable_show_max(driver):
    """
    Best-effort: increase 'Show N entries' to max to reduce paging.
    """
    sels = driver.find_elements(By.CSS_SELECTOR, 'select[name$="_length"]')
    if not sels:
        return
    sel = Select(sels[0])
    values = [o.get_attribute("value") for o in sel.options]
    # prefer "All" (-1) else max numeric
    if "-1" in values:
        sel.select_by_value("-1")
    else:
        nums = []
        for v in values:
            try:
                nums.append(int(v))
            except:
                pass
        if nums:
            sel.select_by_value(str(max(nums)))
    time.sleep(1.0)


def click_excel_export(driver, wait: WebDriverWait):
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.dt-buttons")))
    # Excel button: button.buttons-excel (title Excel)
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.buttons-excel[title="Excel"]')))
    btn = driver.find_element(By.CSS_SELECTOR, 'button.buttons-excel[title="Excel"]')
    click_js(driver, btn)


# -----------------------------
# CSV helpers
# -----------------------------
def excel_to_csv(excel_path: Path, csv_path: Path):
    df = pd.read_excel(excel_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")


def write_csv_rows(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str], append: bool = True):
    """
    Write rows to CSV file.
    
    Args:
        path: Output file path
        rows: List of row dictionaries
        fieldnames: Column names
        append: If True, append to existing file; if False, overwrite
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    
    if append:
        new_file = not path.exists()
        mode = "a"
    else:
        new_file = True
        mode = "w"
    
    with path.open(mode, newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if new_file:
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def get_existing_records(csv_path: Path, key_field: str = "MedicineName") -> Set[str]:
    """
    Get set of existing records from a CSV file to avoid duplicates.
    
    Args:
        csv_path: Path to CSV file
        key_field: Field to use as unique key
        
    Returns:
        Set of existing key values
    """
    existing = set()
    if csv_path.exists():
        try:
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            if key_field in df.columns:
                existing = set(df[key_field].dropna().astype(str).str.strip())
        except Exception as e:
            print(f"[WARN] Could not read existing records from {csv_path}: {e}")
    return existing


# -----------------------------
# Extract search results table (as CSV)
# -----------------------------
def extract_search_table_rows(driver) -> List[Dict[str, Any]]:
    """
    Extract visible rows from the results table.
    Columns: S.No., Medicine Name, Status, Ceiling Price (₹), Unit, M.R.P (₹)/Unit
    """
    rows_out = []
    
    # Find the table containing medicine links
    tables = driver.find_elements(By.CSS_SELECTOR, "table")
    target_table = None
    for table in tables:
        try:
            if table.find_elements(By.CSS_SELECTOR, "a[data-toggle='modal']"):
                target_table = table
                break
            # Use XPath for onclick contains
            if table.find_elements(By.XPATH, ".//a[contains(@onclick, 'getOtherBrandPrice')]"):
                target_table = table
                break
        except:
            continue
    
    if not target_table:
        print("[WARN] Could not find results table")
        return rows_out

    rows = target_table.find_elements(By.CSS_SELECTOR, "tbody tr")
    for r in rows:
        tds = r.find_elements(By.CSS_SELECTOR, "td")
        if len(tds) < 6:
            continue
        med_link = tds[1].find_elements(By.CSS_SELECTOR, "a")
        med_name = med_link[0].text.strip() if med_link else tds[1].text.strip()

        rows_out.append({
            "SNo": tds[0].text.strip(),
            "MedicineName": med_name,
            "Status": tds[2].text.strip(),
            "CeilingPrice": tds[3].text.strip(),
            "Unit": tds[4].text.strip(),
            "MRP_Unit": tds[5].text.strip(),
        })
    return rows_out


# -----------------------------
# Modal extraction (Market Price - Available Brands)
# -----------------------------
def wait_modal_open(wait: WebDriverWait):
    # Example modal id in HTML: #exampleModal (Market Price)
    wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#exampleModal")))
    wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#popupDiv")))
    time.sleep(0.5)


def close_modal(driver, wait: WebDriverWait):
    # Close 'X'
    btns = driver.find_elements(By.CSS_SELECTOR, "#exampleModal button.close")
    if btns:
        click_js(driver, btns[0])
    else:
        driver.switch_to.active_element.send_keys(Keys.ESC)
    time.sleep(0.5)
    # wait till popup hidden
    try:
        wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, "#popupDiv")))
    except:
        pass


def parse_modal_header(driver) -> Dict[str, str]:
    """
    Extract header info from the modal's #testTable.
    Structure:
    Row 1: Formulation/Brand, Company Name, M.R.P.
    Row 2: M.R.P./Unit, M.R.P. as on Date
    """
    out = {
        "Formulation_Brand": "",
        "CompanyName_Header": "",
        "MRP_Header": "",
        "MRP_Unit_Header": "",
        "MRP_AsOnDate": "",
    }
    
    try:
        # Try to find the header table (#testTable)
        header_table = driver.find_element(By.CSS_SELECTOR, "#popupDiv #testTable")
        rows = header_table.find_elements(By.CSS_SELECTOR, "tr")
        
        for row in rows:
            tds = row.find_elements(By.CSS_SELECTOR, "td")
            # Parse each pair of label:value cells
            i = 0
            while i < len(tds) - 1:
                label = tds[i].text.strip().lower()
                value = tds[i + 1].text.strip()
                
                if "formulation" in label or "brand" in label:
                    out["Formulation_Brand"] = value
                elif "company" in label:
                    out["CompanyName_Header"] = value
                elif "m.r.p./unit" in label or "mrp/unit" in label:
                    out["MRP_Unit_Header"] = value
                elif "as on date" in label:
                    out["MRP_AsOnDate"] = value
                elif "m.r.p" in label:
                    out["MRP_Header"] = value
                
                i += 2
    except Exception as e:
        # Fallback: use regex on popup text
        try:
            popup = driver.find_element(By.CSS_SELECTOR, "#popupDiv")
            popup_text = popup.text
            
            def grab(label: str):
                m = re.search(rf"{re.escape(label)}\s*:?\s*(.+?)(?:\n|$)", popup_text, flags=re.IGNORECASE)
                return m.group(1).strip() if m else ""
            
            out["Formulation_Brand"] = grab("Formulation / Brand")
            out["CompanyName_Header"] = grab("Company Name")
            out["MRP_Unit_Header"] = grab("M.R.P./Unit")
            out["MRP_AsOnDate"] = grab("M.R.P. as on Date")
            out["MRP_Header"] = grab("M.R.P.")
        except:
            pass
    
    return out


def extract_modal_substitutes(driver) -> List[Dict[str, str]]:
    """
    Extract substitutes from the modal's #nonSchTable.
    Columns: S.No., Brand Name, Pack Size, M.R.P (₹), M.R.P./Unit (₹), Company Name
    """
    subs = []
    
    try:
        # Target the specific substitutes table
        sub_table = driver.find_element(By.CSS_SELECTOR, "#popupDiv #nonSchTable")
        rows = sub_table.find_elements(By.CSS_SELECTOR, "tbody tr")
        
        for r in rows:
            tds = r.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) >= 6:
                subs.append({
                    "Sub_SNo": tds[0].text.strip(),
                    "Sub_BrandName": tds[1].text.strip(),
                    "Sub_PackSize": tds[2].text.strip(),
                    "Sub_MRP": tds[3].text.strip(),
                    "Sub_MRP_Unit": tds[4].text.strip(),
                    "Sub_CompanyName": tds[5].text.strip(),
                })
    except Exception as e:
        # Fallback: find any table with 6 columns in popup (excluding header table)
        try:
            popup = driver.find_element(By.CSS_SELECTOR, "#popupDiv")
            tables = popup.find_elements(By.CSS_SELECTOR, "table")
            
            for table in tables:
                table_id = table.get_attribute("id") or ""
                # Skip the header table
                if table_id == "testTable":
                    continue
                    
                rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
                for r in rows:
                    tds = r.find_elements(By.CSS_SELECTOR, "td")
                    if len(tds) >= 6:
                        subs.append({
                            "Sub_SNo": tds[0].text.strip(),
                            "Sub_BrandName": tds[1].text.strip(),
                            "Sub_PackSize": tds[2].text.strip(),
                            "Sub_MRP": tds[3].text.strip(),
                            "Sub_MRP_Unit": tds[4].text.strip(),
                            "Sub_CompanyName": tds[5].text.strip(),
                        })
        except:
            pass
    
    return subs


def get_medicine_table(driver):
    """Find the table containing medicine links."""
    tables = driver.find_elements(By.CSS_SELECTOR, "table")
    for table in tables:
        try:
            # Check for modal links
            if table.find_elements(By.CSS_SELECTOR, "a[data-toggle='modal']"):
                return table
            # Check for onclick handlers (use XPath for contains)
            if table.find_elements(By.XPATH, ".//a[contains(@onclick, 'getOtherBrandPrice')]"):
                return table
        except:
            continue
    return None


def scrape_details_for_all_medicines(
    driver, 
    wait: WebDriverWait, 
    formulation: str, 
    max_medicines: int = 0,
    existing_medicines: Set[str] = None
) -> List[Dict[str, Any]]:
    """
    Click each medicine link on the current results view (handles paging best-effort),
    extract modal header + substitutes, and flatten into CSV rows:
    one row per substitute (or one row with blanks if no substitute rows).
    
    Args:
        driver: Selenium WebDriver
        wait: WebDriverWait instance
        formulation: Formulation name being processed
        max_medicines: Maximum number of medicines to process (0 = no limit)
        existing_medicines: Set of already processed medicine names to skip (for resume)
    
    Returns:
        List of detail row dictionaries
    """
    if existing_medicines is None:
        existing_medicines = set()
    
    all_rows = []
    total_processed = 0
    total_skipped = 0
    errors_count = 0
    page_num = 1
    processed_medicines = set()  # Track processed medicine names to avoid duplicates
    
    set_datatable_show_max(driver)
    time.sleep(1)  # Wait for table to reload after changing page size

    # handle pagination if exists
    while True:
        table = get_medicine_table(driver)
        if not table:
            print("[WARN] Could not find medicine table")
            break
            
        rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
        num_rows = len(rows)
        print(f"[INFO] Page {page_num}: Found {num_rows} medicine rows")
        
        # Debug: print first row structure
        if num_rows > 0 and page_num == 1:
            first_row = rows[0]
            first_tds = first_row.find_elements(By.CSS_SELECTOR, "td")
            print(f"[DEBUG] First row has {len(first_tds)} cells")
            if len(first_tds) > 1:
                # Check for links in first few cells
                for cell_idx, td in enumerate(first_tds[:3]):
                    links = td.find_elements(By.CSS_SELECTOR, "a")
                    link_info = f"{len(links)} links" if links else "no links"
                    try:
                        cell_text = td.text[:30].encode('ascii', 'replace').decode('ascii')
                    except:
                        cell_text = "..."
                    print(f"[DEBUG] Cell {cell_idx}: '{cell_text}' ({link_info})")

        for idx in range(num_rows):
            try:
                # Check if we've hit the max limit
                if max_medicines > 0 and total_processed >= max_medicines:
                    print(f"[INFO] Reached max medicines limit ({max_medicines}), stopping")
                    return all_rows
                
                # re-fetch table and rows each time (DOM may change after modal close)
                table = get_medicine_table(driver)
                if not table:
                    break
                rows2 = table.find_elements(By.CSS_SELECTOR, "tbody tr")
                if idx >= len(rows2):
                    break

                row = rows2[idx]
                tds = row.find_elements(By.CSS_SELECTOR, "td")
                if len(tds) < 6:
                    if idx == 0:
                        print(f"[DEBUG] Row {idx+1} has only {len(tds)} cells, skipping")
                    continue

                sno = tds[0].text.strip()
                
                # Find the medicine link - try multiple selectors
                med_links = tds[1].find_elements(By.CSS_SELECTOR, "a")
                if not med_links:
                    # Try finding link in the entire row
                    med_links = row.find_elements(By.CSS_SELECTOR, "a[data-toggle='modal']")
                if not med_links:
                    med_links = row.find_elements(By.XPATH, ".//a[contains(@onclick, 'getOtherBrandPrice')]")
                if not med_links:
                    if idx == 0:
                        # Debug: print what's in td[1]
                        td1_html = tds[1].get_attribute('innerHTML')[:200] if len(tds) > 1 else "N/A"
                        print(f"[DEBUG] No medicine link in row {idx+1}. TD[1] content: {td1_html}")
                    continue
                med_link = med_links[0]
                med_name = med_link.text.strip()
                status = tds[2].text.strip()
                ceiling_price = tds[3].text.strip()
                unit = tds[4].text.strip()
                mrp_unit = tds[5].text.strip()

                # Check for duplicate (already processed this medicine in this session)
                med_key = f"{sno}_{med_name}"
                if med_key in processed_medicines:
                    print(f"[WARN] Skipping duplicate: {med_name} (row {idx+1})")
                    continue
                
                # Check if already in existing records (resume support)
                if med_name in existing_medicines:
                    total_skipped += 1
                    if total_skipped <= 5:  # Only print first few skips
                        try:
                            safe_name = med_name.encode('ascii', 'replace').decode('ascii')
                            print(f"[SKIP] Already processed: {safe_name}")
                        except:
                            print(f"[SKIP] Already processed: Medicine {idx+1}")
                    elif total_skipped == 6:
                        print(f"[SKIP] ... and more (suppressing further skip messages)")
                    continue
                
                processed_medicines.add(med_key)
                
                # Safe print for Windows console
                try:
                    safe_med_name = med_name.encode('ascii', 'replace').decode('ascii')
                except:
                    safe_med_name = f"Medicine {idx+1}"
                print(f"[INFO] Processing medicine {idx+1}/{num_rows}: {safe_med_name}")

                # Scroll the medicine link into view before clicking
                driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'instant'});", med_link)
                time.sleep(0.3)  # Wait for scroll to complete
                
                # open modal by clicking the medicine link
                click_js(driver, med_link)
                
                try:
                    wait_modal_open(wait)

                    # Extract header info from modal
                    header = parse_modal_header(driver)
                    subs = extract_modal_substitutes(driver)

                    base = {
                        "FormulationInput": formulation,
                        "SNo": sno,
                        "MedicineName": med_name,
                        "Status": status,
                        "CeilingPrice": ceiling_price,
                        "Unit": unit,
                        "MRP_Unit": mrp_unit,
                        "CapturedAt": datetime.now().isoformat(timespec="seconds"),
                        **header,
                    }

                    if subs:
                        for s in subs:
                            all_rows.append({**base, **s})
                    else:
                        all_rows.append(base)

                    close_modal(driver, wait)
                    total_processed += 1
                except Exception as modal_e:
                    errors_count += 1
                    error_msg = str(modal_e).encode('ascii', 'replace').decode('ascii')
                    print(f"[ERROR] Failed to extract modal data: {error_msg[:150]}")
                    # Try to close modal if open
                    try:
                        close_modal(driver, wait)
                    except:
                        pass
                    # Continue with next medicine instead of stopping
                    continue
                    
            except Exception as row_e:
                errors_count += 1
                error_msg = str(row_e).encode('ascii', 'replace').decode('ascii')
                print(f"[ERROR] Failed to process row {idx+1}: {error_msg[:150]}")
                continue
        
        # Print progress summary for this page
        print(f"[INFO] Page {page_num} complete: {total_processed} processed, {total_skipped} skipped, {errors_count} errors")

        # go next page if pagination exists
        # Use valid CSS selectors only (no jQuery :contains)
        next_btns = driver.find_elements(By.CSS_SELECTOR, "a.paginate_button.next:not(.disabled)")
        if not next_btns:
            # Try alternative pagination selectors with XPath
            next_btns = driver.find_elements(By.XPATH, "//a[contains(text(),'Next') and not(contains(@class,'disabled'))]")
        
        if not next_btns:
            break
        next_btn = next_btns[0]
        cls = next_btn.get_attribute("class") or ""
        if "disabled" in cls:
            break
        click_js(driver, next_btn)
        page_num += 1
        time.sleep(1.0)

    print(f"[OK] Finished processing: {total_processed} medicines, {len(all_rows)} detail rows, {total_skipped} skipped, {errors_count} errors")
    return all_rows


# -----------------------------
# Main run per formulation
# -----------------------------
def run_for_formulation(
    driver, 
    wait: WebDriverWait, 
    formulation: str, 
    download_dir: Path, 
    out_dir: Path,
    checkpoint: FormulationCheckpoint
) -> Dict[str, Any]:
    """
    Process a single formulation and return statistics.
    
    Returns:
        Dict with stats: {"medicines": int, "substitutes": int, "success": bool}
    """
    formulation_slug = slugify(formulation)
    stats = {"medicines": 0, "substitutes": 0, "success": False}
    
    # Mark as in progress
    checkpoint.mark_in_progress(formulation)

    driver.get(URL)
    
    # Dismiss any initial alerts
    dismiss_alert_if_present(driver)

    # Step 1-2: Click search field and enter formulation name
    inp = wait.until(EC.element_to_be_clickable((By.ID, "searchFormulation")))
    click_js(driver, inp)  # Click to focus
    time.sleep(0.5)
    inp.clear()
    time.sleep(0.3)
    
    # Type the formulation to trigger autocomplete
    inp.send_keys(formulation)
    time.sleep(1.5)  # Wait for autocomplete dropdown to appear
    
    # Trigger input event via JavaScript to ensure autocomplete fires
    driver.execute_script("arguments[0].dispatchEvent(new Event('input', { bubbles: true }));", inp)
    driver.execute_script("arguments[0].dispatchEvent(new Event('keyup', { bubbles: true }));", inp)
    time.sleep(1.0)

    # Step 3: Select exact match from autocomplete dropdown
    autocomplete_success = pick_autocomplete_exact_match(driver, wait, formulation, timeout=15)
    
    if not autocomplete_success:
        # Autocomplete failed - formulation may not exist in database
        safe_name = formulation.encode('ascii', 'replace').decode('ascii')
        print(f"[WARN] Autocomplete failed for '{safe_name}' - skipping this formulation")
        # Dismiss any alert that may have appeared
        dismiss_alert_if_present(driver)
        return stats

    # Step 4: Click GO button
    go = wait.until(EC.element_to_be_clickable((By.ID, "gobtn")))
    click_js(driver, go)
    
    # Check for alert after clicking GO
    time.sleep(0.5)
    if dismiss_alert_if_present(driver):
        safe_name = formulation.encode('ascii', 'replace').decode('ascii')
        print(f"[WARN] Alert appeared after GO for '{safe_name}' - skipping")
        return stats

    # Wait results injected
    wait_results_loaded(driver, wait)

    # Check if results table loaded - look for medicine links
    medicine_links = driver.find_elements(By.CSS_SELECTOR, "a[data-toggle='modal'][data-target='#exampleModal']")
    if not medicine_links:
        # Fallback: check for any table with links using XPath
        medicine_links = driver.find_elements(By.XPATH, "//table//a[contains(@onclick, 'getOtherBrandPrice')]")
    
    if not medicine_links:
        print(f"[WARN] No medicine links found for {formulation}.")
        return stats
    
    print(f"[OK] Found {len(medicine_links)} medicine links")

    # Save search table rows to CSV
    search_rows = extract_search_table_rows(driver)
    search_csv = out_dir / "search_results" / f"{formulation_slug}.csv"
    write_csv_rows(
        search_csv,
        search_rows,
        fieldnames=["SNo", "MedicineName", "Status", "CeilingPrice", "Unit", "MRP_Unit"],
        append=False  # Overwrite for search results
    )
    print(f"[OK] Search rows CSV: {search_csv} ({len(search_rows)} rows)")

    # Step 4: Click Excel button to download
    click_excel_export(driver, wait)
    downloaded = wait_for_download_complete(download_dir, timeout_sec=180)

    excel_target = out_dir / "excel_raw" / f"{formulation_slug}{downloaded.suffix}"
    excel_target.parent.mkdir(parents=True, exist_ok=True)
    if excel_target.exists():
        excel_target.unlink()
    downloaded.rename(excel_target)
    print(f"[OK] Excel saved: {excel_target}")

    exported_csv = out_dir / "excel_as_csv" / f"{formulation_slug}.csv"
    excel_to_csv(excel_target, exported_csv)
    print(f"[OK] Excel->CSV: {exported_csv}")

    # Step 5: Click each medicine link to get individual details
    # Check for existing records to support resume
    details_csv = out_dir / "details" / f"{formulation_slug}.csv"
    existing_medicines = get_existing_records(details_csv, "MedicineName")
    
    if existing_medicines:
        print(f"[RESUME] Found {len(existing_medicines)} already processed medicines for {formulation}")
    
    max_meds = getenv_int("MAX_MEDICINES_PER_FORMULATION", 5000)
    detail_rows = scrape_details_for_all_medicines(
        driver, wait, formulation, 
        max_medicines=max_meds,
        existing_medicines=existing_medicines
    )

    # Columns for details CSV (flattened; one row per substitute)
    detail_fieldnames = [
        "FormulationInput",
        "SNo",
        "MedicineName",
        "Status",
        "CeilingPrice",
        "Unit",
        "MRP_Unit",
        "CapturedAt",
        "Formulation_Brand",
        "CompanyName_Header",
        "MRP_Unit_Header",
        "MRP_AsOnDate",
        "MRP_Header",
        "Sub_SNo",
        "Sub_BrandName",
        "Sub_PackSize",
        "Sub_MRP",
        "Sub_MRP_Unit",
        "Sub_CompanyName",
    ]
    
    # Only write new rows (append mode for resume support)
    if detail_rows:
        write_csv_rows(details_csv, detail_rows, fieldnames=detail_fieldnames, append=True)
        print(f"[OK] Details CSV: {details_csv} ({len(detail_rows)} new rows)")
    else:
        print(f"[INFO] No new detail rows to write for {formulation}")
    
    # Calculate stats
    unique_medicines = set(r.get("MedicineName", "") for r in detail_rows)
    stats["medicines"] = len(unique_medicines)
    stats["substitutes"] = len(detail_rows)
    stats["success"] = True
    
    return stats


def generate_final_report(out_dir: Path, checkpoint: FormulationCheckpoint, total_formulations: int, start_time: datetime):
    """Generate a final summary report."""
    report_file = out_dir / "scraping_report.json"
    
    stats = checkpoint.get_stats()
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    # Count files in output directories
    details_dir = out_dir / "details"
    details_files = list(details_dir.glob("*.csv")) if details_dir.exists() else []
    
    # Count total rows in details
    total_detail_rows = 0
    for csv_file in details_files:
        try:
            df = pd.read_csv(csv_file, encoding='utf-8-sig')
            total_detail_rows += len(df)
        except:
            pass
    
    report = {
        "scraper": "India NPPA Pharma Sahi Daam",
        "generated_at": end_time.isoformat(),
        "duration_seconds": round(duration, 2),
        "duration_formatted": f"{int(duration // 3600)}h {int((duration % 3600) // 60)}m {int(duration % 60)}s",
        "summary": {
            "total_formulations_input": total_formulations,
            "formulations_completed": stats["total_processed"],
            "formulations_skipped": total_formulations - stats["total_processed"],
            "total_medicines_processed": stats["total_medicines"],
            "total_substitute_rows": stats["total_substitutes"],
            "errors": stats["errors"]
        },
        "output_files": {
            "details_csvs": len(details_files),
            "total_detail_rows": total_detail_rows
        },
        "output_directory": str(out_dir)
    }
    
    # Write report
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    # Print summary
    print("\n" + "=" * 70)
    print("FINAL SCRAPING REPORT")
    print("=" * 70)
    print(f"Duration: {report['duration_formatted']}")
    print(f"Formulations processed: {stats['total_processed']}/{total_formulations}")
    print(f"Total medicines: {stats['total_medicines']}")
    print(f"Total detail rows: {total_detail_rows}")
    print(f"Errors: {stats['errors']}")
    print(f"Report saved: {report_file}")
    print("=" * 70)
    
    return report


def main():
    print("=" * 60)
    print("India NPPA Scraper - Step 02: Get Medicine Details")
    print("=" * 60)
    
    start_time = datetime.now()
    
    # Get paths from platform config
    download_dir = get_download_dir()
    out_dir = get_output_dir()
    
    print(f"[CONFIG] Download dir: {download_dir}")
    print(f"[CONFIG] Output dir: {out_dir}")
    
    # Initialize checkpoint manager
    checkpoint = FormulationCheckpoint(out_dir)
    
    # Check for --fresh flag to clear checkpoint
    if "--fresh" in sys.argv:
        checkpoint.clear()
        print("[CONFIG] Starting fresh (checkpoint cleared)")
    
    # Load formulations from ceiling prices (Step 01 output) or fallback to input
    formulations = load_formulations_from_ceiling_prices(out_dir)
    if not formulations:
        formulations = load_formulations_from_input()
    if not formulations:
        formulations = DEFAULT_FORMULATIONS
        print(f"[CONFIG] Using default formulations: {formulations}")
    else:
        print(f"[CONFIG] Loaded {len(formulations)} formulations")
    
    # Apply max limit if configured
    max_formulations = getenv_int("MAX_FORMULATIONS", 0)
    if max_formulations > 0 and len(formulations) > max_formulations:
        formulations = formulations[:max_formulations]
        print(f"[CONFIG] Limited to {max_formulations} formulations")
    
    total_formulations = len(formulations)
    
    # Filter out already completed formulations
    pending_formulations = [f for f in formulations if not checkpoint.is_completed(f)]
    skipped_count = len(formulations) - len(pending_formulations)
    
    if skipped_count > 0:
        print(f"[RESUME] Skipping {skipped_count} already completed formulations")
        print(f"[RESUME] {len(pending_formulations)} formulations remaining")
    
    if not pending_formulations:
        print("[INFO] All formulations already completed!")
        generate_final_report(out_dir, checkpoint, total_formulations, start_time)
        return
    
    wait_seconds = getenv_int("WAIT_SECONDS", 60)
    
    driver = None
    try:
        driver = build_driver(download_dir)
        wait = WebDriverWait(driver, wait_seconds)

        for idx, f in enumerate(pending_formulations):
            f = (f or "").strip()
            if not f:
                continue
            
            # Calculate overall progress including skipped
            completed_so_far = skipped_count + idx
            progress_pct = round(((completed_so_far + 1) / total_formulations) * 100, 1)
            
            print(f"\n[PROGRESS] Processing {completed_so_far + 1}/{total_formulations} ({progress_pct}%)")
            print(f"=== Running formulation: {f} ===")
            
            try:
                stats = run_for_formulation(driver, wait, f, download_dir, out_dir, checkpoint)
                
                if stats["success"]:
                    checkpoint.mark_completed(f, stats["medicines"], stats["substitutes"])
                else:
                    checkpoint.mark_error(f)
                    
            except Exception as e:
                error_msg = str(e).encode('ascii', 'replace').decode('ascii')
                print(f"[ERROR] Failed to process {f}: {error_msg}")
                checkpoint.mark_error(f)
                continue

        print("\n" + "=" * 60)
        print("Medicine details extraction complete!")
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
    
    # Generate final report
    generate_final_report(out_dir, checkpoint, total_formulations, start_time)


if __name__ == "__main__":
    main()
