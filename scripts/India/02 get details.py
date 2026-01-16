#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
India NPPA Pharma Sahi Daam Scraper - Step 02: Get Medicine Details

Searches for formulations, downloads Excel exports, and extracts detailed
medicine information including substitutes/available brands.
"""

import os
import re
import time
import csv
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional

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
# CONFIG - loaded from env/input
# -----------------------------
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
    # "AMLODIPINE",
    # "PARACETAMOL",
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


def pick_autocomplete_exact_match(driver, wait: WebDriverWait, search_term: str, timeout: int = 10):
    """
    Wait for autocomplete dropdown and click on the item that exactly matches the search term.
    If no exact match found, click the first item.
    """
    # Try multiple possible autocomplete selectors
    autocomplete_selectors = [
        "ul.ui-autocomplete li.ui-menu-item",
        "ul.ui-autocomplete li",
        ".ui-autocomplete .ui-menu-item",
        ".ui-autocomplete-results li",
        ".autocomplete-suggestions div",
        "ul.ui-menu li.ui-menu-item",
        ".ui-menu-item",
        "[role='option']",
        ".dropdown-menu li",
        ".typeahead li",
    ]
    
    items = None
    end_time = time.time() + timeout
    
    print(f"[DEBUG] Looking for autocomplete dropdown for '{search_term}'...")
    
    while time.time() < end_time:
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
        print(f"[WARN] No autocomplete dropdown found for '{search_term}'.")
        print(f"[WARN] Proceeding without autocomplete selection.")
        return
    
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
                return
        except Exception as e:
            continue
    
    # If no exact match, click the first item
    if items:
        try:
            safe_text = items[0].text.strip().encode('ascii', 'replace').decode('ascii')
            print(f"[INFO] No exact match for '{search_term}', clicking first item: {safe_text}")
        except:
            print(f"[INFO] No exact match for '{search_term}', clicking first item")
        click_js(driver, items[0])
        time.sleep(0.5)


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


def write_csv_rows(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists()
    with path.open("a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if new_file:
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


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


def scrape_details_for_all_medicines(driver, wait: WebDriverWait, formulation: str, max_medicines: int = 0) -> List[Dict[str, Any]]:
    """
    Click each medicine link on the current results view (handles paging best-effort),
    extract modal header + substitutes, and flatten into CSV rows:
    one row per substitute (or one row with blanks if no substitute rows).
    
    Args:
        max_medicines: Maximum number of medicines to process (0 = no limit)
    """
    all_rows = []
    total_processed = 0
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

                # Check for duplicate (already processed this medicine)
                med_key = f"{sno}_{med_name}"
                if med_key in processed_medicines:
                    print(f"[WARN] Skipping duplicate: {med_name} (row {idx+1})")
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
        print(f"[INFO] Page {page_num} complete: {total_processed} processed, {errors_count} errors so far")

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

    print(f"[OK] Finished processing: {total_processed} medicines, {len(all_rows)} detail rows, {errors_count} errors")
    return all_rows


# -----------------------------
# Main run per formulation
# -----------------------------
def run_for_formulation(driver, wait: WebDriverWait, formulation: str, download_dir: Path, out_dir: Path):
    formulation_slug = slugify(formulation)

    driver.get(URL)

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
    pick_autocomplete_exact_match(driver, wait, formulation, timeout=15)

    # Step 4: Click GO button
    go = wait.until(EC.element_to_be_clickable((By.ID, "gobtn")))
    click_js(driver, go)

    # Wait results injected
    wait_results_loaded(driver, wait)

    # Check if results table loaded - look for medicine links
    medicine_links = driver.find_elements(By.CSS_SELECTOR, "a[data-toggle='modal'][data-target='#exampleModal']")
    if not medicine_links:
        # Fallback: check for any table with links using XPath
        medicine_links = driver.find_elements(By.XPATH, "//table//a[contains(@onclick, 'getOtherBrandPrice')]")
    
    if not medicine_links:
        print(f"[WARN] No medicine links found for {formulation}.")
        return
    
    print(f"[OK] Found {len(medicine_links)} medicine links")

    # Save search table rows to CSV
    search_rows = extract_search_table_rows(driver)
    search_csv = out_dir / "search_results" / f"{formulation_slug}.csv"
    write_csv_rows(
        search_csv,
        search_rows,
        fieldnames=["SNo", "MedicineName", "Status", "CeilingPrice", "Unit", "MRP_Unit"],
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

    # Step 5: Click each medicine link (e.g., "ABAMUNE 300 MG TABLET 30") to get individual details
    # Limit to 50 medicines per formulation to avoid excessive processing
    max_meds = getenv_int("MAX_MEDICINES_PER_FORMULATION", 50)
    detail_rows = scrape_details_for_all_medicines(driver, wait, formulation, max_medicines=max_meds)
    details_csv = out_dir / "details" / f"{formulation_slug}.csv"

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
    write_csv_rows(details_csv, detail_rows, fieldnames=detail_fieldnames)
    print(f"[OK] Details CSV: {details_csv} ({len(detail_rows)} rows)")


def main():
    print("=" * 60)
    print("India NPPA Scraper - Step 02: Get Medicine Details")
    print("=" * 60)
    
    # Get paths from platform config
    download_dir = get_download_dir()
    out_dir = get_output_dir()
    
    print(f"[CONFIG] Download dir: {download_dir}")
    print(f"[CONFIG] Output dir: {out_dir}")
    
    # Load formulations from input file or use defaults
    formulations = load_formulations_from_input()
    if not formulations:
        formulations = DEFAULT_FORMULATIONS
        print(f"[CONFIG] Using default formulations: {formulations}")
    else:
        print(f"[CONFIG] Loaded {len(formulations)} formulations from input file")
    
    # Apply max limit if configured
    max_formulations = getenv_int("MAX_FORMULATIONS", 0)
    if max_formulations > 0 and len(formulations) > max_formulations:
        formulations = formulations[:max_formulations]
        print(f"[CONFIG] Limited to {max_formulations} formulations")
    
    wait_seconds = getenv_int("WAIT_SECONDS", 60)
    
    driver = None
    try:
        driver = build_driver(download_dir)
        wait = WebDriverWait(driver, wait_seconds)

        for idx, f in enumerate(formulations):
            f = (f or "").strip()
            if not f:
                continue
            
            # Progress output for GUI
            progress_pct = round(((idx + 1) / len(formulations)) * 100, 1)
            print(f"\n[PROGRESS] Processing {idx+1}/{len(formulations)} ({progress_pct}%)")
            print(f"=== Running formulation: {f} ===")
            
            try:
                run_for_formulation(driver, wait, f, download_dir, out_dir)
            except Exception as e:
                print(f"[ERROR] Failed to process {f}: {e}")
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


if __name__ == "__main__":
    main()
