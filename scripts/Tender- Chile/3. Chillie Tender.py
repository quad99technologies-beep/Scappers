"""
Script 3: Extract Tender Details
=================================
Extracts detailed tender and lot information from MercadoPublico tender pages.

INPUTS:
  - Output/tender_redirect_urls.csv: Redirect URLs with qs parameters from Script 2
    Required column: tender_details_url

OUTPUTS:
  - Output/tender_details.csv: Tender and lot details
    Columns: Tender ID, Tender Title, TENDERING AUTHORITY, PROVINCE, Closing Date,
             Price Evaluation ratio, Quality Evaluation ratio, Other Evaluation ratio,
             Lot Number, Unique Lot ID, Generic name, Lot Title, Quantity, Source URL
"""

from __future__ import annotations

import json
import re
import time
import sys
import argparse
import os
import csv
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

_script_dir = Path(__file__).resolve().parent
_scraper_root = _script_dir
if str(_scraper_root) not in sys.path:
    sys.path.insert(0, str(_scraper_root))

from config_loader import load_env_file, getenv, getenv_bool, getenv_int, get_output_dir

try:
    from core.chrome_pid_tracker import get_chrome_pids_from_driver, save_chrome_pids, terminate_scraper_pids
except Exception:
    get_chrome_pids_from_driver = None
    save_chrome_pids = None
    terminate_scraper_pids = None

# OpenAI for translation (optional)
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    print("[WARN] OpenAI library not installed. Install with: pip install openai")


def clean(s):
    """Normalize whitespace in strings"""
    return re.sub(r"\s+", " ", str(s or "")).strip()


def translate_text(text: str, client: Optional[Any] = None) -> str:
    """
    Translate text from Spanish to English using OpenAI.
    Returns original text if translation fails or OpenAI is not available.
    """
    if not text or not text.strip():
        return text
    
    if not OPENAI_AVAILABLE:
        return text
    
    # Get API key from environment
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return text
    
    try:
        if client is None:
            client = OpenAI(api_key=api_key)
        
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system",
                    "content": "You are a professional translator. Translate the following Spanish text to English. Only return the translation, no explanations."
                },
                {
                    "role": "user",
                    "content": text
                }
            ],
            temperature=0.3,
            max_tokens=500
        )
        
        translated = response.choices[0].message.content.strip()
        return translated
    except Exception as e:
        print(f"[WARN] Translation failed for text: {e}")
        return text  # Return original on error


def translate_row(row: Dict[str, Any], client: Optional[Any] = None) -> Dict[str, Any]:
    """
    Translate text fields in a data row from Spanish to English.
    Only translates specific text fields, not IDs or numbers.
    """
    # Fields to translate (text fields only)
    text_fields = [
        "Tender Title",
        "TENDERING AUTHORITY",
        "PROVINCE",
        "Generic name",
        "Lot Title",
    ]
    
    translated_row = row.copy()
    
    for field in text_fields:
        if field in translated_row and translated_row[field]:
            translated_row[field] = translate_text(str(translated_row[field]), client)
            time.sleep(0.1)  # Small delay to avoid rate limits
    
    return translated_row


def extract_region_from_page(driver) -> str:
    for elem_id in [
        "lblFicha2Region",
        "lblRegion",
        "lblRegionLicitacion",
        "lblFicha2RegionLicitacion",
    ]:
        try:
            el = driver.find_element(By.ID, elem_id)
            val = clean(el.text)
            if val:
                return val
        except Exception:
            continue

    try:
        region_label = driver.find_element(
            By.XPATH,
            "//*[contains(translate(normalize-space(text()),"
            " 'ÁÉÍÓÚÜÑáéíóúüñ', 'AEIOUUNaeiouun'),"
            " 'region en que se genera la licitacion')]",
        )
        label_text = clean(region_label.text)
        if ":" in label_text:
            return clean(label_text.split(":", 1)[1])
        try:
            sibling = region_label.find_element(By.XPATH, "following-sibling::*[1]")
            sibling_text = clean(sibling.text)
            if sibling_text:
                return sibling_text
        except Exception:
            pass
    except Exception:
        pass

    try:
        body_text = driver.find_element(By.TAG_NAME, "body").text
        match = re.search(
            r"Regi[oó]n en que se genera la licitaci[oó]n\s*:\s*(.+)",
            body_text,
            flags=re.IGNORECASE,
        )
        if match:
            return clean(match.group(1).split("\n")[0])
    except Exception:
        pass

    return ""


def validate_url(url: str) -> bool:
    """Validate that URL is a MercadoPublico DetailsAcquisition page"""
    if not url or not isinstance(url, str):
        return False
    return "DetailsAcquisition.aspx" in url


# Constants
load_env_file()
INPUT_FILENAME = getenv("SCRIPT_02_OUTPUT_CSV", "tender_redirect_urls.csv")
OUTPUT_FILENAME = getenv("SCRIPT_03_OUTPUT_CSV", "tender_details.csv")
REQUIRED_INPUT_COLUMN = "tender_details_url"
MAX_TENDERS = getenv_int("MAX_TENDERS", 100)
HEADLESS_MODE = getenv_bool("HEADLESS", True)
DISABLE_IMAGES = getenv_bool("DISABLE_IMAGES", True)
DISABLE_CSS = getenv_bool("DISABLE_CSS", True)
PAGE_LOAD_TIMEOUT = getenv_int("SCRIPT_03_PAGE_LOAD_TIMEOUT", 120)
NAV_RETRIES = getenv_int("SCRIPT_03_NAV_RETRIES", 3)
NAV_RETRY_SLEEP = getenv_int("SCRIPT_03_NAV_RETRY_SLEEP", 5)
WAIT_AFTER_GET = getenv_int("SCRIPT_03_WAIT_AFTER_GET", 6)
SLEEP_BETWEEN_TENDERS = getenv_int("SCRIPT_03_SLEEP_BETWEEN_TENDERS", 3)


def build_driver(headless=False):
    """Create and configure Chrome WebDriver with performance logging"""
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--lang=es-CL")  # Match page language for better compatibility
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    prefs = {}
    if DISABLE_IMAGES:
        prefs["profile.managed_default_content_settings.images"] = 2
    if DISABLE_CSS:
        prefs["profile.managed_default_content_settings.stylesheets"] = 2
    if prefs:
        opts.add_experimental_option("prefs", prefs)
    
    # Set performance logging via Options (Selenium 4.x)
    opts.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    if get_chrome_pids_from_driver and save_chrome_pids:
        try:
            pids = get_chrome_pids_from_driver(driver)
            if pids:
                save_chrome_pids("Tender_Chile", _scraper_root.parents[1], pids)
        except Exception:
            pass
    return driver


def extract_tender_data(driver) -> Dict[str, Any]:
    """
    Extract tender-level information from the HTML page.
    Returns a dictionary with all tender fields.
    """
    tender_data = {
        "Tender ID": "",
        "Tender Title": "",
        "TENDERING AUTHORITY": "",
        "PROVINCE": "",
        "Closing Date": "",
        "Price Evaluation ratio": None,
        "Quality Evaluation ratio": None,
        "Other Evaluation ratio": None,
    }

    try:
        # Wait for page to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        print("[OK] Page loaded")

        # Extract Tender Title - use specific element ID
        try:
            title_elem = driver.find_element(By.ID, "lblFicha1Nombre")
            tender_data["Tender Title"] = clean(title_elem.text)
        except Exception as e:
            print(f"[WARN] Could not extract Tender Title: {e}")

        # Extract Tender ID - use specific element ID
        try:
            tender_id_elem = driver.find_element(By.ID, "lblNumLicitacion")
            tender_data["Tender ID"] = clean(tender_id_elem.text)
        except Exception as e:
            print(f"[WARN] Could not extract Tender ID: {e}")

        # Extract TENDERING AUTHORITY (Razón Social) - use specific element ID
        try:
            authority_elem = driver.find_element(By.ID, "lnkFicha2Razon")
            tender_data["TENDERING AUTHORITY"] = clean(authority_elem.text)
        except Exception:
            try:
                # Fallback to span if link not found
                authority_elem = driver.find_element(By.ID, "lblFicha2Razon")
                tender_data["TENDERING AUTHORITY"] = clean(authority_elem.text)
            except Exception as e:
                print(f"[WARN] Could not extract TENDERING AUTHORITY: {e}")

        # Extract PROVINCE (Region) - prefer region label, fallback to comuna
        try:
            region = extract_region_from_page(driver)
            if region:
                tender_data["PROVINCE"] = region
            else:
                comuna_elem = driver.find_element(By.ID, "lblFicha2Comuna")
                tender_data["PROVINCE"] = clean(comuna_elem.text)
        except Exception as e:
            print(f"[WARN] Could not extract PROVINCE: {e}")

        # Extract Closing Date (Fecha de cierre) - use specific element ID
        try:
            closing_date_elem = driver.find_element(By.ID, "lblFicha3Cierre")
            tender_data["Closing Date"] = clean(closing_date_elem.text)
        except Exception as e:
            print(f"[WARN] Could not extract Closing Date: {e}")

        # Extract Evaluation Ratios (Criterios de evaluación) - parse table
        try:
            # Find all criteria rows in the table
            criteria_rows = driver.find_elements(By.CSS_SELECTOR, "#grvCriterios tr.estiloSeparador")
            
            for row in criteria_rows:
                try:
                    # Get criterion name and percentage
                    nombre_elem = row.find_element(By.CSS_SELECTOR, "[id*='lblNombreCriterio']")
                    ponderacion_elem = row.find_element(By.CSS_SELECTOR, "[id*='lblPonderacion']")
                    
                    nombre = clean(nombre_elem.text).upper()
                    ponderacion_text = clean(ponderacion_elem.text)
                    
                    # Extract percentage number
                    pct_match = re.search(r"(\d+)%", ponderacion_text)
                    if pct_match:
                        pct = int(pct_match.group(1))
                        
                        if any(k in nombre for k in ["ECONOM", "PRICE", "PRECIO"]):
                            tender_data["Price Evaluation ratio"] = pct
                        elif any(k in nombre for k in ["TECN", "TECH", "CALIDAD"]):
                            tender_data["Quality Evaluation ratio"] = pct
                        else:
                            # Other criteria (sum them)
                            if tender_data["Other Evaluation ratio"] is None:
                                tender_data["Other Evaluation ratio"] = 0
                            tender_data["Other Evaluation ratio"] += pct
                except Exception:
                    continue
        except Exception as e:
            print(f"[WARN] Could not extract Evaluation Ratios: {e}")

    except Exception as e:
        print(f"[WARN] Error extracting tender data: {e}")

    return tender_data


def extract_items_from_html(driver) -> List[Dict[str, Any]]:
    """
    Extract items from HTML table (grvProducto) as fallback when XHR fails.
    Returns list of item dictionaries.
    """
    items = []
    try:
        # Find all product name elements (this ensures we get all products including the first one)
        producto_elems = driver.find_elements(By.CSS_SELECTOR, "[id^='grvProducto_ctl'][id*='lblProducto']")
        
        for producto_elem in producto_elems:
            try:
                # Get the control ID (e.g., "grvProducto_ctl02" from "grvProducto_ctl02_lblProducto")
                elem_id = producto_elem.get_attribute("id")
                control_match = re.search(r"(grvProducto_ctl\d+)", elem_id)
                if not control_match:
                    continue
                
                control_prefix = control_match.group(1)
                
                # Find related elements using the same control prefix
                cantidad_elem = driver.find_element(By.ID, f"{control_prefix}_lblCantidad")
                unidad_elem = driver.find_element(By.ID, f"{control_prefix}_lblUnidad")
                descripcion_elem = driver.find_element(By.ID, f"{control_prefix}_lblDescripcion")
                categoria_elem = driver.find_element(By.ID, f"{control_prefix}_lblCategoria")
                
                item = {
                    "CodigoProducto": clean(categoria_elem.text),  # Use category code as Unique Lot ID
                    "NombreProducto": clean(producto_elem.text),
                    "Descripcion": clean(descripcion_elem.text),
                    "Cantidad": clean(cantidad_elem.text),
                    "Unidad": clean(unidad_elem.text),
                }
                items.append(item)
            except Exception as e:
                # Skip items that don't have all required fields
                print(f"[WARN] Skipping item due to missing fields: {e}")
                continue
        
        if items:
            print(f"[OK] Extracted {len(items)} items from HTML table")
    except Exception as e:
        print(f"[WARN] Could not extract items from HTML: {e}")
    
    return items


def extract_items_from_network(driver) -> List[Dict[str, Any]]:
    """
    Capture XHR JSON responses and extract FichaItemLicitacionEntity items.
    Returns list of item dictionaries.
    """
    logs = driver.get_log("performance")
    print(f"[OK] Captured {len(logs)} performance log entries")

    # Debug: collect all XHR URLs for analysis
    xhr_urls = []
    
    for entry in logs:
        try:
            msg = json.loads(entry["message"])["message"]

            if msg["method"] != "Network.responseReceived":
                continue

            response = msg["params"]["response"]
            url = response.get("url", "")
            
            # Collect all XHR URLs for debugging
            if url and ("RfbService" in url or "GetItems" in url or "Item" in url or "Producto" in url):
                xhr_urls.append(url)

            # MercadoPublico items endpoint - try multiple patterns
            if "RfbService" not in url and "GetItems" not in url and "Item" not in url.lower():
                continue

            print(f"[OK] Found potential items XHR: {url[:100]}...")

            request_id = msg["params"]["requestId"]

            try:
                body = driver.execute_cdp_cmd(
                    "Network.getResponseBody", {"requestId": request_id}
                )
            except Exception as e:
                print(f"[WARN] Could not get response body: {e}")
                continue

            text = body.get("body", "")
            if not text:
                continue

            try:
                data = json.loads(text)
            except Exception as e:
                print(f"[WARN] Could not parse JSON: {e}")
                continue

            # Locate items array
            items = None
            if isinstance(data, dict):
                # Try to find items array in various structures
                for key, v in data.items():
                    if isinstance(v, list) and len(v) > 0:
                        # Check if it looks like item data
                        if isinstance(v[0], dict):
                            # Check for item-like keys
                            if any(key in v[0] for key in ["CodigoProducto", "NombreProducto", "Descripcion", "Cantidad"]):
                                items = v
                                print(f"[OK] Found items array in key '{key}' with {len(items)} items")
                                break
                
                # If not found, try nested structures
                if not items:
                    for v in data.values():
                        if isinstance(v, dict):
                            for nested_v in v.values():
                                if isinstance(nested_v, list) and len(nested_v) > 0:
                                    if isinstance(nested_v[0], dict) and any(
                                        key in nested_v[0] for key in ["CodigoProducto", "NombreProducto", "Descripcion"]
                                    ):
                                        items = nested_v
                                        print(f"[OK] Found items in nested structure with {len(items)} items")
                                        break
                            if items:
                                break

            if items:
                print(f"[OK] Extracted {len(items)} items from XHR")
                return items
            else:
                # Save response for debugging
                try:
                    with open("debug_xhr_response.json", "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    print(f"[WARN] Saved XHR response to debug_xhr_response.json for analysis")
                except:
                    pass

        except Exception as e:
            print(f"[WARN] Error processing XHR entry: {e}")
            continue

    # Debug: print all collected XHR URLs
    if xhr_urls:
        print(f"\n[INFO] Found {len(xhr_urls)} potential XHR URLs:")
        for url in xhr_urls[:10]:  # Show first 10
            print(f"   - {url[:120]}")
        if len(xhr_urls) > 10:
            print(f"   ... and {len(xhr_urls) - 10} more")
    else:
        print("[WARN] No XHR URLs matching item patterns found")

    return []


def extract_single_tender(url: str, headless: bool = False) -> Optional[Dict[str, Any]]:
    """Extract data from a single tender URL"""
    # Validate URL
    if not validate_url(url):
        print(f"[WARN] Invalid URL: {url}")
        return None

    driver = None
    try:
        # Build driver
        driver = build_driver(headless=headless)

        # Load page with retry
        last_err = None
        for attempt in range(NAV_RETRIES):
            try:
                driver.get(url)
                last_err = None
                break
            except (TimeoutException, WebDriverException) as e:
                last_err = e
                if attempt < NAV_RETRIES - 1:
                    time.sleep(NAV_RETRY_SLEEP * (attempt + 1))
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    driver = build_driver(headless=headless)
                else:
                    raise

        # URL already has qs parameter from Script 2, so no need to get redirect again
        # Just wait for page to load
        redirect_url = url  # URL already contains qs parameter

        # Wait for XHRs to complete
        time.sleep(WAIT_AFTER_GET)

        # Extract tender-level data
        tender_data = extract_tender_data(driver)

        # Extract lot-level data - try XHR first, then HTML fallback
        items = extract_items_from_network(driver)

        # If XHR fails, try extracting from HTML table
        if not items:
            items = extract_items_from_html(driver)

        if not items:
            items = []

        # Combine tender and lot data
        rows = []
        if items:
            for i, it in enumerate(items, start=1):
                row = {
                    **tender_data,
                    "Lot Number": i,
                    "Unique Lot ID": it.get("CodigoProducto", ""),
                    "Generic name": clean(it.get("NombreProducto", "")),
                    "Lot Title": clean(it.get("Descripcion", "")),
                    "Quantity": f"{it.get('Cantidad', '')} {it.get('Unidad', '')}".strip(),
                    "Source URL": redirect_url,  # Use redirect URL
                }
                rows.append(row)
        else:
            # Create single row with tender data only (no lots)
            row = {
                **tender_data,
                "Lot Number": None,
                "Unique Lot ID": None,
                "Generic name": None,
                "Lot Title": None,
                "Quantity": None,
                "Source URL": redirect_url,  # Use redirect URL
            }
            rows.append(row)

        return {
            "tender_data": tender_data,
            "lots": rows,
            "url": redirect_url  # Use redirect URL
        }

    except Exception as e:
        print(f"[ERROR] Error extracting tender: {e}")
        return None
    finally:
        if driver:
            driver.quit()


def main():
    """Main extraction function"""
    parser = argparse.ArgumentParser(
        description="Extract tender and lot data from MercadoPublico"
    )
    parser.add_argument(
        "url",
        nargs="?",
        default=None,
        help="MercadoPublico tender URL (or leave empty to process tender_list.csv)",
    )
    args = parser.parse_args()

    output_dir = get_output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)

    if terminate_scraper_pids:
        try:
            terminate_scraper_pids("Tender_Chile", _scraper_root.parents[1], silent=True)
        except Exception:
            pass

    # If URL provided, process single tender
    if args.url:
        url = args.url
        if not validate_url(url):
            print("[ERROR] ERROR: Invalid URL. Must contain 'DetailsAcquisition.aspx'")
            sys.exit(1)

        print(f"[INFO] Extracting data from: {url}")
        print("=" * 80)

        result = extract_single_tender(url, headless=HEADLESS_MODE)
        if not result:
            sys.exit(1)

        # Save single tender result
        df = pd.DataFrame(result["lots"])
        csv_filename = output_dir / "tender_details_single.csv"
        df.to_csv(csv_filename, index=False, encoding="utf-8-sig")
        print(f"[OK] CSV written: {csv_filename}")
        if terminate_scraper_pids:
            try:
                terminate_scraper_pids("Tender_Chile", _scraper_root.parents[1], silent=True)
            except Exception:
                pass
        return

    # Otherwise, process tender_redirect_urls.csv (from Script 2)
    input_path = output_dir / INPUT_FILENAME
    if not input_path.exists():
        print(f"[ERROR] ERROR: {input_path} not found. Run Script 2 first.")
        sys.exit(1)

    # Validate required column exists
    with open(input_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        if REQUIRED_INPUT_COLUMN not in reader.fieldnames:
            print(f"[ERROR] ERROR: Required column '{REQUIRED_INPUT_COLUMN}' not found in {input_path}")
            print(f"   Available columns: {', '.join(reader.fieldnames or [])}")
            sys.exit(1)

    print(f"[INFO] Reading redirect URLs from: {input_path}")
    print("=" * 80)

    # Read URLs with qs parameters from CSV
    tender_urls = []
    skipped_count = 0
    with open(input_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row_num, row in enumerate(reader, start=2):  # Start at 2 (row 1 is header)
            # Use the tender_details_url which has qs parameter
            url = row.get(REQUIRED_INPUT_COLUMN, '').strip()
            if url:
                # Validate URL format (must start with http)
                if (url.startswith('http://') or url.startswith('https://')) and validate_url(url):
                    tender_urls.append(url)
                else:
                    skipped_count += 1
                    if skipped_count <= 3:  # Only show first 3 warnings
                        print(f"   [WARN]  Row {row_num}: Invalid or empty URL: {url[:60] if url else '(empty)'}...")
            else:
                skipped_count += 1
    
    if not tender_urls:
        print("[ERROR] No valid tender URLs found in CSV")
        if skipped_count > 0:
            print(f"   Skipped {skipped_count} rows with invalid/empty URLs")
        sys.exit(1)
    
    if skipped_count > 0:
        print(f"   [WARN]  Skipped {skipped_count} rows with invalid/empty URLs")

    print(f"[OK] Found {len(tender_urls)} tender URLs to process (using qs parameters)")
    print("=" * 80)

    all_results = []
    
    for i, url in enumerate(tender_urls, 1):
        print(f"\n[{i}/{len(tender_urls)}] Processing: {url[:60]}...")
        result = extract_single_tender(url, headless=HEADLESS_MODE)
        if result:
            all_results.extend(result["lots"])
            print(f"   [OK] Completed ({len(result['lots'])} lots)")
        else:
            print(f"   [WARN]  Failed")
        if SLEEP_BETWEEN_TENDERS > 0:
            time.sleep(SLEEP_BETWEEN_TENDERS)

    if not all_results:
        print("\n[ERROR] No tender data extracted")
        sys.exit(1)

    # Create DataFrame
    df = pd.DataFrame(all_results)

    # Ensure column order matches specification
    column_order = [
        "Tender ID",
        "Tender Title",
        "TENDERING AUTHORITY",
        "PROVINCE",
        "Closing Date",
        "Price Evaluation ratio",
        "Quality Evaluation ratio",
        "Other Evaluation ratio",
        "Lot Number",
        "Unique Lot ID",
        "Generic name",
        "Lot Title",
        "Quantity",
        "Source URL",
    ]
    df = df.reindex(columns=column_order)

    # Write CSV
    csv_filename = output_dir / OUTPUT_FILENAME
    df.to_csv(csv_filename, index=False, encoding="utf-8-sig")
    print(f"\n[OK] CSV written: {csv_filename}")

    # Summary
    print("=" * 80)
    print("[OK] SUCCESS")
    print(f"[INFO] Extracted {len(all_results)} row(s)")
    print(f"[INFO] Processed {len(tender_urls)} tender(s)")
    print("\n[INFO] Preview:")
    print(df.head(10).to_string())

    if terminate_scraper_pids:
        try:
            terminate_scraper_pids("Tender_Chile", _scraper_root.parents[1], silent=True)
        except Exception:
            pass


if __name__ == "__main__":
    main()
