#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 2: Extract Tender Details
==============================
Extracts detailed tender and lot information from MercadoPublico tender pages.

INPUTS:
  - output/Tender_Chile/tender_redirect_urls.csv: Redirect URLs with qs parameters from Step 1
    Required column: tender_details_url

OUTPUTS:
  - output/Tender_Chile/tender_details.csv: Tender and lot details
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

# Add repo root to path for imports
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add scripts/Tender- Chile to path for config_loader
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# Import config_loader for platform integration
try:
    from config_loader import (
        load_env_file, getenv, getenv_int, getenv_bool,
        get_output_dir as _get_output_dir
    )
    load_env_file()
    _CONFIG_LOADER_AVAILABLE = True
except ImportError:
    _CONFIG_LOADER_AVAILABLE = False

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


def extract_numeric_quantity(raw_qty: str) -> str:
    """Return only the numeric part of a quantity string.

    Examples:
      "1000" -> "1000"
      "1.000" -> "1000"
      "1,000" -> "1000"
      "1000 Ampoules" -> "1000"
      "" -> ""
    """
    s = clean(raw_qty)
    if not s:
        return ""

    # grab the first number-like token
    m = re.search(r"(\d[\d\.,\s]*)", s)
    if not m:
        return ""

    token = m.group(1)
    token = token.replace(" ", "")

    # normalize thousands separators
    # if both separators exist, assume last one is decimal and drop decimals
    if "," in token and "." in token:
        if token.rfind(",") > token.rfind("."):
            token = token.replace(".", "").split(",", 1)[0]
        else:
            token = token.replace(",", "").split(".", 1)[0]
    elif "," in token:
        parts = token.split(",")
        # treat comma as thousand separator in most tender quantities
        token = "".join(parts)
    elif "." in token:
        parts = token.split(".")
        token = "".join(parts)

    token = re.sub(r"\D", "", token)
    return token


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
    
    NOTE: TENDERING AUTHORITY and PROVINCE must remain in original language
    per EVERSANA requirements. Only translate Tender Title, Generic name, Lot Title.
    """
    # Fields to translate (do NOT translate authority/province)
    text_fields = [
        "Tender Title",
        "Generic name",
        "Lot Title",
    ]
    
    translated_row = row.copy()
    
    for field in text_fields:
        if field in translated_row and translated_row[field]:
            translated_row[field] = translate_text(str(translated_row[field]), client)
            time.sleep(0.1)  # Small delay to avoid rate limits
    
    return translated_row


def validate_url(url: str) -> bool:
    """Validate that URL is a MercadoPublico DetailsAcquisition page"""
    if not url or not isinstance(url, str):
        return False
    return "DetailsAcquisition.aspx" in url


# Constants
INPUT_FILENAME = "tender_redirect_urls.csv"
OUTPUT_FILENAME = "tender_details.csv"
REQUIRED_INPUT_COLUMN = "tender_details_url"

# Constants from config
if _CONFIG_LOADER_AVAILABLE:
    MAX_TENDERS = getenv_int("MAX_TENDERS", 100)
    HEADLESS_MODE = getenv_bool("HEADLESS", True)
else:
    MAX_TENDERS = int(os.getenv("MAX_TENDERS", "100"))
    HEADLESS_MODE = os.getenv("HEADLESS", "True").lower() == "true"


def get_output_dir() -> Path:
    """Get standardized output directory path - uses platform config if available"""
    if _CONFIG_LOADER_AVAILABLE:
        return _get_output_dir()
    return _repo_root / "output" / "Tender_Chile"


def build_driver(headless=False):
    """Create and configure Chrome WebDriver with performance logging"""
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--lang=es-CL")  # Match page language for better compatibility
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    
    # Set performance logging via Options (Selenium 4.x)
    opts.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    return webdriver.Chrome(options=opts)


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

        # Extract PROVINCE (prefer Region if present; else fallback to Comuna)
        province_val = ""
        try:
            # Some pages expose region; if not found, fallback to comuna
            region_elem = driver.find_element(By.ID, "lblFicha2Region")
            province_val = clean(region_elem.text)
        except Exception:
            pass

        if not province_val:
            try:
                comuna_elem = driver.find_element(By.ID, "lblFicha2Comuna")
                province_val = clean(comuna_elem.text)
            except Exception as e:
                print(f"[WARN] Could not extract PROVINCE: {e}")

        tender_data["PROVINCE"] = province_val

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

                        # Price bucket
                        if ("ECONOM" in nombre) or ("PRECIO" in nombre) or ("PRICE" in nombre):
                            tender_data["Price Evaluation ratio"] = pct

                        # Quality bucket
                        elif ("TECNIC" in nombre) or ("TECNICO" in nombre) or ("TECHNICALL" in nombre) or ("TECHNICAL" in nombre):
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
                    "Cantidad": extract_numeric_quantity(cantidad_elem.text),
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
        print(f"\n Found {len(xhr_urls)} potential XHR URLs:")
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
        max_retries = 2
        for attempt in range(max_retries):
            try:
                driver.get(url)
                break
            except WebDriverException as e:
                if attempt < max_retries - 1:
                    time.sleep(3)
                else:
                    raise

        # URL already has qs parameter from Script 2, so no need to get redirect again
        # Just wait for page to load
        redirect_url = url  # URL already contains qs parameter

        # Wait for XHRs to complete
        time.sleep(6)

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
                    # EVERSANA: numeric quantity only (unit omitted)
                    "Quantity": extract_numeric_quantity(str(it.get('Cantidad', ''))),
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

    # If URL provided, process single tender
    if args.url:
        url = args.url
        if not validate_url(url):
            print("[ERROR] ERROR: Invalid URL. Must contain 'DetailsAcquisition.aspx'")
            sys.exit(1)

        print(f" Extracting data from: {url}")
        print("=" * 80)

        result = extract_single_tender(url, headless=HEADLESS_MODE)
        if not result:
            sys.exit(1)

        # Save single tender result
        df = pd.DataFrame(result["lots"])
        csv_filename = output_dir / "tender_details_single.csv"
        df.to_csv(csv_filename, index=False, encoding="utf-8-sig")
        print(f"[OK] CSV written: {csv_filename}")
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

    print(f" Reading redirect URLs from: {input_path}")
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
        time.sleep(2)  # Delay between requests

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
    print(f" Extracted {len(all_results)} row(s)")
    print(f" Processed {len(tender_urls)} tender(s)")
    print("\n Preview:")
    print(df.head(10).to_string())


if __name__ == "__main__":
    main()

