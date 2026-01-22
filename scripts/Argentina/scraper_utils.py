"""
Shared utilities for Argentina scraper scripts.
Contains common functions used by both API and Selenium scrapers.
"""

import csv
import re
import time
import logging
import threading
from pathlib import Path
from datetime import datetime
from typing import Optional, Set, Tuple

from config_loader import (
    get_input_dir, get_output_dir,
    IGNORE_LIST_FILE, PREPARED_URLS_FILE,
    OUTPUT_PRODUCTS_CSV, OUTPUT_PROGRESS_CSV, OUTPUT_ERRORS_CSV
)

# ====== LOGGING ======
log = logging.getLogger("scraper_utils")

# ====== PATHS ======
INPUT_DIR = get_input_dir()
OUTPUT_DIR = get_output_dir()
PREPARED_URLS_FILE_PATH = OUTPUT_DIR / PREPARED_URLS_FILE
OUT_CSV = OUTPUT_DIR / OUTPUT_PRODUCTS_CSV
PROGRESS = OUTPUT_DIR / OUTPUT_PROGRESS_CSV
ERRORS = OUTPUT_DIR / OUTPUT_ERRORS_CSV

# ====== LOCKS ======
CSV_LOCK = threading.Lock()
PROGRESS_LOCK = threading.Lock()
ERROR_LOCK = threading.Lock()

# ====== OUTPUT FIELDS ======
OUT_FIELDS = [
    "input_company", "input_product_name",
    "company", "product_name",
    "active_ingredient", "therapeutic_class",
    "description", "price_ars", "date", "scraped_at",
    # five coverage fields (priority)
    "SIFAR_detail", "PAMI_AF", "PAMI_OS", "IOMA_detail", "IOMA_AF", "IOMA_OS",
    # extras
    "import_status", "coverage_json"
]

# ====== UTILITY FUNCTIONS ======

def ts() -> str:
    """Get current timestamp as ISO string."""
    return datetime.now().isoformat(timespec="seconds")

def strip_accents(s: str) -> str:
    """Remove accents from string."""
    import unicodedata
    return "".join(ch for ch in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(ch))

def nk(s: Optional[str]) -> str:
    """Normalize string for comparison (lowercase, no accents, single spaces)."""
    if not s:
        return ""
    normalized = strip_accents(s.strip())
    return re.sub(r"\s+", " ", normalized).lower()

# ====== CSV IO ======

def ensure_headers():
    """Ensure output CSV files have headers."""
    if not OUT_CSV.exists():
        with open(OUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
            csv.DictWriter(f, fieldnames=OUT_FIELDS).writeheader()
    if not PROGRESS.exists():
        with open(PROGRESS, "w", newline="", encoding="utf-8-sig") as f:
            csv.writer(f).writerow(["input_company","input_product_name","timestamp","records_found"])
    if not ERRORS.exists():
        with open(ERRORS, "w", newline="", encoding="utf-8-sig") as f:
            csv.writer(f).writerow(["input_company","input_product_name","timestamp","error_message"])

def load_progress_set() -> Set[Tuple[str, str]]:
    """Load products from progress file (alfabeta_progress.csv)."""
    done = set()
    if PROGRESS.exists():
        try:
            # Try multiple encodings to handle different file encodings
            for encoding in ["utf-8-sig", "utf-8", "latin-1", "windows-1252", "cp1252"]:
                try:
                    with open(PROGRESS, encoding=encoding) as f:
                        r = csv.DictReader(f)
                        for row in r:
                            company = (row.get("input_company") or "").strip()
                            product = (row.get("input_product_name") or "").strip()
                            records_raw = (row.get("records_found") or "").strip()
                            try:
                                records_val = int(float(records_raw)) if records_raw else 0
                            except ValueError:
                                records_val = 0
                            if company and product and records_val > 0:
                                done.add((nk(company), nk(product)))
                    break  # Success, exit encoding loop
                except UnicodeDecodeError:
                    continue  # Try next encoding
        except Exception as e:
            log.warning(f"[PROGRESS] Failed to load progress file: {e}")
    return done

def load_output_set() -> Set[Tuple[str, str]]:
    """Load products from output file (alfabeta_products_by_product.csv).
    Returns a set of normalized (company, product) tuples that already have data."""
    done = set()
    if OUT_CSV.exists():
        try:
            # Try multiple encodings to handle different file encodings
            for encoding in ["utf-8-sig", "utf-8", "latin-1", "windows-1252", "cp1252"]:
                try:
                    with open(OUT_CSV, encoding=encoding) as f:
                        r = csv.DictReader(f)
                        for row in r:
                            company = (row.get("input_company") or "").strip()
                            product = (row.get("input_product_name") or "").strip()
                            if company and product:
                                done.add((nk(company), nk(product)))
                    break  # Success, exit encoding loop
                except UnicodeDecodeError:
                    continue  # Try next encoding
        except Exception as e:
            log.warning(f"[OUTPUT] Failed to load output file: {e}")
    return done

def load_ignore_list() -> Set[Tuple[str, str]]:
    """Load ignore list from input/Argentina/IGNORE_LIST_FILE (Company, Product format).
    Returns a set of normalized (company, product) tuples to skip."""
    ignore_set = set()
    ignore_file = INPUT_DIR / IGNORE_LIST_FILE
    if ignore_file.exists():
        try:
            # Try multiple encodings to handle different file encodings
            for encoding in ["utf-8-sig", "utf-8", "latin-1", "windows-1252", "cp1252"]:
                try:
                    with open(ignore_file, encoding=encoding) as f:
                        r = csv.DictReader(f)
                        headers = {nk(h): h for h in (r.fieldnames or [])}
                        pcol = headers.get(nk("Product")) or headers.get("product") or "Product"
                        ccol = headers.get(nk("Company")) or headers.get("company") or "Company"
                        for row in r:
                            prod = (row.get(pcol) or "").strip()
                            comp = (row.get(ccol) or "").strip()
                            if prod and comp:
                                ignore_set.add((nk(comp), nk(prod)))
                    break  # Success, exit encoding loop
                except UnicodeDecodeError:
                    continue  # Try next encoding
            log.info(f"[IGNORE_LIST] Loaded {len(ignore_set)} combinations from {IGNORE_LIST_FILE}")
        except Exception as e:
            log.warning(f"[IGNORE_LIST] Failed to load {IGNORE_LIST_FILE}: {e}")
    else:
        log.info(f"[IGNORE_LIST] No {IGNORE_LIST_FILE} found in {INPUT_DIR} (optional file)")
    return ignore_set

def combine_skip_sets() -> Set[Tuple[str, str]]:
    """Combine all three skip sources: progress, output, and ignore list.
    Returns a set of normalized (company, product) tuples."""
    progress_set = load_progress_set()
    output_set = load_output_set()
    ignore_set = load_ignore_list()
    
    skip_set = progress_set | output_set | ignore_set
    
    log.info(f"[SKIP_SET] Loaded skip_set size = {len(skip_set)} (progress={len(progress_set)}, products={len(output_set)}, ignore={len(ignore_set)})")
    
    return skip_set

def append_progress(company: str, product: str, count: int):
    """Append progress entry to progress file. Always writes count as string (0 not blank)."""
    with PROGRESS_LOCK, open(PROGRESS, "a", newline="", encoding="utf-8-sig") as f:
        # Convert count to string explicitly - ensures 0 is written as "0" not blank
        csv.writer(f).writerow([company, product, ts(), str(count)])

def append_error(company: str, product: str, msg: str):
    """Append error entry to error file."""
    with ERROR_LOCK, open(ERRORS, "a", newline="", encoding="utf-8-sig") as f:
        csv.writer(f).writerow([company, product, ts(), msg[:5000]])

def append_rows(rows: list):
    """Append rows to output CSV file."""
    if not rows:
        return
    with CSV_LOCK, open(OUT_CSV, "a", newline="", encoding="utf-8-sig") as f:
        csv.DictWriter(f, fieldnames=OUT_FIELDS, extrasaction="ignore").writerows(rows)

def is_product_already_scraped(company: str, product: str) -> bool:
    """Check if a product is already marked as scraped in Productlist_with_urls.csv.
    Thread-safe: uses CSV_LOCK to prevent race conditions.
    
    Args:
        company: Company name to check
        product: Product name to check
    
    Returns:
        True if product is already marked as Scraped_By_Selenium == "yes", False otherwise
    """
    if not PREPARED_URLS_FILE_PATH.exists():
        return False  # File doesn't exist, not scraped
    
    try:
        with CSV_LOCK:
            # Try multiple encodings
            encoding_attempts = ["utf-8-sig", "utf-8", "latin-1", "windows-1252", "cp1252"]
            for encoding in encoding_attempts:
                try:
                    with open(PREPARED_URLS_FILE_PATH, "r", encoding=encoding, newline="") as f:
                        reader = csv.DictReader(f)
                        if not reader.fieldnames:
                            return False
                        
                        scraped_selenium_col = None
                        scraped_api_col = None
                        for col in reader.fieldnames:
                            col_norm = nk(col)
                            if col_norm == nk("Scraped_By_Selenium"):
                                scraped_selenium_col = col
                            elif col_norm == nk("Scraped_By_API"):
                                scraped_api_col = col
                        
                        if not scraped_selenium_col and not scraped_api_col:
                            return False  # Columns don't exist
                        
                        # Check each row
                        for row in reader:
                            row_company = (row.get("Company") or "").strip()
                            row_product = (row.get("Product") or "").strip()
                            
                            if nk(row_company) == nk(company) and nk(row_product) == nk(product):
                                selenium_value = (row.get(scraped_selenium_col) or "").strip().lower() if scraped_selenium_col else ""
                                api_value = (row.get(scraped_api_col) or "").strip().lower() if scraped_api_col else ""
                                return selenium_value == "yes" or api_value == "yes"
                        
                        return False  # Product not found in CSV
                except UnicodeDecodeError:
                    continue
                except Exception:
                    return False  # Error reading, assume not scraped
    except Exception:
        return False  # Error accessing file, assume not scraped
    
    return False

def update_prepared_urls_source(company: str, product: str, new_source: str = "selenium", 
                                 scraped_by_selenium: str = None, scraped_by_api: str = None,
                                 selenium_records: str = None, api_records: str = None):
    """Update the Source, Scraped_By_Selenium, Scraped_By_API, Selenium_Records, and API_Records columns in Productlist_with_urls.csv.
    Thread-safe: uses CSV_LOCK to prevent race conditions when multiple threads update the file.
    Handles multiple encodings when reading, writes with utf-8-sig.
    
    Args:
        company: Company name to match
        product: Product name to match
        new_source: New Source value (e.g., "selenium" or "api")
        scraped_by_selenium: New Scraped_By_Selenium value (e.g., "yes" or "no"), None to skip update
        scraped_by_api: New Scraped_By_API value (e.g., "yes" or "no"), None to skip update
        selenium_records: New Selenium_Records value (number as string), None to skip update
        api_records: New API_Records value (number as string), None to skip update
    """
    if not PREPARED_URLS_FILE_PATH.exists():
        return  # File doesn't exist, skip update
    
    try:
        # Use lock for both read and write to make operation atomic
        with CSV_LOCK:
            # Read all rows - try multiple encodings
            rows = []
            fieldnames = None
            encoding_used = None
            
            encoding_attempts = ["utf-8-sig", "utf-8", "latin-1", "windows-1252", "cp1252"]
            for encoding in encoding_attempts:
                try:
                    with open(PREPARED_URLS_FILE_PATH, "r", encoding=encoding, newline="") as f:
                        reader = csv.DictReader(f)
                        fieldnames = reader.fieldnames
                        if not fieldnames:
                            return
                        
                        for row in reader:
                            rows.append(row)
                        encoding_used = encoding
                        break  # Success, exit encoding loop
                except UnicodeDecodeError:
                    continue  # Try next encoding
                except Exception as e:
                    log.warning(f"[CSV_UPDATE] Error reading with {encoding}: {e}")
                    continue
            
            if encoding_used is None:
                log.warning(f"[CSV_UPDATE] Failed to read file with any encoding")
                return
            
            # Process rows to find and update matching entry
            updated = False
            for row in rows:
                # Normalize for comparison
                row_company = (row.get("Company") or "").strip()
                row_product = (row.get("Product") or "").strip()
                
                # Update columns if match found
                if nk(row_company) == nk(company) and nk(row_product) == nk(product):
                    update_made = False
                    
                    # Update Source if provided and different
                    if new_source and row.get("Source", "").lower() != new_source.lower():
                        row["Source"] = new_source
                        update_made = True
                    
                    # Update Scraped_By_Selenium if provided
                    if scraped_by_selenium is not None:
                        if "Scraped_By_Selenium" not in row or row.get("Scraped_By_Selenium", "").lower() != scraped_by_selenium.lower():
                            row["Scraped_By_Selenium"] = scraped_by_selenium
                            update_made = True
                    
                    # Update Scraped_By_API if provided
                    if scraped_by_api is not None:
                        if "Scraped_By_API" not in row or row.get("Scraped_By_API", "").lower() != scraped_by_api.lower():
                            row["Scraped_By_API"] = scraped_by_api
                            update_made = True
                    
                    # Update Selenium_Records if provided (ensure "0" not blank)
                    if selenium_records is not None:
                        # Normalize to ensure "0" not blank
                        selenium_str = str(selenium_records).strip() if str(selenium_records).strip() else "0"
                        if "Selenium_Records" not in row or row.get("Selenium_Records", "0") != selenium_str:
                            row["Selenium_Records"] = selenium_str
                            update_made = True
                    
                    # Update API_Records if provided (ensure "0" not blank)
                    if api_records is not None:
                        # Normalize to ensure "0" not blank
                        api_str = str(api_records).strip() if str(api_records).strip() else "0"
                        if "API_Records" not in row or row.get("API_Records", "0") != api_str:
                            row["API_Records"] = api_str
                            update_made = True
                    
                    if update_made:
                        updated = True
                        updates = []
                        if new_source:
                            updates.append(f"Source='{new_source}'")
                        if scraped_by_selenium is not None:
                            updates.append(f"Scraped_By_Selenium='{scraped_by_selenium}'")
                        if scraped_by_api is not None:
                            updates.append(f"Scraped_By_API='{scraped_by_api}'")
                        if selenium_records is not None:
                            updates.append(f"Selenium_Records='{selenium_records}'")
                        if api_records is not None:
                            updates.append(f"API_Records='{api_records}'")
                        log.info(f"[CSV_UPDATE] Updated {', '.join(updates)} for {company} | {product}")
            
            # Write back all rows only if update was made (always use utf-8-sig for writing)
            if updated:
                # Ensure all required columns exist in fieldnames
                if fieldnames:
                    if "Scraped_By_Selenium" not in fieldnames:
                        fieldnames.append("Scraped_By_Selenium")
                    if "Scraped_By_API" not in fieldnames:
                        fieldnames.append("Scraped_By_API")
                    if "Selenium_Records" not in fieldnames:
                        fieldnames.append("Selenium_Records")
                    if "API_Records" not in fieldnames:
                        fieldnames.append("API_Records")
                
                # Normalize all count fields to ensure "0" not blank before writing (for all rows)
                for row_to_write in rows:
                    # Normalize Selenium_Records
                    if "Selenium_Records" in row_to_write:
                        selenium_rec = row_to_write.get("Selenium_Records") or "0"
                        row_to_write["Selenium_Records"] = str(selenium_rec).strip() if str(selenium_rec).strip() else "0"
                    else:
                        row_to_write["Selenium_Records"] = "0"
                    # Normalize API_Records
                    if "API_Records" in row_to_write:
                        api_rec = row_to_write.get("API_Records") or "0"
                        row_to_write["API_Records"] = str(api_rec).strip() if str(api_rec).strip() else "0"
                    else:
                        row_to_write["API_Records"] = "0"
                
                with open(PREPARED_URLS_FILE_PATH, "w", encoding="utf-8-sig", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)
    except Exception as e:
        log.warning(f"[CSV_UPDATE] Failed to update columns for {company} | {product}: {e}")

def sync_files_before_selenium():
    """Pre-sync: Align Productlist_with_urls.csv and alfabeta_progress.csv with alfabeta_products_by_product.csv BEFORE Selenium starts.
    Preserves pending API rows while normalizing counts and progress.
    """
    if not PREPARED_URLS_FILE_PATH.exists():
        log.warning("[PRE-SYNC] Productlist_with_urls.csv not found, cannot pre-sync")
        return
    
    log.info("[PRE-SYNC] Aligning files with output before Selenium step starts...")
    print("[PRE-SYNC] Aligning files before Selenium starts...", flush=True)
    
    encoding_attempts = ["utf-8-sig", "utf-8", "latin-1", "windows-1252", "cp1252"]
    
    # Step 1: Read all products from output file and count records per product (only rows with meaningful data)
    products_from_output = {}  # (normalized_company, normalized_product) -> record_count
    products_with_original_names = {}  # (normalized_company, normalized_product) -> (original_company, original_product)
    
    if OUT_CSV.exists():
        try:
            for encoding in encoding_attempts:
                try:
                    with open(OUT_CSV, encoding=encoding) as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            company = (row.get("input_company") or "").strip()
                            product = (row.get("input_product_name") or "").strip()
                            if company and product:
                                # Check if row has meaningful data (not just blank/fallback row)
                                has_price = row.get("price_ars") is not None and row.get("price_ars") != "" and str(row.get("price_ars")).strip() not in ["", "None", "null"]
                                has_description = row.get("description") and str(row.get("description")).strip()
                                has_coverage = row.get("coverage_json") and str(row.get("coverage_json")).strip() not in ["", "{}", "None", "null"]
                                has_import_status = row.get("import_status") and str(row.get("import_status")).strip()
                                has_product_name = row.get("product_name") and str(row.get("product_name")).strip()
                                has_active_ingredient = row.get("active_ingredient") and str(row.get("active_ingredient")).strip()
                                has_therapeutic_class = row.get("therapeutic_class") and str(row.get("therapeutic_class")).strip()
                                
                                # Only count rows with at least one meaningful value
                                if has_price or has_description or has_coverage or has_import_status or has_product_name or has_active_ingredient or has_therapeutic_class:
                                    key = (nk(company), nk(product))
                                    products_from_output[key] = products_from_output.get(key, 0) + 1
                                    # Store original names for first occurrence
                                    if key not in products_with_original_names:
                                        products_with_original_names[key] = (company, product)
                    break  # Success
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    log.warning(f"[PRE-SYNC] Error reading output file with {encoding}: {e}")
                    continue
        except Exception as e:
            log.error(f"[PRE-SYNC] Failed to read output file: {e}")
    
    log.info(f"[PRE-SYNC] Found {len(products_from_output)} unique products in output file with data")
    
    # Step 2: Update alfabeta_progress.csv (only products with records > 0, counts from output)
    products_to_keep_in_progress = {key for key, count in products_from_output.items() if count > 0}
    
    progress_fieldnames = ["input_company", "input_product_name", "timestamp", "records_found"]
    final_progress_dict = {}  # normalized_key -> row (final entries to write)
    
    if PROGRESS.exists():
        try:
            for encoding in encoding_attempts:
                try:
                    with open(PROGRESS, encoding=encoding, newline="") as f:
                        reader = csv.DictReader(f)
                        if reader.fieldnames:
                            progress_fieldnames = reader.fieldnames
                        for row in reader:
                            company = (row.get("input_company") or "").strip()
                            product = (row.get("input_product_name") or "").strip()
                            if company and product:
                                key = (nk(company), nk(product))
                                # Only keep products that have records > 0 in output
                                if key in products_to_keep_in_progress:
                                    final_progress_dict[key] = row
                    break
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    log.warning(f"[PRE-SYNC] Error reading progress file with {encoding}: {e}")
                    continue
        except Exception as e:
            log.warning(f"[PRE-SYNC] Error reading progress file: {e}")
    
    # Update/add entries for products with records > 0 (counts from output)
    for key, record_count in products_from_output.items():
        if record_count > 0:
            count_str = str(record_count).strip() if record_count else "0"
            if key in final_progress_dict:
                # Update existing entry
                final_progress_dict[key]["records_found"] = count_str if count_str else "0"
                final_progress_dict[key]["timestamp"] = ts()
            else:
                # Add new entry
                if key in products_with_original_names:
                    orig_company, orig_product = products_with_original_names[key]
                    new_row = {
                        "input_company": orig_company,
                        "input_product_name": orig_product,
                        "timestamp": ts(),
                        "records_found": count_str if count_str else "0"
                    }
                    for field in progress_fieldnames:
                        if field not in new_row:
                            new_row[field] = "0" if field == "records_found" else ""
                    final_progress_dict[key] = new_row
    
    # Write back progress file (ONLY products with records > 0)
    with PROGRESS_LOCK:
        with open(PROGRESS, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=progress_fieldnames)
            writer.writeheader()
            for row in final_progress_dict.values():
                # Normalize records_found to ensure "0" not blank
                if "records_found" in row:
                    records_found = row.get("records_found") or "0"
                    row["records_found"] = str(records_found).strip() if str(records_found).strip() else "0"
                else:
                    row["records_found"] = "0"
                writer.writerow(row)
    
    log.info(f"[PRE-SYNC] Progress file: {len(final_progress_dict)} entries (only products with records > 0)")
    
    # Step 3: Update Productlist_with_urls.csv (ALL products: Source="selenium", counts match output)
    updates_made = 0
    try:
        with CSV_LOCK:
            rows = []
            fieldnames = None
            encoding_used = None
            
            for encoding in encoding_attempts:
                try:
                    with open(PREPARED_URLS_FILE_PATH, "r", encoding=encoding, newline="") as f:
                        reader = csv.DictReader(f)
                        fieldnames = reader.fieldnames
                        if not fieldnames:
                            log.warning("[PRE-SYNC] Productlist_with_urls.csv has no headers")
                            return
                        for row in reader:
                            rows.append(row)
                        encoding_used = encoding
                        break
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    log.warning(f"[PRE-SYNC] Error reading Productlist_with_urls.csv with {encoding}: {e}")
                    continue
            
            if encoding_used is None:
                log.warning("[PRE-SYNC] Failed to read Productlist_with_urls.csv with any encoding")
                return
            
            # Ensure required columns exist
            if fieldnames:
                if "Scraped_By_Selenium" not in fieldnames:
                    fieldnames.append("Scraped_By_Selenium")
                if "Scraped_By_API" not in fieldnames:
                    fieldnames.append("Scraped_By_API")
                if "Source" not in fieldnames:
                    fieldnames.append("Source")
                if "Selenium_Records" not in fieldnames:
                    fieldnames.append("Selenium_Records")
                if "API_Records" not in fieldnames:
                    fieldnames.append("API_Records")
            
            # Update all products while preserving pending API state
            for row in rows:
                row_company = (row.get("Company") or "").strip()
                row_product = (row.get("Product") or "").strip()
                
                if row_company and row_product:
                    key = (nk(row_company), nk(row_product))
                    record_count = products_from_output.get(key, 0)
                    
                    update_made = False
                    current_source = (row.get("Source") or "").strip().lower()
                    current_selenium = (row.get("Scraped_By_Selenium") or "no").strip().lower()
                    current_api = (row.get("Scraped_By_API") or "no").strip().lower()
                    
                    # Initialize count columns if missing
                    if "Selenium_Records" not in row:
                        row["Selenium_Records"] = "0"
                    if "API_Records" not in row:
                        row["API_Records"] = "0"
                    
                    current_selenium_records = str(row.get("Selenium_Records") or "0").strip()
                    current_api_records = str(row.get("API_Records") or "0").strip()
                    
                    # Normalize counts to ensure "0" not blank
                    count_str = str(record_count).strip() if record_count else "0"
                    selenium_records = count_str if count_str else "0"
                    api_records = "0"
                    
                    pending_api = (current_api == "no" and current_selenium == "yes") or (current_source == "api" and current_api == "no")
                    
                    if record_count > 0:
                        # Product has records: mark as scraped (prefer Selenium unless API already done)
                        if current_api != "yes":
                            if current_source != "selenium":
                                row["Source"] = "selenium"
                                update_made = True
                            if current_selenium != "yes":
                                row["Scraped_By_Selenium"] = "yes"
                                update_made = True
                            if current_api != "no":
                                row["Scraped_By_API"] = "no"
                                update_made = True
                            if current_selenium_records != selenium_records:
                                row["Selenium_Records"] = selenium_records
                                update_made = True
                            if current_api_records != api_records:
                                row["API_Records"] = api_records
                                update_made = True
                        else:
                            # API already done: keep source as api, preserve counts
                            if current_source != "api":
                                row["Source"] = "api"
                                update_made = True
                    else:
                        # No records found in output
                        if pending_api:
                            # Preserve pending API fallback
                            if current_source != "api":
                                row["Source"] = "api"
                                update_made = True
                            if current_selenium != "yes":
                                row["Scraped_By_Selenium"] = "yes"
                                update_made = True
                            if current_api != "no":
                                row["Scraped_By_API"] = "no"
                                update_made = True
                            if current_selenium_records != "0":
                                row["Selenium_Records"] = "0"
                                update_made = True
                            if current_api_records != "0":
                                row["API_Records"] = "0"
                                update_made = True
                        else:
                            # Fresh item: reset to Selenium
                            if current_source != "selenium":
                                row["Source"] = "selenium"
                                update_made = True
                            if current_selenium != "no":
                                row["Scraped_By_Selenium"] = "no"
                                update_made = True
                            if current_api != "no":
                                row["Scraped_By_API"] = "no"
                                update_made = True
                            if current_selenium_records != "0":
                                row["Selenium_Records"] = "0"
                                update_made = True
                            if current_api_records != "0":
                                row["API_Records"] = "0"
                                update_made = True
                    
                    if update_made:
                        updates_made += 1
            
            # Normalize all count fields before writing (ensure "0" not blank for all rows)
            for row_to_write in rows:
                # Normalize Selenium_Records
                if "Selenium_Records" in row_to_write:
                    selenium_rec = row_to_write.get("Selenium_Records") or "0"
                    row_to_write["Selenium_Records"] = str(selenium_rec).strip() if str(selenium_rec).strip() else "0"
                else:
                    row_to_write["Selenium_Records"] = "0"
                # Normalize API_Records
                if "API_Records" in row_to_write:
                    api_rec = row_to_write.get("API_Records") or "0"
                    row_to_write["API_Records"] = str(api_rec).strip() if str(api_rec).strip() else "0"
                else:
                    row_to_write["API_Records"] = "0"
            
            # Write back if updates were made
            if updates_made > 0:
                with open(PREPARED_URLS_FILE_PATH, "w", encoding="utf-8-sig", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)
                log.info(f"[PRE-SYNC] Updated {updates_made} entries in Productlist_with_urls.csv")
                print(f"[PRE-SYNC] Updated {updates_made} entries in Productlist_with_urls.csv", flush=True)
            else:
                log.info("[PRE-SYNC] No updates needed in Productlist_with_urls.csv (already aligned)")
                print("[PRE-SYNC] Productlist_with_urls.csv already aligned", flush=True)
    except Exception as e:
        log.error(f"[PRE-SYNC] Failed to update Productlist_with_urls.csv: {e}")
        import traceback
        log.error(traceback.format_exc())
    
    log.info("[PRE-SYNC] Pre-sync completed: files aligned before Selenium step")
    print("[PRE-SYNC] Pre-sync completed", flush=True)

def sync_files_from_output():
    """Sync alfabeta_progress.csv and Productlist_with_urls.csv based on data in alfabeta_products_by_product.csv.
    This is called after the Selenium step to ensure all files are aligned with the final output data.
    
    Processes ALL products in Productlist_with_urls.csv:
    
    Logic (for Selenium step only):
    - If product has records (>0) in alfabeta_products_by_product.csv:
      * Update/add count in alfabeta_progress.csv (ONLY products with records > 0 are kept)
      * Update Productlist_with_urls.csv:
        - Scraped_By_Selenium = "yes"
        - Scraped_By_API = "no"
        - Selenium_Records = record_count
        - API_Records = "0"
        - Source = "selenium"
    - If product has NO records (0 or not found):
      * REMOVE from alfabeta_progress.csv (if exists) - products with 0 records should not be in progress
      * Update Productlist_with_urls.csv:
        - Scraped_By_Selenium = "yes" (attempted, no data)
        - Scraped_By_API = "no" (pending API)
        - Selenium_Records = "0"
        - API_Records = "0"
        - Source = "api"
    """
    if not PREPARED_URLS_FILE_PATH.exists():
        log.warning("[SYNC] Productlist_with_urls.csv not found, cannot sync")
        return
    
    log.info("[SYNC] Starting file synchronization from output data...")
    print("[SYNC] Starting file synchronization...", flush=True)
    
    encoding_attempts = ["utf-8-sig", "utf-8", "latin-1", "windows-1252", "cp1252"]
    
    # Step 1: Read all products from output file and count records per product (only rows with meaningful data)
    products_from_output = {}  # (normalized_company, normalized_product) -> record_count
    products_with_original_names = {}  # (normalized_company, normalized_product) -> (original_company, original_product)
    
    if OUT_CSV.exists():
        try:
            for encoding in encoding_attempts:
                try:
                    with open(OUT_CSV, encoding=encoding) as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            company = (row.get("input_company") or "").strip()
                            product = (row.get("input_product_name") or "").strip()
                            if company and product:
                                # Check if row has meaningful data (not just blank/fallback row)
                                has_price = row.get("price_ars") is not None and row.get("price_ars") != "" and str(row.get("price_ars")).strip() not in ["", "None", "null"]
                                has_description = row.get("description") and str(row.get("description")).strip()
                                has_coverage = row.get("coverage_json") and str(row.get("coverage_json")).strip() not in ["", "{}", "None", "null"]
                                has_import_status = row.get("import_status") and str(row.get("import_status")).strip()
                                has_product_name = row.get("product_name") and str(row.get("product_name")).strip()
                                has_active_ingredient = row.get("active_ingredient") and str(row.get("active_ingredient")).strip()
                                has_therapeutic_class = row.get("therapeutic_class") and str(row.get("therapeutic_class")).strip()
                                
                                # Only count rows with at least one meaningful value (same logic as selenium worker)
                                if has_price or has_description or has_coverage or has_import_status or has_product_name or has_active_ingredient or has_therapeutic_class:
                                    key = (nk(company), nk(product))
                                    products_from_output[key] = products_from_output.get(key, 0) + 1
                                    # Store original names for first occurrence
                                    if key not in products_with_original_names:
                                        products_with_original_names[key] = (company, product)
                    break  # Success
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    log.warning(f"[SYNC] Error reading output file with {encoding}: {e}")
                    continue
        except Exception as e:
            log.error(f"[SYNC] Failed to read output file: {e}")
    
    log.info(f"[SYNC] Found {len(products_from_output)} unique products in output file with data")
    print(f"[SYNC] Found {len(products_from_output)} unique products in output file", flush=True)
    
    # Step 2: Build set of products that SHOULD be in progress file (only records > 0)
    products_to_keep_in_progress = {key for key, count in products_from_output.items() if count > 0}
    log.info(f"[SYNC] {len(products_to_keep_in_progress)} products have records > 0 (will be kept in progress file)")
    
    # Step 3: Read existing progress file and filter to only keep products with records > 0
    progress_fieldnames = ["input_company", "input_product_name", "timestamp", "records_found"]
    original_progress_count = 0
    filtered_progress_rows = []  # Only products with records > 0
    
    if PROGRESS.exists():
        try:
            for encoding in encoding_attempts:
                try:
                    with open(PROGRESS, encoding=encoding, newline="") as f:
                        reader = csv.DictReader(f)
                        if reader.fieldnames:
                            progress_fieldnames = reader.fieldnames
                        for row in reader:
                            company = (row.get("input_company") or "").strip()
                            product = (row.get("input_product_name") or "").strip()
                            if company and product:
                                original_progress_count += 1
                                key = (nk(company), nk(product))
                                
                                # Only keep products that have records > 0 in output
                                # Products NOT in products_to_keep_in_progress are filtered out (removed)
                                # This includes:
                                # - Products with 0 records in output
                                # - Products not found in output at all
                                # - Products with records_found=0 in progress (if they don't have records > 0 in output)
                                if key in products_to_keep_in_progress:
                                    filtered_progress_rows.append(row)
                                # All other products are filtered out (removed from progress file)
                    break
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    log.warning(f"[SYNC] Error reading progress file with {encoding}: {e}")
                    continue
        except Exception as e:
            log.warning(f"[SYNC] Error reading progress file: {e}")
    
    removed_count = original_progress_count - len(filtered_progress_rows)
    
    # Step 4: Update/add entries for products with records > 0
    progress_updates = {}  # normalized_key -> record_count (only for products with >0 records)
    for key, record_count in products_from_output.items():
        if record_count > 0:
            progress_updates[key] = record_count
    
    # Build set of existing products in filtered progress (from original progress file)
    existing_filtered_keys = set()
    for row in filtered_progress_rows:
        company = (row.get("input_company") or "").strip()
        product = (row.get("input_product_name") or "").strip()
        if company and product:
            key = (nk(company), nk(product))
            existing_filtered_keys.add(key)
    
    # Update existing entries or add new ones
    final_progress_dict = {}  # normalized_key -> row (final entries to write)
    
    # First, add existing filtered entries (ensure records_found is "0" not blank)
    for row in filtered_progress_rows:
        company = (row.get("input_company") or "").strip()
        product = (row.get("input_product_name") or "").strip()
        if company and product:
            key = (nk(company), nk(product))
            # Ensure records_found is "0" not blank if missing or empty
            if "records_found" not in row or not row.get("records_found") or str(row.get("records_found")).strip() == "":
                row["records_found"] = "0"
            else:
                # Convert to string and ensure it's not blank
                row["records_found"] = str(row.get("records_found")).strip() or "0"
            final_progress_dict[key] = row
    
    # Update existing entries with new counts (ensure "0" not blank)
    for key, record_count in progress_updates.items():
        if key in final_progress_dict:
            # Update existing entry - ensure records_found is always a string and never blank
            # Normalize to ensure valid string value
            count_str = str(record_count).strip() if record_count else "0"
            final_progress_dict[key]["records_found"] = count_str if count_str else "0"
            final_progress_dict[key]["timestamp"] = ts()
        else:
            # Add new entry with original names
            if key in products_with_original_names:
                orig_company, orig_product = products_with_original_names[key]
                # Normalize count to ensure "0" not blank
                count_str = str(record_count).strip() if record_count else "0"
                new_row = {
                    "input_company": orig_company,
                    "input_product_name": orig_product,
                    "timestamp": ts(),
                    "records_found": count_str if count_str else "0"
                }
                # Ensure all fieldnames are present with default values
                for field in progress_fieldnames:
                    if field not in new_row:
                        if field == "records_found":
                            new_row[field] = "0"  # Always use "0" not blank for records_found
                        else:
                            new_row[field] = ""
                final_progress_dict[key] = new_row
    
    # Step 5: Write back progress file (ONLY products with records > 0)
    with PROGRESS_LOCK:
        with open(PROGRESS, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=progress_fieldnames)
            writer.writeheader()
            # Write only products with records > 0 (ensure records_found is "0" not blank)
            for row in final_progress_dict.values():
                # Normalize records_found to ensure "0" not blank
                if "records_found" in row:
                    records_found = row.get("records_found") or "0"
                    row["records_found"] = str(records_found).strip() if str(records_found).strip() else "0"
                else:
                    row["records_found"] = "0"
                writer.writerow(row)
    
    # Log progress file changes
    if original_progress_count > 0:
        log.info(f"[SYNC] Progress file: Started with {original_progress_count} entries")
    if removed_count > 0:
        log.info(f"[SYNC] Removed {removed_count} entries from progress file (products without records or not in output)")
    
    if progress_updates:
        added_count = len([k for k in progress_updates.keys() if k not in existing_filtered_keys])
        updated_count = len(progress_updates) - added_count
        final_count = len(final_progress_dict)
        if added_count > 0 and updated_count > 0:
            log.info(f"[SYNC] Progress file: added {added_count} new entries, updated {updated_count} existing entries (products with records > 0)")
        elif added_count > 0:
            log.info(f"[SYNC] Progress file: added {added_count} new entries (products with records > 0)")
        elif updated_count > 0:
            log.info(f"[SYNC] Progress file: updated {updated_count} existing entries (products with records > 0)")
        log.info(f"[SYNC] Progress file: Final count = {final_count} entries (only products with records > 0)")
    else:
        if original_progress_count > 0:
            log.info("[SYNC] No products with records > 0, progress file cleared (removed all entries)")
        else:
            log.info("[SYNC] No products with records > 0, progress file is empty")
    
    # Step 4: Update Productlist_with_urls.csv for ALL products
    updates_made = 0
    try:
        with CSV_LOCK:
            # Read all rows from Productlist_with_urls.csv
            rows = []
            fieldnames = None
            encoding_used = None
            
            for encoding in encoding_attempts:
                try:
                    with open(PREPARED_URLS_FILE_PATH, "r", encoding=encoding, newline="") as f:
                        reader = csv.DictReader(f)
                        fieldnames = reader.fieldnames
                        if not fieldnames:
                            log.warning("[SYNC] Productlist_with_urls.csv has no headers")
                            return
                        
                        for row in reader:
                            rows.append(row)
                        encoding_used = encoding
                        break
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    log.warning(f"[SYNC] Error reading Productlist_with_urls.csv with {encoding}: {e}")
                    continue
            
            if encoding_used is None:
                log.warning("[SYNC] Failed to read Productlist_with_urls.csv with any encoding")
                return
            
            # Ensure required columns exist
            if fieldnames:
                if "Scraped_By_Selenium" not in fieldnames:
                    fieldnames.append("Scraped_By_Selenium")
                if "Scraped_By_API" not in fieldnames:
                    fieldnames.append("Scraped_By_API")
                if "Source" not in fieldnames:
                    fieldnames.append("Source")
                if "Selenium_Records" not in fieldnames:
                    fieldnames.append("Selenium_Records")
                if "API_Records" not in fieldnames:
                    fieldnames.append("API_Records")
            
            # Update rows for all products
            for row in rows:
                row_company = (row.get("Company") or "").strip()
                row_product = (row.get("Product") or "").strip()
                
                if row_company and row_product:
                    key = (nk(row_company), nk(row_product))
                    record_count = products_from_output.get(key, 0)
                    
                    update_made = False
                    current_selenium = (row.get("Scraped_By_Selenium") or "").strip().lower()
                    current_api = (row.get("Scraped_By_API") or "").strip().lower()
                    current_source = (row.get("Source") or "").strip().lower()
                    # Initialize record count columns if missing
                    if "Selenium_Records" not in row:
                        row["Selenium_Records"] = "0"
                    if "API_Records" not in row:
                        row["API_Records"] = "0"
                    
                    current_selenium_records = str(row.get("Selenium_Records") or "0").strip()
                    current_api_records = str(row.get("API_Records") or "0").strip()
                    
                    if record_count > 0:
                        # Product has records: Selenium found them (sync runs after Selenium step)
                        # Update: Scraped_By_Selenium = yes, Scraped_By_API = no, Selenium_Records = record_count, API_Records = 0
                        
                        if current_selenium != "yes":
                            row["Scraped_By_Selenium"] = "yes"
                            update_made = True
                        
                        if current_api != "no":
                            row["Scraped_By_API"] = "no"
                            update_made = True
                        
                        # Update Source to "selenium" for products with records > 0
                        if current_source != "selenium":
                            row["Source"] = "selenium"
                            update_made = True
                        
                        # Update Selenium_Records with actual count (ensure "0" not blank), API_Records = "0"
                        count_str = str(record_count).strip() if record_count else "0"
                        selenium_records = count_str if count_str else "0"
                        api_records = "0"
                        
                        if current_selenium_records != selenium_records:
                            row["Selenium_Records"] = selenium_records
                            update_made = True
                        
                        if current_api_records != api_records:
                            row["API_Records"] = api_records
                            update_made = True
                    else:
                        # Product has NO records: Selenium didn't find any (will be processed by API)
                        # Update: Scraped_By_Selenium = yes, Scraped_By_API = no, Selenium_Records = 0, API_Records = 0
                        # Note: This product should NOT be in alfabeta_progress.csv (handled above)
                        
                        if current_selenium != "yes":
                            row["Scraped_By_Selenium"] = "yes"
                            update_made = True
                        
                        if current_api != "no":
                            row["Scraped_By_API"] = "no"
                            update_made = True
                        
                        # Update Source to "api" for products with 0 records (ready for API scraping)
                        if current_source != "api":
                            row["Source"] = "api"
                            update_made = True
                        
                        # Both records should be 0
                        selenium_records = "0"
                        api_records = "0"
                        
                        if current_selenium_records != selenium_records:
                            row["Selenium_Records"] = selenium_records
                            update_made = True
                        
                        if current_api_records != api_records:
                            row["API_Records"] = api_records
                            update_made = True
                    
                    if update_made:
                        updates_made += 1
            
            # Write back if updates were made (ensure all count fields are "0" not blank)
            if updates_made > 0:
                # Normalize all count fields to ensure "0" not blank before writing (for all rows, not just updated ones)
                for row_to_write in rows:
                    # Normalize Selenium_Records
                    if "Selenium_Records" in row_to_write:
                        selenium_rec = row_to_write.get("Selenium_Records") or "0"
                        row_to_write["Selenium_Records"] = str(selenium_rec).strip() if str(selenium_rec).strip() else "0"
                    else:
                        row_to_write["Selenium_Records"] = "0"
                    # Normalize API_Records
                    if "API_Records" in row_to_write:
                        api_rec = row_to_write.get("API_Records") or "0"
                        row_to_write["API_Records"] = str(api_rec).strip() if str(api_rec).strip() else "0"
                    else:
                        row_to_write["API_Records"] = "0"
                
                with open(PREPARED_URLS_FILE_PATH, "w", encoding="utf-8-sig", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)
                log.info(f"[SYNC] Updated {updates_made} entries in Productlist_with_urls.csv")
            else:
                log.info("[SYNC] No updates needed in Productlist_with_urls.csv (already synced)")
    except Exception as e:
        log.error(f"[SYNC] Failed to update Productlist_with_urls.csv: {e}")
    
    log.info("[SYNC] File synchronization completed")
    print("[SYNC] File synchronization completed", flush=True)

def update_selenium_attempt(company: str, product: str, attempt_num: int, records_found: int):
    """Update Selenium_Attempt and Last_Attempt_Records columns in Productlist_with_urls.csv.
    Thread-safe: uses CSV_LOCK to prevent race conditions when multiple threads update the file.

    Args:
        company: Company name to match
        product: Product name to match
        attempt_num: Current attempt number (1, 2, or 3)
        records_found: Number of records found in this attempt
    """
    if not PREPARED_URLS_FILE_PATH.exists():
        return  # File doesn't exist, skip update

    try:
        # Use lock for both read and write to make operation atomic
        with CSV_LOCK:
            # Read all rows - try multiple encodings
            rows = []
            fieldnames = None
            encoding_used = None

            encoding_attempts = ["utf-8-sig", "utf-8", "latin-1", "windows-1252", "cp1252"]
            for encoding in encoding_attempts:
                try:
                    with open(PREPARED_URLS_FILE_PATH, "r", encoding=encoding, newline="") as f:
                        reader = csv.DictReader(f)
                        fieldnames = reader.fieldnames
                        if not fieldnames:
                            return

                        for row in reader:
                            rows.append(row)
                        encoding_used = encoding
                        break  # Success, exit encoding loop
                except UnicodeDecodeError:
                    continue  # Try next encoding
                except Exception as e:
                    log.warning(f"[ATTEMPT_UPDATE] Error reading with {encoding}: {e}")
                    continue

            if encoding_used is None:
                log.warning(f"[ATTEMPT_UPDATE] Failed to read file with any encoding")
                return

            # Process rows to find and update matching entry
            updated = False
            for row in rows:
                # Normalize for comparison
                row_company = (row.get("Company") or "").strip()
                row_product = (row.get("Product") or "").strip()

                # Update columns if match found
                if nk(row_company) == nk(company) and nk(row_product) == nk(product):
                    update_made = False

                    # Update Selenium_Attempt
                    if "Selenium_Attempt" not in row or row.get("Selenium_Attempt", "0") != str(attempt_num):
                        row["Selenium_Attempt"] = str(attempt_num)
                        update_made = True

                    # Update Last_Attempt_Records
                    if "Last_Attempt_Records" not in row or row.get("Last_Attempt_Records", "0") != str(records_found):
                        row["Last_Attempt_Records"] = str(records_found)
                        update_made = True

                    if update_made:
                        updated = True
                        log.info(f"[ATTEMPT_UPDATE] Updated Selenium_Attempt={attempt_num}, Last_Attempt_Records={records_found} for {company} | {product}")

            # Write back all rows only if update was made (always use utf-8-sig for writing)
            if updated:
                # Ensure all required columns exist in fieldnames
                if fieldnames:
                    if "Selenium_Attempt" not in fieldnames:
                        fieldnames.append("Selenium_Attempt")
                    if "Last_Attempt_Records" not in fieldnames:
                        fieldnames.append("Last_Attempt_Records")

                # Normalize all count fields to ensure "0" not blank before writing (for all rows)
                for row_to_write in rows:
                    # Normalize Selenium_Attempt
                    if "Selenium_Attempt" in row_to_write:
                        attempt = row_to_write.get("Selenium_Attempt") or "0"
                        row_to_write["Selenium_Attempt"] = str(attempt).strip() if str(attempt).strip() else "0"
                    else:
                        row_to_write["Selenium_Attempt"] = "0"
                    # Normalize Last_Attempt_Records
                    if "Last_Attempt_Records" in row_to_write:
                        last_records = row_to_write.get("Last_Attempt_Records") or "0"
                        row_to_write["Last_Attempt_Records"] = str(last_records).strip() if str(last_records).strip() else "0"
                    else:
                        row_to_write["Last_Attempt_Records"] = "0"

                with open(PREPARED_URLS_FILE_PATH, "w", encoding="utf-8-sig", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)
    except Exception as e:
        log.warning(f"[ATTEMPT_UPDATE] Failed to update attempt for {company} | {product}: {e}")
