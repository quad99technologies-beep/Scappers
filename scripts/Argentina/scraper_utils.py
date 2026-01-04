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
    "SIFAR_detail", "PAMI_AF", "IOMA_detail", "IOMA_AF", "IOMA_OS",
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
        with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=OUT_FIELDS).writeheader()
    if not PROGRESS.exists():
        with open(PROGRESS, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["input_company","input_product_name","timestamp","records_found"])
    if not ERRORS.exists():
        with open(ERRORS, "w", newline="", encoding="utf-8") as f:
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
                            if company and product:
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
            with open(ignore_file, encoding="utf-8-sig") as f:
                r = csv.DictReader(f)
                headers = {nk(h): h for h in (r.fieldnames or [])}
                pcol = headers.get(nk("Product")) or headers.get("product") or "Product"
                ccol = headers.get(nk("Company")) or headers.get("company") or "Company"
                for row in r:
                    prod = (row.get(pcol) or "").strip()
                    comp = (row.get(ccol) or "").strip()
                    if prod and comp:
                        ignore_set.add((nk(comp), nk(prod)))
            log.info(f"[IGNORE_LIST] Loaded {len(ignore_set)} combinations from {IGNORE_LIST_FILE}")
        except Exception as e:
            log.warning(f"[IGNORE_LIST] Failed to load {IGNORE_LIST_FILE}: {e}")
    else:
        log.info(f"[IGNORE_LIST] No {IGNORE_LIST_FILE} found in {INPUT_DIR} (optional file)")
    return ignore_set

def combine_skip_sets() -> Set[Tuple[str, str]]:
    """Combine all three skip sources: progress, output, and ignore list."""
    progress_set = load_progress_set()
    output_set = load_output_set()
    ignore_set = load_ignore_list()
    
    skip_set = progress_set | output_set | ignore_set
    
    log.info(f"[SKIP_SET] Loaded skip combinations:")
    log.info(f"[SKIP_SET]   - Progress file: {len(progress_set)} combinations")
    log.info(f"[SKIP_SET]   - Output file: {len(output_set)} combinations")
    log.info(f"[SKIP_SET]   - Ignore list: {len(ignore_set)} combinations")
    log.info(f"[SKIP_SET]   - Total unique combinations to skip: {len(skip_set)}")
    
    return skip_set

def append_progress(company: str, product: str, count: int):
    """Append progress entry to progress file."""
    with PROGRESS_LOCK, open(PROGRESS, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([company, product, ts(), count])

def append_error(company: str, product: str, msg: str):
    """Append error entry to error file."""
    with ERROR_LOCK, open(ERRORS, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([company, product, ts(), msg[:5000]])

def append_rows(rows: list):
    """Append rows to output CSV file."""
    if not rows:
        return
    with CSV_LOCK, open(OUT_CSV, "a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=OUT_FIELDS, extrasaction="ignore").writerows(rows)

def update_prepared_urls_source(company: str, product: str, new_source: str = "selenium"):
    """Update the Source column in Productlist_with_urls.csv to selenium when API returns null.
    Thread-safe: uses CSV_LOCK to prevent race conditions when multiple threads update the file.
    """
    if not PREPARED_URLS_FILE_PATH.exists():
        return  # File doesn't exist, skip update
    
    try:
        # Use lock for both read and write to make operation atomic
        with CSV_LOCK:
            # Read all rows
            rows = []
            with open(PREPARED_URLS_FILE_PATH, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                if not fieldnames:
                    return
                
                updated = False
                for row in reader:
                    # Normalize for comparison
                    row_company = (row.get("Company") or "").strip()
                    row_product = (row.get("Product") or "").strip()
                    
                    # Update source if match found
                    if nk(row_company) == nk(company) and nk(row_product) == nk(product):
                        if row.get("Source", "").lower() != new_source.lower():
                            row["Source"] = new_source
                            updated = True
                            log.info(f"[CSV_UPDATE] Updated source to '{new_source}' for {company} | {product}")
                    
                    rows.append(row)
            
            # Write back all rows only if update was made
            if updated:
                with open(PREPARED_URLS_FILE_PATH, "w", encoding="utf-8", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)
    except Exception as e:
        log.warning(f"[CSV_UPDATE] Failed to update source for {company} | {product}: {e}")

