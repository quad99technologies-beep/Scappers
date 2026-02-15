"""
Shared utilities for Argentina scraper scripts.
Contains common functions used by both API and Selenium scrapers.
"""

import csv
import os
import re
import time
import tempfile
import logging
import threading
from pathlib import Path
from datetime import datetime
from typing import Optional, Set, Tuple

from config_loader import (
    get_input_dir, get_output_dir,
    PREPARED_URLS_FILE,
    OUTPUT_PRODUCTS_CSV, OUTPUT_PROGRESS_CSV, OUTPUT_ERRORS_CSV,
    SELENIUM_MAX_LOOPS,
)
from core.db.connection import CountryDB
from db.repositories import ArgentinaRepository
from db.schema import apply_argentina_schema
from core.db.models import generate_run_id

# ====== LOGGING ======
log = logging.getLogger("scraper_utils")

# ====== PATHS ======
INPUT_DIR = get_input_dir()
OUTPUT_DIR = get_output_dir()
PREPARED_URLS_FILE_PATH = OUTPUT_DIR / PREPARED_URLS_FILE
OUT_CSV = OUTPUT_DIR / OUTPUT_PRODUCTS_CSV
PROGRESS = OUTPUT_DIR / OUTPUT_PROGRESS_CSV
ERRORS = OUTPUT_DIR / OUTPUT_ERRORS_CSV

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# DB setup
_RUN_ID_FILE = OUTPUT_DIR / ".current_run_id"
def _get_run_id() -> str:
    rid = os.environ.get("ARGENTINA_RUN_ID")
    if rid:
        return rid
    if _RUN_ID_FILE.exists():
        try:
            txt = _RUN_ID_FILE.read_text(encoding="utf-8").strip()
            if txt:
                return txt
        except Exception:
            pass
    rid = generate_run_id()
    os.environ["ARGENTINA_RUN_ID"] = rid
    _RUN_ID_FILE.write_text(rid, encoding="utf-8")
    return rid

_DB = CountryDB("Argentina")
apply_argentina_schema(_DB)
_RUN_ID = _get_run_id()
_REPO = ArgentinaRepository(_DB, _RUN_ID)

# ====== LOCKS ======
CSV_LOCK = threading.Lock()
PROGRESS_LOCK = threading.Lock()
ERROR_LOCK = threading.Lock()

# Track per-process attempts to avoid double-counting if a caller writes progress multiple times.
_ATTEMPTED_THIS_RUN: set[tuple[str, str]] = set()
_ATTEMPTED_LOCK = threading.Lock()


def _simple_state_columns(fieldnames: list) -> tuple[str, str] | tuple[None, None]:
    """Return (loop_col, total_col) if the prepared URLs file uses simple loop-count schema."""
    if not fieldnames:
        return (None, None)
    headers = {nk(h): h for h in fieldnames}
    loop_col = headers.get(nk("Loop Count")) or headers.get(nk("Loop_Count")) or headers.get(nk("LoopCount"))
    total_col = headers.get(nk("Total Records")) or headers.get(nk("Total_Records")) or headers.get(nk("TotalRecords"))
    if loop_col and total_col:
        return (loop_col, total_col)
    return (None, None)


class _LockFile:
    """Best-effort cross-process lock using exclusive lock-file creation."""

    def __init__(self, path: Path, timeout_seconds: float = 30.0, poll_seconds: float = 0.1):
        self.path = path
        self.timeout_seconds = timeout_seconds
        self.poll_seconds = poll_seconds
        self._fd = None

    def __enter__(self):
        deadline = time.time() + self.timeout_seconds
        while True:
            try:
                self._fd = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
                os.write(self._fd, str(os.getpid()).encode("ascii", errors="ignore"))
                return self
            except FileExistsError:
                if time.time() >= deadline:
                    raise TimeoutError(f"Timed out waiting for lock: {self.path}")
                time.sleep(self.poll_seconds)

    def __exit__(self, exc_type, exc, tb):
        try:
            if self._fd is not None:
                os.close(self._fd)
        finally:
            self._fd = None
            try:
                self.path.unlink(missing_ok=True)
            except Exception:
                pass

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
    """Remove accents from string and normalize special characters."""
    import unicodedata
    if not s:
        return ""
    # Pre-process special characters before NFKD normalization
    # German sharp S (ß) -> ss
    s = s.replace("ß", "ss").replace("ẞ", "SS")
    # Handle other common special characters that NFKD doesn't normalize well
    s = s.replace("æ", "ae").replace("Æ", "AE")
    s = s.replace("œ", "oe").replace("Œ", "OE")
    s = s.replace("ø", "o").replace("Ø", "O")
    s = s.replace("ð", "d").replace("Ð", "D")
    s = s.replace("þ", "th").replace("Þ", "TH")
    # Now apply NFKD normalization for accented characters
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))

def nk(s: Optional[str]) -> str:
    """Normalize string for comparison (lowercase, no accents, single spaces)."""
    if not s:
        return ""
    normalized = strip_accents(s.strip())
    return re.sub(r"\s+", " ", normalized).lower()

# ====== CSV IO ======

def ensure_headers():
    """No-op in DB mode."""
    return

def load_progress_set() -> Set[Tuple[str, str]]:
    """Load progress from DB (products already with records or completed)."""
    try:
        rows = _REPO.get_all_product_index()
        return {
            (nk(r["company"]), nk(r["product"]))
            for r in rows
            if (r.get("total_records") or 0) > 0 or r.get("status") == "completed"
        }
    except Exception as e:
        log.warning(f"[PROGRESS] Failed to load progress from DB: {e}")
        return set()

def load_output_set() -> Set[Tuple[str, str]]:
    """Products that already have scraped data (ar_products)."""
    try:
        with _DB.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT input_company, input_product_name FROM ar_products WHERE run_id = %s",
                (_RUN_ID,),
            )
            return {(nk(c or ""), nk(p or "")) for c, p in cur.fetchall()}
    except Exception as e:
        log.warning(f"[OUTPUT] Failed to load output set from DB: {e}")
        return set()

def combine_skip_sets() -> Set[Tuple[str, str]]:
    """Combine skip sources from DB in a single query for speed."""
    skip_set: Set[Tuple[str, str]] = set()
    output_count = progress_count = 0
    try:
        with _DB.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT input_company AS company, input_product_name AS product, 'output' AS src
                  FROM ar_products
                 WHERE run_id = %s
                UNION
                SELECT company, product, 'progress' AS src
                  FROM ar_product_index
                 WHERE run_id = %s
                   AND (COALESCE(total_records,0) > 0 OR status = 'completed')
                """,
                (_RUN_ID, _RUN_ID),
            )
            for row in cur.fetchall():
                c = (row[0] or "") if isinstance(row, tuple) else (row.get("company") or "")
                p = (row[1] or "") if isinstance(row, tuple) else (row.get("product") or "")
                src = row[2] if isinstance(row, tuple) else row.get("src", "")
                key = (nk(c), nk(p))
                if key[0] and key[1]:
                    skip_set.add(key)
                    if src == "output":
                        output_count += 1
                    elif src == "progress":
                        progress_count += 1
    except Exception as e:
        log.warning(f"[SKIP_SET] Combined query failed, falling back to individual loads: {e}")
        output_set = load_output_set()
        progress_set = load_progress_set()
        skip_set = output_set | progress_set
        output_count = len(output_set)
        progress_count = len(progress_set)
    log.info(f"[SKIP_SET] Loaded skip_set size = {len(skip_set)} (output={output_count}, progress={progress_count})")
    return skip_set

def append_progress(company: str, product: str, count: int, source: str = "selenium"):
    """Update ar_product_index in DB: bump loop_count once per process run; set total_records and status."""
    key = (nk(company), nk(product))
    first_for_this_process = False
    with _ATTEMPTED_LOCK:
        if key not in _ATTEMPTED_THIS_RUN:
            _ATTEMPTED_THIS_RUN.add(key)
            first_for_this_process = True
    try:
        if first_for_this_process:
            _REPO.bump_attempt(
                company,
                product,
                total_records=count or 0,
                status="completed" if (count and count > 0) else "failed",
                source=source,
            )
            return

        # Already bumped loop_count in this process for this product.
        # Only allow an upgrade from 0 -> positive total_records without bumping again.
        if count and count > 0:
            try:
                with _DB.cursor() as cur:
                    cur.execute(
                        "SELECT COALESCE(loop_count,0) FROM ar_product_index WHERE run_id=%s AND company=%s AND product=%s",
                        (_RUN_ID, company, product),
                    )
                    row = cur.fetchone()
                    current_loop = row[0] if row else 0
            except Exception:
                current_loop = 0

            _REPO.mark_attempt_by_name(
                company,
                product,
                loop_count=int(current_loop),
                total_records=int(count),
                status="completed",
                source=source,
                error_message=None,
            )
    except Exception as e:
        log.warning(f"[DB] append_progress failed for {company}/{product}: {e}")

def append_error(company: str, product: str, msg: str):
    """Append error entry to DB."""
    try:
        _REPO.log_error(company, product, msg[:5000] if msg else "")
    except Exception as e:
        log.warning(f"[DB] append_error failed: {e}")

def append_rows(rows: list, source: str = "selenium") -> bool:
    """Insert scraped rows into DB. Returns True if insert succeeds."""
    if not rows:
        return True
    try:
        _REPO.insert_products(rows, source=source)
        return True
    except Exception as e:
        log.warning(f"[DB] append_rows failed: {e}")
        return False

def is_product_already_scraped(company: str, product: str) -> bool:
    """Check if a product already has data (total_records > 0 in ar_product_index). DB-only."""
    try:
        with _DB.cursor() as cur:
            cur.execute(
                "SELECT total_records FROM ar_product_index WHERE run_id=%s AND company=%s AND product=%s",
                (_RUN_ID, company, product),
            )
            row = cur.fetchone()
            if not row:
                return False
            val = row[0] if isinstance(row, tuple) else row.get("total_records")
            return (val or 0) > 0
    except Exception:
        return False

def update_prepared_urls_source(company: str, product: str, new_source: str = "selenium",
                                 scraped_by_selenium: str = None, scraped_by_api: str = None,
                                 selenium_records: str = None, api_records: str = None):
    """No-op: state is in DB (ar_product_index). Kept for backward-compatible call sites."""
    return


def sync_files_before_selenium():
    """No-op (DB-only mode)."""
    return

def sync_files_from_output():
    """No-op: state is in DB. Kept for backward-compatible call sites."""
    return


def update_selenium_attempt(company: str, product: str, attempt_num: int, records_found: int):
    """Update Selenium_Attempt and Last_Attempt_Records columns in Productlist_with_urls.csv.
    Thread-safe: uses CSV_LOCK to prevent race conditions when multiple threads update the file.

    Args:
        company: Company name to match
        product: Product name to match
        attempt_num: Current attempt number (1, 2, or 3)
        records_found: Number of records found in this attempt
    """
    # DB-only mode: no CSV tracking.
    return
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
