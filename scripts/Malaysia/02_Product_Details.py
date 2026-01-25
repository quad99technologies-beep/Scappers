import sys
import os
import logging

# Force unbuffered output for real-time console updates
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
os.environ.setdefault('PYTHONUNBUFFERED', '1')

from playwright.sync_api import sync_playwright
from pathlib import Path
import re
import time
import datetime
import sys
from config_loader import load_env_file, require_env, getenv, getenv_int, getenv_bool, get_input_dir, get_output_dir

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.browser_observer import observe_playwright, wait_until_idle
from core.stealth_profile import apply_playwright
from core.human_actions import pause, type_delay
from core.standalone_checkpoint import run_with_checkpoint
# Common selectors to detect when the QUEST3+ table is loading
LOADING_SELECTORS = [
    ".loading",
    ".spinner",
    "[class*='loading']",
    "[class*='spinner']",
    "[data-loading='true']",
    ".dataTables_processing",
    "#searchContent img[src*='spin.gif']"
]

from smart_locator import SmartLocator
from state_machine import NavigationStateMachine, NavigationState, StateCondition

# Import pandas with graceful error handling
try:
    import pandas as pd
except ImportError as e:
    print("=" * 80)
    print("ERROR: Required module 'pandas' is not installed.")
    print("=" * 80)
    print("Please install dependencies by running:")
    print("  pip install -r requirements.txt")
    print("")
    print("Or install pandas directly:")
    print("  pip install pandas")
    print("=" * 80)
    sys.exit(1)

# Load environment variables from .env file
load_env_file()

# ================= CONFIG =================
base_dir_str = getenv("SCRIPT_02_BASE_DIR")
if not base_dir_str:
    base_dir_str = "../"
if base_dir_str.startswith("/") or (len(base_dir_str) > 1 and base_dir_str[1] == ":"):
    BASE_DIR = Path(base_dir_str)
else:
    BASE_DIR = Path(__file__).resolve().parent / base_dir_str

# Use ConfigManager input directory for products.csv
input_products_path = require_env("SCRIPT_02_INPUT_PRODUCTS")
if Path(input_products_path).is_absolute():
    INPUT_PRODUCTS = Path(input_products_path)
else:
    # Use scraper-specific input directory
    INPUT_PRODUCTS = get_input_dir() / input_products_path

from config_loader import get_output_dir

# Use ConfigManager output directory for input file from previous step
input_malaysia_path = require_env("SCRIPT_02_INPUT_MALAYSIA")
if Path(input_malaysia_path).is_absolute():
    INPUT_MALAYSIA = Path(input_malaysia_path)
else:
    # Use scraper-specific output directory
    INPUT_MALAYSIA = get_output_dir() / input_malaysia_path

# Use ConfigManager output directory
out_dir_path = getenv("SCRIPT_02_OUT_DIR", "")
if out_dir_path and Path(out_dir_path).is_absolute():
    OUT_DIR = Path(out_dir_path)
else:
    # Use scraper-specific output directory
    OUT_DIR = get_output_dir()
bulk_dir_name = require_env("SCRIPT_02_BULK_DIR_NAME")
BULK_DIR = OUT_DIR / bulk_dir_name
BULK_DIR.mkdir(exist_ok=True)

OUT_BULK = OUT_DIR / require_env("SCRIPT_02_OUT_BULK")
OUT_MISSING = OUT_DIR / require_env("SCRIPT_02_OUT_MISSING")
OUT_FINAL = OUT_DIR / require_env("SCRIPT_02_OUT_FINAL")
COUNT_REPORT_PATH = OUT_DIR / getenv("SCRIPT_02_OUT_COUNT_REPORT", "bulk_search_counts.csv")
MISSING_SCREENSHOT_DIR = OUT_DIR / getenv("SCRIPT_02_MISSING_SCREENSHOT_DIR", "missing_data_screenshots")
CAPTURE_MISSING_SCREENSHOTS = getenv_bool("SCRIPT_02_CAPTURE_MISSING_SCREENSHOT", False)

SEARCH_URL = require_env("SCRIPT_02_SEARCH_URL")
DETAIL_URL = require_env("SCRIPT_02_DETAIL_URL")

WAIT_BULK = int(require_env("SCRIPT_02_WAIT_BULK"))
HEADLESS = getenv_bool("SCRIPT_02_HEADLESS", True)
DATA_LOAD_WAIT_SECONDS = float(getenv("SCRIPT_02_DATA_LOAD_WAIT", "3"))
CSV_WAIT_SECONDS = float(getenv("SCRIPT_02_CSV_WAIT_SECONDS", "60"))
CSV_WAIT_MAX_SECONDS = float(getenv("SCRIPT_02_CSV_WAIT_MAX_SECONDS", "300"))

# Setup logger
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format='[%(levelname)s] [%(name)s] %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )

# ================= HELPERS =================
def clean(x):
    return re.sub(r"\s+", " ", str(x or "")).strip()

def norm_regno(x):
    return clean(x).upper().replace(" ", "")

def sanitize(x):
    return re.sub(r"[^a-zA-Z0-9]+", "_", clean(x)).strip("_")

def first_words(x, n=3):
    return " ".join(clean(x).split()[:n])

# ================= BULK PHASE =================
def bulk_search(page):
    """Bulk search using smart locator and state machine."""
    # Initialize smart locator and state machine
    locator = SmartLocator(page, logger=logger)
    state_machine = NavigationStateMachine(locator, logger=logger)
    
    print(f"[BULK] Loading product keywords from: {INPUT_PRODUCTS}", flush=True)
    df = pd.read_csv(INPUT_PRODUCTS)
    total_searches = len(df)
    print(f"[BULK] Found {total_searches:,} product keywords to search", flush=True)
    all_csvs = []
    bulk_count_records = []

    bulk_csv_pattern = require_env("SCRIPT_02_BULK_CSV_PATTERN")
    # Ensure BULK_DIR exists before starting
    BULK_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[BULK] Output directory: {BULK_DIR}", flush=True)
    print(f"[BULK] Starting bulk searches...\n", flush=True)
    
    completed = 0
    skipped = 0
    failed = 0
    processed_keywords = set()  # Track processed keywords to prevent duplicates
    
    for i, row in df.iterrows():
        keyword = first_words(row.iloc[0])
        
        # Check for duplicate keywords in the same run
        if keyword in processed_keywords:
            skipped += 1
            print(f"[BULK] [{i+1}/{total_searches}] SKIP {keyword} (duplicate keyword in this run)", flush=True)
            continue
        
        csv_filename = bulk_csv_pattern.format(index=i+1, keyword=sanitize(keyword))
        out_csv = BULK_DIR / csv_filename
        # Ensure parent directory exists for this file
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "page_rows": 0,
            "csv_rows": 0,
            "status": "pending",
            "reason": ""
        }

        if out_csv.exists():
            # Check if file has content (not empty)
            try:
                file_size = out_csv.stat().st_size
                if file_size > 0:
                    skipped += 1
                    print(f"[BULK] [{i+1}/{total_searches}] SKIP {keyword} (already exists with {file_size:,} bytes)", flush=True)
                    all_csvs.append(out_csv)
                    processed_keywords.add(keyword)
                    continue
                else:
                    print(f"[BULK] [{i+1}/{total_searches}] RETRY {keyword} (file exists but is empty)", flush=True)
            except Exception as e:
                print(f"[BULK] [{i+1}/{total_searches}] RETRY {keyword} (error checking file: {e})", flush=True)

        completed += 1
        processed_keywords.add(keyword)  # Mark as processed
        print(f"[BULK] [{i+1}/{total_searches}] SEARCH {keyword} (Completed: {completed}, Skipped: {skipped}, Failed: {failed})", flush=True)
        # Output progress for bulk search
        percent = round((i / total_searches) * 100, 1) if total_searches > 0 else 0
        print(f"[PROGRESS] Bulk search: {i}/{total_searches} ({percent}%)", flush=True)
        
        try:
            print(f"  -> Navigating to search page: {SEARCH_URL}", flush=True)
            page_timeout = int(require_env("SCRIPT_02_PAGE_TIMEOUT"))
            page.goto(SEARCH_URL, timeout=page_timeout)
            state = observe_playwright(page)
            wait_until_idle(state)
            
            # Transition to PAGE_LOADED state
            if not state_machine.transition_to(NavigationState.PAGE_LOADED, reload_on_failure=True):
                raise RuntimeError("Failed to reach PAGE_LOADED state")
            
            # Detect DOM changes
            locator.detect_dom_change("body", "search_page")
            
            print(f"  -> Page loaded, waiting for search form...", flush=True)
            search_by_selector = require_env("SCRIPT_02_SEARCH_BY_SELECTOR")
            search_txt_selector = require_env("SCRIPT_02_SEARCH_TXT_SELECTOR")
            search_button_selector = require_env("SCRIPT_02_SEARCH_BUTTON_SELECTOR")
            
            # Transition to SEARCH_READY state
            search_ready_conditions = [
                StateCondition(element_selector=search_by_selector, min_count=1, max_wait=page_timeout),
                StateCondition(element_selector=search_txt_selector, min_count=1, max_wait=page_timeout),
                StateCondition(element_selector=search_button_selector, min_count=1, max_wait=page_timeout)
            ]
            if not state_machine.transition_to(NavigationState.SEARCH_READY, custom_conditions=search_ready_conditions):
                # Fallback: use smart locator to find elements
                logger.warning("[BULK] State machine transition failed, using smart locator fallback")
                search_by_elem = locator.find_element(css=search_by_selector, timeout=page_timeout, required=True)
                search_txt_elem = locator.find_element(css=search_txt_selector, timeout=page_timeout, required=True)
                search_btn_elem = locator.find_element(css=search_button_selector, timeout=page_timeout, required=True)
            
            print(f"  -> Search form ready", flush=True)
            
            print(f"  -> Selecting search option and entering keyword: '{keyword}'", flush=True)
            page.select_option(search_by_selector, "1")
            pause()  # Human-paced pause after selection
            # Type with human-like delay using press_sequentially (emits per-character events)
            delay_ms = int(type_delay() * 1000) if type_delay() > 0 else 0  # Convert to milliseconds
            if delay_ms > 0:
                # Use press_sequentially for realistic typing with delay
                page.locator(search_txt_selector).press_sequentially(keyword, delay=delay_ms)
            else:
                # Fast fill if human actions disabled
                page.fill(search_txt_selector, keyword)
            pause()  # Human-paced pause after typing
            print(f"  -> Clicking search button...", flush=True)
            search_started_at = time.time()
            page.click(search_button_selector)
            pause()  # Human-paced pause after click
            state = observe_playwright(page)
            wait_until_idle(state)
            _wait_for_search_settle(page, search_started_at, CSV_WAIT_SECONDS, CSV_WAIT_MAX_SECONDS)
            
            # Transition to RESULTS_LOADING state
            state_machine.transition_to(NavigationState.RESULTS_LOADING, reload_on_failure=False)
            
            # Wait for loading to complete and network to be idle
            selector_timeout = int(require_env("SCRIPT_02_SELECTOR_TIMEOUT"))
            print(f"  -> Waiting for loading to complete...", flush=True)
            try:
                page.wait_for_load_state("networkidle", timeout=selector_timeout)
                print(f"  -> Network idle", flush=True)
            except:
                print(f"  -> Network idle timeout, continuing...", flush=True)
            
            result_table_selector = require_env("SCRIPT_02_RESULT_TABLE_SELECTOR")
            result_row_selector = getenv(
                "SCRIPT_02_RESULT_ROW_SELECTOR",
                ""
            )
            if not result_row_selector:
                result_row_selector = (
                    f"{result_table_selector} tbody tr, {result_table_selector} tr"
                )
            csv_button_selector = require_env("SCRIPT_02_CSV_BUTTON_SELECTOR")

            try:
                # Transition to RESULTS_READY state
                results_ready_conditions = [
                    StateCondition(element_selector=result_row_selector, min_count=1, max_wait=selector_timeout),
                    StateCondition(
                        custom_check=lambda p: _check_table_has_data_playwright(p, result_row_selector),
                        max_wait=selector_timeout
                    )
                ]

                if state_machine.transition_to(NavigationState.RESULTS_READY, custom_conditions=results_ready_conditions):
                    print(f"  -> Results table ready", flush=True)
                else:
                    # Fallback: check for no results message
                    print(f"  -> Table not found, checking for 'no results' message...", flush=True)
                    page_text = page.inner_text("body").lower()
                    if "no result" in page_text or "no data" in page_text or "tidak dijumpai" in page_text:
                        print(f"[INFO] No results found for {keyword}", flush=True)
                        out_csv.parent.mkdir(parents=True, exist_ok=True)
                        out_csv.write_text("")
                        # Check for anomalies
                        anomalies = locator.detect_anomalies(
                            table_selector=result_table_selector,
                            csv_path=out_csv,
                            error_text_patterns=["error", "not found", "failed"]
                        )
                        if anomalies:
                            logger.warning(f"[ANOMALY] {keyword}: {anomalies}")
                        time.sleep(1.0)
                        record["status"] = "no_results"
                        record["reason"] = "Quest3+ reported no data"
                        record["csv_rows"] = 0
                        record["page_rows"] = table_row_count
                        _capture_missing_screenshot(page, keyword, "no_results")
                        continue
                    else:
                        # Try to find table with smart locator
                        table = locator.find_element(css=result_table_selector, timeout=selector_timeout, required=False)
                        if not table:
                            raise RuntimeError("Table not found and no 'no results' message detected")
                
                # CRITICAL: Wait for all table data to be fully loaded before CSV download
                data_load_timeout = int(getenv("SCRIPT_02_DATA_LOAD_TIMEOUT", "60000"))  # 60 seconds default
                data_loaded, table_row_count = _wait_for_table_data_loaded(page, result_row_selector, timeout_ms=data_load_timeout)
                info_selector = getenv("SCRIPT_02_TABLE_INFO_SELECTOR", "#searchTable_info")
                total_entries = _get_total_entries_from_info(page, info_selector)
                if total_entries is not None and total_entries > 0:
                    record["page_rows"] = total_entries
                    print(f"  -> Detected {total_entries:,} total entries (info)", flush=True)
                else:
                    record["page_rows"] = table_row_count
                    if table_row_count:
                        print(f"  -> Detected {table_row_count:,} rows on page", flush=True)
                if not data_loaded:
                    logger.warning(f"[WARNING] Table data may not be fully loaded for {keyword}, but proceeding with CSV download")
                
                # Transition to CSV_READY state
                csv_ready_conditions = [
                    StateCondition(element_selector=csv_button_selector, min_count=1, max_wait=selector_timeout),
                    StateCondition(
                        custom_check=lambda p: _check_button_enabled_playwright(p, csv_button_selector),
                        max_wait=5.0
                    )
                ]
                
                if state_machine.transition_to(NavigationState.CSV_READY, custom_conditions=csv_ready_conditions):
                    print(f"  -> CSV button ready", flush=True)
                    # Find button using smart locator
                    btn = locator.find_element(
                        role="button",
                        text="csv",
                        css=csv_button_selector,
                        timeout=selector_timeout,
                        required=False
                    )
                    
                    if not btn:
                        print(f"[WARNING] No CSV button found for {keyword} - may have no results", flush=True)
                        out_csv.parent.mkdir(parents=True, exist_ok=True)
                        out_csv.write_text("")
                        anomalies = locator.detect_anomalies(
                            table_selector=result_table_selector,
                            csv_path=out_csv
                        )
                        if anomalies:
                            logger.warning(f"[ANOMALY] {keyword}: {anomalies}")
                        time.sleep(1.0)
                        record["status"] = "csv_button_missing"
                        record["reason"] = "CSV button not present"
                        record["csv_rows"] = 0
                        _capture_missing_screenshot(page, keyword, "no_csv_button")
                    else:
                        print(f"  -> All data loaded, clicking CSV download button...", flush=True)
                        
                        # Get download timeout from config (default 5 minutes for large files)
                        download_timeout_ms = int(getenv("SCRIPT_02_DOWNLOAD_TIMEOUT", "300000"))  # 5 minutes default
                        
                        download_success = False
                        file_size = 0
                        max_retries = 2  # Retry up to 2 times for large files
                        
                        for download_attempt in range(1, max_retries + 1):
                            try:
                                if download_attempt > 1:
                                    print(f"  -> Retry download attempt {download_attempt}/{max_retries} for {keyword}...", flush=True)
                                    # Wait a bit before retry
                                    time.sleep(3.0)
                                
                                # Set up download with timeout
                                with page.expect_download(timeout=download_timeout_ms) as download_info:
                                    if hasattr(btn, 'click'):
                                        btn.click()
                                    else:
                                        page.click(csv_button_selector)
                                    pause()  # Human-paced pause after click
                                
                                # Save the downloaded file
                                out_csv.parent.mkdir(parents=True, exist_ok=True)
                                
                                # If file exists from previous attempt, remove it first
                                if out_csv.exists() and download_attempt > 1:
                                    try:
                                        out_csv.unlink()
                                    except Exception:
                                        pass
                                
                                download_info.value.save_as(out_csv)
                                
                                # Wait a moment for file to be fully written
                                time.sleep(0.5)
                                
                                # Verify file was downloaded correctly
                                if not out_csv.exists():
                                    raise FileNotFoundError(f"Downloaded file does not exist: {out_csv}")
                                
                                file_size = out_csv.stat().st_size
                                
                                # Check if file is suspiciously small (might be incomplete)
                                if file_size == 0:
                                    raise ValueError(f"Downloaded file is empty (0 bytes)")
                                
                                # Verify file is readable (not corrupted)
                                try:
                                    # Try to read first few lines to verify it's a valid CSV
                                    with open(out_csv, 'r', encoding='utf-8', errors='ignore') as f:
                                        first_line = f.readline()
                                        if not first_line or len(first_line.strip()) == 0:
                                            raise ValueError(f"Downloaded file appears to be empty or corrupted")
                                except Exception as read_err:
                                    if download_attempt < max_retries:
                                        logger.warning(f"[RETRY] File read error for {keyword} (attempt {download_attempt}): {read_err}")
                                        continue
                                    else:
                                        raise ValueError(f"Downloaded file is corrupted or unreadable: {read_err}")
                                
                                # File downloaded successfully
                                download_success = True
                                print(f"[OK] Downloaded CSV for {keyword} ({file_size:,} bytes)", flush=True)
                                break
                                
                            except Exception as download_err:
                                error_msg = str(download_err)
                                error_type = type(download_err).__name__
                                
                                # Check if it's a timeout error
                                is_timeout = (
                                    "timeout" in error_msg.lower() or 
                                    "TimeoutError" in error_type or
                                    "timeout" in error_type
                                )
                                
                                if is_timeout and download_attempt < max_retries:
                                    logger.warning(f"[RETRY] Download timeout for {keyword} (attempt {download_attempt}/{max_retries}): {error_msg}")
                                    logger.info(f"[RETRY] File may be too large, increasing timeout and retrying...")
                                    # Increase timeout for retry (double it)
                                    download_timeout_ms = min(download_timeout_ms * 2, 600000)  # Max 10 minutes
                                    continue
                                elif download_attempt < max_retries:
                                    logger.warning(f"[RETRY] Download error for {keyword} (attempt {download_attempt}/{max_retries}): {error_msg}")
                                    continue
                                else:
                                    # Final attempt failed
                                    logger.error(f"[ERROR] Failed to download CSV for {keyword} after {max_retries} attempts: {error_msg}")
                                    print(f"[ERROR] Download failed for {keyword}: {error_msg}", flush=True)
                                    # Create empty file to mark as failed (will be retried on next run)
                                    out_csv.parent.mkdir(parents=True, exist_ok=True)
                                    out_csv.write_text("")
                                    file_size = 0
                        
                        # Check for anomalies only if download succeeded
                        if download_success and file_size > 0:
                            anomalies = locator.detect_anomalies(
                                table_selector=result_table_selector,
                                csv_path=out_csv
                            )
                            if anomalies:
                                logger.warning(f"[ANOMALY] {keyword}: {anomalies}")
                                # If file is suspiciously small, mark for retry
                                if file_size < 100:
                                    logger.info(f"[RETRY] CSV too small for {keyword} ({file_size} bytes), will retry on next run")
                                    # Don't delete the file - let merge_bulk handle it
                            csv_rows = _count_csv_data_rows(out_csv)
                            record["csv_rows"] = csv_rows
                            page_rows = record.get("page_rows", 0) or 0
                            if page_rows == csv_rows:
                                record["status"] = "count_match"
                            else:
                                record["status"] = "count_mismatch"
                                record["reason"] = f"Page rows {page_rows}, CSV rows {csv_rows}"
                        elif file_size == 0:
                            # Download failed - file is empty, mark for retry
                            logger.warning(f"[RETRY] CSV download failed for {keyword}, empty file created - will retry on next run")
                            record["status"] = "download_failed"
                            record["reason"] = "CSV download failed or returned empty file"
                            record["csv_rows"] = 0
                            _capture_missing_screenshot(page, keyword, "empty_csv")
                        
                        time.sleep(1.0)
                else:
                    # CSV button not ready - create empty file
                    logger.warning(f"[WARNING] CSV button not ready for {keyword}")
                    out_csv.parent.mkdir(parents=True, exist_ok=True)
                    out_csv.write_text("")
                    time.sleep(1.0)
                    
            except Exception as e:
                print(f"[ERROR] Failed to download CSV for {keyword}: {e}", flush=True)
                print(f"[ERROR] Error type: {type(e).__name__}", flush=True)
                logger.exception(f"[ERROR] Exception details for {keyword}")
                out_csv.parent.mkdir(parents=True, exist_ok=True)
                out_csv.write_text("")
                # Capture HTML snapshot on error
                try:
                    snapshot_path = OUT_DIR / f"error_snapshot_{sanitize(keyword)}.html"
                    with open(snapshot_path, "w", encoding="utf-8") as f:
                        f.write(page.content())
                    logger.info(f"[ANOMALY] HTML snapshot saved to {snapshot_path}")
                except Exception:
                    pass
                time.sleep(1.0)
            
        except Exception as outer_error:
            # Catch any unhandled errors in the search process
            failed += 1
            error_msg = str(outer_error)
            print(f"[ERROR] Unexpected error during search for {keyword}: {error_msg}", flush=True)
            print(f"[ERROR] Error type: {type(outer_error).__name__}", flush=True)
            record["status"] = "failed"
            record["reason"] = error_msg
            record["csv_rows"] = 0
            _capture_missing_screenshot(page, keyword, "error")
            
            # Ensure file exists (create empty file on error)
            if not out_csv.exists():
                out_csv.parent.mkdir(parents=True, exist_ok=True)
                out_csv.write_text("")
            
            all_csvs.append(out_csv)
            
            # Pause 1 second after error before continuing
            print(f"  -> Pausing 1s after error before continuing...", flush=True)
            time.sleep(1.0)
            
            print(f"[BULK] [{i+1}/{total_searches}] Failed: {keyword} (Continuing with next search...)\n", flush=True)
            continue
        finally:
            if record["status"] == "pending":
                record["status"] = "unknown"
            _append_bulk_count_record(
                bulk_count_records,
                keyword,
                record["page_rows"],
                record["csv_rows"],
                record["status"],
                record["reason"],
                out_csv
            )
        
        # Success path - ensure file exists and add to list
        if not out_csv.exists():
            out_csv.parent.mkdir(parents=True, exist_ok=True)
            out_csv.write_text("")
        
        all_csvs.append(out_csv)
        print(f"[BULK] [{i+1}/{total_searches}] Completed: {keyword}\n", flush=True)
        
        # Output progress after completion
        percent = round(((i + 1) / total_searches) * 100, 1) if total_searches > 0 else 0
        print(f"[PROGRESS] Bulk search: {i+1}/{total_searches} ({percent}%)", flush=True)
        
        # Small rate-limiting delay between searches (minimal, since we wait dynamically above)
        search_delay = float(require_env("SCRIPT_02_SEARCH_DELAY"))
        if search_delay > 0:
            print(f"  -> Waiting {search_delay}s before next search...", flush=True)
            time.sleep(search_delay)
    
    # Log metrics at end of bulk search
    metrics = locator.get_metrics()
    metrics_summary = metrics.get_summary()
    logger.info(f"[METRICS] Bulk search locator performance: {metrics_summary}")
    
    state_history = state_machine.get_state_history()
    logger.info(f"[METRICS] Bulk search state transitions: {len(state_history)} transitions")
    
    _save_bulk_count_report(bulk_count_records)
    
    return all_csvs


def _wait_for_table_data_loaded(page, row_selector, timeout_ms=60000):
    """
    Wait until all table data is fully loaded before allowing CSV download.

    This function ensures:
    1. Network is idle (no pending requests)
    2. Table rows count is stable (no more rows being added)
    3. Loading indicators are gone
    4. Table has actual data rows

    Args:
        page: Playwright page object
        row_selector: CSS selector(s) for table rows
        timeout_ms: Maximum time to wait in milliseconds

    Returns:
        (bool, int): (True, visible_row_count) if data is loaded, (False, visible_row_count) otherwise.
    """
    start_time = time.time()
    timeout_seconds = timeout_ms / 1000.0
    final_row_count = 0

    print(f"  -> Waiting for all table data to load...", flush=True)

    try:
        page.wait_for_load_state("networkidle", timeout=30000)
        print(f"  -> Network idle", flush=True)
    except Exception:
        logger.debug("Network idle timeout, continuing...")

    stable_checks_required = 3
    stable_count = 0
    last_row_count = -1
    max_stable_wait = timeout_seconds - (time.time() - start_time)

    if max_stable_wait <= 0:
        return False, final_row_count

    check_interval = 1.0
    max_iterations = int(max_stable_wait / check_interval)

    for iteration in range(max_iterations):
        try:
            rows = page.query_selector_all(row_selector)
            visible_rows = [r for r in rows if r.is_visible()]
            current_row_count = len(visible_rows)

            if current_row_count == last_row_count:
                stable_count += 1
                if stable_count >= stable_checks_required:
                    final_row_count = current_row_count
                    print(f"  -> Table rows stable at {current_row_count} rows", flush=True)
                    break
            else:
                stable_count = 0
                last_row_count = current_row_count
                if iteration == 0:
                    print(f"  -> Table has {current_row_count} rows (waiting for stability)...", flush=True)
                elif iteration % 5 == 0:
                    print(f"  -> Table rows: {current_row_count} (waiting for stability)...", flush=True)

            time.sleep(check_interval)
        except Exception as e:
            logger.debug(f"Error checking table stability: {e}")
            time.sleep(check_interval)
            continue

    try:
        for selector in LOADING_SELECTORS:
            try:
                loading_elem = page.query_selector(selector)
                if loading_elem and loading_elem.is_visible():
                    page.wait_for_selector(selector, state="hidden", timeout=10000)
            except Exception:
                pass
    except Exception:
        pass

    try:
        rows = page.query_selector_all(row_selector)
        visible_rows = [r for r in rows if r.is_visible()]
        final_row_count = len(visible_rows)

        if final_row_count == 0:
            logger.warning("Table has no visible rows after wait")
            return False, final_row_count

        for row in visible_rows[:5]:
            try:
                text = row.inner_text().strip()
                if len(text) > 10 and not text.isspace():
                    cells = row.query_selector_all("td, th")
                    if len(cells) >= 2:
                        cell_texts = [cell.inner_text().strip() for cell in cells]
                        non_empty_cells = sum(1 for ct in cell_texts if len(ct) > 0)
                        if non_empty_cells >= 2:
                            final_row_count = _wait_for_additional_rows(
                                page, row_selector, final_row_count, timeout_seconds
                            )
                            print(f"  -> All data loaded ({final_row_count} rows)", flush=True)
                            return True, final_row_count
            except Exception:
                continue

        logger.warning("Table exists but no meaningful data rows found")
        return False, final_row_count
    except Exception as e:
        logger.warning(f"Error verifying table data: {e}")
        return False, final_row_count

def _get_total_entries_from_info(page, info_selector):
    """Return total entries from DataTables info text like 'Showing 1 to 10 of 474 entries'."""
    try:
        info = page.query_selector(info_selector)
        if not info:
            return None
        text = (info.inner_text() or "").strip()
        if not text:
            return None
        match = re.search(r"of\s+([\d,]+)\s+entries", text, re.IGNORECASE)
        if not match:
            return None
        return int(match.group(1).replace(",", ""))
    except Exception:
        return None

def _is_loading_indicator_visible(page):
    """Return True if any known loading indicator is still visible."""
    for selector in LOADING_SELECTORS:
        try:
            elem = page.query_selector(selector)
            if elem and elem.is_visible():
                return True
        except Exception:
            continue
    return False

def _wait_for_search_settle(page, search_started_at, min_wait, max_wait):
    """Wait at least min_wait seconds (and until loading indicators are gone) before proceeding."""
    target_time = search_started_at + min_wait
    deadline = search_started_at + max_wait
    print(f"  -> Ensuring minimum {min_wait}s wait after search click...", flush=True)

    while True:
        now = time.time()
        is_loading = _is_loading_indicator_visible(page)
        if now >= target_time and not is_loading:
            break
        if now >= deadline:
            logger.warning(
                f"[WARNING] Search settle wait exceeded {max_wait}s (loading={is_loading})"
            )
            break
        time_remaining = max(target_time - now, 0)
        if is_loading and now < deadline:
            print("  -> Loading indicator still visible, waiting...", flush=True)
        else:
            print(f"  -> Waiting {round(time_remaining,1)}s more for the minimum delay...", flush=True)
        time.sleep(0.5)


def _check_table_has_data_playwright(page, row_selector):
    """Check if table has data rows (Playwright)."""
    try:
        selectors = [
            selector.strip()
            for selector in row_selector.split(",")
            if selector.strip()
        ]
        if not selectors:
            selectors = [
                "table.table tbody tr",
                "table.table tr",
                "table.table tbody tr:not(:first-child)",
                "table.table tr:not(:first-child)"
            ]

        for selector in selectors:
            try:
                rows = page.query_selector_all(selector)
                visible_rows = [r for r in rows if r.is_visible()]

                for row in visible_rows[:10]:
                    try:
                        text = row.inner_text().strip()
                        if len(text) > 10 and not text.isspace():
                            cells = row.query_selector_all("td, th")
                            if len(cells) >= 2:
                                cell_texts = [cell.inner_text().strip() for cell in cells]
                                non_empty_cells = sum(1 for ct in cell_texts if len(ct) > 0)
                                if non_empty_cells >= 2:
                                    return True
                    except Exception:
                        continue
            except Exception:
                continue

        # Fallback: try to find any table element from the selectors
        for selector in selectors:
            try:
                table_query = selector.split(" ")[0]
                table = page.query_selector(table_query)
                if table and table.is_visible():
                    return True
            except Exception:
                continue

        return False
    except Exception:
        return False


def _check_button_enabled_playwright(page, button_selector):
    """Check if button is enabled (Playwright)."""
    try:
        button = page.query_selector(button_selector)
        if button:
            return not button.is_disabled()
        return False
    except Exception:
        return False

def _wait_for_additional_rows(page, row_selector, initial_count, max_wait_seconds):
    """Wait an extra `DATA_LOAD_WAIT_SECONDS` for table rows to settle."""
    if DATA_LOAD_WAIT_SECONDS <= 0:
        return initial_count
    start_time = time.time()
    deadline = start_time + DATA_LOAD_WAIT_SECONDS
    overall_deadline = start_time + max_wait_seconds
    deadline = min(deadline, overall_deadline)
    last_count = initial_count

    while time.time() < deadline:
        try:
            rows = page.query_selector_all(row_selector)
            visible_rows = [r for r in rows if r.is_visible()]
            current_count = len(visible_rows)
            if current_count != last_count:
                print(f"  -> Table row count updated: {current_count:,} rows (was {last_count:,})", flush=True)
                last_count = current_count
                deadline = min(time.time() + DATA_LOAD_WAIT_SECONDS, overall_deadline)
            time.sleep(0.5)
        except Exception as exc:
            logger.debug(f"Error during additional row wait: {exc}")
            time.sleep(0.5)
    return last_count

def _capture_missing_screenshot(page, keyword, reason):
    """Capture a screenshot when a search returns no data."""
    if not CAPTURE_MISSING_SCREENSHOTS:
        return
    try:
        ks = sanitize(keyword) or "search"
        reason_slug = sanitize(reason) or "missing"
        timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        path = MISSING_SCREENSHOT_DIR / f"{ks}_{reason_slug}_{timestamp}.png"
        MISSING_SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=path, full_page=True)
        logger.info(f"[SCREENSHOT] Saved missing-data screenshot to {path}")
    except Exception as exc:
        logger.debug(f"[SCREENSHOT] Unable to capture screenshot for {keyword}: {exc}")

def _count_csv_data_rows(csv_path):
    """Count non-empty data rows in a downloaded CSV (excluding the header)."""
    if not csv_path.exists():
        return 0
    row_count = 0
    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        header = f.readline()
        if not header:
            return 0
        for line in f:
            if line.strip():
                row_count += 1
    return row_count

def _append_bulk_count_record(records, keyword, page_rows, csv_rows, status, reason, csv_path):
    """Keep track of row count comparisons per bulk search."""
    difference = ""
    if page_rows is not None and csv_rows is not None:
        difference = page_rows - csv_rows
    records.append({
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "keyword": keyword,
        "page_rows": page_rows if page_rows is not None else "",
        "csv_rows": csv_rows,
        "difference": difference,
        "status": status,
        "reason": reason,
        "csv_file": str(csv_path)
    })

def _save_bulk_count_report(records):
    """Save the bulk search count report to disk."""
    if not records:
        return
    try:
        COUNT_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(records).to_csv(COUNT_REPORT_PATH, index=False)
        print(f"[BULK] Saved bulk count report to: {COUNT_REPORT_PATH}", flush=True)
    except Exception as exc:
        logger.warning(f"[BULK] Failed to write bulk count report: {exc}")

# ================= MERGE BULK =================
def merge_bulk(csvs):
    total_files = len(csvs)
    print(f"\n[MERGE] Merging {total_files} bulk search CSV files...", flush=True)
    dfs = []
    processed = 0
    empty_files = []
    corrupted_files = []
    
    for idx, c in enumerate(csvs, 1):
        # Check if file exists and has content
        try:
            if not c.exists():
                print(f"  [{idx}/{total_files}] [SKIP] File does not exist: {c.name}", flush=True)
                empty_files.append(c.name)
                continue
            
            file_size = c.stat().st_size
            
            # Check if file is empty
            if file_size == 0:
                print(f"  [{idx}/{total_files}] [SKIP] Empty file: {c.name} (0 bytes)", flush=True)
                empty_files.append(c.name)
                continue
            
            # Check if file is suspiciously small (might be incomplete download)
            if file_size < 100:
                print(f"  [{idx}/{total_files}] [WARNING] File very small: {c.name} ({file_size} bytes) - may be incomplete", flush=True)
                corrupted_files.append(c.name)
                # Still try to read it, but log warning
            
            # Try to read the CSV file
            try:
                # Try UTF-8 first, fallback to latin-1 if encoding errors occur
                try:
                    df_temp = pd.read_csv(c, on_bad_lines='skip', encoding='utf-8')
                except UnicodeDecodeError:
                    # Fallback to latin-1 for files with encoding issues
                    df_temp = pd.read_csv(c, on_bad_lines='skip', encoding='latin-1')
                row_count = len(df_temp)
                
                # Verify file has actual data (not just headers)
                if row_count == 0:
                    print(f"  [{idx}/{total_files}] [SKIP] File has no data rows: {c.name} (headers only or empty)", flush=True)
                    empty_files.append(c.name)
                    continue
                
                # Check if file has reasonable number of columns (at least 2)
                if len(df_temp.columns) < 2:
                    print(f"  [{idx}/{total_files}] [WARNING] File has too few columns: {c.name} ({len(df_temp.columns)} columns)", flush=True)
                    corrupted_files.append(c.name)
                    # Still include it, but log warning
                
                dfs.append(df_temp)
                processed += 1
                print(f"  [{idx}/{total_files}] [OK] {c.name}: {row_count:,} rows, {len(df_temp.columns)} columns ({file_size:,} bytes)", flush=True)
                
            except pd.errors.EmptyDataError:
                print(f"  [{idx}/{total_files}] [SKIP] File is empty or has no valid CSV data: {c.name}", flush=True)
                empty_files.append(c.name)
                continue
            except pd.errors.ParserError as e:
                print(f"  [{idx}/{total_files}] [WARNING] CSV parsing error for {c.name}: {e}", flush=True)
                corrupted_files.append(c.name)
                # Try to read with more lenient settings
                try:
                    # Try UTF-8 first, fallback to latin-1 if encoding errors occur
                    try:
                        df_temp = pd.read_csv(c, on_bad_lines='skip', encoding='utf-8', sep=None, engine='python')
                    except UnicodeDecodeError:
                        # Fallback to latin-1 for files with encoding issues
                        df_temp = pd.read_csv(c, on_bad_lines='skip', encoding='latin-1', sep=None, engine='python')
                    if len(df_temp) > 0:
                        dfs.append(df_temp)
                        processed += 1
                        print(f"  [{idx}/{total_files}] [OK] {c.name}: {len(df_temp):,} rows (recovered with lenient parsing)", flush=True)
                    else:
                        print(f"  [{idx}/{total_files}] [SKIP] Could not recover data from corrupted file: {c.name}", flush=True)
                except Exception:
                    print(f"  [{idx}/{total_files}] [SKIP] Failed to recover corrupted file: {c.name}", flush=True)
                    continue
            except Exception as e:
                print(f"  [{idx}/{total_files}] [WARNING] Failed to read {c.name}: {e}", flush=True)
                corrupted_files.append(c.name)
                continue
                
        except Exception as e:
            print(f"  [{idx}/{total_files}] [WARNING] Error checking file {c.name}: {e}", flush=True)
            corrupted_files.append(c.name)
            continue
    
    # Summary of files processed
    print(f"[MERGE] Processed {processed}/{total_files} files with data", flush=True)
    if empty_files:
        print(f"[MERGE] Empty files ({len(empty_files)}): {', '.join(empty_files[:5])}{'...' if len(empty_files) > 5 else ''}", flush=True)
        logger.warning(f"[MERGE] Found {len(empty_files)} empty files - these may need to be retried")
    if corrupted_files:
        print(f"[MERGE] Corrupted/problematic files ({len(corrupted_files)}): {', '.join(corrupted_files[:5])}{'...' if len(corrupted_files) > 5 else ''}", flush=True)
        logger.warning(f"[MERGE] Found {len(corrupted_files)} corrupted/problematic files")
    
    print(f"[MERGE] Concatenating dataframes...", flush=True)
    bulk = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    total_rows = len(bulk)
    print(f"[MERGE] Total rows after merge: {total_rows:,}", flush=True)
    
    if total_rows == 0:
        logger.warning(f"[MERGE] WARNING: Merged file has no rows! All CSV files may be empty or corrupted.")
    
    print(f"[MERGE] Saving merged results to: {OUT_BULK}", flush=True)
    bulk.to_csv(OUT_BULK, index=False)
    print(f"[MERGE] Merge complete!", flush=True)
    return bulk

# ================= FIND MISSING =================
def find_missing(bulk_df):
    """Find registration numbers not found in bulk search results."""
    print(f"\n[FIND MISSING] Loading registration numbers from: {INPUT_MALAYSIA}", flush=True)
    malaysia = pd.read_csv(INPUT_MALAYSIA)
    print(f"  -> Processing registration numbers...", flush=True)
    req = set(norm_regno(x) for x in malaysia.iloc[:, 0])
    print(f"  -> Total registration numbers to check: {len(req):,}", flush=True)

    if bulk_df.empty:
        print(f"  -> Bulk search returned no results", flush=True)
        missing = req
    else:
        registration_column = require_env("SCRIPT_02_REGISTRATION_COLUMN")
        print(f"  -> Extracting registration numbers from bulk results...", flush=True)
        bulk_reg = set(norm_regno(x) for x in bulk_df[registration_column])
        print(f"  -> Found in bulk search: {len(bulk_reg):,} registration numbers", flush=True)
        print(f"  -> Calculating missing registration numbers...", flush=True)
        missing = req - bulk_reg
        print(f"  -> Missing from bulk search: {len(missing):,} registration numbers", flush=True)

    print(f"[FIND MISSING] Saving missing registration numbers to: {OUT_MISSING}", flush=True)
    pd.DataFrame({"Registration No": sorted(missing)}).to_csv(OUT_MISSING, index=False)
    print(f"[FIND MISSING] Saved {len(missing):,} missing registration numbers", flush=True)
    return missing

# ================= INDIVIDUAL PHASE =================
def extract_product_details(page, regno, locator=None, state_machine=None):
    """Extract Product Name and Holder from detail page using smart locator."""
    # Initialize locator and state machine if not provided
    if locator is None:
        locator = SmartLocator(page, logger=logger)
    if state_machine is None:
        state_machine = NavigationStateMachine(locator, logger=logger)
    
    detail_url = DETAIL_URL.format(regno)
    print(f"  -> Navigating to detail page: {regno}", flush=True)
    page_timeout = int(require_env("SCRIPT_02_PAGE_TIMEOUT"))
    page.goto(detail_url, timeout=page_timeout)
    
    # Transition to PAGE_LOADED state
    if not state_machine.transition_to(NavigationState.PAGE_LOADED, reload_on_failure=True):
        raise RuntimeError(f"Failed to reach PAGE_LOADED state for {regno}")
    
    # Detect DOM changes
    locator.detect_dom_change("body", f"detail_page_{regno}")
    
    # Wait dynamically for detail page to load - don't use static sleep
    detail_table_selector = require_env("SCRIPT_02_DETAIL_TABLE_SELECTOR")
    selector_timeout = int(require_env("SCRIPT_02_SELECTOR_TIMEOUT"))
    print(f"  -> Waiting for detail page table to load...", flush=True)
    
    # Transition to DETAIL_READY state
    detail_ready_conditions = [
        StateCondition(element_selector=detail_table_selector, min_count=1, max_wait=selector_timeout),
        StateCondition(
            custom_check=lambda p: _check_table_has_data_playwright(p, detail_table_selector),
            max_wait=selector_timeout
        )
    ]
    
    if not state_machine.transition_to(NavigationState.DETAIL_READY, custom_conditions=detail_ready_conditions):
        # Fallback: use smart locator
        table = locator.find_element(css=detail_table_selector, timeout=selector_timeout, required=True)
        if not table:
            raise RuntimeError(f"Detail table not found for {regno}")
    
    print(f"  -> Extracting product details...", flush=True)

    product_name = ""
    holder = ""

    rows = page.query_selector_all(detail_table_selector)
    for r in rows:
        tds = r.query_selector_all("td")

        for td in tds:
            raw = clean(td.inner_text())
            low = raw.lower()

            # Extract Product Name
            product_name_label = require_env("SCRIPT_02_PRODUCT_NAME_LABEL")
            if low.startswith(product_name_label):
                b = td.query_selector("b")
                if b:
                    product_name = clean(b.inner_text())
                else:
                    parts = raw.split(":", 1)
                    product_name = clean(parts[1]) if len(parts) == 2 else ""

            # Must match ONLY "Holder :" field, not "Holder Address :"
            holder_label = require_env("SCRIPT_02_HOLDER_LABEL")
            holder_address_label = require_env("SCRIPT_02_HOLDER_ADDRESS_LABEL")
            if low.startswith(holder_label) and not low.startswith(holder_address_label):
                b = td.query_selector("b")
                if b:
                    holder = clean(b.inner_text())
                else:
                    # fallback: split text after colon if <b> is missing
                    parts = raw.split(":", 1)
                    holder = clean(parts[1]) if len(parts) == 2 else ""

    print(f"  -> Extracted - Product Name: {product_name[:50] if product_name else '(empty)'}..., Holder: {holder[:50] if holder else '(empty)'}...", flush=True)
    return product_name, holder


def individual_phase(page, missing):
    """Extract details for missing registration numbers with incremental saves and timeout handling."""
    # Initialize smart locator and state machine
    locator = SmartLocator(page, logger=logger)
    state_machine = NavigationStateMachine(locator, logger=logger)
    
    print(f"  -> Loading existing results (if any)...", flush=True)
    if OUT_FINAL.exists():
        final_df = pd.read_csv(OUT_FINAL, dtype=str, keep_default_na=False)
        done = set(norm_regno(x) for x in final_df["Registration No"])
        print(f"  -> Found {len(done):,} already processed products", flush=True)
    else:
        final_df = pd.DataFrame(columns=["Registration No", "Product Name", "Holder"])
        done = set()
        print(f"  -> Starting fresh (no existing results)", flush=True)

    processed_count = 0
    total_to_process = len(missing) - len(done)
    print(f"  -> Processing {total_to_process:,} remaining products (saving after each extraction)", flush=True)
    print(f"", flush=True)

    for idx, regno in enumerate(missing, 1):
        if regno in done:
            print(f"[INDIV] [{idx}/{len(missing)}] SKIP {regno} (already processed)", flush=True)
            continue

        processed_count += 1
        print(f"[INDIV] [{idx}/{len(missing)}] DETAIL {regno} (Processing: {processed_count}/{total_to_process})", flush=True)
        
        # Output progress for individual detail extraction
        if total_to_process > 0:
            percent = round((processed_count / total_to_process) * 100, 1)
            print(f"[PROGRESS] Individual search: {processed_count}/{total_to_process} ({percent}%)", flush=True)

        try:
            product_name, holder = extract_product_details(page, regno, locator=locator, state_machine=state_machine)

            # Add to dataframe immediately
            new_row = pd.DataFrame([{
                "Registration No": regno,
                "Product Name": product_name,
                "Holder": holder
            }])
            final_df = pd.concat([final_df, new_row], ignore_index=True)
            done.add(regno)

            # Save after each extraction
            final_df.to_csv(OUT_FINAL, index=False, encoding="utf-8")
            print(f"[SAVED] Saved {processed_count}/{total_to_process} products (just saved: {regno})", flush=True)
            
            # Pause 1 second after data extraction before next search
            print(f"  -> Pausing 1s after data extraction...", flush=True)
            time.sleep(1.0)

        except Exception as e:
            error_msg = str(e)
            print(f"[ERROR] Failed to fetch {regno}: {error_msg}", flush=True)

            # For timeout errors, save what we have and record the failure
            if "timeout" in error_msg.lower() or "TimeoutError" in error_msg:
                print(f"[TIMEOUT] Saving progress and marking {regno} as failed...", flush=True)
                # Add empty entry for failed product so we can track it
                new_row = pd.DataFrame([{
                    "Registration No": regno,
                    "Product Name": f"[TIMEOUT ERROR]",
                    "Holder": ""
                }])
                final_df = pd.concat([final_df, new_row], ignore_index=True)
                done.add(regno)

            # Save progress after error
            final_df.to_csv(OUT_FINAL, index=False, encoding="utf-8")
            print(f"[SAVED] Progress saved. {len(done)}/{len(missing)} complete.", flush=True)
            
            # Pause 1 second after error before continuing
            print(f"  -> Pausing 1s after error before continuing...", flush=True)
            time.sleep(1.0)

            # Continue with next item instead of crashing
            continue

        # Additional rate-limiting delay (if configured) - this is in addition to the 1s pause above
        individual_delay = float(require_env("SCRIPT_02_INDIVIDUAL_DELAY"))
        if individual_delay > 0:
            print(f"  -> Waiting additional {individual_delay}s before next request...", flush=True)
            time.sleep(individual_delay)  # Rate limiting between requests

    # Final save
    if not final_df.empty:
        final_df.to_csv(OUT_FINAL, index=False, encoding="utf-8")
        print(f"[FINAL] Saved all {len(done)} processed products.", flush=True)
    
    # Log metrics
    metrics = locator.get_metrics()
    metrics_summary = metrics.get_summary()
    logger.info(f"[METRICS] Individual phase locator performance: {metrics_summary}")
    
    state_history = state_machine.get_state_history()
    logger.info(f"[METRICS] Individual phase state transitions: {len(state_history)} transitions")


# ================= REPORTING =================
def generate_report(final_df, bulk_df, missing_regnos):
    """Generate human-readable report about missing data and coverage."""
    report_filename = require_env("SCRIPT_02_REPORT_FILE")
    report_path = OUT_DIR / report_filename
    
    # Load expected registration numbers
    malaysia = pd.read_csv(INPUT_MALAYSIA)
    expected_regnos = set(norm_regno(x) for x in malaysia.iloc[:, 0])
    
    # Get actual results
    actual_regnos = set(norm_regno(x) for x in final_df["Registration No"]) if not final_df.empty else set()
    
    # Calculate statistics
    total_expected = len(expected_regnos)
    total_found = len(actual_regnos)
    total_missing = len(expected_regnos - actual_regnos)
    coverage_pct = (total_found / total_expected * 100) if total_expected > 0 else 0
    
    # Missing registration numbers
    missing_regnos_list = sorted(expected_regnos - actual_regnos)
    
    # Products without Product Name
    missing_product_name = final_df[final_df["Product Name"].str.strip() == ""]
    missing_product_name_list = missing_product_name["Registration No"].tolist() if not missing_product_name.empty else []
    
    # Products without Holder
    missing_holder = final_df[final_df["Holder"].str.strip() == ""]
    missing_holder_list = missing_holder["Registration No"].tolist() if not missing_holder.empty else []
    
    # Products missing both
    missing_both = final_df[(final_df["Product Name"].str.strip() == "") & (final_df["Holder"].str.strip() == "")]
    missing_both_list = missing_both["Registration No"].tolist() if not missing_both.empty else []
    
    # Bulk search statistics
    bulk_found = len(bulk_df) if not bulk_df.empty else 0
    bulk_coverage = (bulk_found / total_expected * 100) if total_expected > 0 else 0
    individual_needed = len(missing_regnos)
    
    # Generate report
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("QUEST3+ PRODUCT DETAILS - DATA COVERAGE REPORT\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("\n")
        
        # Summary Statistics
        f.write("=" * 80 + "\n")
        f.write("SUMMARY STATISTICS\n")
        f.write("=" * 80 + "\n")
        f.write(f"Total Expected Products:        {total_expected:,}\n")
        f.write(f"Total Products Found:           {total_found:,}\n")
        f.write(f"Total Products Missing:          {total_missing:,}\n")
        f.write(f"Coverage Percentage:            {coverage_pct:.2f}%\n")
        f.write("\n")
        
        # Data Quality Statistics
        f.write("=" * 80 + "\n")
        f.write("DATA QUALITY STATISTICS\n")
        f.write("=" * 80 + "\n")
        f.write(f"Products with Product Name:     {len(final_df) - len(missing_product_name_list):,}\n")
        f.write(f"Products missing Product Name:  {len(missing_product_name_list):,}\n")
        f.write(f"Products with Holder:           {len(final_df) - len(missing_holder_list):,}\n")
        f.write(f"Products missing Holder:        {len(missing_holder_list):,}\n")
        f.write(f"Products missing both:          {len(missing_both_list):,}\n")
        f.write("\n")
        
        # Bulk Search Statistics
        f.write("=" * 80 + "\n")
        f.write("BULK SEARCH STATISTICS\n")
        f.write("=" * 80 + "\n")
        f.write(f"Products found in bulk search:   {bulk_found:,}\n")
        f.write(f"Bulk search coverage:           {bulk_coverage:.2f}%\n")
        f.write(f"Products requiring individual:  {individual_needed:,}\n")
        f.write("\n")
        
        # Missing Registration Numbers
        if missing_regnos_list:
            f.write("=" * 80 + "\n")
            f.write(f"MISSING REGISTRATION NUMBERS ({len(missing_regnos_list)} products)\n")
            f.write("=" * 80 + "\n")
            f.write("These registration numbers were expected but not found in QUEST3+:\n")
            f.write("\n")
            for i, regno in enumerate(missing_regnos_list, 1):
                f.write(f"  {i:4d}. {regno}\n")
            f.write("\n")
        else:
            f.write("=" * 80 + "\n")
            f.write("MISSING REGISTRATION NUMBERS\n")
            f.write("=" * 80 + "\n")
            f.write("[OK] All expected registration numbers were found!\n")
            f.write("\n")
        
        # Missing Product Names
        if missing_product_name_list:
            f.write("=" * 80 + "\n")
            f.write(f"PRODUCTS MISSING PRODUCT NAME ({len(missing_product_name_list)} products)\n")
            f.write("=" * 80 + "\n")
            f.write("These products were found but are missing Product Name:\n")
            f.write("\n")
            for i, regno in enumerate(missing_product_name_list[:100], 1):  # Limit to first 100
                f.write(f"  {i:4d}. {regno}\n")
            if len(missing_product_name_list) > 100:
                f.write(f"\n  ... and {len(missing_product_name_list) - 100} more (see CSV for complete list)\n")
            f.write("\n")
        else:
            f.write("=" * 80 + "\n")
            f.write("PRODUCTS MISSING PRODUCT NAME\n")
            f.write("=" * 80 + "\n")
            f.write("[OK] All products have Product Name!\n")
            f.write("\n")
        
        # Missing Holders
        if missing_holder_list:
            f.write("=" * 80 + "\n")
            f.write(f"PRODUCTS MISSING HOLDER ({len(missing_holder_list)} products)\n")
            f.write("=" * 80 + "\n")
            f.write("These products were found but are missing Holder information:\n")
            f.write("\n")
            for i, regno in enumerate(missing_holder_list[:100], 1):  # Limit to first 100
                f.write(f"  {i:4d}. {regno}\n")
            if len(missing_holder_list) > 100:
                f.write(f"\n  ... and {len(missing_holder_list) - 100} more (see CSV for complete list)\n")
            f.write("\n")
        else:
            f.write("=" * 80 + "\n")
            f.write("PRODUCTS MISSING HOLDER\n")
            f.write("=" * 80 + "\n")
            f.write("[OK] All products have Holder information!\n")
            f.write("\n")
        
        # Products Missing Both
        if missing_both_list:
            f.write("=" * 80 + "\n")
            f.write(f"PRODUCTS MISSING BOTH PRODUCT NAME AND HOLDER ({len(missing_both_list)} products)\n")
            f.write("=" * 80 + "\n")
            f.write("These products are missing both Product Name and Holder:\n")
            f.write("\n")
            for i, regno in enumerate(missing_both_list, 1):
                f.write(f"  {i:4d}. {regno}\n")
            f.write("\n")
        
        # Recommendations
        f.write("=" * 80 + "\n")
        f.write("RECOMMENDATIONS\n")
        f.write("=" * 80 + "\n")
        if total_missing > 0:
            f.write(f"[WARNING] {total_missing} registration numbers were not found. These may be:\n")
            f.write("   - Deprecated or discontinued products\n")
            f.write("   - Products not yet registered in QUEST3+\n")
            f.write("   - Registration numbers with formatting differences\n")
            f.write("\n")
        if len(missing_product_name_list) > 0 or len(missing_holder_list) > 0:
            f.write("[WARNING] Some products are missing Product Name or Holder information.\n")
            f.write("   Consider re-running Script 02 to retry extraction.\n")
            f.write("\n")
        COVERAGE_HIGH_THRESHOLD = getenv_int("SCRIPT_02_COVERAGE_HIGH_THRESHOLD", 95)
        COVERAGE_MEDIUM_THRESHOLD = getenv_int("SCRIPT_02_COVERAGE_MEDIUM_THRESHOLD", 80)
        if coverage_pct >= COVERAGE_HIGH_THRESHOLD:
            f.write(f"[OK] Excellent coverage! (>{COVERAGE_HIGH_THRESHOLD}%)\n")
        elif coverage_pct >= COVERAGE_MEDIUM_THRESHOLD:
            f.write(f"[WARNING] Good coverage, but some products are missing.\n")
        else:
            f.write("[ERROR] Low coverage. Review missing products and retry.\n")
        f.write("\n")
        
        f.write("=" * 80 + "\n")
        f.write("END OF REPORT\n")
        f.write("=" * 80 + "\n")
    
    print(f"\n[REPORT] Report generated: {report_path}")


# ================= MERGE FINAL RESULTS =================
def merge_final_results(bulk_df, individual_df):
    """Merge bulk search results with individual detail page results."""
    print(f"  -> Preparing bulk results...", flush=True)
    # Prepare bulk results: extract Registration No, Product Name, Holder
    registration_column = require_env("SCRIPT_02_REGISTRATION_COLUMN")
    if not bulk_df.empty and registration_column in bulk_df.columns:
        # Bulk CSV typically has: Registration No / Notification No, Product Name, Holder
        bulk_clean = pd.DataFrame()
        bulk_clean["Registration No"] = bulk_df[registration_column].map(norm_regno)
        
        # Extract Product Name and Holder from bulk results
        if "Product Name" in bulk_df.columns:
            bulk_clean["Product Name"] = bulk_df["Product Name"].map(clean)
        else:
            bulk_clean["Product Name"] = ""
            
        if "Holder" in bulk_df.columns:
            bulk_clean["Holder"] = bulk_df["Holder"].map(clean)
        else:
            bulk_clean["Holder"] = ""
        print(f"  -> Bulk results: {len(bulk_clean):,} rows", flush=True)
    else:
        bulk_clean = pd.DataFrame(columns=["Registration No", "Product Name", "Holder"])
        print(f"  -> Bulk results: empty", flush=True)

    # Prepare individual results
    print(f"  -> Preparing individual results...", flush=True)
    if individual_df.empty:
        individual_clean = pd.DataFrame(columns=["Registration No", "Product Name", "Holder"])
        print(f"  -> Individual results: empty", flush=True)
    else:
        individual_clean = individual_df.copy()
        if "Registration No" in individual_clean.columns:
            individual_clean["Registration No"] = individual_clean["Registration No"].map(norm_regno)
        print(f"  -> Individual results: {len(individual_clean):,} rows", flush=True)

    # Merge: individual results override bulk results for same registration number
    print(f"  -> Merging results (individual overrides bulk for duplicates)...", flush=True)
    # First, combine both dataframes
    combined = pd.concat([bulk_clean, individual_clean], ignore_index=True)
    
    # Remove duplicates, keeping the last (individual results take precedence)
    before_dedup = len(combined)
    final = combined.drop_duplicates(subset=["Registration No"], keep="last")
    after_dedup = len(final)
    if before_dedup != after_dedup:
        print(f"  -> Removed {before_dedup - after_dedup} duplicate registration numbers", flush=True)
    
    # Sort by registration number for consistency
    print(f"  -> Sorting by registration number...", flush=True)
    final = final.sort_values("Registration No").reset_index(drop=True)
    
    return final


# ================= MAIN =================
def main():
    print("\n" + "=" * 70, flush=True)
    print("SCRIPT 02: Product Details Extraction", flush=True)
    print("=" * 70, flush=True)
    print(f"Input products file: {INPUT_PRODUCTS}", flush=True)
    print(f"Input Malaysia file: {INPUT_MALAYSIA}", flush=True)
    print(f"Output directory: {OUT_DIR}", flush=True)
    print("=" * 70 + "\n", flush=True)
    
    # Track Chrome process IDs for this pipeline run
    browser_pids = set()
    repo_root = None
    scraper_name = None
    try:
        from core.chrome_pid_tracker import get_chrome_pids_from_playwright_browser, save_chrome_pids
        from pathlib import Path
        
        # Get repo root (assuming script is in scripts/Malaysia/)
        repo_root = Path(__file__).resolve().parent.parent.parent
        scraper_name = "Malaysia"
    except Exception:
        pass
    
    browser = None
    try:
        with sync_playwright() as p:
            print("[BROWSER] Launching browser...", flush=True)
            browser = p.chromium.launch(headless=HEADLESS)
            context_kwargs = {}
            apply_playwright(context_kwargs)
            context_kwargs.setdefault("accept_downloads", True)
            context = browser.new_context(**context_kwargs)
            page = context.new_page()
            print(f"[BROWSER] Browser launched (headless={HEADLESS})", flush=True)
            
            # Track Playwright browser PIDs
            if repo_root and scraper_name:
                try:
                    from core.chrome_pid_tracker import get_chrome_pids_from_playwright_browser, save_chrome_pids
                    browser_pids = get_chrome_pids_from_playwright_browser(browser)
                    if browser_pids:
                        save_chrome_pids(scraper_name, repo_root, browser_pids)
                except Exception:
                    pass  # PID tracking not critical

            # Stage 1: Bulk search by product type
            print("\n" + "=" * 70, flush=True)
            print("STAGE 1: Bulk Search by Product Type", flush=True)
            print("=" * 70, flush=True)
            bulk_csvs = bulk_search(page)
            print(f"\n[STAGE 1] Bulk search complete. Processing {len(bulk_csvs)} CSV files...", flush=True)
            bulk_df = merge_bulk(bulk_csvs)
            
            # Find missing registration numbers
            print(f"\n[STAGE 1] Finding missing registration numbers...", flush=True)
            missing = find_missing(bulk_df)
            print(f"\n[STAGE 1] Found {len(missing):,} registration numbers not in bulk results", flush=True)
            
            # Stage 2: Individual detail pages for missing products
            if missing:
                print("\n" + "=" * 70, flush=True)
                print(f"STAGE 2: Individual Detail Pages ({len(missing):,} products)", flush=True)
                print("=" * 70, flush=True)
                print(f"[STAGE 2] Starting individual detail extraction for {len(missing):,} products...", flush=True)
                individual_phase(page, missing)
                print(f"\n[STAGE 2] Individual detail extraction complete!", flush=True)
            else:
                print("\n[STAGE 2] All products found in bulk search. Skipping individual phase.", flush=True)

            print(f"[BROWSER] Closing browser...", flush=True)
            browser.close()
            print(f"[BROWSER] Browser closed", flush=True)
    except Exception as e:
        # Ensure browser is closed on error
        if browser:
            try:
                print(f"[BROWSER] Error occurred, closing browser...", flush=True)
                browser.close()
                print(f"[BROWSER] Browser closed after error", flush=True)
            except Exception:
                pass
        raise  # Re-raise the exception after cleanup

    # Load individual results
    print(f"\n[MERGE] Loading individual results...", flush=True)
    if OUT_FINAL.exists():
        print(f"  -> Reading existing final results: {OUT_FINAL}", flush=True)
        individual_df = pd.read_csv(OUT_FINAL, dtype=str, keep_default_na=False)
        print(f"  -> Loaded {len(individual_df):,} individual results", flush=True)
    else:
        print(f"  -> No existing final results found, starting fresh", flush=True)
        individual_df = pd.DataFrame(columns=["Registration No", "Product Name", "Holder"])

    # Merge bulk and individual results
    print("\n" + "=" * 70, flush=True)
    print("MERGING FINAL RESULTS", flush=True)
    print("=" * 70, flush=True)
    print(f"  -> Bulk results: {len(bulk_df):,} rows", flush=True)
    print(f"  -> Individual results: {len(individual_df):,} rows", flush=True)
    final_df = merge_final_results(bulk_df, individual_df)
    print(f"  -> Final merged results: {len(final_df):,} rows", flush=True)
    
    # Save final output
    print(f"\n[SAVE] Saving final results to: {OUT_FINAL}", flush=True)
    final_df.to_csv(OUT_FINAL, index=False, encoding="utf-8")
    print(f"[SAVE] Final results saved!", flush=True)
    
    # Generate report
    print(f"\n[REPORT] Generating coverage report...", flush=True)
    generate_report(final_df, bulk_df, missing)
    print(f"[REPORT] Coverage report generated!", flush=True)
    
    print(f"\n" + "=" * 70, flush=True)
    print(f"[OK] SCRIPT 02 COMPLETE", flush=True)
    print("=" * 70, flush=True)
    print(f"   Total products: {len(final_df):,}", flush=True)
    print(f"   With Product Name: {final_df['Product Name'].str.strip().ne('').sum():,}", flush=True)
    print(f"   With Holder: {final_df['Holder'].str.strip().ne('').sum()}")
    print(f"   Saved to: {OUT_FINAL}")

if __name__ == "__main__":
    run_with_checkpoint(
        main,
        "Malaysia",
        2,
        "Product Details",
        output_files=[OUT_FINAL]
    )
