import sys
import os

# Force unbuffered output for real-time console updates
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
os.environ.setdefault('PYTHONUNBUFFERED', '1')

from playwright.sync_api import sync_playwright
import pandas as pd
from pathlib import Path
import re
import time
from config_loader import load_env_file, require_env, getenv, getenv_int, getenv_bool, get_input_dir

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

SEARCH_URL = require_env("SCRIPT_02_SEARCH_URL")
DETAIL_URL = require_env("SCRIPT_02_DETAIL_URL")

WAIT_BULK = int(require_env("SCRIPT_02_WAIT_BULK"))
HEADLESS = getenv_bool("SCRIPT_02_HEADLESS", True)

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
    print(f"[BULK] Loading product keywords from: {INPUT_PRODUCTS}", flush=True)
    df = pd.read_csv(INPUT_PRODUCTS)
    total_searches = len(df)
    print(f"[BULK] Found {total_searches:,} product keywords to search", flush=True)
    all_csvs = []

    bulk_csv_pattern = require_env("SCRIPT_02_BULK_CSV_PATTERN")
    # Ensure BULK_DIR exists before starting
    BULK_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[BULK] Output directory: {BULK_DIR}", flush=True)
    print(f"[BULK] Starting bulk searches...\n", flush=True)
    
    completed = 0
    skipped = 0
    for i, row in df.iterrows():
        keyword = first_words(row.iloc[0])
        csv_filename = bulk_csv_pattern.format(index=i+1, keyword=sanitize(keyword))
        out_csv = BULK_DIR / csv_filename
        # Ensure parent directory exists for this file
        out_csv.parent.mkdir(parents=True, exist_ok=True)

        if out_csv.exists():
            skipped += 1
            print(f"[BULK] [{i+1}/{total_searches}] SKIP {keyword} (already exists)", flush=True)
            all_csvs.append(out_csv)
            continue

        completed += 1
        print(f"[BULK] [{i+1}/{total_searches}] SEARCH {keyword} (Completed: {completed}, Skipped: {skipped})", flush=True)
        print(f"  -> Navigating to search page: {SEARCH_URL}", flush=True)
        page_timeout = int(require_env("SCRIPT_02_PAGE_TIMEOUT"))
        page.goto(SEARCH_URL, timeout=page_timeout)
        print(f"  -> Page loaded, waiting for search form...", flush=True)
        search_by_selector = require_env("SCRIPT_02_SEARCH_BY_SELECTOR")
        search_txt_selector = require_env("SCRIPT_02_SEARCH_TXT_SELECTOR")
        search_button_selector = require_env("SCRIPT_02_SEARCH_BUTTON_SELECTOR")
        
        # Wait for search form to be ready
        page.wait_for_selector(search_by_selector, timeout=page_timeout)
        page.wait_for_selector(search_txt_selector, timeout=page_timeout)
        page.wait_for_selector(search_button_selector, timeout=page_timeout)
        print(f"  -> Search form ready", flush=True)
        
        print(f"  -> Selecting search option and entering keyword: '{keyword}'", flush=True)
        page.select_option(search_by_selector, "1")
        page.fill(search_txt_selector, keyword)
        print(f"  -> Clicking search button...", flush=True)
        page.click(search_button_selector)
        
        # Wait for loading indicator to disappear first
        selector_timeout = int(require_env("SCRIPT_02_SELECTOR_TIMEOUT"))
        print(f"  -> Waiting for 'loading please wait' to disappear...", flush=True)
        try:
            # Wait for loading text to disappear - both "loading" AND "please wait" must be gone
            page.wait_for_function(
                """
                () => {
                    const bodyText = document.body.innerText.toLowerCase();
                    return !bodyText.includes('loading') && !bodyText.includes('please wait');
                }
                """,
                timeout=selector_timeout
            )
            print(f"  -> Loading indicator disappeared", flush=True)
        except Exception as e:
            print(f"  -> Loading wait timed out, continuing: {e}", flush=True)
        
        # Also wait for network to be idle (no active requests)
        print(f"  -> Waiting for network to be idle...", flush=True)
        try:
            page.wait_for_load_state("networkidle", timeout=selector_timeout)
            print(f"  -> Network idle", flush=True)
        except:
            print(f"  -> Network idle timeout, continuing...", flush=True)
        
        result_table_selector = require_env("SCRIPT_02_RESULT_TABLE_SELECTOR")
        csv_button_selector = require_env("SCRIPT_02_CSV_BUTTON_SELECTOR")
        
        try:
            print(f"  -> Waiting for search results table to appear...", flush=True)
            # Wait for table to appear (this waits for the data to load)
            # If no results, table might not appear - check for "no results" message too
            try:
                # First wait for table structure to appear
                page.wait_for_selector(result_table_selector, timeout=selector_timeout, state="visible")
                print(f"  -> Table structure found, waiting for data rows to load...", flush=True)
                
                # Wait for actual data rows with content to appear
                # Check that rows have actual text content (not just empty rows)
                page.wait_for_function(
                    """
                    () => {
                        const rows = document.querySelectorAll('table.table tbody tr');
                        if (rows.length === 0) return false;
                        // Check if at least one row has actual text content
                        for (let row of rows) {
                            const text = row.innerText.trim();
                            if (text.length > 10) { // At least some meaningful content
                                return true;
                            }
                        }
                        return false;
                    }
                    """,
                    timeout=selector_timeout
                )
                
                # Wait for row count to stabilize (data fully loaded)
                print(f"  -> Data rows found, waiting for data to fully load...", flush=True)
                previous_count = 0
                stable_count = 0
                max_wait_time = selector_timeout
                start_time = time.time()
                current_count = 0
                
                while (time.time() - start_time < max_wait_time / 1000):
                    current_rows = page.query_selector_all(result_table_selector)
                    current_count = len(current_rows)
                    
                    if current_count > 0:
                        # Check if rows have actual data
                        has_data = False
                        for row in current_rows[:5]:  # Check first 5 rows
                            text = row.inner_text().strip()
                            if len(text) > 10:
                                has_data = True
                                break
                        
                        if has_data:
                            if current_count == previous_count:
                                stable_count += 1
                                if stable_count >= 3:  # Stable for 3 checks (about 1.5 seconds)
                                    print(f"  -> Data fully loaded: {current_count} rows", flush=True)
                                    break
                            else:
                                stable_count = 0
                                print(f"  -> Loading... {current_count} rows so far", flush=True)
                            previous_count = current_count
                    
                    time.sleep(0.5)  # Check every 500ms
                
                # Additional wait to ensure all data is rendered
                # Wait time depends on number of records - more records need more time
                final_wait = getenv_int("SCRIPT_02_DATA_LOAD_WAIT", 3)  # Default 3 seconds
                if current_count > 100:
                    final_wait = max(final_wait, 5)  # At least 5 seconds for large datasets
                elif current_count > 50:
                    final_wait = max(final_wait, 4)  # 4 seconds for medium datasets
                
                print(f"  -> Waiting {final_wait}s for final data render ({current_count} rows)...", flush=True)
                time.sleep(final_wait)  # Give extra time based on dataset size
                
                print(f"  -> Table found with data! Waiting for CSV button to be ready...", flush=True)
                # Wait for CSV button to be visible and enabled
                page.wait_for_selector(csv_button_selector, timeout=selector_timeout, state="visible")
                # Additional wait to ensure button is clickable (not disabled)
                page.wait_for_selector(f"{csv_button_selector}:not([disabled])", timeout=5000)
                print(f"  -> CSV button ready", flush=True)
                btn = page.query_selector(csv_button_selector)
                if not btn:
                    print(f"[WARNING] No CSV button found for {keyword} - may have no results", flush=True)
                    out_csv.parent.mkdir(parents=True, exist_ok=True)
                    out_csv.write_text("")
                else:
                    print(f"  -> Clicking CSV download button...", flush=True)
                    with page.expect_download() as d:
                        btn.click()
                    out_csv.parent.mkdir(parents=True, exist_ok=True)
                    d.value.save_as(out_csv)
                    file_size = out_csv.stat().st_size if out_csv.exists() else 0
                    print(f"[OK] Downloaded CSV for {keyword} ({file_size:,} bytes)", flush=True)
            except Exception as table_error:
                # Table didn't appear - check if it's because there are no results
                print(f"  -> Table not found, checking for 'no results' message...", flush=True)
                page_text = page.inner_text("body").lower()
                if "no result" in page_text or "no data" in page_text or "tidak dijumpai" in page_text:
                    print(f"[INFO] No results found for {keyword}", flush=True)
                    out_csv.parent.mkdir(parents=True, exist_ok=True)
                    out_csv.write_text("")
                else:
                    # Re-raise if it's a different error
                    raise table_error
        except Exception as e:
            print(f"[ERROR] Failed to download CSV for {keyword}: {e}", flush=True)
            out_csv.parent.mkdir(parents=True, exist_ok=True)
            out_csv.write_text("")

        # Ensure file exists (create empty file if it doesn't exist from any error)
        if not out_csv.exists():
            out_csv.parent.mkdir(parents=True, exist_ok=True)
            out_csv.write_text("")
        
        # Small rate-limiting delay between searches (minimal, since we wait dynamically above)
        search_delay = float(require_env("SCRIPT_02_SEARCH_DELAY"))
        if search_delay > 0:
            print(f"  -> Waiting {search_delay}s before next search...", flush=True)
            time.sleep(search_delay)
        
        all_csvs.append(out_csv)
        print(f"[BULK] [{i+1}/{total_searches}] Completed: {keyword}\n", flush=True)

    return all_csvs

# ================= MERGE BULK =================
def merge_bulk(csvs):
    total_files = len(csvs)
    print(f"\n[MERGE] Merging {total_files} bulk search CSV files...", flush=True)
    dfs = []
    processed = 0
    for idx, c in enumerate(csvs, 1):
        # Check if file exists and has content
        try:
            if not c.exists():
                print(f"  [{idx}/{total_files}] [SKIP] File does not exist: {c.name}", flush=True)
                continue
            file_size = c.stat().st_size
            if file_size > 0:
                try:
                    df_temp = pd.read_csv(c)
                    row_count = len(df_temp)
                    dfs.append(df_temp)
                    processed += 1
                    print(f"  [{idx}/{total_files}] [OK] {c.name}: {row_count:,} rows ({file_size:,} bytes)", flush=True)
                except Exception as e:
                    print(f"  [{idx}/{total_files}] [WARNING] Failed to read {c.name}: {e}", flush=True)
                    continue
            else:
                print(f"  [{idx}/{total_files}] [SKIP] Empty file: {c.name}", flush=True)
        except Exception as e:
            print(f"  [{idx}/{total_files}] [WARNING] Error checking file {c.name}: {e}", flush=True)
            continue
    print(f"[MERGE] Processed {processed}/{total_files} files with data", flush=True)
    print(f"[MERGE] Concatenating dataframes...", flush=True)
    bulk = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    total_rows = len(bulk)
    print(f"[MERGE] Total rows after merge: {total_rows:,}", flush=True)
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
def extract_product_details(page, regno):
    """Extract Product Name and Holder from detail page."""
    detail_url = DETAIL_URL.format(regno)
    print(f"  -> Navigating to detail page: {regno}", flush=True)
    page_timeout = int(require_env("SCRIPT_02_PAGE_TIMEOUT"))
    page.goto(detail_url, timeout=page_timeout)
    
    # Wait dynamically for detail page to load - don't use static sleep
    detail_table_selector = require_env("SCRIPT_02_DETAIL_TABLE_SELECTOR")
    selector_timeout = int(require_env("SCRIPT_02_SELECTOR_TIMEOUT"))
    print(f"  -> Waiting for detail page table to load...", flush=True)
    page.wait_for_selector(detail_table_selector, timeout=selector_timeout, state="visible")
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

        try:
            product_name, holder = extract_product_details(page, regno)

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

            # Continue with next item instead of crashing
            continue

        individual_delay = float(require_env("SCRIPT_02_INDIVIDUAL_DELAY"))
        if individual_delay > 0:
            print(f"  -> Waiting {individual_delay}s before next request...", flush=True)
            time.sleep(individual_delay)  # Rate limiting between requests

    # Final save
    if not final_df.empty:
        final_df.to_csv(OUT_FINAL, index=False, encoding="utf-8")
        print(f"[FINAL] Saved all {len(done)} processed products.", flush=True)


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
    
    with sync_playwright() as p:
        print("[BROWSER] Launching browser...", flush=True)
        browser = p.chromium.launch(headless=HEADLESS)
        page = browser.new_page(accept_downloads=True)
        print(f"[BROWSER] Browser launched (headless={HEADLESS})", flush=True)

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
    main()
