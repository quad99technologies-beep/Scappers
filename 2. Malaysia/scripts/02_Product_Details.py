from playwright.sync_api import sync_playwright
import pandas as pd
from pathlib import Path
import re
import time
from config_loader import load_env_file, getenv

# Load environment variables from .env file
load_env_file()

# ================= CONFIG =================
base_dir_str = getenv("SCRIPT_02_BASE_DIR", "../")
if base_dir_str.startswith("/") or (len(base_dir_str) > 1 and base_dir_str[1] == ":"):
    BASE_DIR = Path(base_dir_str)
else:
    BASE_DIR = Path(__file__).resolve().parent / base_dir_str

input_products_path = getenv("SCRIPT_02_INPUT_PRODUCTS", "input/products.csv")
INPUT_PRODUCTS = BASE_DIR / input_products_path if not Path(input_products_path).is_absolute() else Path(input_products_path)

input_malaysia_path = getenv("SCRIPT_02_INPUT_MALAYSIA", "output/malaysia_drug_prices_view_all.csv")
INPUT_MALAYSIA = BASE_DIR / input_malaysia_path if not Path(input_malaysia_path).is_absolute() else Path(input_malaysia_path)

out_dir_path = getenv("SCRIPT_02_OUT_DIR", "output")
OUT_DIR = BASE_DIR / out_dir_path if not Path(out_dir_path).is_absolute() else Path(out_dir_path)
BULK_DIR = OUT_DIR / "bulk_search_csvs"
BULK_DIR.mkdir(exist_ok=True)

OUT_BULK = OUT_DIR / getenv("SCRIPT_02_OUT_BULK", "quest3_bulk_results.csv")
OUT_MISSING = OUT_DIR / getenv("SCRIPT_02_OUT_MISSING", "quest3_missing_regnos.csv")
OUT_FINAL = OUT_DIR / getenv("SCRIPT_02_OUT_FINAL", "quest3_product_details.csv")

SEARCH_URL = getenv("SCRIPT_02_SEARCH_URL", "https://quest3plus.bpfk.gov.my/pmo2/index.php")
DETAIL_URL = getenv("SCRIPT_02_DETAIL_URL", "https://quest3plus.bpfk.gov.my/pmo2/detail.php?type=product&id={}")

WAIT_BULK = int(getenv("SCRIPT_02_WAIT_BULK", "125"))
HEADLESS = getenv("SCRIPT_02_HEADLESS", "true").lower() == "true"  # Default to headless (hide browser)

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
    df = pd.read_csv(INPUT_PRODUCTS)
    all_csvs = []

    for i, row in df.iterrows():
        keyword = first_words(row.iloc[0])
        out_csv = BULK_DIR / f"bulk_search_{i+1:03d}_{sanitize(keyword)}.csv"

        if out_csv.exists():
            print(f"[BULK] SKIP {keyword}")
            all_csvs.append(out_csv)
            continue

        print(f"[BULK] SEARCH {keyword}")
        page_timeout = int(getenv("SCRIPT_02_PAGE_TIMEOUT", "300000"))
        page.goto(SEARCH_URL, timeout=page_timeout)
        page.select_option("#searchBy", "1")
        page.fill("#searchTxt", keyword)
        page.click("button.btn-primary")
        search_delay = float(getenv("SCRIPT_02_SEARCH_DELAY", "25"))
        time.sleep(search_delay)

        try:
            selector_timeout = int(getenv("SCRIPT_02_SELECTOR_TIMEOUT", "25000"))
            page.wait_for_selector("table.table tbody tr", timeout=selector_timeout)
            btn = page.query_selector("button.buttons-csv")
            if not btn:
                raise Exception("No CSV")

            with page.expect_download() as d:
                btn.click()
            d.value.save_as(out_csv)
        except:
            out_csv.write_text("")

        time.sleep(WAIT_BULK)
        all_csvs.append(out_csv)

    return all_csvs

# ================= MERGE BULK =================
def merge_bulk(csvs):
    dfs = []
    for c in csvs:
        if c.stat().st_size > 0:
            dfs.append(pd.read_csv(c))
    bulk = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    bulk.to_csv(OUT_BULK, index=False)
    return bulk

# ================= FIND MISSING =================
def find_missing(bulk_df):
    """Find registration numbers not found in bulk search results."""
    malaysia = pd.read_csv(INPUT_MALAYSIA)
    req = set(norm_regno(x) for x in malaysia.iloc[:, 0])

    if bulk_df.empty:
        missing = req
    else:
        bulk_reg = set(norm_regno(x) for x in bulk_df["Registration No / Notification No"])
        missing = req - bulk_reg

    pd.DataFrame({"Registration No": sorted(missing)}).to_csv(OUT_MISSING, index=False)
    return missing

# ================= INDIVIDUAL PHASE =================
def extract_product_details(page, regno):
    """Extract Product Name and Holder from detail page."""
    page_timeout = int(getenv("SCRIPT_02_PAGE_TIMEOUT", "300000"))
    page.goto(DETAIL_URL.format(regno), timeout=page_timeout)
    detail_delay = float(getenv("SCRIPT_02_DETAIL_DELAY", "15"))
    time.sleep(detail_delay)

    product_name = ""
    holder = ""

    rows = page.query_selector_all("table.table tr")
    for r in rows:
        tds = r.query_selector_all("td")

        for td in tds:
            raw = clean(td.inner_text())
            low = raw.lower()

            # Extract Product Name
            if low.startswith("product name :"):
                b = td.query_selector("b")
                if b:
                    product_name = clean(b.inner_text())
                else:
                    parts = raw.split(":", 1)
                    product_name = clean(parts[1]) if len(parts) == 2 else ""

            # Must match ONLY "Holder :" field, not "Holder Address :"
            if low.startswith("holder :") and not low.startswith("holder address"):
                b = td.query_selector("b")
                if b:
                    holder = clean(b.inner_text())
                else:
                    # fallback: split text after colon if <b> is missing
                    parts = raw.split(":", 1)
                    holder = clean(parts[1]) if len(parts) == 2 else ""

    return product_name, holder


def individual_phase(page, missing):
    """Extract details for missing registration numbers with incremental saves and timeout handling."""
    if OUT_FINAL.exists():
        final_df = pd.read_csv(OUT_FINAL, dtype=str, keep_default_na=False)
        done = set(norm_regno(x) for x in final_df["Registration No"])
    else:
        final_df = pd.DataFrame(columns=["Registration No", "Product Name", "Holder"])
        done = set()

    save_interval = int(getenv("SCRIPT_02_SAVE_INTERVAL", "10"))  # Save every N products
    processed_count = 0

    for regno in missing:
        if regno in done:
            print(f"[INDIV] SKIP {regno}")
            continue

        print(f"[INDIV] DETAIL {regno}")

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
            processed_count += 1

            # Save incrementally every N products
            if processed_count % save_interval == 0:
                final_df.to_csv(OUT_FINAL, index=False, encoding="utf-8")
                print(f"[PROGRESS] Saved {processed_count} products so far...")

        except Exception as e:
            error_msg = str(e)
            print(f"[ERROR] Failed to fetch {regno}: {error_msg}")

            # For timeout errors, save what we have and record the failure
            if "timeout" in error_msg.lower() or "TimeoutError" in error_msg:
                print(f"[TIMEOUT] Saving progress and marking {regno} as failed...")
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
            print(f"[SAVED] Progress saved. {len(done)}/{len(missing)} complete.")

            # Continue with next item instead of crashing
            continue

        individual_delay = float(getenv("SCRIPT_02_INDIVIDUAL_DELAY", "15"))
        time.sleep(individual_delay)  # Rate limiting between requests

    # Final save
    if not final_df.empty:
        final_df.to_csv(OUT_FINAL, index=False, encoding="utf-8")
        print(f"[FINAL] Saved all {len(done)} processed products.")


# ================= REPORTING =================
def generate_report(final_df, bulk_df, missing_regnos):
    """Generate human-readable report about missing data and coverage."""
    report_path = OUT_DIR / "quest3_product_details_report.txt"
    
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
        if coverage_pct >= 95:
            f.write("[OK] Excellent coverage! (>95%)\n")
        elif coverage_pct >= 80:
            f.write("[WARNING] Good coverage, but some products are missing.\n")
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
    # Prepare bulk results: extract Registration No, Product Name, Holder
    if not bulk_df.empty and "Registration No / Notification No" in bulk_df.columns:
        # Bulk CSV typically has: Registration No / Notification No, Product Name, Holder
        bulk_clean = pd.DataFrame()
        bulk_clean["Registration No"] = bulk_df["Registration No / Notification No"].map(norm_regno)
        
        # Extract Product Name and Holder from bulk results
        if "Product Name" in bulk_df.columns:
            bulk_clean["Product Name"] = bulk_df["Product Name"].map(clean)
        else:
            bulk_clean["Product Name"] = ""
            
        if "Holder" in bulk_df.columns:
            bulk_clean["Holder"] = bulk_df["Holder"].map(clean)
        else:
            bulk_clean["Holder"] = ""
    else:
        bulk_clean = pd.DataFrame(columns=["Registration No", "Product Name", "Holder"])

    # Prepare individual results
    if individual_df.empty:
        individual_clean = pd.DataFrame(columns=["Registration No", "Product Name", "Holder"])
    else:
        individual_clean = individual_df.copy()
        if "Registration No" in individual_clean.columns:
            individual_clean["Registration No"] = individual_clean["Registration No"].map(norm_regno)

    # Merge: individual results override bulk results for same registration number
    # First, combine both dataframes
    combined = pd.concat([bulk_clean, individual_clean], ignore_index=True)
    
    # Remove duplicates, keeping the last (individual results take precedence)
    final = combined.drop_duplicates(subset=["Registration No"], keep="last")
    
    # Sort by registration number for consistency
    final = final.sort_values("Registration No").reset_index(drop=True)
    
    return final


# ================= MAIN =================
def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        page = browser.new_page(accept_downloads=True)

        # Stage 1: Bulk search by product type
        print("=" * 70)
        print("STAGE 1: Bulk Search by Product Type")
        print("=" * 70)
        bulk_csvs = bulk_search(page)
        bulk_df = merge_bulk(bulk_csvs)
        
        # Find missing registration numbers
        missing = find_missing(bulk_df)
        print(f"\nFound {len(missing)} registration numbers not in bulk results")
        
        # Stage 2: Individual detail pages for missing products
        if missing:
            print("\n" + "=" * 70)
            print(f"STAGE 2: Individual Detail Pages ({len(missing)} products)")
            print("=" * 70)
            individual_phase(page, missing)
        else:
            print("\nAll products found in bulk search. Skipping individual phase.")

        browser.close()

    # Load individual results
    if OUT_FINAL.exists():
        individual_df = pd.read_csv(OUT_FINAL, dtype=str, keep_default_na=False)
    else:
        individual_df = pd.DataFrame(columns=["Registration No", "Product Name", "Holder"])

    # Merge bulk and individual results
    print("\n" + "=" * 70)
    print("Merging Results")
    print("=" * 70)
    final_df = merge_final_results(bulk_df, individual_df)
    
    # Save final output
    final_df.to_csv(OUT_FINAL, index=False, encoding="utf-8")
    
    # Generate report
    generate_report(final_df, bulk_df, missing)
    
    print(f"\n[OK] COMPLETE")
    print(f"   Total products: {len(final_df)}")
    print(f"   With Product Name: {final_df['Product Name'].str.strip().ne('').sum()}")
    print(f"   With Holder: {final_df['Holder'].str.strip().ne('').sum()}")
    print(f"   Saved to: {OUT_FINAL}")

if __name__ == "__main__":
    main()
