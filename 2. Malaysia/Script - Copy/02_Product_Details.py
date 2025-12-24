from playwright.sync_api import sync_playwright
import pandas as pd
from pathlib import Path
import re
import time
import shutil

# ================= CONFIG =================
BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_PRODUCTS = BASE_DIR / "Input" / "products.csv"
INPUT_MALAYSIA = BASE_DIR / "Output" / "malaysia_drug_prices_view_all.csv"

OUT_DIR = BASE_DIR / "Output"
BULK_DIR = OUT_DIR / "bulk_search_csvs"
BULK_DIR.mkdir(exist_ok=True)

OUT_BULK = OUT_DIR / "quest3_bulk_results.csv"
OUT_MISSING = OUT_DIR / "quest3_missing_regnos.csv"
OUT_FINAL = OUT_DIR / "quest3_product_details.csv"

SEARCH_URL = "https://quest3plus.bpfk.gov.my/pmo2/index.php"
DETAIL_URL = "https://quest3plus.bpfk.gov.my/pmo2/detail.php?type=product&id={}"

WAIT_BULK = 25
HEADLESS = False

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
        page.goto(SEARCH_URL, timeout=60000)
        page.select_option("#searchBy", "1")
        page.fill("#searchTxt", keyword)
        page.click("button.btn-primary")
        time.sleep(5)

        try:
            page.wait_for_selector("table.table tbody tr", timeout=5000)
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
    page.goto(DETAIL_URL.format(regno), timeout=60000)
    time.sleep(3)

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
    """Extract details for missing registration numbers."""
    if OUT_FINAL.exists():
        final_df = pd.read_csv(OUT_FINAL)
        done = set(norm_regno(x) for x in final_df["Registration No"])
    else:
        final_df = pd.DataFrame(columns=["Registration No", "Product Name", "Holder"])
        done = set()

    rows = []

    for regno in missing:
        if regno in done:
            print(f"[INDIV] SKIP {regno}")
            continue

        print(f"[INDIV] DETAIL {regno}")
        product_name, holder = extract_product_details(page, regno)
        rows.append({
            "Registration No": regno,
            "Product Name": product_name,
            "Holder": holder
        })
        time.sleep(3)  # Rate limiting between requests

    if rows:
        final_df = pd.concat([final_df, pd.DataFrame(rows)], ignore_index=True)
        final_df.to_csv(OUT_FINAL, index=False)

# ================= MAIN =================
def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        page = browser.new_page(accept_downloads=True)

        bulk_csvs = bulk_search(page)
        bulk_df = merge_bulk(bulk_csvs)
        missing = find_missing(bulk_df)
        individual_phase(page, missing)

        browser.close()

    print("DONE")

if __name__ == "__main__":
    main()
