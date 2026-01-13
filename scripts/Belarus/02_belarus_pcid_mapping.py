# 02_belarus_pcid_mapping.py
# Python 3.10+
# pip install pandas openpyxl

import os
import pandas as pd

RAW_SCRAPE_CSV = "belarus_rceth_raw.csv"

# Use whichever template you want:
TEMPLATE_XLSX = r"/mnt/data/BELARUS PCID MAPPING_12-12-2025_.xlsx"
TEMPLATE_CSV  = r"/mnt/data/BELARUS PCID MAPPING_12-12-2025_.csv"

OUT_MAPPED = "BELARUS_PCID_MAPPED_OUTPUT.csv"
OUT_UNMATCHED = "unmatched_rows.csv"


def load_template():
    if os.path.exists(TEMPLATE_XLSX):
        df = pd.read_excel(TEMPLATE_XLSX)
        return df
    if os.path.exists(TEMPLATE_CSV):
        df = pd.read_csv(TEMPLATE_CSV)
        return df
    raise FileNotFoundError("Template not found. Put template path correctly.")


def norm(s):
    if s is None:
        return ""
    return str(s).strip().lower()


def build_match_key(row):
    # Match key: Generic Name + Local Product Name + Dosage/Form/Local Pack Description
    return "|".join([
        norm(row.get("Generic Name", "")),
        norm(row.get("Local Product Name", "")),
        norm(row.get("Local Pack Description", "")),
    ])


def build_scrape_key(row):
    # Scrape has: inn, trade_name, dosage_form
    return "|".join([
        norm(row.get("inn", "")),
        norm(row.get("trade_name", "")),
        norm(row.get("dosage_form", "")),
    ])


def main():
    tmpl = load_template()
    raw = pd.read_csv(RAW_SCRAPE_CSV)

    # Create lookup dict from scrape
    raw["__k"] = raw.apply(build_scrape_key, axis=1)
    # If multiple rows per key, keep the one with latest scraped time OR first
    raw_sorted = raw.sort_values(by=["scraped_at_utc"], ascending=False)
    lookup = raw_sorted.drop_duplicates("__k", keep="first").set_index("__k").to_dict(orient="index")

    # Add required columns in template if missing
    if "Import Price" not in tmpl.columns:
        tmpl["Import Price"] = ""
    if "Currency" not in tmpl.columns:
        tmpl["Currency"] = ""
    if "Ex Factory Wholesale Price" not in tmpl.columns:
        tmpl["Ex Factory Wholesale Price"] = ""
    if "Local Pack Description" not in tmpl.columns:
        tmpl["Local Pack Description"] = ""

    unmatched = []
    mapped_count = 0

    for i, r in tmpl.iterrows():
        k = build_match_key(r)
        hit = lookup.get(k)
        if not hit:
            unmatched.append(i)
            continue

        # Fill from scrape:
        # Max selling price -> Ex Factory Wholesale Price
        if pd.notna(hit.get("max_selling_price")):
            tmpl.at[i, "Ex Factory Wholesale Price"] = hit.get("max_selling_price")
        if hit.get("max_selling_price_currency"):
            tmpl.at[i, "Currency"] = hit.get("max_selling_price_currency")

        # Import Price (USD) -> new column
        if pd.notna(hit.get("import_price")):
            tmpl.at[i, "Import Price"] = hit.get("import_price")

        # Optional: Keep raw producer/MAH into notes if you want (only if columns exist)
        if "Marketing Authority" in tmpl.columns and hit.get("marketing_authorization_holder"):
            tmpl.at[i, "Marketing Authority"] = hit.get("marketing_authorization_holder")

        if "Local Pack Description" in tmpl.columns and not r.get("Local Pack Description"):
            if hit.get("dosage_form"):
                tmpl.at[i, "Local Pack Description"] = hit.get("dosage_form")

        mapped_count += 1

    tmpl.to_csv(OUT_MAPPED, index=False, encoding="utf-8-sig")

    if unmatched:
        tmpl.loc[unmatched].to_csv(OUT_UNMATCHED, index=False, encoding="utf-8-sig")
    else:
        pd.DataFrame().to_csv(OUT_UNMATCHED, index=False, encoding="utf-8-sig")

    print(f"Mapped rows: {mapped_count}")
    print(f"Saved mapped: {OUT_MAPPED}")
    print(f"Saved unmatched: {OUT_UNMATCHED}")


if __name__ == "__main__":
    main()
