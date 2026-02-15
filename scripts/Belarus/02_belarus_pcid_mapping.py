# 02_belarus_pcid_mapping.py
# Belarus PCID Mapping Script
# Maps scraped RCETH data to PCID template format
# Reads from by_rceth_data (database), same as step 01 output
# Python 3.10+
# pip install pandas openpyxl

import sys
import os
from pathlib import Path

# Add repo root to path for imports
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add scripts/Belarus to path for config_loader
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

# Try to load config, fallback to defaults if not available
try:
    from config_loader import load_env_file, getenv, get_input_dir, get_output_dir
    load_env_file()
    USE_CONFIG = True
except ImportError:
    USE_CONFIG = False
    def getenv(key, default=""):
        return os.getenv(key, default)
    def get_input_dir():
        return Path(__file__).parent
    def get_output_dir():
        return _repo_root / "output" / "Belarus"

import pandas as pd
import re

# Use config paths if available
if USE_CONFIG:
    INPUT_DIR = get_input_dir()
    OUTPUT_DIR = get_output_dir()
    OUT_MAPPED = OUTPUT_DIR / "BELARUS_PCID_MAPPED_OUTPUT.csv"
    OUT_UNMATCHED = OUTPUT_DIR / "unmatched_rows.csv"
    PCID_MAPPING_CSV = INPUT_DIR / "Belarus PCID Mapping.csv"
else:
    OUTPUT_DIR = _repo_root / "output" / "Belarus"
    OUT_MAPPED = OUTPUT_DIR / "BELARUS_PCID_MAPPED_OUTPUT.csv"
    OUT_UNMATCHED = OUTPUT_DIR / "unmatched_rows.csv"
    PCID_MAPPING_CSV = _script_dir / "Belarus PCID Mapping.csv"

# DB imports for reading from by_rceth_data
try:
    from core.db.connection import CountryDB
    from db.schema import apply_belarus_schema
    from db.repositories import BelarusRepository
    HAS_DB = True
except ImportError:
    HAS_DB = False


def load_pcid_mapping():
    """Load PCID mapping from input folder"""
    pcid_path = Path(PCID_MAPPING_CSV)
    if not pcid_path.exists():
        print(f"[WARN] PCID mapping file not found: {pcid_path}")
        return {}
    
    try:
        df = pd.read_csv(pcid_path)
        # Create mapping from WHO ATC Code to PCID
        mapping = {}
        for _, row in df.iterrows():
            atc_code = str(row.get("WHO ATC Code", "")).strip().upper()
            pcid = str(row.get("PCID", "")).strip()
            if atc_code and pcid and pcid.upper() != "DUPLICATE":
                if atc_code not in mapping:
                    mapping[atc_code] = pcid
        return mapping
    except Exception as e:
        print(f"[WARN] Error loading PCID mapping: {e}")
        return {}


def norm(s):
    if s is None:
        return ""
    return str(s).strip().lower()


def build_match_key(row):
    """Build match key from scraped data"""
    return "|".join([
        norm(row.get("Generic Name", "")),
        norm(row.get("Local Product Name", "")),
        norm(row.get("dosage_form", row.get("Local Pack Description", ""))),
    ])


def build_scrape_key(row):
    """Build match key from raw scrape"""
    return "|".join([
        norm(row.get("inn", row.get("Generic Name", ""))),
        norm(row.get("trade_name", row.get("Local Product Name", ""))),
        norm(row.get("dosage_form", "")),
    ])


def extract_atc_code(who_atc_code: str) -> str:
    """Extract clean ATC code from WHO ATC Code field"""
    if not who_atc_code:
        return ""
    # Remove any extra text, keep only the code
    match = re.search(r"([A-Z]\d{2}[A-Z]{2}\d{2})", str(who_atc_code).upper())
    if match:
        return match.group(1)
    return str(who_atc_code).strip().upper()


def _get_run_id() -> str:
    """Resolve run_id from env, .current_run_id files (unified with extract/format)."""
    run_id = os.environ.get("BELARUS_RUN_ID", "").strip()
    if run_id:
        return run_id
    for candidate in [
        get_output_dir() / ".current_run_id",
        _script_dir / "output" / ".current_run_id",
        _repo_root / "output" / ".current_run_id",
        _repo_root / "output" / "Belarus" / ".current_run_id",
    ]:
        if candidate.exists():
            try:
                run_id = candidate.read_text(encoding="utf-8").strip()
                if run_id:
                    return run_id
            except Exception:
                pass
    return ""


def _rceth_row_to_template(row: dict) -> dict:
    """Convert by_rceth_data row to template format expected by PCID mapping loop."""
    wholesale = row.get("wholesale_price") or row.get("retail_price")
    return {
        "Country": "BELARUS",
        "Product Group": (row.get("trade_name") or "").strip().upper(),
        "Local Product Name": (row.get("trade_name") or "").strip(),
        "Generic Name": (row.get("inn") or "").strip(),
        "Indication": "",
        "Pack Size": str(row.get("pack_size") or "1").strip(),
        "Effective Start Date": (row.get("registration_date") or "").strip(),
        "Currency": (row.get("currency") or "BYN").strip(),
        "Ex Factory Wholesale Price": str(wholesale).strip() if wholesale is not None else "",
        "max_selling_price": wholesale,
        "VAT Percent": "0.00",
        "Margin Rule": "65 Manual Entry",
        "Package Notes": "",
        "Discontinued": "NO",
        "Region": "EUROPE",
        "WHO ATC Code": (row.get("who_atc_code") or row.get("atc_code") or "").strip(),
        "Marketing Authority": (row.get("manufacturer") or "").strip(),
        "registration_certificate_number": (row.get("registration_number") or "").strip(),
        "import_price": row.get("import_price"),
        "dosage_form": (row.get("dosage_form") or "").strip(),
        "inn": (row.get("inn") or "").strip(),
        "trade_name": (row.get("trade_name") or "").strip(),
    }


def main():
    print("[INFO] Starting Belarus PCID mapping...")
    print(f"[VERIFY] PCID mapping CSV path: {PCID_MAPPING_CSV}")

    if not HAS_DB:
        print("[ERROR] Database support not available. Cannot run PCID mapping.")
        return

    run_id = _get_run_id()
    if not run_id:
        print("[ERROR] No run_id. Set BELARUS_RUN_ID or run pipeline from step 0.")
        return

    # Load RCETH data from database
    try:
        db = CountryDB("Belarus")
        apply_belarus_schema(db)
        repo = BelarusRepository(db, run_id)
        # Fallback: if current run has no RCETH data, use run with most data
        if repo.get_rceth_data_count() == 0:
            best_run = repo.get_best_rceth_run_id()
            if best_run and best_run != run_id:
                print(f"[INFO] Run {run_id} has no RCETH data; using run {best_run} (has data)")
                run_id = best_run
                repo = BelarusRepository(db, run_id)
        rceth_rows = repo.get_all_rceth_data()
    except Exception as e:
        print(f"[ERROR] Could not load RCETH data from database: {e}")
        return

    if not rceth_rows:
        print(f"[WARN] No RCETH data found for run {run_id}. Nothing to map.")
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        column_order = [
            "Country", "Product Group", "Local Product Name", "Generic Name", "Indication",
            "Pack Size", "Effective Start Date", "Currency", "Ex Factory Wholesale Price",
            "VAT Percent", "Margin Rule", "Package Notes", "Discontinued", "Region",
            "WHO ATC Code", "PCID", "Marketing Authority", "Fill Unit", "Fill Size",
            "Pack Unit", "Strength", "Strength Unit", "Import Type", "Import Price",
            "Combination Molecule", "Source", "Client", "LOCAL_PACK_CODE"
        ]
        pd.DataFrame(columns=column_order).to_csv(OUT_MAPPED, index=False, encoding="utf-8-sig")
        pd.DataFrame(columns=column_order).to_csv(OUT_UNMATCHED, index=False, encoding="utf-8-sig")
        return

    # Convert to DataFrame (same shape as old CSV-based flow)
    raw = pd.DataFrame([_rceth_row_to_template(r) for r in rceth_rows])
    print(f"[INFO] Loaded {len(raw)} rows from database (by_rceth_data)")
    
    # Load PCID mapping
    pcid_mapping = load_pcid_mapping()
    print(f"[INFO] Loaded {len(pcid_mapping)} PCID mappings")
    
    # Create lookup dict from scrape
    raw["__k"] = raw.apply(build_scrape_key, axis=1)
    # If multiple rows per key, keep the one with latest scraped time OR first
    if "scraped_at_utc" in raw.columns:
        raw_sorted = raw.sort_values(by=["scraped_at_utc"], ascending=False)
    else:
        raw_sorted = raw
    
    lookup = raw_sorted.drop_duplicates("__k", keep="first").set_index("__k").to_dict(orient="index")
    
    # Process each row and create final output
    output_rows = []
    unmatched = []
    
    for idx, row in raw.iterrows():
        # Get PCID from WHO ATC Code
        who_atc = str(row.get("WHO ATC Code", "")).strip()
        atc_code = extract_atc_code(who_atc)
        pcid = pcid_mapping.get(atc_code, "")
        
        # Build output row in template format
        output_row = {
            "Country": row.get("Country", "BELARUS"),
            "Product Group": row.get("Product Group", ""),
            "Local Product Name": row.get("Local Product Name", ""),
            "Generic Name": row.get("Generic Name", ""),
            "Indication": row.get("Indication", ""),
            "Pack Size": row.get("Pack Size", "1"),
            "Effective Start Date": row.get("Effective Start Date", ""),
            "Currency": row.get("Currency", "BYN"),
            "Ex Factory Wholesale Price": row.get("Ex Factory Wholesale Price", row.get("max_selling_price", "")),
            "VAT Percent": row.get("VAT Percent", "0.00"),
            "Margin Rule": row.get("Margin Rule", "65 Manual Entry"),
            "Package Notes": row.get("Package Notes", ""),
            "Discontinued": row.get("Discontinued", "NO"),
            "Region": row.get("Region", "EUROPE"),
            "WHO ATC Code": who_atc,
            "PCID": pcid,
            "Marketing Authority": row.get("Marketing Authority", row.get("marketing_authorization_holder", "")),
            "Fill Unit": row.get("Fill Unit", ""),
            "Fill Size": row.get("Fill Size", ""),
            "Pack Unit": row.get("Pack Unit", ""),
            "Strength": row.get("Strength", ""),
            "Strength Unit": row.get("Strength Unit", ""),
            "Import Type": row.get("Import Type", "NONE"),
            "Import Price": row.get("Import Price", row.get("import_price", "")),
            "Combination Molecule": row.get("Combination Molecule", "NO"),
            "Source": row.get("Source", "PRICENTRIC"),
            "Client": row.get("Client", "VALUE NEEDED"),
            "LOCAL_PACK_CODE": row.get("LOCAL_PACK_CODE", row.get("registration_certificate_number", "")),
        }
        
        output_rows.append(output_row)
        
        # Track unmatched rows (no PCID found)
        if not pcid:
            unmatched.append(idx)
    
    # Create output DataFrame
    output_df = pd.DataFrame(output_rows)
    
    # Reorder columns to match template
    column_order = [
        "Country", "Product Group", "Local Product Name", "Generic Name", "Indication",
        "Pack Size", "Effective Start Date", "Currency", "Ex Factory Wholesale Price",
        "VAT Percent", "Margin Rule", "Package Notes", "Discontinued", "Region",
        "WHO ATC Code", "PCID", "Marketing Authority", "Fill Unit", "Fill Size",
        "Pack Unit", "Strength", "Strength Unit", "Import Type", "Import Price",
        "Combination Molecule", "Source", "Client", "LOCAL_PACK_CODE"
    ]
    
    # Ensure all columns exist
    for col in column_order:
        if col not in output_df.columns:
            output_df[col] = ""
    
    output_df = output_df[column_order]
    
    # Save mapped output
    output_path = Path(OUT_MAPPED)
    output_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"[SUCCESS] Saved mapped output: {output_path} ({len(output_df)} rows)")
    
    # Save unmatched rows
    if unmatched:
        unmatched_df = output_df.loc[unmatched]
        unmatched_path = Path(OUT_UNMATCHED)
        unmatched_df.to_csv(unmatched_path, index=False, encoding="utf-8-sig")
        print(f"[INFO] Saved unmatched rows: {unmatched_path} ({len(unmatched_df)} rows)")
    else:
        # Create empty unmatched file
        pd.DataFrame(columns=column_order).to_csv(OUT_UNMATCHED, index=False, encoding="utf-8-sig")
        print(f"[INFO] All rows matched successfully")
    
    # -- Data verification: field completeness --
    _fields_to_check = ["Generic Name", "Local Product Name", "WHO ATC Code", "Ex Factory Wholesale Price", "LOCAL_PACK_CODE"]
    for field in _fields_to_check:
        empty_count = output_df[field].isna().sum() + (output_df[field] == "").sum() if field in output_df.columns else len(output_df)
        if empty_count > 0:
            print(f"[VERIFY] Field '{field}': {empty_count}/{len(output_df)} rows EMPTY ({empty_count/len(output_df)*100:.1f}%)")

    # -- Data verification: unique ATC codes coverage --
    unique_atc_in_data = set(output_df["WHO ATC Code"].dropna().unique()) - {""}
    unique_atc_in_mapping = set(pcid_mapping.keys())
    atc_not_in_mapping = unique_atc_in_data - unique_atc_in_mapping
    if atc_not_in_mapping:
        print(f"[VERIFY] ATC codes in data but NOT in PCID mapping ({len(atc_not_in_mapping)}): {', '.join(sorted(atc_not_in_mapping)[:15])}")
    atc_in_mapping_not_data = unique_atc_in_mapping - unique_atc_in_data
    if atc_in_mapping_not_data:
        print(f"[VERIFY] ATC codes in PCID mapping but NOT in scraped data ({len(atc_in_mapping_not_data)}): {', '.join(sorted(atc_in_mapping_not_data)[:15])}")

    # -- Data verification: price sanity --
    price_col = "Ex Factory Wholesale Price"
    if price_col in output_df.columns:
        prices = pd.to_numeric(output_df[price_col], errors="coerce")
        valid_prices = prices.dropna()
        if not valid_prices.empty:
            print(f"[VERIFY] Prices: min={valid_prices.min():.2f}, max={valid_prices.max():.2f}, "
                  f"median={valid_prices.median():.2f}, zero_count={int((valid_prices == 0).sum())}")
        no_price = prices.isna().sum()
        if no_price > 0:
            print(f"[VERIFY] Rows with NO price: {no_price}/{len(output_df)}")

    # Print summary
    print("\n" + "="*60)
    print("MAPPING SUMMARY")
    print("="*60)
    print(f"Total rows processed: {len(output_df)}")
    print(f"Rows with PCID: {len(output_df) - len(unmatched)}")
    print(f"Rows without PCID: {len(unmatched)}")
    print(f"Unique ATC codes in data: {len(unique_atc_in_data)}")
    print(f"PCID mapping entries: {len(pcid_mapping)}")
    print(f"Success rate: {((len(output_df) - len(unmatched)) / len(output_df) * 100):.1f}%" if not output_df.empty else "N/A")
    print("="*60)


if __name__ == "__main__":
    main()
