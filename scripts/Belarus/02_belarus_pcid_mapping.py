# 02_belarus_pcid_mapping.py
# Belarus PCID Mapping Script
# Maps scraped RCETH data to PCID template format
# Produces 4 standard CSVs: mapped, missing, oos, no_data
# Python 3.10+

import sys
import os
import re
from datetime import datetime
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

from core.utils.pcid_mapper import PcidMapper
from core.utils.pcid_export import categorize_products, write_standard_exports

# Use config paths if available
if USE_CONFIG:
    OUTPUT_DIR = get_output_dir()
else:
    OUTPUT_DIR = _repo_root / "output" / "Belarus"

EXPORTS_DIR = _repo_root / "exports" / "Belarus"
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

DATE_STAMP = datetime.now().strftime("%d%m%Y")

# DB imports for reading from by_rceth_data
try:
    from core.db.connection import CountryDB
    from db.schema import apply_belarus_schema
    from db.repositories import BelarusRepository
    HAS_DB = True
except ImportError:
    HAS_DB = False


EXPORT_COLUMNS = [
    "Country", "Product Group", "Local Product Name", "Generic Name", "Indication",
    "Pack Size", "Effective Start Date", "Currency", "Ex Factory Wholesale Price",
    "VAT Percent", "Margin Rule", "Package Notes", "Discontinued", "Region",
    "WHO ATC Code", "PCID", "Marketing Authority", "Fill Unit", "Fill Size",
    "Pack Unit", "Strength", "Strength Unit", "Import Type", "Import Price",
    "Combination Molecule", "Source", "Client", "LOCAL_PACK_CODE",
]

NO_DATA_COLUMNS = [
    "PCID", "Country", "WHO ATC Code", "Generic Name",
]

NO_DATA_FIELD_MAP = {
    "PCID": "pcid",
    "Country": "_country",
    "WHO ATC Code": "atc_code",
    "Generic Name": "generic_name",
}


def extract_atc_code(who_atc_code: str) -> str:
    """Extract clean ATC code from WHO ATC Code field."""
    if not who_atc_code:
        return ""
    match = re.search(r"([A-Z]\d{2}[A-Z]{2}\d{2})", str(who_atc_code).upper())
    if match:
        return match.group(1)
    return str(who_atc_code).strip().upper()


def _get_run_id() -> str:
    """Resolve run_id from env, .current_run_id files."""
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
    """Convert by_rceth_data row to template format expected by PCID mapping."""
    wholesale = row.get("wholesale_price") or row.get("retail_price")
    # Extract and clean ATC code for matching
    raw_atc = (row.get("who_atc_code") or row.get("atc_code") or "").strip()
    clean_atc = extract_atc_code(raw_atc)
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
        "VAT Percent": "0.00",
        "Margin Rule": "65 Manual Entry",
        "Package Notes": "",
        "Discontinued": "NO",
        "Region": "EUROPE",
        "WHO ATC Code": clean_atc,
        "Marketing Authority": (row.get("manufacturer") or "").strip(),
        "Fill Unit": "",
        "Fill Size": "",
        "Pack Unit": "",
        "Strength": "",
        "Strength Unit": "",
        "Import Type": "NONE",
        "Import Price": row.get("import_price") or "",
        "Combination Molecule": "NO",
        "Source": "PRICENTRIC",
        "Client": "VALUE NEEDED",
        "LOCAL_PACK_CODE": (row.get("registration_number") or "").strip(),
    }


def load_pcid_reference(db) -> list:
    """Load PCID reference as list of dicts for PcidMapper."""
    try:
        with db.cursor(dict_cursor=True) as cur:
            cur.execute("""
                SELECT pcid, generic_name, atc_code, company,
                       local_product_name, local_pack_description
                FROM pcid_mapping
                WHERE source_country = 'Belarus'
            """)
            rows = [dict(r) for r in cur.fetchall()]
            # For Belarus, the ATC code may be stored in generic_name or atc_code.
            # Normalize: ensure each row has a clean atc_code field for matching.
            for row in rows:
                atc = row.get("atc_code") or row.get("generic_name") or ""
                row["atc_code"] = extract_atc_code(atc)
            print(f"[INFO] Loaded {len(rows)} PCID reference entries from database")
            return rows
    except Exception as e:
        print(f"[ERROR] Failed to load PCID reference from DB: {e}")
        return []


def main():
    print("[INFO] Starting Belarus PCID mapping (DB-Only Mode)...")

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
        pd.DataFrame(columns=EXPORT_COLUMNS).to_csv(
            OUTPUT_DIR / "BELARUS_PCID_MAPPED_OUTPUT.csv", index=False, encoding="utf-8-sig"
        )
        return

    # Convert to product dicts
    products = [_rceth_row_to_template(r) for r in rceth_rows]
    print(f"[INFO] Loaded {len(products)} rows from database (by_rceth_data)")

    # Load PCID reference and build mapper
    reference_data = load_pcid_reference(db)

    env_mapping = os.environ.get("PCID_MAPPING_BELARUS", "")
    if env_mapping:
        mapper = PcidMapper.from_env_string(env_mapping)
        print(f"[INFO] Using PCID mapping from env: {env_mapping}")
    else:
        print("[INFO] Using default PCID mapping strategy (WHO ATC Code)")
        mapper = PcidMapper([{"WHO ATC Code": "atc_code"}])

    mapper.build_reference_store(reference_data)

    # Categorize using shared utility
    result = categorize_products(products, mapper)

    # Add country to no_data references
    for ref in result.no_data:
        ref["_country"] = "BELARUS"

    # Write 4 standard CSVs
    files_written = write_standard_exports(
        result=result,
        exports_dir=EXPORTS_DIR,
        prefix="belarus",
        date_stamp=DATE_STAMP,
        product_columns=EXPORT_COLUMNS,
        no_data_columns=NO_DATA_COLUMNS,
        no_data_field_map=NO_DATA_FIELD_MAP,
    )

    for report_type, (fpath, row_count) in files_written.items():
        print(f"  {fpath.name}: {row_count} rows")

    # Data verification
    all_products = result.mapped + result.missing + result.oos
    if all_products:
        output_df = pd.DataFrame(all_products)
        _fields_to_check = ["Generic Name", "Local Product Name", "WHO ATC Code", "Ex Factory Wholesale Price", "LOCAL_PACK_CODE"]
        for field in _fields_to_check:
            if field in output_df.columns:
                empty_count = output_df[field].isna().sum() + (output_df[field] == "").sum()
                if empty_count > 0:
                    print(f"[VERIFY] Field '{field}': {empty_count}/{len(output_df)} rows EMPTY ({empty_count/len(output_df)*100:.1f}%)")

        price_col = "Ex Factory Wholesale Price"
        if price_col in output_df.columns:
            prices = pd.to_numeric(output_df[price_col], errors="coerce")
            valid_prices = prices.dropna()
            if not valid_prices.empty:
                print(f"[VERIFY] Prices: min={valid_prices.min():.2f}, max={valid_prices.max():.2f}, "
                      f"median={valid_prices.median():.2f}, zero_count={int((valid_prices == 0).sum())}")

    # Summary
    total = len(products)
    match_pct = round(len(result.mapped) / total * 100, 1) if total else 0

    print()
    print("=" * 60)
    print("MAPPING SUMMARY")
    print("=" * 60)
    print(f"  Total rows:    {total}")
    print(f"  Mapped:        {len(result.mapped)} ({match_pct}%)")
    print(f"  Missing:       {len(result.missing)}")
    print(f"  OOS:           {len(result.oos)}")
    print(f"  No data (ref): {len(result.no_data)}")
    print(f"  Export dir:    {EXPORTS_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
