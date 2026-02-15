#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Belarus Output Formatter - English Export Slate (Same Template as Russia)

Transforms the scraped and translated Belarus RCETH data into the standardized
export format used by Russia:
1. Pricing Data Template (from by_rceth_data)

Uses English fields (inn_en, trade_name_en, dosage_form_en) with fallback to
original when translation is missing.

Input: by_rceth_data table (from database)
Output: Belarus_Pricing_Data.csv (same columns as Russia_Pricing_Data.csv)
"""

from __future__ import annotations

import csv
import os
import sys
from pathlib import Path
from typing import Dict

# Force unbuffered output
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
_script_dir = Path(__file__).parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

# Try to load config
try:
    from config_loader import load_env_file, get_output_dir, get_central_output_dir
    load_env_file()
    USE_CONFIG = True
except ImportError:
    USE_CONFIG = False

    def get_output_dir():
        return _repo_root / "output" / "Belarus"

    def get_central_output_dir():
        return _repo_root / "exports" / "Belarus"

# DB imports
try:
    from core.db.connection import CountryDB
    from db.schema import apply_belarus_schema
    from db.repositories import BelarusRepository
    HAS_DB = True
except ImportError:
    HAS_DB = False


# Same template columns as Russia 05_format_for_export.py
PRICING_OUTPUT_FIELDNAMES = [
    "PCID",
    "Country",
    "Company",
    "Product Group",
    "Generic Name",
    "Start Date",
    "Currency",
    "Ex-Factory Wholesale Price",
    "Local Pack Description",
    "LOCAL_PACK_CODE",
]


def _safe_str(val, default=""):
    """Return stripped string or default."""
    if val is None:
        return default
    return str(val).strip() or default


def _get_translated_lookup(repo: BelarusRepository) -> Dict[int, Dict]:
    """Build rceth_data_id -> translated row lookup from by_translated_data."""
    translated_rows = repo.get_translated_data()
    return {t.get("rceth_data_id"): t for t in translated_rows if t.get("rceth_data_id")}


def format_pricing_data_from_db(repo: BelarusRepository, output_path: Path) -> int:
    """
    Format by_rceth_data into Pricing Data Template (same as Russia).
    Uses translated English fields from by_translated_data with fallback to original.
    """
    products = repo.get_all_rceth_data()
    if not products:
        print(f"[WARNING] No RCETH data found for run {repo.run_id}")
        with output_path.open("w", encoding="utf-8", newline="") as f_out:
            csv.DictWriter(f_out, fieldnames=PRICING_OUTPUT_FIELDNAMES).writeheader()
        return 0

    # Load translated data lookup
    translated_by_id = _get_translated_lookup(repo)
    print(f"[INFO] Loaded {len(translated_by_id)} translated records")
    
    if len(translated_by_id) == 0:
        print(f"[WARNING] No translated data found in by_translated_data for run {repo.run_id}")
        print(f"[WARNING] Export will use original Russian text. Run Step 3 (Process and Translate) first!")
    
    # Count how many will use translated vs original
    using_translated = 0
    using_original = 0

    with output_path.open("w", encoding="utf-8", newline="") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=PRICING_OUTPUT_FIELDNAMES)
        writer.writeheader()
        for p in products:
            rceth_id = p.get("id")
            translated = translated_by_id.get(rceth_id, {}) if rceth_id else {}
            
            # Use translated English fields with fallback to original
            # Track if we're using translated or original
            trade_name_en = translated.get("trade_name_en") or p.get("trade_name_en")
            inn_en = translated.get("inn_en") or p.get("inn_en")
            dosage_form_en = translated.get("dosage_form_en") or p.get("dosage_form_en")
            manufacturer_country_en = translated.get("manufacturer_country_en")
            
            if trade_name_en or inn_en or dosage_form_en or manufacturer_country_en:
                using_translated += 1
            else:
                using_original += 1
            
            product_group = _safe_str(trade_name_en or p.get("trade_name"))
            generic_name = _safe_str(inn_en or p.get("inn"))
            local_pack_desc = _safe_str(dosage_form_en or p.get("dosage_form"))
            # Use manufacturer_en from translated data, fallback to manufacturer_country_en, then original fields
            manufacturer_en = translated.get("manufacturer_en")
            manufacturer_country_en = translated.get("manufacturer_country_en")
            company = _safe_str(
                manufacturer_en or 
                manufacturer_country_en or 
                p.get("manufacturer") or 
                p.get("manufacturer_country")
            )
            price = p.get("wholesale_price") or p.get("retail_price") or ""
            price_str = str(price).strip() if price is not None else ""

            output_row = {
                "PCID": "",
                "Country": "Belarus",
                "Company": company,
                "Product Group": product_group,
                "Generic Name": generic_name,
                "Start Date": _safe_str(p.get("registration_date")),
                "Currency": _safe_str(p.get("currency"), "BYN"),
                "Ex-Factory Wholesale Price": price_str,
                "Local Pack Description": local_pack_desc,
                "LOCAL_PACK_CODE": _safe_str(p.get("registration_number")),
            }
            writer.writerow(output_row)
    
    # Report translation usage
    if using_translated > 0:
        print(f"[INFO] Using translated English text for {using_translated}/{len(products)} records")
    if using_original > 0:
        print(f"[WARNING] Using original Russian text for {using_original}/{len(products)} records (no translation available)")

    # -- Data verification: field completeness in exported CSV --
    import csv as _csv_mod
    _field_stats = {f: 0 for f in PRICING_OUTPUT_FIELDNAMES}
    try:
        with output_path.open("r", encoding="utf-8") as _f:
            _reader = _csv_mod.DictReader(_f)
            _total = 0
            for _row in _reader:
                _total += 1
                for _field in PRICING_OUTPUT_FIELDNAMES:
                    if not _row.get(_field, "").strip():
                        _field_stats[_field] += 1
            print(f"[VERIFY] Export CSV verification: {_total} rows written to {output_path.name}")
            for _field, _empty in _field_stats.items():
                if _empty > 0:
                    print(f"[VERIFY]   Field '{_field}': {_empty}/{_total} rows EMPTY ({_empty/_total*100:.1f}%)")
            if _total != len(products):
                print(f"[VERIFY] WARNING: CSV has {_total} rows but DB had {len(products)} products")
    except Exception as e:
        print(f"[VERIFY] Could not verify exported CSV: {e}")

    return len(products)


def main():
    print()
    print("=" * 80)
    print("BELARUS OUTPUT FORMATTER (English Export Slate - Same as Russia)")
    print("=" * 80)
    print()

    if not HAS_DB:
        print("[ERROR] Database support not available. Cannot format export.")
        return 1

    # Resolve run_id (unified with extract/02 - check multiple locations)
    run_id = os.environ.get("BELARUS_RUN_ID", "").strip()
    if not run_id:
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
                        break
                except Exception:
                    pass
    if not run_id:
        print("[ERROR] No run_id. Set BELARUS_RUN_ID or run pipeline from step 0.")
        return 1

    # Initialize database
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
        
        # Check if translation step has been run
        translated_count = repo.get_translated_data_count()
        rceth_count = repo.get_rceth_data_count()
        if translated_count == 0 and rceth_count > 0:
            print()
            print("[WARNING] =========================================")
            print("[WARNING] Translation step (Step 3) has not been run!")
            print("[WARNING] Export will contain Russian text instead of English.")
            print("[WARNING] =========================================")
            print()
            print("[INFO] To get English export, ensure Step 3 (Process and Translate) runs before Step 4.")
            print()
    except Exception as e:
        print(f"[ERROR] Could not initialize database: {e}")
        return 1

    central_dir = get_central_output_dir()
    central_dir.mkdir(parents=True, exist_ok=True)

    # Output path (exports only - no intermediate output/ files)
    central_pricing = central_dir / "Belarus_Pricing_Data.csv"

    print("[1/1] Formatting RCETH data into Pricing Data Template (English)...")
    row_count = format_pricing_data_from_db(repo, central_pricing)

    # Summary
    print()
    print("=" * 80)
    print("FORMATTING COMPLETE!")
    print("=" * 80)
    print(f"  Pricing Data: {row_count} rows")
    print()
    print("Export files (exports/Belarus/):")
    print(f"  - {central_pricing.name}")
    print()
    print("[AUDIT] English export slate - same template as Russia (PCID, Country, Company,")
    print("        Product Group, Generic Name, Start Date, Currency, Ex-Factory Wholesale Price,")
    print("        Local Pack Description, LOCAL_PACK_CODE)")
    print("=" * 80)
    print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
