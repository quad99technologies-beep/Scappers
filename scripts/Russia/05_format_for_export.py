#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Russia Output Formatter - DB-Based (No CSV)

Transforms the scraped and translated Russia data into standardized export formats:
1. Pricing Data Template (from VED list)
2. Discontinued List Template (from Excluded list)

Input: ru_translated_products table (from database)
Output: CSV files in standardized templates
"""

from __future__ import annotations

import csv
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None

# Add repo root and script dir to path (script dir first to avoid loading another scraper's db)
_repo_root = Path(__file__).resolve().parents[2]
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Clear conflicting 'db' when run in same process as other scrapers (e.g. GUI)
for mod in list(sys.modules.keys()):
    if mod == "db" or mod.startswith("db."):
        del sys.modules[mod]

# Try to load config
try:
    from config_loader import load_env_file, get_output_dir, get_central_output_dir
    load_env_file()
    USE_CONFIG = True
except ImportError:
    USE_CONFIG = False
    def get_output_dir():
        return _repo_root / "output" / "Russia"
    def get_central_output_dir():
        return _repo_root / "exports" / "Russia"

# DB imports
from core.db.connection import CountryDB
from db.schema import apply_russia_schema
from db.repositories import RussiaRepository


def _translated_lookup(repo: RussiaRepository) -> Dict[str, Dict]:
    """Build item_id -> translated row lookup (one per item_id from ru_translated_products)."""
    rows = repo.get_translated_products()
    return {r["item_id"]: r for r in rows} if rows else {}


def format_pricing_data_from_db(
    repo: RussiaRepository,
    ved_run_id: str,
    output_path: Path,
    translated_by_item: Dict[str, Dict],
) -> int:
    """
    Format VED data into Pricing Data Template: one export row per VED source row.
    Uses ru_ved_products (full row count) + translated fields from ru_translated_products by item_id.
    """
    ved_products = repo.get_ved_products_for_run(ved_run_id) if ved_run_id else []
    output_fieldnames = [
        "PCID",
        "Country",
        "Company",
        "Product Group",
        "Generic Name",
        "Start Date",
        "Currency",
        "Ex-Factory Wholesale Price",
        "Local Pack Description",
        "LOCAL_PACK_CODE"
    ]
    if not ved_products:
        print(f"[WARNING] No VED products found for run {ved_run_id or 'N/A'}")
        with output_path.open("w", encoding="utf-8", newline="") as f_out:
            csv.DictWriter(f_out, fieldnames=output_fieldnames).writeheader()
        return 0

    with output_path.open("w", encoding="utf-8", newline="") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=output_fieldnames)
        writer.writeheader()
        for product in ved_products:
            item_id = product.get("item_id", "")
            trans = translated_by_item.get(item_id, {})
            output_row = {
                "PCID": "",
                "Country": "Russia",
                "Company": trans.get("manufacturer_country_en", "").strip(),
                "Product Group": trans.get("tn_en", "").strip(),
                "Generic Name": trans.get("inn_en", "").strip(),
                "Start Date": trans.get("start_date_text", product.get("start_date_text", "") or "").strip(),
                "Currency": "RUB",
                "Ex-Factory Wholesale Price": (trans.get("registered_price_rub") or product.get("registered_price_rub") or "").strip(),
                "Local Pack Description": trans.get("release_form_en", "").strip(),
                "LOCAL_PACK_CODE": (trans.get("ean") or product.get("ean") or "").strip(),
            }
            writer.writerow(output_row)
    return len(ved_products)


def format_discontinued_list_from_db(
    repo: RussiaRepository,
    excluded_run_id: str,
    output_path: Path,
    translated_by_item: Dict[str, Dict],
) -> int:
    """
    Format Excluded data into Discontinued List Template: one export row per Excluded source row.
    Uses ru_excluded_products (full row count) + translated fields from ru_translated_products by item_id.
    """
    excluded_products = repo.get_excluded_products_for_run(excluded_run_id) if excluded_run_id else []
    output_fieldnames = [
        "PCID",
        "Country",
        "Product Group",
        "Generic Name",
        "Start Date",
        "End Date",
        "Currency",
        "Ex-Factory Wholesale Price",
        "Local Pack Description",
        "LOCAL_PACK_CODE"
    ]
    if not excluded_products:
        print(f"[WARNING] No Excluded products found for run {excluded_run_id or 'N/A'}")
        with output_path.open("w", encoding="utf-8", newline="") as f_out:
            csv.DictWriter(f_out, fieldnames=output_fieldnames).writeheader()
        return 0

    with output_path.open("w", encoding="utf-8", newline="") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=output_fieldnames)
        writer.writeheader()
        for product in excluded_products:
            item_id = product.get("item_id", "")
            trans = translated_by_item.get(item_id, {})
            output_row = {
                "PCID": "",
                "Country": "Russia",
                "Product Group": trans.get("tn_en", "").strip(),
                "Generic Name": trans.get("inn_en", "").strip(),
                "Start Date": trans.get("start_date_text", product.get("start_date_text", "") or "").strip(),
                "End Date": "",
                "Currency": "RUB",
                "Ex-Factory Wholesale Price": (trans.get("registered_price_rub") or product.get("registered_price_rub") or "").strip(),
                "Local Pack Description": trans.get("release_form_en", "").strip(),
                "LOCAL_PACK_CODE": (trans.get("ean") or product.get("ean") or "").strip(),
            }
            writer.writerow(output_row)
    return len(excluded_products)


def main():
    print()
    print("=" * 80)
    print("RUSSIA OUTPUT FORMATTER (DB-Based)")
    print("=" * 80)
    print()
    
    # Resolve run_id (from env or .current_run_id written by pipeline)
    run_id = os.environ.get("RUSSIA_RUN_ID", "").strip()
    if not run_id:
        run_id_file = get_output_dir() / ".current_run_id"
        if run_id_file.exists():
            try:
                run_id = run_id_file.read_text(encoding="utf-8").strip()
            except Exception:
                pass
    if not run_id:
        print("[ERROR] No run_id. Set RUSSIA_RUN_ID or run pipeline from step 0.")
        return 1

    # Initialize database
    try:
        db = CountryDB("Russia")
        apply_russia_schema(db)
        repo = RussiaRepository(db, run_id)
    except Exception as e:
        print(f"[ERROR] Could not initialize database: {e}")
        return 1

    # Paths
    output_dir = get_output_dir()
    central_dir = get_central_output_dir()
    
    # Create directories
    output_dir.mkdir(parents=True, exist_ok=True)
    central_dir.mkdir(parents=True, exist_ok=True)
    
    # Resolve VED/Excluded run_id (same logic as Step 4: use current run or best run with data)
    ved_run_id: Optional[str] = repo.run_id if repo.get_ved_product_count() > 0 else repo.get_best_ved_run_id()
    excluded_run_id: Optional[str] = repo.run_id if repo.get_excluded_product_count() > 0 else repo.get_best_excluded_run_id()
    if ved_run_id and ved_run_id != repo.run_id:
        print(f"[INFO] Using run_id {ved_run_id} for VED (current run has no VED data)")
    if excluded_run_id and excluded_run_id != repo.run_id:
        print(f"[INFO] Using run_id {excluded_run_id} for Excluded (current run has no Excluded data)")

    # One export row per source row: join VED/Excluded with translated by item_id
    translated_by_item = _translated_lookup(repo)

    # Output files (exports only - no intermediate output/ files)
    central_pricing = central_dir / "Russia_Pricing_Data.csv"
    central_discontinued = central_dir / "Russia_Discontinued_List.csv"

    # Format VED data into Pricing Data Template (one row per VED source row)
    print("[1/2] Formatting VED data into Pricing Data Template...")
    ved_rows = format_pricing_data_from_db(repo, ved_run_id or "", central_pricing, translated_by_item)

    # Format Excluded data into Discontinued List Template (one row per Excluded source row)
    print("[2/2] Formatting Excluded data into Discontinued List Template...")
    excluded_rows = format_discontinued_list_from_db(repo, excluded_run_id or "", central_discontinued, translated_by_item)
    
    # Summary
    print()
    print("=" * 80)
    print("FORMATTING COMPLETE!")
    print("=" * 80)
    print(f"  Pricing Data: {ved_rows} rows")
    print(f"  Discontinued List: {excluded_rows} rows")
    print()
    print("Export files (exports/Russia/):")
    print(f"  - {central_pricing.name}")
    print(f"  - {central_discontinued.name}")
    
    # VALIDATION REPORT (export row count = VED source rows + Excluded source rows)
    print()
    print("=" * 80)
    print("STEP 5 VALIDATION REPORT (Final Export)")
    print("=" * 80)

    ved_source_count = repo.get_ved_product_count_for_run(ved_run_id or "")
    excluded_source_count = repo.get_excluded_product_count_for_run(excluded_run_id or "")
    total_exported = ved_rows + excluded_rows
    total_source = ved_source_count + excluded_source_count

    print(f"Source Records (from DB):")
    print(f"  VED (ru_ved_products):     {ved_source_count:,}")
    print(f"  Excluded (ru_excluded):     {excluded_source_count:,}")
    print(f"  Total source rows:          {total_source:,}")
    print()
    print(f"Exported Records (to CSV):")
    print(f"  Pricing Data (VED):         {ved_rows:,}")
    print(f"  Discontinued List (Excl):   {excluded_rows:,}")
    print(f"  TOTAL Exported:             {total_exported:,}")
    print()

    if total_exported == total_source:
        print(f"[VALIDATION PASSED] Export row count matches source: {total_exported:,} rows")
        print("[AUDIT TRAIL] One export row per source row (VED + Excluded); translations joined by item_id.")
    else:
        print(f"[VALIDATION WARNING] Count mismatch:")
        print(f"  Source total: {total_source:,}, Exported: {total_exported:,}")
        print(f"  Difference:   {abs(total_source - total_exported):,}")
    print("=" * 80)
    print()
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
