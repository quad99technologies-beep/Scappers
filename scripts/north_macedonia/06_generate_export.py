#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
North Macedonia - Generate PCID-Mapped Export CSVs

Reads:
  - nm_drug_register (scraped data)
  - pcid_mapping (PCID reference, imported via GUI)

Produces (in exports/NorthMacedonia/):
  - north_macedonia_pcid_mapped_{date}.csv    (products WITH PCID)
  - north_macedonia_pcid_not_mapped_{date}.csv (products WITHOUT PCID)
  - north_macedonia_pcid_no_data_{date}.csv   (reference PCIDs with no scraped match)

Matching logic:
  - Normalize: uppercase, strip non-alphanumeric
  - Composite key: company + product_name + generic_name + description
  - LEFT JOIN on composite key
"""

import csv
import os
import re
import sys
import tempfile
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
_repo_root = Path(__file__).resolve().parents[2]

if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

# Fix for module shadowing: Remove any conflicting 'db' module from sys.modules
# to ensure 'from db ...' resolves to the local db directory.
if "db" in sys.modules:
    del sys.modules["db"]

from config_loader import load_env_file, get_output_dir
from core.db.connection import CountryDB

load_env_file()
OUTPUT_DIR = get_output_dir()
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
_RUN_ID_FILE = OUTPUT_DIR / ".current_run_id"

EXPORTS_DIR = _repo_root / "exports" / "NorthMacedonia"
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

DATE_STAMP = datetime.now().strftime("%d%m%Y")

# Export columns (EVERSANA standard format)
EXPORT_COLUMNS = [
    "PCID",
    "Country",
    "Company",
    "Local Product Name",
    "Generic Name",
    "WHO ATC Code",
    "Formulation",
    "Strength Size",
    "Fill Size",
    "Customized 1",
    "Local Pack Code",
    "Local Pack Description",
    "Effective Start Date",
    "Effective End Date",
    "Public with VAT Price",
    "Pharmacy Purchase Price",
    "Reimbursable Status",
    "Reimbursable Rate",
    "Reimbursable Notes",
    "Copayment Value",
    "Copayment Percent",
    "Margin Rule",
    "VAT Percent",
]


def _get_run_id() -> str:
    rid = os.environ.get("NORTH_MACEDONIA_RUN_ID")
    if rid:
        return rid
    if _RUN_ID_FILE.exists():
        try:
            return _RUN_ID_FILE.read_text(encoding="utf-8").strip()
        except Exception:
            pass
    return ""


def _normalize_key(text: str) -> str:
    """Normalize text for composite key matching."""
    if not text:
        return ""
    text = unicodedata.normalize("NFC", text)
    text = re.sub(r"[^A-Za-z0-9]", "", text)
    return text.upper()


def _build_composite_key(company: str, product: str, generic: str, description: str) -> str:
    """Build composite key from product fields."""
    return _normalize_key(f"{company}{product}{generic}{description}")


def _safe_write_csv(path: Path, rows: List[Dict], columns: List[str]) -> int:
    """Atomic CSV write using tempfile + rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".csv", dir=str(path.parent))
    try:
        with os.fdopen(tmp_fd, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        os.replace(tmp_path, str(path))
        return len(rows)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def load_pcid_reference(db: CountryDB) -> Dict[str, Dict]:
    """Load PCID reference mapping from pcid_mapping table (filtered by source_country).

    Returns dict: composite_key -> {pcid, company, product_name, ...}
    """
    reference = {}
    try:
        with db.cursor() as cur:
            cur.execute("""
                SELECT pcid, company, local_product_name, generic_name,
                       local_pack_description, local_pack_code
                FROM pcid_mapping
                WHERE source_country = %s
            """, ("NorthMacedonia",))
            for row in cur.fetchall():
                pcid = row[0] or ""
                company = row[1] or ""
                product = row[2] or ""
                generic = row[3] or ""
                desc = row[4] or ""

                key = _build_composite_key(company, product, generic, desc)
                if key:
                    reference[key] = {
                        "pcid": pcid,
                        "company": company,
                        "product_name": product,
                        "generic_name": generic,
                        "description": desc,
                        "pack_code": row[5] or "",
                    }
    except Exception as e:
        print(f"[WARN] Could not load PCID reference: {e}")

    return reference


def load_drug_register(db: CountryDB, run_id: str) -> List[Dict]:
    """Load all drug register records for this run."""
    products = []
    with db.cursor() as cur:
        cur.execute("""
            SELECT
                local_product_name, local_pack_code, generic_name,
                who_atc_code, formulation, strength_size, fill_size,
                customized_1, marketing_authority_company_name,
                effective_start_date, effective_end_date,
                public_with_vat_price, pharmacy_purchase_price,
                local_pack_description,
                reimbursable_status, reimbursable_rate, reimbursable_notes,
                copayment_value, copayment_percent, margin_rule, vat_percent
            FROM nm_drug_register
            WHERE run_id = %s
            ORDER BY id
        """, (run_id,))

        for row in cur.fetchall():
            products.append({
                "Local Product Name": row[0] or "",
                "Local Pack Code": row[1] or "",
                "Generic Name": row[2] or "",
                "WHO ATC Code": row[3] or "",
                "Formulation": row[4] or "",
                "Strength Size": row[5] or "",
                "Fill Size": row[6] or "",
                "Customized 1": row[7] or "",
                "Company": row[8] or "",
                "Effective Start Date": row[9] or "",
                "Effective End Date": row[10] or "",
                "Public with VAT Price": row[11] or "",
                "Pharmacy Purchase Price": row[12] or "",
                "Local Pack Description": row[13] or "",
                "Reimbursable Status": row[14] or "",
                "Reimbursable Rate": row[15] or "",
                "Reimbursable Notes": row[16] or "",
                "Copayment Value": row[17] or "",
                "Copayment Percent": row[18] or "",
                "Margin Rule": row[19] or "",
                "VAT Percent": row[20] or "",
                "Country": "NORTH MACEDONIA",
            })

    return products


def match_pcids(products: List[Dict], reference: Dict[str, Dict]) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """Match products against PCID reference.

    Returns: (mapped, not_mapped, no_data)
    """
    mapped = []
    not_mapped = []
    used_keys = set()

    for product in products:
        key = _build_composite_key(
            product["Company"],
            product["Local Product Name"],
            product["Generic Name"],
            product["Local Pack Description"],
        )

        ref = reference.get(key)
        if ref and ref["pcid"] and ref["pcid"].upper() != "OOS":
            product["PCID"] = ref["pcid"]
            mapped.append(product)
            used_keys.add(key)
        else:
            product["PCID"] = ""
            not_mapped.append(product)
            if ref:
                used_keys.add(key)

    # no_data = PCID reference rows that were never matched
    no_data = []
    for key, ref in reference.items():
        if key not in used_keys:
            no_data.append({
                "PCID": ref["pcid"],
                "Country": "NORTH MACEDONIA",
                "Company": ref["company"],
                "Local Product Name": ref["product_name"],
                "Generic Name": ref["generic_name"],
                "Local Pack Description": ref["description"],
                "Local Pack Code": ref["pack_code"],
            })

    return mapped, not_mapped, no_data


def main():
    run_id = _get_run_id()
    if not run_id:
        print("[ERROR] No run_id found. Run the pipeline first.")
        sys.exit(1)

    db = CountryDB("NorthMacedonia")

    print("=" * 70)
    print("NORTH MACEDONIA - GENERATE PCID-MAPPED EXPORT")
    print("=" * 70)
    print(f"Run ID:     {run_id}")
    print(f"Export dir: {EXPORTS_DIR}")
    print()

    # Load data
    print("[1/4] Loading drug register from database...")
    products = load_drug_register(db, run_id)
    print(f"       Loaded {len(products)} products")

    if not products:
        print("[ERROR] No drug register data found. Run Step 2 first.")
        sys.exit(1)

    print("[2/4] Loading PCID reference...")
    reference = load_pcid_reference(db)
    print(f"       Loaded {len(reference)} PCID reference entries")

    if not reference:
        print("[WARN] No PCID reference data. All products will be 'not mapped'.")
        print("[WARN] Import PCID mapping via GUI: Input tab -> PCID Mapping -> Import CSV")

    # Match
    print("[3/4] Matching products to PCIDs...")
    mapped, not_mapped, no_data = match_pcids(products, reference)
    print(f"       Mapped:     {len(mapped)}")
    print(f"       Not mapped: {len(not_mapped)}")
    print(f"       No data:    {len(no_data)} (reference PCIDs with no scraped match)")

    # Export
    print("[4/4] Writing export CSVs...")

    files_written = {}

    # Mapped
    mapped_path = EXPORTS_DIR / f"north_macedonia_pcid_mapped_{DATE_STAMP}.csv"
    count = _safe_write_csv(mapped_path, mapped, EXPORT_COLUMNS)
    files_written["pcid_mapped"] = (mapped_path, count)
    print(f"       {mapped_path.name}: {count} rows")

    # Not mapped
    not_mapped_path = EXPORTS_DIR / f"north_macedonia_pcid_not_mapped_{DATE_STAMP}.csv"
    count = _safe_write_csv(not_mapped_path, not_mapped, EXPORT_COLUMNS)
    files_written["pcid_not_mapped"] = (not_mapped_path, count)
    print(f"       {not_mapped_path.name}: {count} rows")

    # No data
    no_data_cols = ["PCID", "Country", "Company", "Local Product Name", "Generic Name", "Local Pack Description", "Local Pack Code"]
    no_data_path = EXPORTS_DIR / f"north_macedonia_pcid_no_data_{DATE_STAMP}.csv"
    count = _safe_write_csv(no_data_path, no_data, no_data_cols)
    files_written["pcid_no_data"] = (no_data_path, count)
    print(f"       {no_data_path.name}: {count} rows")

    # Log exports to DB
    try:
        from db.repositories import NorthMacedoniaRepository
        repo = NorthMacedoniaRepository(db, run_id)
        for report_type, (fpath, row_count) in files_written.items():
            repo.log_export_report(report_type, row_count, str(fpath))
        repo.finish_run("completed", items_scraped=len(products))
    except Exception as e:
        print(f"[DB WARN] Could not log exports: {e}")

    # Summary
    total = len(products)
    match_pct = round(len(mapped) / total * 100, 1) if total else 0

    print()
    print("=" * 70)
    print("EXPORT SUMMARY")
    print("=" * 70)
    print(f"  Total products:   {total}")
    print(f"  PCID mapped:      {len(mapped)} ({match_pct}%)")
    print(f"  Not mapped:       {len(not_mapped)}")
    print(f"  No data (ref):    {len(no_data)}")
    print(f"  Export directory:  {EXPORTS_DIR}")
    print("=" * 70)
    print()
    print("[DONE] Export complete!")


if __name__ == "__main__":
    main()
