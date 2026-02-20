#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
North Macedonia - Generate PCID-Mapped Export CSVs

Reads:
  - nm_drug_register (scraped data)
  - pcid_mapping (PCID reference, imported via GUI)

Produces (in exports/NorthMacedonia/):
  - north_macedonia_pcid_mapped_{date}.csv    (products WITH valid PCID)
  - north_macedonia_pcid_missing_{date}.csv   (products WITHOUT PCID match)
  - north_macedonia_pcid_oos_{date}.csv       (products matched to OOS)
  - north_macedonia_pcid_no_data_{date}.csv   (reference PCIDs with no scraped match)

Matching logic:
  - Uses PcidMapper (core.utils.pcid_mapper)
  - Fallback strategies (configurable via PCID_MAPPING_NORTH_MACEDONIA env var):
    1. Local Pack Code
    2. Composite (Product + Generic + ATC + Strength + Fill)
"""

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List

SCRIPT_DIR = Path(__file__).resolve().parent
_repo_root = Path(__file__).resolve().parents[2]

if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

# Fix for module shadowing: Remove any conflicting 'db' module from sys.modules
if "db" in sys.modules:
    del sys.modules["db"]

from config_loader import load_env_file, get_output_dir
from core.db.connection import CountryDB
from core.utils.pcid_mapper import PcidMapper
from core.utils.pcid_export import categorize_products, write_standard_exports

try:
    from db.repositories import NorthMacedoniaRepository
except (ImportError, ModuleNotFoundError):
    from scripts.north_macedonia.db.repositories import NorthMacedoniaRepository

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


NO_DATA_COLUMNS = [
    "PCID", "Country", "Company", "Local Product Name", "Generic Name",
    "Local Pack Description", "Local Pack Code", "WHO ATC Code",
    "Strength Size", "Fill Size",
]

NO_DATA_FIELD_MAP = {
    "PCID": "pcid",
    "Country": "_country",
    "Company": "company",
    "Local Product Name": "product_name",
    "Generic Name": "generic_name",
    "Local Pack Description": "description",
    "Local Pack Code": "pack_code",
    "WHO ATC Code": "atc_code",
    "Strength Size": "strength",
    "Fill Size": "fill_size",
}


def load_pcid_reference(db: CountryDB) -> List[Dict]:
    """Load PCID reference mapping rows."""
    reference_list = []
    try:
        with db.cursor() as cur:
            cur.execute("""
                SELECT pcid, company, local_product_name, generic_name,
                       local_pack_description, local_pack_code, atc_code, strength, fill_size
                FROM pcid_mapping
                WHERE source_country = %s
            """, ("NorthMacedonia",))
            for row in cur.fetchall():
                # Clean None -> ""
                data = {
                    "pcid": row[0] or "",
                    "company": row[1] or "",
                    "product_name": row[2] or "",
                    "generic_name": row[3] or "",
                    "description": row[4] or "",
                    "pack_code": row[5] or "",
                    "atc_code": row[6] or "",
                    "strength": row[7] or "",
                    "fill_size": row[8] or "",
                }
                reference_list.append(data)
    except Exception as e:
        print(f"[WARN] Could not load PCID reference: {e}")

    return reference_list


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
                copayment_value, copayment_percent, margin_rule, vat_percent,
                detail_url
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
                "detail_url": row[21] or "",  # keep for DB FK linkage
            })

    return products


def match_pcids(products: List[Dict], reference_list: List[Dict]):
    """Match products against PCID reference using PcidMapper.

    Returns:
        PcidExportResult with mapped, missing, oos, no_data lists.
    """
    env_mapping = os.environ.get("PCID_MAPPING_NORTH_MACEDONIA", "")

    if env_mapping:
        mapper = PcidMapper.from_env_string(env_mapping)
        print(f"[INFO] Using PCID mapping from env: {env_mapping}")
    else:
        print("[INFO] Using default PCID mapping strategies (PCID_MAPPING_NORTH_MACEDONIA not set)")
        strategies = [
            {"Local Pack Code": "pack_code"},
            {
                "Local Product Name": "product_name",
                "Generic Name": "generic_name",
                "WHO ATC Code": "atc_code",
                "Strength Size": "strength",
                "Fill Size": "fill_size",
            },
        ]
        mapper = PcidMapper(strategies)

    mapper.build_reference_store(reference_list)

    return categorize_products(
        products,
        mapper,
        enrich_from_match={
            "WHO ATC Code": "atc_code",
            "Strength Size": "strength",
            "Fill Size": "fill_size",
            "Local Pack Code": "pack_code",
        },
    )


def main():
    run_id = _get_run_id()
    if not run_id:
        print("[ERROR] No run_id found. Run the pipeline first.")
        sys.exit(1)

    db = CountryDB("NorthMacedonia")
    repo = NorthMacedoniaRepository(db, run_id)

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
    result = match_pcids(products, reference)
    print(f"       Mapped:     {len(result.mapped)}")
    print(f"       Missing:    {len(result.missing)}")
    print(f"       OOS:        {len(result.oos)}")
    print(f"       No data:    {len(result.no_data)} (reference PCIDs with no scraped match)")

    # Persist PCID & final output to DB
    print("[3b/4] Saving to nm_pcid_mappings and nm_final_output...")
    with db.cursor() as cur:
        cur.execute("""
            SELECT id, detail_url FROM nm_drug_register WHERE run_id = %s
        """, (run_id,))
        url_to_dr_id = {row[1]: row[0] for row in cur.fetchall()}

    def _to_float(s):
        try:
            return float(str(s).replace(",", ".").strip()) if s else None
        except Exception:
            return None

    saved_pcid = 0
    saved_final = 0
    for product in result.mapped + result.missing + result.oos:
        detail_url = product.get("detail_url") or ""
        dr_id = url_to_dr_id.get(detail_url)
        pcid_val = product.get("PCID") or ""
        match_type = "exact" if pcid_val and pcid_val != "OOS" else "not_found"

        pcid_mapping_id = None
        try:
            pcid_mapping_id = repo.insert_pcid_mapping(
                drug_register_id=dr_id,
                pcid=pcid_val,
                match_type=match_type,
                match_score=1.0 if match_type == "exact" else 0.0,
                product_data={
                    "local_product_name": product.get("Local Product Name"),
                    "generic_name": product.get("Generic Name"),
                    "manufacturer": product.get("Company"),
                    "local_pack_code": product.get("Local Pack Code"),
                    "local_pack_description": product.get("Local Pack Description"),
                },
            )
            saved_pcid += 1
        except Exception as e:
            print(f"[DB WARN] Could not insert pcid_mapping for {detail_url[:60]}: {e}")

        try:
            repo.insert_final_output(
                drug_register_id=dr_id,
                pcid_mapping_id=pcid_mapping_id,
                data={
                    "pcid": pcid_val,
                    "country": "NORTH MACEDONIA",
                    "company": product.get("Company"),
                    "local_product_name": product.get("Local Product Name"),
                    "generic_name": product.get("Generic Name"),
                    "description": product.get("Local Pack Description"),
                    "strength": product.get("Strength Size"),
                    "dosage_form": product.get("Formulation"),
                    "pack_size": product.get("Fill Size"),
                    "public_price": _to_float(product.get("Public with VAT Price")),
                    "pharmacy_price": _to_float(product.get("Pharmacy Purchase Price")),
                    "currency": "MKD",
                    "effective_start_date": product.get("Effective Start Date"),
                    "effective_end_date": product.get("Effective End Date"),
                    "local_pack_code": product.get("Local Pack Code"),
                    "atc_code": product.get("WHO ATC Code"),
                    "reimbursable_status": product.get("Reimbursable Status"),
                    "reimbursable_rate": product.get("Reimbursable Rate"),
                    "copayment_percent": product.get("Copayment Percent"),
                    "margin_rule": product.get("Margin Rule"),
                    "vat_percent": product.get("VAT Percent"),
                    "marketing_authorisation_holder": product.get("Company"),
                    "source_url": detail_url,
                    "source_type": "drug_register",
                },
            )
            saved_final += 1
        except Exception as e:
            print(f"[DB WARN] Could not insert final_output for {detail_url[:60]}: {e}")

    print(f"       nm_pcid_mappings: {saved_pcid} rows saved")
    print(f"       nm_final_output:  {saved_final} rows saved")

    # Export 4 standard CSVs
    print("[4/4] Writing export CSVs...")

    # Add Country field to no_data references for display
    for ref in result.no_data:
        ref["_country"] = "NORTH MACEDONIA"

    files_written = write_standard_exports(
        result=result,
        exports_dir=EXPORTS_DIR,
        prefix="north_macedonia",
        date_stamp=DATE_STAMP,
        product_columns=EXPORT_COLUMNS,
        no_data_columns=NO_DATA_COLUMNS,
        no_data_field_map=NO_DATA_FIELD_MAP,
    )

    for report_type, (fpath, row_count) in files_written.items():
        print(f"       {fpath.name}: {row_count} rows")
        try:
            repo.log_export_report(report_type, row_count, str(fpath))
        except Exception as e:
            print(f"[DB WARN] Could not log export '{report_type}': {e}")

    try:
        repo.finish_run("completed", items_scraped=len(products))
    except Exception as e:
        print(f"[DB WARN] Could not update run ledger: {e}")

    # Summary
    total = len(products)
    match_pct = round(len(result.mapped) / total * 100, 1) if total else 0

    print()
    print("=" * 70)
    print("EXPORT SUMMARY")
    print("=" * 70)
    print(f"  Total products:   {total}")
    print(f"  PCID mapped:      {len(result.mapped)} ({match_pct}%)")
    print(f"  Missing:          {len(result.missing)}")
    print(f"  OOS:              {len(result.oos)}")
    print(f"  No data (ref):    {len(result.no_data)}")
    print(f"  Export directory:  {EXPORTS_DIR}")
    print("=" * 70)
    print()
    print("[DONE] Export complete!")


if __name__ == "__main__":
    main()
