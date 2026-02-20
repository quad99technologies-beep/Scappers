#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Netherlands FK - Export Generation (Step 5)

Reads translated reimbursement data from nl_fk_reimbursement,
applies PCID mapping, and generates 4 standard export CSVs:
  - netherlands_pcid_mapped_{date}.csv
  - netherlands_pcid_missing_{date}.csv
  - netherlands_pcid_oos_{date}.csv
  - netherlands_pcid_no_data_{date}.csv
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List

# Path wiring
SCRIPT_DIR = Path(__file__).resolve().parent
_repo_root = SCRIPT_DIR.parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from core.utils.logger import get_logger
from core.db.postgres_connection import get_db
from core.pipeline.standalone_checkpoint import run_with_checkpoint
from core.utils.pcid_mapper import PcidMapper
from core.utils.pcid_export import categorize_products, write_standard_exports

for _m in list(sys.modules.keys()):
    if _m == "db" or _m.startswith("db."):
        del sys.modules[_m]

from config_loader import get_output_dir, get_central_output_dir
from scripts.Netherlands.db import apply_netherlands_schema, NetherlandsRepository

log = get_logger(__name__, "Netherlands")

SCRIPT_ID = "Netherlands"
STEP_NUMBER = 5
STEP_NAME = "FK Export Generation"
DATE_STAMP = datetime.now().strftime("%d%m%Y")

OUTPUT_COLUMNS = [
    "PCID",
    "COUNTRY",
    "COMPANY",
    "BRAND NAME",
    "GENERIC NAME",
    "PATIENT POPULATION",
    "INDICATION",
    "REIMBURSABLE STATUS",
    "Pack details",
    "ROUTE OF ADMINISTRATION",
    "STRENGTH SIZE",
    "BINDING",
    "REIMBURSEMENT BODY",
    "REIMBURSEMENT DATE",
    "REIMBURSEMENT STATUS",
    "REIMBURSEMENT URL",
]

NO_DATA_COLUMNS = [
    "PCID", "COUNTRY", "COMPANY", "BRAND NAME", "GENERIC NAME",
    "Pack details",
]

NO_DATA_FIELD_MAP = {
    "PCID": "pcid",
    "COUNTRY": "_country",
    "COMPANY": "company",
    "BRAND NAME": "local_product_name",
    "GENERIC NAME": "generic_name",
    "Pack details": "local_pack_description",
}


def _get_run_id() -> str:
    run_id = os.environ.get("NL_RUN_ID", "").strip()
    if run_id:
        return run_id
    run_id_file = get_output_dir() / ".current_run_id"
    if run_id_file.exists():
        try:
            return run_id_file.read_text(encoding="utf-8").strip()
        except Exception:
            pass
    return ""


def _load_pcid_reference(db) -> List[Dict]:
    """Load PCID reference data as list of dicts for PcidMapper."""
    try:
        from core.data.pcid_mapping import PCIDMapping
        pcid_mapping = PCIDMapping("Netherlands", db)
        pcid_rows = pcid_mapping.get_all()
        return [
            {
                "pcid": r.pcid,
                "company": r.company,
                "local_product_name": r.local_product_name,
                "generic_name": r.generic_name,
                "local_pack_description": r.local_pack_description,
                "local_pack_code": r.local_pack_code or "",
            }
            for r in pcid_rows
        ]
    except Exception as e:
        log.warning(f"PCID mapping not available: {e}")
        return []


def main() -> None:
    run_id = _get_run_id()
    if not run_id:
        log.error("No run_id found.")
        raise SystemExit(1)

    log.info(f"Step {STEP_NUMBER}: {STEP_NAME} | run_id={run_id}")

    db = get_db("Netherlands")
    apply_netherlands_schema(db)
    repo = NetherlandsRepository(db, run_id)

    # 1. Load all reimbursement rows
    rows = repo.get_all_fk_reimbursement_for_export()
    if not rows:
        log.warning("No reimbursement data to export")
        return

    log.info(f"Exporting {len(rows)} reimbursement rows")

    # 2. Load PCID reference and build mapper
    reference_data = _load_pcid_reference(db)
    log.info(f"Loaded {len(reference_data)} PCID reference entries")

    env_mapping = os.environ.get("PCID_MAPPING_NETHERLANDS", "")
    if env_mapping:
        mapper = PcidMapper.from_env_string(env_mapping)
        log.info(f"Using PCID mapping from env: {env_mapping}")
    else:
        log.info("Using default PCID mapping strategies (PCID_MAPPING_NETHERLANDS not set)")
        mapper = PcidMapper([{
            "COMPANY": "company",
            "BRAND NAME": "local_product_name",
            "GENERIC NAME": "generic_name",
            "Pack details": "local_pack_description",
        }])

    mapper.build_reference_store(reference_data)

    # 3. Build product dicts with keys matching strategy columns
    products: List[Dict] = []
    for row in rows:
        indication = row.get("indication_en") or row.get("indication_nl") or ""
        products.append({
            "COUNTRY": "NETHERLANDS",
            "COMPANY": (row.get("manufacturer") or "").upper(),
            "BRAND NAME": (row.get("brand_name") or "").upper(),
            "GENERIC NAME": (row.get("generic_name") or "").upper(),
            "PATIENT POPULATION": row.get("patient_population") or "",
            "INDICATION": indication,
            "REIMBURSABLE STATUS": row.get("reimbursable_text") or "",
            "Pack details": row.get("pack_details") or "",
            "ROUTE OF ADMINISTRATION": row.get("route_of_administration") or "",
            "STRENGTH SIZE": row.get("strength") or "",
            "BINDING": row.get("binding") or "NO",
            "REIMBURSEMENT BODY": row.get("reimbursement_body") or "MINISTRY OF HEALTH",
            "REIMBURSEMENT DATE": row.get("reimbursement_date") or "",
            "REIMBURSEMENT STATUS": row.get("reimbursement_status") or "",
            "REIMBURSEMENT URL": row.get("source_url") or "",
        })

    # 4. Categorize products
    result = categorize_products(products, mapper)

    log.info(
        f"PCID results: mapped={len(result.mapped)}, missing={len(result.missing)}, "
        f"oos={len(result.oos)}, no_data={len(result.no_data)}"
    )

    # 5. Write 4 standard CSVs
    export_dir = get_central_output_dir()

    # Add country to no_data references
    for ref in result.no_data:
        ref["_country"] = "NETHERLANDS"

    files_written = write_standard_exports(
        result=result,
        exports_dir=export_dir,
        prefix="netherlands",
        date_stamp=DATE_STAMP,
        product_columns=OUTPUT_COLUMNS,
        no_data_columns=NO_DATA_COLUMNS,
        no_data_field_map=NO_DATA_FIELD_MAP,
    )

    # 6. Log exports
    for report_type, (fpath, row_count) in files_written.items():
        log.info(f"  {fpath.name}: {row_count} rows")
        try:
            repo.log_export_report(report_type, row_count, str(fpath))
        except Exception:
            pass

    log.info(f"Export complete: {len(products)} total rows")

    # Stats summary
    reimb_counts: Dict[str, int] = {}
    for r in result.mapped + result.missing + result.oos:
        status = r.get("REIMBURSEMENT STATUS", "UNKNOWN")
        reimb_counts[status] = reimb_counts.get(status, 0) + 1
    log.info(f"Reimbursement status breakdown: {reimb_counts}")

    pop_counts: Dict[str, int] = {}
    for r in result.mapped + result.missing + result.oos:
        pop = r.get("PATIENT POPULATION", "NONE") or "NONE"
        pop_counts[pop] = pop_counts.get(pop, 0) + 1
    log.info(f"Population breakdown: {pop_counts}")


if __name__ == "__main__":
    run_with_checkpoint(main, SCRIPT_ID, STEP_NUMBER, STEP_NAME)
