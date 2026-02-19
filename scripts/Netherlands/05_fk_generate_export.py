#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Netherlands FK - Export Generation (Step 5)

Reads translated reimbursement data from nl_fk_reimbursement,
applies PCID mapping, and generates final CSV export.
"""

from __future__ import annotations

import csv
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

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

for _m in list(sys.modules.keys()):
    if _m == "db" or _m.startswith("db."):
        del sys.modules[_m]

from config_loader import get_output_dir, get_central_output_dir
from db.schema import apply_netherlands_schema
from db.repositories import NetherlandsRepository

log = get_logger(__name__, "Netherlands")

SCRIPT_ID = "Netherlands"
STEP_NUMBER = 5
STEP_NAME = "FK Export Generation"

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


def _load_pcid_mapping():
    """Load PCID mapping if available."""
    try:
        from core.data.pcid_mapping_contract import get_pcid_mapping
        return get_pcid_mapping("Netherlands")
    except Exception as e:
        log.warning(f"PCID mapping not available: {e}")
        return None


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

    # 2. Load PCID mapping
    pcid = _load_pcid_mapping()

    # 3. Build CSV rows
    csv_rows: List[Dict[str, str]] = []
    for row in rows:
        pcid_value = ""
        if pcid:
            try:
                pcid_value = pcid.lookup(
                    company=row.get("manufacturer") or "",
                    product=row.get("brand_name") or "",
                    generic=row.get("generic_name") or "",
                    pack_desc=row.get("pack_details") or "",
                ) or ""
            except Exception:
                pcid_value = ""

        # Use translated indication if available, fall back to Dutch
        indication = row.get("indication_en") or row.get("indication_nl") or ""

        csv_rows.append({
            "PCID": pcid_value,
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

    # 4. Write CSV
    export_dir = get_central_output_dir()
    export_dir.mkdir(parents=True, exist_ok=True)
    output_path = export_dir / "fk_reimbursement_export.csv"

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        for r in csv_rows:
            writer.writerow({k: r.get(k, "") for k in OUTPUT_COLUMNS})

    # 5. Log export
    repo.log_export_report("fk_reimbursement", len(csv_rows), "csv")
    log.info(f"Export complete: {len(csv_rows)} rows -> {output_path}")

    # Stats summary
    reimb_counts = {}
    for r in csv_rows:
        status = r.get("REIMBURSEMENT STATUS", "UNKNOWN")
        reimb_counts[status] = reimb_counts.get(status, 0) + 1
    log.info(f"Reimbursement status breakdown: {reimb_counts}")

    pop_counts = {}
    for r in csv_rows:
        pop = r.get("PATIENT POPULATION", "NONE") or "NONE"
        pop_counts[pop] = pop_counts.get(pop, 0) + 1
    log.info(f"Population breakdown: {pop_counts}")


if __name__ == "__main__":
    run_with_checkpoint(main, SCRIPT_ID, STEP_NUMBER, STEP_NAME)
