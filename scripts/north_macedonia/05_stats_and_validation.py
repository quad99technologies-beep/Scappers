#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
North Macedonia - Statistics & Data Validation

Reads from nm_* tables and produces:
  - Console summary of pipeline coverage
  - JSON report saved to logs/
  - Validation warnings for data quality issues
"""

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

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

LOGS_DIR = _repo_root / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)


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


def _safe_count(cur, sql, params) -> int:
    try:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else 0
    except Exception:
        return 0


def main():
    run_id = _get_run_id()
    if not run_id:
        print("[ERROR] No run_id found. Run the pipeline first.")
        sys.exit(1)

    db = CountryDB("NorthMacedonia")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 70)
    print("NORTH MACEDONIA - STATISTICS & DATA VALIDATION")
    print("=" * 70)
    print(f"Run ID:    {run_id}")
    print(f"Timestamp: {ts}")
    print()

    stats = {}
    warnings = []

    with db.cursor() as cur:
        # ── URL coverage ──
        urls_total = _safe_count(cur, "SELECT COUNT(*) FROM nm_urls WHERE run_id = %s", (run_id,))
        urls_scraped = _safe_count(cur, "SELECT COUNT(*) FROM nm_urls WHERE run_id = %s AND status = 'scraped'", (run_id,))
        urls_failed = _safe_count(cur, "SELECT COUNT(*) FROM nm_urls WHERE run_id = %s AND status = 'failed'", (run_id,))
        urls_pending = _safe_count(cur, "SELECT COUNT(*) FROM nm_urls WHERE run_id = %s AND status = 'pending'", (run_id,))

        stats["urls"] = {
            "total": urls_total,
            "scraped": urls_scraped,
            "failed": urls_failed,
            "pending": urls_pending,
            "coverage_pct": round(urls_scraped / urls_total * 100, 1) if urls_total else 0,
        }

        # ── Drug register ──
        dr_total = _safe_count(cur, "SELECT COUNT(*) FROM nm_drug_register WHERE run_id = %s", (run_id,))
        dr_with_atc = _safe_count(cur, "SELECT COUNT(*) FROM nm_drug_register WHERE run_id = %s AND who_atc_code IS NOT NULL AND who_atc_code != ''", (run_id,))
        dr_with_price = _safe_count(cur, "SELECT COUNT(*) FROM nm_drug_register WHERE run_id = %s AND public_with_vat_price IS NOT NULL AND public_with_vat_price != ''", (run_id,))
        dr_with_generic = _safe_count(cur, "SELECT COUNT(*) FROM nm_drug_register WHERE run_id = %s AND generic_name IS NOT NULL AND generic_name != ''", (run_id,))
        dr_with_ean = _safe_count(cur, "SELECT COUNT(*) FROM nm_drug_register WHERE run_id = %s AND local_pack_code IS NOT NULL AND local_pack_code != ''", (run_id,))
        dr_distinct_companies = _safe_count(cur, "SELECT COUNT(DISTINCT marketing_authority_company_name) FROM nm_drug_register WHERE run_id = %s", (run_id,))

        stats["drug_register"] = {
            "total": dr_total,
            "with_atc_code": dr_with_atc,
            "with_price": dr_with_price,
            "with_generic": dr_with_generic,
            "with_ean": dr_with_ean,
            "distinct_companies": dr_distinct_companies,
            "atc_coverage_pct": round(dr_with_atc / dr_total * 100, 1) if dr_total else 0,
            "price_coverage_pct": round(dr_with_price / dr_total * 100, 1) if dr_total else 0,
        }

        # ── PCID mapping (if exists) ──
        pcid_total = _safe_count(cur, "SELECT COUNT(*) FROM nm_pcid_mappings WHERE run_id = %s", (run_id,))
        pcid_matched = _safe_count(cur, "SELECT COUNT(*) FROM nm_pcid_mappings WHERE run_id = %s AND pcid IS NOT NULL AND pcid != ''", (run_id,))

        stats["pcid_mapping"] = {
            "total": pcid_total,
            "matched": pcid_matched,
            "not_matched": pcid_total - pcid_matched,
            "match_pct": round(pcid_matched / pcid_total * 100, 1) if pcid_total else 0,
        }

        # ── PCID reference ──
        pcid_ref_total = 0
        try:
            cur.execute("SELECT COUNT(*) FROM pcid_mapping WHERE source_country = %s", ("NorthMacedonia",))
            row = cur.fetchone()
            pcid_ref_total = row[0] if row else 0
        except Exception:
            pass

        stats["pcid_reference"] = {"total": pcid_ref_total}

        # ── Export reports ──
        export_count = _safe_count(cur, "SELECT COUNT(*) FROM nm_export_reports WHERE run_id = %s", (run_id,))
        stats["exports"] = {"total": export_count}

        # ── Errors ──
        error_count = _safe_count(cur, "SELECT COUNT(*) FROM nm_errors WHERE run_id = %s", (run_id,))
        stats["errors"] = {"total": error_count}

        # ── Step progress ──
        try:
            cur.execute("""
                SELECT step_number, step_name, status, error_message,
                       started_at, completed_at
                FROM nm_step_progress
                WHERE run_id = %s
                ORDER BY step_number
            """, (run_id,))
            step_rows = cur.fetchall()
            steps = []
            for r in step_rows:
                steps.append({
                    "step": r[0], "name": r[1], "status": r[2],
                    "error": r[3],
                    "started": str(r[4]) if r[4] else None,
                    "completed": str(r[5]) if r[5] else None,
                })
            stats["steps"] = steps
        except Exception:
            stats["steps"] = []

    # ── Validation warnings ──
    if urls_total == 0:
        warnings.append("CRITICAL: No URLs collected. Run Step 1 first.")
    elif urls_scraped < urls_total * 0.9:
        warnings.append(f"WARNING: Only {stats['urls']['coverage_pct']}% URLs scraped ({urls_scraped}/{urls_total})")

    if urls_failed > 0:
        warnings.append(f"WARNING: {urls_failed} URLs failed to scrape")

    if dr_total == 0:
        warnings.append("CRITICAL: No drug register data. Run Step 2 first.")
    elif dr_total < urls_scraped * 0.8:
        warnings.append(f"WARNING: Drug register rows ({dr_total}) much less than scraped URLs ({urls_scraped})")

    if dr_total > 0 and dr_with_price == 0:
        warnings.append("WARNING: No products have pricing data")

    if dr_total > 0 and dr_with_atc == 0:
        warnings.append("WARNING: No products have ATC codes")

    if pcid_ref_total == 0:
        warnings.append("INFO: No PCID reference data loaded. Import via GUI Input tab for PCID mapping.")

    if pcid_total > 0 and pcid_matched < pcid_total * 0.5:
        warnings.append(f"WARNING: Low PCID match rate ({stats['pcid_mapping']['match_pct']}%)")

    stats["warnings"] = warnings
    stats["run_id"] = run_id
    stats["generated_at"] = ts

    # ── Print summary ──
    print("-" * 70)
    print("URL COLLECTION")
    print("-" * 70)
    print(f"  Total URLs:      {urls_total}")
    print(f"  Scraped:         {urls_scraped}  ({stats['urls']['coverage_pct']}%)")
    print(f"  Failed:          {urls_failed}")
    print(f"  Pending:         {urls_pending}")

    print()
    print("-" * 70)
    print("DRUG REGISTER")
    print("-" * 70)
    print(f"  Total records:   {dr_total}")
    print(f"  With ATC code:   {dr_with_atc}  ({stats['drug_register']['atc_coverage_pct']}%)")
    print(f"  With price:      {dr_with_price}  ({stats['drug_register']['price_coverage_pct']}%)")
    print(f"  With generic:    {dr_with_generic}")
    print(f"  With EAN:        {dr_with_ean}")
    print(f"  Companies:       {dr_distinct_companies}")

    print()
    print("-" * 70)
    print("PCID MAPPING")
    print("-" * 70)
    print(f"  Reference PCIDs: {pcid_ref_total}")
    print(f"  Mapped:          {pcid_matched}")
    print(f"  Not mapped:      {pcid_total - pcid_matched}")
    if pcid_total:
        print(f"  Match rate:      {stats['pcid_mapping']['match_pct']}%")

    print()
    print("-" * 70)
    print("EXPORTS & ERRORS")
    print("-" * 70)
    print(f"  Exports:         {export_count}")
    print(f"  Errors:          {error_count}")

    if warnings:
        print()
        print("-" * 70)
        print("VALIDATION WARNINGS")
        print("-" * 70)
        for w in warnings:
            print(f"  {w}")

    # ── Save JSON report ──
    report_path = LOGS_DIR / f"north_macedonia_stats_{run_id}_{ts}.json"
    report_path.write_text(json.dumps(stats, indent=2, default=str), encoding="utf-8")
    print()
    print(f"[REPORT] Saved to: {report_path}")

    # ── Log to DB ──
    try:
        from db.repositories import NorthMacedoniaRepository
        repo = NorthMacedoniaRepository(db, run_id)
        repo.log_export_report("statistics", dr_total, str(report_path))
    except Exception as e:
        print(f"[DB WARN] Could not log report: {e}")

    print()
    print("=" * 70)
    print("Statistics complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
