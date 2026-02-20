#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
North Macedonia - Statistics & Data Validation

Reads from nm_* tables and produces:
  - Console summary of pipeline coverage
  - Rows in nm_statistics (per-metric DB rows)
  - Rows in nm_validation_results (per-warning DB rows)
  - JSON report saved to logs/
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
if "db" in sys.modules:
    del sys.modules["db"]

from config_loader import load_env_file, get_output_dir
from core.db.connection import CountryDB
try:
    from db.repositories import NorthMacedoniaRepository
except (ImportError, ModuleNotFoundError):
    from scripts.north_macedonia.db.repositories import NorthMacedoniaRepository

load_env_file()
OUTPUT_DIR = get_output_dir()
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
_RUN_ID_FILE = OUTPUT_DIR / ".current_run_id"

LOGS_DIR = OUTPUT_DIR / "logs"
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
    repo = NorthMacedoniaRepository(db, run_id)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 70)
    print("NORTH MACEDONIA - STATISTICS & DATA VALIDATION")
    print("=" * 70)
    print(f"Run ID:    {run_id}")
    print(f"Timestamp: {ts}")
    print()

    stats = {}
    validation_items = []  # list of (type, field, rule, status, message, severity)

    with db.cursor() as cur:
        # ── URL coverage ──
        urls_total   = _safe_count(cur, "SELECT COUNT(*) FROM nm_urls WHERE run_id = %s", (run_id,))
        urls_scraped = _safe_count(cur, "SELECT COUNT(*) FROM nm_urls WHERE run_id = %s AND status = 'scraped'", (run_id,))
        urls_failed  = _safe_count(cur, "SELECT COUNT(*) FROM nm_urls WHERE run_id = %s AND status = 'failed'", (run_id,))
        urls_pending = _safe_count(cur, "SELECT COUNT(*) FROM nm_urls WHERE run_id = %s AND status = 'pending'", (run_id,))

        stats["urls"] = {
            "total": urls_total,
            "scraped": urls_scraped,
            "failed": urls_failed,
            "pending": urls_pending,
            "coverage_pct": round(urls_scraped / urls_total * 100, 1) if urls_total else 0,
        }

        # ── Drug register ──
        dr_total             = _safe_count(cur, "SELECT COUNT(*) FROM nm_drug_register WHERE run_id = %s", (run_id,))
        dr_with_atc          = _safe_count(cur, "SELECT COUNT(*) FROM nm_drug_register WHERE run_id = %s AND who_atc_code IS NOT NULL AND who_atc_code != ''", (run_id,))
        dr_with_price        = _safe_count(cur, "SELECT COUNT(*) FROM nm_drug_register WHERE run_id = %s AND public_with_vat_price IS NOT NULL AND public_with_vat_price != ''", (run_id,))
        dr_with_generic      = _safe_count(cur, "SELECT COUNT(*) FROM nm_drug_register WHERE run_id = %s AND generic_name IS NOT NULL AND generic_name != ''", (run_id,))
        dr_with_ean          = _safe_count(cur, "SELECT COUNT(*) FROM nm_drug_register WHERE run_id = %s AND local_pack_code IS NOT NULL AND local_pack_code != ''", (run_id,))
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
            "generic_coverage_pct": round(dr_with_generic / dr_total * 100, 1) if dr_total else 0,
            "ean_coverage_pct": round(dr_with_ean / dr_total * 100, 1) if dr_total else 0,
        }

        # ── PCID mapping (if exists) ──
        pcid_total   = _safe_count(cur, "SELECT COUNT(*) FROM nm_pcid_mappings WHERE run_id = %s", (run_id,))
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

        # ── Final output ──
        final_count = _safe_count(cur, "SELECT COUNT(*) FROM nm_final_output WHERE run_id = %s", (run_id,))
        stats["final_output"] = {"total": final_count}

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

    # ── Build validation items ──
    if urls_total == 0:
        validation_items.append(("coverage", "nm_urls", "urls_collected", "fail", "No URLs collected — run Step 1 first.", "critical"))
    elif urls_scraped < urls_total * 0.9:
        pct = stats["urls"]["coverage_pct"]
        validation_items.append(("coverage", "nm_urls", "url_scrape_rate", "fail", f"Only {pct}% URLs scraped ({urls_scraped}/{urls_total})", "high"))
    else:
        validation_items.append(("coverage", "nm_urls", "url_scrape_rate", "pass", f"{stats['urls']['coverage_pct']}% URLs scraped", "info"))

    if urls_failed > 0:
        validation_items.append(("coverage", "nm_urls", "urls_no_error", "warning", f"{urls_failed} URLs failed to scrape", "medium"))

    if dr_total == 0:
        validation_items.append(("coverage", "nm_drug_register", "drug_register_populated", "fail", "No drug register data — run Step 2 first.", "critical"))
    elif dr_total < urls_scraped * 0.8:
        validation_items.append(("coverage", "nm_drug_register", "drug_register_vs_urls", "warning", f"Drug register rows ({dr_total}) much less than scraped URLs ({urls_scraped})", "high"))
    else:
        validation_items.append(("coverage", "nm_drug_register", "drug_register_populated", "pass", f"{dr_total} records in drug register", "info"))

    if dr_total > 0 and dr_with_price == 0:
        validation_items.append(("completeness", "nm_drug_register", "price_present", "fail", "No products have pricing data", "high"))
    elif dr_total > 0:
        validation_items.append(("completeness", "nm_drug_register", "price_present", "pass", f"{stats['drug_register']['price_coverage_pct']}% have price", "info"))

    if dr_total > 0 and dr_with_atc == 0:
        validation_items.append(("completeness", "nm_drug_register", "atc_present", "warning", "No products have ATC codes", "medium"))
    elif dr_total > 0:
        validation_items.append(("completeness", "nm_drug_register", "atc_present", "pass", f"{stats['drug_register']['atc_coverage_pct']}% have ATC code", "info"))

    if dr_total > 0 and dr_with_generic == 0:
        validation_items.append(("completeness", "nm_drug_register", "generic_name_present", "warning", "No products have generic name", "medium"))

    if pcid_ref_total == 0:
        validation_items.append(("pcid", "pcid_mapping", "pcid_reference_loaded", "warning", "No PCID reference data loaded — import via GUI Input tab.", "low"))
    elif pcid_total > 0 and pcid_matched < pcid_total * 0.5:
        match_pct = stats["pcid_mapping"]["match_pct"]
        validation_items.append(("pcid", "nm_pcid_mappings", "pcid_match_rate", "warning", f"Low PCID match rate ({match_pct}%)", "medium"))

    if error_count > 50:
        validation_items.append(("errors", "nm_errors", "error_volume", "warning", f"High error count: {error_count} errors logged", "medium"))

    stats["validation_items"] = [
        {"type": v[0], "table": v[1], "rule": v[2], "status": v[3], "message": v[4], "severity": v[5]}
        for v in validation_items
    ]
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
    print(f"  With generic:    {dr_with_generic}  ({stats['drug_register']['generic_coverage_pct']}%)")
    print(f"  With EAN:        {dr_with_ean}  ({stats['drug_register']['ean_coverage_pct']}%)")
    print(f"  Companies:       {dr_distinct_companies}")

    print()
    print("-" * 70)
    print("PCID MAPPING")
    print("-" * 70)
    print(f"  Reference PCIDs: {pcid_ref_total}")
    print(f"  Mapped (in DB):  {pcid_matched}")
    print(f"  Not mapped:      {pcid_total - pcid_matched}")
    if pcid_total:
        print(f"  Match rate:      {stats['pcid_mapping']['match_pct']}%")

    print()
    print("-" * 70)
    print("FINAL OUTPUT / EXPORTS / ERRORS")
    print("-" * 70)
    print(f"  Final output:    {final_count}")
    print(f"  Export reports:  {export_count}")
    print(f"  Errors:          {error_count}")

    warnings_shown = [v for v in validation_items if v[3] in ("fail", "warning")]
    if warnings_shown:
        print()
        print("-" * 70)
        print("VALIDATION WARNINGS")
        print("-" * 70)
        for v in warnings_shown:
            prefix = "CRITICAL" if v[5] == "critical" else ("WARNING" if v[3] == "warning" else "FAIL")
            print(f"  [{prefix}] {v[4]}")

    # ── Persist statistics to nm_statistics ──
    print()
    print("[DB] Saving statistics to nm_statistics...")
    try:
        stat_rows = [
            # Step 1 - URLs
            (1, "urls_total",   urls_total,   "count",      "url",  "Total URLs collected"),
            (1, "urls_scraped", urls_scraped, "count",      "url",  "URLs successfully scraped"),
            (1, "urls_failed",  urls_failed,  "count",      "url",  "URLs that failed"),
            (1, "urls_pending", urls_pending, "count",      "url",  "URLs still pending"),
            (1, "url_coverage", stats["urls"]["coverage_pct"], "percentage", "url", "Scrape coverage %"),
            # Step 2 - Drug register
            (2, "drug_register_total",    dr_total,              "count",      "drug", "Total drug register records"),
            (2, "drug_with_atc",          dr_with_atc,           "count",      "drug", "Records with ATC code"),
            (2, "drug_with_price",        dr_with_price,         "count",      "drug", "Records with public price"),
            (2, "drug_with_generic",      dr_with_generic,       "count",      "drug", "Records with generic name"),
            (2, "drug_with_ean",          dr_with_ean,           "count",      "drug", "Records with EAN code"),
            (2, "drug_distinct_companies",dr_distinct_companies, "count",      "drug", "Distinct marketing companies"),
            (2, "atc_coverage_pct",       stats["drug_register"]["atc_coverage_pct"], "percentage", "drug", "ATC code coverage %"),
            (2, "price_coverage_pct",     stats["drug_register"]["price_coverage_pct"], "percentage", "drug", "Price coverage %"),
            (2, "generic_coverage_pct",   stats["drug_register"]["generic_coverage_pct"], "percentage", "drug", "Generic name coverage %"),
            # Step 5 - PCID
            (5, "pcid_reference_total",   pcid_ref_total, "count",      "pcid", "PCID reference rows"),
            (5, "pcid_mapped",            pcid_matched,   "count",      "pcid", "Products with PCID"),
            (5, "pcid_not_mapped",        pcid_total - pcid_matched, "count", "pcid", "Products without PCID"),
            (5, "pcid_match_pct",         stats["pcid_mapping"]["match_pct"], "percentage", "pcid", "PCID match rate %"),
            # Final output
            (5, "final_output_total",     final_count,  "count", "output", "Rows in nm_final_output"),
            (5, "export_reports_total",   export_count, "count", "output", "Export report entries"),
            (5, "errors_total",           error_count,  "count", "errors", "Total errors logged"),
        ]
        for step, name, value, mtype, cat, desc in stat_rows:
            try:
                repo.insert_statistic(step, name, value, mtype, cat, desc)
            except Exception as e:
                print(f"[DB WARN] Could not insert statistic '{name}': {e}")
        print(f"[DB] Saved {len(stat_rows)} metrics to nm_statistics")
    except Exception as e:
        print(f"[DB WARN] Could not save statistics: {e}")

    # ── Persist validation results to nm_validation_results ──
    print("[DB] Saving validation results to nm_validation_results...")
    saved_validations = 0
    for v in validation_items:
        vtype, table, rule, status, message, severity = v
        try:
            repo.insert_validation_result(
                validation_type=vtype,
                table_name=table,
                record_id=None,
                field_name=rule,
                validation_rule=rule,
                status=status,
                message=message,
                severity=severity,
            )
            saved_validations += 1
        except Exception as e:
            print(f"[DB WARN] Could not save validation result '{rule}': {e}")
    print(f"[DB] Saved {saved_validations} validation results to nm_validation_results")

    # ── Save JSON report ──
    report_path = LOGS_DIR / f"north_macedonia_stats_{run_id}_{ts}.json"
    report_path.write_text(json.dumps(stats, indent=2, default=str), encoding="utf-8")
    print()
    print(f"[REPORT] Saved to: {report_path}")

    # ── Log stats run to nm_export_reports ──
    try:
        repo.log_export_report("statistics", dr_total, str(report_path))
    except Exception as e:
        print(f"[DB WARN] Could not log report: {e}")

    print()
    print("=" * 70)
    print("Statistics complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
