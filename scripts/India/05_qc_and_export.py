#!/usr/bin/env python3
"""
India NPPA: Quality Gate + CSV Export from PostgreSQL

Reads from PostgreSQL (populated by Scrapy spider),
runs QC checks, and exports the final CSV deliverable.

This replaces the CSV merge/consolidation step.
"""

import csv
import json
import logging
import sys
import argparse
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from config_loader import load_env_file, get_output_dir

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("india_qc_export")


def run_qc_checks(db, run_id: str, previous_count: int = 0, max_drop_pct: float = 30.0) -> dict:
    """Run quality checks on the data."""
    results = {
        "passed": True,
        "checks": [],
        "run_id": run_id,
    }

    # Check 1: Row count
    cur = db.execute("SELECT COUNT(*) FROM in_sku_main WHERE run_id = %s", (run_id,))
    row_count = cur.fetchone()[0]
    results["row_count"] = row_count

    if row_count == 0:
        results["passed"] = False
        results["checks"].append({
            "name": "row_count",
            "passed": False,
            "message": "No rows found for this run",
        })
    else:
        results["checks"].append({
            "name": "row_count",
            "passed": True,
            "message": f"Found {row_count} rows",
        })

    # Check 2: Required columns have data
    required_cols = ["hidden_id", "sku_name", "formulation"]
    for col in required_cols:
        cur = db.execute(
            f"SELECT COUNT(*) FROM in_sku_main WHERE run_id = %s AND ({col} IS NULL OR {col} = '')",
            (run_id,)
        )
        null_count = cur.fetchone()[0]
        null_pct = (null_count / row_count * 100) if row_count > 0 else 0

        if null_pct > 50:
            results["passed"] = False
            results["checks"].append({
                "name": f"null_check_{col}",
                "passed": False,
                "message": f"Column {col} has {null_pct:.1f}% null/empty values",
            })
        else:
            results["checks"].append({
                "name": f"null_check_{col}",
                "passed": True,
                "message": f"Column {col} has {null_pct:.1f}% null/empty values (OK)",
            })

    # Check 3: Row count drop (if previous count available)
    if previous_count > 0:
        drop_pct = ((previous_count - row_count) / previous_count) * 100 if previous_count > 0 else 0
        if drop_pct > max_drop_pct:
            results["passed"] = False
            results["checks"].append({
                "name": "row_count_drop",
                "passed": False,
                "message": f"Row count dropped by {drop_pct:.1f}% (from {previous_count} to {row_count})",
            })
        else:
            results["checks"].append({
                "name": "row_count_drop",
                "passed": True,
                "message": f"Row count change: {previous_count} -> {row_count} ({drop_pct:.1f}% drop, OK)",
            })

    # Check 4: Duplicate check
    cur = db.execute(
        "SELECT COUNT(*) - COUNT(DISTINCT hidden_id) FROM in_sku_main WHERE run_id = %s",
        (run_id,)
    )
    dup_count = cur.fetchone()[0]
    if dup_count > 0:
        results["checks"].append({
            "name": "duplicates",
            "passed": True,  # Warning only
            "message": f"Found {dup_count} duplicate hidden_id values (warning)",
        })
    else:
        results["checks"].append({
            "name": "duplicates",
            "passed": True,
            "message": "No duplicate hidden_id values found",
        })

    return results


def export_table_csv(db, table: str, run_id: str, output_path: Path):
    """Export a table to CSV file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Get column names
    cur = db.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = %s ORDER BY ordinal_position",
        (table,)
    )
    columns = [row[0] for row in cur.fetchall()]

    # Export data
    if 'run_id' in columns:
        cur = db.execute(f"SELECT * FROM \"{table}\" WHERE run_id = %s", (run_id,))
    else:
        cur = db.execute(f"SELECT * FROM \"{table}\"")

    rows = cur.fetchall()

    with output_path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

    return len(rows)


def export_combined_csv(db, run_id: str, output_path: Path) -> int:
    """Export combined details CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Build a combined query joining sku_main with other details
    query = """
        SELECT
            s.run_id,
            s.formulation,
            s.hidden_id,
            s.sku_name,
            s.company,
            s.composition,
            s.pack_size,
            s.dosage_form,
            s.schedule_status,
            s.ceiling_price,
            s.mrp,
            s.mrp_per_unit,
            s.year_month,
            s.created_at
        FROM in_sku_main s
        WHERE s.run_id = %s
        ORDER BY s.formulation, s.sku_name
    """

    cur = db.execute(query, (run_id,))
    rows = cur.fetchall()

    columns = [
        "run_id", "formulation", "hidden_id", "sku_name", "company",
        "composition", "pack_size", "dosage_form", "schedule_status",
        "ceiling_price", "mrp", "mrp_per_unit", "year_month", "created_at"
    ]

    with output_path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

    return len(rows)


def main():
    load_env_file()
    output_dir = get_output_dir()

    parser = argparse.ArgumentParser(description="India NPPA QC + Export")
    parser.add_argument("--run-id", type=str, help="Run ID to export (defaults to last_run_id.json or latest)")
    args = parser.parse_args()

    run_id = args.run_id
    if not run_id:
        try:
            last_path = output_dir / "last_run_id.json"
            if last_path.exists():
                run_id = json.loads(last_path.read_text(encoding="utf-8")).get("run_id")
        except Exception:
            run_id = None

    # Connect to PostgreSQL
    from core.db.postgres_connection import PostgresDB
    db = PostgresDB("India")
    db.connect()

    # Get the latest run_id if not provided
    if not run_id:
        cur = db.execute(
            "SELECT run_id FROM run_ledger WHERE scraper_name = 'India' ORDER BY started_at DESC LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            logger.error("No runs found in database")
            db.close()
            sys.exit(1)
        run_id = row[0]

    logger.info("Exporting run: %s", run_id)

    # --- QC Gate ---
    # Get previous row count for delta check
    previous_count = 0
    try:
        cur = db.execute(
            "SELECT items_scraped FROM run_ledger "
            "WHERE scraper_name = 'India' AND status = 'completed' AND run_id != %s "
            "ORDER BY started_at DESC LIMIT 1",
            (run_id,)
        )
        row = cur.fetchone()
        if row:
            previous_count = row[0] or 0
    except Exception:
        pass

    qc_results = run_qc_checks(db, run_id, previous_count=previous_count, max_drop_pct=30.0)

    # Save QC report
    qc_path = output_dir / "qc_report.json"
    qc_path.write_text(json.dumps(qc_results, indent=2), encoding="utf-8")
    logger.info("QC report saved: %s", qc_path)

    if not qc_results["passed"]:
        logger.error("Quality gate FAILED — export blocked. Check %s", qc_path)
        for check in qc_results["checks"]:
            if not check["passed"]:
                logger.error("  FAILED: %s - %s", check["name"], check["message"])
        db.close()
        sys.exit(2)

    for check in qc_results["checks"]:
        status = "PASS" if check["passed"] else "FAIL"
        logger.info("  QC [%s] %s: %s", status, check["name"], check["message"])

    # --- Export CSV ---
    export_path = output_dir / "details_combined.csv"
    row_count = export_combined_csv(db, run_id, export_path)

    # Export individual tables
    tables_dir = output_dir / "tables"
    table_names = [
        "in_formulation_status",
        "in_sku_main",
        "in_sku_mrp",
        "in_brand_alternatives",
        "in_med_details",
    ]
    for table in table_names:
        try:
            display_name = table.replace("in_", "")
            out_path = tables_dir / f"{display_name}.csv"
            count = export_table_csv(db, table, run_id, out_path)
            logger.info("  Exported %s: %d rows", display_name, count)
        except Exception as exc:
            logger.warning("Failed exporting table %s: %s", table, exc)

    db.close()

    logger.info("Export complete: %s (%d rows)", export_path, row_count)

    # --- Copy final CSV to exports/ folder ---
    import shutil
    try:
        from platform_config import get_path_manager
        exports_dir = get_path_manager().get_exports_dir("India")
    except Exception:
        exports_dir = _repo_root / "exports" / "India"
    exports_dir.mkdir(parents=True, exist_ok=True)
    exports_csv = exports_dir / "details_combined.csv"
    shutil.copy2(str(export_path), str(exports_csv))
    logger.info("Final CSV copied to exports: %s", exports_csv)

    # --- Summary ---
    print(f"\n{'='*60}")
    print(f"  India NPPA Export Summary")
    print(f"{'='*60}")
    print(f"  Run ID:       {run_id}")
    print(f"  QC:           PASSED")
    print(f"  Output:       {export_path}")
    print(f"  Exports:      {exports_csv}")
    print(f"  Rows:         {row_count}")
    print(f"  QC Report:    {qc_path}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
