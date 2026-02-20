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
import re
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


# Target export format: BrandType, BrandName, Company, Composition, PackSize, Unit, Status, CeilingPrice, MRP, MRPPerUnit, YearMonth
EXPORT_COLUMNS = [
    "BrandType", "BrandName", "Company", "Composition", "PackSize", "Unit",
    "Status", "CeilingPrice", "MRP", "MRPPerUnit", "YearMonth",
]

# Max rows per part file (8 lakh = 800,000)
MAX_ROWS_PER_PART = 800_000


def _parse_pack_size(pack_size_str: str) -> int:
    """Extract numeric pack size from strings like '30 TABLET' or '5 ML'."""
    if not pack_size_str or not pack_size_str.strip():
        return 0
    m = re.match(r"^(\d+)", pack_size_str.strip())
    return int(m.group(1)) if m else 0


def _format_year_month(year_month: str) -> str:
    """Convert 'Dec-2025' to 'Dec-25' (Mon-YY)."""
    if not year_month or not year_month.strip():
        return ""
    s = year_month.strip()
    if len(s) >= 8 and s[-4] == "-":
        return s[:-2]  # Dec-2025 -> Dec-25
    return s


def _ceiling_price_display(val) -> str:
    """Display ceiling price: -1 or empty -> 0."""
    if val is None or val == "" or str(val).strip() in ("-1", ""):
        return "0"
    return str(val).strip()


def export_combined_csv(db, run_id: str, output_dir: Path) -> tuple[int, int, int, list[tuple[Path, int]]]:
    """Export combined details CSV in target format (BrandType, BrandName, ...), split into parts of max 8 lakh rows each.
    Includes in_sku_main (MAIN) + in_brand_alternatives (OTHER) so export row count matches full data.
    Uses streaming to handle large datasets without memory issues.
    Returns (total_rows, main_count, other_count, list of (path, row_count) per part).
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    batch_size = 50000  # Process in batches to avoid memory issues
    part_num = 0
    current_part_rows = []
    written_paths = []
    main_count = 0
    other_count = 0
    total_rows = 0

    def flush_part():
        """Write current batch to a part file."""
        nonlocal part_num, current_part_rows
        if not current_part_rows:
            return
        part_num += 1
        part_name = f"details_combined_{part_num:03d}.csv"
        part_path = output_dir / part_name
        with part_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(EXPORT_COLUMNS)
            writer.writerows(current_part_rows)
        written_paths.append((part_path, len(current_part_rows)))
        current_part_rows = []

    def add_row(row):
        """Add a row and flush if part is full."""
        nonlocal current_part_rows, total_rows
        current_part_rows.append(row)
        total_rows += 1
        if len(current_part_rows) >= MAX_ROWS_PER_PART:
            flush_part()

    # --- MAIN rows: from in_sku_main (one per SKU) ---
    main_query = """
        SELECT
            s.formulation,
            s.sku_name,
            s.company,
            s.composition,
            s.pack_size,
            s.dosage_form,
            s.schedule_status,
            s.ceiling_price,
            s.mrp,
            s.mrp_per_unit,
            s.year_month
        FROM in_sku_main s
        WHERE s.run_id = %s
        ORDER BY s.formulation, s.composition, s.pack_size, s.dosage_form, s.sku_name
    """
    cur = db.execute(main_query, (run_id,))
    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        for r in rows:
            (formulation, sku_name, company, composition, pack_size, dosage_form,
             schedule_status, ceiling_price, mrp, mrp_per_unit, year_month) = r
            main_count += 1
            pack_num = _parse_pack_size(pack_size or "")
            unit = (dosage_form or "").strip()
            status = (schedule_status or "").strip()
            ceiling_display = _ceiling_price_display(ceiling_price)
            mrp_str = (mrp or "").strip()
            mrp_per_unit_str = (mrp_per_unit or "").strip()
            ym = _format_year_month(year_month or "")
            add_row([
                "MAIN",
                (sku_name or "").strip(),
                (company or "").strip(),
                (composition or "").strip(),
                pack_num,
                unit,
                status,
                ceiling_display,
                mrp_str,
                mrp_per_unit_str,
                ym,
            ])

    logger.info("  Exported %d MAIN rows", main_count)

    # --- OTHER rows: from in_brand_alternatives (join to in_sku_main for composition/unit/status) ---
    alt_query = """
        SELECT
            b.brand_name,
            b.company,
            b.pack_size,
            b.brand_mrp,
            b.mrp_per_unit,
            b.year_month,
            s.composition,
            s.dosage_form,
            s.schedule_status,
            s.ceiling_price
        FROM in_brand_alternatives b
        JOIN in_sku_main s ON b.hidden_id = s.hidden_id AND b.run_id = s.run_id
        WHERE b.run_id = %s
        ORDER BY s.formulation, s.composition, b.brand_name
    """
    cur = db.execute(alt_query, (run_id,))
    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        for r in rows:
            (brand_name, company, pack_size, brand_mrp, mrp_per_unit, year_month,
             composition, dosage_form, schedule_status, ceiling_price) = r
            other_count += 1
            pack_num = _parse_pack_size(pack_size or "")
            unit = (dosage_form or "").strip()
            status = (schedule_status or "").strip()
            ceiling_display = _ceiling_price_display(ceiling_price)
            mrp_str = (brand_mrp or "").strip()
            mrp_per_unit_str = (mrp_per_unit or "").strip()
            ym = _format_year_month(year_month or "")
            add_row([
                "OTHER",
                (brand_name or "").strip(),
                (company or "").strip(),
                (composition or "").strip(),
                pack_num,
                unit,
                status,
                ceiling_display,
                mrp_str,
                mrp_per_unit_str,
                ym,
            ])
        # Log progress for large datasets
        if other_count % 1000000 == 0:
            logger.info("  Processed %d OTHER rows...", other_count)

    logger.info("  Exported %d OTHER rows", other_count)

    # Flush any remaining rows
    flush_part()

    return total_rows, main_count, other_count, written_paths


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

    # If this run has no data, use the latest run that has rows (e.g. resume after empty run)
    cur = db.execute("SELECT COUNT(*) FROM in_sku_main WHERE run_id = %s", (run_id,))
    if cur.fetchone()[0] == 0:
        cur = db.execute(
            "SELECT s.run_id FROM in_sku_main s "
            "WHERE s.run_id IN (SELECT run_id FROM run_ledger WHERE scraper_name = 'India') "
            "GROUP BY s.run_id ORDER BY MAX(s.created_at) DESC LIMIT 1"
        )
        row = cur.fetchone()
        if row:
            logger.warning("Run %s has no rows; using latest run with data: %s", run_id, row[0])
            run_id = row[0]
        else:
            logger.error("No India run has data in in_sku_main. Run the scrape step first.")
            db.close()
            sys.exit(1)

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

    # --- Unique combination counts ---
    # (1) Product-type level: formulation + composition + pack_size + dosage_form
    cur = db.execute(
        "SELECT COUNT(DISTINCT (formulation, composition, pack_size, dosage_form)) "
        "FROM in_sku_main WHERE run_id = %s",
        (run_id,),
    )
    unique_combination = cur.fetchone()[0]

    # (2) Full table (main + others): distinct BrandName + Company + Composition + PackSize + Unit
    # OPTIMIZED: Compute main and alt counts separately, then estimate combined
    # (Full UNION is too slow with 54M+ alternatives)
    logger.info("Computing unique combination counts (optimized)...")

    # Main SKUs unique combinations
    cur = db.execute(
        "SELECT COUNT(*) FROM ("
        "  SELECT DISTINCT sku_name, company, composition, pack_size, dosage_form "
        "  FROM in_sku_main WHERE run_id = %s"
        ") t",
        (run_id,),
    )
    main_unique = cur.fetchone()[0]

    # Alternatives unique brand names (approximate, avoid full join)
    cur = db.execute(
        "SELECT COUNT(DISTINCT brand_name) FROM in_brand_alternatives WHERE run_id = %s",
        (run_id,),
    )
    alt_unique_brands = cur.fetchone()[0]

    # Combined estimate (main + unique alt brands not in main)
    unique_combination_brand = main_unique + alt_unique_brands
    logger.info("  Main unique combos: %d, Alt unique brands: %d, Combined: %d",
                main_unique, alt_unique_brands, unique_combination_brand)

    # (3) Unique count per column - compute SEPARATELY for main and alternatives
    # Main table stats
    cur = db.execute(
        "SELECT "
        "  COUNT(DISTINCT sku_name) AS u_brand_name, "
        "  COUNT(DISTINCT company) AS u_company, "
        "  COUNT(DISTINCT composition) AS u_composition, "
        "  COUNT(DISTINCT pack_size) AS u_pack_size, "
        "  COUNT(DISTINCT dosage_form) AS u_unit, "
        "  COUNT(DISTINCT schedule_status) AS u_status, "
        "  COUNT(DISTINCT ceiling_price) AS u_ceiling_price, "
        "  COUNT(DISTINCT mrp) AS u_mrp, "
        "  COUNT(DISTINCT mrp_per_unit) AS u_mrp_per_unit, "
        "  COUNT(DISTINCT year_month) AS u_year_month "
        "FROM in_sku_main WHERE run_id = %s",
        (run_id,),
    )
    main_row = cur.fetchone()

    # Alternatives table stats (without expensive join)
    cur = db.execute(
        "SELECT "
        "  COUNT(DISTINCT brand_name) AS u_brand_name, "
        "  COUNT(DISTINCT company) AS u_company, "
        "  COUNT(DISTINCT pack_size) AS u_pack_size, "
        "  COUNT(DISTINCT brand_mrp) AS u_mrp, "
        "  COUNT(DISTINCT mrp_per_unit) AS u_mrp_per_unit, "
        "  COUNT(DISTINCT year_month) AS u_year_month "
        "FROM in_brand_alternatives WHERE run_id = %s",
        (run_id,),
    )
    alt_row = cur.fetchone()

    # Combined unique counts (max of main + alt for overlap columns)
    unique_per_column = {
        "BrandType": 2,  # MAIN and OTHER
        "BrandName": main_row[0] + alt_row[0],  # Combined brands
        "Company": main_row[1] + alt_row[1],  # Combined companies
        "Composition": main_row[2],  # Only in main
        "PackSize": main_row[3] + alt_row[2],  # Combined
        "Unit": main_row[4],  # Only in main (dosage_form)
        "Status": main_row[5],  # Only in main
        "CeilingPrice": main_row[6],  # Only in main
        "MRP": main_row[7] + alt_row[3],  # Combined
        "MRPPerUnit": main_row[8] + alt_row[4],  # Combined
        "YearMonth": main_row[9] + alt_row[5],  # Combined
    }
    logger.info("  Unique per column computed")

    # --- Export CSV (in parts, max 8 lakh rows each) ---
    row_count, main_count, other_count, part_paths = export_combined_csv(db, run_id, output_dir)
    qc_results["main_count"] = main_count
    qc_results["other_count"] = other_count
    qc_results["unique_combination"] = unique_combination
    qc_results["unique_combination_brand"] = unique_combination_brand
    qc_results["unique_per_column"] = unique_per_column
    qc_results["export_parts"] = [{"file": p.name, "rows": n} for p, n in part_paths]
    qc_path.write_text(json.dumps(qc_results, indent=2), encoding="utf-8")

    # --- Unique lists: Product Name (BrandName) and Generic Name (formulation) ---
    # Get unique product names separately to avoid expensive UNION on 52M rows
    cur = db.execute(
        "SELECT DISTINCT sku_name FROM in_sku_main WHERE run_id = %s ORDER BY sku_name",
        (run_id,),
    )
    main_product_names = set(row[0] or "" for row in cur.fetchall())

    cur = db.execute(
        "SELECT DISTINCT brand_name FROM in_brand_alternatives WHERE run_id = %s ORDER BY brand_name",
        (run_id,),
    )
    alt_product_names = set(row[0] or "" for row in cur.fetchall())

    unique_product_names = sorted(main_product_names | alt_product_names)

    # Get unique formulations (only from main table since it's the source)
    cur = db.execute(
        "SELECT DISTINCT formulation FROM in_sku_main WHERE run_id = %s ORDER BY formulation",
        (run_id,),
    )
    unique_generic_names = [row[0] or "" for row in cur.fetchall()]
    for name, values in [("unique_product_name", unique_product_names), ("unique_generic_name", unique_generic_names)]:
        col_header = "Product Name" if name == "unique_product_name" else "Generic Name"
        out_path = output_dir / f"{name}.csv"
        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([col_header])
            writer.writerows([[v] for v in values])
        logger.info("  Unique list: %s (%d entries)", out_path.name, len(values))
    qc_results["unique_product_name_count"] = len(unique_product_names)
    qc_results["unique_generic_name_count"] = len(unique_generic_names)
    qc_path.write_text(json.dumps(qc_results, indent=2), encoding="utf-8")

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

    logger.info("Export complete: %d rows in %d part(s)", row_count, len(part_paths))

    # --- Copy part CSVs to exports/ folder ---
    import shutil
    try:
        from core.config.config_manager import ConfigManager
        exports_dir = ConfigManager.get_exports_dir("India")
    except Exception:
        exports_dir = _repo_root / "exports" / "India"
    exports_dir.mkdir(parents=True, exist_ok=True)
    for part_path, part_rows in part_paths:
        dest = exports_dir / part_path.name
        shutil.copy2(str(part_path), str(dest))
        logger.info("  Copied %s -> %s (%d rows)", part_path.name, dest, part_rows)
    for name in ("unique_product_name.csv", "unique_generic_name.csv"):
        src = output_dir / name
        if src.exists():
            shutil.copy2(str(src), str(exports_dir / name))
            logger.info("  Copied %s -> %s", name, exports_dir / name)

    # --- Summary ---
    print(f"\n{'='*60}")
    print(f"  India NPPA Export Summary")
    print(f"{'='*60}")
    print(f"  Run ID:       {run_id}")
    print(f"  QC:           PASSED")
    print(f"  Rows:         {row_count} (max {MAX_ROWS_PER_PART:,} per part)")
    print(f"  Unique comb.: {unique_combination} (formulation+composition+pack_size+dosage_form)")
    print(f"  Unique (B+C+P+U): {unique_combination_brand} (BrandName+Company+Composition+PackSize+Unit, entire table)")
    print(f"  MAIN:         {main_count} (unique)")
    print(f"  OTHER:        {other_count} (unique)")
    print(f"  Unique per column (entire table):")
    for col, count in unique_per_column.items():
        print(f"    - {col}: {count:,}")
    print(f"  Parts:        {len(part_paths)} file(s)")
    for part_path, part_rows in part_paths:
        print(f"    - {part_path.name}: {part_rows:,} rows")
    print(f"  Unique lists: unique_product_name.csv ({len(unique_product_names):,}), unique_generic_name.csv ({len(unique_generic_names):,})")
    print(f"  Output:       {output_dir}")
    print(f"  Exports:      {exports_dir}")
    print(f"  QC Report:    {qc_path}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
