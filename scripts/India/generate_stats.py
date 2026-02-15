#!/usr/bin/env python3
"""
India NPPA Pipeline Statistics Generator

Generates detailed statistics after a scraping run:
- High-level summary (totals, rates, coverage)
- Per-formulation breakdown (SKUs, brands, alternatives)
- Export to CSV for analysis

Usage:
    python generate_stats.py                    # Use latest run
    python generate_stats.py --run-id <id>      # Specific run
    python generate_stats.py --export           # Export to CSV
    python generate_stats.py --top 50           # Show top 50 formulations
"""

import argparse
import csv
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.db.postgres_connection import PostgresDB

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("india_stats")


def get_latest_run_id(db) -> Optional[str]:
    """Get the most recent India run_id."""
    cur = db.execute("""
        SELECT run_id FROM run_ledger
        WHERE scraper_name = 'India'
        ORDER BY started_at DESC LIMIT 1
    """)
    row = cur.fetchone()
    return row[0] if row else None


def get_run_info(db, run_id: str) -> dict:
    """Get run metadata."""
    cur = db.execute("""
        SELECT run_id, status, started_at, items_scraped, mode, thread_count, totals_json
        FROM run_ledger WHERE run_id = %s
    """, (run_id,))
    row = cur.fetchone()
    if not row:
        return {}
    return {
        "run_id": row[0],
        "status": row[1],
        "started_at": row[2],
        "items_scraped": row[3],
        "mode": row[4],
        "thread_count": row[5],
        "totals_json": row[6],
    }


def get_high_level_stats(db, run_id: str) -> dict:
    """Generate high-level statistics."""
    stats = {}

    # Formulation status breakdown
    cur = db.execute("""
        SELECT status, COUNT(*)
        FROM in_formulation_status
        WHERE run_id = %s
        GROUP BY status
    """, (run_id,))
    status_counts = dict(cur.fetchall())
    stats["formulation_status"] = status_counts
    stats["total_formulations"] = sum(status_counts.values())
    stats["completed_formulations"] = status_counts.get("completed", 0)
    stats["zero_records"] = status_counts.get("zero_records", 0)
    stats["failed"] = status_counts.get("failed", 0)
    stats["pending"] = status_counts.get("pending", 0)
    stats["in_progress"] = status_counts.get("in_progress", 0)

    done = stats["completed_formulations"] + stats["zero_records"] + stats["failed"]
    stats["completion_pct"] = round((done / stats["total_formulations"]) * 100, 2) if stats["total_formulations"] > 0 else 0

    # Main SKU stats
    cur = db.execute("""
        SELECT
            COUNT(*) as total_skus,
            COUNT(DISTINCT hidden_id) as unique_skus,
            COUNT(DISTINCT formulation) as formulations_with_data,
            COUNT(DISTINCT company) as unique_companies,
            COUNT(DISTINCT sku_name) as unique_product_names
        FROM in_sku_main WHERE run_id = %s
    """, (run_id,))
    row = cur.fetchone()
    stats["total_skus"] = row[0]
    stats["unique_skus"] = row[1]
    stats["formulations_with_data"] = row[2]
    stats["unique_companies"] = row[3]
    stats["unique_product_names"] = row[4]

    # --- in_sku_main stats ---
    # Unique Product Name × Generic Name combinations
    cur = db.execute("""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT sku_name, formulation
            FROM in_sku_main WHERE run_id = %s
        ) AS unique_combos
    """, (run_id,))
    stats["main_product_generic_combos"] = cur.fetchone()[0]

    # Unique Generic × Brand combinations (main SKUs)
    cur = db.execute("""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT formulation, sku_name
            FROM in_sku_main WHERE run_id = %s
        ) t
    """, (run_id,))
    stats["main_generic_brand_combos"] = cur.fetchone()[0]

    # --- in_brand_alternatives stats ---
    # Unique Generic × Brand combinations (alternatives)
    cur = db.execute("""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT formulation, brand_name
            FROM in_brand_alternatives WHERE run_id = %s
        ) t
    """, (run_id,))
    stats["alt_generic_brand_combos"] = cur.fetchone()[0]

    # Brand alternatives stats
    cur = db.execute("""
        SELECT
            COUNT(*) as total_alternatives,
            COUNT(DISTINCT hidden_id) as skus_with_alternatives,
            COUNT(DISTINCT brand_name) as unique_alt_brands
        FROM in_brand_alternatives WHERE run_id = %s
    """, (run_id,))
    row = cur.fetchone()
    stats["total_alternatives"] = row[0]
    stats["skus_with_alternatives"] = row[1]
    stats["unique_alt_brands"] = row[2]
    stats["avg_alternatives_per_sku"] = round(row[0] / row[1], 1) if row[1] > 0 else 0

    # SKU MRP and Med Details counts
    cur = db.execute("SELECT COUNT(*) FROM in_sku_mrp WHERE run_id = %s", (run_id,))
    stats["sku_mrp_records"] = cur.fetchone()[0]

    cur = db.execute("SELECT COUNT(*) FROM in_med_details WHERE run_id = %s", (run_id,))
    stats["med_details_records"] = cur.fetchone()[0]

    # Top companies by SKU count
    cur = db.execute("""
        SELECT company, COUNT(*) as cnt
        FROM in_sku_main WHERE run_id = %s AND company IS NOT NULL AND company != ''
        GROUP BY company ORDER BY cnt DESC LIMIT 10
    """, (run_id,))
    stats["top_companies"] = [{"company": r[0], "sku_count": r[1]} for r in cur.fetchall()]

    # Dosage form distribution
    cur = db.execute("""
        SELECT dosage_form, COUNT(*) as cnt
        FROM in_sku_main WHERE run_id = %s AND dosage_form IS NOT NULL AND dosage_form != ''
        GROUP BY dosage_form ORDER BY cnt DESC LIMIT 10
    """, (run_id,))
    stats["top_dosage_forms"] = [{"dosage_form": r[0], "count": r[1]} for r in cur.fetchall()]

    return stats


def get_per_formulation_stats(db, run_id: str, limit: int = None) -> list:
    """Get detailed stats for each formulation (optimized - no expensive joins)."""
    limit_clause = f"LIMIT {limit}" if limit else ""

    # Step 1: Get formulation status and SKU stats (fast)
    cur = db.execute(f"""
        SELECT
            fs.formulation,
            fs.status,
            fs.medicines_count,
            fs.error_message,
            COALESCE(ss.sku_count, 0) as sku_count,
            COALESCE(ss.company_count, 0) as company_count,
            COALESCE(ss.dosage_forms, 0) as dosage_forms,
            COALESCE(ss.unique_products, 0) as unique_products
        FROM in_formulation_status fs
        LEFT JOIN (
            SELECT
                formulation,
                COUNT(*) as sku_count,
                COUNT(DISTINCT company) as company_count,
                COUNT(DISTINCT dosage_form) as dosage_forms,
                COUNT(DISTINCT sku_name) as unique_products
            FROM in_sku_main
            WHERE run_id = %s
            GROUP BY formulation
        ) ss ON ss.formulation = fs.formulation
        WHERE fs.run_id = %s
        ORDER BY COALESCE(ss.sku_count, 0) DESC, fs.formulation
        {limit_clause}
    """, (run_id, run_id))

    results = []
    formulation_list = []
    for row in cur.fetchall():
        formulation_list.append(row[0])
        results.append({
            "formulation": row[0],
            "status": row[1],
            "medicines_count_reported": row[2],
            "error_message": row[3],
            "sku_count": row[4],
            "company_count": row[5],
            "dosage_forms": row[6],
            "unique_products": row[7],
            "alternative_count": 0,  # Will be populated if fast enough
            "unique_alt_brands": 0,
        })

    # Step 2: Get alternative counts per formulation using pre-aggregated data
    # This uses a faster approach: count from in_brand_alternatives grouped by hidden_id
    # then sum by formulation via the sku_main link
    formulations_with_data = [r["formulation"] for r in results if r["sku_count"] > 0]
    if formulations_with_data:
        try:
            # For large datasets, use a simpler aggregation
            cur = db.execute("""
                SELECT
                    s.formulation,
                    SUM(alt_counts.cnt) as alternative_count
                FROM in_sku_main s
                INNER JOIN (
                    SELECT hidden_id, COUNT(*) as cnt
                    FROM in_brand_alternatives
                    WHERE run_id = %s
                    GROUP BY hidden_id
                ) alt_counts ON alt_counts.hidden_id = s.hidden_id
                WHERE s.run_id = %s
                GROUP BY s.formulation
            """, (run_id, run_id))

            alt_map = {row[0]: int(row[1]) for row in cur.fetchall()}
            for r in results:
                if r["formulation"] in alt_map:
                    r["alternative_count"] = alt_map[r["formulation"]]
        except Exception as e:
            logger.debug(f"Alternative count query skipped: {e}")

    return results


def print_stats(run_info: dict, high_level: dict, per_formulation: list, top_n: int = 20):
    """Print formatted statistics to console."""

    print("\n" + "=" * 80)
    print("                    INDIA NPPA PIPELINE - RUN STATISTICS")
    print("=" * 80)

    # Run Info
    print(f"\n{'-' * 40}")
    print("RUN INFORMATION")
    print(f"{'-' * 40}")
    print(f"  Run ID:        {run_info.get('run_id', 'N/A')}")
    print(f"  Status:        {run_info.get('status', 'N/A')}")
    print(f"  Started:       {run_info.get('started_at', 'N/A')}")
    print(f"  Mode:          {run_info.get('mode', 'N/A')}")
    print(f"  Workers:       {run_info.get('thread_count', 'N/A')}")

    # High-Level Stats
    print(f"\n{'-' * 40}")
    print("HIGH-LEVEL STATISTICS")
    print(f"{'-' * 40}")

    print("\n  FORMULATION PROCESSING:")
    print(f"    Total Formulations:     {high_level['total_formulations']:,}")
    print(f"    Completed:              {high_level['completed_formulations']:,}")
    print(f"    Zero Records:           {high_level['zero_records']:,}")
    print(f"    Failed:                 {high_level['failed']:,}")
    print(f"    Pending:                {high_level['pending']:,}")
    print(f"    In Progress:            {high_level['in_progress']:,}")
    print(f"    Completion:             {high_level['completion_pct']}%")

    print("\n  SKU DATA (in_sku_main):")
    print(f"    Total SKU Records:      {high_level['total_skus']:,}")
    print(f"    Unique SKUs:            {high_level['unique_skus']:,}")
    print(f"    Formulations w/ Data:   {high_level['formulations_with_data']:,}")
    print(f"    Unique Companies:       {high_level['unique_companies']:,}")
    print(f"    Unique Product Names:   {high_level.get('unique_product_names', 0):,}")
    print(f"    Product x Generic:      {high_level.get('main_product_generic_combos', 0):,}")
    print(f"    Generic x Brand:        {high_level.get('main_generic_brand_combos', 0):,}")

    print("\n  BRAND ALTERNATIVES (in_brand_alternatives):")
    print(f"    Total Records:          {high_level['total_alternatives']:,}")
    print(f"    SKUs with Alternatives: {high_level['skus_with_alternatives']:,}")
    print(f"    Unique Alt Brands:      {high_level['unique_alt_brands']:,}")
    print(f"    Generic x Brand:        {high_level.get('alt_generic_brand_combos', 0):,}")
    print(f"    Avg Alts per SKU:       {high_level['avg_alternatives_per_sku']:,}")

    print("\n  DETAIL TABLES:")
    print(f"    SKU MRP Records:        {high_level['sku_mrp_records']:,}")
    print(f"    Med Details Records:    {high_level['med_details_records']:,}")

    # Top Companies
    print(f"\n{'-' * 40}")
    print("TOP 10 COMPANIES BY SKU COUNT")
    print(f"{'-' * 40}")
    for i, c in enumerate(high_level.get("top_companies", []), 1):
        print(f"  {i:2}. {c['company'][:40]:<40} {c['sku_count']:>8,} SKUs")

    # Top Dosage Forms
    print(f"\n{'-' * 40}")
    print("TOP 10 DOSAGE FORMS")
    print(f"{'-' * 40}")
    for i, d in enumerate(high_level.get("top_dosage_forms", []), 1):
        print(f"  {i:2}. {d['dosage_form'][:40]:<40} {d['count']:>8,}")

    # Per-Formulation Stats
    print(f"\n{'-' * 40}")
    print(f"PER-FORMULATION BREAKDOWN (Top {top_n})")
    print(f"{'-' * 40}")
    print(f"  {'#':<4} {'Formulation':<40} {'Status':<10} {'SKUs':>7} {'Products':>9} {'Companies':>10} {'Alternatives':>12}")
    print(f"  {'-'*4} {'-'*40} {'-'*10} {'-'*7} {'-'*9} {'-'*10} {'-'*12}")

    for i, f in enumerate(per_formulation[:top_n], 1):
        name = f['formulation'][:39] if f['formulation'] else "N/A"
        status = f['status'][:9] if f['status'] else "N/A"
        print(f"  {i:<4} {name:<40} {status:<10} {f['sku_count']:>7,} {f.get('unique_products', 0):>9,} {f['company_count']:>10,} {f['alternative_count']:>12,}")

    # Summary
    total_skus = sum(f['sku_count'] for f in per_formulation)
    total_products = sum(f.get('unique_products', 0) for f in per_formulation)
    total_alts = sum(f['alternative_count'] for f in per_formulation)
    with_data = sum(1 for f in per_formulation if f['sku_count'] > 0)

    print(f"\n{'-' * 40}")
    print("SUMMARY")
    print(f"{'-' * 40}")
    print(f"  Formulations with data:   {with_data:,} / {len(per_formulation):,}")
    print(f"  Total SKUs:               {total_skus:,}")
    print(f"  Total Unique Products:    {total_products:,}")
    print("\n  Main SKUs (in_sku_main):")
    print(f"    Product x Generic:      {high_level.get('main_product_generic_combos', 0):,}")
    print(f"    Generic x Brand:        {high_level.get('main_generic_brand_combos', 0):,}")
    print("\n  Alternatives (in_brand_alternatives):")
    print(f"    Total Records:          {total_alts:,}")
    print(f"    Generic x Brand:        {high_level.get('alt_generic_brand_combos', 0):,}")
    print(f"    Unique Brand Names:     {high_level['unique_alt_brands']:,}")

    print("\n" + "=" * 80)


def export_to_csv(per_formulation: list, output_path: Path):
    """Export per-formulation stats to CSV."""
    fieldnames = [
        "formulation", "status", "sku_count", "unique_products", "company_count",
        "dosage_forms", "alternative_count", "unique_alt_brands",
        "medicines_count_reported", "error_message"
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(per_formulation)

    print(f"\nExported {len(per_formulation)} rows to: {output_path}")


def export_summary_json(run_info: dict, high_level: dict, output_path: Path):
    """Export summary stats to JSON."""
    data = {
        "generated_at": datetime.now().isoformat(),
        "run_info": {k: str(v) if isinstance(v, datetime) else v for k, v in run_info.items()},
        "statistics": high_level,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)

    print(f"Exported summary to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Generate India NPPA pipeline statistics")
    parser.add_argument("--run-id", help="Specific run ID (default: latest)")
    parser.add_argument("--export", action="store_true", help="Export to CSV and JSON")
    parser.add_argument("--top", type=int, default=20, help="Number of top formulations to show (default: 20)")
    parser.add_argument("--output-dir", help="Output directory for exports")
    args = parser.parse_args()

    # Connect to DB
    db = PostgresDB("India")
    db.connect()

    try:
        # Get run ID
        run_id = args.run_id or get_latest_run_id(db)
        if not run_id:
            print("No India runs found in database.")
            return 1

        print(f"Generating statistics for run: {run_id}")

        # Gather stats
        run_info = get_run_info(db, run_id)
        high_level = get_high_level_stats(db, run_id)
        per_formulation = get_per_formulation_stats(db, run_id)

        # Print to console
        print_stats(run_info, high_level, per_formulation, top_n=args.top)

        # Export if requested
        if args.export:
            output_dir = Path(args.output_dir) if args.output_dir else Path("output/India")
            output_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Export CSV
            csv_path = output_dir / f"india_stats_{run_id}_{timestamp}.csv"
            export_to_csv(per_formulation, csv_path)

            # Export JSON summary
            json_path = output_dir / f"india_summary_{run_id}_{timestamp}.json"
            export_summary_json(run_info, high_level, json_path)

        return 0

    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
