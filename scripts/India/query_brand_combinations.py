#!/usr/bin/env python3
"""
Query unique combinations of brand and brand alternative in India data.
"""

import argparse
import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.db.postgres_connection import PostgresDB


def _get_latest_run_id(db) -> str | None:
    with db.cursor() as cur:
        # Prefer a timestamp/sequence column if present; fall back to lexical run_id.
        candidates = [
            "SELECT run_id FROM in_sku_main ORDER BY created_at DESC NULLS LAST LIMIT 1",
            "SELECT run_id FROM in_sku_main ORDER BY id DESC NULLS LAST LIMIT 1",
            "SELECT run_id FROM in_sku_main ORDER BY run_id DESC LIMIT 1",
        ]
        for q in candidates:
            try:
                cur.execute(q)
                row = cur.fetchone()
                if row and row[0]:
                    return row[0]
            except Exception:
                continue
    return None


def parse_args():
    p = argparse.ArgumentParser(description="Query unique brand + brand alternative combinations (India)")
    p.add_argument("--run-id", help="Run ID to query (defaults to latest run_id in in_sku_main)")
    p.add_argument("--breakdown", type=int, default=0, help="Show per-run breakdown for N latest runs")
    return p.parse_args()


def count_unique_brand_combinations(run_id: str | None, breakdown: int = 0):
    """Count uniqueness metrics for sku_main + brand_alternatives for a run_id."""
    db = PostgresDB("India")

    try:
        if not run_id:
            run_id = _get_latest_run_id(db)
        if not run_id:
            raise RuntimeError("Could not determine latest run_id from in_sku_main; pass --run-id explicitly.")

        # hidden_id is NOT used here. We compute uniqueness within each table and across both.
        # "pack count" is the leading integer from pack_size (e.g., "10 TABLET" -> 10).
        query = """
        WITH
            main AS (
                SELECT
                    sku_name AS brand,
                    pack_size,
                    substring(pack_size from '^[0-9]+') AS pack_qty
                FROM in_sku_main
                WHERE run_id = %s
                  AND sku_name IS NOT NULL AND sku_name != ''
            ),
            other AS (
                SELECT
                    brand_name AS brand,
                    pack_size,
                    substring(pack_size from '^[0-9]+') AS pack_qty
                FROM in_brand_alternatives
                WHERE run_id = %s
                  AND brand_name IS NOT NULL AND brand_name != ''
            ),
            main_distinct_brand_pack AS (
                SELECT DISTINCT brand, pack_size
                FROM main
            ),
            other_distinct_brand_pack AS (
                SELECT DISTINCT brand, pack_size
                FROM other
            ),
            all_distinct_brand_pack AS (
                SELECT brand, pack_size FROM main_distinct_brand_pack
                UNION
                SELECT brand, pack_size FROM other_distinct_brand_pack
            ),
            common_brands AS (
                SELECT brand FROM (SELECT DISTINCT brand FROM main) m
                INTERSECT
                SELECT brand FROM (SELECT DISTINCT brand FROM other) o
            )
        SELECT
            (SELECT COUNT(*) FROM main) AS main_rows,
            (SELECT COUNT(DISTINCT brand) FROM main) AS main_unique_brands,
            (SELECT COUNT(DISTINCT pack_size) FROM main WHERE pack_size IS NOT NULL AND pack_size != '') AS main_unique_pack_sizes,
            (SELECT COUNT(DISTINCT pack_qty) FROM main WHERE pack_qty IS NOT NULL AND pack_qty != '') AS main_unique_pack_counts,
            (SELECT COUNT(*) FROM main_distinct_brand_pack) AS main_unique_brand_pack,
            (SELECT COUNT(*) FROM main) - (SELECT COUNT(*) FROM main_distinct_brand_pack) AS main_duplicate_brand_pack_rows,

            (SELECT COUNT(*) FROM other) AS other_rows,
            (SELECT COUNT(DISTINCT brand) FROM other) AS other_unique_brands,
            (SELECT COUNT(DISTINCT pack_size) FROM other WHERE pack_size IS NOT NULL AND pack_size != '') AS other_unique_pack_sizes,
            (SELECT COUNT(DISTINCT pack_qty) FROM other WHERE pack_qty IS NOT NULL AND pack_qty != '') AS other_unique_pack_counts,
            (SELECT COUNT(*) FROM other_distinct_brand_pack) AS other_unique_brand_pack,
            (SELECT COUNT(*) FROM other) - (SELECT COUNT(*) FROM other_distinct_brand_pack) AS other_duplicate_brand_pack_rows,

            (SELECT COUNT(*) FROM all_distinct_brand_pack) AS unique_brand_pack_across_both,
            (SELECT COUNT(*) FROM common_brands) AS brands_present_in_both
        """

        with db.cursor() as cur:
            cur.execute(query, (run_id, run_id))
            result = cur.fetchone()

            (
                main_rows,
                main_unique_brands,
                main_unique_pack_sizes,
                main_unique_pack_counts,
                main_unique_brand_pack,
                main_duplicate_brand_pack_rows,
                other_rows,
                other_unique_brands,
                other_unique_pack_sizes,
                other_unique_pack_counts,
                other_unique_brand_pack,
                other_duplicate_brand_pack_rows,
                unique_brand_pack_across_both,
                brands_present_in_both,
            ) = result

            print("=" * 60)
            print("India Brand and Brand Alternative Combinations")
            print("=" * 60)
            print(f"Run ID: {run_id}")
            print(f"MAIN sku_main rows: {main_rows:,}")
            print(f"MAIN unique brands (sku_name): {main_unique_brands:,}")
            print(f"MAIN unique pack sizes (pack_size): {main_unique_pack_sizes:,}")
            print(f"MAIN unique pack counts (leading number): {main_unique_pack_counts:,}")
            print(f"MAIN unique (brand, pack_size): {main_unique_brand_pack:,}")
            print(f"MAIN duplicate rows by (brand, pack_size): {main_duplicate_brand_pack_rows:,}")
            print("-" * 60)
            print(f"OTHER brand_alternatives rows: {other_rows:,}")
            print(f"OTHER unique brands (brand_name): {other_unique_brands:,}")
            print(f"OTHER unique pack sizes (pack_size): {other_unique_pack_sizes:,}")
            print(f"OTHER unique pack counts (leading number): {other_unique_pack_counts:,}")
            print(f"OTHER unique (brand, pack_size): {other_unique_brand_pack:,}")
            print(f"OTHER duplicate rows by (brand, pack_size): {other_duplicate_brand_pack_rows:,}")
            print("-" * 60)
            print(f"Unique (brand, pack_size) across BOTH tables: {unique_brand_pack_across_both:,}")
            print(f"Brands present in BOTH tables (by name): {brands_present_in_both:,}")
            print("=" * 60)

            # Duplicate signals within each source table (same run_id)
            if breakdown and breakdown > 0:
                breakdown_query = f"""
                SELECT
                    s.run_id,
                    COUNT(*) AS sku_main_rows,
                    COUNT(DISTINCT s.sku_name) AS sku_main_unique_brands,
                    (SELECT COUNT(*) FROM in_brand_alternatives b2 WHERE b2.run_id = s.run_id) AS brand_alt_rows,
                    (SELECT COUNT(DISTINCT b2.brand_name) FROM in_brand_alternatives b2 WHERE b2.run_id = s.run_id AND b2.brand_name IS NOT NULL AND b2.brand_name != '') AS brand_alt_unique_brands
                FROM in_sku_main s
                GROUP BY s.run_id
                ORDER BY s.run_id DESC
                LIMIT {int(breakdown)}
                """
                cur.execute(breakdown_query)
                breakdown_results = cur.fetchall()
                if breakdown_results:
                    print(f"\nBreakdown by Run ID (latest {int(breakdown)} runs):")
                    print("-" * 60)
                    print(f"{'Run ID':<30} {'MAIN rows':<12} {'MAIN uniq':<10} {'OTHER rows':<12} {'OTHER uniq':<10}")
                    print("-" * 60)
                    for r_run_id, s_rows, s_uniq, o_rows, o_uniq in breakdown_results:
                        print(f"{r_run_id:<30} {s_rows:<12,} {s_uniq:<10,} {o_rows:<12,} {o_uniq:<10,}")

            return unique_brand_pack_across_both, run_id
            
    except Exception as e:
        print(f"Error querying database: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        db.close()

if __name__ == "__main__":
    args = parse_args()
    count_unique_brand_combinations(run_id=args.run_id, breakdown=args.breakdown)
