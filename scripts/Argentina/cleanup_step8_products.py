#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Argentina - Cleanup Step 8 (No-Data Retry) Products

Removes or resets products that were added/modified by Step 8 (no-data retry).
"""

import os
import sys
from pathlib import Path

# Add repo root to path for core imports (MUST be before any core imports)
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

from config_loader import get_output_dir
from core.db.connection import CountryDB
from db.repositories import ArgentinaRepository


def _get_run_id(output_dir: Path) -> str:
    rid = os.environ.get("ARGENTINA_RUN_ID", "").strip()
    if rid:
        return rid
    run_id_file = output_dir / ".current_run_id"
    if run_id_file.exists():
        txt = run_id_file.read_text(encoding="utf-8").strip()
        if txt:
            return txt
    raise RuntimeError("ARGENTINA_RUN_ID not set and .current_run_id missing.")


def main():
    output_dir = get_output_dir()
    run_id = _get_run_id(output_dir)

    db = CountryDB("Argentina")

    print("\n" + "=" * 80)
    print("CLEANUP STEP 8 PRODUCTS")
    print("=" * 80 + "\n")
    print(f"Run ID: {run_id}\n")

    # Check how many products were added by Step 8
    with db.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) as total,
                   COUNT(CASE WHEN total_records > 0 THEN 1 END) as scraped
            FROM ar_product_index
            WHERE run_id = %s AND scrape_source = 'step7'
        """, (run_id,))
        row = cur.fetchone()
        total = row[0] if isinstance(row, tuple) else row['total']
        scraped = row[1] if isinstance(row, tuple) else row['scraped']

    print(f"Products marked with scrape_source='step7': {total}")
    print(f"  - Successfully scraped (total_records > 0): {scraped}")
    print(f"  - Failed to scrape (total_records = 0): {total - scraped}")
    print()

    if total == 0:
        print("No products found with scrape_source='step7'. Nothing to clean up.")
        return

    # Ask user what to do
    print("Options:")
    print("  1. Delete from ar_product_index (remove queue entries)")
    print("  2. Reset to pending status (keep in queue but reset)")
    print("  3. Delete scraped data from ar_products (keep queue entries)")
    print("  4. Cancel")
    print()

    choice = input("Enter choice (1-4): ").strip()

    if choice == "1":
        # Delete from ar_product_index
        with db.cursor() as cur:
            cur.execute("""
                DELETE FROM ar_product_index
                WHERE run_id = %s AND scrape_source = 'step7'
            """, (run_id,))
        print(f"\n✓ Deleted {total} products from ar_product_index")

    elif choice == "2":
        # Reset to pending
        with db.cursor() as cur:
            cur.execute("""
                UPDATE ar_product_index
                SET status = 'pending',
                    loop_count = 0,
                    total_records = 0,
                    scraped_by_selenium = FALSE,
                    scraped_by_api = FALSE,
                    scrape_source = NULL,
                    error_message = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE run_id = %s AND scrape_source = 'step7'
            """, (run_id,))
        print(f"\n✓ Reset {total} products to pending status")

    elif choice == "3":
        # Delete scraped data
        with db.cursor() as cur:
            cur.execute("""
                DELETE FROM ar_products
                WHERE run_id = %s
                  AND (company, product) IN (
                    SELECT company, product
                    FROM ar_product_index
                    WHERE run_id = %s AND scrape_source = 'step7'
                  )
            """, (run_id, run_id))
        print(f"\n✓ Deleted scraped data for Step 8 products from ar_products")

    elif choice == "4":
        print("\nCancelled. No changes made.")
    else:
        print("\nInvalid choice. No changes made.")

    print("\n" + "=" * 80 + "\n")


if __name__ == "__main__":
    main()
