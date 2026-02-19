#!/usr/bin/env python3
"""
Delete all India NPPA scraper data from the database.

This clears all run history, scraped data, and queue state.
The input table (in_input_formulations) is preserved.

Usage:
    python 000_delete_data.py            # interactive confirmation
    python 000_delete_data.py --force    # skip confirmation
"""
import sys
import argparse
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from core.db.postgres_connection import get_db

# FK-ordered: children before parents
TABLES_TO_CLEAR = [
    "in_brand_alternatives",
    "in_sku_mrp",
    "in_med_details",
    "in_sku_main",
    "in_formulation_status",
    "in_progress_snapshots",
    "in_errors",
    "in_formulation_map",
]


def main():
    parser = argparse.ArgumentParser(description="Delete all India NPPA scraper data")
    parser.add_argument("--force", action="store_true", help="Skip confirmation prompt")
    args = parser.parse_args()

    if not args.force:
        print("WARNING: This will delete ALL India scraper data (all runs, all scraped records).")
        print("The input table (in_input_formulations) will be preserved.")
        answer = input("Type 'yes' to confirm: ").strip().lower()
        if answer != "yes":
            print("Aborted.")
            sys.exit(0)

    db = get_db("India")
    db.connect()

    try:
        print("\nDeleting India data...")
        for table in TABLES_TO_CLEAR:
            try:
                cur = db.execute(f"DELETE FROM {table}")
                print(f"  {table}: {cur.rowcount} rows deleted")
            except Exception as e:
                print(f"  {table}: SKIP ({e})")

        cur = db.execute("DELETE FROM run_ledger WHERE scraper_name = 'India'")
        print(f"  run_ledger (India runs): {cur.rowcount} rows deleted")

        # Also clear pipeline checkpoint file
        try:
            from config_loader import get_output_dir
            cp_file = get_output_dir() / ".checkpoints" / "pipeline_checkpoint.json"
            if cp_file.exists():
                cp_file.unlink()
                print(f"  Checkpoint file removed: {cp_file}")
        except Exception:
            pass

        print("\nAll India data cleared. Ready for a fresh run.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
