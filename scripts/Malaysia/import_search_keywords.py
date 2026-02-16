#!/usr/bin/env python3
"""
Import Search Keywords CSV into my_input_products table
=======================================================
Upload user-provided search keywords for Quest3Plus bulk search.
Primary method: Use GUI Input page to upload CSV (data goes to my_input_products).
This script is for CLI import when you have a CSV file.

Usage:
    python import_search_keywords.py <path_to_csv>

CSV Format Expected:
    product_name,registration_no (optional)
    Panadol,
    Aspirin,MAL12345678
    ...
"""

import csv
import sys
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.db.connection import CountryDB


def import_search_keywords(csv_path: Path):
    """Import search keywords CSV into my_input_products table."""

    if not csv_path.exists():
        raise FileNotFoundError(f"Search keywords file not found: {csv_path}")

    print(f"[IMPORT] Reading search keywords from: {csv_path}")

    # Read CSV
    records = []
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            product_name = (row.get("product_name") or row.get("Product Name") or "").strip()
            registration_no = (row.get("registration_no") or row.get("Registration No") or "").strip()

            if product_name:  # Only add if product_name exists
                records.append({
                    "product_name": product_name,
                    "registration_no": registration_no if registration_no else None,
                })

    if not records:
        print("[WARNING] No valid records found in CSV")
        return

    print(f"[IMPORT] Read {len(records)} search keywords from CSV")

    # Connect to DB
    with CountryDB("Malaysia") as db:
        # Clear existing data
        with db.cursor() as cur:
            cur.execute("DELETE FROM my_input_products")
            print(f"[DB] Cleared existing search keywords")

        # Insert new data
        with db.cursor() as cur:
            sql = """
                INSERT INTO my_input_products (product_name, registration_no)
                VALUES (%s, %s)
            """
            for record in records:
                cur.execute(sql, (record["product_name"], record["registration_no"]))

        print(f"[DB] Imported {len(records)} search keywords into my_input_products")

        # Verify count
        with db.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM my_input_products")
            count = cur.fetchone()[0]
            print(f"[DB] Total search keywords in table: {count}")

        # Show sample
        with db.cursor() as cur:
            cur.execute("SELECT product_name FROM my_input_products LIMIT 5")
            samples = [row[0] for row in cur.fetchall()]
            print(f"[DB] Sample keywords: {', '.join(samples)}")


def main():
    """Main entry point."""
    # Determine CSV path - require path or use default
    if len(sys.argv) > 1:
        csv_path = Path(sys.argv[1])
    else:
        csv_path = _repo_root / "input" / "Malaysia" / "products.csv"
        print(f"[INFO] No path provided. Using default: {csv_path}")
        print(f"[INFO] Primary: Upload via GUI Input page (no CSV needed).")
        print(f"[INFO] Or: python import_search_keywords.py <path_to_csv>\n")

    print(f"\n{'='*60}")
    print(f"[STARTUP] Malaysia Search Keywords Import")
    print(f"{'='*60}")
    print(f"CSV path: {csv_path}")
    print(f"{'='*60}\n")

    try:
        import_search_keywords(csv_path)
        print(f"\n[SUCCESS] Search keywords import completed")
        print(f"\nNext steps:")
        print(f"  1. Run Step 2 (Quest3Plus scraper)")
        print(f"  2. Bulk search will use keywords from my_input_products table")
        return 0
    except Exception as e:
        print(f"\n[ERROR] Import failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
