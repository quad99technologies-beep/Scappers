#!/usr/bin/env python3
"""
Import PCID Mapping CSV into my_pcid_reference table
====================================================
Loads PCID mapping reference data from CSV into PostgreSQL.

Usage:
    python import_pcid_mapping.py [path_to_pcid_mapping.csv]

If no path provided, uses: input/Malaysia/PCID Mapping - Malaysia.csv
"""

import csv
import sys
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.db.connection import CountryDB


def import_pcid_mapping(csv_path: Path):
    """Import PCID mapping CSV into my_pcid_reference table."""

    if not csv_path.exists():
        raise FileNotFoundError(f"PCID mapping file not found: {csv_path}")

    print(f"[IMPORT] Reading PCID mapping from: {csv_path}")

    # Read CSV
    records = []
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append({
                "pcid": row.get("PCID", "").strip(),
                "local_pack_code": row.get("Local Pack Code", "").strip(),
                "package_number": row.get("Package Number", "").strip(),
                "product_group": row.get("Product Group", "").strip(),
                "generic_name": row.get("Generic Name", "").strip(),
                "description": row.get("Description", "").strip(),
            })

    if not records:
        print("[WARNING] No records found in CSV")
        return

    print(f"[IMPORT] Read {len(records)} records from CSV")

    # Connect to DB
    with CountryDB("Malaysia") as db:
        # Clear existing data
        with db.cursor() as cur:
            cur.execute("DELETE FROM my_pcid_reference")
            print(f"[DB] Cleared existing PCID reference data")

        # Insert new data
        with db.cursor() as cur:
            sql = """
                INSERT INTO my_pcid_reference
                (pcid, local_pack_code, package_number, product_group, generic_name, description)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (local_pack_code) DO UPDATE SET
                    pcid = EXCLUDED.pcid,
                    package_number = EXCLUDED.package_number,
                    product_group = EXCLUDED.product_group,
                    generic_name = EXCLUDED.generic_name,
                    description = EXCLUDED.description
            """
            for record in records:
                cur.execute(sql, (
                    record["pcid"],
                    record["local_pack_code"],
                    record["package_number"],
                    record["product_group"],
                    record["generic_name"],
                    record["description"]
                ))

        print(f"[DB] Imported {len(records)} PCID mappings into my_pcid_reference")

        # Verify count
        with db.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM my_pcid_reference")
            count = cur.fetchone()[0]
            print(f"[DB] Total PCID reference records: {count}")


def main():
    """Main entry point."""
    # Determine CSV path
    if len(sys.argv) > 1:
        csv_path = Path(sys.argv[1])
    else:
        csv_path = _repo_root / "input" / "Malaysia" / "PCID Mapping - Malaysia.csv"

    print(f"\n{'='*60}")
    print(f"[STARTUP] Malaysia PCID Mapping Import")
    print(f"{'='*60}")
    print(f"CSV path: {csv_path}")
    print(f"{'='*60}\n")

    try:
        import_pcid_mapping(csv_path)
        print(f"\n[SUCCESS] PCID mapping import completed")
        return 0
    except Exception as e:
        print(f"\n[ERROR] Import failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
