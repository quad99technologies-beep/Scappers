#!/usr/bin/env python3
"""
India: Fix duplicate data and ensure unique indexes exist.

This script:
1. Removes duplicate rows from all India tables
2. Creates/recreates unique indexes
3. Verifies no duplicates remain

Run this ONCE before running the scraper if you see duplicate key errors.
"""

import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.db.postgres_connection import PostgresDB

def main():
    print("=" * 60)
    print("India: Fixing Duplicate Data")
    print("=" * 60)
    
    db = PostgresDB("India")
    db.connect()
    
    # Read the fix script
    fix_script_path = Path(__file__).parent.parent.parent / "sql" / "schemas" / "postgres" / "india_fix_duplicates.sql"
    if not fix_script_path.exists():
        print(f"ERROR: Fix script not found: {fix_script_path}")
        sys.exit(1)
    
    fix_sql = fix_script_path.read_text(encoding="utf-8")
    
    print("\nStep 1: Checking for duplicates...")
    
    # Count duplicates before fix
    tables_to_check = [
        ("in_sku_mrp", "hidden_id, run_id"),
        ("in_med_details", "hidden_id, run_id"),
        ("in_brand_alternatives", "hidden_id, brand_name, pack_size, run_id"),
        ("in_sku_main", "hidden_id, run_id"),
    ]
    
    total_dups = 0
    for table, key_cols in tables_to_check:
        try:
            cur = db.execute(f"SELECT COUNT(*) - COUNT(DISTINCT ({key_cols})) FROM {table}")
            dup_count = cur.fetchone()[0]
            if dup_count > 0:
                print(f"  {table}: {dup_count} duplicates found")
                total_dups += dup_count
            else:
                print(f"  {table}: No duplicates ✓")
        except Exception as e:
            print(f"  {table}: Error checking - {e}")
    
    if total_dups == 0:
        print("\n✓ No duplicates found! Indexes should already be in place.")
        db.close()
        return
    
    print(f"\nStep 2: Removing {total_dups} duplicate rows...")
    
    try:
        db.executescript(fix_sql)
        print("✓ Duplicates removed and indexes created successfully!")
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        db.close()
        sys.exit(1)
    
    print("\nStep 3: Verifying fix...")
    
    # Verify no duplicates remain
    all_clean = True
    for table, key_cols in tables_to_check:
        try:
            cur = db.execute(f"SELECT COUNT(*) - COUNT(DISTINCT ({key_cols})) FROM {table}")
            dup_count = cur.fetchone()[0]
            if dup_count > 0:
                print(f"  {table}: Still has {dup_count} duplicates ✗")
                all_clean = False
            else:
                print(f"  {table}: Clean ✓")
        except Exception as e:
            print(f"  {table}: Error verifying - {e}")
            all_clean = False
    
    db.close()
    
    if all_clean:
        print("\n" + "=" * 60)
        print("SUCCESS: All India tables are now duplicate-free!")
        print("You can now run the scraper without duplicate key errors.")
        print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("WARNING: Some duplicates may still exist.")
        print("Please check the errors above.")
        print("=" * 60)
        sys.exit(1)

if __name__ == "__main__":
    main()
