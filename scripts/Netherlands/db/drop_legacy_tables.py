#!/usr/bin/env python3
"""
One-off script to DROP legacy/unused Netherlands tables from the database.
This will permanently remove the tables and their data.
"""

import sys
import os
from pathlib import Path

# Add project root to path
repo_root = Path(__file__).resolve().parents[3]
sys.path.append(str(repo_root))

from core.db.postgres_connection import get_db

def drop_legacy_tables():
    print("=" * 60)
    print("DROPPING LEGACY NETHERLANDS TABLES")
    print("=" * 60)
    
    db = get_db("Netherlands")
    
    start_tables = [
        "nl_products",
        "nl_reimbursement",
        "nl_details",
        "nl_costs",
        "nl_consolidated"
    ]
    
    try:
        with db.cursor() as cur:
            for table in start_tables:
                print(f"[DROP] Dropping table {table}...", end="", flush=True)
                try:
                    cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                    print(" OK")
                except Exception as e:
                    print(f" FAILED: {e}")
                    db.rollback()
        
        db.commit()
        print("\n[SUCCESS] Legacy tables dropped.")
        
    except Exception as e:
        print(f"\n[ERROR] Failed to drop tables: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    drop_legacy_tables()
