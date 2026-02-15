#!/usr/bin/env python3
"""
Drop nm_drug_register_backup_* tables

This script drops any backup tables created for nm_drug_register.
Backup tables are named: nm_drug_register_backup_<timestamp>
"""

import sys
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.db.connection import CountryDB


def drop_backup_tables():
    """Drop all nm_drug_register_backup_* tables."""
    print("[DB] Dropping nm_drug_register_backup_* tables...")
    
    try:
        with CountryDB("NorthMacedonia") as db:
            with db.cursor() as cur:
                # Find all backup tables
                cur.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name LIKE 'nm_drug_register_backup_%'
                """)
                backup_tables = [row[0] for row in cur.fetchall()]
                
                if not backup_tables:
                    print("[DB] No backup tables found.")
                    return
                
                print(f"[DB] Found {len(backup_tables)} backup table(s):")
                for table in backup_tables:
                    print(f"  - {table}")
                
                # Drop each backup table
                dropped = []
                for table in backup_tables:
                    try:
                        cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                        dropped.append(table)
                        print(f"[DB] Dropped: {table}")
                    except Exception as e:
                        print(f"[DB] Error dropping {table}: {e}")
                
                db.commit()
                print(f"[DB] Successfully dropped {len(dropped)} backup table(s).")
                
    except Exception as e:
        print(f"[DB] Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    drop_backup_tables()
