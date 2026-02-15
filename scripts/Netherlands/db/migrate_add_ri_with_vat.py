#!/usr/bin/env python3
"""
Migration script to add ri_with_vat column to nl_packs table.
"""

import sys
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.db.postgres_connection import get_db


def migrate_add_ri_with_vat():
    """Add ri_with_vat column to nl_packs table."""
    print("[MIGRATION] Adding ri_with_vat column to nl_packs table...")
    
    try:
        db = get_db("Netherlands")
        
        with db.cursor() as cur:
            # Check if column already exists
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'nl_packs' AND column_name = 'ri_with_vat'
            """)
            
            if cur.fetchone():
                print("[MIGRATION] Column 'ri_with_vat' already exists. Skipping.")
                return
            
            # Add the column
            cur.execute("""
                ALTER TABLE nl_packs 
                ADD COLUMN ri_with_vat NUMERIC(12,4)
            """)
            
            db.commit()
            print("[MIGRATION] Successfully added 'ri_with_vat' column to nl_packs table.")
            
    except Exception as e:
        print(f"[MIGRATION ERROR] {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    migrate_add_ri_with_vat()
