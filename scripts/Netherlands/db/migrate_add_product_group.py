#!/usr/bin/env python3
"""
Migration script to add product_group column to nl_packs table.
Run this if you get 'column "product_group" of relation "nl_packs" does not exist' error.
"""

import sys
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.db.postgres_connection import get_db


def migrate_add_product_group():
    """Add product_group column to nl_packs table."""
    print("[MIGRATION] Adding product_group column to nl_packs table...")
    
    try:
        db = get_db("Netherlands")
        
        with db.cursor() as cur:
            # Check if column already exists
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'nl_packs' AND column_name = 'product_group'
            """)
            
            if cur.fetchone():
                print("[MIGRATION] Column 'product_group' already exists. Skipping.")
                return
            
            # Add the column
            cur.execute("""
                ALTER TABLE nl_packs 
                ADD COLUMN product_group TEXT
            """)
            
            db.commit()
            print("[MIGRATION] Successfully added 'product_group' column to nl_packs table.")
            
    except Exception as e:
        print(f"[MIGRATION ERROR] {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    migrate_add_product_group()
