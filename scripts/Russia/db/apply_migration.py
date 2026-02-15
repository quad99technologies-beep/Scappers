#!/usr/bin/env python3
"""
Apply migration to add unique constraint to ru_input_dictionary table.
"""
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from core.db.postgres_connection import PostgresDB

def apply_migration():
    """Add unique constraint to ru_input_dictionary table."""
    db = PostgresDB(country='russia')
    
    try:
        # First, remove duplicates (keep the one with smallest id)
        print("[INFO] Removing duplicate entries...")
        delete_sql = """
            DELETE FROM ru_input_dictionary a
            USING ru_input_dictionary b
            WHERE a.id > b.id 
              AND a.source_term = b.source_term 
              AND a.language_from = b.language_from 
              AND a.language_to = b.language_to
        """
        with db.cursor() as cur:
            cur.execute(delete_sql)
        db.commit()
        print("[SUCCESS] Duplicates removed (if any)")
        
        # Check if constraint already exists
        check_sql = """
            SELECT 1 FROM pg_constraint 
            WHERE conname = 'ru_input_dictionary_source_from_to_unique'
        """
        with db.cursor() as cur:
            cur.execute(check_sql)
            exists = cur.fetchone()
        
        if exists:
            print("[INFO] Unique constraint already exists, skipping...")
        else:
            # Add the unique constraint
            print("[INFO] Adding unique constraint...")
            alter_sql = """
                ALTER TABLE ru_input_dictionary 
                ADD CONSTRAINT ru_input_dictionary_source_from_to_unique 
                UNIQUE (source_term, language_from, language_to)
            """
            with db.cursor() as cur:
                cur.execute(alter_sql)
            db.commit()
            print("[SUCCESS] Unique constraint added successfully!")
        
        print("\n[MIGRATION COMPLETE] The ON CONFLICT clause will now work correctly.")
        return True
        
    except Exception as e:
        db.rollback()
        print(f"[ERROR] Migration failed: {e}")
        return False
    finally:
        db.close()

if __name__ == "__main__":
    success = apply_migration()
    sys.exit(0 if success else 1)
