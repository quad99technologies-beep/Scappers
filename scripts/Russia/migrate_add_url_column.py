#!/usr/bin/env python3
"""Migration script to add url column to ru_step_progress table."""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.db.connection import CountryDB


def migrate_add_url_column():
    db = CountryDB("Russia")
    
    try:
        with db.cursor() as cur:
            # Check if column exists
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'ru_step_progress' 
                AND column_name = 'url'
            """)
            result = cur.fetchone()
            
            if result:
                print("Column 'url' already exists in ru_step_progress table.")
                return
            
            # Add the column
            cur.execute("""
                ALTER TABLE ru_step_progress 
                ADD COLUMN url TEXT
            """)
            
        db.commit()
        print("Successfully added 'url' column to ru_step_progress table.")
        
    except Exception as e:
        print(f"Migration failed: {e}")
        db.rollback()
        raise


if __name__ == "__main__":
    migrate_add_url_column()
