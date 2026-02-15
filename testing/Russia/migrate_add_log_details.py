#!/usr/bin/env python3
"""
Migration script to add log_details and metrics columns to ru_step_progress table.
Run this if you have an existing database and want to capture detailed metrics.
"""

import sys
import os

# Add parent directories to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.db.connection import CountryDB


def column_exists(db, table, column):
    """Check if a column exists in a table."""
    with db.cursor() as cur:
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s AND column_name = %s
        """, (table, column))
        return cur.fetchone() is not None


def migrate_add_columns():
    """Add metrics columns to ru_step_progress table."""
    db = CountryDB("Russia")
    
    columns_to_add = [
        ("log_details", "TEXT"),
        ("rows_found", "INTEGER DEFAULT 0"),
        ("ean_found", "INTEGER DEFAULT 0"),
        ("rows_scraped", "INTEGER DEFAULT 0"),
        ("rows_inserted", "INTEGER DEFAULT 0"),
        ("ean_missing", "INTEGER DEFAULT 0"),
        ("db_count_before", "INTEGER DEFAULT 0"),
        ("db_count_after", "INTEGER DEFAULT 0"),
    ]
    
    try:
        with db.cursor() as cur:
            for column, data_type in columns_to_add:
                if not column_exists(db, "ru_step_progress", column):
                    cur.execute(f"""
                        ALTER TABLE ru_step_progress 
                        ADD COLUMN {column} {data_type}
                    """)
                    print(f"Added column '{column}' ({data_type})")
                else:
                    print(f"Column '{column}' already exists")
            
        db.commit()
        print("\nMigration completed successfully!")
        
    except Exception as e:
        print(f"Migration failed: {e}")
        db.rollback()
        raise


if __name__ == "__main__":
    migrate_add_columns()
