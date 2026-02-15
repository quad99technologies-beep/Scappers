#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Initialize Belarus Database Schema

This script applies the Belarus database schema to PostgreSQL.
Run this if tables are not showing in the Output Browser.
"""

import sys
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add scripts/Belarus to path
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))


def main():
    print("=" * 60)
    print("Belarus Database Schema Initialization")
    print("=" * 60)
    print()
    
    try:
        print("[1/3] Connecting to PostgreSQL...")
        from core.db.connection import CountryDB
        db = CountryDB("Belarus")
        print("[OK] Connected to PostgreSQL")
        print()
        
        print("[2/3] Applying Belarus schema...")
        from db.schema import apply_belarus_schema
        apply_belarus_schema(db)
        print("[OK] Schema applied successfully")
        print()
        
        print("[3/3] Verifying tables...")
        with db.cursor() as cur:
            cur.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE 'by_%'
                ORDER BY table_name
            """)
            tables = [row[0] for row in cur.fetchall()]
            
        if tables:
            print(f"[OK] Found {len(tables)} Belarus tables:")
            for table in tables:
                print(f"     - {table}")
        else:
            print("[WARN] No Belarus tables found!")
        
        print()
        print("=" * 60)
        print("Database initialization complete!")
        print("=" * 60)
        return 0
        
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
