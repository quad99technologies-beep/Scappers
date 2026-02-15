#!/usr/bin/env python3
"""
Migration script to add missing fields to nl_packs table
Run this before running the updated scraper
"""

import sys
from pathlib import Path

# Force UTF-8 output to prevent Charmap codec errors on Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

# Add script directory to path for config_loader import
_script_dir = Path(__file__).resolve().parent.parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

# Add repo root to path for core imports
_repo_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.db.postgres_connection import get_db

def run_migration():
    """Add missing fields to nl_packs table"""
    print("[MIGRATION] Connecting to Netherlands database...")
    db = get_db("Netherlands")

    print("[MIGRATION] Adding missing fields to nl_packs table...")

    migrations = [
        ("active_substance", "TEXT"),
        ("manufacturer", "TEXT"),
        ("deductible", "NUMERIC(12,4)")
    ]

    with db.cursor() as cur:
        for column_name, column_type in migrations:
            # Check if column exists
            cur.execute("""
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_name = 'nl_packs' AND column_name = %s
            """, (column_name,))

            exists = cur.fetchone()[0] > 0

            if not exists:
                print(f"[MIGRATION] Adding column '{column_name}' ({column_type})...")
                cur.execute(f"ALTER TABLE nl_packs ADD COLUMN {column_name} {column_type}")
                db.commit()
                print(f"[MIGRATION] OK | Added column '{column_name}'")
            else:
                print(f"[MIGRATION] SKIP | Column '{column_name}' already exists")

    print("[MIGRATION] SUCCESS | Migration complete!")
    print()
    print("You can now run the scraper with the updated fields:")
    print("  python run_pipeline_resume.py")

if __name__ == "__main__":
    try:
        run_migration()
    except Exception as e:
        print(f"[ERROR] Migration failed: {e}")
        sys.exit(1)
