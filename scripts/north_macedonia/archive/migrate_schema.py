#!/usr/bin/env python3
"""
Migration script to update North Macedonia database schema.
Handles transition from old schema to new modernized schema.
"""

import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_repo_root))

_script_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(_script_dir))

from core.db import get_db

def migrate_schema():
    """Migrate from old schema to new schema."""
    print("="*60)
    print("NORTH MACEDONIA SCHEMA MIGRATION")
    print("="*60)
    print()
    
    db = get_db("NorthMacedonia")
    
    with db.cursor() as cur:
        # Check if old schema exists
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name='nm_drug_register')")
        table_exists = cur.fetchone()[0]
        
        if table_exists:
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='nm_drug_register'")
            columns = [r[0] for r in cur.fetchall()]
            
            # Check if it's the old schema (missing url_id)
            if 'url_id' not in columns:
                print("[INFO] Old schema detected. Backing up and recreating tables...")
                
                # Backup old tables
                backup_tables = [
                    'nm_drug_register',
                    'nm_urls',
                    'nm_pcid_mappings',
                    'nm_final_output',
                    'nm_step_progress',
                    'nm_export_reports',
                    'nm_errors'
                ]
                
                for table in backup_tables:
                    cur.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name='{table}')")
                    if cur.fetchone()[0]:
                        backup_name = f"{table}_backup_{int(__import__('time').time())}"
                        print(f"[BACKUP] Renaming {table} to {backup_name}")
                        cur.execute(f"ALTER TABLE IF EXISTS {table} RENAME TO {backup_name}")
                
                db.commit()
                print("[OK] Old tables backed up")
            else:
                print("[INFO] Schema is already up to date")
                return
        else:
            print("[INFO] No existing tables - fresh installation")
    
    # Apply new schema
    print("\n[SCHEMA] Applying new modernized schema...")
    from db import apply_schema
    apply_schema(db)
    
    print("\n[SUCCESS] Migration complete!")
    print("="*60)

if __name__ == "__main__":
    try:
        migrate_schema()
    except Exception as e:
        print(f"\n[ERROR] Migration failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
