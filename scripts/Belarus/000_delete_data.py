
import sys
import os
from pathlib import Path

# Add script directory to path for config_loader import
_script_dir = Path(__file__).resolve().parent

# Add repo root to path for shared imports
_repo_root = _script_dir.parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Ensure Belarus directory is at the front of sys.path to prioritize local 'db' package
sys.path = [p for p in sys.path if not Path(p).name == 'core']
if str(_script_dir) in sys.path:
    sys.path.remove(str(_script_dir))
sys.path.insert(0, str(_script_dir))

# Force re-import of db module if it was incorrectly loaded from core/db
if 'db' in sys.modules:
    del sys.modules['db']

from core.db.connection import CountryDB

def main():
    print("Deleting all Belarus data (keeping input dictionary)...")
    db = CountryDB("Belarus")
    
    tables_to_truncate = [
        "by_rceth_data",
        "by_pcid_mappings",
        "by_step_progress",
        "by_export_reports",
        "by_final_output",
        "by_errors",
        "by_translation_cache",
        "by_translated_data"
    ]
    
    for table in tables_to_truncate:
        try:
            # Check if table exists before truncating
            with db.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    )
                """, (table,))
                exists = cur.fetchone()[0]
            
            if exists:
                print(f"Truncating {table}...")
                db.execute(f"TRUNCATE TABLE {table} CASCADE")
            else:
                print(f"Skipping {table} (does not exist)")
        except Exception as e:
            print(f"Error truncating {table}: {e}")
            
    print("Data deletion complete.")

if __name__ == "__main__":
    main()
