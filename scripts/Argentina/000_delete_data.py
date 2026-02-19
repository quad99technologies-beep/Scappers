
import sys
import os
from pathlib import Path

# Add script directory to path for config_loader import
_script_dir = Path(__file__).resolve().parent

# Add repo root to path for shared imports
_repo_root = _script_dir.parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Ensure Argentina directory is at the front of sys.path to prioritize local 'db' package
sys.path = [p for p in sys.path if not Path(p).name == 'core']
if str(_script_dir) in sys.path:
    sys.path.remove(str(_script_dir))
sys.path.insert(0, str(_script_dir))

# Force re-import of db module if it was incorrectly loaded from core/db
if 'db' in sys.modules:
    del sys.modules['db']

from core.db.connection import CountryDB

def main():
    print("Deleting all Argentina data (keeping dictionary)...")
    db = CountryDB("Argentina")
    
    tables_to_truncate = [
        "ar_product_index", 
        "ar_products", 
        "ar_products_translated", 
        "ar_errors", 
        "ar_step_progress", 
        "ar_scrape_stats", 
        "ar_export_reports", 
        "ar_artifacts", 
        "ar_translation_cache"
    ]
    
    for table in tables_to_truncate:
        try:
            print(f"Truncating {table}...")
            db.execute(f"TRUNCATE TABLE {table} CASCADE")
        except Exception as e:
            print(f"Error truncating {table}: {e}")
            
    print("Data deletion complete.")

if __name__ == "__main__":
    main()
