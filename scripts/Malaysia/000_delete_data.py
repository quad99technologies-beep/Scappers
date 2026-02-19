
import sys
import os
from pathlib import Path

# Add script directory to path for config_loader import
_script_dir = Path(__file__).resolve().parent

# Add repo root to path for shared imports
_repo_root = _script_dir.parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Ensure Malaysia directory is at the front of sys.path to prioritize local 'db' package
sys.path = [p for p in sys.path if not Path(p).name == 'core']
if str(_script_dir) in sys.path:
    sys.path.remove(str(_script_dir))
sys.path.insert(0, str(_script_dir))

# Force re-import of db module if it was incorrectly loaded from core/db
if 'db' in sys.modules:
    del sys.modules['db']

from core.db.connection import CountryDB

def main():
    print("Deleting all Malaysia data (keeping PCID reference)...")
    db = CountryDB("Malaysia")
    
    tables_to_truncate = [
        "my_products", 
        "my_product_details", 
        "my_consolidated_products", 
        "my_reimbursable_drugs", 
        "my_pcid_mappings", 
        "my_step_progress", 
        "my_bulk_search_counts", 
        "my_export_reports", 
        "my_errors",
        # "my_pcid_reference"  <-- DATA KEPT (Dictionary equivalent)
    ]
    
    for table in tables_to_truncate:
        try:
            print(f"Truncating {table}...")
            # CASCADE is important to handle foreign keys
            db.execute(f"TRUNCATE TABLE {table} CASCADE")
        except Exception as e:
            print(f"Error truncating {table}: {e}")
            
    print("Data deletion complete.")

if __name__ == "__main__":
    main()
