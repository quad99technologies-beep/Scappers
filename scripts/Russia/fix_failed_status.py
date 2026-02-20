
import sys
from pathlib import Path
import os

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.db.connection import CountryDB

def fix_failed_status():
    print("Connecting to Russia DB...")
    try:
        with CountryDB("Russia") as db:
            with db.cursor() as cur:
                # Count before
                cur.execute("SELECT COUNT(*) FROM ru_failed_pages WHERE status = 'failed_permanently'")
                count_before = cur.fetchone()[0]
                print(f"Found {count_before} pages with 'failed_permanently' status.")
                
                if count_before > 0:
                    # Update
                    cur.execute("UPDATE ru_failed_pages SET status = 'pending' WHERE status = 'failed_permanently'")
                    print(f"Converting pages to 'pending'...")
                else:
                    print("No pages to update.")
            
            if count_before > 0:
                db.commit()
                print("Changes committed.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    fix_failed_status()
