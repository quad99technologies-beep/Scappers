import sys
import os
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parent.parent.parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.db.postgres_connection import get_db

def check_instances():
    try:
        db = get_db("Netherlands") # Or generic connection
    except Exception:
        # Fallback if connection fails
        print("Could not connect via get_db(Netherlands), trying direct/generic logic if needed.")
        return

    print("Checking nl_chrome_instances table...")
    try:
        with db.cursor() as cur:
            cur.execute("SELECT * FROM nl_chrome_instances ORDER BY id DESC LIMIT 5")
            rows = cur.fetchall()
            print(f"Found {len(rows)} instances (showing last 5):")
            for r in rows:
                print(r)
    except Exception as e:
        print(f"Error querying table: {e}")

if __name__ == "__main__":
    check_instances()
