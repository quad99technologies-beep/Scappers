
import sys
import psycopg2
from pathlib import Path


_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.db.connection import CountryDB

def kill_locks():
    print("Killing locks for Italy tables...")
    with CountryDB("Italy") as db:
        with db.cursor() as cur:
            # Terminate other sessions accessing 'it_%' tables
            cur.execute("""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE pid <> pg_backend_pid()
                AND datname = current_database()
                AND query LIKE '%it_%'
            """)
            print(f"Terminated sessions.")

if __name__ == "__main__":
    kill_locks()
