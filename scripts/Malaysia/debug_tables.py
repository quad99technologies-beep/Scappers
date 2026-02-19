import sys
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.db.connection import CountryDB
try:
    with CountryDB('Malaysia') as db:
        with db.cursor() as cur:
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = [row[0] for row in cur.fetchall()]
            print("Tables found:", tables)
            
            # Check for PCID mapping content
            if 'pcid_mapping' in tables:
                cur.execute("SELECT count(*) FROM pcid_mapping")
                print(f"Total pcid_mapping count: {cur.fetchone()[0]}")
                
                cur.execute("SELECT count(*) FROM pcid_mapping WHERE source_country = 'Malaysia'")
                print(f"Malaysia pcid_mapping count: {cur.fetchone()[0]}")
                
                cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'pcid_mapping'")
                print("pcid_mapping columns:", [row[0] for row in cur.fetchall()])
            
            if 'my_pcid_reference' in tables:
                cur.execute("SELECT count(*) FROM my_pcid_reference")
                print(f"my_pcid_reference count: {cur.fetchone()[0]}")

except Exception as e:
    print(f"Error: {e}")
