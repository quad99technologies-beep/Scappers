import os
import sys
from pathlib import Path

# Add repo root to path
_repo_root = Path(r"d:\quad99\Scrappers")
sys.path.insert(0, str(_repo_root))
sys.path.insert(0, str(_repo_root / "scripts" / "Russia"))

from core.db.connection import CountryDB
from db.repositories import RussiaRepository

def check():
    db = CountryDB("Russia")
    # Get latest run_id
    with db.cursor() as cur:
        cur.execute("SELECT run_id FROM run_ledger ORDER BY started_at DESC LIMIT 1")
        run_id = cur.fetchone()[0]
    
    repo = RussiaRepository(db, run_id)
    print(f"Checking run_id: {run_id}")
    
    # Force UTF-8 for printing
    import sys
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

    # Check VED products for Zenlistik
    with db.cursor() as cur:
        cur.execute("SELECT item_id, tn, inn, manufacturer_country, registered_price_rub, start_date_text FROM ru_ved_products WHERE tn ILIKE '%Zenlistik%' OR tn ILIKE '%Зенлистик%' LIMIT 5")
        rows = cur.fetchall()
        print(f"Found {len(rows)} Zenlistik rows in ru_ved_products")
        for row in rows:
            print(f"VED: item_id={row[0]} | tn={row[1]} | inn={row[2]} | price={row[4]} | date={row[5]}")
            
    # Check translated products
    with db.cursor() as cur:
        cur.execute("SELECT item_id, tn_en, inn_en, manufacturer_country_en, registered_price_rub, start_date_text FROM ru_translated_products WHERE tn_ru ILIKE '%Zenlistik%' OR tn_ru ILIKE '%Зенлистик%' LIMIT 5")
        rows = cur.fetchall()
        print(f"Found {len(rows)} Zenlistik rows in ru_translated_products")
        for row in rows:
            hex_tn = row[1].encode('utf-8').hex()
            print(f"TRANS: item_id={row[0]} | tn_en={row[1]} (hex:{hex_tn}) | inn_en={row[2]} | price={row[4]}")

if __name__ == "__main__":
    check()
