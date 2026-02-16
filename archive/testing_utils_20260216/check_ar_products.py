#!/usr/bin/env python3
"""
Check if SARVAL/SAROMET products are in ar_products table
"""

import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from core.db.connection import CountryDB

def check_ar_products():
    db = CountryDB("Argentina")
    
    # Get current run_id
    run_id_file = Path(__file__).parent / "output" / "Argentina" / ".current_run_id"
    if run_id_file.exists():
        run_id = run_id_file.read_text().strip()
    else:
        with db.cursor() as cur:
            cur.execute("SELECT run_id FROM ar_product_index ORDER BY created_at DESC LIMIT 1")
            result = cur.fetchone()
            run_id = result[0] if result else None
    
    print(f"Using run_id: {run_id}")
    print()
    
    # Check if SARVAL/SAROMET products are in ar_products
    with db.cursor() as cur:
        cur.execute("""
            SELECT input_company, input_product_name, COUNT(*) as record_count
            FROM ar_products
            WHERE run_id = %s
            AND (input_product_name ILIKE '%%SARVAL%%' OR input_product_name ILIKE '%%SAROMET%%')
            GROUP BY input_company, input_product_name
            ORDER BY input_product_name
        """, (run_id,))
        
        rows = cur.fetchall()
        if rows:
            print(f"Found {len(rows)} SARVAL/SAROMET products in ar_products:")
            for row in rows:
                print(f"  {row[0]} | {row[1]} | {row[2]} records")
        else:
            print("No SARVAL/SAROMET products found in ar_products table.")
            print("\nThis means they should NOT be in skip_set.")
    
    # Check the skip_set calculation
    print("\n" + "=" * 60)
    print("Checking skip_set calculation:")
    print("=" * 60)
    
    with db.cursor() as cur:
        cur.execute("SELECT COUNT(DISTINCT input_product_name) FROM ar_products WHERE run_id = %s", (run_id,))
        total_products = cur.fetchone()[0]
        print(f"Total unique products in ar_products: {total_products}")
    
    db.close()

if __name__ == "__main__":
    check_ar_products()
