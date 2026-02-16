#!/usr/bin/env python3
"""
Check SARVAL/SAROMET product status in database
"""

import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from core.db.connection import CountryDB

def check_sarval_status():
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
    
    # Check SARVAL/SAROMET products
    with db.cursor() as cur:
        cur.execute("""
            SELECT id, product, company, loop_count, total_records, status, 
                   scraped_by_selenium, scraped_by_api, url
            FROM ar_product_index
            WHERE run_id = %s
            AND (product ILIKE '%%SARVAL%%' OR product ILIKE '%%SAROMET%%')
            ORDER BY product
        """, (run_id,))
        
        rows = cur.fetchall()
        print(f"Found {len(rows)} SARVAL/SAROMET products:")
        print("-" * 120)
        for row in rows:
            print(f"ID: {row[0]}")
            print(f"  Product: {row[1]}")
            print(f"  Company: {row[2]}")
            print(f"  Loop Count: {row[3]}")
            print(f"  Total Records: {row[4]}")
            print(f"  Status: {row[5]}")
            print(f"  Scraped by Selenium: {row[6]}")
            print(f"  Scraped by API: {row[7]}")
            print(f"  URL: {row[8][:60] if row[8] else 'None'}...")
            print()
        
        # Check if they would be eligible
        print("\n" + "=" * 60)
        print("ELIGIBILITY CHECK (max_loop=5):")
        print("=" * 60)
        
        cur.execute("""
            SELECT COUNT(*) 
            FROM ar_product_index
            WHERE run_id = %s
            AND (product ILIKE '%%SARVAL%%' OR product ILIKE '%%SAROMET%%')
            AND total_records = 0
            AND loop_count < 5
            AND status IN ('pending','failed','in_progress')
        """, (run_id,))
        
        eligible = cur.fetchone()[0]
        print(f"Eligible for Selenium (total_records=0, loop_count<5): {eligible}")
    
    db.close()

if __name__ == "__main__":
    check_sarval_status()
