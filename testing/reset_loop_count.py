#!/usr/bin/env python3
"""
Reset loop_count for products with zero total_records
This allows them to be retried by Selenium or moved to API
"""

import sys
from pathlib import Path

# Add repo root to path
repo_root = Path(__file__).resolve().parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from core.db.connection import CountryDB

def reset_loop_count(scraper_name: str = "Argentina"):
    """Reset loop_count to 1 for all products with total_records = 0"""
    
    db = CountryDB(scraper_name)
    
    # Get current run_id
    run_id_file = Path(__file__).parent / "output" / scraper_name / ".current_run_id"
    if run_id_file.exists():
        run_id = run_id_file.read_text().strip()
    else:
        # Get latest run_id from DB
        with db.cursor() as cur:
            cur.execute("SELECT run_id FROM ar_product_index ORDER BY created_at DESC LIMIT 1")
            result = cur.fetchone()
            run_id = result[0] if result else None
    
    if not run_id:
        print("ERROR: Could not determine run_id")
        return False
    
    print(f"Using run_id: {run_id}")
    
    # Find products with total_records = 0
    with db.cursor() as cur:
        cur.execute("""
            SELECT id, product, company, loop_count, total_records, status
            FROM ar_product_index
            WHERE run_id = %s
            AND COALESCE(total_records, 0) = 0
            ORDER BY product, company
        """, (run_id,))
        
        rows = cur.fetchall()
        print(f"\nFound {len(rows)} products with total_records = 0")
        
        if not rows:
            print("No products to reset.")
            return True
        
        print("\nProducts to reset:")
        for row in rows[:20]:  # Show first 20
            print(f"  ID {row[0]}: {row[1]} | {row[2]} | loop_count={row[3]} | status={row[5]}")
        
        if len(rows) > 20:
            print(f"  ... and {len(rows) - 20} more")
    
    # Reset loop_count to 1 for all these products
    confirm = input(f"\nReset loop_count to 1 for all {len(rows)} products? (yes/no): ")
    if confirm.lower() != 'yes':
        print("Cancelled.")
        return False
    
    with db.cursor() as cur:
        cur.execute("""
            UPDATE ar_product_index
            SET loop_count = 1,
                status = 'pending',
                scraped_by_selenium = FALSE,
                scraped_by_api = FALSE,
                error_message = NULL,
                updated_at = NOW()
            WHERE run_id = %s
            AND COALESCE(total_records, 0) = 0
        """, (run_id,))
        
        updated = cur.rowcount
        print(f"\n✓ Reset {updated} products")
        print("  - loop_count = 1")
        print("  - status = pending")
        print("  - scraped_by_selenium = FALSE")
        print("  - scraped_by_api = FALSE")
    
    db.close()
    return True

if __name__ == "__main__":
    success = reset_loop_count("Argentina")
    sys.exit(0 if success else 1)
