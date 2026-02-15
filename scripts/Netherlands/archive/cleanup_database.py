#!/usr/bin/env python3
"""
Netherlands Database Cleanup Script
Removes old data and unused tables
"""

import sys
import os
from datetime import datetime

# Add parent directories to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from core.db.postgres_connection import get_db

def cleanup_old_data():
    """Remove all old data from Netherlands tables"""
    print("=" * 80)
    print("NETHERLANDS DATABASE CLEANUP")
    print("=" * 80)
    print()
    
    db = get_db("Netherlands")
    
    # Tables to clean (keep structure, delete data)
    tables_to_clean = [
        'nl_collected_urls',
        'nl_packs',
        'nl_consolidated',
        'nl_chrome_instances',
        'nl_errors',
    ]
    
    # Tables to drop completely (unused)
    tables_to_drop = [
        'nl_search_combinations',
        'nl_details',
        'nl_costs',
        'nl_products',
        'nl_reimbursement',
        'nl_step_progress',
        'nl_export_reports',
    ]
    
    print("[CLEANUP] Step 1: Deleting old data from active tables...")
    print()
    
    for table in tables_to_clean:
        try:
            with db.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count_before = cur.fetchone()[0]
                
                cur.execute(f"DELETE FROM {table}")
                db.commit()
                
                print(f"  [OK] {table}: Deleted {count_before:,} rows")
        except Exception as e:
            print(f"  [WARN] {table}: {e}")
    
    print()
    print("[CLEANUP] Step 2: Dropping unused tables...")
    print()
    
    for table in tables_to_drop:
        try:
            with db.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                db.commit()
                print(f"  [OK] Dropped table: {table}")
        except Exception as e:
            print(f"  [WARN] {table}: {e}")
    
    print()
    print("[CLEANUP] Step 3: Cleaning run_ledger entries...")
    print()
    
    try:
        with db.cursor() as cur:
            # Count Netherlands runs
            cur.execute("""
                SELECT COUNT(*) 
                FROM run_ledger 
                WHERE run_id LIKE 'nl_%'
            """)
            count_before = cur.fetchone()[0]
            
            # Delete Netherlands runs
            cur.execute("""
                DELETE FROM run_ledger 
                WHERE run_id LIKE 'nl_%'
            """)
            db.commit()
            
            print(f"  [OK] run_ledger: Deleted {count_before:,} Netherlands entries")
    except Exception as e:
        print(f"  [WARN] run_ledger: {e}")
    
    print()
    print("[CLEANUP] Step 4: Vacuuming database...")
    print()
    
    try:
        # Vacuum to reclaim space
        db.autocommit = True
        with db.cursor() as cur:
            cur.execute("VACUUM ANALYZE")
        db.autocommit = False
        print("  [OK] Database vacuumed successfully")
    except Exception as e:
        print(f"  [WARN] Vacuum failed: {e}")
    
    print()
    print("=" * 80)
    print("CLEANUP COMPLETE")
    print("=" * 80)
    print()
    print("Database is now clean and ready for fresh scraping!")
    print()


if __name__ == "__main__":
    # Confirm before proceeding
    print()
    print("WARNING: This will DELETE ALL Netherlands data from the database!")
    print()
    print("Tables to clean (data deleted, structure kept):")
    print("  - nl_collected_urls")
    print("  - nl_packs")
    print("  - nl_consolidated")
    print("  - nl_chrome_instances")
    print("  - nl_errors")
    print()
    print("Tables to drop (completely removed):")
    print("  - nl_search_combinations")
    print("  - nl_details")
    print("  - nl_costs")
    print("  - nl_products")
    print("  - nl_reimbursement")
    print("  - nl_step_progress")
    print("  - nl_export_reports")
    print()
    
    response = input("Are you sure you want to proceed? (yes/no): ").strip().lower()
    
    if response == "yes":
        cleanup_old_data()
    else:
        print()
        print("Cleanup cancelled.")
        print()
