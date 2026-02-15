#!/usr/bin/env python3
"""
Fix loop_count for current Argentina run.
Sets loop_count = 1 for all records in the current run.
"""

import sys
from pathlib import Path

# Add repo root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from core.db.postgres_connection import PostgresDB


def fix_loop_count_for_run(run_id: str = None):
    """Set loop_count = 1 for all records in the specified run."""
    
    db = PostgresDB("Argentina")
    
    with db.cursor() as cur:
        # If no run_id provided, get the latest run
        if not run_id:
            cur.execute("""
                SELECT run_id FROM run_ledger 
                WHERE scraper_name = 'Argentina' 
                ORDER BY started_at DESC 
                LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                run_id = row[0]
            else:
                print("[ERROR] No Argentina run found in run_ledger")
                return
        
        print(f"[INFO] Fixing loop_count for run_id: {run_id}")
        
        # Count records before update
        cur.execute("""
            SELECT COUNT(*), 
                   SUM(CASE WHEN COALESCE(loop_count,0) = 0 THEN 1 ELSE 0 END) as zero_count,
                   SUM(CASE WHEN COALESCE(loop_count,0) = 1 THEN 1 ELSE 0 END) as one_count,
                   SUM(CASE WHEN COALESCE(loop_count,0) > 1 THEN 1 ELSE 0 END) as high_count
            FROM ar_product_index 
            WHERE run_id = %s
        """, (run_id,))
        
        row = cur.fetchone()
        total, zero_count, one_count, high_count = row
        
        print(f"[INFO] Total records: {total}")
        print(f"[INFO] Records with loop_count=0: {zero_count or 0}")
        print(f"[INFO] Records with loop_count=1: {one_count or 0}")
        print(f"[INFO] Records with loop_count>1: {high_count or 0}")
        
        # Update all records to loop_count = 1
        cur.execute("""
            UPDATE ar_product_index 
            SET loop_count = 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE run_id = %s
              AND COALESCE(loop_count,0) != 1
        """, (run_id,))
        
        updated = cur.rowcount
        print(f"[OK] Updated {updated} records to loop_count = 1")
        
        # Verify
        cur.execute("""
            SELECT COUNT(*) 
            FROM ar_product_index 
            WHERE run_id = %s AND loop_count = 1
        """, (run_id,))
        
        final_count = cur.fetchone()[0]
        print(f"[OK] Verification: {final_count} records now have loop_count = 1")
    
    db.close()
    print("[DONE] Loop count fix completed")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Fix loop_count for Argentina run")
    parser.add_argument("--run-id", help="Specific run_id to fix (default: latest)")
    args = parser.parse_args()
    
    fix_loop_count_for_run(args.run_id)
