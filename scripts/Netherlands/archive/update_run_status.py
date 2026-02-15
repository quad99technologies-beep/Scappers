#!/usr/bin/env python3
"""
Update Netherlands Run Status to Stopped
Changes all Netherlands run_id statuses to 'stopped'
"""

import sys
import os

# Add parent directories to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from core.db.postgres_connection import get_db

def update_run_status_to_stopped():
    """Update all Netherlands runs to stopped status"""
    print("=" * 80)
    print("UPDATE NETHERLANDS RUN STATUS TO STOPPED")
    print("=" * 80)
    print()
    
    db = get_db("Netherlands")
    
    try:
        # First, show current status
        with db.cursor() as cur:
            cur.execute("""
                SELECT run_id, status, mode
                FROM run_ledger
                WHERE run_id LIKE 'nl_%'
                ORDER BY started_at DESC
                LIMIT 20
            """)
            rows = cur.fetchall()
            
            if rows:
                print(f"Found {len(rows)} Netherlands runs")
                print()
                print("Current Status:")
                print("-" * 80)
                print(f"{'Run ID':<30} {'Current Status':<20} {'Mode':<20}")
                print("-" * 80)
                for run_id, status, mode in rows:
                    print(f"{run_id:<30} {status or 'NULL':<20} {mode or 'N/A':<20}")
                print()
            else:
                print("No Netherlands runs found in run_ledger")
                print()
                return
        
        # Update all to stopped
        print("Updating all statuses to 'stopped'...")
        print()
        
        with db.cursor() as cur:
            cur.execute("""
                UPDATE run_ledger
                SET status = 'stopped'
                WHERE run_id LIKE 'nl_%'
            """)
            updated_count = cur.rowcount
            db.commit()
            
            print(f"[OK] Updated {updated_count} run(s) to status='stopped'")
        
        # Show updated status
        print()
        print("Updated Status:")
        print("-" * 80)
        
        with db.cursor() as cur:
            cur.execute("""
                SELECT run_id, status, mode
                FROM run_ledger
                WHERE run_id LIKE 'nl_%'
                ORDER BY started_at DESC
                LIMIT 20
            """)
            rows = cur.fetchall()
            
            print(f"{'Run ID':<30} {'New Status':<20} {'Mode':<20}")
            print("-" * 80)
            for run_id, status, mode in rows:
                print(f"{run_id:<30} {status or 'NULL':<20} {mode or 'N/A':<20}")
        
        print()
        print("=" * 80)
        print("UPDATE COMPLETE")
        print("=" * 80)
        print()
        
    except Exception as e:
        print(f"[ERROR] Failed to update run status: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    update_run_status_to_stopped()
