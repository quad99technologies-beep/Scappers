#!/usr/bin/env python3
"""
Update run_ledger status from 'stopped' to 'running' for a specific run_id.
"""

import sys
from pathlib import Path

# Add repo root to path
repo_root = Path(__file__).resolve().parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from core.db.connection import CountryDB

def update_run_status():
    run_id = '20260206_160604_5d97a684'
    
    try:
        with CountryDB("Argentina") as db:
            with db.cursor() as cur:
                # Check current status
                cur.execute(
                    "SELECT status FROM run_ledger WHERE run_id = %s AND scraper_name = %s",
                    (run_id, "Argentina")
                )
                row = cur.fetchone()
                if not row:
                    print(f"ERROR: Run ID {run_id} not found in run_ledger for Argentina")
                    return False
                
                current_status = row[0]
                print(f"Current status: {current_status}")
                
                # Update status to 'running'
                cur.execute(
                    "UPDATE run_ledger SET status = 'running' WHERE run_id = %s AND scraper_name = %s",
                    (run_id, "Argentina")
                )
                
                if cur.rowcount > 0:
                    db.commit()
                    print(f"SUCCESS: Updated run_id {run_id} status from '{current_status}' to 'running'")
                    
                    # Verify the update
                    cur.execute(
                        "SELECT run_id, scraper_name, status, started_at, ended_at FROM run_ledger WHERE run_id = %s",
                        (run_id,)
                    )
                    result = cur.fetchone()
                    if result:
                        print(f"\nVerification:")
                        print(f"  run_id: {result[0]}")
                        print(f"  scraper_name: {result[1]}")
                        print(f"  status: {result[2]}")
                        print(f"  started_at: {result[3]}")
                        print(f"  ended_at: {result[4]}")
                    return True
                else:
                    print(f"WARNING: No rows updated. Run ID might not exist or already has status 'running'")
                    return False
                    
    except Exception as e:
        print(f"ERROR: Failed to update run status: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = update_run_status()
    sys.exit(0 if success else 1)
