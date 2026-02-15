#!/usr/bin/env python3
"""
Reset a run to start fresh while keeping the same run_id.
This clears the nl_packs data but keeps the collected URLs.
"""

import sys
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parent.parent.parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

import os

# Get run_id from environment or .current_run_id file
run_id = os.environ.get("NL_RUN_ID")
if not run_id:
    current_run_id_file = Path("output/Netherlands/.current_run_id")
    if current_run_id_file.exists():
        run_id = current_run_id_file.read_text().strip()

if not run_id:
    print("[ERROR] No run_id found. Set NL_RUN_ID or check .current_run_id file.")
    sys.exit(1)

print(f"[RESET] Resetting run: {run_id}")

from core.db.postgres_connection import get_db

try:
    db = get_db("Netherlands")
    
    with db.cursor() as cur:
        # Delete packs data for this run
        cur.execute("DELETE FROM nl_packs WHERE run_id = %s", (run_id,))
        packs_deleted = cur.rowcount
        
        # Delete collected_urls data for this run
        cur.execute("DELETE FROM nl_collected_urls WHERE run_id = %s", (run_id,))
        urls_deleted = cur.rowcount
        
        # Update run_ledger status
        cur.execute("""
            UPDATE run_ledger 
            SET status = 'running', items_scraped = 0, error_message = NULL
            WHERE run_id = %s
        """, (run_id,))
        
        db.commit()
        
        print(f"[RESET] Deleted {packs_deleted} packs")
        print(f"[RESET] Deleted {urls_deleted} collected URLs")
        print(f"[RESET] Run status reset to 'running'")
        print(f"[RESET] Ready to restart scraping")
        
except Exception as e:
    print(f"[RESET ERROR] {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
