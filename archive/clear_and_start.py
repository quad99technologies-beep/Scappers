#!/usr/bin/env python3
"""Clear existing run data and start fresh pipeline."""

import sys
sys.path.insert(0, r'D:\quad99\Scrappers')

import os
import time
import subprocess
from pathlib import Path
from core.db.connection import CountryDB

# Generate new run ID
new_run_id = f"run_{time.strftime('%Y%m%d_%H%M%S')}"
print(f"[INFO] New Run ID: {new_run_id}")

# Clear old run data from DB
db = CountryDB('Tender_Chile')
db.connect()

with db.cursor() as cur:
    # Get list of old runs to clean up (check all runs except the most recent)
    cur.execute("""
        SELECT run_id FROM run_ledger 
        WHERE run_id != %s
        AND status IN ('running', 'failed', 'completed')
    """, (new_run_id,))
    old_runs = [r[0] for r in cur.fetchall()]
    
    for run_id in old_runs[:5]:  # Limit to 5 most recent old runs
        print(f"[CLEAN] Clearing old run: {run_id}")
        for table in ['tc_tender_redirects', 'tc_tender_details', 'tc_tender_awards', 
                      'tc_final_output', 'tc_step_progress']:
            cur.execute(f"DELETE FROM {table} WHERE run_id = %s", (run_id,))
        cur.execute("DELETE FROM run_ledger WHERE run_id = %s", (run_id,))
    
    db.commit()
    print(f"[OK] Cleared {len(old_runs[:5])} old runs")

db.close()

# Set environment variable
os.environ["TENDER_CHILE_RUN_ID"] = new_run_id

print("\n" + "="*80)
print("STARTING FRESH PIPELINE RUN")
print("="*80)
print(f"Run ID: {new_run_id}")
print(f"Max Tenders: 6000")
print(f"Step 2: Parallel processing (4 workers)")
print()

# Start the pipeline
script_dir = Path(r'D:\quad99\Scrappers\scripts\Tender- Chile')
os.chdir(script_dir)

result = subprocess.run(
    ["python", "run_pipeline_resume.py", "--fresh"],
    capture_output=False
)

sys.exit(result.returncode)
