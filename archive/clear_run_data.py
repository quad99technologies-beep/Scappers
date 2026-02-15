#!/usr/bin/env python3
"""Clear all data for the current run to start fresh."""

import sys
sys.path.insert(0, r'D:\quad99\Scrappers')

from core.db.connection import CountryDB

run_id = "20260210_080716_dfbbcfa5"

db = CountryDB('Tender_Chile')
db.connect()

# Clear all step data for this run
tables = [
    'tc_tender_redirects',
    'tc_tender_details', 
    'tc_tender_awards',
    'tc_final_output',
    'tc_step_progress'
]

with db.cursor() as cur:
    for table in tables:
        cur.execute(f"DELETE FROM {table} WHERE run_id = %s", (run_id,))
        print(f"[OK] Cleared {table}: {cur.rowcount} rows deleted")

print(f"\n[OK] All data cleared for run_id: {run_id}")
