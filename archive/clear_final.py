#!/usr/bin/env python3
"""Clear final output data for the current run."""

import sys
sys.path.insert(0, r'D:\quad99\Scrappers')

from core.db.connection import CountryDB

run_id = "20260210_081520_4465bc65"

db = CountryDB('Tender_Chile')
db.connect()

with db.cursor() as cur:
    cur.execute("DELETE FROM tc_final_output WHERE run_id = %s", (run_id,))
    print(f"[OK] Cleared tc_final_output: {cur.rowcount} rows deleted")
