#!/usr/bin/env python3
"""Register a new run in the database."""

import sys
sys.path.insert(0, r'D:\quad99\Scrappers')

from core.db.connection import CountryDB
import time

# Create new run ID
new_run_id = f"run_{time.strftime('%Y%m%d_%H%M%S')}"
print(f'New Run ID: {new_run_id}')

# Register in run_ledger
db = CountryDB('Tender_Chile')
db.connect()

with db.cursor() as cur:
    cur.execute('''
        INSERT INTO run_ledger (run_id, scraper_name, status, mode)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (run_id) DO UPDATE SET
            status = EXCLUDED.status,
            started_at = NOW()
    ''', (new_run_id, 'Tender_Chile', 'running', 'fresh'))
    db.commit()
    print(f'[OK] Registered run in ledger')

print(f'Run ID ready: {new_run_id}')
print(f'Export this: set TENDER_CHILE_RUN_ID={new_run_id}')
