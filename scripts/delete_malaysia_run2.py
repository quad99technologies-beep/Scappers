#!/usr/bin/env python3
import os
import sys
_repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

import psycopg2
from dotenv import load_dotenv
load_dotenv(os.path.join(_repo_root, '.env'))

conn = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST', 'localhost'),
    port=int(os.getenv('POSTGRES_PORT', '5432')),
    database=os.getenv('POSTGRES_DB', 'pharma_db'),
    user=os.getenv('POSTGRES_USER', 'postgres'),
    password=os.getenv('POSTGRES_PASSWORD', '')
)
conn.autocommit = True

run_id = 'Malaysia_20260216_051417'

with conn.cursor() as cur:
    # Delete http_requests first
    cur.execute('DELETE FROM http_requests WHERE run_id = %s', (run_id,))
    print(f'[OK] Deleted from http_requests: {cur.rowcount} rows')
    
    # Then delete from run_ledger
    cur.execute('DELETE FROM run_ledger WHERE run_id = %s', (run_id,))
    print(f'[OK] Deleted from run_ledger: {cur.rowcount} rows')

conn.close()
print('[DONE] Run deleted successfully!')
