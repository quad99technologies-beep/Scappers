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

# Tables that have foreign key references to run_ledger
fk_tables = [
    'http_requests',
    'data_quality_checks',
    'scrape_stats_snapshots',
    'pipeline_checkpoints',
    'frontier_queue',
    'artifacts',
    'errors',
    'progress',
]

with conn.cursor() as cur:
    for table in fk_tables:
        try:
            cur.execute(f'DELETE FROM {table} WHERE run_id = %s', (run_id,))
            print(f'[OK] {table}: {cur.rowcount} rows deleted')
        except Exception as e:
            print(f'[SKIP] {table}: {str(e)[:40]}')
    
    # Finally delete from run_ledger
    try:
        cur.execute('DELETE FROM run_ledger WHERE run_id = %s', (run_id,))
        print(f'[OK] run_ledger: {cur.rowcount} rows deleted')
    except Exception as e:
        print(f'[ERR] run_ledger: {e}')

conn.close()
print('[DONE]')
