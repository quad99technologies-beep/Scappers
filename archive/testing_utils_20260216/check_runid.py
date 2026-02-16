import sys
sys.path.insert(0, 'd:/quad99/Scrappers')
sys.path.insert(0, 'd:/quad99/Scrappers/scripts/Argentina')

from core.db.connection import CountryDB
from pathlib import Path

db = CountryDB('Argentina')

# Get current run_id from file
run_id_file = Path('output/Argentina/.current_run_id')
if run_id_file.exists():
    current_run_id = run_id_file.read_text().strip()
    print(f'Current run_id from file: {current_run_id}')
else:
    print('No .current_run_id file found')
    current_run_id = None

with db.cursor() as cur:
    # Check run_id distribution in product_index
    cur.execute('SELECT run_id, COUNT(*) FROM ar_product_index GROUP BY run_id ORDER BY COUNT(*) DESC')
    print(f'\nRun IDs in ar_product_index:')
    for row in cur.fetchall():
        marker = " <-- CURRENT" if current_run_id and row[0] == current_run_id else ""
        print(f'  {row[0]}: {row[1]} products{marker}')
    
    # Check if current run_id has any products
    if current_run_id:
        cur.execute('SELECT COUNT(*) FROM ar_product_index WHERE run_id = %s', (current_run_id,))
        count = cur.fetchone()[0]
        print(f'\nProducts with current run_id: {count}')
        
        # Check eligible products for current run_id
        cur.execute('''
            SELECT COUNT(*) FROM ar_product_index 
            WHERE run_id = %s 
            AND COALESCE(total_records,0) = 0 
            AND COALESCE(loop_count,0) < 5
            AND url IS NOT NULL AND url <> ''
        ''', (current_run_id,))
        eligible = cur.fetchone()[0]
        print(f'Eligible for Selenium (current run_id): {eligible}')
