#!/usr/bin/env python3
"""Apply database schema updates for Tender Chile."""

import sys
sys.path.insert(0, r'D:\quad99\Scrappers')

from core.db.connection import CountryDB

# Read the alter script
with open(r'D:\quad99\Scrappers\sql\schemas\postgres\tender_chile_alter_2025.sql', 'r') as f:
    sql_script = f.read()

# Connect and execute
db = CountryDB('Tender_Chile')
db.connect()

with db.cursor() as cur:
    cur.execute(sql_script)
    
print('[OK] Database schema updated successfully')

# Verify the columns were added
with db.cursor() as cur:
    cur.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'tc_tender_awards'
        ORDER BY ordinal_position
    """)
    columns = cur.fetchall()
    print('\nColumns in tc_tender_awards:')
    for col in columns:
        print(f'  {col[0]}')
        
# Also check tc_tender_details
with db.cursor() as cur:
    cur.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'tc_tender_details'
        ORDER BY ordinal_position
    """)
    columns = cur.fetchall()
    print('\nColumns in tc_tender_details:')
    for col in columns:
        print(f'  {col[0]}')
