#!/usr/bin/env python3
"""Verify database data matches expected data."""

import sys
sys.path.insert(0, r'D:\quad99\Scrappers')

import pandas as pd
from core.db.connection import CountryDB

expected = pd.read_csv('C:/Users/Vishw/OneDrive/Desktop/New folder (2)/FinalData.csv', encoding='latin-1')

# Read from database
db = CountryDB('Tender_Chile')
db.connect()

with db.cursor(dict_cursor=True) as cur:
    cur.execute("""
        SELECT 
            tender_id as "Source Tender Id",
            tender_name as "Tender Title",
            organization as "TENDERING AUTHORITY",
            lot_number as "Lot Number",
            lot_title as "AWARDED LOT TITLE",
            supplier_name as "Bidder",
            award_amount as "Lot_Award_Value_Local",
            award_date as "Award Date",
            source_url as "Original_Publication_Link_Award"
        FROM tc_final_output 
        WHERE run_id = %s
        ORDER BY lot_number, supplier_name
    """, ('20260210_081520_4465bc65',))
    rows = cur.fetchall()
    
extracted = pd.DataFrame(rows)

print(f'Expected rows: {len(expected)}')
print(f'DB rows: {len(extracted)}')
print()

# Compare
if len(expected) != len(extracted):
    print(f'[ERROR] Row count mismatch!')
else:
    print('[OK] Row count matches')

# Check key fields
print('\nSample data from database:')
print(extracted.head())
