#!/usr/bin/env python3
"""Check differences between expected and extracted data."""

import pandas as pd

expected = pd.read_csv('C:/Users/Vishw/OneDrive/Desktop/New folder (2)/FinalData.csv', encoding='latin-1')
extracted = pd.read_csv('output/Tender_Chile/final_tender_data.csv', encoding='utf-8-sig')

print(f'Expected rows: {len(expected)}')
print(f'Extracted rows: {len(extracted)}')
print()

# Check for differences in key fields
key_fields = ['Source Tender Id', 'Tender Title', 'PROVINCE', 'TENDERING AUTHORITY', 
              'Lot Title', 'Unique Lot ID', 'Lot Number', 'Bidder', 'Bid Status Award',
              'Awarded Unit Price', 'Lot_Award_Value_Local', 'QUANTITY', 'Deadline Date']

diff_count = 0
for field in key_fields:
    exp_vals = expected[field].astype(str).tolist()
    ext_vals = extracted[field].astype(str).tolist()
    
    field_diffs = []
    for i, (e, x) in enumerate(zip(exp_vals, ext_vals)):
        e_clean = e.strip() if e != 'nan' else ''
        x_clean = x.strip() if x != 'nan' else ''
        if e_clean != x_clean:
            field_diffs.append((i, e_clean, x_clean))
    
    if field_diffs:
        diff_count += len(field_diffs)
        print(f'[DIFF] {field} ({len(field_diffs)} differences):')
        for row_idx, e, x in field_diffs[:5]:  # Show first 5
            print(f'  Row {row_idx}: Expected "{e}" vs Extracted "{x}"')
        if len(field_diffs) > 5:
            print(f'  ... and {len(field_diffs) - 5} more')
        print()

if diff_count == 0:
    print('[OK] All fields match!')
else:
    print(f'Total differences: {diff_count}')
