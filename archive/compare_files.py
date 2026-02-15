#!/usr/bin/env python3
"""Compare expected vs extracted files."""

import pandas as pd

expected = pd.read_csv('C:/Users/Vishw/OneDrive/Desktop/New folder (2)/FinalData.csv', encoding='latin-1')
extracted = pd.read_csv('output/Tender_Chile/final_tender_data.csv', encoding='utf-8-sig')

print('='*100)
print('FILE COMPARISON: Expected vs Extracted')
print('='*100)
print(f'\nExpected file: C:/Users/Vishw/OneDrive/Desktop/New folder (2)/FinalData.csv')
print(f'Extracted file: output/Tender_Chile/final_tender_data_v2.csv')
print(f'\nExpected rows: {len(expected)}')
print(f'Extracted rows: {len(extracted)}')
print(f'Columns: {list(expected.columns)}')

# Sort both by Lot Number and Bidder
sort_keys = ['Lot Number', 'Bidder']
exp = expected.sort_values(by=sort_keys).reset_index(drop=True)
ext = extracted.sort_values(by=sort_keys).reset_index(drop=True)

# Show first 10 rows side by side
print('\n' + '='*100)
print('FIRST 10 ROWS COMPARISON')
print('='*100)

for i in range(min(10, len(exp))):
    print(f'\n--- Row {i} ---')
    print(f'Lot: {exp.iloc[i]["Lot Number"]} | Bidder: {exp.iloc[i]["Bidder"][:50]}...')
    
    for col in ['Tender Title', 'PROVINCE', 'TENDERING AUTHORITY', 'Lot Title', 'Unique Lot ID', 'QUANTITY', 'Bid Status Award', 'Awarded Unit Price']:
        e_val = str(exp.iloc[i][col]).strip() if pd.notna(exp.iloc[i][col]) else ''
        x_val = str(ext.iloc[i][col]).strip() if pd.notna(ext.iloc[i][col]) else ''
        match = '[OK]' if e_val == x_val else '[DIFF]'
        print(f'  {match} {col}: Expected="{e_val}" | Extracted="{x_val}"')

# Summary
print('\n' + '='*100)
print('SUMMARY')
print('='*100)

total_diffs = 0
for col in expected.columns:
    diffs = 0
    for i in range(len(exp)):
        e_val = str(exp.iloc[i][col]).strip() if pd.notna(exp.iloc[i][col]) else ''
        x_val = str(ext.iloc[i][col]).strip() if pd.notna(ext.iloc[i][col]) else ''
        if e_val != x_val:
            diffs += 1
    if diffs > 0:
        total_diffs += diffs
        print(f'{col}: {diffs} differences')

if total_diffs == 0:
    print('\n[OK] All data matches perfectly!')
else:
    print(f'\nTotal differences: {total_diffs}')
