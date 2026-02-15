#!/usr/bin/env python3
"""
Compare extracted data with expected data for Chile scraper.
"""

import pandas as pd
from pathlib import Path

def compare_data():
    # Read expected data
    expected_path = Path("C:/Users/Vishw/OneDrive/Desktop/New folder (2)/FinalData.csv")
    extracted_path = Path("D:/quad99/Scrappers/output/Tender_Chile/final_tender_data.csv")
    
    expected = pd.read_csv(expected_path, encoding='latin-1')
    extracted = pd.read_csv(extracted_path, encoding='utf-8-sig')
    
    print("=" * 100)
    print("COMPARISON: Expected vs Extracted Data")
    print("=" * 100)
    
    print(f"\nRow Count:")
    print(f"  Expected: {len(expected)}")
    print(f"  Extracted: {len(extracted)}")
    
    print(f"\nColumn Comparison:")
    print(f"  Expected columns: {list(expected.columns)}")
    print(f"  Extracted columns: {list(extracted.columns)}")
    
    # Check for missing columns
    missing_in_extracted = set(expected.columns) - set(extracted.columns)
    missing_in_expected = set(extracted.columns) - set(expected.columns)
    
    if missing_in_extracted:
        print(f"\n  [FAIL] Columns missing in extracted: {missing_in_extracted}")
    else:
        print(f"\n  [OK] All expected columns present in extracted")
        
    if missing_in_expected:
        print(f"  [WARN] Extra columns in extracted: {missing_in_expected}")
    
    # Compare key fields for first few rows
    print("\n" + "=" * 100)
    print("Sample Row Comparison (First 5 rows):")
    print("=" * 100)
    
    key_fields = [
        'Source Tender Id', 'Tender Title', 'TENDERING AUTHORITY', 'PROVINCE',
        'Lot Title', 'Unique Lot ID', 'QUANTITY', 'Bidder', 'Awarded Unit Price',
        'Lot_Award_Value_Local', 'Bid Status Award'
    ]
    
    for i in range(min(5, len(expected), len(extracted))):
        print(f"\n--- Row {i+1} ---")
        for field in key_fields:
            exp_val = expected[field].iloc[i] if field in expected.columns else 'N/A'
            ext_val = extracted[field].iloc[i] if field in extracted.columns else 'N/A'
            
            # Handle NaN values
            exp_val = '' if pd.isna(exp_val) else str(exp_val).strip()
            ext_val = '' if pd.isna(ext_val) else str(ext_val).strip()
            
            match = "[MATCH]" if exp_val == ext_val else "[DIFF]"
            print(f"  {match} {field}:")
            print(f"      Expected: {exp_val[:80]}{'...' if len(exp_val) > 80 else ''}")
            print(f"      Extracted: {ext_val[:80]}{'...' if len(ext_val) > 80 else ''}")
    
    # Summary statistics
    print("\n" + "=" * 100)
    print("Summary Statistics:")
    print("=" * 100)
    
    # Check Source Tender Id
    exp_tender_ids = set(expected['Source Tender Id'].dropna().astype(str))
    ext_tender_ids = set(extracted['Source Tender Id'].dropna().astype(str))
    print(f"\nSource Tender IDs:")
    print(f"  Expected unique: {exp_tender_ids}")
    print(f"  Extracted unique: {ext_tender_ids}")
    print(f"  Match: {exp_tender_ids == ext_tender_ids}")
    
    # Check Tender Title
    print(f"\nTender Title populated:")
    print(f"  Expected: {expected['Tender Title'].notna().sum()}/{len(expected)}")
    print(f"  Extracted: {extracted['Tender Title'].notna().sum()}/{len(extracted)}")
    
    # Check PROVINCE
    print(f"\nPROVINCE populated:")
    print(f"  Expected: {expected['PROVINCE'].notna().sum()}/{len(expected)}")
    print(f"  Extracted: {extracted['PROVINCE'].notna().sum()}/{len(extracted)}")
    
    # Check TENDERING AUTHORITY
    print(f"\nTENDERING AUTHORITY populated:")
    print(f"  Expected: {expected['TENDERING AUTHORITY'].notna().sum()}/{len(expected)}")
    print(f"  Extracted: {extracted['TENDERING AUTHORITY'].notna().sum()}/{len(extracted)}")
    
    # Check Lot Title
    print(f"\nLot Title populated:")
    print(f"  Expected: {expected['Lot Title'].notna().sum()}/{len(expected)}")
    print(f"  Extracted: {extracted['Lot Title'].notna().sum()}/{len(extracted)}")
    
    # Check Unique Lot ID
    print(f"\nUnique Lot ID populated:")
    print(f"  Expected: {expected['Unique Lot ID'].notna().sum()}/{len(expected)}")
    print(f"  Extracted: {extracted['Unique Lot ID'].notna().sum()}/{len(extracted)}")
    
    # Check QUANTITY
    print(f"\nQUANTITY populated:")
    print(f"  Expected: {expected['QUANTITY'].notna().sum()}/{len(expected)}")
    print(f"  Extracted: {extracted['QUANTITY'].notna().sum()}/{len(extracted)}")
    
    print("\n" + "=" * 100)

if __name__ == "__main__":
    compare_data()
