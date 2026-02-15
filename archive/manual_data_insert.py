#!/usr/bin/env python3
"""Manually insert test data into database for debugging Step 4."""

import sys
sys.path.insert(0, r'D:\quad99\Scrappers')

import pandas as pd
from core.db.connection import CountryDB

# The run ID from the latest execution
run_id = "20260210_081520_4465bc65"

# Use a specific backup that has all the data
latest_backup = r'D:\quad99\Scrappers\backups\Tender_Chile\output_20260210_133715'

print(f"[INFO] Using backup: {latest_backup}")

# Read the CSV files
tender_details = pd.read_csv(f"{latest_backup}/tender_details.csv")
supplier_rows = pd.read_csv(f"{latest_backup}/mercadopublico_supplier_rows.csv")

print(f"[INFO] Loaded {len(tender_details)} tender details")
print(f"[INFO] Loaded {len(supplier_rows)} supplier rows")

# Connect to database
db = CountryDB('Tender_Chile')
db.connect()

# Insert tender details
with db.cursor() as cur:
    for _, row in tender_details.iterrows():
        cur.execute("""
            INSERT INTO tc_tender_details 
            (run_id, tender_id, tender_name, tender_status, publication_date, 
             closing_date, organization, province, contact_info, description,
             currency, estimated_amount, source_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, tender_id) DO UPDATE SET
                tender_name = EXCLUDED.tender_name,
                organization = EXCLUDED.organization,
                province = EXCLUDED.province,
                closing_date = EXCLUDED.closing_date,
                source_url = EXCLUDED.source_url
        """, (
            run_id,
            row['Tender ID'],
            row['Tender Title'],
            '',  # tender_status
            '',  # publication_date
            row['Closing Date'],
            row['TENDERING AUTHORITY'],
            row['PROVINCE'],
            '',  # contact_info
            '',  # description
            'CLP',
            None,  # estimated_amount
            row['Source URL']
        ))
    print(f"[OK] Inserted {len(tender_details)} tender details")

# Insert supplier rows (all bidders)
with db.cursor() as cur:
    for _, row in supplier_rows.iterrows():
        cur.execute("""
            INSERT INTO tc_tender_awards 
            (run_id, tender_id, lot_number, lot_title, un_classification_code, 
             buyer_specifications, lot_quantity, supplier_name, supplier_rut,
             supplier_specifications, unit_price_offer, awarded_quantity, 
             total_net_awarded, award_amount, award_date, award_status, is_awarded,
             awarded_unit_price, source_url, source_tender_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            run_id,
            row['tender_id'],
            row['lot_number'],
            row['item_title'],
            row['un_classification_code'],
            row['buyer_specifications'],
            row['lot_quantity'],
            row['supplier'],
            '',  # supplier_rut
            row['supplier_specifications'],
            row['unit_price_offer'] if pd.notna(row['unit_price_offer']) else None,
            row['awarded_quantity'],
            row['total_net_awarded'] if pd.notna(row['total_net_awarded']) else None,
            row['total_net_awarded'] if row.get('is_awarded') == 'YES' and pd.notna(row['total_net_awarded']) else None,
            row['award_date'],
            row['state'],
            row['is_awarded'],
            row['awarded_unit_price'] if pd.notna(row['awarded_unit_price']) else None,
            row['source_url'],
            row['source_tender_url']
        ))
    print(f"[OK] Inserted {len(supplier_rows)} supplier rows")

print("\n[OK] All data inserted successfully!")
print(f"[INFO] Run ID: {run_id}")
