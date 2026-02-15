#!/usr/bin/env python3
"""Get a sample award URL for testing"""
import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root))

from core.db.connection import CountryDB

db = CountryDB('Tender_Chile')
db.connect()

with db.cursor(dict_cursor=True) as cur:
    cur.execute('SELECT redirect_url FROM tc_tender_redirects LIMIT 5')
    rows = cur.fetchall()

print("\n" + "=" * 80)
print("SAMPLE AWARD URLs TO TEST")
print("=" * 80)

for i, row in enumerate(rows, 1):
    details_url = row['redirect_url']
    award_url = details_url.replace('DetailsAcquisition.aspx', 'Results.aspx')
    
    print(f"\n{i}. Details URL:")
    print(f"   {details_url}")
    print(f"\n   Award URL:")
    print(f"   {award_url}")

print("\n" + "=" * 80)
print("Please open one of these Award URLs in your browser and:")
print("1. Take a screenshot")
print("2. View page source (Ctrl+U)")
print("3. Search for 'grdItemOC' in the HTML")
print("=" * 80)

db.close()
