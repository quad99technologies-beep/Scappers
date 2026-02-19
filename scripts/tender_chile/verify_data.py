#!/usr/bin/env python3
"""Verify tender details are being saved to database"""
import sys
from pathlib import Path

# Add repo root to path
repo_root = Path(__file__).resolve().parents[2]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from core.db.connection import CountryDB
import os

# Get current run_id
run_id = os.getenv("TENDER_CHILE_RUN_ID", "")
if not run_id:
    print("[ERROR] TENDER_CHILE_RUN_ID not set")
    print("Please run this from the pipeline or set the run_id manually")
    sys.exit(1)

db = CountryDB('Tender_Chile')
db.connect()

cur = db.cursor(dict_cursor=True)

# Check total count
cur.execute('SELECT COUNT(*) as count FROM tc_tender_details WHERE run_id = %s', (run_id,))
total = cur.fetchone()['count']

print(f"Run ID: {run_id}")
print(f"Total tender details saved: {total}")
print("=" * 80)

if total > 0:
    # Show first 5 records
    cur.execute('''
        SELECT tender_id, tender_name, organization, source_url 
        FROM tc_tender_details 
        WHERE run_id = %s 
        ORDER BY scraped_at DESC 
        LIMIT 5
    ''', (run_id,))
    
    rows = cur.fetchall()
    print(f"\nMost recent {len(rows)} tender details:")
    print("-" * 80)
    for i, r in enumerate(rows, 1):
        print(f"\n{i}. Tender ID: {r['tender_id']}")
        print(f"   Name: {r['tender_name'][:80]}")
        print(f"   Organization: {r['organization'][:80]}")
        print(f"   URL: {r['source_url'][:100]}")
else:
    print("\n[WARNING] No tender details found in database!")
    print("This could mean:")
    print("  1. The scraper hasn't saved any data yet")
    print("  2. You're checking a different run_id")
    print("  3. There's an issue with the database connection")

db.close()
