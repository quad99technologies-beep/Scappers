#!/usr/bin/env python3
"""
Comprehensive status checker for Tender Chile pipeline
Shows data counts for all tables and current run status
"""
import sys
from pathlib import Path

# Add repo root to path
repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root))

from core.db.connection import CountryDB
import os

# Get current run_id
run_id = os.getenv("TENDER_CHILE_RUN_ID", "")

db = CountryDB('Tender_Chile')
db.connect()
cur = db.cursor(dict_cursor=True)

print("=" * 80)
print("TENDER CHILE PIPELINE STATUS")
print("=" * 80)

if run_id:
    print(f"\nCurrent Run ID: {run_id}")
else:
    print("\n⚠️  TENDER_CHILE_RUN_ID not set - showing all runs")

# Check run ledger
print("\n" + "-" * 80)
print("RUN LEDGER (Most Recent 5 Runs)")
print("-" * 80)
cur.execute('''
    SELECT run_id, mode, status, started_at, finished_at, items_scraped 
    FROM tc_run_ledger 
    ORDER BY started_at DESC 
    LIMIT 5
''')
runs = cur.fetchall()
for r in runs:
    status_icon = "✅" if r['status'] == 'completed' else "🔄" if r['status'] == 'running' else "❌"
    print(f"{status_icon} {r['run_id']} | {r['mode']} | {r['status']} | Items: {r['items_scraped'] or 0}")

# Data counts for current run
if run_id:
    print(f"\n" + "-" * 80)
    print(f"DATA COUNTS FOR RUN: {run_id}")
    print("-" * 80)
    
    tables = [
        ('tc_tender_redirects', 'Redirect URLs'),
        ('tc_tender_details', 'Tender Details'),
        ('tc_tender_awards', 'Tender Awards'),
        ('tc_final_output', 'Final Output'),
    ]
    
    for table, label in tables:
        cur.execute(f'SELECT COUNT(*) as count FROM {table} WHERE run_id = %s', (run_id,))
        count = cur.fetchone()['count']
        icon = "✅" if count > 0 else "⚠️ "
        print(f"{icon} {label:20s}: {count:,} records")

# Overall database stats (all runs)
print(f"\n" + "-" * 80)
print("OVERALL DATABASE STATS (All Runs)")
print("-" * 80)

for table, label in tables:
    cur.execute(f'SELECT COUNT(*) as count FROM {table}')
    count = cur.fetchone()['count']
    print(f"   {label:20s}: {count:,} total records")

# Check if current run has any progress
if run_id:
    print(f"\n" + "-" * 80)
    print(f"LATEST ACTIVITY FOR RUN: {run_id}")
    print("-" * 80)
    
    # Check latest tender detail
    cur.execute('''
        SELECT tender_id, tender_name, scraped_at 
        FROM tc_tender_details 
        WHERE run_id = %s 
        ORDER BY scraped_at DESC 
        LIMIT 1
    ''', (run_id,))
    
    latest = cur.fetchone()
    if latest:
        print(f"✅ Latest tender scraped:")
        print(f"   ID: {latest['tender_id']}")
        print(f"   Name: {latest['tender_name'][:60]}")
        print(f"   Time: {latest['scraped_at']}")
    else:
        print("⚠️  No tender details scraped yet for this run")

print("\n" + "=" * 80)

db.close()
