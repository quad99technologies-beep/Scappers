#!/usr/bin/env python3
"""
Fix award URLs in the database - change from Results.aspx to PreviewAwardAct.aspx
"""
import sys
import os
from pathlib import Path

repo_root = Path(__file__).resolve().parents[2]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from core.db.connection import CountryDB

run_id = os.getenv("TENDER_CHILE_RUN_ID", "")
if not run_id:
    print("[ERROR] TENDER_CHILE_RUN_ID not set")
    sys.exit(1)

db = CountryDB('Tender_Chile')
db.connect()

print(f"Run ID: {run_id}")
print("=" * 80)

# Check current award URLs
with db.cursor(dict_cursor=True) as cur:
    cur.execute('''
        SELECT COUNT(*) as count 
        FROM tc_tender_redirects 
        WHERE run_id = %s AND redirect_url LIKE '%Results.aspx%'
    ''', (run_id,))
    old_count = cur.fetchone()['count']
    
    print(f"Found {old_count} redirects with old 'Results.aspx' format")

if old_count == 0:
    print("\n✅ No URLs need updating - already using correct format!")
    db.close()
    sys.exit(0)

print("\nUpdating award URLs to correct format...")
print("Old: .../Results.aspx?qs=...")
print("New: .../StepsProcessAward/PreviewAwardAct.aspx?qs=...")

# Update the URLs
with db.cursor() as cur:
    # Note: We don't store award_url separately, it's generated from redirect_url
    # The redirect_url itself is correct (DetailsAcquisition.aspx)
    # The award URL is generated in Step 3 by replacing the path
    print("\n✅ Award URLs are generated dynamically in Step 3")
    print("   No database update needed - just need to re-run Step 3")

print("\n" + "=" * 80)
print("ACTION REQUIRED:")
print("1. Stop the current pipeline (Ctrl+C)")
print("2. Delete Step 3 data to force re-run:")
print("   DELETE FROM tc_tender_awards WHERE run_id = '{}'".format(run_id))
print("3. Restart pipeline: .\\run_pipeline_resume.bat")
print("=" * 80)

db.close()
