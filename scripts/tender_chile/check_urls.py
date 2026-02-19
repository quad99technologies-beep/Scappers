#!/usr/bin/env python3
"""Quick check of redirect URLs in database"""
import sys
from pathlib import Path

# Add repo root to path
repo_root = Path(__file__).resolve().parents[2]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from core.db.connection import CountryDB

db = CountryDB('Tender_Chile')
db.connect()

cur = db.cursor(dict_cursor=True)
cur.execute('SELECT redirect_url, source_url FROM tc_tender_redirects LIMIT 5')
rows = cur.fetchall()

print("Sample redirect URLs from database:")
print("=" * 80)
for i, r in enumerate(rows, 1):
    print(f"\n{i}. Redirect URL:")
    print(f"   {r['redirect_url']}")
    print(f"   Source URL:")
    print(f"   {r['source_url']}")

db.close()
