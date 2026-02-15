#!/usr/bin/env python3
"""Apply final output table alter script."""

import sys
sys.path.insert(0, r'D:\quad99\Scrappers')

from core.db.connection import CountryDB

# Read the alter script
with open(r'D:\quad99\Scrappers\sql\schemas\postgres\tender_chile_alter_final.sql', 'r') as f:
    sql_script = f.read()

# Connect and execute
db = CountryDB('Tender_Chile')
db.connect()

with db.cursor() as cur:
    cur.execute(sql_script)
    
print('[OK] Database schema updated successfully')
