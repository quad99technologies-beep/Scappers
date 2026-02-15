#!/usr/bin/env python3
"""Check existing database schema."""
import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_repo_root))

from core.db import get_db

db = get_db("NorthMacedonia")

with db.cursor() as cur:
    # Check if table exists
    cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name='nm_drug_register')")
    exists = cur.fetchone()[0]

    if exists:
        print("Table nm_drug_register exists")
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='nm_drug_register' ORDER BY ordinal_position")
        columns = [r[0] for r in cur.fetchall()]
        print(f"Columns: {columns}")
        
        if 'url_id' in columns:
            print("[OK] url_id column exists")
        else:
            print("[MISSING] url_id column MISSING - need to add it")
    else:
        print("Table nm_drug_register does NOT exist - will be created fresh")

