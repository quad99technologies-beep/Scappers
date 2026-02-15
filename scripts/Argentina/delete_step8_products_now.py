#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Quick script to delete Step 8 products"""

import os
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

from config_loader import get_output_dir
from core.db.connection import CountryDB

def _get_run_id(output_dir: Path) -> str:
    rid = os.environ.get("ARGENTINA_RUN_ID", "").strip()
    if rid:
        return rid
    run_id_file = output_dir / ".current_run_id"
    if run_id_file.exists():
        txt = run_id_file.read_text(encoding="utf-8").strip()
        if txt:
            return txt
    raise RuntimeError("ARGENTINA_RUN_ID not set and .current_run_id missing.")

output_dir = get_output_dir()
run_id = _get_run_id(output_dir)
db = CountryDB("Argentina")

# Count first
with db.cursor() as cur:
    cur.execute("""
        SELECT COUNT(*) FROM ar_product_index
        WHERE run_id = %s AND scrape_source = 'step7'
    """, (run_id,))
    count = cur.fetchone()[0]

print(f"Found {count} products with scrape_source='step7'")

if count > 0:
    # Delete them
    with db.cursor() as cur:
        cur.execute("""
            DELETE FROM ar_product_index
            WHERE run_id = %s AND scrape_source = 'step7'
        """, (run_id,))
    print(f"✓ Deleted {count} products from ar_product_index")
else:
    print("No products to delete")
