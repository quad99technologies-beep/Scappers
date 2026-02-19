#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Check for pending/queued products"""

import os
import sys
from pathlib import Path

# Add repo root to path for core imports (MUST be before any core imports)
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_SCRIPT_DIR = Path(__file__).resolve().parent
# Ensure Argentina directory is at the front of sys.path to prioritize local 'db' package
# This fixes conflict with core/db which might be in sys.path
sys.path = [p for p in sys.path if not Path(p).name == 'core']
if str(_SCRIPT_DIR) in sys.path:
    sys.path.remove(str(_SCRIPT_DIR))
sys.path.insert(0, str(_SCRIPT_DIR))

# Force re-import of db module if it was incorrectly loaded from core/db
if 'db' in sys.modules:
    del sys.modules['db']

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
    raise RuntimeError("ARGENTINA_RUN_ID not set")

output_dir = get_output_dir()
run_id = _get_run_id(output_dir)
db = CountryDB("Argentina")

print(f"Checking products for run_id: {run_id}\n")

# Check various product states
with db.cursor() as cur:
    # Total products in index
    cur.execute("SELECT COUNT(*) FROM ar_product_index WHERE run_id = %s", (run_id,))
    total = cur.fetchone()[0]

    # Pending products
    cur.execute("SELECT COUNT(*) FROM ar_product_index WHERE run_id = %s AND status = 'pending'", (run_id,))
    pending = cur.fetchone()[0]

    # Products with total_records = 0
    cur.execute("SELECT COUNT(*) FROM ar_product_index WHERE run_id = %s AND COALESCE(total_records, 0) = 0", (run_id,))
    no_data = cur.fetchone()[0]

    # Products with scrape_source = 'step7'
    cur.execute("SELECT COUNT(*) FROM ar_product_index WHERE run_id = %s AND scrape_source = 'step7'", (run_id,))
    step7 = cur.fetchone()[0]

    # Recently updated products (last 1 hour)
    cur.execute("""
        SELECT COUNT(*) FROM ar_product_index
        WHERE run_id = %s AND updated_at > NOW() - INTERVAL '1 hour'
    """, (run_id,))
    recent = cur.fetchone()[0]

print(f"Total products in index: {total}")
print(f"Pending (status='pending'): {pending}")
print(f"No data (total_records=0): {no_data}")
print(f"Marked as Step 7/8 (scrape_source='step7'): {step7}")
print(f"Recently updated (last hour): {recent}")
print()

# Show some sample pending products
if pending > 0:
    print("Sample pending products:")
    with db.cursor(dict_cursor=True) as cur:
        cur.execute("""
            SELECT company, product, status, total_records, loop_count, scrape_source, updated_at
            FROM ar_product_index
            WHERE run_id = %s AND status = 'pending'
            ORDER BY updated_at DESC
            LIMIT 10
        """, (run_id,))
        for row in cur.fetchall():
            print(f"  - {row['company']} / {row['product']} (records={row['total_records']}, loop={row['loop_count']}, source={row['scrape_source']})")
