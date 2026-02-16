#!/usr/bin/env python3
"""
Fix helper: requeue items that were marked as having records in ar_product_index
but have no corresponding rows in ar_products (e.g., when DB inserts failed mid-run).

Example:
  python requeue_missing_products.py
  python requeue_missing_products.py --run-id 20260131_112535_28e0da17 --dry-run
"""

import argparse
import os

# Ensure Argentina directory is at the front of sys.path to prioritize local 'db' package
# This fixes conflict with core/db which might be in sys.path
import sys
from pathlib import Path
sys.path = [p for p in sys.path if not Path(p).name == 'core']
_script_dir = Path(__file__).resolve().parent
if str(_script_dir) in sys.path:
    sys.path.remove(str(_script_dir))
sys.path.insert(0, str(_script_dir))

# Force re-import of db module if it was incorrectly loaded from core/db
if 'db' in sys.modules:
    del sys.modules['db']

from core.db.connection import CountryDB
from db.schema import apply_argentina_schema
from core.db.models import generate_run_id
from config_loader import get_output_dir


def _get_run_id() -> str:
    output_dir = get_output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)
    run_id_file = output_dir / ".current_run_id"

    rid = os.environ.get("ARGENTINA_RUN_ID")
    if rid:
        return rid
    if run_id_file.exists():
        txt = run_id_file.read_text(encoding="utf-8", errors="ignore").strip()
        if txt:
            return txt
    rid = generate_run_id()
    os.environ["ARGENTINA_RUN_ID"] = rid
    run_id_file.write_text(rid, encoding="utf-8")
    return rid


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", default=None, help="Run id (defaults to output/Argentina/.current_run_id)")
    ap.add_argument("--dry-run", action="store_true", help="Only print counts; do not update DB")
    args = ap.parse_args()

    run_id = (args.run_id or _get_run_id()).strip()
    if not run_id:
        raise SystemExit("run_id is empty")

    db = CountryDB("Argentina")
    apply_argentina_schema(db)

    with db.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
              FROM ar_product_index i
             WHERE i.run_id = %s
               AND COALESCE(i.total_records,0) > 0
               AND NOT EXISTS (
                   SELECT 1
                     FROM ar_products p
                    WHERE p.run_id = i.run_id
                      AND p.input_company = i.company
                      AND p.input_product_name = i.product
               )
            """,
            (run_id,),
        )
        missing = cur.fetchone()[0]

    print(f"[CHECK] run_id={run_id} | index_has_records_but_no_products_rows={missing}", flush=True)
    if args.dry_run or missing == 0:
        return 0

    with db.cursor() as cur:
        cur.execute(
            """
            UPDATE ar_product_index i
               SET total_records = 0,
                   status = 'pending',
                   error_message = 'requeued_missing_ar_products',
                   updated_at = CURRENT_TIMESTAMP
             WHERE i.run_id = %s
               AND COALESCE(i.total_records,0) > 0
               AND NOT EXISTS (
                   SELECT 1
                     FROM ar_products p
                    WHERE p.run_id = i.run_id
                      AND p.input_company = i.company
                      AND p.input_product_name = i.product
               )
            """,
            (run_id,),
        )
        updated = cur.rowcount

    print(f"[OK] Requeued rows={updated} (total_records reset to 0) | run_id={run_id}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

