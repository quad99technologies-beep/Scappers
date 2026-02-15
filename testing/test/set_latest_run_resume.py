#!/usr/bin/env python3
"""
Set the latest Argentina run_ledger row status to 'resume' so the pipeline can resume.

Usage:
    python set_latest_run_resume.py
    python set_latest_run_resume.py --run-id 20260202_123456_abc12345  # specific run_id
"""

import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

def main():
    import argparse
    from core.db.connection import CountryDB
    from core.db.models import run_ledger_mark_resumable

    parser = argparse.ArgumentParser(description="Set latest Argentina run status to 'resume'")
    parser.add_argument("--run-id", default=None, help="Specific run_id (default: latest Argentina run)")
    args = parser.parse_args()

    db = CountryDB("Argentina")
    db.connect()

    try:
        with db.cursor() as cur:
            if args.run_id:
                run_id = args.run_id.strip()
                cur.execute(
                    "SELECT run_id, status, started_at FROM run_ledger WHERE run_id = %s AND scraper_name = %s",
                    (run_id, "Argentina"),
                )
                row = cur.fetchone()
                if not row:
                    print(f"[ERROR] No run_ledger row found for run_id={run_id} (Argentina)")
                    return 1
            else:
                cur.execute(
                    """
                    SELECT run_id, status, started_at
                    FROM run_ledger
                    WHERE scraper_name = %s
                    ORDER BY started_at DESC
                    LIMIT 1
                    """,
                    ("Argentina",),
                )
                row = cur.fetchone()
                if not row:
                    print("[ERROR] No run_ledger rows found for Argentina")
                    return 1
                run_id = row[0]

            sql, params = run_ledger_mark_resumable(run_id)
            cur.execute(sql, params)

        print(f"[OK] run_ledger status set to 'resume' for run_id={run_id}")
        return 0
    except Exception as e:
        print(f"[ERROR] {e}")
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
