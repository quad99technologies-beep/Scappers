#!/usr/bin/env python3
"""Check progress logs for the latest Russia run."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.db.connection import CountryDB


def check_progress_logs():
    db = CountryDB('Russia')
    
    with db.cursor() as cur:
        # Get latest run
        cur.execute('''
            SELECT run_id, status, started_at 
            FROM run_ledger 
            ORDER BY started_at DESC
            LIMIT 1
        ''')
        run = cur.fetchone()
        
        if not run:
            print("No runs found.")
            return
        
        run_id, status, started_at = run
        print(f"Latest Run: {run_id}")
        print(f"Status: {status}, Started: {started_at}")
        print("=" * 80)
        
        # Get progress entries for this run
        cur.execute('''
            SELECT progress_key, status, log_details, started_at, completed_at 
            FROM ru_step_progress 
            WHERE run_id = %s
            ORDER BY progress_key
        ''', (run_id,))
        
        rows = cur.fetchall()
        print(f"\nTotal pages tracked: {len(rows)}")
        print()
        
        for row in rows[:10]:  # Show first 10
            page_key, status, log_details, started, completed = row
            print(f"{page_key}:")
            print(f"  Status: {status}")
            print(f"  Log: {log_details}")
            print(f"  Started: {started}")
            print(f"  Completed: {completed}")
            print()
        
        if len(rows) > 10:
            print(f"... and {len(rows) - 10} more pages")


if __name__ == "__main__":
    check_progress_logs()
