#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Run Comparison Tool

Side-by-side comparison of two pipeline runs.

Usage:
    python core/run_comparison.py Malaysia run_20260201_abc run_20260202_def
"""

import sys
import argparse
from pathlib import Path
from typing import Dict, Any, List, Tuple
from datetime import datetime

# Add repo root to path
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from core.db.postgres_connection import get_db


def get_run_metrics(scraper_name: str, run_id: str) -> Dict[str, Any]:
    """Get all metrics for a run."""
    try:
        db = get_db(scraper_name)
        
        table_prefix_map = {
            "Argentina": "ar",
            "Malaysia": "my",
            "Netherlands": "nl",
        }
        prefix = table_prefix_map.get(scraper_name, scraper_name.lower()[:2])
        table_name = f"{prefix}_step_progress"
        
        with db.cursor() as cur:
            # Get run info
            cur.execute("""
                SELECT run_id, status, started_at, ended_at, step_count,
                       total_runtime_seconds, slowest_step_number, slowest_step_name
                FROM run_ledger
                WHERE run_id = %s AND scraper_name = %s
            """, (run_id, scraper_name))
            
            run_row = cur.fetchone()
            if not run_row:
                return {}
            
            # Get step metrics
            cur.execute(f"""
                SELECT step_number, step_name, status, duration_seconds,
                       rows_read, rows_processed, rows_inserted, rows_updated, rows_rejected
                FROM {table_name}
                WHERE run_id = %s
                ORDER BY step_number
            """, (run_id,))
            
            steps = {}
            for row in cur.fetchall():
                steps[row[0]] = {
                    "step_name": row[1],
                    "status": row[2],
                    "duration_seconds": row[3] or 0,
                    "rows_read": row[4] or 0,
                    "rows_processed": row[5] or 0,
                    "rows_inserted": row[6] or 0,
                    "rows_updated": row[7] or 0,
                    "rows_rejected": row[8] or 0,
                }
            
            return {
                "run_id": run_row[0],
                "status": run_row[1],
                "started_at": run_row[2],
                "ended_at": run_row[3],
                "step_count": run_row[4],
                "total_runtime_seconds": run_row[5] or 0,
                "slowest_step_number": run_row[6],
                "slowest_step_name": run_row[7],
                "steps": steps
            }
    except Exception as e:
        print(f"Error getting metrics for {run_id}: {e}")
        return {}


def compare_runs(scraper_name: str, run_id1: str, run_id2: str):
    """Compare two runs side-by-side."""
    metrics1 = get_run_metrics(scraper_name, run_id1)
    metrics2 = get_run_metrics(scraper_name, run_id2)
    
    if not metrics1 or not metrics2:
        print("Error: Could not load metrics for one or both runs")
        return
    
    print(f"\n{'='*80}")
    print(f"Run Comparison: {scraper_name}")
    print(f"{'='*80}\n")
    
    print(f"Run 1: {run_id1}")
    print(f"  Status: {metrics1['status']}")
    print(f"  Started: {metrics1['started_at']}")
    print(f"  Total Runtime: {metrics1['total_runtime_seconds']:.1f}s")
    print(f"  Steps Completed: {metrics1['step_count']}")
    
    print(f"\nRun 2: {run_id2}")
    print(f"  Status: {metrics2['status']}")
    print(f"  Started: {metrics2['started_at']}")
    print(f"  Total Runtime: {metrics2['total_runtime_seconds']:.1f}s")
    print(f"  Steps Completed: {metrics2['step_count']}")
    
    # Compare total runtime
    runtime_diff = metrics2['total_runtime_seconds'] - metrics1['total_runtime_seconds']
    runtime_pct = (runtime_diff / metrics1['total_runtime_seconds'] * 100) if metrics1['total_runtime_seconds'] > 0 else 0
    print(f"\n{'='*80}")
    print(f"Total Runtime Comparison:")
    print(f"  Difference: {runtime_diff:+.1f}s ({runtime_pct:+.1f}%)")
    if abs(runtime_pct) > 10:
        print(f"  ⚠️  Significant difference detected (>10%)")
    
    # Compare steps
    print(f"\n{'='*80}")
    print(f"Step-by-Step Comparison:")
    print(f"{'Step':<20} {'Run 1 Duration':<20} {'Run 2 Duration':<20} {'Difference':<20}")
    print("-" * 80)
    
    all_steps = set(metrics1['steps'].keys()) | set(metrics2['steps'].keys())
    for step_num in sorted(all_steps):
        step1 = metrics1['steps'].get(step_num, {})
        step2 = metrics2['steps'].get(step_num, {})
        
        name1 = step1.get('step_name', 'N/A')
        name2 = step2.get('step_name', 'N/A')
        name = name1 if name1 != 'N/A' else name2
        
        dur1 = step1.get('duration_seconds', 0)
        dur2 = step2.get('duration_seconds', 0)
        diff = dur2 - dur1
        diff_pct = (diff / dur1 * 100) if dur1 > 0 else 0
        
        marker = ""
        if abs(diff_pct) > 20:
            marker = " ⚠️"
        
        print(f"{name:<20} {dur1:>15.1f}s {dur2:>15.1f}s {diff:>+15.1f}s ({diff_pct:>+5.1f}%){marker}")
    
    # Compare row counts
    print(f"\n{'='*80}")
    print(f"Row Count Comparison:")
    print(f"{'Step':<20} {'Run 1 Processed':<20} {'Run 2 Processed':<20} {'Difference':<20}")
    print("-" * 80)
    
    for step_num in sorted(all_steps):
        step1 = metrics1['steps'].get(step_num, {})
        step2 = metrics2['steps'].get(step_num, {})
        
        name1 = step1.get('step_name', 'N/A')
        name2 = step2.get('step_name', 'N/A')
        name = name1 if name1 != 'N/A' else name2
        
        rows1 = step1.get('rows_processed', 0)
        rows2 = step2.get('rows_processed', 0)
        diff = rows2 - rows1
        
        marker = ""
        if abs(diff) > 0 and rows1 > 0:
            diff_pct = (diff / rows1 * 100)
            if abs(diff_pct) > 10:
                marker = " ⚠️"
        
        print(f"{name:<20} {rows1:>15,} {rows2:>15,} {diff:>+15,}{marker}")


def main():
    parser = argparse.ArgumentParser(description="Compare two pipeline runs")
    parser.add_argument("scraper_name", help="Scraper name (e.g., Malaysia)")
    parser.add_argument("run_id1", help="First run ID")
    parser.add_argument("run_id2", help="Second run ID")
    
    args = parser.parse_args()
    compare_runs(args.scraper_name, args.run_id1, args.run_id2)


if __name__ == "__main__":
    main()
