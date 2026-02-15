#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Run Rollback Capability

Revert to a previous run's state.

Usage:
    python core/run_rollback.py Malaysia run_20260201_abc
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime

# Add repo root to path
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from core.db.postgres_connection import get_db, COUNTRY_PREFIX_MAP
from core.audit_logger import audit_log


def create_snapshot(scraper_name: str, run_id: str) -> bool:
    """Create a snapshot of current run state."""
    try:
        db = get_db(scraper_name)
        prefix = COUNTRY_PREFIX_MAP.get(scraper_name, scraper_name.lower()[:2])
        
        with db.cursor() as cur:
            # Check if table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'run_snapshots'
                )
            """)
            table_exists = cur.fetchone()[0]
            
            if not table_exists:
                cur.execute("""
                    CREATE TABLE run_snapshots (
                        id SERIAL PRIMARY KEY,
                        run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
                        scraper_name TEXT NOT NULL,
                        snapshot_type TEXT,
                        step_number INTEGER,
                        table_name TEXT NOT NULL,
                        row_count INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE INDEX idx_snapshots_run ON run_snapshots(run_id);
                """)
                db.commit()
            
            # Get table counts
            tables = [
                f"{prefix}_products",
                f"{prefix}_step_progress",
            ]
            
            for table in tables:
                try:
                    cur.execute(f"""
                        SELECT COUNT(*) FROM {table}
                        WHERE run_id = %s
                    """, (run_id,))
                    count = cur.fetchone()[0]
                    
                    cur.execute("""
                        INSERT INTO run_snapshots
                            (run_id, scraper_name, snapshot_type, table_name, row_count)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (run_id, scraper_name, "final", table, count))
                except Exception:
                    pass
            
            db.commit()
            return True
    except Exception as e:
        print(f"Error creating snapshot: {e}")
        return False


def rollback_to_run(scraper_name: str, target_run_id: str) -> bool:
    """
    Rollback to a previous run's state.
    
    WARNING: This deletes data from runs after target_run_id.
    """
    try:
        db = get_db(scraper_name)
        prefix = COUNTRY_PREFIX_MAP.get(scraper_name, scraper_name.lower()[:2])
        
        with db.cursor() as cur:
            # Get all run_ids after target
            cur.execute("""
                SELECT run_id FROM run_ledger
                WHERE scraper_name = %s
                AND started_at > (
                    SELECT started_at FROM run_ledger
                    WHERE run_id = %s AND scraper_name = %s
                )
                ORDER BY started_at
            """, (scraper_name, target_run_id, scraper_name))
            
            runs_to_delete = [row[0] for row in cur.fetchall()]
            
            if not runs_to_delete:
                print("No runs to rollback")
                return True
            
            print(f"Rolling back {len(runs_to_delete)} run(s)...")
            
            # Delete data from tables
            tables = [
                f"{prefix}_products",
                f"{prefix}_step_progress",
            ]
            
            for table in tables:
                for run_id in runs_to_delete:
                    try:
                        cur.execute(f"""
                            DELETE FROM {table}
                            WHERE run_id = %s
                        """, (run_id,))
                        deleted = cur.rowcount
                        if deleted > 0:
                            print(f"  Deleted {deleted} rows from {table} (run: {run_id})")
                    except Exception:
                        pass
            
            db.commit()
            
            # Audit log
            audit_log(
                action="run_rollback",
                scraper_name=scraper_name,
                run_id=target_run_id,
                user="system",
                details={"deleted_runs": runs_to_delete}
            )
            
            print("Rollback completed")
            return True
    except Exception as e:
        print(f"Error during rollback: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Rollback to a previous run")
    parser.add_argument("scraper_name", help="Scraper name")
    parser.add_argument("run_id", help="Target run ID to rollback to")
    parser.add_argument("--confirm", action="store_true", help="Confirm rollback")
    
    args = parser.parse_args()
    
    if not args.confirm:
        print("WARNING: This will delete data from runs after the target run.")
        print("Use --confirm to proceed.")
        return
    
    rollback_to_run(args.scraper_name, args.run_id)


if __name__ == "__main__":
    main()
