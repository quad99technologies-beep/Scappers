#!/usr/bin/env python3
"""
Cleanup old run from database.
Usage: python cleanup_run.py <scraper_name> <run_id>
Example: python cleanup_run.py Malaysia Malaysia_20260216_051417
"""

import sys
import os

# Add repo root to path
_repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from core.db.connection import CountryDB


def cleanup_run(scraper_name: str, run_id: str):
    """Delete all data for a specific run_id from the database."""
    
    print(f"Cleaning up run: {run_id} for scraper: {scraper_name}")
    
    with CountryDB(scraper_name) as db:
        with db.cursor() as cur:
            # First, delete from tables that might have foreign key references
            # Common tables that reference run_ledger
            reference_tables = [
                'http_requests',
                'scrape_stats_snapshots', 
                'pipeline_checkpoints',
                'frontier_queue',
                'artifacts',
                'errors',
                'progress'
            ]
            
            for table in reference_tables:
                try:
                    cur.execute(f"DELETE FROM {table} WHERE run_id = %s", (run_id,))
                    if cur.rowcount > 0:
                        print(f"  - Deleted from {table}: {cur.rowcount} rows")
                except Exception as e:
                    # Rollback and continue
                    db.rollback()
                    pass
            
            # Get list of tables for this scraper
            try:
                cur.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name LIKE %s
                """, (f'{scraper_name.lower()}%',))
                
                scraper_tables = [row[0] for row in cur.fetchall()]
                
                # Delete from scraper-specific tables first
                for table in scraper_tables:
                    try:
                        cur.execute(f"DELETE FROM {table} WHERE run_id = %s", (run_id,))
                        if cur.rowcount > 0:
                            print(f"  - Deleted from {table}: {cur.rowcount} rows")
                    except Exception as e:
                        db.rollback()
                        pass
            except Exception:
                pass
            
            # Finally delete from run_ledger
            try:
                cur.execute("DELETE FROM run_ledger WHERE run_id = %s", (run_id,))
                print(f"  - Deleted from run_ledger: {cur.rowcount} rows")
            except Exception as e:
                print(f"  - Could not delete from run_ledger: {e}")
            
            db.commit()
    
    print(f"\n✓ Cleanup complete for run_id: {run_id}")
    print("You can now start a fresh run.")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python cleanup_run.py <scraper_name> <run_id>")
        print("Example: python cleanup_run.py Malaysia Malaysia_20260216_051417")
        sys.exit(1)
    
    scraper_name = sys.argv[1]
    run_id = sys.argv[2]
    
    cleanup_run(scraper_name, run_id)
