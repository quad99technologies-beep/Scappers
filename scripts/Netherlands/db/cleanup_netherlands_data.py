#!/usr/bin/env python3
"""
Stand-alone script to cleanly delete all Netherlands scraper data from the database.
Handles large-scale deletions by ordering operations and setting appropriate timeouts.
"""

import sys
import os
from pathlib import Path

# Add project root to path
repo_root = Path(__file__).resolve().parents[3]
sys.path.append(str(repo_root))

from core.db.postgres_connection import get_db

def cleanup_netherlands():
    print("=" * 60)
    print("NETHERLANDS DATA CLEANUP")
    print("=" * 60)
    
    db = get_db("Netherlands")
    
    try:
        # 1. Increase statement timeout for this session
        print("[INIT] Setting statement_timeout to 1 hour...")
        with db.cursor() as cur:
            cur.execute("SET statement_timeout = '3600s'")
        
        # 2. Get all Netherlands Run IDs
        print("[INIT] Fetching Netherlands run IDs...")
        with db.cursor() as cur:
            cur.execute("SELECT run_id FROM run_ledger WHERE scraper_name = 'Netherlands'")
            run_ids = [row[0] for row in cur.fetchall()]
        
        print(f"[INIT] Found {len(run_ids)} runs associated with Netherlands")
        
        if not run_ids:
            print("[INFO] No Netherlands runs found. Checking nl_ tables anyway...")
        
        # 3. List of Netherlands-specific tables (names starting with nl_)
        nl_tables = [
            "nl_packs",
            "nl_collected_urls",
            "nl_chrome_instances",
            "nl_search_combinations",
            "nl_step_progress",
            "nl_export_reports",
            "nl_errors"
        ]
        
        # 4. Delete from child tables first to respect foreign keys
        print("\n" + "-" * 40)
        print("PHASE 1: Deleting from Netherlands-specific tables")
        print("-" * 40)
        
        for table in nl_tables:
            print(f"[DELETE] Deleting from {table}...", end="", flush=True)
            try:
                with db.cursor() as cur:
                    cur.execute(f"DELETE FROM {table}")
                    count = cur.rowcount
                print(f" OK ({count} rows)")
            except Exception as e:
                print(f" FAILED: {e}")
                db.rollback()

        # 5. Delete from shared tables for these run IDs
        if run_ids:
            print("\n" + "-" * 40)
            print("PHASE 2: Deleting from Shared tables (by run_id)")
            print("-" * 40)
            
            shared_tables = [
                "scraped_items",
                "http_requests",
                "data_quality_checks",
                "step_retries"
            ]
            
            # Batching to avoid huge IN clauses if there are thousands of runs
            batch_size = 100
            for table in shared_tables:
                total_deleted = 0
                print(f"[DELETE] Deleting from {table} in batches of {batch_size}...", end="", flush=True)
                for i in range(0, len(run_ids), batch_size):
                    batch = run_ids[i:i+batch_size]
                    try:
                        with db.cursor() as cur:
                            cur.execute(f"DELETE FROM {table} WHERE run_id IN %s", (tuple(batch),))
                            total_deleted += cur.rowcount
                    except Exception as e:
                        # Some tables might not exist or have different column names
                        if "does not exist" in str(e).lower():
                            pass
                        else:
                            print(f"\n[WARN] Failed to delete from {table}: {e}")
                        break
                print(f" OK ({total_deleted} rows)")

        # 6. Optimized run_ledger cleanup (handle missing indexes in foreign keys)
        print("\n" + "-" * 40)
        print("PHASE 3: Final cleanup of run_ledger (Optimized)")
        print("-" * 40)
        
        # Check if in_brand_alternatives_old exists and create temp index to avoid table scans
        print("[DB] Checking for orphan foreign key references...")
        with db.cursor() as cur:
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'in_brand_alternatives_old'")
            has_alt_table = cur.fetchone()
            
            if has_alt_table:
                print("[DB] Found in_brand_alternatives_old. Creating temporary index on run_id...")
                try:
                    cur.execute("CREATE INDEX IF NOT EXISTS tmp_idx_brand_alt_run_id ON in_brand_alternatives_old(run_id)")
                    db.commit()
                    print("[DB] Index created.")
                except Exception as e:
                    print(f"[DB] Could not create index: {e}")
                    db.rollback()

        print("[DELETE] Deleting Netherlands entries from run_ledger...", end="", flush=True)
        try:
            with db.cursor() as cur:
                cur.execute("DELETE FROM run_ledger WHERE scraper_name = 'Netherlands'")
                count = cur.rowcount
            print(f" OK ({count} rows)")
        except Exception as e:
            print(f" FAILED: {e}")
            
        # Optional: Cleanup temp index
        if has_alt_table:
            print("[DB] Cleaning up temporary index...")
            with db.cursor() as cur:
                try:
                    cur.execute("DROP INDEX IF EXISTS tmp_idx_brand_alt_run_id")
                    db.commit()
                except Exception:
                    db.rollback()

        print("\n" + "=" * 60)
        print("CLEANUP COMPLETE")
        print("=" * 60)
        
    finally:
        db.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="Force deletion without confirmation")
    args = parser.parse_args()
    
    if args.force:
        cleanup_netherlands()
    else:
        confirm = input("Are you sure you want to delete ALL Netherlands data? (y/N): ")
        if confirm.lower() == 'y':
            cleanup_netherlands()
        else:
            print("Cleanup aborted.")
