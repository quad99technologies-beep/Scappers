#!/usr/bin/env python3
"""
Fix checkpoint run_id issue:
1. Delete all records for run_id 20260204_015528_443a85fc
2. Set run_id 20260206_160604_5d97a684 to have step 2 completed and step 3 pending
3. Update checkpoint metadata and .current_run_id file
"""

import sys
from pathlib import Path

# Add repo root to path
repo_root = Path(__file__).resolve().parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from core.db.connection import CountryDB
from scripts.Argentina.db.schema import apply_argentina_schema
from scripts.Argentina.config_loader import get_output_dir
from core.pipeline_checkpoint import get_checkpoint_manager
import os

OLD_RUN_ID = '20260204_015528_443a85fc'
CORRECT_RUN_ID = '20260206_160604_5d97a684'

def delete_run_id_records(run_id):
    """Delete all records for a run_id across all Argentina tables."""
    print(f"\n[DELETE] Deleting all records for run_id: {run_id}")
    
    with CountryDB("Argentina") as db:
        apply_argentina_schema(db)
        
        # Get all Argentina tables
        with db.cursor() as cur:
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                  AND table_name LIKE 'ar_%'
                ORDER BY table_name
            """)
            tables = [row[0] for row in cur.fetchall()]
        
        deleted_counts = {}
        
        # Delete from all tables (except run_ledger - do that last)
        for table in tables:
            if table == 'run_ledger':
                continue
            try:
                with db.cursor() as cur:
                    cur.execute(f'DELETE FROM "{table}" WHERE run_id = %s', (run_id,))
                    count = cur.rowcount
                    if count > 0:
                        deleted_counts[table] = count
                        print(f"  Deleted {count} rows from {table}")
            except Exception as e:
                print(f"  Warning: Could not delete from {table}: {e}")
        
        # Delete from run_ledger last
        try:
            with db.cursor() as cur:
                cur.execute('DELETE FROM "run_ledger" WHERE run_id = %s', (run_id,))
                count = cur.rowcount
                if count > 0:
                    deleted_counts['run_ledger'] = count
                    print(f"  Deleted {count} rows from run_ledger")
        except Exception as e:
            print(f"  Warning: Could not delete from run_ledger: {e}")
        
        db.commit()
        
        total_deleted = sum(deleted_counts.values())
        print(f"[DELETE] Total rows deleted: {total_deleted}")
        return total_deleted

def set_checkpoint_steps(run_id, step_2_completed=True, step_3_pending=True):
    """Set step 2 as completed and step 3 as pending for the run_id."""
    print(f"\n[CHECKPOINT] Setting steps for run_id: {run_id}")
    
    with CountryDB("Argentina") as db:
        apply_argentina_schema(db)
        
        # Ensure run_id exists in run_ledger
        from core.db.models import run_ledger_ensure_exists
        sql, params = run_ledger_ensure_exists(run_id, "Argentina", mode="resume")
        with db.cursor() as cur:
            cur.execute(sql, params)
        db.commit()
        print(f"  Ensured run_id exists in run_ledger")
        
        # Update step 2 to completed
        if step_2_completed:
            with db.cursor() as cur:
                cur.execute("""
                    INSERT INTO ar_step_progress
                        (run_id, step_number, step_name, progress_key, status, completed_at)
                    VALUES
                        (%s, 2, 'Prepare URLs', 'pipeline', 'completed', CURRENT_TIMESTAMP)
                    ON CONFLICT (run_id, step_number, progress_key) DO UPDATE SET
                        step_name = EXCLUDED.step_name,
                        status = 'completed',
                        completed_at = CURRENT_TIMESTAMP,
                        error_message = NULL
                """, (run_id,))
                print(f"  Step 2 marked as completed")
        
        # Update step 3 to pending
        if step_3_pending:
            with db.cursor() as cur:
                cur.execute("""
                    INSERT INTO ar_step_progress
                        (run_id, step_number, step_name, progress_key, status)
                    VALUES
                        (%s, 3, 'Scrape Products (Selenium)', 'pipeline', 'pending')
                    ON CONFLICT (run_id, step_number, progress_key) DO UPDATE SET
                        step_name = EXCLUDED.step_name,
                        status = 'pending',
                        completed_at = NULL,
                        error_message = NULL
                """, (run_id,))
                print(f"  Step 3 marked as pending")
        
        db.commit()
        
        # Update checkpoint file
        cp = get_checkpoint_manager("Argentina")
        cp.update_metadata({"run_id": run_id})
        
        # Mark step 2 as complete in checkpoint
        if step_2_completed:
            cp.mark_step_complete(2, "Prepare URLs")
            print(f"  Checkpoint file: Step 2 marked complete")
        
        # Ensure step 3 is NOT marked complete in checkpoint
        # (it should be pending, so we don't mark it)
        print(f"  Checkpoint file: Step 3 left as pending")
        
        # Update .current_run_id file
        output_dir = get_output_dir()
        run_id_file = output_dir / '.current_run_id'
        run_id_file.parent.mkdir(parents=True, exist_ok=True)
        run_id_file.write_text(run_id, encoding='utf-8')
        print(f"  Updated .current_run_id file")
        
        # Set in environment
        os.environ["ARGENTINA_RUN_ID"] = run_id
        print(f"  Set ARGENTINA_RUN_ID environment variable")

def main():
    print("=" * 80)
    print("FIX CHECKPOINT RUN_ID ISSUE")
    print("=" * 80)
    
    # Step 1: Delete all records for old run_id
    print(f"\n[STEP 1] Deleting records for old run_id: {OLD_RUN_ID}")
    deleted = delete_run_id_records(OLD_RUN_ID)
    
    # Step 2: Set correct run_id checkpoint
    print(f"\n[STEP 2] Setting checkpoint for correct run_id: {CORRECT_RUN_ID}")
    set_checkpoint_steps(CORRECT_RUN_ID, step_2_completed=True, step_3_pending=True)
    
    # Step 3: Verify
    print(f"\n[VERIFY] Verifying checkpoint state:")
    cp = get_checkpoint_manager("Argentina")
    metadata = cp.get_metadata() or {}
    checkpoint_run_id = metadata.get('run_id', 'NOT SET')
    print(f"  Checkpoint metadata run_id: {checkpoint_run_id}")
    
    info = cp.get_checkpoint_info()
    completed_steps = info.get("completed_steps", [])
    print(f"  Completed steps: {completed_steps}")
    
    output_dir = get_output_dir()
    run_id_file = output_dir / '.current_run_id'
    if run_id_file.exists():
        file_run_id = run_id_file.read_text(encoding='utf-8').strip()
        print(f"  .current_run_id file: {file_run_id}")
    else:
        print(f"  .current_run_id file: NOT FOUND")
    
    print("\n" + "=" * 80)
    print("FIX COMPLETE")
    print("=" * 80)
    print(f"\nSummary:")
    print(f"  - Deleted {deleted} records for old run_id: {OLD_RUN_ID}")
    print(f"  - Set correct run_id: {CORRECT_RUN_ID}")
    print(f"  - Step 2: completed")
    print(f"  - Step 3: pending")
    print(f"\nThe pipeline should now resume from step 3 using run_id {CORRECT_RUN_ID}")

if __name__ == "__main__":
    main()
