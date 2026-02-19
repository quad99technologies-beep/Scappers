#!/usr/bin/env python3
"""
Delete Malaysia run from database.
Run: python scripts\delete_malaysia_run.py
"""

import sys
import os

# Add repo root to path
_repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv(os.path.join(_repo_root, '.env'))


def get_db_connection():
    """Get database connection from environment variables."""
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        database=os.getenv('POSTGRES_DB', 'pharma_db'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', '')
    )


def delete_malaysia_run():
    """Delete Malaysia run from database."""
    
    run_id = 'Malaysia_20260216_051417'
    
    print(f"Deleting run: {run_id}")
    print("=" * 60)
    
    conn = get_db_connection()
    conn.autocommit = True
    
    try:
        with conn.cursor() as cur:
            # Delete from tables with foreign key references first
            tables_to_delete = [
                'http_requests',
                'malaysia_product_index',
                'malaysia_products',
                'malaysia_product_details',
                'malaysia_progress',
                'malaysia_errors',
                'malaysia_artifacts',
                'run_ledger'
            ]
            
            for table in tables_to_delete:
                try:
                    cur.execute(f"DELETE FROM {table} WHERE run_id = %s", (run_id,))
                    if cur.rowcount > 0:
                        print(f"[OK] Deleted from {table}: {cur.rowcount} rows")
                except Exception as e:
                    error_msg = str(e)
                    if "does not exist" in error_msg:
                        print(f"[SKIP] {table}: table does not exist")
                    else:
                        print(f"[ERR] {table}: {error_msg[:50]}")
            
            print("=" * 60)
            print(f"[DONE] Run {run_id} cleanup completed!")
            print("You can now start a fresh run.")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    delete_malaysia_run()
