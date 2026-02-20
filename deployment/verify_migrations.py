#!/usr/bin/env python3
"""
Verify all migrations are applied for Malaysia, Argentina, and Netherlands
"""

import sys
import os
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

try:
    import psycopg2
    from dotenv import load_dotenv
except ImportError as e:
    print(f"[ERROR] Missing dependency: {e}")
    sys.exit(1)

# Load environment variables
load_dotenv()

def verify_migrations():
    """Verify all migrations are applied."""
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    database = os.getenv("POSTGRES_DB", "scrappers")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")
    
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        cur = conn.cursor()
        
        print("=" * 60)
        print("Migration Verification")
        print("=" * 60)
        print()
        
        # Check schema versions
        print("Schema Versions:")
        cur.execute("SELECT version, filename FROM _schema_versions ORDER BY version")
        versions = cur.fetchall()
        for version, filename in versions:
            print(f"  Version {version}: {filename}")
        print()
        
        # Check tables
        print("Table Verification:")
        tables = [
            'my_step_progress', 'ar_step_progress', 'nl_step_progress',
            'chrome_instances', 'run_ledger', 'http_requests', 'step_retries',
            'scraper_run_statistics', 'scraper_step_statistics',
        ]
        for table in tables:
            cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)", (table,))
            exists = cur.fetchone()[0]
            status = "[OK]" if exists else "[MISSING]"
            print(f"  {status} {table}")
        print()
        
        # Check enhanced columns
        print("Enhanced Columns Check:")
        for prefix in ['my', 'ar', 'nl']:
            table_name = f"{prefix}_step_progress"
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s 
                AND column_name IN ('duration_seconds', 'rows_read', 'log_file_path')
                ORDER BY column_name
            """, (table_name,))
            cols = [r[0] for r in cur.fetchall()]
            if cols:
                print(f"  [OK] {table_name}: {len(cols)}/3 columns - {', '.join(cols)}")
            else:
                print(f"  [WARN] {table_name}: Enhanced columns not found (table may not exist)")
        print()
        
        # Check run_ledger enhanced columns
        print("Run Ledger Enhanced Columns:")
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'run_ledger' 
            AND column_name IN ('total_runtime_seconds', 'slowest_step_number', 'failure_step_number')
            ORDER BY column_name
        """)
        cols = [r[0] for r in cur.fetchall()]
        if cols:
            print(f"  [OK] Found: {', '.join(cols)}")
        else:
            print("  [WARN] Enhanced columns not found")
        print()
        
        # Check chrome_instances table structure
        print("Chrome Instances Table Structure:")
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'chrome_instances' 
            ORDER BY ordinal_position
        """)
        cols = cur.fetchall()
        if cols:
            for col, dtype in cols:
                print(f"  {col}: {dtype}")
            # Explicit check for all_pids (migration 008)
            has_all_pids = any(col == "all_pids" for col, _ in cols)
            print(f"  all_pids column: {'[OK]' if has_all_pids else '[MISSING]'}")
        else:
            print("  [WARN] chrome_instances table not found")
        print()
        
        cur.close()
        conn.close()
        
        print("=" * 60)
        print("[OK] Verification complete!")
        print("=" * 60)
        return True
        
    except Exception as e:
        print(f"[ERROR] Verification failed: {e}")
        return False

if __name__ == "__main__":
    success = verify_migrations()
    sys.exit(0 if success else 1)
