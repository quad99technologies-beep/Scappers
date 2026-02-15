#!/usr/bin/env python3
"""
Deploy database migration 005: Add Enhanced Step Tracking Columns
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
    print("Install with: pip install psycopg2-binary python-dotenv")
    sys.exit(1)

# Load environment variables
load_dotenv()

def run_migration():
    """Run the database migration."""
    migration_file = Path(__file__).parent.parent / "sql" / "migrations" / "postgres" / "005_add_step_tracking_columns.sql"
    
    if not migration_file.exists():
        print(f"[ERROR] Migration file not found: {migration_file}")
        sys.exit(1)
    
    # Get database connection parameters
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    database = os.getenv("POSTGRES_DB", "scrappers")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")
    
    print(f"Connecting to PostgreSQL: {user}@{host}:{port}/{database}")
    
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        cur = conn.cursor()
        
        # Read and execute migration
        print(f"Reading migration file: {migration_file}")
        migration_sql = migration_file.read_text(encoding="utf-8")
        
        print("Executing migration...")
        cur.execute(migration_sql)
        conn.commit()
        
        # Verify migration
        cur.execute("SELECT version FROM _schema_versions WHERE version = 5")
        result = cur.fetchone()
        
        if result:
            print("[OK] Migration successful! Schema version 5 recorded.")
        else:
            print("[WARN] Migration executed but version not found in _schema_versions")
        
        # Check enhanced columns
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'my_step_progress' 
            AND column_name IN ('duration_seconds', 'rows_read', 'log_file_path')
            ORDER BY column_name
        """)
        cols = [r[0] for r in cur.fetchall()]
        if cols:
            print(f"[OK] Enhanced columns verified: {', '.join(cols)}")
        else:
            print("[WARN] Enhanced columns not found (table may not exist yet)")
        
        cur.close()
        conn.close()
        
        print("\n[OK] Deployment complete!")
        return True
        
    except psycopg2.Error as e:
        print(f"[ERROR] Database error: {e}")
        return False
    except Exception as e:
        print(f"[ERROR] Error: {e}")
        return False

if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)
