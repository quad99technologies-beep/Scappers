#!/usr/bin/env python3
"""
Deploy all database migrations for Malaysia, Argentina, and Netherlands
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

def run_migration(migration_file: Path) -> bool:
    """Run a single migration file."""
    if not migration_file.exists():
        print(f"[ERROR] Migration file not found: {migration_file}")
        return False
    
    # Get database connection parameters
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
        
        # Read and execute migration
        migration_sql = migration_file.read_text(encoding="utf-8")
        
        print(f"Executing: {migration_file.name}...")
        cur.execute(migration_sql)
        conn.commit()
        
        cur.close()
        conn.close()
        
        print(f"[OK] {migration_file.name} completed")
        return True
        
    except psycopg2.Error as e:
        print(f"[ERROR] Database error in {migration_file.name}: {e}")
        return False
    except Exception as e:
        print(f"[ERROR] Error in {migration_file.name}: {e}")
        return False

def main():
    """Run all migrations."""
    migrations_dir = Path(__file__).parent.parent / "sql" / "migrations" / "postgres"
    
    # List of migrations to run (in order)
    migrations = [
        "005_add_step_tracking_columns.sql",  # Enhanced step tracking
        "006_add_chrome_instances_table.sql",  # Chrome instance tracking
        "007_add_run_ledger_live_fields.sql",  # Optional live tracking fields
    ]
    
    print("=" * 60)
    print("Deploying All Migrations")
    print("=" * 60)
    print()
    
    success_count = 0
    failed_count = 0
    
    for migration_name in migrations:
        migration_file = migrations_dir / migration_name
        if run_migration(migration_file):
            success_count += 1
        else:
            failed_count += 1
        print()
    
    # Verify migrations
    print("=" * 60)
    print("Verification")
    print("=" * 60)
    
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
        
        # Check schema versions
        cur.execute("SELECT version, filename FROM _schema_versions ORDER BY version")
        versions = cur.fetchall()
        print("\n[OK] Schema versions:")
        for version, filename in versions:
            print(f"  Version {version}: {filename}")
        
        # Check chrome_instances table
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'chrome_instances'
            )
        """)
        chrome_table_exists = cur.fetchone()[0]
        if chrome_table_exists:
            print("\n[OK] chrome_instances table exists")
        else:
            print("\n[WARN] chrome_instances table not found")
        
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
            print(f"\n[OK] Enhanced columns in my_step_progress: {', '.join(cols)}")
        else:
            print("\n[WARN] Enhanced columns not found")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"\n[ERROR] Verification failed: {e}")
    
    print()
    print("=" * 60)
    print(f"Summary: {success_count} succeeded, {failed_count} failed")
    print("=" * 60)
    
    return failed_count == 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
