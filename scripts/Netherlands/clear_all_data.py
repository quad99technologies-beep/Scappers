
import sys
import os
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parent.parent.parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.db.postgres_connection import get_db

def clear_all_data():
    print("Connecting to Netherlands DB...")
    cleanup_db = None
    try:
        cleanup_db = get_db("Netherlands")
        
        # Kill active connections first to break locks
        print("Terminating other connections to break locks...")
        try:
            # We need a raw connection for this
            conn = cleanup_db.connect()
            # Set autocommit to True for pg_terminate_backend
            conn.autocommit = True
            
            with conn.cursor() as cur:
                # Terminate all connections to this database except our own
                cur.execute("""
                    SELECT pg_terminate_backend(pid) 
                    FROM pg_stat_activity 
                    WHERE datname = current_database() 
                      AND pid <> pg_backend_pid()
                      AND application_name != 'psql'
                """)
            print("Terminated other connections.")
        except Exception as e:
            print(f"Warning: Could not terminate connections (might need superuser): {e}")
        finally:
            if cleanup_db:
                cleanup_db.close() # Return to pool/close
    except Exception as e:
        print(f"Error during connection termination: {e}")

    # Re-connect for actual cleanup
    print("Re-connecting for cleanup...")
    try:
        db = get_db("Netherlands")
        
        with db.cursor() as cur:
            # Get all nl_ tables
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'nl_%'")
            tables = [row[0] for row in cur.fetchall()]
            
            if not tables:
                print("No nl_ tables found.")
            else:
                print(f"Found tables: {tables}")
                for table in tables:
                    if table == "nl_input_search_terms":
                        print(f"Dropping table {table} (as requested)...")
                        cur.execute(f'DROP TABLE "{table}" CASCADE')
                    else:
                        print(f"Truncating {table}...")
                        cur.execute(f'TRUNCATE TABLE "{table}" CASCADE')

            # Clean up run_ledger
            print("Cleaning up run_ledger for Netherlands...")
            cur.execute("DELETE FROM run_ledger WHERE scraper_name = 'Netherlands'")
            
            # Clean up chrome_instances (shared table but filtered by scraper_name)
            print("Cleaning up chrome_instances for Netherlands...")
            cur.execute("DELETE FROM chrome_instances WHERE scraper_name = 'Netherlands'")

            # Clean up input_uploads for Netherlands tables
            print("Cleaning up input_uploads for Netherlands...")
            cur.execute("DELETE FROM input_uploads WHERE table_name LIKE 'nl_%'")
            
            # Clean up pcid_mapping for Netherlands
            print("Cleaning up pcid_mapping for Netherlands...")
            cur.execute("DELETE FROM pcid_mapping WHERE source_country = 'Netherlands'")
            
        db.commit()
        print("All Netherlands data cleared.")
        
    except Exception as e:
        print(f"Error during cleanup: {e}")

if __name__ == "__main__":
    clear_all_data()
