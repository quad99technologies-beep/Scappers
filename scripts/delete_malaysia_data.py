
import sys
import os
import psycopg2

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from core.db.postgres_connection import PostgresDB
except ImportError:
    print("Could not import PostgresDB.")
    sys.exit(1)

def force_delete_malaysia():
    db = PostgresDB("Malaysia")
    
    print("Connecting to database...", flush=True)
    
    # 1. Get Malaysia run_ids
    run_ids = []
    try:
        with db.cursor() as cur:
            print("Finding Malaysia runs...", flush=True)
            cur.execute("SELECT run_id FROM run_ledger WHERE scraper_name ILIKE '%Malaysia%'")
            rows = cur.fetchall()
            run_ids = [row[0] for row in rows]
            print(f"Found {len(run_ids)} runs to delete.", flush=True)
    except Exception as e:
        print(f"Error finding runs: {e}", flush=True)
        return

    if not run_ids:
        print("No runs found. Exiting.", flush=True)
        return

    run_ids_tuple = tuple(run_ids)
    
    # 2. Get list of tables referencing run_ledger
    referencing_tables = []
    try:
        with db.cursor() as cur:
            cur.execute("""
                SELECT
                    tc.table_name, 
                    kcu.column_name
                FROM 
                    information_schema.table_constraints AS tc 
                    JOIN information_schema.key_column_usage AS kcu
                      ON tc.constraint_name = kcu.constraint_name
                      AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage AS ccu
                      ON ccu.constraint_name = tc.constraint_name
                      AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY' AND ccu.table_name='run_ledger';
            """)
            rows = cur.fetchall()
            referencing_tables = [(row[0], row[1]) for row in rows]
            print(f"Found {len(referencing_tables)} tables referencing run_ledger.", flush=True)
    except Exception as e:
        print(f"Error finding dependencies: {e}", flush=True)
        return

    # 3. Delete from all referencing tables
    for table, column in referencing_tables:
        # Skip if table is my_* because they are already truncated (but double check is harmless)
        # Skip if table doesn't actually contain these run_ids (delete 0 is fast)
        
        # We need to be careful with 'input_uploads' if it has no run_id column but appeared in my view? 
        # The query returns table_name and column_name. So it definitely has the column.
        
        try:
            with db.cursor() as cur:
                 # Check if table has any of these run_ids to avoid lock wait if not needed
                 # This check might be slow if table is huge. 
                 # But DELETE with WHERE run_id IN (...) is implicitly checking keys.
                 
                 # Just try to delete.
                 print(f"Cleaning {table}...", flush=True)
                 cur.execute(f"DELETE FROM {table} WHERE {column} IN %s", (run_ids_tuple,))
        except Exception as e:
            print(f"Error cleaning {table}: {e}", flush=True)

    # 4. Finally delete from run_ledger
    try:
        with db.cursor() as cur:
            print("Deleting from run_ledger...", flush=True)
            cur.execute("DELETE FROM run_ledger WHERE run_id IN %s", (run_ids_tuple,))
            print("Deleted runs from run_ledger.", flush=True)
    except Exception as e:
        print(f"Error deleting from run_ledger: {e}", flush=True)
        
    print("Force deletion complete.", flush=True)

if __name__ == "__main__":
    force_delete_malaysia()
