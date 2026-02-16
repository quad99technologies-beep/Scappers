
import sys
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

try:
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="scrappers",
        user="postgres",
        password="admin123"
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    
    # 1. Truncate nl_ tables
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'nl_%'")
    tables = [row[0] for row in cur.fetchall()]
    
    for table in tables:
        if table == "nl_input_search_terms":
            print(f"Dropping {table}...")
            cur.execute(f'DROP TABLE "{table}" CASCADE')
        else:
            print(f"Truncating {table}...")
            cur.execute(f'TRUNCATE TABLE "{table}" CASCADE')

    # 2. Cleanup run_ledger
    print("Cleaning run_ledger for Netherlands...")
    cur.execute("DELETE FROM run_ledger WHERE scraper_name = 'Netherlands'")
    
    # 3. Cleanup chrome_instances
    print("Cleaning chrome_instances for Netherlands...")
    cur.execute("DELETE FROM chrome_instances WHERE scraper_name = 'Netherlands'")

    # 4. Cleanup input_uploads
    print("Cleaning input_uploads for Netherlands...")
    cur.execute("DELETE FROM input_uploads WHERE table_name LIKE 'nl_%'")
    
    print("Manual cleanup completed successfully.")
    
except Exception as e:
    print(f"Error during manual cleanup: {e}")
finally:
    if 'conn' in locals() and conn:
        conn.close()
