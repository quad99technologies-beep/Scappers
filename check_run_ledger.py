
import sys
import os
import psycopg2
from psycopg2.extras import RealDictCursor

# Connect to DB
try:
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="scrappers",
        user="postgres",
        password="admin123"
    )
    cur = conn.cursor(cursor_factory=RealDictCursor)

    run_id = "nl_20260216_185910"
    
    print(f"Checking run_ledger for run_id: {run_id}")
    cur.execute("SELECT * FROM run_ledger WHERE run_id = %s", (run_id,))
    row = cur.fetchone()
    if row:
        print(f"FOUND in run_ledger: {dict(row)}")
    else:
        print("NOT FOUND in run_ledger")
        
        # Check if table exists
        cur.execute("SELECT to_regclass('public.run_ledger')")
        if cur.fetchone()['to_regclass']:
             print("Table 'run_ledger' exists.")
        else:
             print("Table 'run_ledger' DOES NOT exist.")

    print(f"\nChecking nl_collected_urls for run_id: {run_id}")
    cur.execute("SELECT COUNT(*) as count FROM nl_collected_urls WHERE run_id = %s", (run_id,))
    count = cur.fetchone()['count']
    print(f"Found {count} rows in nl_collected_urls for this run_id.")
    
    # Check if there are any rows in run_ledger
    cur.execute("SELECT COUNT(*) as count FROM run_ledger")
    total = cur.fetchone()['count']
    print(f"Total rows in run_ledger: {total}")

except Exception as e:
    print(f"Error: {e}")
finally:
    if 'conn' in locals() and conn:
        conn.close()
