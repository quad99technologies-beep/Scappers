
import os
import sys
from pathlib import Path

# Add repo root and script dir to path
sys.path.append(os.path.abspath("."))
sys.path.append(os.path.abspath("scripts/canada_ontario"))

from core.db.postgres_connection import PostgresDB
from config_loader import get_run_id

def verify_columns():
    run_id = get_run_id()
    print(f"Checking columns for run_id: {run_id}")

    db = PostgresDB("CanadaOntario")
    db.connect()
    try:
        with db.cursor() as cur:
            # Check for non-null values in the new columns
            sql = """
                SELECT count(*) 
                FROM co_products 
                WHERE run_id = %s 
                AND (strength IS NOT NULL OR dosage_form IS NOT NULL OR pack_size IS NOT NULL)
            """
            cur.execute(sql, (run_id,))
            count = cur.fetchone()[0]
            
            print(f"Rows with populated strength/dosage/pack_size: {count}")
            
            if count > 0:
                print("SUCCESS: Data is being populated in the new columns.")
                # Show sample
                cur.execute("""
                    SELECT product_name, strength, dosage_form, pack_size 
                    FROM co_products 
                    WHERE run_id = %s 
                    AND (strength IS NOT NULL OR dosage_form IS NOT NULL) 
                    LIMIT 5
                """, (run_id,))
                for row in cur.fetchall():
                    print(f"  {row}")
            else:
                print("FAILURE: No data found in the new columns yet (or run hasn't inserted anything).")

    finally:
        db.close()

if __name__ == "__main__":
    verify_columns()
