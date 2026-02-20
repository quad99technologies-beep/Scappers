from core.db.connection import CountryDB
import pandas as pd

db = CountryDB("Malaysia")
with db.cursor() as cur:
    print("Checking my_product_details...")
    cur.execute("SELECT holder, COUNT(*) FROM my_product_details GROUP BY holder HAVING holder = '' OR holder ILIKE 'N/a' OR holder IS NULL")
    rows = cur.fetchall()
    for row in rows:
        print(f"Holder: '{row[0]}', Count: {row[1]}")

    print("\nChecking my_consolidated_products...")
    cur.execute("SELECT holder, COUNT(*) FROM my_consolidated_products GROUP BY holder HAVING holder = '' OR holder ILIKE 'N/a' OR holder IS NULL")
    rows = cur.fetchall()
    for row in rows:
        print(f"Holder: '{row[0]}', Count: {row[1]}")

    print("\nChecking my_pcid_mappings...")
    cur.execute("SELECT company, COUNT(*) FROM my_pcid_mappings GROUP BY company HAVING company = '' OR company ILIKE 'N/a' OR company IS NULL")
    rows = cur.fetchall()
    for row in rows:
        print(f"Company: '{row[0]}', Count: {row[1]}")
