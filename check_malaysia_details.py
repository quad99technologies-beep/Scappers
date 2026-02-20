from core.db.connection import CountryDB
import pandas as pd

db = CountryDB("Malaysia")
with db.cursor() as cur:
    print("Checking for TIMEOUT ERROR in my_product_details...")
    cur.execute("SELECT registration_no, product_name, holder FROM my_product_details WHERE product_name = '[TIMEOUT ERROR]'")
    rows = cur.fetchall()
    for row in rows:
        print(f"Reg No: {row[0]}, Name: {row[1]}, Holder: '{row[2]}'")

    print("\nChecking for N/A holder details...")
    cur.execute("SELECT registration_no, product_name, holder FROM my_product_details WHERE holder = 'N/A' LIMIT 5")
    rows = cur.fetchall()
    for row in rows:
        print(f"Reg No: {row[0]}, Name: {row[1]}, Holder: '{row[2]}'")
