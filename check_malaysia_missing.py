from core.db.connection import CountryDB

db = CountryDB("Malaysia")
with db.cursor() as cur:
    cur.execute("""
        SELECT p.registration_no, p.product_name
        FROM my_products p
        LEFT JOIN my_product_details pd ON p.registration_no = pd.registration_no
        WHERE pd.registration_no IS NULL
    """)
    rows = cur.fetchall()
    print(f"Products with NO details in my_product_details: {len(rows)}")
    for row in rows[:5]:
        print(f"Reg No: {row[0]}, Name: {row[1]}")

    cur.execute("""
        SELECT pd.registration_no, pd.product_name, pd.holder
        FROM my_product_details pd
        WHERE pd.holder = ''
    """)
    rows = cur.fetchall()
    print(f"\nDetails with BLANK holder: {len(rows)}")
    for row in rows[:5]:
        print(f"Reg No: {row[0]}, Name: {row[1]}, Holder: '{row[2]}'")
