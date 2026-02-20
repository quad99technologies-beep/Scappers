from core.db.connection import CountryDB
db = CountryDB('NorthMacedonia')
with db.cursor() as cur:
    cur.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'chrome_instances'
    """)
    cols = cur.fetchall()
    for c in cols:
        print(c)
