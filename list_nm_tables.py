from core.db.connection import CountryDB
db = CountryDB('NorthMacedonia')
with db.cursor() as cur:
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'nm_%'")
    tables = cur.fetchall()
    print("NM Tables:")
    for t in tables:
        print(f"  {t[0]}")
        cur.execute(f"SELECT COUNT(*) FROM {t[0]}")
        print(f"    Rows: {cur.fetchone()[0]}")
