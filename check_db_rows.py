from core.db.connection import CountryDB
db = CountryDB('NorthMacedonia')
with db.cursor() as cur:
    cur.execute("SELECT id, run_id, url_id, detail_url FROM nm_drug_register ORDER BY id DESC LIMIT 5")
    rows = cur.fetchall()
    print(f"Total rows found: {len(rows)}")
    for r in rows:
        print(r)
    
    cur.execute("SELECT COUNT(*) FROM nm_drug_register")
    print(f"Total count IN TABLE: {cur.fetchone()[0]}")
