from core.db.connection import CountryDB
db = CountryDB('NorthMacedonia')
with db.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM nm_drug_register")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM nm_drug_register WHERE url_id IS NOT NULL")
    with_id = cur.fetchone()[0]
    print(f'Total records in nm_drug_register: {total}')
    print(f'Records with url_id: {with_id}')
    
    if total > 0:
        cur.execute("SELECT id, run_id, url_id, detail_url FROM nm_drug_register LIMIT 5")
        rows = cur.fetchall()
        for r in rows:
            print(r)
