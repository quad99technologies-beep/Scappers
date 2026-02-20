from core.db.connection import CountryDB
db = CountryDB('NorthMacedonia')
with db.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'chrome_instances'")
    exists = cur.fetchone()[0] > 0
    if exists:
        cur.execute("SELECT * FROM chrome_instances ORDER BY started_at DESC LIMIT 5")
        rows = cur.fetchall()
        print(f'chrome_instances recent records: {len(rows)}')
        for r in rows:
            print(r)
    else:
        print('chrome_instances table does NOT exist')
