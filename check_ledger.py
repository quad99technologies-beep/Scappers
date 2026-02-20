from core.db.connection import CountryDB
db = CountryDB('NorthMacedonia')
with db.cursor() as cur:
    cur.execute("SELECT run_id, mode, status, started_at FROM run_ledger WHERE scraper_name = 'NorthMacedonia' ORDER BY started_at DESC LIMIT 20")
    rows = cur.fetchall()
    print("Recent Runs in Ledger:")
    for r in rows:
        print(r)
