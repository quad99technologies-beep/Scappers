from core.db.connection import CountryDB
db = CountryDB('NorthMacedonia')
with db.cursor() as cur:
    cur.execute("SELECT run_id, scraper_name, started_at FROM run_ledger WHERE run_id LIKE '20260219%'")
    rows = cur.fetchall()
    for r in rows:
        print(r)
