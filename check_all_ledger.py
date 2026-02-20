from core.db.connection import CountryDB
db = CountryDB('NorthMacedonia')
with db.cursor() as cur:
    cur.execute("SELECT DISTINCT scraper_name FROM run_ledger")
    scrapers = cur.fetchall()
    print("Scrapers in ledger:")
    for s in scrapers:
        print(f"  {s[0]}")
    
    cur.execute("SELECT run_id, scraper_name, mode, status, started_at FROM run_ledger ORDER BY started_at DESC LIMIT 10")
    rows = cur.fetchall()
    print("\nTop 10 runs in ANY scraper:")
    for r in rows:
        print(r)
