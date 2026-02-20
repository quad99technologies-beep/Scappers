from core.db.connection import CountryDB
db = CountryDB('NorthMacedonia')
with db.cursor() as cur:
    cur.execute("SELECT run_id, scraper_name, status FROM run_ledger WHERE run_id = '20260219_023232_b2b094ef'")
    row = cur.fetchone()
    print(f"Run info for 20260219_023232_b2b094ef: {row}")
    
    cur.execute("SELECT COUNT(*) FROM nm_urls WHERE run_id = '20260219_023232_b2b094ef'")
    print(f"nm_urls count: {cur.fetchone()[0]}")
    
    cur.execute("SELECT COUNT(*) FROM nm_drug_register WHERE run_id = '20260219_023232_b2b094ef'")
    print(f"nm_drug_register count: {cur.fetchone()[0]}")
