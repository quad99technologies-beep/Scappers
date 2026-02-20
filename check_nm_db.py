from core.db.connection import CountryDB
from pathlib import Path

db = CountryDB('NorthMacedonia')
with db.cursor() as cur:
    # Check all runs with URLs
    cur.execute("""
        SELECT r.run_id, r.mode, r.started_at, r.status, r.step_count,
               (SELECT COUNT(*) FROM nm_urls WHERE run_id = r.run_id) as url_count,
               (SELECT COUNT(*) FROM nm_drug_register WHERE run_id = r.run_id) as dr_count
        FROM run_ledger r
        ORDER BY r.started_at DESC
        LIMIT 10
    """)
    rows = cur.fetchall()
    print("All recent runs:")
    print(f"{'run_id':<35} {'mode':<8} {'status':<10} {'steps':<6} {'urls':<6} {'drug_reg'}")
    print("-"*80)
    for r in rows:
        print(f"{r[0]:<35} {r[1]:<8} {r[3]:<10} {r[4]:<6} {r[5]:<6} {r[6]}")

# Show current_run_id file
rid_file = Path(r'D:\quad99\Scrappers\output\NorthMacedonia\.current_run_id')
if rid_file.exists():
    print(f"\n.current_run_id: {rid_file.read_text().strip()}")
