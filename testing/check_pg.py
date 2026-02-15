import sys
sys.path.insert(0, 'd:/quad99/Scrappers')
sys.path.insert(0, 'd:/quad99/Scrappers/scripts/Argentina')

from core.db.connection import CountryDB

db = CountryDB('Argentina')
print('Connected to Argentina DB')

with db.cursor() as cur:
    # List all tables
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name LIKE 'ar_%'
    """)
    tables = cur.fetchall()
    print(f'Tables: {[t[0] for t in tables]}')
    
    # Check product_index stats
    cur.execute('SELECT COUNT(*) FROM ar_product_index WHERE total_records > 0')
    scraped = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM ar_product_index WHERE total_records = 0')
    not_scraped = cur.fetchone()[0]
    print(f'\nProducts with records: {scraped}')
    print(f'Products without records: {not_scraped}')
    
    # Check loop_count distribution
    cur.execute('SELECT loop_count, COUNT(*) FROM ar_product_index GROUP BY loop_count ORDER BY loop_count')
    print(f'\nLoop count distribution:')
    for row in cur.fetchall():
        print(f'  loop_count={row[0]}: {row[1]} products')
