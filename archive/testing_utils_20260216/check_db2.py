import sqlite3
from pathlib import Path

# Check all possible db locations
for db_path in [Path('ar_scraper.db'), Path('output/Argentina/ar_scraper.db')]:
    print(f'\n=== Checking: {db_path.absolute()} ===')
    if db_path.exists():
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        print(f'Tables: {[t[0] for t in tables]}')
        
        for table in [t[0] for t in tables]:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f'  {table}: {count} rows')
        
        conn.close()
    else:
        print('  Database does not exist')
