import sqlite3
from pathlib import Path

db_path = Path('ar_scraper.db')
print(f'DB path: {db_path.absolute()}')
print(f'Exists: {db_path.exists()}')

if db_path.exists():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print(f'Tables: {[t[0] for t in tables]}')
    
    conn.close()
