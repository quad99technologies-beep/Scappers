import sys, os
sys.path.insert(0, '.')
from scripts.Argentina.reset_try_counter import get_db_connection
conn = get_db_connection()
cursor = conn.cursor()
cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'ar_product_index'")
print("Columns in ar_product_index:")
for row in cursor.fetchall():
    print(f"  - {row[0]}")
