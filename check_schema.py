
from core.db.connection import CountryDB

def check_schema():
    with CountryDB("NorthMacedonia") as db:
        with db.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'pcid_mapping';
            """)
            columns = cur.fetchall()
            print("Columns in pcid_mapping:")
            for col in columns:
                print(f"  {col[0]} ({col[1]})")

if __name__ == "__main__":
    check_schema()
