
import sys
import os
sys.path.append(r'D:\quad99\Scrappers')

from core.db.connection import CountryDB

def main():
    try:
        db = CountryDB("Russia")
        db.connect()
        
        print("Dropping old constraint...")
        try:
            db.execute("ALTER TABLE ru_failed_pages DROP CONSTRAINT ru_failed_pages_status_check")
        except Exception as e:
            print(f"Warning dropping constraint (might not exist): {e}")

        print("Adding new constraint...")
        db.execute("ALTER TABLE ru_failed_pages ADD CONSTRAINT ru_failed_pages_status_check CHECK (status IN ('pending', 'retrying', 'failed_permanently', 'resolved'))")
        
        db.commit()
        db.close()
        print("Constraint successfully updated to include 'resolved' status.")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
