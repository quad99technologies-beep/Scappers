from database import Database

def run_migration():
    db = Database()
    try:
        with db.get_cursor() as cur:
            print("Adding urls_inserted column to nl_search_combinations...")
            cur.execute("""
                ALTER TABLE nl_search_combinations 
                ADD COLUMN IF NOT EXISTS urls_inserted INTEGER DEFAULT 0;
            """)
        db.commit()
        print("Migration successful.")
    except Exception as e:
        print(f"Migration failed: {e}")
        db.rollback()

if __name__ == "__main__":
    run_migration()
