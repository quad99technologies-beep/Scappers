
import sys
import os
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Load environment variables
load_dotenv()

def deploy_schema():
    """Deploy the Canada Ontario database schema."""
    schema_file = _repo_root / "sql" / "schemas" / "postgres" / "canada_ontario.sql"
    
    if not schema_file.exists():
        print(f"[ERROR] Schema file not found: {schema_file}")
        sys.exit(1)
    
    # Get database connection parameters
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    database = os.getenv("POSTGRES_DB", "scrappers")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")
    
    print(f"Connecting to PostgreSQL: {user}@{host}:{port}/{database}")
    
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        cur = conn.cursor()
        
        # Read and execute schema
        print(f"Reading schema file: {schema_file}")
        schema_sql = schema_file.read_text(encoding="utf-8")
        
        print("Executing schema...")
        cur.execute(schema_sql)
        conn.commit()
        
        print("[OK] Schema deployment successful!")
        
        cur.close()
        conn.close()
        return True
        
    except psycopg2.Error as e:
        print(f"[ERROR] Database error: {e}")
        return False
    except Exception as e:
        print(f"[ERROR] Error: {e}")
        return False

if __name__ == "__main__":
    success = deploy_schema()
    sys.exit(0 if success else 1)
