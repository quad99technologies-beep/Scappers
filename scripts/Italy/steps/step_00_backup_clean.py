
#!/usr/bin/env python3
import os
import sys
from pathlib import Path

# Boilerplate to ensure imports work
_repo_root = Path(__file__).resolve().parents[3]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.db.connection import CountryDB
from core.db.schema_registry import SchemaRegistry

def main():
    print("Initializing Italy Schema...")
    
    schema_path = _repo_root / "sql/schemas/postgres/italy.sql"
    if not schema_path.exists():
        print(f"Error: Schema file not found at {schema_path}")
        sys.exit(1)
        
    with CountryDB("Italy") as db:
        # SchemaRegistry requires db instance
        registry = SchemaRegistry(db)
        # apply_schema takes Path object
        registry.apply_schema(schema_path)
        print("Schema applied successfully.")
        
    print("Step 0 Complete.")

if __name__ == "__main__":
    main()
