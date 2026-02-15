#!/usr/bin/env python3
"""
Test database connections for Malaysia, Argentina, and Netherlands
"""

import sys
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.db.postgres_connection import PostgresDB

def test_connections():
    """Test database connections for all three countries."""
    print("=" * 60)
    print("Database Connection Test")
    print("=" * 60)
    print()
    
    countries = ['Malaysia', 'Argentina', 'Netherlands']
    results = {}
    
    for country in countries:
        try:
            db = PostgresDB(country)
            db.connect()
            print(f"[OK] {country}: Database connection successful")
            db.close()
            results[country] = True
        except Exception as e:
            print(f"[ERROR] {country}: {e}")
            results[country] = False
    
    print()
    print("=" * 60)
    if all(results.values()):
        print("[OK] All database connections successful!")
    else:
        print("[WARN] Some connections failed")
    print("=" * 60)
    
    return all(results.values())

if __name__ == "__main__":
    success = test_connections()
    sys.exit(0 if success else 1)
