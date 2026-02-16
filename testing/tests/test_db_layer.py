#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test script to verify North Macedonia database layer is working correctly.
"""

import sys
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add script directory to path
_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

def test_imports():
    """Test that all database modules can be imported."""
    print("Testing imports...")
    
    try:
        from core.db import get_db
        print("[OK] core.db.get_db imported successfully")
    except ImportError as e:
        print(f"[FAIL] Failed to import core.db.get_db: {e}")
        return False
    
    try:
        from db import NorthMacedoniaRepository, apply_schema, apply_north_macedonia_schema
        print("[OK] db.NorthMacedoniaRepository imported successfully")
        print("[OK] db.apply_schema imported successfully")
        print("[OK] db.apply_north_macedonia_schema imported successfully")
    except ImportError as e:
        print(f"[FAIL] Failed to import from db: {e}")
        return False
    
    try:
        from db import DataValidator, StatisticsCollector
        print("[OK] db.DataValidator imported successfully")
        print("[OK] db.StatisticsCollector imported successfully")
    except ImportError as e:
        print(f"[FAIL] Failed to import validator/statistics: {e}")
        return False
    
    return True

def test_schema():
    """Test that schema can be applied."""
    print("\nTesting schema application...")
    
    try:
        from core.db import get_db
        from db import apply_schema
        
        db = get_db("NorthMacedonia")
        print("[OK] Database connection established")
        
        apply_schema(db)
        print("[OK] Schema applied successfully")
        
        return True
    except Exception as e:
        print(f"[FAIL] Failed to apply schema: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_repository():
    """Test that repository can be instantiated."""
    print("\nTesting repository...")
    
    try:
        from core.db import get_db
        from db import NorthMacedoniaRepository
        from datetime import datetime
        
        db = get_db("NorthMacedonia")
        run_id = f"test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        repo = NorthMacedoniaRepository(db, run_id)
        print(f"[OK] Repository created with run_id: {run_id}")
        
        # Test a simple query
        stats = repo.get_run_stats()
        print(f"[OK] Repository query successful: {stats}")
        
        return True
    except Exception as e:
        print(f"[FAIL] Failed to test repository: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all tests."""
    print("="*60)
    print("NORTH MACEDONIA DATABASE LAYER TEST")
    print("="*60)
    print()
    
    all_passed = True
    
    # Test 1: Imports
    if not test_imports():
        all_passed = False
        print("\n[WARNING] Import test failed. Fix imports before proceeding.")
        return 1
    
    # Test 2: Schema
    if not test_schema():
        all_passed = False
        print("\n[WARNING] Schema test failed. Check database connection.")
        return 1
    
    # Test 3: Repository
    if not test_repository():
        all_passed = False
        print("\n[WARNING] Repository test failed.")
        return 1
    
    print()
    print("="*60)
    if all_passed:
        print("[SUCCESS] ALL TESTS PASSED")
        print("="*60)
        print("\nThe database layer is working correctly!")
        print("You can now run the scraper pipeline.")
        return 0
    else:
        print("[ERROR] SOME TESTS FAILED")
        print("="*60)
        return 1

if __name__ == "__main__":
    sys.exit(main())

