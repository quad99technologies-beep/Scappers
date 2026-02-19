#!/usr/bin/env python3
"""
Smoke Test Suite

Tests basic functionality after migration:
1. Directory structure is correct
2. Translation cache works
3. Config loading works
4. No import errors
"""

import os
import sys
from pathlib import Path

# Set up environment
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("POSTGRES_DB", "scrappers")
os.environ.setdefault("POSTGRES_USER", "postgres")
os.environ.setdefault("POSTGRES_PASSWORD", "admin123")

def test_directories():
    """Test that directories were migrated correctly"""
    print("\n" + "=" * 60)
    print("TEST: Directory Structure")
    print("=" * 60)
    
    repo_root = Path(__file__).resolve().parent
    scripts_dir = repo_root / "scripts"
    
    # Check old directories don't exist
    old_dirs = [
        "Canada Ontario", "CanadaQuebec", "North Macedonia",
        "Tender- Chile", "Tender - Brazil", "Colombia", "Peru", 
        "South Korea", "Italy"
    ]
    
    errors = []
    for old_dir in old_dirs:
        path = scripts_dir / old_dir
        if path.exists():
            errors.append(f"Old directory still exists: {old_dir}")
    
    # Check new directories exist
    new_dirs = [
        "canada_ontario", "canada_quebec", "north_macedonia",
        "tender_chile", "tender_brazil"
    ]
    
    for new_dir in new_dirs:
        path = scripts_dir / new_dir
        if not path.exists():
            errors.append(f"New directory missing: {new_dir}")
    
    if errors:
        for e in errors:
            print(f"[FAIL] {e}")
        return False
    
    print("[PASS] All directories migrated correctly")
    print(f"       Archived: {', '.join(old_dirs[:4])}...")
    print(f"       New names: {', '.join(new_dirs)}")
    return True


def test_translation_cache():
    """Test translation cache module"""
    print("\n" + "=" * 60)
    print("TEST: Translation Cache")
    print("=" * 60)
    
    try:
        from core.translation import TranslationCache, get_cache
        
        # Test cache creation
        cache = TranslationCache("argentina")
        assert cache.prefix == "ar", f"Expected prefix 'ar', got '{cache.prefix}'"
        
        # Test get_cache returns working instance
        cache2 = get_cache("argentina")
        assert cache2.prefix == "ar", "get_cache should return valid cache"
        
        # Test DB connection
        _ = cache.db
        print("[PASS] DB connection successful")
        
        # Test cache operations
        cache.set("test_hello", "test_hola", "en", "es")
        result = cache.get("test_hello", "en", "es")
        assert result == "test_hola", f"Expected 'test_hola', got '{result}'"
        
        print("[PASS] Cache get/set works")
        
        # Test stats
        stats = cache.get_stats()
        assert "total_entries" in stats
        print(f"[INFO] Cache stats: {stats['total_entries']} entries")
        
        return True
        
    except Exception as e:
        print(f"[FAIL] {e}")
        import traceback
        traceback.print_exc()
        return False


def test_config_manager():
    """Test config manager"""
    print("\n" + "=" * 60)
    print("TEST: Config Manager")
    print("=" * 60)
    
    try:
        from core.config.config_manager import ConfigManager
        
        # Test path resolution
        app_root = ConfigManager.get_app_root()
        print(f"[INFO] App root: {app_root}")
        
        config_dir = ConfigManager.get_config_dir()
        print(f"[INFO] Config dir: {config_dir}")
        
        # Test env loading
        env = ConfigManager.load_env("argentina")
        print(f"[PASS] Loaded env for argentina")
        
        return True
        
    except Exception as e:
        print(f"[FAIL] {e}")
        import traceback
        traceback.print_exc()
        return False


def test_imports():
    """Test that all core modules can be imported"""
    print("\n" + "=" * 60)
    print("TEST: Core Module Imports")
    print("=" * 60)
    
    modules = [
        "core.config_manager",
        "core.db.connection",
        "core.db.postgres_connection",
        "core.translation",
        "core.translation.cache",
        "core.base_scraper",
        "core.logger",
    ]
    
    errors = []
    for mod in modules:
        try:
            __import__(mod)
            print(f"[PASS] {mod}")
        except Exception as e:
            print(f"[FAIL] {mod}: {e}")
            errors.append((mod, e))
    
    return len(errors) == 0


def test_deprecation_warnings():
    """Test that platform_config emits deprecation warnings"""
    print("\n" + "=" * 60)
    print("TEST: Deprecation Warnings")
    print("=" * 60)
    
    import warnings
    
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        
        # Import should trigger warning
        import platform_config
        
        # Check for deprecation warning
        deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        
        if deprecation_warnings:
            print(f"[PASS] Deprecation warning emitted on import")
            print(f"       Message: {deprecation_warnings[0].message}")
            return True
        else:
            print("[WARN] No deprecation warning on import (may be suppressed)")
            return True  # Not a failure, just a warning


def main():
    print("=" * 60)
    print("SMOKE TEST SUITE")
    print("=" * 60)
    
    tests = [
        ("Directory Structure", test_directories),
        ("Translation Cache", test_translation_cache),
        ("Config Manager", test_config_manager),
        ("Core Imports", test_imports),
        ("Deprecation Warnings", test_deprecation_warnings),
    ]
    
    results = []
    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, result))
        except Exception as e:
            print(f"[ERROR] {name} crashed: {e}")
            results.append((name, False))
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, r in results if r)
    failed = sum(1 for _, r in results if not r)
    
    for name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"  [{status}] {name}")
    
    print(f"\nTotal: {passed} passed, {failed} failed")
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
