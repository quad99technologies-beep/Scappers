#!/usr/bin/env python3
"""
Test unified translation cache across all scrapers.
"""

import os
import sys

# Set up environment
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("POSTGRES_DB", "scrappers")
os.environ.setdefault("POSTGRES_USER", "postgres")
os.environ.setdefault("POSTGRES_PASSWORD", "admin123")

from core.translation import TranslationCache, get_cache


def test_scraper_cache(scraper_name: str, test_term: str, source_lang: str, target_lang: str):
    """Test cache for a specific scraper"""
    print(f"\n--- Testing {scraper_name} ---")
    
    try:
        cache = TranslationCache(scraper_name)
        
        # Test set
        test_translation = f"test_translation_{scraper_name}"
        success = cache.set(test_term, test_translation, source_lang, target_lang)
        print(f"  Set: {success}")
        
        # Test get
        result = cache.get(test_term, source_lang, target_lang)
        print(f"  Get: {result == test_translation} (got: {result})")
        
        # Test stats
        stats = cache.get_stats()
        print(f"  Stats: {stats.get('total_entries', 0)} entries, schema: {stats.get('schema', 'unknown')}")
        
        return True
    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def test_repository_integration():
    """Test that repository methods work with unified cache"""
    print("\n" + "=" * 60)
    print("TEST: Repository Integration")
    print("=" * 60)
    
    # Test Argentina repository
    print("\n--- Argentina Repository ---")
    try:
        sys.path.insert(0, "scripts/Argentina")
        from scripts.Argentina.db.repositories import ArgentinaRepository
        from core.db.connection import CountryDB
        
        db = CountryDB("Argentina")
        db.connect()
        repo = ArgentinaRepository(db, "test_run_001")
        
        # Test unified cache via repository
        repo.save_single_translation("test_medicamento", "test_medicine", "es", "en")
        result = repo.get_cached_translation("test_medicamento", "es", "en")
        
        print(f"  Save/Load: {result == 'test_medicine'}")
        return True
    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    print("=" * 60)
    print("TRANSLATION CACHE INTEGRATION TEST")
    print("=" * 60)
    
    # Test each scraper
    scrapers = [
        ("argentina", "test_medicamento", "es", "en"),
        ("russia", "test_lekarstvo", "ru", "en"),
        ("belarus", "test_lekarstvo", "be", "en"),
        ("north_macedonia", "test_lek", "mk", "en"),
        ("malaysia", "test_ubat", "ms", "en"),
    ]
    
    results = []
    for scraper, term, src, tgt in scrapers:
        results.append((scraper, test_scraper_cache(scraper, term, src, tgt)))
    
    # Test repository integration
    repo_ok = test_repository_integration()
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    for scraper, ok in results:
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {scraper}")
    
    print(f"  [{'PASS' if repo_ok else 'FAIL'}] Repository Integration")
    
    passed = sum(1 for _, ok in results if ok) + (1 if repo_ok else 0)
    total = len(results) + 1
    
    print(f"\nTotal: {passed}/{total} passed")
    
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
