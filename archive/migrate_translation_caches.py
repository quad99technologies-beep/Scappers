#!/usr/bin/env python3
"""
Migrate existing translation caches to unified DB format.

This script:
1. Migrates Argentina JSON cache -> DB
2. Migrates Russia JSON cache -> DB  
3. Migrates Belarus JSON cache -> DB
4. Sets up North Macedonia persistent cache (was in-memory only)
5. Verifies Malaysia is already using DB
"""

import os
import sys
from pathlib import Path

# Add repo root to path
repo_root = Path(__file__).resolve().parent
sys.path.insert(0, str(repo_root))

from core.translation import TranslationCache


def migrate_argentina():
    """Migrate Argentina JSON cache"""
    print("\n" + "=" * 60)
    print("MIGRATING: Argentina")
    print("=" * 60)
    
    cache = TranslationCache("argentina")
    json_path = repo_root / "cache" / "argentina_translation_cache.json"
    
    if not json_path.exists():
        print(f"[WARN] JSON cache not found: {json_path}")
        # Create table anyway for future use
        cache._ensure_table()
        print("[OK] Created empty cache table")
        return 0
    
    count = cache.migrate_from_json(str(json_path), "es", "en")
    print(f"[OK] Migrated {count} entries from Argentina cache")
    
    # Show stats
    stats = cache.get_stats()
    print(f"[INFO] Total entries in DB: {stats.get('total_entries', 0)}")
    
    return count


def migrate_russia():
    """Migrate Russia JSON cache"""
    print("\n" + "=" * 60)
    print("MIGRATING: Russia")
    print("=" * 60)
    
    cache = TranslationCache("russia")
    json_path = repo_root / "cache" / "russia_translation_cache.json"
    
    if not json_path.exists():
        print(f"[WARN] JSON cache not found: {json_path}")
        cache._ensure_table()
        print("[OK] Created empty cache table")
        return 0
    
    count = cache.migrate_from_json(str(json_path), "ru", "en")
    print(f"[OK] Migrated {count} entries from Russia cache")
    
    stats = cache.get_stats()
    print(f"[INFO] Total entries in DB: {stats.get('total_entries', 0)}")
    
    return count


def migrate_belarus():
    """Migrate Belarus JSON cache"""
    print("\n" + "=" * 60)
    print("MIGRATING: Belarus")
    print("=" * 60)
    
    cache = TranslationCache("belarus")
    json_path = repo_root / "cache" / "belarus_translation_cache.json"
    
    if not json_path.exists():
        print(f"[WARN] JSON cache not found: {json_path}")
        cache._ensure_table()
        print("[OK] Created empty cache table")
        return 0
    
    count = cache.migrate_from_json(str(json_path), "be", "en")
    print(f"[OK] Migrated {count} entries from Belarus cache")
    
    stats = cache.get_stats()
    print(f"[INFO] Total entries in DB: {stats.get('total_entries', 0)}")
    
    return count


def setup_north_macedonia():
    """Setup North Macedonia persistent cache (was in-memory only)"""
    print("\n" + "=" * 60)
    print("SETTING UP: North Macedonia")
    print("=" * 60)
    
    cache = TranslationCache("north_macedonia")
    # Access db property to trigger connection and table creation
    _ = cache.db
    
    stats = cache.get_stats()
    print(f"[OK] Created cache table for North Macedonia")
    print(f"[INFO] Total entries: {stats.get('total_entries', 0)}")
    
    return 0


def verify_malaysia():
    """Verify Malaysia is using DB (no migration needed)"""
    print("\n" + "=" * 60)
    print("VERIFYING: Malaysia")
    print("=" * 60)
    
    cache = TranslationCache("malaysia")
    # Access db property to trigger connection and table creation
    _ = cache.db
    
    stats = cache.get_stats()
    print(f"[OK] Malaysia cache table ready")
    print(f"[INFO] Malaysia uses dictionary table, translation cache is ready for future use")
    
    return 0


def main():
    print("=" * 60)
    print("TRANSLATION CACHE MIGRATION")
    print("=" * 60)
    print("\nThis will migrate all translation caches to unified DB format.")
    print("Existing JSON files will NOT be deleted (kept as backup).")
    print()
    
    results = {
        "argentina": migrate_argentina(),
        "russia": migrate_russia(),
        "belarus": migrate_belarus(),
        "north_macedonia": setup_north_macedonia(),
        "malaysia": verify_malaysia(),
    }
    
    print("\n" + "=" * 60)
    print("MIGRATION SUMMARY")
    print("=" * 60)
    
    total_migrated = 0
    for name, count in results.items():
        if count > 0:
            print(f"[OK] {name}: {count} entries migrated")
            total_migrated += count
        else:
            print(f"[OK] {name}: ready (no migration needed)")
    
    print(f"\nTotal entries migrated: {total_migrated}")
    print("\nNext steps:")
    print("1. Update scraper scripts to use core.translation.TranslationCache")
    print("2. Test translations work correctly")
    print("3. After verification, JSON files can be moved to archive/")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
