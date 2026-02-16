#!/usr/bin/env python3
"""
Migrate platform_config.py imports to core.config_manager

This script updates Python files to replace:
  from platform_config import PathManager, get_path_manager, ...
  
With:
  from core.config_manager import ConfigManager
  
And updates the usage patterns accordingly.
"""

import os
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent

# Mapping of old imports to new imports
IMPORT_PATTERNS = [
    # Pattern 1: from platform_config import get_path_manager
    (
        r'from platform_config import get_path_manager',
        'from core.config_manager import ConfigManager'
    ),
    # Pattern 2: from platform_config import PathManager, ConfigResolver, ...
    (
        r'from platform_config import PathManager(?:, ConfigResolver)?(?:, get_path_manager)?(?:, get_config_resolver)?',
        'from core.config_manager import ConfigManager'
    ),
    # Pattern 3: from platform_config import get_path_manager, get_config_resolver
    (
        r'from platform_config import get_path_manager, get_config_resolver',
        'from core.config_manager import ConfigManager'
    ),
]

# Usage pattern replacements
USAGE_PATTERNS = [
    # get_path_manager().get_output_dir(scraper) -> ConfigManager.get_output_dir(scraper)
    (
        r'(\w+)\s*=\s*get_path_manager\(\)',
        '# Migrated: get_path_manager() -> ConfigManager'
    ),
    # pm.get_output_dir(x) -> ConfigManager.get_output_dir(x)
    (
        r'(?<!\w)pm\.get_output_dir\(',
        'ConfigManager.get_output_dir('
    ),
    # pm.get_input_dir(x) -> ConfigManager.get_input_dir(x)
    (
        r'(?<!\w)pm\.get_input_dir\(',
        'ConfigManager.get_input_dir('
    ),
    # pm.get_exports_dir(x) -> ConfigManager.get_exports_dir(x)
    (
        r'(?<!\w)pm\.get_exports_dir\(',
        'ConfigManager.get_exports_dir('
    ),
    # pm.get_backups_dir(x) -> ConfigManager.get_backups_dir(x)
    (
        r'(?<!\w)pm\.get_backups_dir\(',
        'ConfigManager.get_backups_dir('
    ),
    # pm.get_config_dir() -> ConfigManager.get_config_dir()
    (
        r'(?<!\w)pm\.get_config_dir\(',
        'ConfigManager.get_config_dir('
    ),
    # pm.get_runs_dir() -> ConfigManager.get_runs_dir()
    (
        r'(?<!\w)pm\.get_runs_dir\(',
        'ConfigManager.get_runs_dir('
    ),
    # pm.get_platform_root() -> ConfigManager.get_app_root()
    (
        r'(?<!\w)pm\.get_platform_root\(',
        'ConfigManager.get_app_root('
    ),
]


def migrate_file(filepath: Path) -> bool:
    """Migrate a single file. Returns True if changes were made."""
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
    except Exception as e:
        print(f"[ERROR] Cannot read {filepath}: {e}")
        return False
    
    original_content = content
    
    # Check if file uses platform_config
    if 'platform_config' not in content:
        return False
    
    # Replace imports
    for old_pattern, new_pattern in IMPORT_PATTERNS:
        content = re.sub(old_pattern, new_pattern, content)
    
    # Replace usage patterns
    for old_pattern, new_pattern in USAGE_PATTERNS:
        content = re.sub(old_pattern, new_pattern, content)
    
    # Handle special case: get_path_manager() usage without pm variable
    content = re.sub(
        r'get_path_manager\(\)\.get_output_dir\(',
        'ConfigManager.get_output_dir(',
        content
    )
    content = re.sub(
        r'get_path_manager\(\)\.get_input_dir\(',
        'ConfigManager.get_input_dir(',
        content
    )
    content = re.sub(
        r'get_path_manager\(\)\.get_exports_dir\(',
        'ConfigManager.get_exports_dir(',
        content
    )
    content = re.sub(
        r'get_path_manager\(\)\.get_backups_dir\(',
        'ConfigManager.get_backups_dir(',
        content
    )
    content = re.sub(
        r'get_path_manager\(\)\.get_config_dir\(',
        'ConfigManager.get_config_dir(',
        content
    )
    content = re.sub(
        r'get_path_manager\(\)\.get_runs_dir\(',
        'ConfigManager.get_runs_dir(',
        content
    )
    
    # Remove "Try to import platform_config" comments
    content = re.sub(
        r'#\s*Try to import platform_config.*\n',
        '',
        content
    )
    
    # Remove "_PLATFORM_CONFIG_AVAILABLE" checks (keep fallback logic)
    content = re.sub(
        r'if _PLATFORM_CONFIG_AVAILABLE:\s*\n\s*pm = get_path_manager\(\)',
        'pm = None  # Migrated to ConfigManager',
        content
    )
    
    if content != original_content:
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            return True
        except Exception as e:
            print(f"[ERROR] Cannot write {filepath}: {e}")
            return False
    
    return False


def main():
    print("=" * 70)
    print("PLATFORM_CONFIG MIGRATION")
    print("=" * 70)
    
    # Files to migrate (excluding smoke_test.py which tests deprecation)
    files_to_migrate = [
        # Core files
        "shared_workflow_runner.py",
        "tools/telegram_bot.py",
        
        # Core modules
        "core/pipeline_checkpoint.py",
        "core/pipeline_start_lock.py",
        
        # Common scripts
        "services/api_server.py",
        "services/pipeline_api.py",
        
        # Scrapers - config_loader.py files
        "scripts/Argentina/config_loader.py",
        "scripts/Belarus/config_loader.py",
        "scripts/canada_ontario/config_loader.py",
        "scripts/canada_quebec/config_loader.py",
        "scripts/India/config_loader.py",
        "scripts/Malaysia/config_loader.py",
        "scripts/Netherlands/config_loader.py",
        "scripts/north_macedonia/config_loader.py",
        "scripts/Russia/config_loader.py",
        "scripts/Taiwan/config_loader.py",
        "scripts/tender_brazil/config_loader.py",
        "scripts/tender_chile/config_loader.py",
        
        # Scrapers - cleanup_lock.py files
        "scripts/Argentina/cleanup_lock.py",
        "scripts/Belarus/cleanup_lock.py",
        "scripts/canada_ontario/cleanup_lock.py",
        "scripts/canada_quebec/cleanup_lock.py",
        "scripts/India/archive/cleanup_lock.py",
        "scripts/Malaysia/cleanup_lock.py",
        "scripts/Netherlands/archive/cleanup_lock.py",
        "scripts/north_macedonia/cleanup_lock.py",
        "scripts/Russia/cleanup_lock.py",
        "scripts/Taiwan/cleanup_lock.py",
        "scripts/tender_chile/cleanup_lock.py",
        
        # Misc scraper files
        "scripts/canada_ontario/00_backup_and_clean.py",
        "scripts/India/05_qc_and_export.py",
        "scripts/Russia/run_pipeline_resume.py",
    ]
    
    migrated = []
    errors = []
    skipped = []
    
    for rel_path in files_to_migrate:
        filepath = REPO_ROOT / rel_path
        if not filepath.exists():
            skipped.append(rel_path)
            continue
        
        try:
            if migrate_file(filepath):
                migrated.append(rel_path)
                print(f"[MIGRATED] {rel_path}")
            else:
                skipped.append(rel_path)
        except Exception as e:
            errors.append((rel_path, str(e)))
            print(f"[ERROR] {rel_path}: {e}")
    
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Migrated: {len(migrated)}")
    print(f"Skipped (no changes needed): {len(skipped)}")
    print(f"Errors: {len(errors)}")
    
    if migrated:
        print("\nMigrated files:")
        for f in migrated:
            print(f"  - {f}")
    
    if errors:
        print("\nErrors:")
        for f, e in errors:
            print(f"  - {f}: {e}")
    
    print("\n" + "=" * 70)
    print("NOTE: scraper_gui.py has many complex usages - migrate manually")
    print("=" * 70)


if __name__ == "__main__":
    main()
