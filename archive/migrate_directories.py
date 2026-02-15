#!/usr/bin/env python3
"""
Directory Migration Script
Migrates scraper directories from old naming (spaces, hyphens) to standardized naming.
"""

import os
import sys
import shutil
from pathlib import Path

# Mapping: old_name -> new_name
RENAME_MAP = {
    "scripts/Canada Ontario": "scripts/canada_ontario",
    "scripts/CanadaQuebec": "scripts/canada_quebec",
    "scripts/North Macedonia": "scripts/north_macedonia",
    "scripts/Tender- Chile": "scripts/tender_chile",
    "scripts/Tender - Brazil": "scripts/tender_brazil",
}

def migrate_directories():
    """Perform the migration"""
    repo_root = Path(__file__).resolve().parent
    
    print("=" * 60)
    print("DIRECTORY MIGRATION")
    print("=" * 60)
    
    migrated = []
    errors = []
    
    for old_rel, new_rel in RENAME_MAP.items():
        old_path = repo_root / old_rel
        new_path = repo_root / new_rel
        
        if not old_path.exists():
            print(f"[WARN] Source not found: {old_rel}")
            continue
            
        if new_path.exists():
            print(f"[WARN] Target already exists: {new_rel}")
            errors.append((old_rel, "Target exists"))
            continue
        
        try:
            shutil.move(str(old_path), str(new_path))
            print(f"[OK] {old_rel} -> {new_rel}")
            migrated.append((old_rel, new_rel))
        except Exception as e:
            print(f"[ERR] Error moving {old_rel}: {e}")
            errors.append((old_rel, str(e)))
    
    print("\n" + "=" * 60)
    print(f"MIGRATED: {len(migrated)}")
    print(f"ERRORS: {len(errors)}")
    print("=" * 60)
    
    return migrated, errors

def update_references():
    """Update references in config files, imports, etc."""
    print("\n" + "=" * 60)
    print("UPDATING REFERENCES")
    print("=" * 60)
    
    # These need to be updated in various files
    reference_updates = {
        # Config files
        "Canada Ontario": "canada_ontario",
        "CanadaQuebec": "canada_quebec", 
        "North Macedonia": "north_macedonia",
        "Tender- Chile": "tender_chile",
        "Tender - Brazil": "tender_brazil",
        # Also check for variations
        "Tender_Chile": "tender_chile",
        "Tender_Brazil": "tender_brazil",
    }
    
    # Files to check for references
    files_to_check = [
        "platform_config.py",
        "scraper_gui.py",
        "telegram_bot.py",
        "shared_workflow_runner.py",
        "core/config_manager.py",
    ]
    
    # Add config files
    config_files = list(Path("config").glob("*.json")) + list(Path("config").glob("*.env*"))
    files_to_check.extend([str(f) for f in config_files])
    
    updated_files = []
    
    for file_path in files_to_check:
        full_path = Path(file_path)
        if not full_path.exists():
            continue
            
        try:
            with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            original_content = content
            
            for old, new in reference_updates.items():
                content = content.replace(old, new)
            
            if content != original_content:
                with open(full_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                print(f"[OK] Updated: {file_path}")
                updated_files.append(file_path)
        except Exception as e:
            print(f"[ERR] Error updating {file_path}: {e}")
    
    print(f"\nUpdated {len(updated_files)} files")
    return updated_files

def main():
    print("Starting directory migration...")
    print("This will:")
    print("1. Rename directories to use snake_case")
    print("2. Update references in config files")
    print()
    
    # Step 1: Migrate directories
    migrated, errors = migrate_directories()
    
    if errors:
        print("\n[WARN] Some errors occurred. Fix them before continuing.")
        return 1
    
    # Step 2: Update references
    updated = update_references()
    
    print("\n" + "=" * 60)
    print("MIGRATION COMPLETE")
    print("=" * 60)
    print("\nNext steps:")
    print("1. Run smoke tests to verify everything works")
    print("2. Update any hardcoded paths in scraper scripts")
    print("3. Test a few scrapers to ensure they still run")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
