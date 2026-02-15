#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
First-Time Configuration Setup Script

This script:
1. Creates the Documents/ScraperPlatform/config/ directory structure
2. Copies platform.env.example to the correct location
3. Helps migrate existing .env files from repo (if found)
4. Validates the setup

Run this ONCE when first setting up the Scraper Platform.
"""

import sys
import shutil
from pathlib import Path

# Add repo root to path
repo_root = Path(__file__).resolve().parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

try:
    from core.config_manager import ConfigManager
except ImportError as e:
    print("=" * 70)
    print("ERROR: Failed to import ConfigManager")
    print(f"  {e}")
    print("=" * 70)
    sys.exit(1)


def main():
    print("=" * 70)
    print("Scraper Platform - First-Time Configuration Setup")
    print("=" * 70)
    print()

    # Step 1: Initialize directories
    print("[1/5] Creating directory structure...")
    try:
        ConfigManager.ensure_dirs()
        app_root = ConfigManager.get_app_root()
        config_dir = ConfigManager.get_config_dir()
        print(f"  ✓ App root created: {app_root}")
        print(f"  ✓ Config directory: {config_dir}")
    except Exception as e:
        print(f"  ✗ ERROR: {e}")
        sys.exit(1)
    print()

    # Step 2: Check if platform.env already exists
    platform_env = config_dir / "platform.env"
    if platform_env.exists():
        print("[2/5] Configuration already exists")
        print(f"  Found existing: {platform_env}")
        response = input("  Overwrite? (yes/no) [no]: ").strip().lower()
        if response != "yes":
            print("  Skipping template copy (keeping existing config)")
            skip_template = True
        else:
            skip_template = False
    else:
        skip_template = False

    # Step 3: Copy template if needed
    if not skip_template:
        print("[2/5] Copying platform.env template...")
        template_file = repo_root / "config" / "platform.env.example"

        if not template_file.exists():
            print(f"  ✗ ERROR: Template not found: {template_file}")
            sys.exit(1)

        try:
            shutil.copy2(template_file, platform_env)
            print(f"  ✓ Copied template to: {platform_env}")
        except Exception as e:
            print(f"  ✗ ERROR: Failed to copy template: {e}")
            sys.exit(1)
    print()

    # Step 4: Check for old .env files in repo and offer migration
    print("[3/5] Checking for legacy .env files in repo...")
    legacy_env_files = []

    # Check repo root
    repo_env = repo_root / ".env"
    if repo_env.exists():
        legacy_env_files.append(("Repo root", repo_env))

    # Check scraper directories (new structure: scripts/{ScraperName})
    for scraper_dir_name in ["scripts/CanadaQuebec", "scripts/Malaysia", "scripts/Argentina"]:
        scraper_env = repo_root / scraper_dir_name / ".env"
        if scraper_env.exists():
            legacy_env_files.append((scraper_dir_name, scraper_env))

    if legacy_env_files:
        print(f"  Found {len(legacy_env_files)} legacy .env file(s) in repo:")
        for location, path in legacy_env_files:
            print(f"    - {location}: {path}")
        print()
        print("  IMPORTANT: These files should be migrated to the new config location.")
        print("  The setup script CANNOT migrate automatically (secrets must be manually copied).")
        print()
        print("  MANUAL MIGRATION STEPS:")
        print(f"  1. Open each legacy .env file and copy the secrets")
        print(f"  2. Edit: {platform_env}")
        print(f"  3. Paste the secrets into the appropriate sections")
        print(f"  4. Delete the legacy .env files from the repo")
        print()
        input("  Press Enter to continue after you've noted these files...")
    else:
        print("  ✓ No legacy .env files found in repo")
    print()

    # Step 5: Validate setup
    print("[4/5] Validating configuration...")
    validation = ConfigManager.validate()

    if validation["errors"]:
        print("  ✗ ERRORS FOUND:")
        for error in validation["errors"]:
            print(f"    - {error}")
    else:
        print("  ✓ No critical errors")

    if validation["warnings"]:
        print("  ⚠ WARNINGS:")
        for warning in validation["warnings"]:
            print(f"    - {warning}")
    else:
        print("  ✓ No warnings")
    print()

    # Step 6: Next steps
    print("[5/5] Setup Complete!")
    print()
    print("  NEXT STEPS:")
    print(f"  1. Edit your configuration file:")
    print(f"     {platform_env}")
    print()
    print("  2. Set required secrets:")
    print("     - OPENAI_API_KEY (required for CanadaQuebec)")
    print("     - ALFABETA_USER and ALFABETA_PASS (required for Argentina)")
    print()
    print("  3. Run the doctor command to verify:")
    print("     python doctor.py")
    print()
    print("  4. Optional: Create scraper-specific overrides:")
    print(f"     {config_dir / 'CanadaQuebec.env'}")
    print(f"     {config_dir / 'Malaysia.env'}")
    print(f"     {config_dir / 'Argentina.env'}")
    print()

    if legacy_env_files:
        print("  ⚠ REMINDER: Migrate secrets from legacy .env files, then delete them:")
        for location, path in legacy_env_files:
            print(f"     {path}")
        print()

    print("=" * 70)
    print("Configuration setup complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
