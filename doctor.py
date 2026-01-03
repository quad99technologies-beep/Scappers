#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper Platform Doctor - Configuration and Path Verification Tool

Verifies:
- ConfigManager initialization
- App root directory structure
- Environment file locations
- No env files in wrong locations
- Single-instance lock status
"""

import sys
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
    print("Scraper Platform - Doctor Command")
    print("=" * 70)
    print()
    
    # Initialize ConfigManager
    try:
        ConfigManager.ensure_dirs()
        print("[OK] ConfigManager initialized")
    except Exception as e:
        print(f"[ERROR] ConfigManager initialization failed: {e}")
        sys.exit(1)
    
    # Get app root
    app_root = ConfigManager.get_app_root()
    print()
    print("PATHS:")
    print(f"  App Root:        {app_root}")
    print(f"  App Root Exists: {'[OK]' if app_root.exists() else '[MISSING]'}")
    print()
    
    # Check directory structure
    dirs = {
        "config": ConfigManager.get_config_dir(),
        "input": ConfigManager.get_input_dir(),
        "output": ConfigManager.get_output_dir(),
        "exports": ConfigManager.get_exports_dir(),
        "runs": ConfigManager.get_runs_dir(),
        "sessions": ConfigManager.get_sessions_dir(),
        "logs": ConfigManager.get_logs_dir(),
        "cache": ConfigManager.get_cache_dir(),
    }
    
    print("DIRECTORY STRUCTURE:")
    for name, path in dirs.items():
        exists = path.exists()
        status = "[OK]" if exists else "[MISSING]"
        print(f"  {name:12} {status:8} {path}")
    print()
    
    # Check environment files
    print("ENVIRONMENT FILES:")
    platform_env, _ = ConfigManager.env_paths("CanadaQuebec")  # Just for path example
    platform_env = ConfigManager.get_config_dir() / "platform.env"
    
    print(f"  Platform .env:  {platform_env}")
    if platform_env.exists():
        print(f"    Status: [OK] exists")
        # Count lines
        try:
            with open(platform_env, 'r', encoding='utf-8') as f:
                lines = [l for l in f if l.strip() and not l.strip().startswith('#')]
                print(f"    Variables: {len(lines)}")
        except Exception as e:
            print(f"    Error reading: {e}")
    else:
        print(f"    Status: [MISSING] - REQUIRED")
        print(f"    Action: Create {platform_env} from .env.example template")
    print()
    
    # Check for scraper-specific env files
    print("SCRAPER ENVIRONMENT FILES (optional):")
    for scraper_name in ["CanadaQuebec", "Malaysia", "Argentina"]:
        _, scraper_env = ConfigManager.env_paths(scraper_name)
        if scraper_env.exists():
            print(f"  {scraper_name:15} [OK] {scraper_env}")
        else:
            print(f"  {scraper_name:15} [  ] {scraper_env} (optional)")
    print()
    
    # Validate configuration
    print("VALIDATION:")
    validation = ConfigManager.validate()
    
    if validation["errors"]:
        print("  ERRORS:")
        for error in validation["errors"]:
            print(f"    - {error}")
    else:
        print("  [OK] No critical errors")
    
    if validation["warnings"]:
        print("  WARNINGS:")
        for warning in validation["warnings"]:
            print(f"    - {warning}")
    else:
        print("  [OK] No warnings")
    print()
    
    # Check for env files in wrong locations
    print("ENV FILE LOCATION CHECK:")
    wrong_locations = []
    for pattern in ["*.env", "platform.env"]:
        # Check output directories
        output_dir = ConfigManager.get_output_dir()
        if output_dir.exists():
            for env_file in output_dir.rglob(pattern):
                wrong_locations.append(env_file)
        
        # Check backups
        backups_dir = output_dir / "backups"
        if backups_dir.exists():
            for env_file in backups_dir.rglob(pattern):
                wrong_locations.append(env_file)
        
        # Check runs
        runs_dir = ConfigManager.get_runs_dir()
        if runs_dir.exists():
            for env_file in runs_dir.rglob(pattern):
                wrong_locations.append(env_file)
    
    if wrong_locations:
        print(f"  [ERROR] Found {len(wrong_locations)} env file(s) in wrong locations:")
        for loc in wrong_locations[:10]:
            print(f"    - {loc}")
        if len(wrong_locations) > 10:
            print(f"    ... and {len(wrong_locations) - 10} more")
        print()
        print("  ACTION REQUIRED: Remove these files. Config must be ONLY in:")
        print(f"    {ConfigManager.get_config_dir()}")
    else:
        print("  [OK] No env files found in output/, backups/, or runs/")
    print()
    
    # Check lock file
    print("SINGLE-INSTANCE LOCK:")
    lock_file = ConfigManager.get_sessions_dir() / "app.lock"
    if lock_file.exists():
        try:
            with open(lock_file, 'r') as f:
                content = f.read().strip().split('\n')
                if content and content[0].isdigit():
                    pid = int(content[0])
                    print(f"  Lock file exists: {lock_file}")
                    print(f"  Lock PID: {pid}")
                    print(f"  Status: [LOCKED] - Another instance may be running")
                else:
                    print(f"  Lock file exists but format is invalid")
                    print(f"  Status: [STALE] - Safe to remove")
        except Exception as e:
            print(f"  Lock file exists but cannot be read: {e}")
            print(f"  Status: [STALE] - Safe to remove")
    else:
        print(f"  Lock file: {lock_file}")
        print(f"  Status: [OK] No lock (no instance running)")
    print()
    
    # Test env loading (dry run)
    print("ENVIRONMENT LOADING TEST:")
    for scraper_name in ["CanadaQuebec", "Malaysia", "Argentina"]:
        try:
            platform_env, scraper_env = ConfigManager.env_paths(scraper_name)
            if platform_env.exists():
                print(f"  {scraper_name:15} [OK] Can load from {platform_env.name}")
                if scraper_env.exists():
                    print(f"                 [OK] Override available: {scraper_env.name}")
            else:
                print(f"  {scraper_name:15} [ERROR] platform.env missing")
        except Exception as e:
            print(f"  {scraper_name:15} [ERROR] {e}")
    print()
    
    # Final summary
    print("=" * 70)
    if validation["errors"] or wrong_locations:
        print("DOCTOR CHECK: ISSUES FOUND")
        print()
        if validation["errors"]:
            print("CRITICAL ERRORS:")
            for error in validation["errors"]:
                print(f"  • {error}")
            print()
        if wrong_locations:
            print("CONFIG FILE VIOLATIONS:")
            print(f"  • {len(wrong_locations)} .env file(s) in wrong locations")
            print()
            print("RECOMMENDED ACTION:")
            print("  1. Ensure platform.env exists in config directory")
            print("  2. Remove misplaced .env files (they should ONLY be in config/)")
            print(f"     python doctor.py --cleanup")
        print()
        print("Run: python setup_config.py  (for first-time setup)")
        print("=" * 70)
        sys.exit(1)
    else:
        print("DOCTOR CHECK: ALL SYSTEMS GO! ✓")
        print("=" * 70)
        sys.exit(0)


def cleanup_wrong_env_files():
    """Remove .env files from wrong locations (interactive)"""
    print("=" * 70)
    print("Scraper Platform - Cleanup Wrong .env Files")
    print("=" * 70)
    print()

    ConfigManager.ensure_dirs()

    # Find wrong locations
    wrong_locations = []
    for pattern in ["*.env", "platform.env"]:
        output_dir = ConfigManager.get_output_dir()
        if output_dir.exists():
            for env_file in output_dir.rglob(pattern):
                wrong_locations.append(env_file)

        backups_dir = output_dir / "backups"
        if backups_dir.exists():
            for env_file in backups_dir.rglob(pattern):
                wrong_locations.append(env_file)

        runs_dir = ConfigManager.get_runs_dir()
        if runs_dir.exists():
            for env_file in runs_dir.rglob(pattern):
                wrong_locations.append(env_file)

    if not wrong_locations:
        print("[OK] No misplaced .env files found!")
        print("=" * 70)
        return

    print(f"Found {len(wrong_locations)} .env file(s) in wrong locations:")
    print()
    for i, env_file in enumerate(wrong_locations[:20], 1):
        print(f"  {i}. {env_file}")
    if len(wrong_locations) > 20:
        print(f"  ... and {len(wrong_locations) - 20} more")
    print()

    response = input("Delete all these files? (yes/no) [no]: ").strip().lower()
    if response != "yes":
        print("Cancelled. No files deleted.")
        print("=" * 70)
        return

    deleted = 0
    errors = 0
    for env_file in wrong_locations:
        try:
            env_file.unlink()
            deleted += 1
        except Exception as e:
            print(f"  Error deleting {env_file}: {e}")
            errors += 1

    print()
    print(f"✓ Deleted: {deleted} file(s)")
    if errors:
        print(f"✗ Errors: {errors} file(s)")
    print()
    print("=" * 70)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--cleanup":
        cleanup_wrong_env_files()
    else:
        main()

