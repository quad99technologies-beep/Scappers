#!/usr/bin/env python3
"""
Repository Cleanup Script - Remove old archived code and backups

This script will:
1. Delete .bak files (archived old scripts)
2. Delete old backup directories (backups/)
3. Remove archive/ directories with old code
4. Clean up temp/test files

SAFE TO RUN: Only removes files that are:
- Already in archive/ directories
- .bak backup files
- Old output backups
- Not tracked in git (untracked files)
"""

import os
import shutil
from pathlib import Path

REPO_ROOT = Path(__file__).parent

def get_size_mb(path: Path) -> float:
    """Get total size of directory or file in MB."""
    if path.is_file():
        return path.stat().st_size / (1024 * 1024)

    total = 0
    try:
        for item in path.rglob('*'):
            if item.is_file():
                total += item.stat().st_size
    except Exception:
        pass
    return total / (1024 * 1024)


def cleanup_bak_files():
    """Remove .bak files (old script backups)."""
    print("\n" + "="*60)
    print("CLEANING: .bak files (old script backups)")
    print("="*60)

    bak_files = list(REPO_ROOT.rglob("*.bak"))
    total_size = sum(get_size_mb(f) for f in bak_files)

    print(f"Found {len(bak_files)} .bak files ({total_size:.2f} MB)")

    if bak_files:
        for f in bak_files:
            rel_path = f.relative_to(REPO_ROOT)
            print(f"  Deleting: {rel_path}")
            f.unlink()
        print(f"✓ Deleted {len(bak_files)} .bak files")
    else:
        print("✓ No .bak files found")


def cleanup_old_backups():
    """Remove old backup directories."""
    print("\n" + "="*60)
    print("CLEANING: Old backup directories")
    print("="*60)

    backups_dir = REPO_ROOT / "backups"
    if not backups_dir.exists():
        print("✓ No backups/ directory found")
        return

    # Count subdirectories
    backup_dirs = [d for d in backups_dir.rglob("*") if d.is_dir()]
    total_size = get_size_mb(backups_dir)

    print(f"Found backups/ with {len(backup_dirs)} subdirectories ({total_size:.2f} MB)")

    # List some examples
    country_dirs = [d for d in backups_dir.iterdir() if d.is_dir()]
    for country_dir in country_dirs[:5]:  # Show first 5
        backup_count = len([d for d in country_dir.iterdir() if d.is_dir()])
        print(f"  {country_dir.name}: {backup_count} backups")

    if len(country_dirs) > 5:
        print(f"  ... and {len(country_dirs) - 5} more countries")

    response = input(f"\nDelete entire backups/ directory? ({total_size:.2f} MB) [y/N]: ")
    if response.lower() == 'y':
        shutil.rmtree(backups_dir)
        print(f"✓ Deleted backups/ directory ({total_size:.2f} MB freed)")
    else:
        print("⊘ Skipped backups/ deletion")


def cleanup_archive_dirs():
    """Remove archive/ directories with old code."""
    print("\n" + "="*60)
    print("CLEANING: archive/ directories (old code)")
    print("="*60)

    archive_dirs = [d for d in REPO_ROOT.rglob("archive") if d.is_dir()]

    print(f"Found {len(archive_dirs)} archive/ directories")

    total_size = 0
    for archive_dir in archive_dirs:
        size_mb = get_size_mb(archive_dir)
        total_size += size_mb
        file_count = len([f for f in archive_dir.rglob("*") if f.is_file()])
        rel_path = archive_dir.relative_to(REPO_ROOT)
        print(f"  {rel_path}: {file_count} files ({size_mb:.2f} MB)")

    print(f"\nTotal: {total_size:.2f} MB in archive directories")

    response = input(f"\nDelete all archive/ directories? [y/N]: ")
    if response.lower() == 'y':
        for archive_dir in archive_dirs:
            shutil.rmtree(archive_dir)
        print(f"✓ Deleted {len(archive_dirs)} archive/ directories ({total_size:.2f} MB freed)")
    else:
        print("⊘ Skipped archive/ deletion")


def cleanup_temp_files():
    """Remove temporary and test files."""
    print("\n" + "="*60)
    print("CLEANING: Temporary/test files")
    print("="*60)

    patterns = [
        "temp_*.txt",
        "temp_*.html",
        "temp_*.json",
        "test_*.py",
        "*_test.py",
        "*.tmp",
    ]

    temp_files = []
    for pattern in patterns:
        temp_files.extend(REPO_ROOT.rglob(pattern))

    # Filter out legitimate test files (in tests/ or testing/ dirs)
    temp_files = [f for f in temp_files if 'testing' not in f.parts and 'tests' not in f.parts]

    total_size = sum(get_size_mb(f) for f in temp_files if f.is_file())

    if temp_files:
        print(f"Found {len(temp_files)} temp files ({total_size:.2f} MB)")
        for f in temp_files[:10]:  # Show first 10
            rel_path = f.relative_to(REPO_ROOT)
            print(f"  {rel_path}")

        if len(temp_files) > 10:
            print(f"  ... and {len(temp_files) - 10} more")

        response = input(f"\nDelete temp files? [y/N]: ")
        if response.lower() == 'y':
            for f in temp_files:
                if f.is_file():
                    f.unlink()
            print(f"✓ Deleted {len(temp_files)} temp files ({total_size:.2f} MB freed)")
        else:
            print("⊘ Skipped temp files deletion")
    else:
        print("✓ No temp files found")


def main():
    print("="*60)
    print("REPOSITORY CLEANUP SCRIPT")
    print("="*60)
    print(f"Repository: {REPO_ROOT}")
    print("")
    print("This script will help clean up old files:")
    print("  • .bak files (old script backups)")
    print("  • backups/ directory (old output backups)")
    print("  • archive/ directories (old code)")
    print("  • temp/test files")
    print("")

    response = input("Continue with cleanup? [y/N]: ")
    if response.lower() != 'y':
        print("Cleanup cancelled")
        return

    # Run cleanup steps
    cleanup_bak_files()
    cleanup_old_backups()
    cleanup_archive_dirs()
    cleanup_temp_files()

    print("\n" + "="*60)
    print("CLEANUP COMPLETE!")
    print("="*60)


if __name__ == "__main__":
    main()
