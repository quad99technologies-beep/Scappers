#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shared Utilities Module

Common utilities used across all scrapers to reduce code duplication
and maintain consistency.
"""

from pathlib import Path
import shutil
import os
from datetime import datetime
from typing import Dict, Optional


def _is_within(path: Path, root: Path) -> bool:
    """Return True if path is the same as or inside root."""
    try:
        return path.is_relative_to(root)
    except AttributeError:  # Python <3.9 fallback
        path_str = str(path)
        root_str = str(root)
        return path_str == root_str or path_str.startswith(root_str + os.sep)


def _is_within_any(path: Path, roots: list[Path]) -> bool:
    """Return True if path is the same as or inside any path in roots."""
    for root in roots:
        if _is_within(path, root):
            return True
    return False


def _collect_files(base: Path, exclude_dirs: Optional[list] = None) -> list[Path]:
    """
    Collect all file paths under base (relative to base), excluding directories in exclude_dirs.
    """
    base = base.resolve()
    exclude_resolved = [Path(d).resolve() for d in (exclude_dirs or [])]
    files: list[Path] = []

    for root, dirs, filenames in os.walk(base):
        root_path = Path(root).resolve()

        # Prune excluded directories
        dirs[:] = [d for d in dirs if not _is_within_any((root_path / d).resolve(), exclude_resolved)]

        for name in filenames:
            file_path = (root_path / name).resolve()
            if _is_within_any(file_path, exclude_resolved):
                continue
            files.append(file_path.relative_to(base))

    return files


def get_latest_modification_time(directory: Path, exclude_dirs: Optional[list] = None) -> datetime:
    """
    Get the latest modification time of any file in the directory tree.
    
    Args:
        directory: Directory to scan
        exclude_dirs: List of directory paths to exclude from scanning
    
    Returns:
        datetime of the most recent file modification
    """
    if not directory.exists():
        return datetime.now()
    
    if exclude_dirs is None:
        exclude_dirs = []
    
    latest_time = None
    directory_resolved = directory.resolve()
    exclude_resolved = [Path(d).resolve() for d in exclude_dirs]
    
    # Walk through all files in the directory
    for root, dirs, files in os.walk(directory):
        root_path = Path(root).resolve()
        
        # Skip excluded directories
        for excl_dir in exclude_resolved:
            try:
                if excl_dir.is_relative_to(root_path) or root_path.is_relative_to(excl_dir):
                    dirs[:] = []  # Don't descend into excluded directory
                    break
            except (AttributeError, ValueError):
                # Python < 3.9 fallback
                if str(excl_dir) in str(root_path) or str(root_path) in str(excl_dir):
                    dirs[:] = []
                    break
        
        for file in files:
            file_path = root_path / file
            try:
                mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                if latest_time is None or mtime > latest_time:
                    latest_time = mtime
            except Exception:
                continue
    
    return latest_time if latest_time else datetime.now()


def backup_output_folder(
    output_dir: Path,
    backup_dir: Path,
    central_output_dir: Optional[Path] = None,
    exclude_dirs: Optional[list] = None
) -> Dict[str, any]:
    """
    Backup the output folder to backups directory with timestamp.
    
    Args:
        output_dir: Output directory to backup
        backup_dir: Backup directory where backup will be created
        central_output_dir: Optional central output directory to also backup
        exclude_dirs: Optional list of directories to exclude from backup
    
    Returns:
        dict with status and details
    """
    # Check if output folder exists
    if not output_dir.exists():
        return {
            "status": "skipped",
            "message": "Output folder does not exist, nothing to backup"
        }

    exclude_dirs = exclude_dirs or []
    # Always exclude the destination backup directory to avoid recursion
    if backup_dir:
        exclude_dirs = [*exclude_dirs, str(backup_dir)]

    # Gather source file list (relative paths) to verify copy completeness
    source_files = _collect_files(output_dir, exclude_dirs)

    if not source_files:
        return {
            "status": "skipped",
            "message": "Output folder is empty, nothing to backup"
        }
    
    # Get latest modification time from output folder
    latest_time = get_latest_modification_time(output_dir, exclude_dirs)
    
    # Create backup folder name with timestamp
    timestamp = latest_time.strftime("%Y%m%d_%H%M%S")
    backup_folder = backup_dir / f"output_{timestamp}"
    
    # Create backups directory if it doesn't exist
    backup_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # If a backup folder with the same timestamp exists, rebuild it to avoid partial copies
        if backup_folder.exists():
            shutil.rmtree(backup_folder)

        # Copy entire output folder to backup location
        # Use ignore function to prevent copying backup directories (recursion prevention)
        backup_dir_str = str(backup_dir.resolve())
        exclude_dirs_str = [str(Path(d).resolve()) for d in (exclude_dirs or [])]
        
        def ignore_backup_dirs(dirname, names):
            """Ignore backup directories to prevent recursion"""
            ignored = []
            for name in names:
                item_path = Path(dirname) / name
                try:
                    item_resolved = str(item_path.resolve())
                    # Skip if this path is the backup directory or inside it
                    if item_resolved == backup_dir_str or item_resolved.startswith(backup_dir_str + os.sep):
                        ignored.append(name)
                    # Skip other excluded directories
                    for excl_dir in exclude_dirs_str:
                        if item_resolved == excl_dir or item_resolved.startswith(excl_dir + os.sep):
                            ignored.append(name)
                            break
                except:
                    pass
            return ignored
        
        shutil.copytree(output_dir, backup_folder, ignore=ignore_backup_dirs, dirs_exist_ok=True)
        
        # Also backup final reports (exports) to scraper-specific backup folder
        if central_output_dir and central_output_dir.exists() and central_output_dir != output_dir:
            exports_backup_dir = backup_folder / "exports"
            exports_backup_dir.mkdir(parents=True, exist_ok=True)
            # Copy all files from exports directory
            for item in central_output_dir.iterdir():
                if item.is_file():
                    shutil.copy2(item, exports_backup_dir / item.name)
                elif item.is_dir():
                    shutil.copytree(item, exports_backup_dir / item.name, dirs_exist_ok=True)

            # Track central/export files for completeness verification
            exports_files = _collect_files(central_output_dir, [])
            source_files.extend([Path("exports") / rel for rel in exports_files])
        
        # Count files backed up
        dest_files = [p.relative_to(backup_folder) for p in backup_folder.rglob('*') if p.is_file()]
        file_count = len(dest_files)

        # Verify completeness
        if file_count != len(source_files):
            return {
                "status": "error",
                "message": f"Backup incomplete: copied {file_count} of {len(source_files)} files",
                "backup_folder": str(backup_folder),
                "timestamp": timestamp,
                "latest_modification": latest_time.strftime("%Y-%m-%d %H:%M:%S"),
                "files_backed_up": file_count,
                "files_expected": len(source_files),
            }
        
        return {
            "status": "ok",
            "backup_folder": str(backup_folder),
            "timestamp": timestamp,
            "latest_modification": latest_time.strftime("%Y-%m-%d %H:%M:%S"),
            "files_backed_up": file_count
        }
    
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to backup output folder: {str(e)}"
        }


def clean_output_folder(
    output_dir: Path,
    backup_dir: Optional[Path] = None,
    central_output_dir: Optional[Path] = None,
    keep_files: Optional[list] = None,
    keep_dirs: Optional[list] = None
) -> Dict[str, any]:
    """
    Delete all files and subdirectories from the output folder.
    
    Args:
        output_dir: Output directory to clean
        backup_dir: Backup directory to exclude from deletion
        central_output_dir: Central output directory to exclude from deletion
        keep_files: List of file names to keep (e.g., ["execution_log.txt"])
        keep_dirs: List of directory names to keep (e.g., ["runs", "backups"])
    
    Returns:
        dict with status and details
    """
    if not output_dir.exists():
        return {
            "status": "skipped",
            "message": "Output folder does not exist"
        }
    
    if keep_files is None:
        keep_files = []
    if keep_dirs is None:
        keep_dirs = ["runs", "backups"]
    
    try:
        files_deleted = 0
        dirs_deleted = 0
        
        # Get resolved paths for comparison
        output_dir_resolved = output_dir.resolve()
        backup_dir_resolved = backup_dir.resolve() if backup_dir else None
        central_output_dir_resolved = central_output_dir.resolve() if central_output_dir else None
        
        # Remove all contents, excluding specified directories and files
        for item in output_dir.iterdir():
            item_resolved = item.resolve()
            
            # Skip backup directory
            if backup_dir_resolved and (item_resolved == backup_dir_resolved or 
                                       (backup_dir_resolved.exists() and 
                                        str(item_resolved).startswith(str(backup_dir_resolved) + os.sep))):
                continue
            
            # Skip central output directory (final output files - do not remove)
            if central_output_dir_resolved and output_dir_resolved == central_output_dir_resolved:
                # If OUTPUT_DIR is the same as CENTRAL_OUTPUT_DIR, skip final output files (CSV/XLSX)
                if item.is_file() and item.suffix.lower() in ('.csv', '.xlsx'):
                    continue
            
            # Skip files in keep_files list
            if item.is_file() and item.name in keep_files:
                continue
            
            # Skip directories in keep_dirs list
            if item.is_dir() and item.name.lower() in keep_dirs:
                continue
            
            try:
                if item.is_file():
                    item.unlink()
                    files_deleted += 1
                elif item.is_dir():
                    shutil.rmtree(item)
                    dirs_deleted += 1
            except (PermissionError, OSError) as e:
                # Skip files/dirs that are in use (like open log files)
                continue
        
        return {
            "status": "ok",
            "files_deleted": files_deleted,
            "directories_deleted": dirs_deleted
        }
    
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to clean output folder: {str(e)}"
        }

