#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backup Output Folder - Canada Ontario

Creates a backup of the output folder with a timestamp based on the latest
file modification date, then cleans the output folder for a fresh run.
"""

from pathlib import Path
import sys

# Add repo root to path for shared utilities
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.shared_utils import backup_output_folder, clean_output_folder
from core.logger import setup_standard_logger
from core.progress_tracker import StandardProgress
from config_loader import get_run_id, get_run_dir

# Get script directory
SCRIPT_DIR = Path(__file__).resolve().parent

from platform_config import get_path_manager
pm = get_path_manager()
OUTPUT_DIR = pm.get_output_dir("CanadaOntario")
BACKUP_DIR = pm.get_backups_dir("CanadaOntario")
LOCAL_OUTPUT_DIR = SCRIPT_DIR / "output"

# Ensure directories exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_DIR.mkdir(parents=True, exist_ok=True)
if LOCAL_OUTPUT_DIR != OUTPUT_DIR:
    LOCAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def main() -> None:
    """Main entry point."""
    run_id = get_run_id()
    run_dir = get_run_dir(run_id)
    logger = setup_standard_logger(
        "canada_ontario_backup",
        scraper_name="CanadaOntario",
        log_file=run_dir / "logs" / "backup.log",
    )
    progress = StandardProgress("canada_ontario_backup", total=2, unit="steps", logger=logger, state_path=BACKUP_DIR / "backup_progress.json")
    logger.info("Backup and clean output folder - Canada Ontario")

    # Backup both platform output and local output (if different)
    output_dirs_to_backup = []
    if OUTPUT_DIR.exists():
        output_dirs_to_backup.append(OUTPUT_DIR)
    if LOCAL_OUTPUT_DIR != OUTPUT_DIR and LOCAL_OUTPUT_DIR.exists():
        output_dirs_to_backup.append(LOCAL_OUTPUT_DIR)

    if not output_dirs_to_backup:
        logger.info("No output directories found to backup")
    else:
        errors = 0
        # Step 1: Backup
        progress.update(0, message="backup start", force=True)
        for output_dir in output_dirs_to_backup:
            backup_result = backup_output_folder(
                output_dir=output_dir,
                backup_dir=BACKUP_DIR,
                central_output_dir=None,
                exclude_dirs=[str(BACKUP_DIR)]
            )

            if backup_result["status"] == "ok":
                logger.info("Backup created for %s: %s (files=%s)", output_dir.name, backup_result["backup_folder"], backup_result["files_backed_up"])
            elif backup_result["status"] == "skipped":
                logger.info("Backup skipped: %s", backup_result["message"])
            else:
                logger.error("Backup error: %s", backup_result["message"])
                errors += 1
        progress.update(1, message="backup done", force=True)

        # Step 2: Clean
        progress.update(1, message="clean start", force=True)
        for output_dir in output_dirs_to_backup:
            clean_result = clean_output_folder(
                output_dir=output_dir,
                backup_dir=BACKUP_DIR,
                central_output_dir=None,
                keep_files=[],
                keep_dirs=["runs", "backups"]
            )

            if clean_result["status"] == "ok":
                logger.info("Output folder cleaned: %s (files=%s dirs=%s)", output_dir.name, clean_result["files_deleted"], clean_result["directories_deleted"])
            elif clean_result["status"] == "skipped":
                logger.info("Cleanup skipped: %s", clean_result["message"])
            else:
                logger.error("Cleanup error: %s", clean_result["message"])
                errors += 1
        progress.update(2, message="clean done", force=True)

        if errors:
            logger.error("Backup/cleanup completed with errors")
            raise SystemExit(1)

    logger.info("Backup and cleanup complete")


if __name__ == "__main__":
    main()
