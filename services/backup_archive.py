#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Automated Backup & Archive

Automatic backup of critical data and exports.

Usage:
    python services/backup_archive.py --strategy daily
    python services/backup_archive.py --strategy weekly
    python services/backup_archive.py --strategy monthly
"""

import os
import sys
import argparse
import logging
import shutil
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from typing import List

# Add repo root to path
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from core.db.postgres_connection import get_db

logger = logging.getLogger(__name__)


def backup_database(scraper_name: str, backup_dir: Path) -> Path:
    """Backup database for a scraper."""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = backup_dir / f"{scraper_name}_backup_{timestamp}.sql"
        
        # Use pg_dump (adjust connection string as needed)
        db_name = os.getenv("POSTGRES_DB", "scrapers")
        db_user = os.getenv("POSTGRES_USER", "postgres")
        db_host = os.getenv("POSTGRES_HOST", "localhost")
        
        cmd = [
            "pg_dump",
            "-h", db_host,
            "-U", db_user,
            "-d", db_name,
            "-t", f"{scraper_name.lower()[:2]}*",  # Country-specific tables
            "-t", "run_ledger",
            "-t", "pcid_mapping",
            "-f", str(backup_file)
        ]
        
        subprocess.run(cmd, check=True, env={**os.environ, "PGPASSWORD": os.getenv("POSTGRES_PASSWORD", "")})
        
        logger.info(f"Database backup created: {backup_file}")
        return backup_file
    except Exception as e:
        logger.error(f"Could not backup database: {e}")
        return None


def archive_exports(scraper_name: str, archive_dir: Path, days_old: int = 30) -> int:
    """Archive exports older than specified days."""
    try:
        exports_dir = REPO_ROOT / "output" / scraper_name / "exports"
        if not exports_dir.exists():
            return 0
        
        archived_count = 0
        cutoff_date = datetime.now() - timedelta(days=days_old)
        
        for export_file in exports_dir.glob("*.csv"):
            if export_file.stat().st_mtime < cutoff_date.timestamp():
                archive_path = archive_dir / export_file.name
                shutil.move(str(export_file), str(archive_path))
                archived_count += 1
        
        logger.info(f"Archived {archived_count} export files")
        return archived_count
    except Exception as e:
        logger.error(f"Could not archive exports: {e}")
        return 0


def compress_archive(archive_dir: Path) -> Path:
    """Compress archive directory."""
    try:
        timestamp = datetime.now().strftime("%Y%m%d")
        archive_file = archive_dir.parent / f"archive_{timestamp}.tar.gz"
        
        subprocess.run(
            ["tar", "-czf", str(archive_file), "-C", str(archive_dir.parent), archive_dir.name],
            check=True
        )
        
        logger.info(f"Archive compressed: {archive_file}")
        return archive_file
    except Exception as e:
        logger.error(f"Could not compress archive: {e}")
        return None


def backup_strategy_daily():
    """Daily backup strategy."""
    backup_dir = REPO_ROOT / "backups" / "daily" / datetime.now().strftime("%Y%m%d")
    backup_dir.mkdir(parents=True, exist_ok=True)
    
    scrapers = ["Malaysia", "Argentina", "Netherlands"]
    for scraper in scrapers:
        backup_database(scraper, backup_dir)
    
    logger.info("Daily backup completed")


def backup_strategy_weekly():
    """Weekly backup strategy."""
    backup_dir = REPO_ROOT / "backups" / "weekly" / datetime.now().strftime("%Y%m%d")
    backup_dir.mkdir(parents=True, exist_ok=True)
    
    scrapers = ["Malaysia", "Argentina", "Netherlands"]
    for scraper in scrapers:
        backup_database(scraper, backup_dir)
        archive_exports(scraper, backup_dir / "exports", days_old=7)
    
    logger.info("Weekly backup completed")


def backup_strategy_monthly():
    """Monthly backup strategy."""
    backup_dir = REPO_ROOT / "backups" / "monthly" / datetime.now().strftime("%Y%m")
    backup_dir.mkdir(parents=True, exist_ok=True)
    
    scrapers = ["Malaysia", "Argentina", "Netherlands"]
    for scraper in scrapers:
        backup_database(scraper, backup_dir)
        archive_exports(scraper, backup_dir / "exports", days_old=30)
    
    # Compress monthly archive
    compress_archive(backup_dir)
    
    logger.info("Monthly backup completed")


def main():
    parser = argparse.ArgumentParser(description="Backup and Archive Tool")
    parser.add_argument("--strategy", choices=["daily", "weekly", "monthly"], required=True,
                        help="Backup strategy")
    
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    
    if args.strategy == "daily":
        backup_strategy_daily()
    elif args.strategy == "weekly":
        backup_strategy_weekly()
    elif args.strategy == "monthly":
        backup_strategy_monthly()


if __name__ == "__main__":
    main()
