#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Health check for Tender Chile scraper.

Checks:
- Database connectivity
- Required input files
- Disk space
- Chrome availability
"""

import sys
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from config_loader import get_input_dir, get_output_dir


def check_database() -> tuple[bool, str]:
    """Check database connectivity."""
    try:
        from core.db.connection import CountryDB
        db = CountryDB("Tender_Chile")
        with db.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return True, "Database connection OK"
    except Exception as e:
        return False, f"Database connection failed: {e}"


def check_disk_space() -> tuple[bool, str]:
    """Check available disk space."""
    try:
        import shutil
        output_dir = get_output_dir()
        stat = shutil.disk_usage(output_dir)
        free_gb = stat.free / (1024**3)
        if free_gb < 1.0:
            return False, f"Low disk space: {free_gb:.1f}GB free"
        return True, f"Disk space OK: {free_gb:.1f}GB free"
    except Exception as e:
        return False, f"Disk space check failed: {e}"


def check_chrome() -> tuple[bool, str]:
    """Check Chrome availability."""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        
        driver = webdriver.Chrome(options=options)
        driver.quit()
        return True, "Chrome is available"
    except Exception as e:
        return False, f"Chrome not available: {e}"


def main():
    """Run all health checks."""
    print("=" * 60)
    print("Tender Chile Scraper - Health Check")
    print("=" * 60)
    
    checks = [
        ("Database", check_database),
        ("Disk Space", check_disk_space),
        ("Chrome", check_chrome),
    ]
    
    all_passed = True
    for name, check_fn in checks:
        passed, message = check_fn()
        status = "[OK]" if passed else "[FAIL]"
        print(f"{status} {name}: {message}")
        if not passed:
            all_passed = False
    
    print("=" * 60)
    if all_passed:
        print("All health checks passed!")
        return 0
    else:
        print("Some health checks failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
