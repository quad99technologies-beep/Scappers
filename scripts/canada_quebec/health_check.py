#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Health check for Canada Quebec scraper.

Checks:
- Database connectivity
- Disk space
- OpenAI API key availability
- Required input files (PDF)
"""

import os
import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from config_loader import get_input_dir, get_output_dir


def check_database() -> tuple:
    """Check database connectivity."""
    try:
        from core.db.connection import CountryDB
        db = CountryDB("CanadaQuebec")
        with db.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return True, "Database connection OK"
    except Exception as e:
        return False, f"Database connection failed: {e}"


def check_disk_space() -> tuple:
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


def check_openai_key() -> tuple:
    """Check OpenAI API key availability."""
    try:
        api_key = os.getenv("OPENAI_API_KEY", "")
        if not api_key:
            # Try from config
            try:
                from config_loader import getenv
                api_key = getenv("OPENAI_API_KEY", "")
            except Exception:
                pass
        if api_key and api_key.startswith("sk-"):
            return True, "OpenAI API key is configured"
        elif api_key:
            return False, "OpenAI API key format invalid (should start with 'sk-')"
        else:
            return False, "OpenAI API key not set (required for PDF extraction)"
    except Exception as e:
        return False, f"OpenAI key check failed: {e}"


def check_input_files() -> tuple:
    """Check required input PDF file."""
    try:
        input_dir = get_input_dir()
        pdf_file = input_dir / "liste-med.pdf"
        if pdf_file.exists():
            size_mb = pdf_file.stat().st_size / (1024 * 1024)
            return True, f"Input PDF found: {pdf_file.name} ({size_mb:.1f}MB)"
        return False, f"Input PDF missing: {pdf_file}"
    except Exception as e:
        return False, f"Input file check failed: {e}"


def main():
    """Run all health checks."""
    print("=" * 60)
    print("Canada Quebec Scraper - Health Check")
    print("=" * 60)
    checks = [
        ("Database", check_database),
        ("Disk Space", check_disk_space),
        ("OpenAI API Key", check_openai_key),
        ("Input Files", check_input_files),
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
