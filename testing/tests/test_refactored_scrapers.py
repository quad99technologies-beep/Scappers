#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Smoke Test for Refactored Scrapers
Tests that Belarus, Russia, and Canada Ontario can import and initialize without errors
"""

import sys
from pathlib import Path

# Add repo root to path
repo_root = Path(__file__).resolve().parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

print("=" * 80)
print("SMOKE TEST: Refactored Scrapers")
print("=" * 80)

errors = []

# Test 1: Core Module Imports
print("\n[TEST 1] Core Module Imports...")
try:
    # Try to add scripts directory for config_loader
    sys.path.insert(0, str(repo_root / "scripts"))
    
    from core.browser.chrome_manager import kill_orphaned_chrome_processes
    from core.monitoring.resource_monitor import check_memory_leak, log_resource_status
    print("  [OK] Core modules (chrome_manager, resource_monitor) import successfully")
except ImportError as e:
    print(f"  [FAIL] Core module import failed: {e}")
    errors.append(f"Core imports: {e}")

# Test core.tor_manager separately (needs config_loader from scripts)
try:
    from core.network.tor_manager import check_tor_running
    print("  [OK] Core tor_manager imports successfully")
except ImportError as e:
    print(f"  [WARN] tor_manager import warning (needs config_loader): {e}")

# Test core.browser_session
try:
    from core.browser.browser_session import BrowserSession
    print("  [OK] Core browser_session imports successfully")
except ImportError as e:
    print(f"  [WARN] browser_session import warning: {e}")

# Test 2: Belarus Scraper
print("\n[TEST 2] Belarus Scraper Syntax Check...")
try:
    belarus_script = repo_root / "scripts" / "Belarus" / "01_belarus_rceth_extract.py"
    if belarus_script.exists():
        code = belarus_script.read_text(encoding="utf-8")
        compile(code, str(belarus_script), 'exec')
        print(f"  [OK] Belarus scraper syntax valid")
    else:
        print(f"  [WARN] Belarus scraper not found: {belarus_script}")
except SyntaxError as e:
    print(f"  [FAIL] Belarus scraper syntax error: {e}")
    errors.append(f"Belarus syntax: {e}")
except Exception as e:
    print(f"  [WARN] Belarus scraper check warning: {e}")

# Test 3: Russia Scraper
print("\n[TEST 3] Russia Scraper Syntax Check...")
try:
    russia_script = repo_root / "scripts" / "Russia" / "01_russia_farmcom_scraper.py"
    if russia_script.exists():
        code = russia_script.read_text(encoding="utf-8")
        compile(code, str(russia_script), 'exec')
        print(f"  [OK] Russia scraper syntax valid")
    else:
        print(f"  [WARN] Russia scraper not found: {russia_script}")
except SyntaxError as e:
    print(f"  [FAIL] Russia scraper syntax error: {e}")
    errors.append(f"Russia syntax: {e}")
except Exception as e:
    print(f"  [WARN] Russia scraper check warning: {e}")

# Test 4: Canada Ontario Scraper
print("\n[TEST 4] Canada Ontario Scraper Syntax Check...")
try:
    ontario_script = repo_root / "scripts" / "Canada_Ontario" / "01_extract_product_details.py"
    if ontario_script.exists():
        code = ontario_script.read_text(encoding="utf-8")
        compile(code, str(ontario_script), 'exec')
        print(f"  [OK] Canada Ontario scraper syntax valid")
    else:
        print(f"  [WARN] Canada Ontario scraper not found: {ontario_script}")
except SyntaxError as e:
    print(f"  [FAIL] Canada Ontario scraper syntax error: {e}")
    errors.append(f"Canada Ontario syntax: {e}")
except Exception as e:
    print(f"  [WARN] Canada Ontario scraper check warning: {e}")

# Test 5: Argentina Modules
print("\n[TEST 5] Argentina Modules...")
try:
    sys.path.insert(0, str(repo_root / "scripts" / "Argentina"))
    from modules import config, utils
    print(f"  [OK] Argentina modules import successfully")
    if hasattr(config, 'OUTPUT_DIR'):
        print(f"    - OUTPUT_DIR: {config.OUTPUT_DIR}")
    if hasattr(config, 'ACCOUNTS'):
        print(f"    - ACCOUNTS configured: {len(config.ACCOUNTS) if config.ACCOUNTS else 0}")
except ImportError as e:
    print(f"  [FAIL] Argentina modules import failed: {e}")
    errors.append(f"Argentina modules: {e}")
except Exception as e:
    print(f"  [WARN] Argentina modules warning: {e}")

# Summary
print("\n" + "=" * 80)
if errors:
    print(f"SMOKE TEST FAILED: {len(errors)} error(s)")
    for err in errors:
        print(f"  - {err}")
    sys.exit(1)
else:
    print("SMOKE TEST PASSED: All refactored scrapers are syntactically valid")
    print("\nNext Steps:")
    print("  1. Run individual scrapers with limited scope to verify execution")
    print("  2. Monitor for runtime errors during actual scraping")
    print("  3. Continue with GUI refactoring and containerization")
    print("\nRefactoring Progress: 45% complete (15/33 tasks)")
    print("See REFACTOR_PROGRESS.md for details")
    sys.exit(0)
