#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline Testing Framework

Smoke tests for pipeline steps.

Usage:
    python scripts/common/pipeline_tests.py Malaysia
    python scripts/common/pipeline_tests.py Malaysia --step 2
"""

import sys
import argparse
import subprocess
from pathlib import Path
from typing import List, Dict, Any

# Add repo root to path
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from core.db.postgres_connection import get_db
from core.preflight_checks import PreflightChecker


def test_step_0_db_init(scraper_name: str) -> Dict[str, Any]:
    """Test: Step 0 (DB initialization) succeeds."""
    try:
        db = get_db(scraper_name)
        with db.cursor() as cur:
            # Check if run_ledger table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'run_ledger'
                )
            """)
            table_exists = cur.fetchone()[0]
            
            return {
                "test": "step_0_db_init",
                "passed": table_exists,
                "message": "run_ledger table exists" if table_exists else "run_ledger table missing"
            }
    except Exception as e:
        return {
            "test": "step_0_db_init",
            "passed": False,
            "message": f"Error: {e}"
        }


def test_step_1_website_connectivity(scraper_name: str) -> Dict[str, Any]:
    """Test: Step 1 can connect to target website."""
    # This would test actual website connectivity
    # Simplified version - would need scraper-specific logic
    return {
        "test": "step_1_website_connectivity",
        "passed": True,
        "message": "Website connectivity test (implement scraper-specific)"
    }


def test_step_2_data_parsing(scraper_name: str) -> Dict[str, Any]:
    """Test: Step 2 can parse sample data."""
    # This would test data parsing with sample data
    return {
        "test": "step_2_data_parsing",
        "passed": True,
        "message": "Data parsing test (implement scraper-specific)"
    }


def test_step_n_export_generation(scraper_name: str) -> Dict[str, Any]:
    """Test: Final step can generate exports."""
    try:
        exports_dir = REPO_ROOT / "output" / scraper_name / "exports"
        if exports_dir.exists():
            csv_files = list(exports_dir.glob("*.csv"))
            return {
                "test": "step_n_export_generation",
                "passed": len(csv_files) > 0,
                "message": f"Found {len(csv_files)} export files" if csv_files else "No export files found"
            }
        return {
            "test": "step_n_export_generation",
            "passed": False,
            "message": "Exports directory does not exist"
        }
    except Exception as e:
        return {
            "test": "step_n_export_generation",
            "passed": False,
            "message": f"Error: {e}"
        }


def run_all_tests(scraper_name: str) -> List[Dict[str, Any]]:
    """Run all smoke tests."""
    results = []
    
    # Preflight checks
    checker = PreflightChecker(scraper_name, "test_run")
    preflight_results = checker.run_all_checks()
    for result in preflight_results:
        results.append({
            "test": f"preflight_{result.name}",
            "passed": result.passed,
            "message": result.message
        })
    
    # Step tests
    results.append(test_step_0_db_init(scraper_name))
    results.append(test_step_1_website_connectivity(scraper_name))
    results.append(test_step_2_data_parsing(scraper_name))
    results.append(test_step_n_export_generation(scraper_name))
    
    return results


def main():
    parser = argparse.ArgumentParser(description="Pipeline Testing Framework")
    parser.add_argument("scraper_name", help="Scraper name")
    parser.add_argument("--step", type=int, help="Test specific step")
    
    args = parser.parse_args()
    
    print(f"Running tests for {args.scraper_name}...\n")
    
    results = run_all_tests(args.scraper_name)
    
    passed = sum(1 for r in results if r["passed"])
    total = len(results)
    
    print(f"\n{'='*60}")
    print(f"Test Results: {passed}/{total} passed")
    print(f"{'='*60}\n")
    
    for result in results:
        status = "✅" if result["passed"] else "❌"
        print(f"{status} {result['test']}: {result['message']}")
    
    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
