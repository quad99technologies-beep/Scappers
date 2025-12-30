#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stop Workflow Script

Stops a running scraper pipeline by killing its process and cleaning up the lock file.

Usage:
    python stop_workflow.py <scraper_name>

Example:
    python stop_workflow.py Malaysia
    python stop_workflow.py CanadaQuebec
    python stop_workflow.py Argentina
"""

import sys
from pathlib import Path

# Add repo root to path
repo_root = Path(__file__).resolve().parent
sys.path.insert(0, str(repo_root))

from shared_workflow_runner import WorkflowRunner


def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        print("Usage: python stop_workflow.py <scraper_name>")
        print("\nAvailable scrapers:")
        print("  - CanadaQuebec")
        print("  - Malaysia")
        print("  - Argentina")
        print("\nExample:")
        print("  python stop_workflow.py Malaysia")
        return 1

    scraper_name = sys.argv[1]

    # Validate scraper name
    valid_scrapers = ["CanadaQuebec", "Malaysia", "Argentina"]
    if scraper_name not in valid_scrapers:
        print(f"Error: Invalid scraper name '{scraper_name}'")
        print(f"Valid scrapers: {', '.join(valid_scrapers)}")
        return 1

    print(f"Stopping {scraper_name} pipeline...")
    print("=" * 80)

    result = WorkflowRunner.stop_pipeline(scraper_name, repo_root)

    if result["status"] == "ok":
        print(f"\n[SUCCESS] {result['message']}")
        if "pid" in result:
            print(f"  Process ID: {result['pid']}")
        return 0
    else:
        print(f"\n[ERROR] {result['message']}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
