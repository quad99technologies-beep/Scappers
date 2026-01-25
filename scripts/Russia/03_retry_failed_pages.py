#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Russia VED Scraper - Retry Failed and Missing EAN Pages

This script retries pages that:
1. Failed during extraction
2. Have missing EAN numbers after retry

This should be run BEFORE translation to ensure complete data.
"""

import sys
import json
from pathlib import Path
import argparse
import pandas as pd

# Add repo root to path
_file = Path(__file__).resolve()
_repo_root = _file.parents[2]
sys.path.insert(0, str(_repo_root))

def get_output_dir():
    """Get output directory for Russia"""
    return _repo_root / "output" / "Russia"

def check_missing_ean_in_csv():
    """Check CSV files for missing EAN data"""
    output_dir = get_output_dir()
    ved_csv = output_dir / "russia_farmcom_ved_moscow_region.csv"
    excluded_csv = output_dir / "russia_farmcom_excluded_list.csv"

    results = {
        "ved_total": 0,
        "ved_missing": 0,
        "excluded_total": 0,
        "excluded_missing": 0,
        "total_missing": 0
    }

    # Check VED file
    if ved_csv.exists():
        try:
            df = pd.read_csv(ved_csv)
            results["ved_total"] = len(df)
            if 'EAN' in df.columns:
                results["ved_missing"] = df['EAN'].isna().sum() + (df['EAN'] == '').sum()
        except Exception as e:
            print(f"[WARNING] Could not check VED CSV: {e}")

    # Check Excluded file
    if excluded_csv.exists():
        try:
            df = pd.read_csv(excluded_csv)
            results["excluded_total"] = len(df)
            if 'EAN' in df.columns:
                results["excluded_missing"] = df['EAN'].isna().sum() + (df['EAN'] == '').sum()
        except Exception as e:
            print(f"[WARNING] Could not check Excluded CSV: {e}")

    results["total_missing"] = results["ved_missing"] + results["excluded_missing"]
    return results

def load_progress():
    """Load the scraper progress file"""
    progress_file = get_output_dir() / "russia_scraper_progress.json"
    if not progress_file.exists():
        print(f"[WARNING] Progress file not found: {progress_file}")
        print("[INFO] Progress file may have been cleaned up after scraper completion")
        print()

        # Check if CSV output exists
        csv_file = get_output_dir() / "russia_farmcom_ved_moscow_region.csv"
        if csv_file.exists():
            print(f"[INFO] Checking CSV files for missing EAN data...")
            ean_results = check_missing_ean_in_csv()

            print()
            print("="*80)
            print("Missing EAN Summary")
            print("="*80)
            print(f"VED Registry:   {ean_results['ved_missing']:,} / {ean_results['ved_total']:,} rows missing EAN ({ean_results['ved_missing']/ean_results['ved_total']*100:.2f}%)" if ean_results['ved_total'] > 0 else "VED Registry: No data")
            print(f"Excluded List:  {ean_results['excluded_missing']:,} / {ean_results['excluded_total']:,} rows missing EAN ({ean_results['excluded_missing']/ean_results['excluded_total']*100:.2f}%)" if ean_results['excluded_total'] > 0 else "Excluded List: No data")
            print(f"Total Missing:  {ean_results['total_missing']:,} rows")
            print("="*80)
            print()

            if ean_results['total_missing'] > 0:
                missing_pct = ean_results['total_missing'] / (ean_results['ved_total'] + ean_results['excluded_total']) * 100
                print(f"[ERROR] Found {ean_results['total_missing']} rows with missing EAN ({missing_pct:.2f}%)")
                print("[ERROR] 100% EAN coverage is MANDATORY for this pipeline")
                print("[ERROR] Without progress file, cannot identify which pages to retry")
                print()
                print("=" * 80)
                print("SOLUTION: Re-run Step 01 to regenerate progress tracking")
                print("=" * 80)
                print("1. Stop the pipeline")
                print("2. Run: python scripts/Russia/01_russia_farmcom_scraper.py --fresh")
                print("3. Wait for scraping to complete (will track pages with missing EAN)")
                print("4. Run this retry step again to fix missing EAN pages")
                print("5. Then proceed to translation")
                print("=" * 80)
                print()
                return {"retry_required": True, "missing_ean_count": ean_results['total_missing']}
            else:
                print("[SUCCESS] No missing EAN data found!")
                return {"no_retry_needed": True}
        else:
            print(f"[ERROR] VED CSV file not found: {csv_file}")
            print("[ERROR] Run step 01 (Extract VED Registry) first")
            return None

    with open(progress_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def get_pages_to_retry(progress_data):
    """Get list of pages that need retry"""
    failed_pages = set(progress_data.get('failed_pages', []))
    missing_ean_pages = set(progress_data.get('pages_with_missing_ean', []))

    all_pages = sorted(failed_pages | missing_ean_pages)
    return all_pages, len(failed_pages), len(missing_ean_pages)

def retry_pages(pages_to_retry, headless=None):
    """Retry specific pages using the main scraper"""
    import subprocess

    if not pages_to_retry:
        print("[INFO] No pages to retry!")
        return True

    print(f"\n[RETRY] Found {len(pages_to_retry)} pages to retry")
    print(f"[RETRY] Pages: {pages_to_retry}")
    print()

    # Path to the main scraper script
    scraper_script = _file.parent / "01_russia_farmcom_scraper.py"

    # Retry each page individually
    success_count = 0
    for i, page_num in enumerate(pages_to_retry, 1):
        print(f"\n{'='*80}")
        print(f"[RETRY] Processing page {page_num} ({i}/{len(pages_to_retry)})")
        print(f"{'='*80}\n")

        try:
            # Build command
            cmd = [sys.executable, str(scraper_script), "--start", str(page_num), "--end", str(page_num)]
            if headless is False:
                cmd.append("--visible")

            # Call main scraper for this specific page
            result = subprocess.run(cmd, check=True, cwd=str(_repo_root))

            if result.returncode == 0:
                success_count += 1
                print(f"\n[OK] Successfully retried page {page_num}")
            else:
                print(f"\n[ERROR] Failed to retry page {page_num} (exit code {result.returncode})")
        except subprocess.CalledProcessError as e:
            print(f"\n[ERROR] Failed to retry page {page_num}: {e}")
        except Exception as e:
            print(f"\n[ERROR] Failed to retry page {page_num}: {e}")

    print(f"\n{'='*80}")
    print(f"[RETRY] Retry Summary")
    print(f"{'='*80}")
    print(f"Total pages retried: {success_count}/{len(pages_to_retry)}")

    if success_count == len(pages_to_retry):
        print("[SUCCESS] All pages retried successfully!")
        return True
    else:
        print(f"[WARNING] {len(pages_to_retry) - success_count} pages still have issues")
        return False

def main(headless=None, skip_check=False):
    """
    Main retry function

    Args:
        headless: Run Chrome in headless mode (None = use config)
        skip_check: Skip the check and retry all pages in list
    """
    print()
    print("="*80)
    print("Russia VED Scraper - Retry Failed Pages")
    print("="*80)
    print()

    # Load progress
    progress = load_progress()
    if not progress:
        return 1

    # Check if retry is needed
    if progress.get("no_retry_needed"):
        print("[SUCCESS] No retry needed - scraper completed successfully")
        print("[INFO] Proceeding to next pipeline step")
        return 0

    # Check if retry is required due to missing EAN
    if progress.get("retry_required"):
        missing_count = progress.get("missing_ean_count", 0)
        print(f"[ERROR] Cannot proceed with {missing_count} missing EAN rows")
        print("[ERROR] Pipeline MUST stop - retry is required")
        print()
        print("[ACTION REQUIRED] Run the following command to fix:")
        print("  python scripts/Russia/01_russia_farmcom_scraper.py --fresh")
        print()
        return 1

    # Get pages to retry
    pages_to_retry, failed_count, missing_ean_count = get_pages_to_retry(progress)

    if not pages_to_retry:
        print("[SUCCESS] No pages need retry!")
        print("[INFO] All pages extracted successfully")
        return 0

    print(f"[INFO] Pages needing retry:")
    print(f"  - Failed pages: {failed_count}")
    print(f"  - Pages with missing EAN: {missing_ean_count}")
    print(f"  - Total pages to retry: {len(pages_to_retry)}")

    if not skip_check:
        response = input("\n[PROMPT] Retry these pages? (y/n): ")
        if response.lower() != 'y':
            print("[CANCELLED] Retry cancelled by user")
            return 1

    # Retry pages
    success = retry_pages(pages_to_retry, headless=headless)

    # Check final status
    print()
    print("="*80)
    print("Checking final status...")
    print("="*80)

    final_progress = load_progress()
    if final_progress:
        final_pages, final_failed, final_missing = get_pages_to_retry(final_progress)
        print(f"[INFO] Remaining issues:")
        print(f"  - Failed pages: {final_failed}")
        print(f"  - Pages with missing EAN: {final_missing}")

        if len(final_pages) == 0:
            print("\n[SUCCESS] All pages now complete! Ready for translation.")
            return 0
        else:
            print(f"\n[WARNING] {len(final_pages)} pages still have issues")
            print(f"[WARNING] Pages: {final_pages}")
            print("[INFO] You may need to run this retry script again")
            return 1

    return 0 if success else 1

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Russia VED Scraper - Retry Failed Pages")
    parser.add_argument("--visible", action="store_true", help="Show browser window (not headless)")
    parser.add_argument("--skip-check", action="store_true", help="Skip confirmation prompt")

    args = parser.parse_args()

    headless = not args.visible if args.visible else None
    exit_code = main(headless=headless, skip_check=args.skip_check)
    sys.exit(exit_code)
