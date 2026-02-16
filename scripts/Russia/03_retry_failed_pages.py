#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Russia VED Scraper - Retry Failed and Missing EAN Pages

This script retries pages that:
1. Failed during extraction
2. Have missing EAN numbers after extraction

This should be run BEFORE translation to ensure complete data.
"""

import os
import sys
import time
from pathlib import Path
from typing import List, Tuple

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
os.environ.setdefault('PYTHONUNBUFFERED', '1')

# Add repo root and script dir to path (script dir first to avoid loading another scraper's db)
_repo_root = Path(__file__).resolve().parents[2]
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Clear conflicting 'db' when run in same process as other scrapers (e.g. GUI)
for mod in list(sys.modules.keys()):
    if mod == "db" or mod.startswith("db."):
        del sys.modules[mod]

# Config loader
from config_loader import load_env_file, getenv_bool, get_output_dir
load_env_file()

# DB imports
from core.db.connection import CountryDB
from db.schema import apply_russia_schema
from db.repositories import RussiaRepository


def check_ean_coverage(repo: RussiaRepository) -> dict:
    """Check database for missing EAN coverage."""
    results = {
        "ved_total": 0,
        "ved_missing": 0,
        "excluded_total": 0,
        "excluded_missing": 0,
        "total_missing": 0
    }
    try:
        with repo.db.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ru_ved_products")
            results["ved_total"] = cur.fetchone()[0] or 0
            cur.execute("SELECT COUNT(*) FROM ru_ved_products WHERE ean IS NULL OR ean = ''")
            results["ved_missing"] = cur.fetchone()[0] or 0
            cur.execute("SELECT COUNT(*) FROM ru_excluded_products")
            results["excluded_total"] = cur.fetchone()[0] or 0
            cur.execute("SELECT COUNT(*) FROM ru_excluded_products WHERE ean IS NULL OR ean = ''")
            results["excluded_missing"] = cur.fetchone()[0] or 0
            results["total_missing"] = results["ved_missing"] + results["excluded_missing"]
    except Exception as e:
        print(f"[WARNING] Could not check EAN coverage: {e}")
    return results


def get_failed_pages(repo: RussiaRepository) -> Tuple[List[int], List[int]]:
    """Get failed pages and pages with missing EAN from database."""
    failed_pages = []
    missing_ean_pages = []
    try:
        with repo.db.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT page_number
                FROM ru_failed_pages
                WHERE source_type = 'ved' AND retry_count < 3
                ORDER BY page_number
            """)
            failed_pages = [row[0] for row in cur.fetchall()]
            cur.execute("""
                SELECT DISTINCT progress_key
                FROM ru_step_progress
                WHERE step_number = 1 AND status = 'ean_missing'
            """)
            for row in cur.fetchall():
                key = row[0]
                if key and key.startswith("ved_page:"):
                    try:
                        page_num = int(key.split(":")[1])
                        if page_num not in missing_ean_pages:
                            missing_ean_pages.append(page_num)
                    except (ValueError, IndexError):
                        pass
    except Exception as e:
        print(f"[WARNING] Could not get failed pages: {e}")
    return failed_pages, missing_ean_pages


def retry_page_with_scraper(page_num: int, scraper_script: Path, fetch_ean: bool = True) -> bool:
    """Retry a single page using the main scraper"""
    import subprocess
    
    try:
        # Build command - use environment variable to enable EAN fetching
        env = os.environ.copy()
        if fetch_ean:
            env["SCRIPT_01_FETCH_EAN"] = "true"
        
        cmd = [sys.executable, str(scraper_script), "--start", str(page_num), "--end", str(page_num)]
        
        print(f"  [RETRY] Running: {' '.join(cmd)}")
        
        # Call main scraper for this specific page
        result = subprocess.run(
            cmd, 
            cwd=str(_repo_root),
            env=env,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout per page
        )
        
        if result.returncode == 0:
            print(f"  [OK] Successfully retried page {page_num}")
            return True
        else:
            print(f"  [ERROR] Failed to retry page {page_num} (exit code {result.returncode})")
            if result.stderr:
                print(f"  [ERROR] stderr: {result.stderr[:500]}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"  [ERROR] Timeout retrying page {page_num}")
        return False
    except Exception as e:
        print(f"  [ERROR] Failed to retry page {page_num}: {e}")
        return False


def main():
    """Main retry function"""
    print()
    print("="*80)
    print("Russia VED Scraper - Retry Failed and Missing EAN Pages")
    print("="*80)
    print()
    
    # Resolve run_id (from env or .current_run_id written by pipeline)
    run_id = os.environ.get("RUSSIA_RUN_ID", "").strip()
    if not run_id:
        run_id_file = get_output_dir() / ".current_run_id"
        if run_id_file.exists():
            try:
                run_id = run_id_file.read_text(encoding="utf-8").strip()
            except Exception:
                pass
    if not run_id:
        print("[ERROR] No run_id. Set RUSSIA_RUN_ID or run pipeline from step 0.")
        return 1

    # Initialize database
    try:
        db = CountryDB("Russia")
        apply_russia_schema(db)
        repo = RussiaRepository(db, run_id)
    except Exception as e:
        print(f"[ERROR] Could not initialize database: {e}")
        return 1
    
    # Check EAN coverage
    print("[INFO] Checking EAN coverage in database...")
    ean_results = check_ean_coverage(repo)
    
    print()
    print("-"*80)
    print("EAN Coverage Summary")
    print("-"*80)
    ved_total = ean_results['ved_total']
    ved_missing = ean_results['ved_missing']
    excluded_total = ean_results['excluded_total']
    excluded_missing = ean_results['excluded_missing']
    
    if ved_total > 0:
        ved_pct = (ved_total - ved_missing) / ved_total * 100
        print(f"VED Registry:   {ved_missing:,} / {ved_total:,} rows missing EAN ({ved_pct:.1f}% coverage)")
    else:
        print(f"VED Registry:   No data")
        
    if excluded_total > 0:
        excluded_pct = (excluded_total - excluded_missing) / excluded_total * 100
        print(f"Excluded List:  {excluded_missing:,} / {excluded_total:,} rows missing EAN ({excluded_pct:.1f}% coverage)")
    else:
        print(f"Excluded List:  No data")
        
    print(f"Total Missing:  {ean_results['total_missing']:,} rows")
    print("-"*80)
    print()
    
    # Get failed pages
    failed_pages, missing_ean_pages = get_failed_pages(repo)
    
    # Combine unique pages
    all_pages = sorted(set(failed_pages + missing_ean_pages))
    
    if not all_pages:
        print("[SUCCESS] No pages need retry!")
        if ean_results['total_missing'] > 0:
            print(f"[WARNING] {ean_results['total_missing']} rows still missing EAN")
            print("[INFO] This may be due to data that genuinely doesn't have EAN codes")
            print("[INFO] Continuing with pipeline...")
        return 0
    
    print(f"[INFO] Pages needing retry:")
    print(f"  - Failed pages: {len(failed_pages)}")
    print(f"  - Pages with missing EAN: {len(missing_ean_pages)}")
    print(f"  - Total unique pages: {len(all_pages)}")
    print(f"  - Page numbers: {all_pages[:20]}{'...' if len(all_pages) > 20 else ''}")
    print()
    
    # Proceed with retry by default (no prompt, so pipeline does not block)
    if "--no" in sys.argv or "--skip-retry" in sys.argv:
        print("[SKIP] Retry skipped (--no or --skip-retry).")
        return 0
    print("[INFO] Proceeding with retry (default). Use --no or --skip-retry to skip.")
    print()
    print("="*80)
    print("Starting retry process...")
    print("="*80)
    print()
    
    # Retry pages
    scraper_script = _script_dir / "01_russia_farmcom_scraper.py"
    success_count = 0
    
    for i, page_num in enumerate(all_pages, 1):
        print(f"\n[{i}/{len(all_pages)}] Retrying page {page_num}...")
        if retry_page_with_scraper(page_num, scraper_script, fetch_ean=True):
            success_count += 1
        time.sleep(1)  # Brief delay between retries
    
    # Final summary
    print()
    print("="*80)
    print("Retry Summary")
    print("="*80)
    print(f"Pages retried successfully: {success_count}/{len(all_pages)}")
    
    if success_count < len(all_pages):
        print(f"Pages still failing: {len(all_pages) - success_count}")
    
    # Check final EAN coverage
    print()
    print("[INFO] Checking final EAN coverage...")
    final_ean = check_ean_coverage(repo)
    
    print()
    print("-"*80)
    print("Final EAN Coverage")
    print("-"*80)
    if final_ean['ved_total'] > 0:
        ved_pct = (final_ean['ved_total'] - final_ean['ved_missing']) / final_ean['ved_total'] * 100
        print(f"VED Registry:   {final_ean['ved_missing']:,} / {final_ean['ved_total']:,} rows missing EAN ({ved_pct:.1f}% coverage)")
    if final_ean['excluded_total'] > 0:
        excluded_pct = (final_ean['excluded_total'] - final_ean['excluded_missing']) / final_ean['excluded_total'] * 100
        print(f"Excluded List:  {final_ean['excluded_missing']:,} / {final_ean['excluded_total']:,} rows missing EAN ({excluded_pct:.1f}% coverage)")
    print("-"*80)
    
    # VALIDATION REPORT
    print()
    print("="*80)
    print("STEP 3 VALIDATION REPORT (After Retry)")
    print("="*80)
    print(f"VED Products in DB:      {final_ean['ved_total']:,}")
    print(f"Excluded Products in DB: {final_ean['excluded_total']:,}")
    print(f"TOTAL Records in DB:     {final_ean['ved_total'] + final_ean['excluded_total']:,}")
    
    if final_ean['total_missing'] == 0:
        print("\n[VALIDATION PASSED] 100% EAN coverage achieved!")
    else:
        print(f"\n[VALIDATION WARNING] {final_ean['total_missing']} rows still missing EAN")
    print("="*80)
    
    return 0 if (success_count == len(all_pages) and final_ean['total_missing'] == 0) else 1


if __name__ == "__main__":
    sys.exit(main())
