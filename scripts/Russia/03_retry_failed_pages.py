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
                WHERE status IN ('pending', 'retrying')
                ORDER BY page_number
            """)
            # Separate them by type if needed, but for now just get page numbers. 
            # Actually we need source_type to know WHICH script to run.
            
            cur.execute("""
                SELECT page_number, source_type
                FROM ru_failed_pages
                WHERE status IN ('pending', 'retrying')
                ORDER BY source_type, page_number
            """)
            failed_pages = [{"page": row[0], "type": row[1]} for row in cur.fetchall()]
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


def retry_page_with_scraper(page_num: int, source_type: str, fetch_ean: bool = True) -> bool:
    """Retry a single page using the correct scraper"""
    import subprocess
    
    script_name = "01_russia_farmcom_scraper.py" if source_type == "ved" else "02_russia_farmcom_excluded_scraper.py"
    scraper_script = _script_dir / script_name
    
    try:
        # Build command
        env = os.environ.copy()
        if fetch_ean and source_type == "ved":
            env["SCRIPT_01_FETCH_EAN"] = "true"
        
        cmd = [sys.executable, str(scraper_script), "--start", str(page_num), "--end", str(page_num)]
        
        print(f"  [RETRY] Type: {source_type.upper()} | Running: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd, 
            cwd=str(_repo_root),
            env=env,
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.returncode == 0:
            print(f"  [OK] Successfully retried page {page_num}")
            return True
        else:
            print(f"  [ERROR] Failed to retry page {page_num} (exit code {result.returncode})")
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
    
    # Get failed pages (list of dicts)
    failed_items, missing_ean_pages = get_failed_pages(repo)

    # Missing EANs are always type 'ved' currently
    all_items = []
    for item in failed_items:
        all_items.append(item)
    for p in missing_ean_pages:
        # Check if already in list
        if not any(x['page'] == p and x['type'] == 'ved' for x in all_items):
            all_items.append({'page': p, 'type': 'ved'})
            
    all_items.sort(key=lambda x: (x['type'], x['page']))

    if not all_items:
        # ... (unchanged success message) ...
        print("[SUCCESS] No pages need retry!")
        return 0

    print(f"[INFO] Pages needing retry: {len(all_items)}")
    
    # ... (skipping prompt logic) ...

    success_count = 0
    for i, item in enumerate(all_items, 1):
        page_num = item['page']
        source_type = item['type']
        
        print(f"\n[{i}/{len(all_items)}] Retrying {source_type} page {page_num}...")
        
        if retry_page_with_scraper(page_num, source_type, fetch_ean=True):
            success_count += 1
            try:
                # Resolve in DB
                if source_type in ('ved', 'excluded'):
                    repo.mark_failed_page_resolved(page_num, source_type)
                    print(f"  [DB] Marked {source_type} page {page_num} as resolved")
            except Exception as e:
                print(f"  [WARN] DB update failed: {e}")
        
        time.sleep(1)
    
    # Final summary
    print()
    print("="*80)
    print("Retry Summary")
    print("="*80)
    print(f"Pages retried successfully: {success_count}/{len(all_items)}")
    
    if success_count < len(all_items):
        print(f"Pages still failing: {len(all_items) - success_count}")
    
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
    
    # Return 0 if we successfully ran the retry logic for all pages
    # Even if some EANs are still missing, we've done our best.
    # Return 0 if we successfully ran the retry logic for all pages
    # Even if some EANs are still missing, we've done our best.
    return 0 if success_count == len(all_items) else 1


if __name__ == "__main__":
    sys.exit(main())
