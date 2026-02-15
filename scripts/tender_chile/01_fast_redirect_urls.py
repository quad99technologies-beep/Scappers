#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 1: Get Redirect URLs — httpx (Fast)

Features:
  - Optional Tor SOCKS5 proxy (TOR_ENABLED=1) with NEWNYM every 12 min
  - Rate limiting: max 200 requests/min
  - Periodic DB saves (crash-safe)
  - Retry pass for failed tenders after main pass
  - All data inserted to PostgreSQL + CSV export

Requires: pip install httpx[socks]
Old Selenium version: archive/01_get_redirect_urls_selenium.py.bak
"""

from __future__ import annotations

import asyncio
import csv
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

import httpx

# ---- Path wiring ----
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

# ---- Config loader ----
try:
    from config_loader import load_env_file, getenv_int, getenv_bool, get_output_dir as _get_output_dir
    load_env_file()
    _CONFIG = True
except Exception:
    _CONFIG = False

# ---- Tor proxy ----
try:
    from core.network.tor_httpx import TorConfig, setup_tor, TorRotator, AsyncRateLimiter
    TOR_AVAILABLE = True
except ImportError:
    TOR_AVAILABLE = False

try:
    from config_loader import getenv as _cfg_getenv
    _TOR_GETENV = _cfg_getenv
except ImportError:
    _TOR_GETENV = None

# ---- Constants ----
MAX_TENDERS = getenv_int("MAX_TENDERS", 6000) if _CONFIG else int(os.getenv("MAX_TENDERS", "6000"))
MAX_WORKERS = getenv_int("SCRIPT_01_WORKERS", 10) if _CONFIG else int(os.getenv("SCRIPT_01_WORKERS", "10"))
MAX_REQ_PER_MIN = getenv_int("MAX_REQ_PER_MIN", 200) if _CONFIG else int(os.getenv("MAX_REQ_PER_MIN", "200"))
OUTPUT_FILENAME = "tender_redirect_urls.csv"
REQUIRED_OUTPUT_COLUMNS = ["original_url", "redirect_url", "qs_parameter", "tender_details_url", "tender_award_url"]


def get_output_dir() -> Path:
    if _CONFIG:
        return _get_output_dir()
    return _repo_root / "output" / "Tender_Chile"


def extract_qs_from_url(url: str) -> Optional[str]:
    m = re.search(r"[?&]qs=([^&]+)", url)
    return m.group(1) if m else None


async def process_tenders(
    rows: List[Dict], run_id: str, proxy_url: str = None, tor_cfg=None,
) -> List[Dict]:
    """Process all tenders concurrently with rate limiting and retry."""
    total = min(len(rows), MAX_TENDERS)
    out_rows: List[Dict] = []
    failed_items: List[Dict] = []
    start_time = time.time()

    # Rate limiter
    if TOR_AVAILABLE:
        rate_limiter = AsyncRateLimiter(max_per_minute=MAX_REQ_PER_MIN)
    else:
        class _SimpleLimiter:
            def __init__(self):
                self._interval = 60.0 / MAX_REQ_PER_MIN
                self._lock = asyncio.Lock()
                self._last = 0.0
                self.total_requests = 0
            async def acquire(self):
                async with self._lock:
                    now = time.monotonic()
                    if now - self._last < self._interval:
                        await asyncio.sleep(self._interval - (now - self._last))
                    self._last = time.monotonic()
                    self.total_requests += 1
        rate_limiter = _SimpleLimiter()

    # Tor NEWNYM rotation
    rotator = None
    if tor_cfg and TOR_AVAILABLE:
        rotator = TorRotator(tor_cfg)
        rotator.start()

    semaphore = asyncio.Semaphore(MAX_WORKERS)

    async def _run_pass(items: List[Dict], pass_name: str) -> tuple[List[Dict], List[Dict]]:
        passed: List[Dict] = []
        local_failed: List[Dict] = []
        fail_count = 0
        completed_count = 0
        completed_lock = asyncio.Lock()

        async with httpx.AsyncClient(
            headers={
                "Accept": "text/html,application/xhtml+xml",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/131.0 Safari/537.36",
            },
            timeout=60.0,
            follow_redirects=True,
            limits=httpx.Limits(max_connections=MAX_WORKERS + 5, max_keepalive_connections=MAX_WORKERS),
            proxy=proxy_url,
        ) as client:

            async def process_one(i: int, row: Dict) -> Optional[Dict]:
                nonlocal fail_count, completed_count
                tender_id = (row.get('tender_id') or '').strip()
                url = (row.get('url') or '').strip()

                if not url and tender_id:
                    start_url = (
                        "https://www.mercadopublico.cl/Procurement/Modules/RFB/"
                        f"DetailsAcquisition.aspx?idLicitacion={tender_id}"
                    )
                elif url:
                    start_url = url
                else:
                    return None

                async with semaphore:
                    await rate_limiter.acquire()
                    for attempt in range(2):
                        try:
                            resp = await client.get(start_url)
                            final_url = str(resp.url)
                            qs = extract_qs_from_url(final_url) or ""

                            if not qs:
                                details_url = start_url
                                award_url = start_url.replace("DetailsAcquisition.aspx", "StepsProcessAward/PreviewAwardAct.aspx")
                            else:
                                details_url = final_url
                                award_url = final_url.replace("DetailsAcquisition.aspx", "StepsProcessAward/PreviewAwardAct.aspx")

                            result = {
                                "tender_id": tender_id,
                                "original_url": start_url,
                                "redirect_url": final_url,
                                "qs_parameter": qs,
                                "tender_details_url": details_url,
                                "tender_award_url": award_url,
                            }

                            async with completed_lock:
                                completed_count += 1
                                cc = completed_count
                            if cc % 50 == 0 or cc == len(items):
                                elapsed = time.time() - start_time
                                rate = cc / elapsed if elapsed > 0 else 0
                                eta_min = (len(items) - cc) / rate / 60 if rate > 0 else 0
                                print(f"[{pass_name}] {cc}/{len(items)} | {rate:.1f}/s | ETA: {eta_min:.1f}min")
                            return result

                        except Exception as e:
                            if attempt == 0:
                                await asyncio.sleep(2)
                            else:
                                fail_count += 1
                                if fail_count <= 20:
                                    print(f"   [{pass_name} ERROR] {tender_id}: {type(e).__name__}: {str(e)[:100]}")
                                local_failed.append(row)
                                return None

            tasks = [process_one(i, row) for i, row in enumerate(items[:total], 1)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, dict):
                    passed.append(r)

        return passed, local_failed

    # ---- Main pass ----
    print(f"[MAIN] Processing {total} tenders...")
    main_results, failed_items = await _run_pass(rows[:total], "MAIN")
    out_rows.extend(main_results)

    # ---- Retry pass ----
    if failed_items:
        print(f"\n[RETRY] Retrying {len(failed_items)} failed tenders...")
        retry_results, still_failed = await _run_pass(failed_items, "RETRY")
        out_rows.extend(retry_results)
        if still_failed:
            print(f"[RETRY] Still failed: {len(still_failed)}")

    # Stop rotator
    if rotator:
        rotator.stop()
        if rotator.rotation_count > 0:
            print(f"[TOR] Completed {rotator.rotation_count} NEWNYM rotations")

    elapsed = time.time() - start_time
    print(f"\n[TIMING] Processed {len(out_rows)}/{total} tenders in {elapsed:.1f}s ({len(out_rows)/max(elapsed,1):.1f}/s)")

    return out_rows


async def async_main():
    output_dir = get_output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)

    run_id = os.getenv("TENDER_CHILE_RUN_ID", "")
    if not run_id:
        print("[ERROR] TENDER_CHILE_RUN_ID environment variable not set")
        sys.exit(1)

    # ---- Tor proxy setup ----
    proxy_url = None
    tor_cfg = None
    if TOR_AVAILABLE:
        tor_cfg = TorConfig.from_env(getenv_fn=_TOR_GETENV)
        proxy_url = setup_tor(tor_cfg)
    else:
        print("[TOR] core.tor_httpx not available — running without Tor")

    # Read input from PostgreSQL
    try:
        from core.db.connection import CountryDB
        db = CountryDB("Tender_Chile")
        db.connect()

        with db.cursor(dict_cursor=True) as cur:
            cur.execute("SELECT tender_id, description, url FROM tc_input_tender_list ORDER BY id")
            rows = cur.fetchall()

        print(f"Reading: PostgreSQL table 'tc_input_tender_list'")
        print(f"   Found {len(rows)} tender(s)")
        db.close()
    except Exception as e:
        print(f"[ERROR] Failed to read from PostgreSQL: {e}")
        sys.exit(1)

    if not rows:
        print("[ERROR] Input table has no rows")
        sys.exit(1)

    # Process tenders
    print(f"\n[SCRAPER] Processing {min(len(rows), MAX_TENDERS)} tenders with {MAX_WORKERS} workers")
    if proxy_url:
        print(f"[SCRAPER] Using Tor proxy: {proxy_url}")
    print(f"[SCRAPER] Rate limit: {MAX_REQ_PER_MIN} req/min")

    out_rows = await process_tenders(rows, run_id, proxy_url=proxy_url, tor_cfg=tor_cfg)

    if not out_rows:
        print("[ERROR] No tender URLs processed")
        sys.exit(1)

    # Stats
    with_qs = sum(1 for r in out_rows if r.get("qs_parameter"))
    without_qs = len(out_rows) - with_qs
    print(f"\n[STATS] With qs=: {with_qs} | Without: {without_qs}")
    if without_qs > len(out_rows) * 0.5:
        print("[WARN] >50% missing qs parameter — site may need JS redirects")
        print("[WARN] Consider Selenium fallback: archive/01_get_redirect_urls_selenium.py.bak")

    # Save to PostgreSQL
    try:
        from core.db.connection import CountryDB
        from db.repositories import ChileRepository

        db = CountryDB("Tender_Chile")
        db.connect()
        repo = ChileRepository(db, run_id)
        repo.ensure_run_in_ledger(mode="resume")

        redirects_data = [{
            'tender_id': r.get('tender_id', ''),
            'redirect_url': r.get('redirect_url', ''),
            'source_url': r.get('original_url', ''),
        } for r in out_rows]

        count = repo.insert_tender_redirects_bulk(redirects_data)
        db.commit()
        verify_count = repo.get_tender_redirects_count()
        print(f"[DB] Saved {count} redirect URLs (verified: {verify_count})")

        if verify_count == 0:
            raise RuntimeError("Database insert verification failed")
        db.close()
    except Exception as e:
        import traceback
        print(f"[ERROR] Could not save to PostgreSQL: {e}")
        traceback.print_exc()
        raise

    # CSV export
    out_path = output_dir / OUTPUT_FILENAME
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=REQUIRED_OUTPUT_COLUMNS)
        w.writeheader()
        w.writerows([{k: r.get(k, "") for k in REQUIRED_OUTPUT_COLUMNS} for r in out_rows])

    print(f"[OK] {len(out_rows)} tender URLs processed")
    print(f"[OK] CSV: {out_path}")


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
