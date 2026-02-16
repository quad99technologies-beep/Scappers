#!/usr/bin/env python3
"""
Step 2 OPTIMIZED: Extract Tender Details using HTTPX (Ultra Fast)
===============================================================
- 50 concurrent workers (up from 10)
- 5000 req/min rate limit (up from 3000)
- HTTP/2 support
- Optimized connection pooling
- Larger DB batches (100 up from 50)
- Smart retry with exponential backoff
"""

from __future__ import annotations

import asyncio
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import random

import httpx
import pandas as pd
from bs4 import BeautifulSoup

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

try:
    from config_loader import load_env_file, getenv_int
    load_env_file()
except Exception:
    pass

# Output paths
OUTPUT_DIR = Path(_repo_root) / "output" / "Tender_Chile"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "tender_details.csv"

# OPTIMIZED Config
MAX_TENDERS = getenv_int("MAX_TENDERS", 6000)
MAX_WORKERS = 50                    # Increased from 10
MAX_REQ_PER_MIN = 5000              # Increased from 3000
BATCH_SIZE = 100                    # Increased from 50
MAX_RETRIES = 3                     # Retry failed requests
RETRY_DELAY_BASE = 2.0              # Base delay for exponential backoff

# Connection pool limits
MAX_CONNECTIONS = 100
MAX_KEEPALIVE = 50

# Request timeout
TIMEOUT = httpx.Timeout(30.0, connect=10.0)


def clean(s):
    return re.sub(r"\s+", " ", str(s or "")).strip()


def extract_tender_data(html: str, url: str) -> Dict:
    """Extract tender data from HTML using BeautifulSoup."""
    soup = BeautifulSoup(html, "html.parser")
    data = {
        "Tender ID": "",
        "Tender Title": "",
        "TENDERING AUTHORITY": "",
        "PROVINCE": "",
        "Closing Date": "",
        "Price Evaluation ratio": "",
        "Quality Evaluation ratio": "",
        "Other Evaluation ratio": "",
    }
    
    # Tender ID
    tid_elem = soup.find("span", {"id": "lblNumLicitacion"})
    if tid_elem:
        data["Tender ID"] = clean(tid_elem.get_text())
    
    # Tender Title
    title_elem = soup.find("span", {"id": "lblFicha1Nombre"})
    if title_elem:
        data["Tender Title"] = clean(title_elem.get_text())
    
    # Authority
    auth_elem = soup.find("span", {"id": "lblFicha1Organismo"})
    if auth_elem:
        data["TENDERING AUTHORITY"] = clean(auth_elem.get_text())
    
    # Province/Region
    prov_elem = soup.find("span", {"id": "lblFicha1Region"})
    if prov_elem:
        data["PROVINCE"] = clean(prov_elem.get_text())
    
    # Closing Date
    closing_elem = soup.find("span", {"id": "lblFechaCierre"})
    if closing_elem:
        data["Closing Date"] = clean(closing_elem.get_text())
    
    # Evaluation ratios
    eval_table = soup.find("table", {"id": "tblEvaluacion"})
    if eval_table:
        rows = eval_table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) >= 2:
                label = clean(cells[0].get_text()).lower()
                value = clean(cells[1].get_text())
                if "precio" in label or "price" in label:
                    data["Price Evaluation ratio"] = value
                elif "calidad" in label or "quality" in label:
                    data["Quality Evaluation ratio"] = value
                elif "otro" in label or "other" in label:
                    data["Other Evaluation ratio"] = value
    
    return data


class AsyncRateLimiter:
    """Token bucket rate limiter."""
    def __init__(self, max_per_minute: int):
        self._interval = 60.0 / max_per_minute
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


async def fetch_with_retry(
    client: httpx.AsyncClient, 
    url: str, 
    rate_limiter: AsyncRateLimiter,
    max_retries: int = MAX_RETRIES
) -> Optional[httpx.Response]:
    """Fetch URL with exponential backoff retry."""
    for attempt in range(max_retries):
        await rate_limiter.acquire()
        try:
            resp = await client.get(url, timeout=TIMEOUT)
            if resp.status_code == 200:
                return resp
            elif resp.status_code in (429, 503, 502, 504):  # Rate limit or server error
                wait_time = RETRY_DELAY_BASE * (2 ** attempt) + random.uniform(0, 1)
                print(f"[RETRY] Status {resp.status_code}, waiting {wait_time:.1f}s (attempt {attempt + 1})")
                await asyncio.sleep(wait_time)
            else:
                return resp
        except (httpx.TimeoutException, httpx.ConnectError, httpx.NetworkError) as e:
            if attempt < max_retries - 1:
                wait_time = RETRY_DELAY_BASE * (2 ** attempt) + random.uniform(0, 1)
                print(f"[RETRY] {type(e).__name__}, waiting {wait_time:.1f}s (attempt {attempt + 1})")
                await asyncio.sleep(wait_time)
            else:
                raise
    return None


async def process_tenders(run_id: str = None) -> List[Dict]:
    """Process all tenders with optimized concurrency."""
    from core.db.connection import CountryDB
    from db.repositories import ChileRepository
    
    db = CountryDB("Tender_Chile")
    db.connect()
    repo = ChileRepository(db, run_id)
    
    # Read from PostgreSQL
    print("[DB] Reading tender redirects from PostgreSQL...")
    with db.cursor(dict_cursor=True) as cur:
        cur.execute("""
            SELECT tender_id, redirect_url, qs_parameter 
            FROM tc_tender_redirects 
            ORDER BY id
        """)
        rows = cur.fetchall()
    
    total = min(len(rows), MAX_TENDERS)
    rows = rows[:total]
    print(f"[DB] Found {total} tenders to process")
    
    out_rows: List[Dict] = []
    failed_items: List[Dict] = []
    start_time = time.time()
    
    rate_limiter = AsyncRateLimiter(max_per_minute=MAX_REQ_PER_MIN)
    semaphore = asyncio.Semaphore(MAX_WORKERS)
    
    # Statistics
    processed = 0
    success_count = 0
    fail_count = 0
    stats_lock = asyncio.Lock()
    
    # Headers for requests
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-CL,es;q=0.9,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.0.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.0",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    
    # Connection limits
    limits = httpx.Limits(
        max_connections=MAX_CONNECTIONS,
        max_keepalive_connections=MAX_KEEPALIVE
    )
    
    async with httpx.AsyncClient(
        headers=headers,
        timeout=TIMEOUT,
        limits=limits,
        http2=True,  # Enable HTTP/2
    ) as client:
        
        async def process_one(row: Dict) -> Optional[Dict]:
            nonlocal processed, success_count, fail_count
            
            tender_id = row.get('tender_id', '')
            redirect_url = row.get('redirect_url', '')
            
            if not redirect_url:
                return None
            
            async with semaphore:
                try:
                    resp = await fetch_with_retry(client, redirect_url, rate_limiter)
                    
                    if resp and resp.status_code == 200:
                        data = extract_tender_data(resp.text, redirect_url)
                        data["tender_id"] = tender_id
                        data["source_url"] = redirect_url
                        
                        async with stats_lock:
                            success_count += 1
                        return data
                    else:
                        async with stats_lock:
                            fail_count += 1
                        return None
                        
                except Exception as e:
                    async with stats_lock:
                        fail_count += 1
                    return None
                finally:
                    async with stats_lock:
                        processed += 1
                        p = processed
                    
                    # Progress report every 100 items
                    if p % 100 == 0 or p == total:
                        elapsed = time.time() - start_time
                        rate = p / elapsed if elapsed > 0 else 0
                        eta_min = (total - p) / rate / 60 if rate > 0 else 0
                        print(f"[{p}/{total}] {rate:.1f}/s | ETA: {eta_min:.1f}min | OK:{success_count} Fail:{fail_count}")
        
        # Process all tenders
        print(f"[START] Processing {total} tenders with {MAX_WORKERS} workers...")
        tasks = [process_one(row) for row in rows]
        results = await asyncio.gather(*tasks)
        
        out_rows = [r for r in results if r is not None]
    
    # Save to database in batches
    print(f"\n[DB] Saving {len(out_rows)} records to PostgreSQL...")
    batch = []
    for i, row in enumerate(out_rows):
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            repo.insert_tender_details_bulk(batch)
            db.commit()
            print(f"[DB] Saved batch {i//BATCH_SIZE + 1}/{(len(out_rows)//BATCH_SIZE)+1}")
            batch = []
    
    if batch:
        repo.insert_tender_details_bulk(batch)
        db.commit()
    
    # Save to CSV
    df = pd.DataFrame(out_rows)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"[CSV] Saved to {OUTPUT_FILE}")
    
    # Final stats
    elapsed = time.time() - start_time
    rate = total / elapsed if elapsed > 0 else 0
    print(f"\n[DONE] Processed {total} tenders in {elapsed:.1f}s ({rate:.1f}/s)")
    print(f"[STATS] Success: {success_count}, Failed: {fail_count}")
    
    db.close()
    return out_rows


def main():
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    results = asyncio.run(process_tenders(run_id))
    print(f"\nExtracted {len(results)} tender details")


if __name__ == "__main__":
    main()
