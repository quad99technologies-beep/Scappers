#!/usr/bin/env python3
"""
Step 3 OPTIMIZED: Extract Tender Awards — httpx + BeautifulSoup (Ultra Fast)
============================================================================
- 50 concurrent workers (up from 8)
- 5000 req/min rate limit (up from 200)
- HTTP/2 support
- Optimized connection pooling
- Larger DB batches (100 up from 50)
- Smart retry with exponential backoff
"""

from __future__ import annotations

import asyncio
import csv
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import random

import httpx
from bs4 import BeautifulSoup

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

try:
    from config_loader import load_env_file, getenv_int, get_output_dir as _get_output_dir
    load_env_file()
    _CONFIG_LOADER_AVAILABLE = True
except ImportError:
    _CONFIG_LOADER_AVAILABLE = False

# Output paths
OUTPUT_DIR = _get_output_dir() if _CONFIG_LOADER_AVAILABLE else (_repo_root / "output" / "Tender_Chile")
SUPPLIER_OUTPUT_FILENAME = "mercadopublico_supplier_rows.csv"
LOT_SUMMARY_OUTPUT_FILENAME = "mercadopublico_lot_summary.csv"

# OPTIMIZED Config
MAX_WORKERS = 50                    # Increased from 8
MAX_REQ_PER_MIN = 5000              # Increased from 200
BATCH_SIZE = 100                    # Increased from 50
MAX_RETRIES = 3                     # Retry failed requests
RETRY_DELAY_BASE = 2.0              # Base delay for exponential backoff

# Connection pool limits
MAX_CONNECTIONS = 100
MAX_KEEPALIVE = 50

# Request timeout
TIMEOUT = httpx.Timeout(30.0, connect=10.0)


def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip()


def parse_money(value: str) -> str:
    """Parse monetary value, return cleaned string."""
    if not value:
        return ""
    cleaned = re.sub(r"[^\d,\.\-]", "", value)
    return cleaned


def extract_award_data(html: str, tender_id: str, url: str) -> List[Dict]:
    """Extract award data from HTML. Returns list of supplier rows."""
    soup = BeautifulSoup(html, "html.parser")
    results = []
    
    # Find award sections
    award_sections = soup.find_all("div", class_=re.compile(r"award|adjudicacion|supplier|proveedor", re.I))
    
    if not award_sections:
        # Try alternative selectors
        award_sections = soup.find_all("table", class_=re.compile(r"award|adjudicacion", re.I))
    
    for section in award_sections:
        supplier_name = ""
        supplier_rut = ""
        award_amount = ""
        lot_number = ""
        
        # Supplier name
        name_elem = section.find(["span", "td", "div"], class_=re.compile(r"supplier|proveedor|razon", re.I))
        if name_elem:
            supplier_name = clean_text(name_elem.get_text())
        
        # RUT (tax ID)
        rut_elem = section.find(["span", "td", "div"], class_=re.compile(r"rut|tax", re.I))
        if rut_elem:
            supplier_rut = clean_text(rut_elem.get_text())
        
        # Award amount
        amount_elem = section.find(["span", "td", "div"], class_=re.compile(r"amount|monto|valor", re.I))
        if amount_elem:
            award_amount = parse_money(amount_elem.get_text())
        
        # Lot number
        lot_elem = section.find(["span", "td", "div"], class_=re.compile(r"lot|lote", re.I))
        if lot_elem:
            lot_number = clean_text(lot_elem.get_text())
        
        if supplier_name or supplier_rut:
            results.append({
                "tender_id": tender_id,
                "supplier_name": supplier_name,
                "supplier_rut": supplier_rut,
                "award_amount": award_amount,
                "lot_number": lot_number,
                "source_url": url,
            })
    
    return results


def extract_lot_summary(html: str, tender_id: str) -> List[Dict]:
    """Extract lot summary data from HTML."""
    soup = BeautifulSoup(html, "html.parser")
    results = []
    
    # Find lot tables
    lot_tables = soup.find_all("table", class_=re.compile(r"lot|lote", re.I))
    
    for table in lot_tables:
        rows = table.find_all("tr")
        for row in rows[1:]:  # Skip header
            cells = row.find_all(["td", "th"])
            if len(cells) >= 3:
                lot_num = clean_text(cells[0].get_text())
                lot_desc = clean_text(cells[1].get_text())
                lot_qty = clean_text(cells[2].get_text())
                
                results.append({
                    "tender_id": tender_id,
                    "lot_number": lot_num,
                    "lot_description": lot_desc,
                    "lot_quantity": lot_qty,
                })
    
    return results


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
            elif resp.status_code in (429, 503, 502, 504):
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


async def process_awards(run_id: str = None) -> Tuple[List[Dict], List[Dict]]:
    """Process all tender awards with optimized concurrency."""
    from core.db.connection import CountryDB
    from db.repositories import ChileRepository
    
    db = CountryDB("Tender_Chile")
    db.connect()
    repo = ChileRepository(db, run_id)
    
    # Read from PostgreSQL
    print("[DB] Reading tender redirects from PostgreSQL...")
    with db.cursor(dict_cursor=True) as cur:
        cur.execute("""
            SELECT tender_id, tender_award_url 
            FROM tc_tender_redirects 
            WHERE tender_award_url IS NOT NULL AND tender_award_url != ''
            ORDER BY id
        """)
        rows = cur.fetchall()
    
    total = len(rows)
    print(f"[DB] Found {total} tenders with award URLs")
    
    supplier_rows: List[Dict] = []
    lot_rows: List[Dict] = []
    failed_items: List[Dict] = []
    start_time = time.time()
    
    rate_limiter = AsyncRateLimiter(max_per_minute=MAX_REQ_PER_MIN)
    semaphore = asyncio.Semaphore(MAX_WORKERS)
    
    # Statistics
    processed = 0
    success_count = 0
    fail_count = 0
    stats_lock = asyncio.Lock()
    
    # Headers
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-CL,es;q=0.9,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.0.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.0",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    
    limits = httpx.Limits(
        max_connections=MAX_CONNECTIONS,
        max_keepalive_connections=MAX_KEEPALIVE
    )
    
    async with httpx.AsyncClient(
        headers=headers,
        timeout=TIMEOUT,
        limits=limits,
        http2=True,
    ) as client:
        
        async def process_one(row: Dict) -> bool:
            nonlocal processed, success_count, fail_count
            
            tender_id = row.get('tender_id', '')
            award_url = row.get('tender_award_url', '')
            
            if not award_url:
                return False
            
            async with semaphore:
                try:
                    resp = await fetch_with_retry(client, award_url, rate_limiter)
                    
                    if resp and resp.status_code == 200:
                        # Extract supplier data
                        suppliers = extract_award_data(resp.text, tender_id, award_url)
                        if suppliers:
                            async with stats_lock:
                                supplier_rows.extend(suppliers)
                        
                        # Extract lot data
                        lots = extract_lot_summary(resp.text, tender_id)
                        if lots:
                            async with stats_lock:
                                lot_rows.extend(lots)
                        
                        async with stats_lock:
                            success_count += 1
                        return True
                    else:
                        async with stats_lock:
                            fail_count += 1
                        return False
                        
                except Exception as e:
                    async with stats_lock:
                        fail_count += 1
                    return False
                finally:
                    async with stats_lock:
                        processed += 1
                        p = processed
                    
                    if p % 100 == 0 or p == total:
                        elapsed = time.time() - start_time
                        rate = p / elapsed if elapsed > 0 else 0
                        eta_min = (total - p) / rate / 60 if rate > 0 else 0
                        print(f"[{p}/{total}] {rate:.1f}/s | ETA: {eta_min:.1f}min | OK:{success_count} Fail:{fail_count}")
        
        # Process all
        print(f"[START] Processing {total} award pages with {MAX_WORKERS} workers...")
        tasks = [process_one(row) for row in rows]
        await asyncio.gather(*tasks)
    
    # Save suppliers to DB
    print(f"\n[DB] Saving {len(supplier_rows)} supplier records...")
    batch = []
    for i, row in enumerate(supplier_rows):
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            repo.insert_supplier_rows_bulk(batch)
            db.commit()
            print(f"[DB] Saved supplier batch {i//BATCH_SIZE + 1}")
            batch = []
    if batch:
        repo.insert_supplier_rows_bulk(batch)
        db.commit()
    
    # Save lots to DB
    print(f"[DB] Saving {len(lot_rows)} lot records...")
    batch = []
    for i, row in enumerate(lot_rows):
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            repo.insert_lot_summary_bulk(batch)
            db.commit()
            print(f"[DB] Saved lot batch {i//BATCH_SIZE + 1}")
            batch = []
    if batch:
        repo.insert_lot_summary_bulk(batch)
        db.commit()
    
    # Save to CSV
    supplier_file = OUTPUT_DIR / SUPPLIER_OUTPUT_FILENAME
    with open(supplier_file, "w", newline="", encoding="utf-8-sig") as f:
        if supplier_rows:
            writer = csv.DictWriter(f, fieldnames=supplier_rows[0].keys())
            writer.writeheader()
            writer.writerows(supplier_rows)
    print(f"[CSV] Suppliers saved to {supplier_file}")
    
    lot_file = OUTPUT_DIR / LOT_SUMMARY_OUTPUT_FILENAME
    with open(lot_file, "w", newline="", encoding="utf-8-sig") as f:
        if lot_rows:
            writer = csv.DictWriter(f, fieldnames=lot_rows[0].keys())
            writer.writeheader()
            writer.writerows(lot_rows)
    print(f"[CSV] Lots saved to {lot_file}")
    
    # Final stats
    elapsed = time.time() - start_time
    rate = total / elapsed if elapsed > 0 else 0
    print(f"\n[DONE] Processed {total} awards in {elapsed:.1f}s ({rate:.1f}/s)")
    print(f"[STATS] Success: {success_count}, Failed: {fail_count}")
    print(f"[DATA] Suppliers: {len(supplier_rows)}, Lots: {len(lot_rows)}")
    
    db.close()
    return supplier_rows, lot_rows


def main():
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    suppliers, lots = asyncio.run(process_awards(run_id))
    print(f"\nExtracted {len(suppliers)} supplier rows, {len(lots)} lot rows")


if __name__ == "__main__":
    main()
