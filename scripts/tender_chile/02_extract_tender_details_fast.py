#!/usr/bin/env python3
"""
Step 2 FAST: Extract Tender Details using HTTPX (async)
=======================================================
Replaces Selenium with HTTPX for 10x faster processing.
Reads from PostgreSQL table tc_tender_redirects.
"""

from __future__ import annotations

import asyncio
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime

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

# Config
MAX_TENDERS = getenv_int("MAX_TENDERS", 2000)
MAX_WORKERS = 10
MAX_REQ_PER_MIN = 3000
BATCH_SIZE = 50

# Tor proxy
TOR_PROXY = "socks5://127.0.0.1:9050"


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
    auth_elem = soup.find("a", {"id": "lnkFicha2Razon"})
    if not auth_elem:
        auth_elem = soup.find("span", {"id": "lblFicha2Razon"})
    if auth_elem:
        data["TENDERING AUTHORITY"] = clean(auth_elem.get_text())
    
    # Province - look in contact info section
    contact_div = soup.find("div", {"id": "FichaContacto"})
    if contact_div:
        text = contact_div.get_text()
        # Look for "Región" pattern
        m = re.search(r'(Región\s+de\s+\w+|Región\s+\w+|Región\s+del?\s+\w+)', text, re.IGNORECASE)
        if m:
            data["PROVINCE"] = clean(m.group(1))
    
    # Closing Date
    closing_elem = soup.find("span", {"id": "lblFicha3Cierre"})
    if closing_elem:
        data["Closing Date"] = clean(closing_elem.get_text())
    
    # Evaluation ratios - look in adjudication section
    adjud_table = soup.find("table", {"id": re.compile(r".*Adjudicacion.*", re.I)})
    if adjud_table:
        rows = adjud_table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) >= 2:
                name = clean(cells[0].get_text()).upper()
                val_text = clean(cells[1].get_text())
                pct_match = re.search(r'(\d+)%', val_text)
                if pct_match:
                    pct = int(pct_match.group(1))
                    if any(k in name for k in ["PRECIO", "PRICE", "ECONOMICA"]):
                        data["Price Evaluation ratio"] = pct
                    elif any(k in name for k in ["TECNICO", "TECHNICAL", "CALIDAD", "TECNICA"]):
                        data["Quality Evaluation ratio"] = pct
                    else:
                        data["Other Evaluation ratio"] = (data["Other Evaluation ratio"] or 0) + pct
    
    return data


def extract_lot_data(html: str, url: str) -> List[Dict]:
    """Extract lot data from HTML."""
    soup = BeautifulSoup(html, "html.parser")
    lots = []
    
    # Find lot table - usually grdItemsB or similar
    lot_table = soup.find("table", {"id": re.compile(r".*grdItem.*", re.I)})
    if not lot_table:
        return lots
    
    rows = lot_table.find_all("tr")
    for row in rows[1:]:  # Skip header
        cells = row.find_all("td")
        if len(cells) >= 4:
            lot_num = clean(cells[0].get_text())
            un_code = clean(cells[1].get_text())
            lot_title = clean(cells[2].get_text())
            qty = clean(cells[3].get_text())
            
            lots.append({
                "Lot Number": lot_num,
                "Unique Lot ID": un_code,
                "Lot Title": lot_title,
                "Quantity": qty,
            })
    
    return lots


async def process_tender(client: httpx.AsyncClient, url: str, tender_id: str, 
                         semaphore: asyncio.Semaphore, db, repo, run_id: str) -> Optional[Dict]:
    """Process a single tender URL."""
    async with semaphore:
        try:
            resp = await client.get(url, timeout=30.0)
            resp.raise_for_status()
            html = resp.text
            
            # Extract data
            tender_data = extract_tender_data(html, url)
            lots = extract_lot_data(html, url)
            
            if not tender_data.get("Tender ID"):
                return None
            
            # Prepare rows for CSV
            result_rows = []
            if lots:
                for lot in lots:
                    row = {
                        **tender_data,
                        **lot,
                        "Source URL": url,
                    }
                    result_rows.append(row)
            else:
                # No lots found, still save tender data
                result_rows.append({
                    **tender_data,
                    "Lot Number": "",
                    "Unique Lot ID": "",
                    "Lot Title": "",
                    "Quantity": "",
                    "Source URL": url,
                })
            
            # Save to database
            db_row = {
                "tender_id": tender_data.get("Tender ID", ""),
                "tender_name": tender_data.get("Tender Title", ""),
                "tender_status": "",
                "publication_date": "",
                "closing_date": tender_data.get("Closing Date", ""),
                "organization": tender_data.get("TENDERING AUTHORITY", ""),
                "province": tender_data.get("PROVINCE", ""),
                "contact_info": "",
                "description": "",
                "currency": "CLP",
                "estimated_amount": None,
                "source_url": url,
            }
            
            return {"rows": result_rows, "db_row": db_row}
            
        except Exception as e:
            print(f"[ERROR] Failed to process {url}: {e}")
            return None


async def async_main():
    """Main async entry point."""
    import sys
    sys.path.insert(0, str(_repo_root))
    from core.db.connection import CountryDB
    from db.repositories import ChileRepository
    
    run_id = os.getenv("TENDER_CHILE_RUN_ID", "")
    if not run_id:
        print("[ERROR] TENDER_CHILE_RUN_ID not set")
        sys.exit(1)
    
    # Connect to DB
    db = CountryDB("Tender_Chile")
    db.connect()
    repo = ChileRepository(db, run_id)
    
    # Read redirect URLs from DB
    with db.cursor(dict_cursor=True) as cur:
        cur.execute("""
            SELECT tender_id, redirect_url, source_url 
            FROM tc_tender_redirects 
            WHERE run_id = %s 
            ORDER BY id
            LIMIT %s
        """, (run_id, MAX_TENDERS))
        redirects = cur.fetchall()
    
    print(f"[INFO] Processing {len(redirects)} tenders with HTTPX (FAST)")
    print(f"[INFO] Workers: {MAX_WORKERS}, Rate: {MAX_REQ_PER_MIN}/min")
    
    # Setup HTTPX client with Tor proxy
    proxy = TOR_PROXY if os.path.exists(r"C:\TorProxy\Tor\tor.exe") else None
    limits = httpx.Limits(max_connections=MAX_WORKERS)
    timeout = httpx.Timeout(30.0, connect=10.0)
    
    async with httpx.AsyncClient(
        limits=limits,
        timeout=timeout,
        proxy=proxy,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
    ) as client:
        semaphore = asyncio.Semaphore(MAX_WORKERS)
        
        tasks = []
        for r in redirects:
            url = r.get("redirect_url") or r.get("source_url")
            tid = r.get("tender_id", "")
            if url:
                tasks.append(process_tender(client, url, tid, semaphore, db, repo, run_id))
        
        # Process with progress
        all_rows = []
        db_batch = []
        completed = 0
        start_time = time.time()
        
        for coro in asyncio.as_completed(tasks):
            result = await coro
            completed += 1
            
            if result:
                all_rows.extend(result["rows"])
                db_batch.append(result["db_row"])
                
                # Batch save to DB
                if len(db_batch) >= BATCH_SIZE:
                    repo.insert_tender_details_bulk(db_batch)
                    db.commit()
                    print(f"[DB] Saved {len(db_batch)} tenders (total: {len(all_rows)})")
                    db_batch.clear()
            
            # Progress
            if completed % 50 == 0 or completed == len(tasks):
                elapsed = time.time() - start_time
                rate = completed / elapsed if elapsed > 0 else 0
                eta = (len(tasks) - completed) / rate / 60 if rate > 0 else 0
                print(f"[{completed}/{len(tasks)}] {rate:.1f}/s | ETA: {eta:.1f}min")
        
        # Save remaining
        if db_batch:
            repo.insert_tender_details_bulk(db_batch)
            db.commit()
    
    # Save to CSV
    if all_rows:
        df = pd.DataFrame(all_rows)
        df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
        print(f"[OK] Saved {len(df)} rows to {OUTPUT_FILE}")
    
    db.close()
    print(f"[DONE] Processed {completed} tenders in {time.time() - start_time:.1f}s")


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
