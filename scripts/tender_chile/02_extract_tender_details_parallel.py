#!/usr/bin/env python3
"""
Step 2 PARALLEL: Extract Tender Details with multiprocessing
============================================================
Uses multiple Chrome instances in parallel for faster processing.
"""

from __future__ import annotations

import os
import sys
import time
import json
import re
import csv
import unicodedata
from pathlib import Path
from typing import Dict, List, Any, Optional
from multiprocessing import Pool, cpu_count
from datetime import datetime

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

_script_dir = Path(__file__).parent
if str(_script_dir) in sys.path:
    sys.path.remove(str(_script_dir))
sys.path.insert(0, str(_script_dir))

try:
    from config_loader import load_env_file, getenv_int, getenv_bool
    load_env_file()
except Exception:
    pass

# Output paths
OUTPUT_DIR = Path(_repo_root) / "output" / "Tender_Chile"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "tender_details.csv"

# Config
MAX_TENDERS = getenv_int("MAX_TENDERS", 6000)
HEADLESS_MODE = True

# Number of parallel workers (use half of CPU cores to avoid overload)
NUM_WORKERS = min(4, cpu_count() // 2)


def clean(s):
    return re.sub(r"\s+", " ", str(s or "")).strip()


def _strip_accents(s: str) -> str:
    if not s:
        return ""
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))


def norm_key(s: str) -> str:
    return _strip_accents(clean(s).upper()).upper()


QUALITY_KEYS = [
    "TECNICO", "TECHNICAL", "CALIDAD TECNICA", "TECNICA", "CALIDAD DEL PRODUCTO",
    "EVALUACION TECNICA", "TECHNICAL PROPOSAL",
]

PRICE_KEYS = ["PRECIO", "ECONOMIC", "PRICE", "OFERTA ECONOMICA", "ECONOMICA"]


def extract_single_tender(url_data: tuple) -> Optional[Dict]:
    """Extract tender data from a single URL. Runs in separate process."""
    url, idx, total = url_data
    
    # Import selenium here to avoid issues with multiprocessing
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    
    # Setup Chrome options
    chrome_options = Options()
    if HEADLESS_MODE:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    driver = None
    try:
        # Use chromedriver from path
        driver = webdriver.Chrome(options=chrome_options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        driver.get(url)
        
        # Fast wait for key elements
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.ID, "lblNumLicitacion")))
        
        # Extract tender data
        tender_data = {
            "Tender ID": "",
            "Tender Title": "",
            "TENDERING AUTHORITY": "",
            "PROVINCE": "",
            "Closing Date": "",
            "Price Evaluation ratio": "",
            "Quality Evaluation ratio": "",
            "Other Evaluation ratio": "",
        }
        
        try:
            tender_data["Tender Title"] = clean(driver.find_element(By.ID, "lblFicha1Nombre").text)
        except:
            pass
        
        try:
            tender_data["Tender ID"] = clean(driver.find_element(By.ID, "lblNumLicitacion").text)
        except:
            pass
        
        try:
            tender_data["TENDERING AUTHORITY"] = clean(driver.find_element(By.ID, "lnkFicha2Razon").text)
        except:
            try:
                tender_data["TENDERING AUTHORITY"] = clean(driver.find_element(By.ID, "lblFicha2Razon").text)
            except:
                pass
        
        try:
            tender_data["Closing Date"] = clean(driver.find_element(By.ID, "lblFicha3Cierre").text)
        except:
            pass
        
        # Province extraction
        province_val = ""
        try:
            contact_div = driver.find_element(By.ID, "FichaContacto")
            text = contact_div.text
            m = re.search(r'(Región\s+de\s+\w+|Región\s+\w+|Región\s+del?\s+\w+)', text, re.IGNORECASE)
            if m:
                province_val = clean(m.group(1))
        except:
            pass
        tender_data["PROVINCE"] = province_val
        
        # Extract lots
        lots = []
        try:
            lot_table = driver.find_element(By.ID, "grdItemsB")
            rows = lot_table.find_elements(By.TAG_NAME, "tr")
            for row in rows[1:]:  # Skip header
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 4:
                    lots.append({
                        "Lot Number": clean(cells[0].text),
                        "Unique Lot ID": clean(cells[1].text),
                        "Lot Title": clean(cells[2].text),
                        "Quantity": clean(cells[3].text),
                        "Source URL": url,
                        **tender_data
                    })
        except:
            pass
        
        if not lots:
            lots.append({
                "Lot Number": "",
                "Unique Lot ID": "",
                "Lot Title": "",
                "Quantity": "",
                "Source URL": url,
                **tender_data
            })
        
        return {
            "url": url,
            "lots": lots,
            "tender_data": tender_data,
            "success": True
        }
        
    except Exception as e:
        return {"url": url, "error": str(e), "success": False}
    
    finally:
        if driver:
            driver.quit()


def main():
    """Main entry point with multiprocessing."""
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
    
    tender_urls = [(r.get("redirect_url") or r.get("source_url"), i+1, len(redirects)) 
                   for i, r in enumerate(redirects) if r.get("redirect_url") or r.get("source_url")]
    
    print(f"[INFO] Processing {len(tender_urls)} tenders with {NUM_WORKERS} parallel workers")
    print(f"[INFO] This is ~{NUM_WORKERS}x faster than sequential processing")
    
    # Process in parallel
    all_rows = []
    batch_details = []
    total_saved = 0
    BATCH_SIZE = 10
    
    start_time = time.time()
    
    with Pool(processes=NUM_WORKERS) as pool:
        results = pool.imap_unordered(extract_single_tender, tender_urls)
        
        for i, result in enumerate(results, 1):
            if result and result.get("success"):
                all_rows.extend(result["lots"])
                
                # Prepare DB row
                tender_data = result.get("tender_data", {})
                tender_id = tender_data.get("Tender ID", "").strip()
                if tender_id:
                    batch_details.append({
                        "tender_id": tender_id,
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
                        "source_url": result.get("url", ""),
                    })
                    
                    if len(batch_details) >= BATCH_SIZE:
                        count = repo.insert_tender_details_bulk(batch_details)
                        db.commit()
                        total_saved += count
                        print(f"[DB] Batch saved: {count} tenders (total: {total_saved})")
                        batch_details.clear()
            
            # Progress
            if i % 10 == 0 or i == len(tender_urls):
                elapsed = time.time() - start_time
                rate = i / elapsed if elapsed > 0 else 0
                eta = (len(tender_urls) - i) / rate / 60 if rate > 0 else 0
                print(f"[{i}/{len(tender_urls)}] {rate:.1f}/s | ETA: {eta:.1f}min")
    
    # Save remaining
    if batch_details:
        count = repo.insert_tender_details_bulk(batch_details)
        db.commit()
        total_saved += count
        print(f"[DB] Final batch saved: {count} tenders (total: {total_saved})")
    
    # Save to CSV
    if all_rows:
        import pandas as pd
        df = pd.DataFrame(all_rows)
        df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
        print(f"[OK] Saved {len(df)} rows to {OUTPUT_FILE}")
    
    db.close()
    elapsed = time.time() - start_time
    print(f"[DONE] Processed {len(tender_urls)} tenders in {elapsed:.1f}s ({len(tender_urls)/elapsed:.1f}/s)")


if __name__ == "__main__":
    main()
