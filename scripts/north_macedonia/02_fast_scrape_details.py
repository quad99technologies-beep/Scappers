#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
North Macedonia Drug Detail Scraper - httpx + lxml (Fast)
=========================================================
Replaces Selenium-based 02_scrape_details.py with pure HTTP scraping.

All detail pages on lekovi.zdravstvo.gov.mk are server-rendered HTML.
No JavaScript execution needed — httpx + lxml is 10-20x faster.

Old Selenium version archived at: archive/02_scrape_details_selenium.py.bak
"""

import asyncio
import os
import re
import sys
import time
import csv
from pathlib import Path
from typing import Dict, List, Optional

# Add repo root for core imports (MUST be before any core imports)
SCRIPT_DIR = Path(__file__).resolve().parent
_repo_root = Path(__file__).resolve().parents[2]

if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from core.monitoring.audit_logger import audit_log

import logging
import httpx
from lxml import html as lxml_html

logger = logging.getLogger(__name__)

# Optional translation (only used if values still Macedonian)
try:
    from googletrans import Translator  # type: ignore
    _translator = Translator()
except Exception:
    logger.warning("googletrans not available; translations will be skipped")
    _translator = None

# Semaphore: cap concurrent Google Translate calls to avoid WinError 10035
# (WSAEWOULDBLOCK — Windows non-blocking socket exhaustion with many workers)
_translate_sem: asyncio.Semaphore = asyncio.Semaphore(3)


# -----------------------------
# CONFIG
# -----------------------------

# Fix for module shadowing: Remove any conflicting 'db' module from sys.modules
# to ensure 'from db ...' resolves to the local db directory.
if "db" in sys.modules:
    del sys.modules["db"]

try:
    from config_loader import load_env_file, get_output_dir, getenv, getenv_bool, getenv_int, getenv_float
    load_env_file()
    OUTPUT_DIR = get_output_dir()
    USE_CONFIG = True
except Exception:
    OUTPUT_DIR = SCRIPT_DIR
    USE_CONFIG = False
    def getenv(key: str, default: str = None) -> str:
        return os.getenv(key, default if default is not None else "")
    def getenv_bool(key: str, default: bool = False) -> bool:
        return str(os.getenv(key, str(default))).lower() in ("1", "true", "yes", "on")
    def getenv_int(key: str, default: int = 0) -> int:
        try: return int(os.getenv(key, str(default)))
        except: return default
    def getenv_float(key: str, default: float = 0.0) -> float:
        try: return float(os.getenv(key, str(default)))
        except: return default

MAX_WORKERS = getenv_int("SCRIPT_02_DETAIL_WORKERS", 15)
SLEEP_BETWEEN = getenv_float("SCRIPT_02_SLEEP_BETWEEN_DETAILS", 0.15)
BATCH_SIZE = 100

# Reimbursement constants (as per requirement)
REIMBURSABLE_STATUS = "PARTIALLY REIMBURSABLE"
REIMBURSABLE_RATE = "80.00%"
REIMBURSABLE_NOTES = ""
COPAYMENT_VALUE = ""
COPAYMENT_PERCENT = "20.00%"
MARGIN_RULE = "650 PPP & PPI Listed"
VAT_PERCENT = "5"

OUT_COLUMNS = [
    "Local Product Name",
    "Local Pack Code",
    "Generic Name",
    "WHO ATC Code",
    "Formulation",
    "Strength Size",
    "Fill Size",
    "Customized 1",
    "Marketing Authority / Company Name",
    "Effective Start Date",
    "Effective End Date",
    "Public with VAT Price",
    "Pharmacy Purchase Price",
    "Local Pack Description",
    "Reimbursable Status",
    "Reimbursable Rate",
    "Reimbursable Notes",
    "Copayment Value",
    "Copayment Percent",
    "Margin Rule",
    "VAT Percent",
    "detail_url",
]


# -----------------------------
# HELPERS
# -----------------------------
def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


_cyrillic_re = re.compile(r"[\u0400-\u04FF]")

def looks_cyrillic(text: str) -> bool:
    return bool(_cyrillic_re.search(text or ""))


def translate_to_en(text: str) -> str:
    text = normalize_ws(text)
    if not text or not looks_cyrillic(text):
        return text
    if _translator is None:
        return text

    # Retry with backoff for WSAEWOULDBLOCK (WinError 10035)
    # which occurs when too many concurrent calls saturate the socket pool.
    max_retries = 3
    for attempt in range(max_retries):
        try:
            result = _translator.translate(text, src="mk", dest="en")
            return normalize_ws(result.text)
        except OSError as e:
            if getattr(e, 'winerror', None) == 10035 or getattr(e, 'errno', None) == 10035:
                # WSAEWOULDBLOCK — back off and retry
                sleep_s = 0.5 * (2 ** attempt)  # 0.5s, 1s, 2s
                if attempt < max_retries - 1:
                    time.sleep(sleep_s)
                    continue
            logger.debug(f"[NM] Translation failed for {text!r}: {e}")
            return text
        except Exception as e:
            logger.debug(f"[NM] Translation failed for {text!r}: {e}")
            return text
    logger.debug(f"[NM] Translation gave up after {max_retries} retries for {text!r}")
    return text


def make_local_pack_description(formulation, fill_size, strength, composition):
    parts = [normalize_ws(p) for p in [formulation, fill_size, strength, composition] if p]
    return " ".join(parts)


def get_by_any_contains(data: Dict[str, str], *needles: str) -> str:
    wants = [n.strip().lower() for n in needles if n and n.strip()]
    if not wants:
        return ""
    for label, value in data.items():
        ll = (label or "").lower()
        if any(w in ll for w in wants):
            return value
    return ""


# Unused CSV loader removed.


# -----------------------------
# EXTRACTION (lxml — no browser)
# -----------------------------
def extract_from_html(html_text: str, url: str) -> Optional[Dict[str, str]]:
    """
    Extract drug details from HTML using lxml.
    Parses div.row-fluid label→value pairs, same as Selenium version
    but 10-20x faster.
    """
    doc = lxml_html.fromstring(html_text)

    # Parse div.row-fluid for label→value pairs
    data: Dict[str, str] = {}
    rows = doc.xpath('//div[contains(@class,"row-fluid")]')
    for row in rows:
        label = normalize_ws(row.xpath('string(.//div[contains(@class,"span2")]//b)'))
        value = normalize_ws(row.xpath('string(.//div[contains(@class,"span6")])'))
        if label:
            data[label] = value

    if not data:
        return None

    # Extract fields using label matching (supports both MK and EN)
    local_product = get_by_any_contains(data, "име на лекот (латиница)", "name of the drug (latin)")
    ean = get_by_any_contains(data, "ean", "ean код")
    generic = get_by_any_contains(data, "генеричко име", "generic name")
    atc = get_by_any_contains(data, "атц", "atc")
    formulation = get_by_any_contains(data, "фармацевтска форма", "pharmaceutical form")
    strength = get_by_any_contains(data, "јачина", "strength", "reliability")
    packaging = get_by_any_contains(data, "пакување", "packaging")
    composition = get_by_any_contains(data, "состав", "composition")
    manufacturers = get_by_any_contains(data, "производители", "manufacturers")
    eff_start = get_by_any_contains(data, "датум на решение", "decision date", "date of solution")
    eff_end = get_by_any_contains(data, "датум на важност", "expiration date", "date of validity")
    retail_vat = get_by_any_contains(data, "малопродажна цена со", "retail price with vat")
    wholesale = get_by_any_contains(data, "големопродажна цена", "wholesale price excluding vat", "wholesale price without vat")

    # Validate: at least EAN or Product Name or Generic must exist
    if not any([ean, local_product, generic]):
        return None

    local_pack_desc = make_local_pack_description(formulation, packaging, strength, composition)

    return {
        "Local Product Name": translate_to_en(local_product),
        "Local Pack Code": normalize_ws(ean),
        "Generic Name": translate_to_en(generic),
        "WHO ATC Code": normalize_ws(atc),
        "Formulation": translate_to_en(formulation),
        "Strength Size": translate_to_en(strength),
        "Fill Size": translate_to_en(packaging),
        "Customized 1": translate_to_en(composition),
        "Marketing Authority / Company Name": translate_to_en(manufacturers),
        "Effective Start Date": normalize_ws(eff_start),
        "Effective End Date": normalize_ws(eff_end),
        "Public with VAT Price": normalize_ws(retail_vat),
        "Pharmacy Purchase Price": normalize_ws(wholesale),
        "Local Pack Description": translate_to_en(local_pack_desc),
        "Reimbursable Status": REIMBURSABLE_STATUS,
        "Reimbursable Rate": REIMBURSABLE_RATE,
        "Reimbursable Notes": REIMBURSABLE_NOTES,
        "Copayment Value": COPAYMENT_VALUE,
        "Copayment Percent": COPAYMENT_PERCENT,
        "Margin Rule": MARGIN_RULE,
        "VAT Percent": VAT_PERCENT,
        "detail_url": url,
    }


# -----------------------------
# CONCURRENT SCRAPER
# -----------------------------
async def scrape_details_concurrent(urls: List[str], repo=None, url_to_id=None) -> int:
    """
    Scrape drug detail pages using httpx + lxml (no browser).

    DB-FIRST ARCHITECTURE: All data written to nm_drug_register table.
    No CSV output (removed for consistency with other scrapers).

    Args:
        urls: List of detail page URLs to scrape
        repo: NorthMacedoniaRepository for DB writes (REQUIRED)

    Returns:
        Number of successfully scraped products
    """
    if not repo:
        raise ValueError("Repository is required for DB-first architecture. Pass NorthMacedoniaRepository instance.")
    
    audit_log("RUN_STARTED", scraper_name="NorthMacedonia", run_id=repo.run_id, details={"step": "02_details", "urls": len(urls)})

    print(f"\n[SCRAPER] Starting httpx scraping with {MAX_WORKERS} workers", flush=True)
    print(f"[SCRAPER] Total URLs to scrape: {len(urls)}", flush=True)
    print(f"[SCRAPER] Batch size: {BATCH_SIZE}", flush=True)

    completed = 0
    failed = 0
    start_time = time.time()
    buffer: List[Dict] = []
    buffer_lock = asyncio.Lock()

    url_queue: asyncio.Queue = asyncio.Queue()
    for u in urls:
        await url_queue.put(u)

    async with httpx.AsyncClient(
        headers={
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9,mk;q=0.8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36",
        },
        timeout=30.0,
        follow_redirects=True,
        limits=httpx.Limits(max_connections=MAX_WORKERS + 5, max_keepalive_connections=MAX_WORKERS),
    ) as client:

        async def worker(wid: int):
            nonlocal completed, failed

            while True:
                try:
                    url = url_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                result = None
                error_msg = None
                for attempt in range(2):
                    start_ts = time.time()
                    try:
                        resp = await client.get(url)
                        elapsed = (time.time() - start_ts) * 1000
                        await asyncio.to_thread(repo.log_request, url, "GET", resp.status_code, len(resp.content), elapsed)

                        resp.raise_for_status()
                        # Translation is sync (googletrans), run in thread
                        result = await asyncio.to_thread(extract_from_html, resp.text, url)
                        break
                    except Exception as e:
                        elapsed = (time.time() - start_ts) * 1000
                        status_code = 0
                        if hasattr(e, 'response') and e.response:
                             status_code = e.response.status_code
                        await asyncio.to_thread(repo.log_request, url, "GET", status_code, 0, elapsed, str(e))

                        error_msg = str(e).split(chr(10))[0][:200]
                        if attempt == 0:
                            await asyncio.sleep(2)
                        else:
                            if failed < 30:
                                print(f"[ERROR] W{wid}: {error_msg[:100]}", flush=True)
                            failed += 1

                if result:
                    # Link back to source URL record
                    if url_to_id and url in url_to_id:
                        result["url_id"] = url_to_id[url]

                    async with buffer_lock:
                        buffer.append(result)
                        completed += 1
                        # Note: URL will be marked 'scraped' AFTER successful batch insert
                else:
                    # Extraction failed or HTTP error
                    failed += 1
                    if not error_msg:
                        error_msg = "Extraction failed: missing required fields (EAN, product name, or generic)"

                    # Mark URL as failed in DB
                    if repo:
                        try:
                            with repo.db.cursor() as cur:
                                cur.execute("""
                                    UPDATE nm_urls
                                    SET status = 'failed',
                                        error_message = %s,
                                        retry_count = retry_count + 1
                                    WHERE run_id = %s AND detail_url = %s
                                """, (error_msg, repo.run_id, url))
                            repo.db.commit()
                        except Exception as e:
                            print(f"[DB WARN] Could not update failed URL: {e}")

                # Check if buffer is full and needs to be flushed (outside both if/else blocks)
                async with buffer_lock:
                    if len(buffer) >= BATCH_SIZE:
                        batch = buffer.copy()
                        buffer.clear()
                    else:
                        batch = None

                # Process batch outside the lock to avoid blocking workers
                if batch:
                    # Write to DB (DB-first architecture)
                    if repo:
                        try:
                            db_records = []
                            batch_urls = []
                            for row in batch:
                                db_records.append({
                                    "registration_number": row.get("Local Pack Code", ""),
                                    "product_name": row.get("Local Product Name", ""),
                                    "product_name_en": row.get("Local Product Name", ""),
                                    "generic_name": row.get("Generic Name", ""),
                                    "generic_name_en": row.get("Generic Name", ""),
                                    "dosage_form": row.get("Formulation", ""),
                                    "strength": row.get("Strength Size", ""),
                                    "pack_size": row.get("Fill Size", ""),
                                    "composition": row.get("Customized 1", ""),
                                    "manufacturer": row.get("Marketing Authority / Company Name", ""),
                                    "marketing_authorisation_holder": row.get("Marketing Authority / Company Name", ""),
                                    "atc_code": row.get("WHO ATC Code", ""),
                                    "url_id": row.get("url_id"),
                                    "source_url": row.get("detail_url", ""),
                                    "public_price": row.get("Public with VAT Price", ""),
                                    "pharmacy_price": row.get("Pharmacy Purchase Price", ""),
                                    "description": row.get("Local Pack Description", ""),
                                    "effective_start_date": row.get("Effective Start Date", ""),
                                    "effective_end_date": row.get("Effective End Date", ""),
                                })
                                batch_urls.append(row.get("detail_url", ""))

                            # Insert drug records
                            repo.insert_drug_register_batch(db_records)
                            audit_log("INSERT_BATCH", scraper_name="NorthMacedonia", run_id=repo.run_id, details={"inserted": len(db_records)})

                            # Mark URLs as scraped AFTER successful insert
                            if batch_urls:
                                with repo.db.cursor() as cur:
                                    for batch_url in batch_urls:
                                        cur.execute("""
                                            UPDATE nm_urls
                                            SET status = 'scraped',
                                                scraped_at = CURRENT_TIMESTAMP,
                                                error_message = NULL
                                            WHERE run_id = %s AND detail_url = %s
                                        """, (repo.run_id, batch_url))
                                repo.db.commit()

                            elapsed = time.time() - start_time
                            rate = completed / elapsed if elapsed > 0 else 0
                            remaining = len(urls) - completed - failed
                            eta_min = (remaining / rate / 60) if rate > 0 else 0
                            print(f"[PROGRESS] {completed}/{len(urls)} | "
                                  f"{rate:.1f}/s | Failed: {failed} | ETA: {eta_min:.0f}min", flush=True)
                        except Exception as e:
                            print(f"[DB ERROR] {e}", flush=True)

                await asyncio.sleep(SLEEP_BETWEEN)

        tasks = [asyncio.create_task(worker(i)) for i in range(MAX_WORKERS)]
        await asyncio.gather(*tasks, return_exceptions=True)

    # Final batch - write to DB
    if buffer:
        if repo:
            try:
                db_records = []
                batch_urls = []
                for row in buffer:
                    db_records.append({
                        "registration_number": row.get("Local Pack Code", ""),
                        "product_name": row.get("Local Product Name", ""),
                        "product_name_en": row.get("Local Product Name", ""),
                        "generic_name": row.get("Generic Name", ""),
                        "generic_name_en": row.get("Generic Name", ""),
                        "dosage_form": row.get("Formulation", ""),
                        "strength": row.get("Strength Size", ""),
                        "pack_size": row.get("Fill Size", ""),
                        "composition": row.get("Customized 1", ""),
                        "manufacturer": row.get("Marketing Authority / Company Name", ""),
                        "marketing_authorisation_holder": row.get("Marketing Authority / Company Name", ""),
                        "atc_code": row.get("WHO ATC Code", ""),
                        "url_id": row.get("url_id"),
                        "source_url": row.get("detail_url", ""),
                        "public_price": row.get("Public with VAT Price", ""),
                        "pharmacy_price": row.get("Pharmacy Purchase Price", ""),
                        "description": row.get("Local Pack Description", ""),
                        "effective_start_date": row.get("Effective Start Date", ""),
                        "effective_end_date": row.get("Effective End Date", ""),
                    })
                    batch_urls.append(row.get("detail_url", ""))

                # Insert drug records
                repo.insert_drug_register_batch(db_records)
                audit_log("INSERT_BATCH", scraper_name="NorthMacedonia", run_id=repo.run_id, details={"inserted": len(db_records)})

                # Mark URLs as scraped AFTER successful insert
                if batch_urls:
                    with repo.db.cursor() as cur:
                        for batch_url in batch_urls:
                            cur.execute("""
                                UPDATE nm_urls
                                SET status = 'scraped',
                                    scraped_at = CURRENT_TIMESTAMP,
                                    error_message = NULL
                                WHERE run_id = %s AND detail_url = %s
                            """, (repo.run_id, batch_url))
                    repo.db.commit()
            except Exception as e:
                print(f"[DB ERROR] {e}")

        print(f"[DB] Final batch: {len(buffer)} products")
        buffer.clear()

    elapsed = time.time() - start_time
    print(f"\n[SCRAPER] Complete! ({elapsed/60:.1f} min)", flush=True)
    print(f"  Scraped: {completed}/{len(urls)}", flush=True)
    if failed:
        print(f"  Failed: {failed}", flush=True)
    return completed


# -----------------------------
# MAIN
# -----------------------------
async def async_main():
    # Optional DB integration (load from DB if available, otherwise fallback to CSV)
    repo = None
    run_id = os.environ.get("NORTH_MACEDONIA_RUN_ID", "").strip()
    all_urls = []
    already_scraped = set()

    # Fallback: read run_id from .current_run_id file if env var not set
    if not run_id:
        try:
            from config_loader import get_output_dir as _god
            _rid_file = _god() / ".current_run_id"
        except ImportError:
            _rid_file = Path(__file__).resolve().parents[2] / "output" / "NorthMacedonia" / ".current_run_id"
        if _rid_file.exists():
            run_id = _rid_file.read_text(encoding="utf-8").strip()
            if run_id:
                print(f"[DB] Loaded run_id from file: {run_id}", flush=True)

    if not run_id:
        print("[ERROR] No run_id found. Run Step 0 (backup and clean) first.", flush=True)
        return

    # Try DB first
    try:
        from core.db.connection import CountryDB
        from db.repositories import NorthMacedoniaRepository
        db = CountryDB("NorthMacedonia")
        repo = NorthMacedoniaRepository(db, run_id)
        repo.ensure_run_in_ledger(mode="resume")
    except (ImportError, ModuleNotFoundError):
        # Fallback to absolute paths
        try:
            from core.db.connection import CountryDB
            from scripts.north_macedonia.db.repositories import NorthMacedoniaRepository
            db = CountryDB("NorthMacedonia")
            repo = NorthMacedoniaRepository(db, run_id)
            repo.ensure_run_in_ledger(mode="resume")
            print("[INFO] Used fallback import path for NorthMacedoniaRepository")
        except Exception as e:
            print(f"[DB ERROR] Fallback import failed: {e}")
            raise e


    except Exception as e:
        print(f"[DB ERROR] Could not load from database: {e}")
        print(f"[DB ERROR] Database connection is required. CSV fallback removed for DB-first architecture.")
        raise RuntimeError(f"Database connection failed. Run Step 0 first to initialize schema, then Step 1 to collect URLs. Error: {e}")

    # Load ALL URLs from nm_urls (pending + scraped) for this run
    url_to_id = {}
    if repo is not None:
        try:
            with db.cursor() as cur:
                cur.execute("""
                    SELECT id, detail_url, status
                    FROM nm_urls
                    WHERE run_id = %s
                    ORDER BY id
                """, (run_id,))
                rows = cur.fetchall()
                for row in rows:
                    uid    = row[0] if isinstance(row, tuple) else row["id"]
                    url    = row[1] if isinstance(row, tuple) else row["detail_url"]
                    status = row[2] if isinstance(row, tuple) else row["status"]
                    
                    all_urls.append(url)
                    url_to_id[url] = uid
                    if status == "scraped":
                        already_scraped.add(url)
            print(f"[DB] Loaded {len(all_urls)} URLs ({len(already_scraped)} already scraped)", flush=True)
        except Exception as e:
            print(f"[DB ERROR] Could not load URLs from nm_urls: {e}")
            raise RuntimeError(f"Failed to load URLs from database: {e}")

    todo_urls = [u for u in all_urls if u not in already_scraped]


    print(f"\n{'='*60}")
    print(f"[STARTUP] North Macedonia Fast Detail Scraper (httpx + lxml)")
    print(f"{'='*60}")
    print(f"Database:          Connected (run_id: {run_id})")
    print(f"Total URLs:        {len(all_urls)}")
    print(f"Already scraped:   {len(already_scraped)}")
    print(f"URLs to scrape:    {len(todo_urls)}")
    print(f"Workers:           {MAX_WORKERS}")
    print(f"{'='*60}\n")

    if not todo_urls:
        print("[INFO] No new URLs to scrape. All URLs already processed in database.")
        return

    scraped = await scrape_details_concurrent(todo_urls, repo, url_to_id=url_to_id)

    total = len(already_scraped) + scraped
    print(f"\n[DONE] Total rows in output: {total}")

    # Update run ledger
    if repo:
        try:
            repo.finish_run("completed", items_scraped=total)
        except Exception as e:
            print(f"[DB] Could not update run status: {e}")


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
