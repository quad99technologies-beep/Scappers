#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 3: Extract Tender Awards — httpx + BeautifulSoup (Fast)

Features:
  - Optional Tor SOCKS5 proxy (TOR_ENABLED=1) with NEWNYM every 12 min
  - Rate limiting: max 200 requests/min
  - Periodic DB saves (crash-safe)
  - Retry pass for failed tenders after main pass
  - All data inserted to PostgreSQL + CSV export

Requires: pip install httpx[socks] beautifulsoup4 lxml
Old Selenium version: archive/03_extract_tender_awards_selenium.py.bak
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
from typing import Any, Dict, List, Optional, Tuple

import httpx
from bs4 import BeautifulSoup

# ---- Path wiring ----
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

# ---- Config loader ----
try:
    from config_loader import load_env_file, getenv_int, get_output_dir as _get_output_dir
    load_env_file()
    _CONFIG_LOADER_AVAILABLE = True
except ImportError:
    _CONFIG_LOADER_AVAILABLE = False

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
SUPPLIER_OUTPUT_FILENAME = "mercadopublico_supplier_rows.csv"
LOT_SUMMARY_OUTPUT_FILENAME = "mercadopublico_lot_summary.csv"
MAX_WORKERS = getenv_int("SCRIPT_03_WORKERS", 8) if _CONFIG_LOADER_AVAILABLE else int(os.getenv("SCRIPT_03_WORKERS", "8"))
MAX_REQ_PER_MIN = getenv_int("MAX_REQ_PER_MIN", 200) if _CONFIG_LOADER_AVAILABLE else int(os.getenv("MAX_REQ_PER_MIN", "200"))
BATCH_SIZE = 50


def get_output_dir() -> Path:
    if _CONFIG_LOADER_AVAILABLE:
        return _get_output_dir()
    return _repo_root / "output" / "Tender_Chile"


# =====================================================================
# Parsing helpers (unchanged from original)
# =====================================================================

def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def parse_locale_number(raw: str) -> Optional[float]:
    if raw is None:
        return None
    s = clean_text(raw)
    if not s:
        return None
    s = re.sub(r"[^\d\.,\-\s]", "", s)
    s = clean_text(s)
    if re.fullmatch(r"\d+\s+\d{1,2}", s):
        s = s.replace(" ", "")
    if not s:
        return None
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        if s.count(",") > 1:
            s = s.replace(",", "")
        else:
            left, right = s.split(",", 1)
            if 1 <= len(right) <= 4:
                s = left.replace(".", "") + "." + right
            else:
                s = s.replace(",", "")
    elif "." in s:
        if s.count(".") > 1:
            s = s.replace(".", "")
        else:
            left, right = s.split(".", 1)
            if len(right) == 3:
                s = left + right
    try:
        return float(s)
    except ValueError:
        return None


def extract_award_date(html_str: str) -> Optional[str]:
    text = clean_text(BeautifulSoup(html_str, "lxml").get_text(" ", strip=True))
    m = re.search(
        r"\b(?:In|En)\s+[A-Za-zÁÉÍÓÚÜÑáéíóúüñ\.\-\s]+,\s*(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})\b",
        text, flags=re.IGNORECASE,
    )
    if m:
        d, mo, y = m.group(1), m.group(2), m.group(3)
        try:
            dt = datetime(int(y), int(mo), int(d))
            return dt.strftime("%Y-%m-%d")
        except Exception:
            return f"{y}-{mo.zfill(2)}-{d.zfill(2)}"
    return None


def is_awarded_state(state: str) -> bool:
    s = clean_text(state).lower()
    if re.search(r"\bnot\s+awarded\b", s):
        return False
    if re.search(r"\bno\s+adjudic", s):
        return False
    if re.search(r"\bno\s+award", s):
        return False
    return bool(re.search(r"\bawarded\b", s) or re.search(r"\badjudic", s))


def find_lot_container_for_gv(gv: Any) -> Any:
    node = gv
    while node is not None:
        lot_no_el = node.find(id=lambda x: x and x.endswith("__lblNumber"))
        if lot_no_el:
            return node
        node = node.find_parent()
    return None


def extract_lot_total_line(lot_container: Any) -> Tuple[str, Optional[float]]:
    lot_text = clean_text(lot_container.get_text(" ", strip=True))
    m = re.search(r"Total\s+(?:L[ií]nea|Linea|Line)\s*\$?\s*([\d\.\,\s]+)", lot_text, re.IGNORECASE)
    if not m:
        return "", None
    raw = clean_text(m.group(1))
    return raw, parse_locale_number(raw)


def extract_supplier_rows_and_lot_summary(html_text: str, award_url: str) -> Tuple[List[Dict], List[Dict]]:
    """Parse award HTML (same logic as Selenium version)."""
    soup = BeautifulSoup(html_text, "lxml")
    grd = soup.find("table", id="grdItemOC")
    if not grd:
        raise RuntimeError("Could not find #grdItemOC table.")

    award_date = extract_award_date(html_text)
    gv_tables = grd.find_all("table", id=lambda x: x and x.endswith("_gvLines"))
    if not gv_tables:
        raise RuntimeError("Found #grdItemOC but no bidder tables.")

    lots: Dict[str, Dict] = {}

    for gv in gv_tables:
        lot_container = find_lot_container_for_gv(gv)
        if not lot_container:
            continue

        lot_number_el = lot_container.find(id=lambda x: x and x.endswith("__lblNumber"))
        lot_number = clean_text(lot_number_el.get_text()) if lot_number_el else ""
        onu_code_el = lot_container.find(id=lambda x: x and x.endswith("lblCodeonu"))
        un_code = clean_text(onu_code_el.get_text()) if onu_code_el else ""
        schema_title_el = lot_container.find(id=lambda x: x and x.endswith("__LblSchemaTittle"))
        item_title = clean_text(schema_title_el.get_text()) if schema_title_el else ""
        buyer_desc_el = lot_container.find(id=lambda x: x and x.endswith("lblDescription"))
        buyer_spec = clean_text(buyer_desc_el.get_text()) if buyer_desc_el else ""
        qty_el = lot_container.find(id=lambda x: x and x.endswith("__LblRBICuantityNumber"))
        lot_quantity = clean_text(qty_el.get_text()) if qty_el else ""
        lot_total_line_raw, lot_total_line = extract_lot_total_line(lot_container)

        if lot_number not in lots:
            lots[lot_number] = {
                "award_date": award_date, "source_url": award_url,
                "lot_number": lot_number, "un_classification_code": un_code,
                "item_title": item_title, "buyer_specifications": buyer_spec,
                "lot_quantity": lot_quantity,
                "lot_total_line_raw": lot_total_line_raw, "lot_total_line": lot_total_line,
                "supplier_rows": [],
            }

        for tr in gv.find_all("tr")[1:]:
            tds = tr.find_all("td", recursive=False)
            if len(tds) < 6:
                continue
            supplier = clean_text(tds[0].get_text(" ", strip=True))
            supplier_specs = clean_text(tds[1].get_text(" ", strip=True))
            unit_offer_raw = clean_text(tds[2].get_text(" ", strip=True))
            awarded_qty_raw = clean_text(tds[3].get_text(" ", strip=True))
            total_net_awarded_raw = clean_text(tds[4].get_text(" ", strip=True))
            state = clean_text(tds[5].get_text(" ", strip=True))
            awarded = is_awarded_state(state)

            lots[lot_number]["supplier_rows"].append({
                "award_date": award_date, "source_url": award_url,
                "lot_number": lot_number, "un_classification_code": un_code,
                "item_title": item_title, "buyer_specifications": buyer_spec,
                "lot_quantity": lot_quantity,
                "supplier": supplier, "supplier_specifications": supplier_specs,
                "AWARDED LOT TITLE": supplier_specs,
                "unit_price_offer_raw": unit_offer_raw,
                "unit_price_offer": parse_locale_number(unit_offer_raw),
                "awarded_quantity_raw": awarded_qty_raw,
                "awarded_quantity": parse_locale_number(awarded_qty_raw),
                "total_net_awarded_raw": total_net_awarded_raw,
                "total_net_awarded": parse_locale_number(total_net_awarded_raw),
                "state": state, "is_awarded": "YES" if awarded else "NO",
                "awarded_unit_price": parse_locale_number(unit_offer_raw) if awarded else None,
                "lot_total_line_raw": lot_total_line_raw, "lot_total_line": lot_total_line,
            })

    supplier_rows: List[Dict] = []
    lot_summary_rows: List[Dict] = []

    def lot_sort_key(x):
        return (x == "" or x is None, int(x) if str(x).isdigit() else 999999, str(x))

    for lot_no in sorted(lots.keys(), key=lot_sort_key):
        lot = lots[lot_no]
        rows_in_lot = lot["supplier_rows"]
        supplier_rows.extend(rows_in_lot)
        awarded_rows = [r for r in rows_in_lot if r.get("is_awarded") == "YES"]
        has_award = "YES" if awarded_rows else "NO"
        first_awarded = awarded_rows[0] if awarded_rows else {}
        lot_summary_rows.append({
            "award_date": lot["award_date"], "source_url": lot["source_url"],
            "lot_number": lot["lot_number"],
            "un_classification_code": lot["un_classification_code"],
            "item_title": lot["item_title"], "buyer_specifications": lot["buyer_specifications"],
            "lot_quantity": lot["lot_quantity"],
            "lot_total_line_raw": lot["lot_total_line_raw"], "lot_total_line": lot["lot_total_line"],
            "HAS_AWARD": has_award,
            "LOT_RESULT": "Awarded" if has_award == "YES" else "No Award",
            "AWARDED_SUPPLIER": first_awarded.get("supplier", ""),
            "AWARDED LOT TITLE": first_awarded.get("AWARDED LOT TITLE", ""),
            "AWARDED_UNIT_PRICE": first_awarded.get("awarded_unit_price"),
            "AWARDED_AMOUNT": first_awarded.get("total_net_awarded"),
        })

    return supplier_rows, lot_summary_rows


# =====================================================================
# Main scraping logic
# =====================================================================

async def async_main():
    from core.db.connection import CountryDB
    from db.repositories import ChileRepository

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

    # Read redirect URLs from PostgreSQL
    db = CountryDB("Tender_Chile")
    db.connect()
    repo = ChileRepository(db, run_id)
    redirects = repo.get_all_tender_redirects()

    tender_award_pairs: List[Tuple[str, str, str]] = []
    for r in redirects:
        redirect_url = (r.get('redirect_url') or '').strip()
        if redirect_url:
            details_url = redirect_url
            award_url = redirect_url.replace("DetailsAcquisition.aspx", "StepsProcessAward/PreviewAwardAct.aspx")
            tender_id = r.get('tender_id', '')
            tender_award_pairs.append((details_url, award_url, tender_id))

    print(f"Reading: PostgreSQL 'tc_tender_redirects'")
    print(f"   Found {len(tender_award_pairs)} tender/award URL pair(s)")
    db.close()

    if not tender_award_pairs:
        print("[ERROR] No tender/award URL pairs found")
        sys.exit(1)

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

    print(f"\n[SCRAPER] Processing {len(tender_award_pairs)} tenders with {MAX_WORKERS} workers")
    if proxy_url:
        print(f"[SCRAPER] Using Tor proxy: {proxy_url}")
    print(f"[SCRAPER] Rate limit: {MAX_REQ_PER_MIN} req/min | Batch save: every {BATCH_SIZE} awards")

    all_supplier_rows: List[Dict] = []
    all_lot_summary_rows: List[Dict] = []
    total_awards_saved = 0
    completed = 0
    start_time = time.time()
    semaphore = asyncio.Semaphore(MAX_WORKERS)

    async def _run_pass(
        pairs: List[Tuple[str, str, str]], pass_name: str
    ) -> List[Tuple[str, str, str]]:
        nonlocal completed, total_awards_saved
        local_failed: List[Tuple[str, str, str]] = []
        batch_awards: List[Dict] = []

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

            batch_lock = asyncio.Lock()

            async def _save_batch(awards: List[Dict]):
                nonlocal total_awards_saved
                if not awards:
                    return
                try:
                    db2 = CountryDB("Tender_Chile")
                    db2.connect()
                    repo2 = ChileRepository(db2, run_id)
                    repo2.insert_tender_awards_bulk(awards)
                    db2.close()
                    total_awards_saved += len(awards)
                    print(f"[DB] Batch saved: {len(awards)} awards (total: {total_awards_saved})")
                except Exception as e:
                    print(f"[DB WARN] Batch save failed: {e}")

            async def process_tender(det: str, aw: str, tid: str):
                nonlocal completed
                async with semaphore:
                    await rate_limiter.acquire()
                    for attempt in range(2):
                        try:
                            resp = await client.get(aw, timeout=30.0)
                            resp.raise_for_status()
                            html_text = resp.text

                            if "grdItemOC" not in html_text:
                                local_failed.append((det, aw, tid))
                                completed += 1
                                if completed % 5 == 0:  # Show progress every 5 tenders
                                    elapsed = time.time() - start_time
                                    rate = completed / elapsed if elapsed > 0 else 0
                                    print(f"[{pass_name}] {completed}/{len(tender_award_pairs)} | {rate:.1f}/s | No award data")
                                return

                            supplier_rows, lot_summary_rows = extract_supplier_rows_and_lot_summary(html_text, aw)
                            if not supplier_rows:
                                completed += 1
                                return

                            for r in supplier_rows:
                                r["source_tender_url"] = det
                                r["tender_id"] = tid  # Add tender_id for merge in Step 4
                            for r in lot_summary_rows:
                                r["source_tender_url"] = det
                                r["tender_id"] = tid  # Add tender_id for merge in Step 4

                            all_supplier_rows.extend(supplier_rows)
                            all_lot_summary_rows.extend(lot_summary_rows)

                            # Collect ALL supplier rows for DB (not just winners)
                            for r in supplier_rows:
                                if tid:
                                    async with batch_lock:
                                        batch_awards.append({
                                            "tender_id": tid,
                                            "lot_number": r.get("lot_number", ""),
                                            "lot_title": r.get("item_title", ""),
                                            "un_classification_code": r.get("un_classification_code", ""),
                                            "buyer_specifications": r.get("buyer_specifications", ""),
                                            "lot_quantity": r.get("lot_quantity", ""),
                                            "supplier_name": r.get("supplier", ""),
                                            "supplier_rut": "",
                                            "supplier_specifications": r.get("supplier_specifications", ""),
                                            "unit_price_offer": r.get("unit_price_offer"),
                                            "awarded_quantity": r.get("awarded_quantity", ""),
                                            "total_net_awarded": r.get("total_net_awarded"),
                                            "award_amount": r.get("total_net_awarded") if r.get("is_awarded") == "YES" else None,
                                            "award_date": r.get("award_date", ""),
                                            "award_status": r.get("state", ""),
                                            "is_awarded": r.get("is_awarded", ""),
                                            "awarded_unit_price": r.get("awarded_unit_price"),
                                            "source_url": aw,
                                            "source_tender_url": det,
                                        })
                                        if len(batch_awards) >= BATCH_SIZE:
                                            await _save_batch(batch_awards.copy())
                                            batch_awards.clear()

                            completed += 1
                            if completed % 5 == 0 or completed == len(tender_award_pairs):  # Every 5 tenders
                                elapsed = time.time() - start_time
                                rate = completed / elapsed if elapsed > 0 else 0
                                eta = (len(tender_award_pairs) - completed) / rate / 60 if rate > 0 else 0
                                print(f"[{pass_name}] {completed}/{len(tender_award_pairs)} | "
                                      f"{rate:.1f}/s | Suppliers: {len(all_supplier_rows)} | ETA: {eta:.0f}min")
                            return

                        except Exception as e:
                            if attempt == 0:
                                await asyncio.sleep(2)
                            else:
                                # Log error details
                                error_msg = f"{type(e).__name__}: {str(e)[:100]}"
                                if completed < 20:  # Only show first 20 errors
                                    print(f"   [ERROR] {tid}: {error_msg}")
                                local_failed.append((det, aw, tid))
                                completed += 1
                                return


            print(f"[{pass_name}] Starting to process {len(pairs)} tenders...")
            tasks = [process_tender(det, aw, tid) for det, aw, tid in pairs]
            await asyncio.gather(*tasks, return_exceptions=True)

        # Save remaining batch
        if batch_awards:
            await _save_batch(batch_awards)

        return local_failed

    # ---- Main pass ----
    failed = await _run_pass(tender_award_pairs, "MAIN")

    # ---- Retry pass ----
    if failed:
        print(f"\n[RETRY] Retrying {len(failed)} failed tenders...")
        still_failed = await _run_pass(failed, "RETRY")
        if still_failed:
            print(f"[RETRY] Still failed: {len(still_failed)}")

    # Stop rotator
    if rotator:
        rotator.stop()
        if rotator.rotation_count > 0:
            print(f"[TOR] Completed {rotator.rotation_count} NEWNYM rotations")

    elapsed = time.time() - start_time
    print(f"\n[TIMING] Processed in {elapsed:.1f}s")

    if not all_supplier_rows:
        print("\n[WARN] No supplier rows extracted")
        print("[WARN] Award pages may need JavaScript — use Selenium fallback")
        sys.exit(1)

    # CSV exports
    supplier_fields = list(all_supplier_rows[0].keys())
    summary_fields = list(all_lot_summary_rows[0].keys()) if all_lot_summary_rows else []

    supplier_csv = output_dir / SUPPLIER_OUTPUT_FILENAME
    lot_summary_csv = output_dir / LOT_SUMMARY_OUTPUT_FILENAME

    with open(supplier_csv, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=supplier_fields)
        w.writeheader()
        w.writerows(all_supplier_rows)

    if all_lot_summary_rows:
        with open(lot_summary_csv, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=summary_fields)
            w.writeheader()
            w.writerows(all_lot_summary_rows)

    print(f"\n{'='*80}")
    print(f"[OK] Supplier rows: {len(all_supplier_rows)} -> {supplier_csv}")
    print(f"[OK] Lot summary: {len(all_lot_summary_rows)} -> {lot_summary_csv}")
    print(f"[OK] Awards in DB: {total_awards_saved}")
    print(f"{'='*80}")


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
