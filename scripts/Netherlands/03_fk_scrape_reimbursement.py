#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Netherlands FK - Reimbursement Detail Scraping (Step 3)

Fetches each FK detail page, parses composition/indications/reimbursement data.
Uses httpx.AsyncClient + asyncio.Queue worker pool.
Stores raw Dutch text in nl_fk_reimbursement (translation is Step 4).
"""

from __future__ import annotations

import asyncio
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Path wiring
SCRIPT_DIR = Path(__file__).resolve().parent
_repo_root = SCRIPT_DIR.parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import httpx
from bs4 import BeautifulSoup

from core.utils.logger import get_logger
from core.db.postgres_connection import get_db
from core.pipeline.standalone_checkpoint import run_with_checkpoint

for _m in list(sys.modules.keys()):
    if _m == "db" or _m.startswith("db."):
        del sys.modules[_m]

from config_loader import getenv, getenv_int, getenv_float, get_output_dir
from scripts.Netherlands.db import apply_netherlands_schema, NetherlandsRepository

log = get_logger(__name__, "Netherlands")

SCRIPT_ID = "Netherlands"
STEP_NUMBER = 3
STEP_NAME = "FK Reimbursement Scraping"

FK_BASE_URL = "https://www.farmacotherapeutischkompas.nl"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml",
}


# ---------------------------------------------------------------
# Text Helpers
# ---------------------------------------------------------------

def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def abs_url(href: str) -> str:
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return FK_BASE_URL + href
    return FK_BASE_URL + "/" + href


def split_strengths(raw: str) -> List[str]:
    raw = norm_space(raw)
    if not raw:
        return []
    parts = [norm_space(p) for p in re.split(r",\s+|;", raw) if norm_space(p)]
    if not parts:
        return [raw.upper()]
    return [p.upper() for p in parts]


def route_from_dosage_form(dosage_form: str) -> str:
    if not dosage_form:
        return "ORAL"
    df = dosage_form.lower()
    if any(k in df for k in ("tablet", "capsul", "suspension", "granulat", "drank", "poeder", "siroop")):
        return "ORAL"
    if any(k in df for k in ("inject", "infus", "intraveneu")):
        return "PARENTERAL"
    if any(k in df for k in ("crème", "creme", "zalf", "gel ", "pleister", "cutaan")):
        return "TOPICAL"
    if any(k in df for k in ("inhal", "aerosol", "vernevel")):
        return "INHALATION"
    if any(k in df for k in ("oogdr", "oog")):
        return "OPHTHALMIC"
    if any(k in df for k in ("neusspray", "neusdr")):
        return "NASAL"
    if any(k in df for k in ("zetpil", "rect")):
        return "RECTAL"
    return "ORAL"


def pack_details(brand: str, dosage_form: str, strength: str) -> str:
    b = (brand or "").upper().strip()
    df = (dosage_form or "").lower()
    st = (strength or "").upper().strip()
    if "tablet" in df:
        return f"{b} TABLETS {st}"
    if "capsul" in df:
        return f"{b} CAPSULES {st}"
    if "granulat" in df or "suspension" in df or "drank" in df:
        st2 = st.replace("MG/ML", "MG / ML")
        return f"{b} SUSPENSION FOR ORAL USE {st2}"
    if "inject" in df or "infus" in df:
        return f"{b} INJECTION {st}"
    return f"{b} {st}".strip()


def normalize_company(manfact: str) -> str:
    m = norm_space(manfact)
    if not m:
        return ""
    return m.upper()


def likely_generic_from_title(soup: BeautifulSoup) -> str:
    title = soup.title.get_text(strip=True) if soup.title else ""
    return title.upper().strip() if title else ""


# ---------------------------------------------------------------
# Product data structure
# ---------------------------------------------------------------

@dataclass
class ProductCore:
    generic_name: str
    brand_name: str
    manufacturer: str
    dosage_form: str
    strengths: List[str]
    reimbursement_status: str = "REIMBURSED"


# ---------------------------------------------------------------
# HTML Parsing
# ---------------------------------------------------------------

def parse_reimbursement_status(rcp) -> str:
    """Parse reimbursement status from a recipe section."""
    xgvs = rcp.select_one("span.xgvs")
    if xgvs:
        return "NOT REIMBURSED"
    bijlage2 = rcp.select_one("span.bijlage2") or rcp.select_one("span.bijlage-2")
    if bijlage2:
        return "CONDITIONAL"
    otc = rcp.select_one("span.otc")
    if otc:
        return "OTC"
    return "REIMBURSED"


def parse_all_compositions(soup: BeautifulSoup) -> List[ProductCore]:
    """Find ALL product recipes in the composition div."""
    generic_guess = likely_generic_from_title(soup)

    comp_div = None
    for div in soup.find_all("div", id=True):
        if str(div.get("id", "")).endswith("-samenstelling"):
            comp_div = div
            break

    results: List[ProductCore] = []
    if not comp_div:
        return [ProductCore(generic_guess, "", "", "", [])]

    recipes = comp_div.select("section.recipe")
    for rcp in recipes:
        name_span = rcp.select_one("span.name")
        manf_span = rcp.select_one("span.manfact")
        name = norm_space(name_span.get_text(" ", strip=True)) if name_span else ""
        manf = norm_space(manf_span.get_text(" ", strip=True)) if manf_span else ""

        reimb_status = parse_reimbursement_status(rcp)

        dose_blocks = rcp.select("dl.details")
        if not dose_blocks:
            dose_blocks = [rcp]

        for block in dose_blocks:
            app_dd = block.select_one("dt.application + dd")
            conc_dd = block.select_one("dt.concentration + dd")
            if not app_dd and not conc_dd:
                continue

            dosage_form_str = norm_space(app_dd.get_text(" ", strip=True)) if app_dd else ""
            strengths_raw = norm_space(conc_dd.get_text(" ", strip=True)) if conc_dd else ""
            strengths = split_strengths(strengths_raw)

            results.append(ProductCore(
                generic_name=generic_guess,
                brand_name=(name or "").upper().strip(),
                manufacturer=norm_space(manf),
                dosage_form=dosage_form_str,
                strengths=strengths,
                reimbursement_status=reimb_status,
            ))

    if not results:
        results.append(ProductCore(generic_guess, "", "", "", []))

    return results


def parse_indications_by_population(soup: BeautifulSoup) -> Dict[str, List[str]]:
    """Parse indications grouped by patient population."""
    indic_div = None
    for div in soup.find_all("div", id=True):
        if str(div.get("id", "")).endswith("-indicaties"):
            indic_div = div
            break

    out: Dict[str, List[str]] = {}
    if not indic_div:
        return out

    headers = indic_div.select("h4.list-header")
    if headers:
        for h in headers:
            header_txt = norm_space(h.get_text(" ", strip=True)).lower()
            pops = []

            if "adult" in header_txt or "volwassene" in header_txt:
                pops = ["ADULTS", "ELDERLY"]
            elif any(k in header_txt for k in ("children", "child", "kind", "adolescent", "jongere")):
                pops.append("CHILDREN")
            elif any(k in header_txt for k in ("infant", "baby", "zuigeling", "neonate")):
                pops.append("INFANTS")
            else:
                pops = ["ADULTS", "ELDERLY"]

            ul = h.find_next("ul")
            if not ul:
                continue
            items = [norm_space(li.get_text(" ", strip=True)) for li in ul.select("li")]
            items = [it for it in items if it]
            if items:
                for p in pops:
                    out.setdefault(p, []).extend(items)
    else:
        all_lis = indic_div.select("ul li")
        if not all_lis:
            paras = [norm_space(p.get_text(" ", strip=True))
                     for p in indic_div.find_all("p") if p.get_text(strip=True)]
            items = paras if paras else []
        else:
            items = [norm_space(li.get_text(" ", strip=True)) for li in all_lis]
            items = [it for it in items if it]

        if items:
            out["ADULTS"] = items
            out["ELDERLY"] = items.copy()

    # De-duplicate preserving order
    for pop in out:
        seen = set()
        dedup = []
        for it in out[pop]:
            key = it.lower()
            if key not in seen:
                seen.add(key)
                dedup.append(it)
        out[pop] = dedup

    return out


def derive_reimbursement_rows(html_text: str, url: str) -> List[Dict]:
    """Parse a detail page into reimbursement row dicts (raw Dutch, no translation)."""
    soup = BeautifulSoup(html_text, "lxml")
    all_variants = parse_all_compositions(soup)
    indications = parse_indications_by_population(soup)

    rows: List[Dict] = []
    for core in all_variants:
        reimb_text = "Reimbursed" if core.reimbursement_status == "REIMBURSED" else "Not Reimbursed"

        if not indications:
            for st in (core.strengths or [""]):
                rows.append({
                    "generic_name": core.generic_name,
                    "brand_name": core.brand_name,
                    "manufacturer": normalize_company(core.manufacturer),
                    "dosage_form": core.dosage_form,
                    "strength": st,
                    "patient_population": "",
                    "indication_nl": "",
                    "reimbursement_status": core.reimbursement_status,
                    "reimbursable_text": reimb_text,
                    "route_of_administration": route_from_dosage_form(core.dosage_form),
                    "pack_details": pack_details(core.brand_name or core.generic_name, core.dosage_form, st),
                    "binding": "NO",
                    "reimbursement_body": "MINISTRY OF HEALTH",
                    "source_url": url,
                })
            continue

        for pop, indic_list in indications.items():
            full_indic = " ; ".join(indic_list)
            for st in (core.strengths or [""]):
                rows.append({
                    "generic_name": core.generic_name,
                    "brand_name": core.brand_name,
                    "manufacturer": normalize_company(core.manufacturer),
                    "dosage_form": core.dosage_form,
                    "strength": st,
                    "patient_population": pop,
                    "indication_nl": full_indic,
                    "reimbursement_status": core.reimbursement_status,
                    "reimbursable_text": reimb_text,
                    "route_of_administration": route_from_dosage_form(core.dosage_form),
                    "pack_details": pack_details(core.brand_name or core.generic_name, core.dosage_form, st),
                    "binding": "NO",
                    "reimbursement_body": "MINISTRY OF HEALTH",
                    "source_url": url,
                })

    return rows


# ---------------------------------------------------------------
# Async scraper
# ---------------------------------------------------------------

async def scrape_fk_details(repo: NetherlandsRepository) -> int:
    """Scrape FK detail pages using httpx + asyncio.Queue worker pool."""

    max_workers = getenv_int("FK_SCRAPE_WORKERS", 10)
    batch_size = getenv_int("FK_BATCH_SIZE", 100)
    max_retries = getenv_int("FK_MAX_RETRIES", 3)
    sleep_between = getenv_float("FK_SLEEP_BETWEEN", 0.15)

    pending_urls = repo.get_pending_fk_urls(limit=50000)
    retryable_urls = repo.get_retryable_fk_urls(max_retries=max_retries)
    all_urls = pending_urls + retryable_urls

    if not all_urls:
        log.info("No pending FK URLs to scrape")
        return 0

    log.info(f"Scraping {len(all_urls)} FK URLs ({len(pending_urls)} pending, {len(retryable_urls)} retry) with {max_workers} workers")

    url_queue: asyncio.Queue = asyncio.Queue()
    for u in all_urls:
        await url_queue.put(u)

    buffer: List[Dict] = []
    buffer_lock = asyncio.Lock()
    stats = {"completed": 0, "failed": 0, "rows": 0}
    stats_lock = asyncio.Lock()
    total = len(all_urls)

    async with httpx.AsyncClient(
        headers=DEFAULT_HEADERS,
        timeout=40.0,
        follow_redirects=True,
        limits=httpx.Limits(max_connections=max_workers + 5, max_keepalive_connections=max_workers),
    ) as client:

        async def worker(wid: int) -> None:
            while True:
                try:
                    url_rec = url_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                url = url_rec["url"]
                url_id = url_rec["id"]
                result_rows = None
                t_start = time.monotonic()
                status_code = None
                resp_bytes = None
                req_error = None

                for attempt in range(2):
                    try:
                        resp = await client.get(url)
                        resp.raise_for_status()
                        status_code = resp.status_code
                        resp_bytes = len(resp.content)
                        result_rows = await asyncio.to_thread(derive_reimbursement_rows, resp.text, url)
                        break
                    except Exception as e:
                        req_error = str(e)[:500]
                        if attempt == 0:
                            await asyncio.sleep(2 + random.uniform(0, 1))
                        else:
                            repo.mark_fk_url_status(url_id, "failed", str(e)[:500])
                            async with stats_lock:
                                stats["failed"] += 1
                            log.debug(f"Worker {wid}: failed {url}: {e}")

                # Log HTTP request to shared table
                elapsed_ms = (time.monotonic() - t_start) * 1000
                try:
                    await asyncio.to_thread(
                        repo.log_request,
                        url, "GET", status_code, resp_bytes, elapsed_ms, req_error
                    )
                except Exception:
                    pass  # HTTP logging is best-effort

                if result_rows is not None:
                    for row in result_rows:
                        row["fk_url_id"] = url_id

                    repo.mark_fk_url_status(url_id, "success")

                    async with buffer_lock:
                        buffer.extend(result_rows)

                    async with stats_lock:
                        stats["completed"] += 1
                        stats["rows"] += len(result_rows)

                    # Flush buffer
                    async with buffer_lock:
                        if len(buffer) >= batch_size:
                            batch = buffer.copy()
                            buffer.clear()
                        else:
                            batch = None
                    if batch:
                        await asyncio.to_thread(repo.insert_fk_reimbursement_batch, batch)

                    # Progress logging every 10 items
                    async with stats_lock:
                        done = stats["completed"] + stats["failed"]
                    if done % 10 == 0 or done == total:
                        pct = (done / total * 100) if total else 0
                        print(f"[PROGRESS] FK Scraping: {done}/{total} ({pct:.1f}%) - {stats['rows']} rows", flush=True)

                await asyncio.sleep(sleep_between + random.uniform(0, 0.1))

        tasks = [asyncio.create_task(worker(i)) for i in range(max_workers)]
        await asyncio.gather(*tasks)

    # Flush remaining buffer
    if buffer:
        repo.insert_fk_reimbursement_batch(buffer)

    log.info(
        f"FK scraping complete: {stats['completed']} success, {stats['failed']} failed, "
        f"{stats['rows']} reimbursement rows"
    )
    return stats["rows"]


# ---------------------------------------------------------------
# Run ID / Main
# ---------------------------------------------------------------

def _get_run_id() -> str:
    run_id = os.environ.get("NL_RUN_ID", "").strip()
    if run_id:
        return run_id
    run_id_file = get_output_dir() / ".current_run_id"
    if run_id_file.exists():
        try:
            return run_id_file.read_text(encoding="utf-8").strip()
        except Exception:
            pass
    return ""


def main() -> None:
    run_id = _get_run_id()
    if not run_id:
        log.error("No run_id found. Run pipeline with --fresh first.")
        raise SystemExit(1)

    log.info(f"Step {STEP_NUMBER}: {STEP_NAME} | run_id={run_id}")

    db = get_db("Netherlands")
    apply_netherlands_schema(db)
    repo = NetherlandsRepository(db, run_id)

    # Resume check
    url_stats = repo.get_fk_url_stats()
    if url_stats["total"] == 0:
        log.error("No FK URLs found. Run Step 2 (02_fk_collect_urls.py) first.")
        raise SystemExit(1)

    if url_stats["pending"] == 0 and repo.get_retryable_fk_urls() == []:
        log.info(
            f"All FK URLs already processed "
            f"({url_stats['success']} success, {url_stats['failed']} failed). Skipping."
        )
        return

    rows_inserted = asyncio.run(scrape_fk_details(repo))
    log.info(f"Total reimbursement rows in DB: {repo.get_fk_reimbursement_count()}")


if __name__ == "__main__":
    run_with_checkpoint(main, SCRIPT_ID, STEP_NUMBER, STEP_NAME)
