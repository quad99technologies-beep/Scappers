
#!/usr/bin/env python3
"""
Step 1: List Determinas
Fetches ALL DET and Riduzione records from AIFA by iterating month-by-month
from 2002 to today.

PROGRESS TRACKING (fully resumable)
-------------------------------------
Every (keyword, year, month) combination is a single progress_key stored in
it_step_progress.  On resume the step:
  1. Prints a summary of what is already done (months completed, records fetched).
  2. Skips any month-window already marked 'completed'.
  3. Retries month-windows previously marked 'failed'.
  4. Tracks records_fetched per window in the DB.
  5. Prints a final summary when the keyword finishes.

DATE FILTER PARAMS (confirmed working)
---------------------------------------
  dataPubblicazioneDa = "2024-01-01T00:00:00.000Z"
  dataPubblicazioneA  = "2024-01-31T23:59:59.999Z"

API NOTE
--------
  totalElementNum     -> ALWAYS capped at 100 (ignore for pagination)
  elementAvailableNum -> REAL count for the window  (use this)
"""
import os
import sys
import time
import logging
from pathlib import Path
from datetime import datetime
import calendar
import requests

# Path setup
_repo_root = Path(__file__).resolve().parents[3]
_italy_dir  = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_repo_root))
sys.path.insert(0, str(_italy_dir))

from core.db.connection import CountryDB
from db.repositories import ItalyRepository

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_URL   = "https://trovanorme.aifa.gov.it/tnf-service/ricerca/"
HEADERS    = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":     "application/json, text/plain, */*",
    "Referer":    "https://trovanorme.aifa.gov.it/",
}

PAGE_SIZE  = 20
def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)).strip())
    except Exception:
        return default

START_YEAR = _env_int("ITALY_START_YEAR", 1990)
STEP_NUM   = 1
STEP_NAME  = "list_determinas"


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def month_window(year: int, month: int):
    last_day  = calendar.monthrange(year, month)[1]
    date_from = f"{year}-{month:02d}-01T00:00:00.000Z"
    date_to   = f"{year}-{month:02d}-{last_day:02d}T23:59:59.999Z"
    return date_from, date_to


def pkey(keyword: str, year: int, month: int) -> str:
    return f"{keyword}:{year}-{month:02d}"


def get_page(keyword: str, page_num: int, date_from: str, date_to: str):
    """Fetch one page. Retries up to 4 times with exponential back-off."""
    params = {
        "pageSize":                         PAGE_SIZE,
        "totalElementNum":                  0,
        "pageNum":                          page_num,
        "sortColumn":                       "dataPubblicazione",
        "determinaGUSource":                "true",
        "determinaTNFSource":               "true",
        "documentoAIFASource":              "true",
        "modificheSecondarieFarmaciSource": "true",
        "newsSource":                       "true",
        "tutti":                            "true",
        "parola":                           keyword,
        "dataPubblicazioneDa":              date_from,
        "dataPubblicazioneA":               date_to,
    }
    for attempt in range(4):
        try:
            r = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=60)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            wait = 10 * (attempt + 1)
            logger.warning(f"    [{keyword}] {date_from[:7]} p{page_num} "
                           f"attempt {attempt+1} failed: {e}. Retry in {wait}s")
            time.sleep(wait)
    return None


def fetch_month(keyword: str, year: int, month: int):
    """
    Paginate ALL results for a keyword within one calendar month.
    Returns list of items, or None on total failure.
    """
    date_from, date_to = month_window(year, month)
    all_items: list = []
    real_total = 0
    page_num = 0

    while True:
        data = get_page(keyword, page_num, date_from, date_to)
        if data is None:
            return None, 0          # Total failure

        items = data.get("resourceList", [])
        if not items:
            break

        all_items.extend(items)

        # elementAvailableNum = real total (totalElementNum is capped at 100)
        real_total = data.get("elementAvailableNum") or data.get("totalElementNum", 0)

        if len(all_items) >= real_total or len(items) < PAGE_SIZE:
            break                # Got everything or last partial page

        page_num += 1
        time.sleep(0.3)

    return all_items, real_total


# ─────────────────────────────────────────────
# Progress display helpers
# ─────────────────────────────────────────────

def print_resume_summary(repo: ItalyRepository):
    """Print a table of what is already done before resuming."""
    s = repo.get_step1_summary()
    total_months = s["completed"] + s["failed"] + s["in_progress"]
    if total_months == 0:
        logger.info("  Fresh run - no prior progress found.")
        return

    logger.info(f"  ┌─ Resume summary ───────────────────────────────")
    logger.info(f"  │  Months completed  : {s['completed']}")
    logger.info(f"  │  Months failed     : {s['failed']}")
    logger.info(f"  │  Months in-progress: {s['in_progress']}")
    logger.info(f"  │  Records saved     : {s['total_saved']}")
    logger.info(f"  │  Records found     : {s['total_found']}")
    for kw, kd in sorted(s["by_keyword"].items()):
        logger.info(f"  │    {kw:12s}  done={kd.get('completed',0):4d}  "
                    f"fail={kd.get('failed',0):3d}  "
                    f"saved={kd.get('total_saved',0):6d}  "
                    f"found={kd.get('total_found',0):6d}")
    logger.info(f"  └─────────────────────────────────────────────────")


def print_keyword_summary(keyword: str, repo: ItalyRepository):
    """Print per-keyword totals after finishing that keyword."""
    s = repo.get_step1_summary()
    kd = s["by_keyword"].get(keyword, {})
    logger.info(f"\n  ╔═ {keyword} FINISHED ══════════════════════════════")
    logger.info(f"  ║  Months completed  : {kd.get('completed', 0)}")
    logger.info(f"  ║  Months failed     : {kd.get('failed', 0)}")
    logger.info(f"  ║  Total saved       : {kd.get('total_saved', 0)}")
    logger.info(f"  ║  Total found       : {kd.get('total_found', 0)}")
    logger.info(f"  ╚══════════════════════════════════════════════════\n")


def print_final_summary(repo: ItalyRepository):
    """Print grand-total summary at completion."""
    s = repo.get_step1_summary()
    logger.info(f"\n  ╔═ STEP 1 COMPLETE ════════════════════════════════")
    logger.info(f"  ║  Total months completed : {s['completed']}")
    logger.info(f"  ║  Total months failed    : {s['failed']}")
    logger.info(f"  ║  Total saved            : {s['total_saved']}")
    logger.info(f"  ║  Total found            : {s['total_found']}")
    for kw, kd in sorted(s["by_keyword"].items()):
        logger.info(f"  ║    {kw:12s}  months={kd.get('completed',0):4d}  "
                    f"saved={kd.get('total_saved',0):6d}  "
                    f"found={kd.get('total_found',0):6d}")
    logger.info(f"  ╚══════════════════════════════════════════════════\n")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    run_id = os.environ.get("ITALY_RUN_ID", "manual_run")
    db     = CountryDB("Italy")
    repo   = ItalyRepository(db, run_id)

    # Ensure run is registered (FK guard)
    repo.ensure_run_in_ledger()

    logger.info(f"Starting Step 1: List Determinas  (RunID: {run_id})")

    # ── Print resume summary ───────────────────────────────────────────
    print_resume_summary(repo)

    # ── Load completed keys for fast O(1) skip ─────────────────────────
    completed_keys = set(repo.get_completed_keys(STEP_NUM))
    if completed_keys:
        logger.info(f"  {len(completed_keys)} month-windows already completed - will skip them.")

    now = datetime.now()
    raw_kw = os.environ.get("ITALY_KEYWORDS", "").strip()
    keywords = [k.strip() for k in raw_kw.split(",") if k.strip()] if raw_kw else ["DET", "Riduzione"]

    for kw in keywords:
        logger.info(f"\n{'='*60}")
        logger.info(f"Keyword: {kw}  |  {START_YEAR} -> {now.year}")
        logger.info(f"{'='*60}")

        kw_new_records = 0
        kw_new_months  = 0
        kw_skipped     = 0
        kw_failed      = 0

        for year in range(START_YEAR, now.year + 1):
            for month in range(1, 13):
                if year == now.year and month > now.month:
                    break

                pk = pkey(kw, year, month)

                # ── Skip already-done windows ──────────────────────────
                if pk in completed_keys:
                    kw_skipped += 1
                    continue

                # ── Fetch month ────────────────────────────────────────
                # ── Fetch month ────────────────────────────────────────
                try:
                    repo.mark_month_progress(STEP_NUM, STEP_NAME, pk, "in_progress")

                    items, total_found = fetch_month(kw, year, month)

                    if items is None:
                        # Total failure after all retries
                        repo.mark_month_progress(
                            STEP_NUM, STEP_NAME, pk, "failed",
                            records_fetched=0,
                            api_total_count=0,
                            error_message="All retries exhausted"
                        )
                        kw_failed += 1
                        logger.error(f"  FAILED {pk} - will retry on next resume")
                        continue

                    # ── Insert into DB ─────────────────────────────────────
                    if items:
                        repo.insert_determinas(items, source_keyword=kw)
                        kw_new_records += len(items)
                        kw_new_months  += 1

                    # ── Mark completed with record count ───────────────────
                    # Pass the API total we found, plus the actual saved count
                    repo.mark_month_progress(
                        STEP_NUM, STEP_NAME, pk, "completed",
                        records_fetched=len(items) if items else 0,
                        api_total_count=total_found
                    )

                    if items:
                        logger.info(f"  {pk}: Found {total_found}, Saved {len(items)}  "
                                    f"[Total saved this run: {kw_new_records}]")

                    time.sleep(0.5)

                except Exception as e:
                    import traceback
                    logger.error(f"CRASH processing {pk}: {e}")
                    logger.error(traceback.format_exc())
                    # Mark as failed in DB so we can resume later
                    try:
                        repo.mark_month_progress(
                            STEP_NUM, STEP_NAME, pk, "failed",
                            records_fetched=0,
                            error_message=f"Crash: {str(e)}"
                        )
                    except:
                        pass
                    raise  # Re-raise to stop execution


        logger.info(
            f"  {kw} scan done - "
            f"new months={kw_new_months}  new records={kw_new_records}  "
            f"skipped={kw_skipped}  failed={kw_failed}"
        )
        print_keyword_summary(kw, repo)
        try:
            repo.refresh_step1_keyword_stats(kw)
        except Exception as e:
            logger.warning("Could not refresh Step 1 stats for %s: %s", kw, e)

    print_final_summary(repo)
    logger.info("Step 1 Complete.")


if __name__ == "__main__":
    main()
