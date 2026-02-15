#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
North Macedonia URL Collector - httpx + lxml (Fast)
====================================================
Replaces Selenium-based 01_collect_urls.py with pure HTTP scraping.

The Telerik grid pager URLs work with plain HTTP requests — each page
returns 10 rows by default. With ~4100 records that's ~411 pages, but
async httpx can fetch them all concurrently (20+ workers), making it
significantly faster than the 3-thread Selenium version.

Old Selenium version archived at: archive/01_collect_urls_selenium.py.bak
"""

import asyncio
import json
import math
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Set

import httpx
from lxml import html as lxml_html

# --------------------------------------------------
# PATH SETUP
# --------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
_repo_root = Path(__file__).resolve().parents[2]

if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

try:
    from config_loader import load_env_file, get_output_dir, getenv, getenv_bool, getenv_int, getenv_float
    load_env_file()
    OUTPUT_DIR = get_output_dir()
    USE_CONFIG = True
except ImportError:
    OUTPUT_DIR = SCRIPT_DIR
    USE_CONFIG = False

    def getenv(key: str, default: str = None) -> str:
        return os.getenv(key, default if default is not None else "")

    def getenv_bool(key: str, default: bool = False) -> bool:
        return str(os.getenv(key, str(default))).lower() in ("1", "true", "yes", "on")

    def getenv_int(key: str, default: int = 0) -> int:
        try:
            return int(os.getenv(key, str(default)))
        except (ValueError, TypeError):
            return default

    def getenv_float(key: str, default: float = 0.0) -> float:
        try:
            return float(os.getenv(key, str(default)))
        except (ValueError, TypeError):
            return default

# Import Telegram notifier
try:
    from core.telegram_notifier import TelegramNotifier
    TELEGRAM_NOTIFIER_AVAILABLE = True
except ImportError:
    TELEGRAM_NOTIFIER_AVAILABLE = False
    TelegramNotifier = None

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------
BASE_URL = "https://lekovi.zdravstvo.gov.mk/drugsregister/overview"
PAGER_URL_TEMPLATE = "https://lekovi.zdravstvo.gov.mk/drugsregister.grid.pager/{page}/grid_0?t:ac=overview"
ROWS_PER_PAGE = 10  # Server default, cannot be changed via URL alone

URLS_CSV = getenv("SCRIPT_01_URLS_CSV", "north_macedonia_detail_urls.csv")
CHECKPOINT_JSON = getenv("SCRIPT_01_CHECKPOINT_JSON", "mk_urls_checkpoint.json")
TOTAL_PAGES_OVERRIDE = getenv("SCRIPT_01_TOTAL_PAGES", "")

MAX_WORKERS = getenv_int("SCRIPT_01_NUM_WORKERS", 20)
MAX_RETRIES = getenv_int("SCRIPT_01_MAX_RETRIES_PER_PAGE", 3)
SLEEP_BETWEEN = getenv_float("SCRIPT_01_SLEEP_BETWEEN", 0.05)
REQUEST_TIMEOUT = getenv_float("SCRIPT_01_REQUEST_TIMEOUT", 30.0)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "mk-MK,mk;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
}


# --------------------------------------------------
# CHECKPOINT
# --------------------------------------------------
def read_checkpoint() -> Dict:
    checkpoint_path = OUTPUT_DIR / CHECKPOINT_JSON
    default = {
        "page": 1,
        "total_pages": 0,
        "pages": {},
        "failed_pages": [],
    }
    if checkpoint_path.exists():
        try:
            with open(checkpoint_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                for key in ("page", "total_pages", "pages", "failed_pages"):
                    if key not in data:
                        data[key] = default[key]
                data["page"] = int(data.get("page", 1))
                return data
        except Exception:
            pass
    return default


def write_checkpoint(total_pages: int, pages_info: dict, failed_pages: list) -> None:
    checkpoint_path = OUTPUT_DIR / CHECKPOINT_JSON
    last_page = max((int(p) for p in pages_info if pages_info[p].get("status") == "complete"), default=0)
    payload = {
        "page": last_page,
        "total_pages": total_pages,
        "pages": pages_info,
        "failed_pages": failed_pages,
    }
    with open(checkpoint_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


# --------------------------------------------------
# CSV HELPERS
# --------------------------------------------------
CSV_COLUMNS = ["detail_url", "page_num", "detailed_view_scraped"]


def load_existing_urls(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    try:
        import csv
        with open(path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            return {row["detail_url"] for row in reader if row.get("detail_url")}
    except Exception:
        return set()


def ensure_csv_header(path: Path) -> None:
    if not path.exists():
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            import csv
            csv.writer(f).writerow(CSV_COLUMNS)


def append_urls_to_csv(path: Path, rows: List[Dict]) -> None:
    if not rows:
        return
    import csv
    with open(path, "a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writerows(rows)


# --------------------------------------------------
# HTML PARSING
# --------------------------------------------------
def parse_total_records(html_text: str) -> Optional[int]:
    """Parse total record count from pager text like '1-10 од 4102'."""
    m = re.search(r"(\d+)-(\d+)\s+(?:од|of)\s+(\d+)", html_text, re.IGNORECASE)
    if m:
        return int(m.group(3))
    return None


def parse_detail_urls(html_text: str) -> List[str]:
    """Extract detail URLs from grid page HTML."""
    try:
        doc = lxml_html.fromstring(html_text)
    except Exception:
        return []
    links = doc.cssselect("td.latinName a")
    urls = []
    for a in links:
        href = a.get("href", "")
        if href and "detaileddrug" in href:
            # Normalize to full URL if relative
            if href.startswith("/"):
                href = f"https://lekovi.zdravstvo.gov.mk{href}"
            urls.append(href)
    return urls


# --------------------------------------------------
# ASYNC SCRAPER
# --------------------------------------------------
async def fetch_page(
    client: httpx.AsyncClient,
    page_num: int,
    semaphore: asyncio.Semaphore,
) -> tuple:
    """Fetch a single pager page and extract detail URLs.

    Returns (page_num, urls_list, error_string_or_None).
    """
    url = PAGER_URL_TEMPLATE.format(page=page_num)
    last_err = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with semaphore:
                resp = await client.get(url)
                resp.raise_for_status()

            urls = parse_detail_urls(resp.text)
            if urls:
                return (page_num, urls, None)

            # Page returned 0 URLs — might be a transient issue
            if attempt < MAX_RETRIES:
                await asyncio.sleep(1.0)
                continue
            return (page_num, [], f"0 URLs extracted after {MAX_RETRIES} attempts")

        except Exception as e:
            last_err = str(e)
            if attempt < MAX_RETRIES:
                await asyncio.sleep(2.0 * attempt)

    return (page_num, [], last_err)


async def collect_urls_async() -> None:
    """Main async URL collection workflow."""
    urls_path = OUTPUT_DIR / URLS_CSV
    ensure_csv_header(urls_path)

    # Load existing state
    checkpoint = read_checkpoint()
    seen_urls: Set[str] = load_existing_urls(urls_path)
    pages_info = checkpoint.get("pages", {})
    failed_pages_list = checkpoint.get("failed_pages", [])

    # Init Telegram notifier
    telegram_notifier = None
    if TELEGRAM_NOTIFIER_AVAILABLE:
        try:
            telegram_notifier = TelegramNotifier("NorthMacedonia", rate_limit=30.0)
            if telegram_notifier and telegram_notifier.enabled:
                telegram_notifier.send_started("Collect URLs (Fast) - Step 1")
                print("[INFO] Telegram notifications enabled", flush=True)
        except Exception:
            telegram_notifier = None

    print("\n" + "=" * 60, flush=True)
    print("URL COLLECTION - FAST httpx VERSION", flush=True)
    print("=" * 60, flush=True)

    # ---- Step 1: Determine total pages ----
    print("\n[INIT] Fetching page 1 to determine total records...", flush=True)

    async with httpx.AsyncClient(
        headers=HEADERS,
        timeout=REQUEST_TIMEOUT,
        follow_redirects=True,
        limits=httpx.Limits(max_connections=MAX_WORKERS + 5, max_keepalive_connections=MAX_WORKERS),
    ) as client:

        resp = await client.get(BASE_URL)
        resp.raise_for_status()

        total_records = parse_total_records(resp.text)
        if not total_records:
            print("[ERROR] Could not determine total records from page 1", flush=True)
            return

        total_pages = math.ceil(total_records / ROWS_PER_PAGE)

        if TOTAL_PAGES_OVERRIDE:
            try:
                total_pages = int(TOTAL_PAGES_OVERRIDE)
            except ValueError:
                pass

        print(f"[INIT] Total records: {total_records}, Pages: {total_pages} (@ {ROWS_PER_PAGE}/page)", flush=True)

        # Also grab page 1 URLs
        page1_urls = parse_detail_urls(resp.text)
        if page1_urls:
            new_p1 = [u for u in page1_urls if u not in seen_urls]
            if new_p1:
                rows = [{"detail_url": u, "page_num": 1, "detailed_view_scraped": "no"} for u in new_p1]
                append_urls_to_csv(urls_path, rows)
                seen_urls.update(new_p1)
            pages_info["1"] = {"status": "complete", "urls_extracted": len(page1_urls), "new_urls": len(new_p1)}

        # ---- Step 2: Build page list ----
        all_pages = list(range(1, total_pages + 1))
        completed = {int(p) for p, info in pages_info.items() if info.get("status") == "complete"}
        pending = [p for p in all_pages if p not in completed]

        # Add failed pages for retry
        pages_to_process = sorted(set(pending + failed_pages_list))

        print(f"\n[STATUS] Pages completed: {len(completed)}", flush=True)
        print(f"[STATUS] Pages failed: {len(failed_pages_list)}", flush=True)
        print(f"[STATUS] Pages pending: {len(pages_to_process)}", flush=True)
        print(f"[STATUS] Workers: {MAX_WORKERS}", flush=True)

        if not pages_to_process:
            print("\n[COMPLETE] All pages already scraped!", flush=True)
            if telegram_notifier:
                try:
                    telegram_notifier.send_success(
                        "URL Collection Already Complete",
                        details=f"Total URLs: {len(seen_urls)}\nAll {total_pages} pages complete",
                    )
                except Exception:
                    pass
            return

        print(f"\n[START] Processing {len(pages_to_process)} pages with {MAX_WORKERS} async workers...\n", flush=True)

        # ---- Step 3: Fetch all pages concurrently ----
        semaphore = asyncio.Semaphore(MAX_WORKERS)
        tasks = [fetch_page(client, p, semaphore) for p in pages_to_process]

        done_count = 0
        new_url_count = 0
        new_failed: List[int] = []
        batch_rows: List[Dict] = []
        batch_size = 50  # Write to CSV every N pages

        t_start = time.time()

        for coro in asyncio.as_completed(tasks):
            page_num, urls, err = await coro
            done_count += 1

            if err:
                pages_info[str(page_num)] = {"status": "failed", "error": err}
                new_failed.append(page_num)
                print(f"  [FAIL] Page {page_num}: {err}", flush=True)
            else:
                new_urls = [u for u in urls if u not in seen_urls]
                seen_urls.update(new_urls)
                new_url_count += len(new_urls)

                pages_info[str(page_num)] = {
                    "status": "complete",
                    "urls_extracted": len(urls),
                    "new_urls": len(new_urls),
                }

                # Remove from failed list if it was retried
                if page_num in failed_pages_list:
                    failed_pages_list.remove(page_num)

                if new_urls:
                    batch_rows.extend(
                        {"detail_url": u, "page_num": page_num, "detailed_view_scraped": "no"}
                        for u in new_urls
                    )

            # Flush batch to CSV periodically
            if len(batch_rows) >= batch_size * ROWS_PER_PAGE or done_count == len(pages_to_process):
                if batch_rows:
                    append_urls_to_csv(urls_path, batch_rows)
                    batch_rows.clear()

                # Update checkpoint
                write_checkpoint(total_pages, pages_info, new_failed + [p for p in failed_pages_list if p not in new_failed])

            # Progress
            if done_count % 50 == 0 or done_count == len(pages_to_process):
                elapsed = time.time() - t_start
                pct = done_count * 100.0 / len(pages_to_process)
                rate = done_count / elapsed if elapsed > 0 else 0
                eta = (len(pages_to_process) - done_count) / rate if rate > 0 else 0
                print(
                    f"[PROGRESS] {done_count}/{len(pages_to_process)} ({pct:.1f}%) "
                    f"- {new_url_count} new URLs - {rate:.1f} pages/s - ETA {eta:.0f}s",
                    flush=True,
                )

    # Flush remaining rows
    if batch_rows:
        append_urls_to_csv(urls_path, batch_rows)

    # Final checkpoint
    final_failed = sorted(set(new_failed + [p for p in failed_pages_list if str(p) not in pages_info or pages_info[str(p)].get("status") != "complete"]))
    write_checkpoint(total_pages, pages_info, final_failed)

    # ---- Step 4: Write to DB ----
    repo = None
    try:
        run_id = os.environ.get("NORTH_MACEDONIA_RUN_ID", "")
        if run_id:
            from core.db.connection import CountryDB
            from db.repositories import NorthMacedoniaRepository
            db = CountryDB("NorthMacedonia")
            repo = NorthMacedoniaRepository(db, run_id)
            repo.ensure_run_in_ledger(mode="resume")

            # Bulk insert all URLs to DB
            db_rows = [{"detail_url": u, "page_num": 0, "status": "pending"} for u in seen_urls]
            if db_rows:
                # Insert in batches to avoid huge transactions
                batch = 500
                inserted = 0
                for i in range(0, len(db_rows), batch):
                    chunk = db_rows[i : i + batch]
                    try:
                        repo.insert_urls(chunk)
                        inserted += len(chunk)
                    except Exception as e:
                        print(f"[DB WARN] Batch insert error (offset {i}): {e}", flush=True)
                print(f"[DB] Inserted {inserted} URLs into nm_urls", flush=True)
    except Exception as e:
        print(f"[DB] Not available: {e} (CSV-only mode)", flush=True)

    # ---- Summary ----
    complete_count = sum(1 for info in pages_info.values() if info.get("status") == "complete")
    elapsed_total = time.time() - t_start

    print(f"\n{'=' * 60}", flush=True)
    print("URL COLLECTION COMPLETED", flush=True)
    print(f"{'=' * 60}", flush=True)
    print(f"Total unique detail URLs: {len(seen_urls)}", flush=True)
    print(f"New URLs added: {new_url_count}", flush=True)
    print(f"Completed pages: {complete_count}/{total_pages}", flush=True)
    print(f"Failed pages: {len(final_failed)}", flush=True)
    print(f"Time: {elapsed_total:.1f}s ({complete_count / elapsed_total:.1f} pages/s)", flush=True)

    if final_failed:
        print(f"\n[WARNING] {len(final_failed)} pages failed:", flush=True)
        print(f"  Pages: {final_failed[:20]}{'...' if len(final_failed) > 20 else ''}", flush=True)
        print("  Run again to retry failed pages.", flush=True)
        if telegram_notifier:
            try:
                telegram_notifier.send_warning(
                    "URL Collection Completed with Issues",
                    details=f"Total URLs: {len(seen_urls)}\nNew URLs: {new_url_count}\nFailed pages: {len(final_failed)}",
                )
            except Exception:
                pass
    else:
        print("\n[SUCCESS] All pages extracted successfully.", flush=True)
        if telegram_notifier:
            try:
                telegram_notifier.send_success(
                    "URL Collection Completed",
                    details=f"Total URLs: {len(seen_urls)}\nNew URLs: {new_url_count}\nPages: {complete_count}/{total_pages}\nTime: {elapsed_total:.1f}s",
                )
            except Exception:
                pass

    print("=" * 60 + "\n", flush=True)


# --------------------------------------------------
# ENTRY POINT
# --------------------------------------------------
def main():
    asyncio.run(collect_urls_async())


if __name__ == "__main__":
    main()
