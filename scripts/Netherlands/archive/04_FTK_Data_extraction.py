# nl_ftk_all_urls_cost_per_day_only.py
# End-to-end:
#   Phase 1: Open alphabet listing -> expand all -> collect ALL detail URLs
#   Phase 2: Visit each detail URL -> expand "Kosten" -> parse ONLY per-day (day=1)
#
# Install:
#   pip install playwright beautifulsoup4 lxml
#   playwright install
#
# Run (default alphabet page = H):
#   python nl_ftk_all_urls_cost_per_day_only.py
#
# Run (choose another letter page URL):
#   python nl_ftk_all_urls_cost_per_day_only.py "https://www.farmacotherapeutischkompas.nl/bladeren/preparaatteksten/alfabet/a#medicine-listing"
#
from __future__ import annotations

import csv
import json
import re
import sys
import time
import threading
import queue
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, Error as PWError

# -----------------------------
# Config
# -----------------------------
BASE = "https://www.farmacotherapeutischkompas.nl"

DEFAULT_START_URL = (
    "https://www.farmacotherapeutischkompas.nl/bladeren/preparaatteksten/alfabet/h#medicine-listing"
)

HEADLESS = True
SLOW_MO_MS = 50
TIMEOUT_MS = 60_000

NETWORK_RETRY_MAX = 3
NETWORK_RETRY_DELAY = 3  # seconds

DATA_LOAD_WAIT_SECONDS = 2
WORKERS = 3  # multi-thread scraping workers

OUT_DIR = Path("output_nl_cost_per_day_only")
OUT_DIR.mkdir(parents=True, exist_ok=True)

ALL_URLS_JSON = OUT_DIR / "all_detail_urls.json"
OUT_CSV = OUT_DIR / "nl_cost_per_day_only.csv"


# -----------------------------
# Utils
# -----------------------------
def _to_float(x: str) -> Optional[float]:
    if x is None:
        return None
    s = str(x).strip().replace("\xa0", " ").replace(",", ".")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _first_number_eur(text: str) -> Optional[float]:
    if not text:
        return None
    s = text.replace("\xa0", " ").replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    return _to_float(m.group(1)) if m else None


def split_brand_full(brand_full: str) -> Dict[str, str]:
    # Split on FIRST comma
    if not brand_full:
        return {"brand_name": "", "pack_presentation": ""}
    s = brand_full.strip()
    parts = re.split(r"\s*,\s*", s, maxsplit=1)
    return {
        "brand_name": parts[0].strip() if parts else "",
        "pack_presentation": parts[1].strip() if len(parts) > 1 else "",
    }


def ensure_csv_headers(path: Path, fieldnames: List[str]) -> None:
    if not path.exists():
        with path.open("w", encoding="utf-8", newline="") as f:
            csv.DictWriter(f, fieldnames=fieldnames).writeheader()


def append_rows_threadsafe(path: Path, fieldnames: List[str], rows: List[Dict], lock: threading.Lock) -> None:
    if not rows:
        return
    with lock:
        with path.open("a", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            for r in rows:
                w.writerow({k: r.get(k) for k in fieldnames})


def goto_with_retry(page, url: str) -> None:
    last_err = None
    for attempt in range(1, NETWORK_RETRY_MAX + 1):
        try:
            page.goto(url, wait_until="domcontentloaded")
            return
        except (PWTimeoutError, PWError) as e:
            last_err = e
            if attempt < NETWORK_RETRY_MAX:
                time.sleep(NETWORK_RETRY_DELAY * attempt)
            else:
                raise last_err


# -----------------------------
# Phase 1: Expand all + collect URLs
# -----------------------------
def expand_all_listing(page) -> int:
    """
    On the alphabet listing page, expand blocks:
      .block-title.collapsible-closed
    """
    page.wait_for_load_state("domcontentloaded")
    expanded = 0
    max_rounds = 120

    for _ in range(max_rounds):
        blocks = page.locator(".block-title.collapsible-closed")
        count = blocks.count()
        if count == 0:
            break

        for i in range(count):
            try:
                blk = blocks.nth(i)
                blk.scroll_into_view_if_needed(timeout=2000)
                blk.click(timeout=2000)
                page.wait_for_timeout(250)
                expanded += 1
            except Exception:
                pass

        page.wait_for_timeout(400)

    return expanded


def collect_detail_links(page) -> List[str]:
    """
    Collect all hrefs from:
      a.medicine[href*='/bladeren/preparaatteksten/']
    """
    page.wait_for_timeout(1000)
    medicine_links = page.locator("a.medicine[href*='/bladeren/preparaatteksten/']")
    count = medicine_links.count()

    hrefs: List[str] = []
    for i in range(count):
        try:
            href = medicine_links.nth(i).get_attribute("href")
            if href:
                hrefs.append(href)
        except Exception:
            pass

    normalized: Set[str] = set()
    for h in hrefs:
        if h.startswith("/"):
            normalized.add(urljoin(BASE, h))
        elif h.startswith("http"):
            if "/bladeren/preparaatteksten/" in h:
                normalized.add(h)
        else:
            normalized.add(urljoin(BASE, "/" + h.lstrip("/")))

    return sorted(normalized)


def phase1_collect_urls(start_url: str) -> List[str]:
    print("\n" + "=" * 60)
    print("PHASE 1: COLLECTING DETAIL URLs")
    print("=" * 60)
    print(f"[OPEN] {start_url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO_MS)
        ctx = browser.new_context()
        page = ctx.new_page()
        page.set_default_timeout(TIMEOUT_MS)

        goto_with_retry(page, start_url)

        print("[EXPAND] Expanding all sections on listing page…")
        expanded = expand_all_listing(page)
        print(f"[EXPAND] Expanded blocks: {expanded}")

        print("[COLLECT] Collecting detail URLs…")
        urls = collect_detail_links(page)
        print(f"[COLLECT] Total URLs: {len(urls)}")

        # Save for debug/reuse
        ALL_URLS_JSON.write_text(json.dumps(urls, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[SAVE]   {ALL_URLS_JSON.resolve()}")

        browser.close()

    return urls


# -----------------------------
# Phase 2: Per-day only parsing
# -----------------------------
def expand_kosten_section(page) -> bool:
    """
    Expand the 'Kosten' section if collapsed.
    """
    try:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(800)

        found = page.evaluate(
            """
            () => {
              const headers = document.querySelectorAll('h2.page-module-title');
              for (const h of headers) {
                const t = (h.textContent || h.innerText || '').trim();
                if (t.includes('Kosten') || t.includes('Costs')) {
                  h.scrollIntoView({behavior:'instant', block:'center'});
                  if (h.classList.contains('collapsible-closed')) {
                    h.click();
                  }
                  return true;
                }
              }
              return false;
            }
            """
        )
        if found:
            try:
                page.wait_for_selector("table.cost-table", state="visible", timeout=5000)
            except Exception:
                pass
        return bool(found)
    except Exception:
        return False


def parse_cost_table_per_day_only(html: str) -> List[Dict]:
    """
    Extract ONLY:
      - avg_price_per_day (days=1)
      - reimbursed_per_day
      - copay_per_day
    from each cost-table row (one row per brand).
    """
    soup = BeautifulSoup(html, "lxml")
    table = soup.select_one("table.cost-table")
    if not table:
        return []

    # Identify select id like inline56783days
    select_id = None
    cap = table.select_one("caption")
    if cap:
        sel = cap.select_one("select[id^='inline'][id$='days']")
        if sel:
            select_id = sel.get("id")  # e.g., inline56783days

    out: List[Dict] = []

    for tr in table.select("tbody > tr"):
        if "separation-row" in (tr.get("class") or []):
            continue

        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        has_ddd = len(tds) >= 3 and ("ddd" in (tds[1].get("class") or []))
        if has_ddd:
            brand_td, ddd_td, price_td = tds[0], tds[1], tds[2]
            graph_td = tds[3] if len(tds) >= 4 else None
            ddd_text = ddd_td.get_text(" ", strip=True)
        else:
            brand_td, price_td = tds[0], tds[1]
            graph_td = tds[2] if len(tds) >= 3 else None
            ddd_text = None

        brand_full = brand_td.get_text(" ", strip=True)
        split = split_brand_full(brand_full)

        currency_el = price_td.select_one("abbr.currency")
        currency = currency_el.get_text(strip=True) if currency_el else "€"

        # ONLY day=1
        avg_price_per_day = None
        if select_id:
            span = price_td.select_one(f'.pat-depends[data-pat-depends="{select_id}=1"]')
            if span:
                avg_price_per_day = _to_float(span.get_text(strip=True))
        else:
            price_text = price_td.get_text(" ", strip=True)
            price_text = re.sub(r"^[€$£]\s*", "", price_text).strip()
            avg_price_per_day = _to_float(price_text)

        reimbursed_per_day = None
        copay_per_day = 0.0

        if graph_td:
            reimb_span = graph_td.select_one(".segment.reimbursed")
            contrib_span = graph_td.select_one(".segment.contribution")

            reimbursed_per_day = _first_number_eur(reimb_span.get_text(" ", strip=True) if reimb_span else "")
            contrib_val = _first_number_eur(contrib_span.get_text(" ", strip=True) if contrib_span else "")
            copay_per_day = contrib_val if contrib_val is not None else 0.0

        out.append(
            {
                "brand_full": brand_full,
                "brand_name": split["brand_name"],
                "pack_presentation": split["pack_presentation"],
                "ddd_text": ddd_text,
                "currency": currency,
                "avg_price_per_day": avg_price_per_day,
                "reimbursed_per_day": reimbursed_per_day,
                "copay_per_day": copay_per_day,
                "days_basis": 1,
            }
        )

    return out


OUTPUT_FIELDS = [
    "detail_url",
    "brand_full",
    "brand_name",
    "pack_presentation",
    "ddd_text",
    "currency",
    "avg_price_per_day",
    "reimbursed_per_day",
    "copay_per_day",
    "days_basis",
]


@dataclass
class Counters:
    done: int = 0
    skipped: int = 0
    errors: int = 0
    timeouts: int = 0


def worker_scrape(
    worker_id: int,
    q: "queue.Queue[str]",
    scraped: Set[str],
    scraped_lock: threading.Lock,
    csv_lock: threading.Lock,
    counters: Counters,
    counters_lock: threading.Lock,
):
    thread_name = f"W{worker_id}"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO_MS)
        ctx = browser.new_context()
        page = ctx.new_page()
        page.set_default_timeout(TIMEOUT_MS)

        while True:
            try:
                url = q.get_nowait()
            except queue.Empty:
                break

            with scraped_lock:
                if url in scraped:
                    with counters_lock:
                        counters.skipped += 1
                    q.task_done()
                    continue

            try:
                print(f"[{thread_name}] LOAD {url}")
                goto_with_retry(page, url)

                expand_kosten_section(page)

                try:
                    page.wait_for_load_state("networkidle", timeout=10_000)
                except Exception:
                    pass
                page.wait_for_timeout(DATA_LOAD_WAIT_SECONDS * 1000)

                html = page.content()
                rows = parse_cost_table_per_day_only(html)

                # attach detail_url per row
                for r in rows:
                    r["detail_url"] = url

                # write CSV
                append_rows_threadsafe(OUT_CSV, OUTPUT_FIELDS, rows, csv_lock)

                with scraped_lock:
                    scraped.add(url)

                with counters_lock:
                    counters.done += 1

            except PWTimeoutError:
                with counters_lock:
                    counters.timeouts += 1
                print(f"[{thread_name}] TIMEOUT {url}")

            except Exception as e:
                with counters_lock:
                    counters.errors += 1
                print(f"[{thread_name}] ERROR {url} :: {type(e).__name__}: {e}")

            finally:
                q.task_done()

        browser.close()


def phase2_scrape_all(detail_urls: List[str]) -> None:
    print("\n" + "=" * 60)
    print(f"PHASE 2: SCRAPING ALL URLs (WORKERS={WORKERS})")
    print("=" * 60)

    ensure_csv_headers(OUT_CSV, OUTPUT_FIELDS)

    scraped: Set[str] = set()
    scraped_lock = threading.Lock()
    csv_lock = threading.Lock()

    counters = Counters()
    counters_lock = threading.Lock()

    q: "queue.Queue[str]" = queue.Queue()
    for u in detail_urls:
        q.put(u)

    threads: List[threading.Thread] = []
    for i in range(1, WORKERS + 1):
        t = threading.Thread(
            target=worker_scrape,
            args=(i, q, scraped, scraped_lock, csv_lock, counters, counters_lock),
            daemon=True,
        )
        threads.append(t)
        t.start()

    # Progress loop
    total = len(detail_urls)
    while any(t.is_alive() for t in threads):
        time.sleep(3)
        with counters_lock:
            done = counters.done
            print(f"[PROGRESS] done={done}/{total} skip={counters.skipped} err={counters.errors} t/o={counters.timeouts}")

    for t in threads:
        t.join()

    with counters_lock:
        print("\n========== SUMMARY ==========")
        print(f"Total URLs   : {total}")
        print(f"Done         : {counters.done}")
        print(f"Skipped      : {counters.skipped}")
        print(f"Timeouts     : {counters.timeouts}")
        print(f"Errors       : {counters.errors}")
        print(f"CSV          : {OUT_CSV.resolve()}")


def main():
    start_url = sys.argv[1] if len(sys.argv) >= 2 else DEFAULT_START_URL

    detail_urls = phase1_collect_urls(start_url)
    if not detail_urls:
        print("[ERROR] No URLs collected. Exiting.")
        return

    phase2_scrape_all(detail_urls)


if __name__ == "__main__":
    main()
