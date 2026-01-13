# ftk_alfabet_expand_and_scrape_progress_mt3_FIXED.py
# Visible Playwright browser + expand all + collect links + scrape each link
# Phase 2 runs with MULTI-THREAD = 3 workers (3 browsers)
#
# FIXES (for your "stuck after opening threads"):
# 1) Removed deadlock: no nested acquisition of the same Lock()
# 2) Do NOT mark URL as scraped before success (only after CSV write)
# 3) Thread-safe CSV append using a single csv_lock
# 4) Added brand_name + pack_presentation columns (split brand_full on FIRST comma)
# 5) Robust worker loop: handles timeouts/errors without poisoning resume state
#
# Install:
#   python -m venv venv
#   venv\Scripts\activate
#   pip install -U pip
#   pip install playwright beautifulsoup4 lxml
#   playwright install
#
# Run:
#   python ftk_alfabet_expand_and_scrape_progress_mt3_FIXED.py

from __future__ import annotations

import csv
import json
import re
import sys
import time
import threading
import queue
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin

# Add script directory to path for config_loader import
_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from config_loader import get_output_dir

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, Error as PWError
try:
    from playwright.sync_api import TargetClosedError
except ImportError:
    # Fallback for older Playwright versions
    TargetClosedError = Exception


BASE = "https://www.farmacotherapeutischkompas.nl"
START_URL = "https://www.farmacotherapeutischkompas.nl/bladeren/preparaatteksten/alfabet/h#medicine-listing"

# Use platform config for output directory
OUT_DIR = get_output_dir()
OUT_DIR.mkdir(parents=True, exist_ok=True)

SCRAPED_JSON = OUT_DIR / "scraped_links.json"
ALL_URLS_JSON = OUT_DIR / "all_detail_urls.json"
DETAILS_CSV = OUT_DIR / "details.csv"
COSTS_CSV = OUT_DIR / "costs.csv"

HEADLESS = True           # Hide browser instances
SLOW_MO_MS = 50
TIMEOUT_MS = 60_000

DATA_LOAD_WAIT_SECONDS = 3
DELAY_BETWEEN_DETAILS = 0

WORKERS = 3               # <==== multi-thread count
PRINT_EVERY_N = 25        # per-worker progress print throttle
FLUSH_SCRAPED_EVERY = 10  # reduce json writes (performance)

# Network retry settings
NETWORK_RETRY_MAX = 3
NETWORK_RETRY_DELAY = 5  # seconds

DETAIL_FIELDS = [
    "detail_url",
    "product_name",
    "product_type",
    "manufacturer",
    "administration_form",
    "strengths_raw",
]

# Added brand_name + pack_presentation
COST_FIELDS = [
    "detail_url",
    "brand_full",
    "brand_name",
    "pack_presentation",
    "ddd_text",
    "currency",
    "price_per_day",
    "price_per_week",
    "price_per_month",
    "price_per_six_months",
    "reimbursed_per_day",
    "extra_payment_per_day",
    "table_type",
    "unit_type",
    "unit_amount",
]


# -------------------------
# Persistence helpers
# -------------------------

def ensure_csv_headers(path: Path, fieldnames: List[str]) -> None:
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=fieldnames).writeheader()

def append_rows(path: Path, fieldnames: List[str], rows: List[Dict], lock: threading.Lock) -> None:
    if not rows:
        return
    with lock:
        with path.open("a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            for r in rows:
                w.writerow({k: r.get(k) for k in fieldnames})

def load_scraped_json() -> Set[str]:
    if SCRAPED_JSON.exists():
        try:
            return set(json.loads(SCRAPED_JSON.read_text(encoding="utf-8")))
        except Exception:
            return set()
    return set()

def save_scraped_json_unlocked(s: Set[str]) -> None:
    # IMPORTANT: caller must hold scraped_lock if using in multi-thread context
    SCRAPED_JSON.write_text(json.dumps(sorted(s), indent=2), encoding="utf-8")

def save_all_urls(urls: List[str]) -> None:
    ALL_URLS_JSON.write_text(json.dumps(urls, indent=2), encoding="utf-8")

def load_scraped_from_csv() -> Set[str]:
    scraped = set()

    if DETAILS_CSV.exists():
        try:
            with DETAILS_CSV.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    u = row.get("detail_url")
                    if u:
                        scraped.add(u)
        except Exception:
            pass

    if COSTS_CSV.exists():
        try:
            with COSTS_CSV.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    u = row.get("detail_url")
                    if u:
                        scraped.add(u)
        except Exception:
            pass

    return scraped


# -------------------------
# Parsers
# -------------------------

def parse_recipe_section(html: str) -> Dict:
    soup = BeautifulSoup(html, "lxml")
    sec = soup.select_one("section.recipe")
    if not sec:
        return {}

    name_el = sec.select_one(".title .product .name")
    product_name = name_el.get_text(" ", strip=True) if name_el else None

    ptype_el = sec.select_one(".title .product .product-type")
    product_type = ptype_el.get_text(" ", strip=True) if ptype_el else None

    man_el = sec.select_one(".title .manfact")
    manufacturer = man_el.get_text(" ", strip=True) if man_el else None

    details = {}
    for dt in sec.select(".doses dl.details dt"):
        dd = dt.find_next_sibling("dd")
        if not dd:
            continue
        key = dt.get_text(" ", strip=True).lower()
        val = dd.get_text(" ", strip=True)
        details[key] = val

    return {
        "product_name": product_name,
        "product_type": product_type,
        "manufacturer": manufacturer,
        "administration_form": details.get("administration form"),
        "strengths_raw": details.get("strength"),
    }

def _to_float(x: str) -> Optional[float]:
    if not x:
        return None
    x = x.strip().replace("\xa0", " ").replace(",", ".")
    try:
        return float(x)
    except ValueError:
        return None

def split_brand_full(brand_full: str) -> Dict[str, str]:
    # Split on FIRST comma
    if not brand_full:
        return {"brand_name": "", "pack_presentation": ""}
    s = str(brand_full).strip()
    parts = re.split(r"\s*,\s*", s, maxsplit=1)
    return {
        "brand_name": (parts[0].strip() if parts else ""),
        "pack_presentation": (parts[1].strip() if len(parts) > 1 else ""),
    }

def expand_costs_section(page) -> bool:
    """
    Expands the Kosten (Costs) section by clicking h2.page-module-title if collapsed.
    """
    try:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(800)

        result = page.evaluate("""
            () => {
                const headers = document.querySelectorAll('h2.page-module-title');
                for (let h of headers) {
                    const text = h.textContent || h.innerText || '';
                    if (text.includes('Kosten') || text.includes('Costs')) {
                        h.scrollIntoView({ behavior: 'smooth', block: 'center' });
                        if (h.classList.contains('collapsible-closed')) {
                            h.click();
                            return true;
                        }
                        return true;
                    }
                }
                return false;
            }
        """)

        if result:
            page.wait_for_timeout(1000)
            try:
                page.wait_for_selector("table.cost-table", state="visible", timeout=5000)
            except Exception:
                pass
            return True

        return False
    except Exception:
        return False

def parse_cost_table(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    tables = soup.select("table.cost-table")
    if not tables:
        tables = soup.select("table[class*='cost']")
    if not tables:
        return []

    out: List[Dict] = []

    for table in tables:
        caption_el = table.select_one("caption")
        caption_text = ""
        table_type = ""
        unit_type = ""
        unit_amount = ""

        if caption_el:
            caption_text = caption_el.get_text(" ", strip=True)

            unit_entity = caption_el.select_one("abbr.entity")
            if unit_entity:
                inner_entity = unit_entity.select_one("abbr.entity")
                unit_type = (inner_entity.get_text(" ", strip=True)
                             if inner_entity else unit_entity.get_text(" ", strip=True))

            unit_amount_el = caption_el.select_one("span.unit.amount")
            if unit_amount_el:
                unit_amount = unit_amount_el.get_text(" ", strip=True)

            if caption_text:
                table_type = caption_text.split("Average price")[0].strip() or caption_text

        is_dropdown_format = bool(caption_el and caption_el.select_one("select[id^='inline'][id$='days']"))
        select_id = None
        if is_dropdown_format and caption_el:
            sel = caption_el.select_one("select[id^='inline'][id$='days']")
            if sel:
                select_id = sel.get("id", "")

        for tr in table.select("tbody > tr"):
            if "separation-row" in (tr.get("class") or []):
                continue

            tds = tr.find_all("td")
            if len(tds) < 2:
                continue

            has_ddd = len(tds) >= 3 and "ddd" in (tds[1].get("class") or [])
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

            price_per_day = price_per_week = price_per_month = price_per_six_months = None

            if is_dropdown_format and select_id:
                def get_price(days: int) -> Optional[float]:
                    span = price_td.select_one(f'.pat-depends[data-pat-depends="{select_id}={days}"]')
                    if span:
                        return _to_float(span.get_text(strip=True))
                    return None

                price_per_day = get_price(1)
                price_per_week = get_price(7)
                price_per_month = get_price(30)
                price_per_six_months = get_price(182)
            else:
                price_span = price_td.select_one("span")
                if price_span:
                    price_per_day = _to_float(price_span.get_text(strip=True))
                else:
                    price_text = price_td.get_text(" ", strip=True)
                    price_text = re.sub(r'^[€$£]\s*', '', price_text).strip()
                    price_per_day = _to_float(price_text)

            reimbursed = None
            extra = None

            if graph_td:
                reimb_span = graph_td.select_one(".segment.reimbursed")
                contrib_span = graph_td.select_one(".segment.contribution")

                def first_num(txt: str) -> Optional[float]:
                    if not txt:
                        return None
                    txt = txt.replace("€", "").replace("\xa0", " ").replace(",", ".")
                    m = re.search(r"(\d+[.,]\d+|\d+)", txt)
                    return _to_float(m.group(1)) if m else None

                reimbursed = first_num(reimb_span.get_text(" ", strip=True) if reimb_span else "")
                extra = first_num(contrib_span.get_text(" ", strip=True) if contrib_span else "")

                if extra is None:
                    contrib_p = graph_td.select_one("p.contribution strong.contribute")
                    if contrib_p:
                        extra = first_num(contrib_p.get_text(" ", strip=True))

            out.append({
                "brand_full": brand_full,
                "brand_name": split["brand_name"],
                "pack_presentation": split["pack_presentation"],
                "ddd_text": ddd_text,
                "currency": currency,
                "price_per_day": price_per_day,
                "price_per_week": price_per_week,
                "price_per_month": price_per_month,
                "price_per_six_months": price_per_six_months,
                "reimbursed_per_day": reimbursed,
                "extra_payment_per_day": extra,
                "table_type": table_type,
                "unit_type": unit_type,
                "unit_amount": unit_amount,
            })

    return out


# -------------------------
# Page helpers
# -------------------------

def expand_all(page) -> int:
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
    page.wait_for_timeout(1000)
    medicine_links = page.locator("a.medicine[href*='/bladeren/preparaatteksten/']")
    count = medicine_links.count()

    hrefs = []
    for i in range(count):
        try:
            href = medicine_links.nth(i).get_attribute("href")
            if href:
                hrefs.append(href)
        except Exception:
            pass

    normalized = set()
    for h in hrefs:
        if h.startswith("/"):
            normalized.add(urljoin(BASE, h))
        elif h.startswith("http"):
            if "/bladeren/preparaatteksten/" in h:
                normalized.add(h)
        else:
            normalized.add(urljoin(BASE, "/" + h.lstrip("/")))

    return sorted(normalized)

def fmt_hms(seconds: float) -> str:
    s = int(seconds)
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    return f"{h:02d}:{m:02d}:{sec:02d}"


# -------------------------
# Phase 1
# -------------------------

def phase1_collect_urls() -> List[str]:
    print("\n" + "=" * 60)
    print("PHASE 1: COLLECTING DETAIL URLs")
    print("=" * 60)

    start_ts = time.time()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO_MS)
        
        # Track Playwright browser PIDs for UI display
        try:
            from core.chrome_pid_tracker import get_chrome_pids_from_playwright_browser, save_chrome_pids
            repo_root = Path(__file__).resolve().parent.parent.parent
            scraper_name = "Netherlands"
            browser_pids = get_chrome_pids_from_playwright_browser(browser)
            if browser_pids:
                save_chrome_pids(scraper_name, repo_root, browser_pids)
        except Exception:
            pass  # PID tracking not critical
        
        ctx = browser.new_context()
        page = ctx.new_page()
        page.set_default_timeout(TIMEOUT_MS)

        print(f"[OPEN] {START_URL}")
        # Retry logic for network errors
        for attempt in range(1, NETWORK_RETRY_MAX + 1):
            try:
                page.goto(START_URL, wait_until="domcontentloaded")
                break  # Success, exit retry loop
            except PWError as e:
                error_msg = str(e).lower()
                # Check if it's a network/connection error
                if any(keyword in error_msg for keyword in ["connection", "network", "err_connection"]):
                    if attempt < NETWORK_RETRY_MAX:
                        wait_time = NETWORK_RETRY_DELAY * attempt
                        print(f"[RETRY] Network error (attempt {attempt}/{NETWORK_RETRY_MAX}): {e}")
                        print(f"[RETRY] Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"[ERROR] Network error after {NETWORK_RETRY_MAX} attempts: {e}")
                        raise
                else:
                    # Not a network error, re-raise immediately
                    raise

        print("[EXPAND] Clicking all .block-title.collapsible-closed …")
        expanded = expand_all(page)
        print(f"[EXPAND] Expanded blocks: {expanded}")

        print("[COLLECT] Collecting a.medicine links …")
        detail_urls = collect_detail_links(page)
        save_all_urls(detail_urls)

        print(f"[COLLECT] Total URLs: {len(detail_urls)}")
        print(f"[SAVE]   {ALL_URLS_JSON}")

        browser.close()

    print(f"[DONE] Phase 1 in {fmt_hms(time.time() - start_ts)}")
    return detail_urls


# -------------------------
# Phase 2 (Multi-thread) - FIXED
# -------------------------

class Counters:
    def __init__(self):
        self.done = 0
        self.skipped = 0
        self.errors = 0
        self.timeouts = 0

def worker_run(
    worker_id: int,
    q: "queue.Queue[str]",
    scraped_set: Set[str],
    scraped_lock: threading.Lock,
    csv_lock: threading.Lock,
    counters: Counters,
    counters_lock: threading.Lock,
    start_ts: float,
    total_pending: int,
    browser_pids_lock: threading.Lock = None,
    all_browser_pids: set = None,
) -> None:
    """
    Each worker launches its own Playwright instance + browser.
    - Checks scraped_set before processing
    - Writes CSV rows under csv_lock
    - Marks URL scraped ONLY AFTER successful CSV write
    - Flushes scraped_links.json periodically (not every URL)
    """
    thread_name = f"W{worker_id}"

    local_done = 0
    local_since_flush = 0

    def create_browser_and_page(p):
        """Helper to create browser, context, and page"""
        browser = p.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO_MS)
        
        # Track Playwright browser PIDs for UI display
        try:
            from core.chrome_pid_tracker import get_chrome_pids_from_playwright_browser, save_chrome_pids
            repo_root = Path(__file__).resolve().parent.parent.parent
            scraper_name = "Netherlands"
            browser_pids = get_chrome_pids_from_playwright_browser(browser)
            if browser_pids:
                # Add to shared set if provided (for multi-worker tracking)
                if browser_pids_lock is not None and all_browser_pids is not None:
                    with browser_pids_lock:
                        all_browser_pids.update(browser_pids)
                # Also save immediately for this worker
                save_chrome_pids(scraper_name, repo_root, browser_pids)
        except Exception:
            pass  # PID tracking not critical
        
        ctx = browser.new_context(
            locale="en-US",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        page = ctx.new_page()
        page.set_default_timeout(TIMEOUT_MS)
        return browser, ctx, page
    
    def is_browser_valid(browser, ctx, page):
        """Check if browser, context, and page are still valid"""
        try:
            # Try to check if browser is connected
            if browser and not browser.is_connected():
                return False
            # Try to check if context is still valid
            if ctx and ctx.pages:
                # Context seems valid, check page
                if page and page.url:
                    return True
            return True  # Assume valid if we can't check
        except Exception:
            return False  # If any check fails, assume invalid

    with sync_playwright() as p:
        browser, ctx, page = create_browser_and_page(p)
        browser_recreated = False

        while True:
            try:
                url = q.get_nowait()
            except queue.Empty:
                break

            # Skip if already scraped (thread-safe read)
            with scraped_lock:
                if url in scraped_set:
                    with counters_lock:
                        counters.skipped += 1
                    q.task_done()
                    continue

            # Check if browser/page/context is still valid, recreate if needed
            if not is_browser_valid(browser, ctx, page):
                print(f"[{thread_name}] Browser closed, recreating...")
                try:
                    if browser:
                        browser.close()
                except Exception:
                    pass
                browser, ctx, page = create_browser_and_page(p)
                browser_recreated = True

            try:
                print(f"[{thread_name}] LOAD {url}")
                page.goto(url, wait_until="domcontentloaded")

                expand_costs_section(page)
                time.sleep(DATA_LOAD_WAIT_SECONDS)
                html = page.content()

                recipe = parse_recipe_section(html)
                costs = parse_cost_table(html)

                # Write to CSV (atomic in lock)
                append_rows(
                    DETAILS_CSV,
                    DETAIL_FIELDS,
                    [{
                        "detail_url": url,
                        "product_name": recipe.get("product_name"),
                        "product_type": recipe.get("product_type"),
                        "manufacturer": recipe.get("manufacturer"),
                        "administration_form": recipe.get("administration_form"),
                        "strengths_raw": recipe.get("strengths_raw"),
                    }],
                    lock=csv_lock
                )

                if costs:
                    cost_rows = [{"detail_url": url, **c} for c in costs]
                    append_rows(COSTS_CSV, COST_FIELDS, cost_rows, lock=csv_lock)

                # Mark scraped ONLY after successful write
                with scraped_lock:
                    scraped_set.add(url)
                    local_since_flush += 1
                    if local_since_flush >= FLUSH_SCRAPED_EVERY:
                        save_scraped_json_unlocked(scraped_set)
                        local_since_flush = 0

                # Update counters
                with counters_lock:
                    counters.done += 1

                local_done += 1
                if local_done % PRINT_EVERY_N == 0:
                    with counters_lock:
                        elapsed = time.time() - start_ts
                        done = counters.done
                        percent = (done / total_pending * 100) if total_pending > 0 else 0.0
                        print(
                            f"[{thread_name}] PROGRESS done={done}/{total_pending} "
                            f"skip={counters.skipped} err={counters.errors} t/o={counters.timeouts} "
                            f"elapsed={fmt_hms(elapsed)}"
                        )
                        # Output GUI-compatible progress format
                        print(
                            f"[PROGRESS] Extracting reimbursement data: {done}/{total_pending} ({percent:.1f}%)",
                            flush=True
                        )

                time.sleep(DELAY_BETWEEN_DETAILS)
                browser_recreated = False  # Reset flag after successful operation

            except PWTimeoutError:
                with counters_lock:
                    counters.timeouts += 1
                print(f"[{thread_name}] TIMEOUT {url}")

            except (TargetClosedError, Exception) as e:
                error_type = type(e).__name__
                error_msg = str(e)
                with counters_lock:
                    counters.errors += 1
                print(f"[{thread_name}] ERROR {url} :: {error_type}: {e}")
                
                # If browser/page/context was closed, recreate immediately
                is_closed_error = (
                    isinstance(e, TargetClosedError) or
                    "TargetClosed" in error_type or 
                    "Target page" in error_msg or 
                    "browser has been closed" in error_msg or
                    "context or browser has been closed" in error_msg
                )
                
                if is_closed_error:
                    print(f"[{thread_name}] Browser closed detected, recreating browser...")
                    try:
                        if browser:
                            try:
                                browser.close()
                            except Exception:
                                pass
                    except Exception:
                        pass
                    
                    # Recreate browser, context, and page
                    try:
                        browser, ctx, page = create_browser_and_page(p)
                        print(f"[{thread_name}] Browser recreated successfully")
                        browser_recreated = True
                    except Exception as recreate_error:
                        print(f"[{thread_name}] Failed to recreate browser: {recreate_error}")
                        # Mark as invalid so next iteration will try again
                        browser = None
                        ctx = None
                        page = None

            finally:
                q.task_done()

        # final flush for this worker
        with scraped_lock:
            save_scraped_json_unlocked(scraped_set)

        # Clean up browser
        try:
            if browser:
                browser.close()
        except Exception:
            pass


def phase2_extract_details_mt(detail_urls: List[str]) -> None:
    print("\n" + "=" * 60)
    print(f"PHASE 2: EXTRACTING DETAILS AND COSTS (MULTI-THREAD={WORKERS})")
    print("=" * 60)

    # resume state
    scraped = load_scraped_from_csv()
    scraped |= load_scraped_json()

    total = len(detail_urls)
    pending_urls = [u for u in detail_urls if u not in scraped]

    print(f"[TOTAL]  detail URLs     : {total}")
    print(f"[RESUME] already scraped: {len(scraped)}")
    print(f"[PENDING] scrape now    : {len(pending_urls)}")
    
    # Output initial progress
    if len(pending_urls) > 0:
        print(f"[PROGRESS] Extracting reimbursement data: 0/{len(pending_urls)} (0.0%)", flush=True)
    else:
        print(f"[PROGRESS] Extracting reimbursement data: {len(scraped)}/{total} (100.0%)", flush=True)

    scraped_lock = threading.Lock()
    csv_lock = threading.Lock()
    counters = Counters()
    counters_lock = threading.Lock()

    # shared scraped_set includes already-scraped urls
    scraped_set = set(scraped)

    # queue
    q: "queue.Queue[str]" = queue.Queue()
    for u in pending_urls:
        q.put(u)

    start_ts = time.time()
    
    # Track all browser PIDs from workers
    all_browser_pids = set()
    browser_pids_lock = threading.Lock()

    threads = []
    for i in range(1, WORKERS + 1):
        t = threading.Thread(
            target=worker_run,
            args=(i, q, scraped_set, scraped_lock, csv_lock, counters, counters_lock, start_ts, len(pending_urls), browser_pids_lock, all_browser_pids),
            daemon=True,
        )
        threads.append(t)
        t.start()

    # Periodically update progress and Chrome PIDs while workers are running
    import time as time_module
    last_progress_update = 0
    PROGRESS_UPDATE_INTERVAL = 5  # Update progress every 5 seconds
    
    while any(t.is_alive() for t in threads):
        time_module.sleep(1)  # Check every second
        current_time = time_module.time()
        
        # Update progress periodically
        if current_time - last_progress_update >= PROGRESS_UPDATE_INTERVAL:
            with counters_lock:
                done = counters.done
                total_pending = len(pending_urls)
                if total_pending > 0:
                    percent = (done / total_pending * 100)
                    print(f"[PROGRESS] Extracting reimbursement data: {done}/{total_pending} ({percent:.1f}%)", flush=True)
            
            # Update Chrome PIDs periodically
            try:
                from core.chrome_pid_tracker import save_chrome_pids
                repo_root = Path(__file__).resolve().parent.parent.parent
                scraper_name = "Netherlands"
                with browser_pids_lock:
                    if all_browser_pids:
                        save_chrome_pids(scraper_name, repo_root, all_browser_pids)
            except Exception:
                pass  # PID tracking not critical
            
            last_progress_update = current_time

    for t in threads:
        t.join()

    # Final progress update
    with counters_lock:
        done = counters.done
        total_pending = len(pending_urls)
        if total_pending > 0:
            percent = (done / total_pending * 100)
            print(f"[PROGRESS] Extracting reimbursement data: {done}/{total_pending} ({percent:.1f}%)", flush=True)

    # Final Chrome PID update
    try:
        from core.chrome_pid_tracker import save_chrome_pids
        repo_root = Path(__file__).resolve().parent.parent.parent
        scraper_name = "Netherlands"
        with browser_pids_lock:
            if all_browser_pids:
                save_chrome_pids(scraper_name, repo_root, all_browser_pids)
    except Exception:
        pass  # PID tracking not critical

    # final global flush
    with scraped_lock:
        save_scraped_json_unlocked(scraped_set)

    elapsed = time.time() - start_ts

    print("\n========== PHASE 2 SUMMARY ==========")
    print(f"Pending processed     : {len(pending_urls)}")
    print(f"Scraped this run      : {counters.done}")
    print(f"Skipped (dedupe)      : {counters.skipped}")
    print(f"Timeouts              : {counters.timeouts}")
    print(f"Errors                : {counters.errors}")
    print(f"Elapsed               : {fmt_hms(elapsed)}")
    print(f"Output folder         : {OUT_DIR.resolve()}")


def main():
    ensure_csv_headers(DETAILS_CSV, DETAIL_FIELDS)
    ensure_csv_headers(COSTS_CSV, COST_FIELDS)

    detail_urls = phase1_collect_urls()
    if not detail_urls:
        print("[ERROR] No URLs collected. Exiting.")
        return

    phase2_extract_details_mt(detail_urls)

    print("\n" + "=" * 60)
    print("ALL PHASES COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    main()
