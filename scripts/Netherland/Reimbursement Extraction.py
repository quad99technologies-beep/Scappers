# ftk_alfabet_expand_and_scrape_progress.py
# Visible Playwright browser + expand all + collect links + scrape each link
# Console shows live progress (done / skipped / errors + ETA)
#
# Install:
#   python3.11 -m venv venv
#   source venv/bin/activate
#   pip install -U pip
#   pip install playwright beautifulsoup4 lxml
#   playwright install
#
# Run:
#   python ftk_alfabet_expand_and_scrape_progress.py

from __future__ import annotations

import csv
import json
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

BASE = "https://www.farmacotherapeutischkompas.nl"
START_URL = "https://www.farmacotherapeutischkompas.nl/bladeren/preparaatteksten/alfabet/h#medicine-listing"

OUT_DIR = Path("output")
OUT_DIR.mkdir(exist_ok=True)

SCRAPED_JSON = OUT_DIR / "scraped_links.json"
ALL_URLS_JSON = OUT_DIR / "all_detail_urls.json"
DETAILS_CSV = OUT_DIR / "details.csv"
COSTS_CSV = OUT_DIR / "costs.csv"

HEADLESS = False       # visible browser
SLOW_MO_MS = 50
TIMEOUT_MS = 60_000

DELAY_BETWEEN_DETAILS = 0  # 2 second pause after storing data
DATA_LOAD_WAIT_SECONDS = 3  # 10 seconds wait for data to load

DETAIL_FIELDS = [
    "detail_url",
    "product_name",
    "product_type",
    "manufacturer",
    "administration_form",
    "strengths_raw",
]

COST_FIELDS = [
    "detail_url",
    "brand_full",
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

def load_scraped() -> Set[str]:
    """Load scraped URLs from JSON cache."""
    if SCRAPED_JSON.exists():
        try:
            return set(json.loads(SCRAPED_JSON.read_text(encoding="utf-8")))
        except Exception:
            return set()
    return set()

def load_scraped_from_csv() -> Set[str]:
    """Load scraped URLs by reading the CSV files directly."""
    scraped = set()
    
    # Check details CSV
    if DETAILS_CSV.exists():
        try:
            with DETAILS_CSV.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    url = row.get("detail_url")
                    if url:
                        scraped.add(url)
        except Exception:
            pass
    
    # Also check costs CSV for any additional URLs
    if COSTS_CSV.exists():
        try:
            with COSTS_CSV.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    url = row.get("detail_url")
                    if url:
                        scraped.add(url)
        except Exception:
            pass
    
    return scraped

def save_scraped(s: Set[str]) -> None:
    SCRAPED_JSON.write_text(json.dumps(sorted(s), indent=2), encoding="utf-8")

def save_all_urls(urls: List[str]) -> None:
    ALL_URLS_JSON.write_text(json.dumps(urls, indent=2), encoding="utf-8")

def ensure_csv_headers(path: Path, fieldnames: List[str]) -> None:
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=fieldnames).writeheader()

def append_rows(path: Path, fieldnames: List[str], rows: List[Dict]) -> None:
    if not rows:
        return
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})


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

def expand_costs_section(page) -> bool:
    """
    Expands the Kosten (Costs) section by clicking the h2.page-module-title if collapsed.
    Returns True if expanded or already open, False if not found.
    """
    try:
        # Scroll page to bottom first to ensure Kosten section is loaded
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(800)
        
        # Try JavaScript approach first - more reliable for finding elements with nested text
        # Search for "Kosten" (Dutch for "Costs")
        result = page.evaluate("""
            () => {
                const headers = document.querySelectorAll('h2.page-module-title');
                for (let h of headers) {
                    const text = h.textContent || h.innerText || '';
                    if (text.includes('Kosten') || text.includes('Costs')) {
                        // Scroll into view
                        h.scrollIntoView({ behavior: 'smooth', block: 'center' });
                        // Check if collapsed and click
                        if (h.classList.contains('collapsible-closed')) {
                            h.click();
                            return true;
                        }
                        return true; // Already open
                    }
                }
                return false;
            }
        """)
        
        if result:
            page.wait_for_timeout(1000)  # Wait for section to expand
            # Wait for cost table to be visible
            try:
                page.wait_for_selector("table.cost-table", state="visible", timeout=5000)
            except Exception:
                pass  # Table might not exist, continue anyway
            return True
        
        # Fallback: Try Playwright locator approach
        headers = page.locator("h2.page-module-title")
        count = headers.count()
        
        for i in range(count):
            try:
                h = headers.nth(i)
                # Try both inner_text and text_content
                text = h.inner_text() or h.text_content() or ""
                if "Kosten" in text or "Costs" in text:
                    h.scroll_into_view_if_needed(timeout=5000)
                    page.wait_for_timeout(500)
                    
                    classes = h.get_attribute("class") or ""
                    if "collapsible-closed" in classes:
                        h.click(timeout=5000)
                        page.wait_for_timeout(1000)
                    
                    try:
                        page.wait_for_selector("table.cost-table", state="visible", timeout=5000)
                    except Exception:
                        pass
                    return True
            except Exception:
                continue
        
        return False
    except Exception as e:
        # If expanding fails, continue anyway - table might already be visible
        return False

def parse_cost_table(html: str) -> List[Dict]:
    """
    Parse all cost tables from HTML, handling multiple formats:
    1. Simple format: Single price per unit (no dropdown)
    2. Dropdown format: Prices with pat-depends for different time periods
    3. With/without DDD column
    4. Different unit types (PRE-FILLED PEN, disposable syringe, bottle, pieces, etc.)
    """
    soup = BeautifulSoup(html, "lxml")
    tables = soup.select("table.cost-table")
    if not tables:
        # Try alternative selector
        tables = soup.select("table[class*='cost']")
    if not tables:
        return []

    out: List[Dict] = []
    
    for table in tables:
        # Extract caption/table type
        caption_el = table.select_one("caption")
        caption_text = ""
        table_type = ""
        unit_type = ""
        unit_amount = ""
        
        if caption_el:
            caption_text = caption_el.get_text(" ", strip=True)
            # Check if caption has select dropdown (dropdown format)
            select_el = caption_el.select_one("select[id^='inline'][id$='days']")
            
            # Extract unit type from caption (e.g., "PRE-FILLED PEN", "disposable syringe", "bottle", "pieces")
            # Handle nested abbr tags
            unit_entity = caption_el.select_one("abbr.entity")
            if unit_entity:
                # Get the innermost entity text
                inner_entity = unit_entity.select_one("abbr.entity")
                if inner_entity:
                    unit_type = inner_entity.get_text(" ", strip=True)
                else:
                    unit_type = unit_entity.get_text(" ", strip=True)
            
            # Extract unit amount (e.g., "1", "20")
            unit_amount_el = caption_el.select_one("span.unit.amount")
            if unit_amount_el:
                unit_amount = unit_amount_el.get_text(" ", strip=True)
            
            # Extract table type from caption text (e.g., "parenterals", "packaging")
            if caption_text:
                # Remove unit info to get table type
                table_type = caption_text.split("Average price")[0].strip()
                if not table_type:
                    table_type = caption_text
        
        # Determine if this is dropdown format
        is_dropdown_format = bool(caption_el and caption_el.select_one("select[id^='inline'][id$='days']"))
        
        # Find select ID if dropdown format
        select_id = None
        if is_dropdown_format:
            select_el = caption_el.select_one("select[id^='inline'][id$='days']")
            if select_el:
                select_id = select_el.get("id", "")
            else:
                # Fallback: find from pat-depends attribute
                first_pat_depends = table.select_one('[data-pat-depends*="days="]')
                if first_pat_depends:
                    attr = first_pat_depends.get("data-pat-depends", "")
                    match = re.search(r'(inline\d+days)', attr)
                    if match:
                        select_id = match.group(1)
        
        # Parse table rows
        for tr in table.select("tbody > tr"):
            if "separation-row" in (tr.get("class") or []):
                continue

            tds = tr.find_all("td")
            if len(tds) < 2:
                continue

            # Determine column structure
            # Check if second column is DDD column (has class "ddd")
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
            
            currency_el = price_td.select_one("abbr.currency")
            currency = currency_el.get_text(strip=True) if currency_el else "€"

            # Extract prices based on format
            price_per_day = None
            price_per_week = None
            price_per_month = None
            price_per_six_months = None
            
            if is_dropdown_format and select_id:
                # Dropdown format: extract from pat-depends spans
                def get_price(days: int) -> Optional[float]:
                    span = price_td.select_one(f'.pat-depends[data-pat-depends="{select_id}={days}"]')
                    if span:
                        text = span.get_text(strip=True)
                        return _to_float(text)
                    return None
                
                price_per_day = get_price(1)
                price_per_week = get_price(7)
                price_per_month = get_price(30)
                price_per_six_months = get_price(182)
            else:
                # Simple format: extract single price
                # Price is usually in a span after the currency abbr
                price_span = price_td.select_one("span")
                if price_span:
                    price_text = price_span.get_text(strip=True)
                    price_per_day = _to_float(price_text)
                else:
                    # Fallback: extract from all text, removing currency
                    price_text = price_td.get_text(" ", strip=True)
                    # Remove currency symbol and whitespace
                    price_text = re.sub(r'^[€$£]\s*', '', price_text).strip()
                    price_per_day = _to_float(price_text)
                
                # For simple format, set other periods to None
                price_per_week = None
                price_per_month = None
                price_per_six_months = None

            # Extract reimbursed and extra payment from graph
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
                
                # Also try to extract from contribution paragraph
                if extra is None:
                    contrib_p = graph_td.select_one("p.contribution strong.contribute")
                    if contrib_p:
                        extra = first_num(contrib_p.get_text(" ", strip=True))

            out.append({
                "brand_full": brand_full,
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
# Translation helpers
# -------------------------

def translate_page_to_english(page) -> None:
    """
    Attempt to translate the page content to English.
    Tries to find and click Chrome's translate button if available.
    Note: Chrome's auto-translate may require browser settings to be enabled.
    """
    try:
        # Try to find and click translate button if Chrome shows translate banner
        translate_selectors = [
            'button:has-text("Translate")',
            'button[aria-label*="Translate"]',
            '[data-testid="translate-button"]',
            '.translate-button',
            'button:has-text("Vertalen")'  # Dutch for "Translate"
        ]
        
        for selector in translate_selectors:
            try:
                translate_btn = page.locator(selector).first
                if translate_btn.is_visible(timeout=3000):
                    translate_btn.click()
                    page.wait_for_timeout(3000)  # Wait for translation to apply
                    print("[TRANSLATE] Translation triggered via button")
                    return
            except Exception:
                continue
        
        # Check if page content suggests it's in Dutch
        page.evaluate("""
            () => {
                const bodyText = document.body.innerText || document.body.textContent || '';
                const dutchWords = ['Kosten', 'Preparaat', 'Recept', 'Dosering', 'Toedieningsvorm'];
                const hasDutch = dutchWords.some(word => bodyText.includes(word));
                if (hasDutch) {
                    console.log('Page appears to be in Dutch. Chrome auto-translate should trigger if enabled.');
                }
            }
        """)
        
        print("[TRANSLATE] Language preferences set to English (browser context)")
        print("[TRANSLATE] Note: If page is still in Dutch, enable Chrome's auto-translate in browser settings")
        
    except Exception as e:
        print(f"[TRANSLATE] Warning: Could not trigger translation: {e}")


# -------------------------
# Playwright: expand + collect
# -------------------------

def expand_all(page) -> int:
    """
    Expands all collapsed medicine blocks by clicking
    .block-title.collapsible-closed until none remain.
    Returns number of blocks expanded.
    """
    page.wait_for_load_state("domcontentloaded")

    expanded = 0
    max_rounds = 100  # safety

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
                page.wait_for_timeout(300)
                expanded += 1
            except Exception:
                pass

        # small pause to allow DOM updates
        page.wait_for_timeout(500)

    return expanded

def collect_detail_links(page) -> List[str]:
    """Collect all medicine detail links from the expanded page."""
    # Wait a moment for DOM to stabilize after expansion
    page.wait_for_timeout(1000)
    
    # Use Playwright locator to find all medicine links
    # Selector: links with class "medicine" that contain "/bladeren/preparaatteksten/" in href
    medicine_links = page.locator("a.medicine[href*='/bladeren/preparaatteksten/']")
    count = medicine_links.count()
    
    hrefs = []
    for i in range(count):
        try:
            link = medicine_links.nth(i)
            href = link.get_attribute("href")
            if href:
                hrefs.append(href)
        except Exception:
            pass
    
    # Normalize URLs and remove duplicates
    normalized = set()
    for h in hrefs:
        if not h:
            continue
        # If it's a relative URL, make it absolute
        if h.startswith('/'):
            normalized.add(urljoin(BASE, h))
        elif h.startswith('http'):
            # Ensure it's from the correct domain
            if '/bladeren/preparaatteksten/' in h:
                normalized.add(h)
        else:
            normalized.add(urljoin(BASE, '/' + h.lstrip('/')))
    
    return sorted(normalized)


# -------------------------
# Pretty progress helpers
# -------------------------

def fmt_hms(seconds: float) -> str:
    s = int(seconds)
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    return f"{h:02d}:{m:02d}:{sec:02d}"

def eta(done: int, total: int, elapsed_sec: float) -> str:
    if done <= 0:
        return "--:--:--"
    rate = done / max(elapsed_sec, 1e-6)  # pages per second (done=successful scrapes)
    remaining = max(total - done, 0)
    est = remaining / max(rate, 1e-6)
    return fmt_hms(est)


# -------------------------
# Main - Phase 1: Collect URLs
# -------------------------

def phase1_collect_urls() -> List[str]:
    """
    Phase 1: Open browser, go to main URL, expand all blocks, collect detail URLs, then close browser.
    Returns list of detail URLs.
    """
    print("\n" + "="*60)
    print("PHASE 1: COLLECTING DETAIL URLs")
    print("="*60)
    
    print("[OPEN] Starting browser… (visible)")
    start_ts = time.time()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO_MS)
        ctx = browser.new_context()
        page = ctx.new_page()
        page.set_default_timeout(TIMEOUT_MS)

        print(f"[OPEN] Loading alfabet page: {START_URL}")
        page.goto(START_URL, wait_until="domcontentloaded")

        print("[EXPAND] Expanding all collapsible blocks (.block-title.collapsible-closed)…")
        expanded = expand_all(page)
        print(f"[EXPAND] Done. Blocks expanded: {expanded}")

        print("[COLLECT] Collecting detail links (a.medicine)…")
        detail_urls = collect_detail_links(page)
        save_all_urls(detail_urls)

        print(f"[COLLECT] Total detail links found: {len(detail_urls)}")
        print(f"[SAVE]   Cached URLs -> {ALL_URLS_JSON}")
        
        # Close browser session
        browser.close()
        print("[CLOSE] Browser session closed")

    elapsed = time.time() - start_ts
    print(f"[DONE] Phase 1 completed in {fmt_hms(elapsed)}")
    
    return detail_urls


# -------------------------
# Main - Phase 2: Extract Details
# -------------------------

def phase2_extract_details(detail_urls: List[str]) -> None:
    """
    Phase 2: Open new browser session, loop through URLs, translate to English,
    wait 10 minutes for data to load, then extract details and costs.
    """
    print("\n" + "="*60)
    print("PHASE 2: EXTRACTING DETAILS AND COSTS")
    print("="*60)
    
    # Load scraped URLs from CSV files (primary source) and JSON cache (backup)
    print("[LOAD] Loading already scraped URLs from CSV files...")
    scraped = load_scraped_from_csv()
    # Also merge with JSON cache if it exists
    json_scraped = load_scraped()
    scraped.update(json_scraped)
    print(f"[LOAD] Found {len(scraped)} already scraped URLs")

    print("[OPEN] Starting browser… (visible)")
    start_ts = time.time()

    done = 0
    skipped = 0
    errors = 0
    timeouts = 0

    total_links = len(detail_urls)
    already = sum(1 for u in detail_urls if u in scraped)
    pending = total_links - already

    print(f"[RESUME] Already scraped        : {already}")
    print(f"[PENDING] Will scrape now        : {pending}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO_MS)
        ctx = browser.new_context(
            locale="en-US",
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9"
            }
        )
        page = ctx.new_page()
        page.set_default_timeout(TIMEOUT_MS)
        
        # Set language preferences via JavaScript before first navigation
        page.add_init_script("""
            Object.defineProperty(navigator, 'language', {
                get: function() { return 'en-US'; }
            });
            Object.defineProperty(navigator, 'languages', {
                get: function() { return ['en-US', 'en']; }
            });
        """)

        for idx, url in enumerate(detail_urls, start=1):
            if url in scraped:
                skipped += 1
                if idx % 50 == 0:  # don't spam console on huge skip bursts
                    elapsed = time.time() - start_ts
                    print(f"[SKIP] {idx}/{total_links}  skipped={skipped} done={done} err={errors} t/o={timeouts}  elapsed={fmt_hms(elapsed)}")
                continue

            try:
                print(f"\n[LOAD] {idx}/{total_links} Loading: {url}")
                page.goto(url, wait_until="domcontentloaded")
                
                # Translate page to English
                print(f"[TRANSLATE] Translating page to English...")
                translate_page_to_english(page)
                page.wait_for_timeout(2000)  # Wait for translation to apply
                
                # Expand Costs section if collapsed
                print(f"[EXPAND] Expanding Costs section...")
                costs_expanded = expand_costs_section(page)
                
                # Wait for data to load
                print(f"[WAIT] Waiting {DATA_LOAD_WAIT_SECONDS} seconds for data to load...")
                time.sleep(DATA_LOAD_WAIT_SECONDS)
                print(f"[WAIT] Wait completed")
                
                # Get HTML after waiting and expanding
                html = page.content()

                print(f"[PARSE] Parsing recipe section and cost table...")
                recipe = parse_recipe_section(html)
                costs = parse_cost_table(html)

                # Store data immediately after extraction
                append_rows(DETAILS_CSV, DETAIL_FIELDS, [{
                    "detail_url": url,
                    "product_name": recipe.get("product_name"),
                    "product_type": recipe.get("product_type"),
                    "manufacturer": recipe.get("manufacturer"),
                    "administration_form": recipe.get("administration_form"),
                    "strengths_raw": recipe.get("strengths_raw"),
                }])

                # Only append costs if we found any
                if costs:
                    append_rows(COSTS_CSV, COST_FIELDS, [{"detail_url": url, **c} for c in costs])

                # Mark as scraped and save immediately
                scraped.add(url)
                save_scraped(scraped)
                done += 1

                elapsed = time.time() - start_ts
                cost_count = len(costs) if costs else 0
                print(
                    f"[OK] {idx}/{total_links}  done={done} pending~={pending - done} "
                    f"skipped={skipped} err={errors} t/o={timeouts} costs={cost_count} "
                    f"elapsed={fmt_hms(elapsed)}  ETA={eta(done, pending, elapsed)}"
                )

                # 2 second pause after storing data
                time.sleep(DELAY_BETWEEN_DETAILS)

            except PWTimeoutError:
                timeouts += 1
                elapsed = time.time() - start_ts
                print(f"[TIMEOUT] {idx}/{total_links}  t/o={timeouts}  elapsed={fmt_hms(elapsed)}\n         {url}")
            except Exception as e:
                errors += 1
                elapsed = time.time() - start_ts
                print(f"[ERROR] {idx}/{total_links}  err={errors}  elapsed={fmt_hms(elapsed)}\n       {url}\n       {type(e).__name__}: {e}")

        save_scraped(scraped)
        browser.close()

    elapsed = time.time() - start_ts
    print("\n========== PHASE 2 SUMMARY ==========")
    print(f"Total links found   : {total_links}")
    print(f"Already scraped     : {already}")
    print(f"Scraped this run    : {done}")
    print(f"Skipped (dedupe)    : {skipped}")
    print(f"Timeouts            : {timeouts}")
    print(f"Errors              : {errors}")
    print(f"Elapsed             : {fmt_hms(elapsed)}")
    print(f"Output folder        : {OUT_DIR.resolve()}")


# -------------------------
# Main
# -------------------------

def main():
    ensure_csv_headers(DETAILS_CSV, DETAIL_FIELDS)
    ensure_csv_headers(COSTS_CSV, COST_FIELDS)

    # Phase 1: Collect URLs (then close browser)
    detail_urls = phase1_collect_urls()
    
    if not detail_urls:
        print("[ERROR] No detail URLs collected. Exiting.")
        return
    
    # Phase 2: Extract details (open new browser, translate, wait, extract)
    phase2_extract_details(detail_urls)
    
    print("\n" + "="*60)
    print("ALL PHASES COMPLETED")
    print("="*60)


if __name__ == "__main__":
    main()
