"""
Netherlands (Farmacotherapeutisch Kompas) – Reimbursement Extraction (End-to-End)

What this does:
1) Phase 1 (URL discovery): Opens FK "Medicines" listing page and collects all detail URLs:
   /bladeren/preparaatteksten/...
2) Phase 2 (Derivation logic): For each detail URL, builds reimbursement rows using:
   (Population block) × (each indication bullet) × (each strength)
   and writes an EVERSANA-style CSV.

How to run:
  python 02_reimbursement_extraction_end_to_end.py --start-url "https://www.farmacotherapeutischkompas.nl/bladeren/preparaatteksten/groep" --limit 50
  python 02_reimbursement_extraction_end_to_end.py --start-url "https://www.farmacotherapeutischkompas.nl/bladeren/preparaatteksten/groep" --only "rivaroxaban"
  python 02_reimbursement_extraction_end_to_end.py --phase2-only --urls-file detail_urls.txt

Requires:
  pip install playwright requests beautifulsoup4 lxml
  playwright install
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Tuple, Any

import requests
from bs4 import BeautifulSoup

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


# -------------------------
# Config / Constants
# -------------------------
DEFAULT_START_URL = "https://www.farmacotherapeutischkompas.nl/bladeren/preparaatteksten/groep"
BASE = "https://www.farmacotherapeutischkompas.nl"

OUTPUT_COLUMNS = [
    "PCID",
    "COUNTRY",
    "COMPANY",
    "BRAND NAME",
    "GENERIC NAME",
    "PATIENT POPULATION",
    "INDICATION",
    "REIMBURSABLE STATUS",
    "Pack details",
    "ROUTE OF ADMINISTRATION",
    "STRENGTH SIZE",
    "BINDING",
    "REIMBURSEMENT BODY",
    "REIMBURSEMENT DATE",
    "REIMBURSEMENT STATUS",
    "REIMBURSEMENT URL",
]

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


# -------------------------
# Helpers
# -------------------------
def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def abs_url(href: str) -> str:
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return BASE + href
    return BASE + "/" + href


def safe_write_lines(path: str, lines: Iterable[str]) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(line.rstrip("\n") + "\n")


def read_lines(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip()]


def split_strengths(raw: str) -> List[str]:
    """
    Examples:
      "2.5 mg, 10 mg, 15 mg, 20 mg" -> ["2.5 MG","10 MG","15 MG","20 MG"]
      "1 mg/ml" -> ["1 MG/ML"]
    """
    raw = norm_space(raw)
    if not raw:
        return []
    # split by comma+space or semicolon to avoid splitting decimal commas like 2,5
    parts = [norm_space(p) for p in re.split(r",\s+|;", raw) if norm_space(p)]
    if not parts:
        return [raw.upper()]
    return [p.upper() for p in parts]


def route_from_dosage_form(dosage_form: str) -> str:
    # FK dosage forms for this use-case are oral (tablets/suspension).
    if not dosage_form:
        return "ORAL"
    df = dosage_form.lower()
    if "tablet" in df or "suspension" in df or "granulate" in df:
        return "ORAL"
    return "ORAL"


def pack_details(brand: str, dosage_form: str, strength: str) -> str:
    b = (brand or "").upper().strip()
    df = (dosage_form or "").lower()
    st = (strength or "").upper().strip()

    if "tablet" in df:
        # "XARELTO TABLETS 10 MG"
        return f"{b} TABLETS {st}"
    if "granulate" in df or "suspension" in df:
        # "XARELTO SUSPENSION FOR ORAL USE 1 MG / ML"
        st2 = st.replace("MG/ML", "MG / ML")
        return f"{b} SUSPENSION FOR ORAL USE {st2}"
    # fallback
    return f"{b} {st}".strip()


def normalize_company(manfact: str) -> str:
    # Your sample standardizes "Bayer BV" to "BAYER"
    m = norm_space(manfact)
    if not m:
        return ""
    if "bayer" in m.lower():
        return "BAYER"
    return m.upper()


def likely_generic_from_title(soup: BeautifulSoup) -> str:
    # <title>rivaroxaban</title>
    title = soup.title.get_text(strip=True) if soup.title else ""
    return title.upper().strip() if title else ""


# -------------------------
# Phase 1: URL discovery
# -------------------------
def expand_all_sections(page) -> None:
    """
    Tries to expand all collapsible sections.
    1. Looks for the 'Unfold all sections' button (common on product pages).
    2. Looks for closed section headers (common on listing pages) and clicks them.
    3. Repeats to handle nested or lazy-loaded headers.
    """
    # Helper to check if page is still valid
    def is_page_valid() -> bool:
        try:
            # Simple check to see if page is still accessible
            page.evaluate("1")
            return True
        except Exception:
            return False
    
    # 1. Product page "Unfold all" button
    try:
        if not is_page_valid():
            return
        btn = page.locator("#button-open-all-sections")
        try:
            count = btn.count()
            if count > 0:
                btn.first.click(timeout=3000)
                time.sleep(1.0)
        except Exception:
            pass
    except Exception:
        pass

    # 2. Listing page collapsible blocks
    # We loop up to 10 times to handle content that loads more collapsible sections
    for attempt in range(1, 11):
        try:
            if not is_page_valid():
                print(f"[DEBUG] Page closed during expansion attempt {attempt}")
                break
                
            closed_selectors = [
                "section.pat-collapsible.closed h3",
                ".pat-collapsible.closed h3",
                "h3.collapsible-closed",
                "section.closed h3",
                "h3.pat-collapsible-header.closed"
            ]
            
            found_any_this_pass = False
            for sel in closed_selectors:
                try:
                    if not is_page_valid():
                        break
                    headers = page.locator(sel)
                    try:
                        count = headers.count()
                    except Exception:
                        # Page might have been closed during count()
                        continue
                    if count > 0:
                        print(f"[INFO] Expansion pass {attempt}: Found {count} closed sections with selector '{sel}'.")
                        found_any_this_pass = True
                        for i in range(count):
                            try:
                                if not is_page_valid():
                                    break
                                # Use evaluate if click() is blocked or slow
                                headers.nth(i).click(timeout=1000)
                                time.sleep(0.1) # small throttle
                            except Exception:
                                pass
                except Exception:
                    # Continue to next selector if this one fails
                    continue
            
            if not found_any_this_pass:
                break
            
            # Wait for content to load after expansion pass
            time.sleep(1.5)
        except Exception as e:
            print(f"[DEBUG] Expansion attempt {attempt} error: {e}")
            break


def auto_scroll(page, max_loops: int = 50, sleep_s: float = 0.4) -> None:
    """
    Scroll down to load more items (if infinite/virtual lists).
    """
    last_height = 0
    stuck = 0
    for _ in range(max_loops):
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(sleep_s)
            new_height = page.evaluate("document.body.scrollHeight")
            if new_height == last_height:
                stuck += 1
            else:
                stuck = 0
            last_height = new_height
            if stuck >= 3:
                break
        except Exception as e:
            # Page might have been closed
            print(f"[DEBUG] Scroll error (page may be closed): {e}")
            break


def collect_detail_links_from_listing(
    start_url: str,
    headless: bool = True,
    limit: Optional[int] = None,
    only_contains: Optional[str] = None,
) -> List[str]:
    """
    Collects unique URLs matching /bladeren/preparaatteksten/...
    """
    only_contains_lc = only_contains.lower().strip() if only_contains else None

    links: Set[str] = set()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        ctx = browser.new_context(user_agent=DEFAULT_HEADERS["User-Agent"], locale="en-US")
        page = ctx.new_page()

        page.goto(start_url, wait_until="domcontentloaded", timeout=60000)
        time.sleep(1.0)

        # sometimes cookie banner blocks clicks; try generic accept
        for sel in [
            "button:has-text('Accept')",
            "button:has-text('Akkoord')",
            "button:has-text('OK')",
            "button:has-text('I agree')",
        ]:
            try:
                b = page.locator(sel)
                if b.count() > 0:
                    b.first.click(timeout=1500)
                    time.sleep(0.5)
                    break
            except Exception:
                pass

        expand_all_sections(page)
        # For 4000+ items, we scroll more and wait longer
        print("[INFO] Scrolling to load lazy content...")
        auto_scroll(page, max_loops=200, sleep_s=0.6)

        # Optimization: collect all hrefs at once via JS for speed (4000+ links)
        raw_hrefs = page.evaluate("() => Array.from(document.querySelectorAll('a[href]')).map(a => a.getAttribute('href'))")
        print(f"[INFO] Found {len(raw_hrefs)} total anchors on page. Filtering...")
        
        for href in raw_hrefs:
            if not href: continue
            
            # Pattern check: detail pages are /bladeren/preparaatteksten/LETTER/PRODUCT
            # Navigation/Group pages contain /groep/ or /alfabet/ or are the base list
            if "/bladeren/preparaatteksten/" not in href: continue
            
            # Exclude non-product links
            if "/groep/" in href: continue
            if "/alfabet/" in href and href.rstrip("/").split("/")[-1] in "abcdefghijklmnopqrstuvwxyz5": 
                continue # This is a letter sub-header or letter link
            if "#medicine-listing" in href: continue
            if "/zoeken" in href: continue
            
            full = abs_url(href)
            if only_contains_lc and only_contains_lc not in full.lower(): continue
            links.add(full)
            if limit and len(links) >= limit: break

        ctx.close()
        browser.close()

    return sorted(links)


# -------------------------
# Phase 2: Parse + derive rows
# -------------------------
@dataclass
class ProductCore:
    generic_name: str
    brand_name: str
    manufacturer: str
    dosage_form: str
    strengths: List[str]
    reimbursement_status: str = "REIMBURSED"  # Default to reimbursed

def parse_reimbursement_status(rcp) -> str:
    """
    Parse reimbursement status from a recipe section.
    Looks for XGVS, GVS, Bijlage 2, OTC symbols.
    Returns: "REIMBURSED", "NOT REIMBURSED", "CONDITIONAL", or "OTC"
    """
    # Check for XGVS (not in GVS = not reimbursed)
    xgvs = rcp.select_one("span.xgvs")
    if xgvs:
        return "NOT REIMBURSED"
    
    # Check for Bijlage 2 (conditional reimbursement)
    bijlage2 = rcp.select_one("span.bijlage2") or rcp.select_one("span.bijlage-2")
    if bijlage2:
        return "CONDITIONAL"
    
    # Check for OTC (over the counter)
    otc = rcp.select_one("span.otc")
    if otc:
        return "OTC"
    
    # If no XGVS symbol found, product is reimbursed
    return "REIMBURSED"


def parse_all_compositions(soup: BeautifulSoup) -> List[ProductCore]:
    """
    Finds ALL product recipes in the composition div.
    Each recipe can have multiple dosage forms (dl.details).
    Returns a list of ProductCore entries.
    """
    generic_guess = likely_generic_from_title(soup)
    # find composition div
    comp_div = None
    for div in soup.find_all("div", id=True):
        if str(div.get("id", "")).endswith("-samenstelling"):
            comp_div = div
            break
            
    results: List[ProductCore] = []
    if not comp_div:
        # Fallback to title-only
        return [ProductCore(generic_name=generic_guess, brand_name="", manufacturer="", dosage_form="", strengths=[])]

    recipes = comp_div.select("section.recipe")
    for rcp in recipes:
        # Extract brand and manufacturer from h3
        name_span = rcp.select_one("span.name")
        manf_span = rcp.select_one("span.manfact")
        name = norm_space(name_span.get_text(" ", strip=True)) if name_span else ""
        manf = norm_space(manf_span.get_text(" ", strip=True)) if manf_span else ""
        
        # Parse reimbursement status for this recipe
        reimb_status = parse_reimbursement_status(rcp)
        
        # Each recipe can have multiple "doses" blocks (dl.details)
        dose_blocks = rcp.select("dl.details")
        if not dose_blocks:
            # Maybe just one block without the wrapper? Check for application/concentration directly
            dose_blocks = [rcp]

        for block in dose_blocks:
            app_dd = block.select_one("dt.application + dd")
            conc_dd = block.select_one("dt.concentration + dd")
            
            if not app_dd and not conc_dd:
                continue
                
            dosage_form = norm_space(app_dd.get_text(" ", strip=True)) if app_dd else ""
            strengths_raw = norm_space(conc_dd.get_text(" ", strip=True)) if conc_dd else ""
            strengths = split_strengths(strengths_raw)
            
            results.append(ProductCore(
                generic_name=generic_guess,
                brand_name=(name or "").upper().strip(),
                manufacturer=norm_space(manf),
                dosage_form=dosage_form,
                strengths=strengths,
                reimbursement_status=reimb_status,
            ))
            
    if not results:
        results.append(ProductCore(generic_name=generic_guess, brand_name="", manufacturer="", dosage_form="", strengths=[]))
        
    return results


def fetch_html(url: str, timeout: int = 40) -> str:
    r = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text


# Removed old parse_composition_core in favor of parse_all_compositions


def parse_indications_by_population(soup: BeautifulSoup) -> Dict[str, List[str]]:
    """
    From the Indications module:
      <div id="rivaroxaban-indicaties"> ... <h4 class="list-header">In adults</h4> <ul><li>...</li></ul> ...
    Returns:
      {"ADULTS": [...], "CHILDREN": [...]}
    
    Updated: Also handles dosage-form headers (e.g., "Tablet en drank", "Injectievloeistof")
    """
    indic_div = None
    for div in soup.find_all("div", id=True):
        if str(div.get("id", "")).endswith("-indicaties"):
            indic_div = div
            break
    out: Dict[str, List[str]] = {}

    if not indic_div:
        return out

    # Find h4.list-header then following ul
    headers = indic_div.select("h4.list-header")
    if headers:
        for h in headers:
            header_txt = norm_space(h.get_text(" ", strip=True)).lower()
            pops = []
            
            # Check for population-based headers
            if "adult" in header_txt or "volwassene" in header_txt:
                pops.append("ADULTS")
                # Always include ELDERLY for adult indications as per sample requirements
                pops.append("ELDERLY")
            elif "children" in header_txt or "child" in header_txt or "kind" in header_txt or "adolescent" in header_txt or "jongere" in header_txt:
                pops.append("CHILDREN")
            elif "infant" in header_txt or "baby" in header_txt or "zuigeling" in header_txt or "neonate" in header_txt:
                pops.append("INFANTS")
            else:
                # Dosage form headers (e.g., "Tablet en drank", "Injectievloeistof")
                # Assume these are for adults/elderly if no population specified
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
        # Fallback for pages without population headers (e.g. natural/herbal products)
        # We assume ADULTS/ELDERLY if not specified.
        # Find all ul/li in the indications div
        all_lis = indic_div.select("ul li")
        if not all_lis:
            # Maybe just paragraphs?
            paras = [norm_space(p.get_text(" ", strip=True)) for p in indic_div.find_all("p") if p.get_text(strip=True)]
            # Often the first paragraph is "Traditional herbal medicine used for:", so we might want to skip it if it's short
            # But safer to just take everything if no bullets exist.
            if paras:
                items = paras
            else:
                items = []
        else:
            items = [norm_space(li.get_text(" ", strip=True)) for li in all_lis]
            items = [it for it in items if it]

        if items:
            out["ADULTS"] = items
            out["ELDERLY"] = items.copy()

    # de-dup while preserving order
    for pop, items in out.items():
        seen = set()
        dedup = []
        for it in items:
            key = it.lower()
            if key in seen:
                continue
            seen.add(key)
            dedup.append(it)
        out[pop] = dedup

    return out


def derive_rows(detail_url: str) -> List[Dict[str, str]]:
    """
    Fetches product detail page and derives CSV rows.
    Handles multiple product variants and population segments.
    """
    try:
        html = fetch_html(detail_url)
    except Exception as e:
        print(f"[ERROR] Failed to fetch {detail_url}: {e}")
        return []

    soup = BeautifulSoup(html, "lxml")

    all_variants = parse_all_compositions(soup)
    indications = parse_indications_by_population(soup)

    rows: List[Dict[str, str]] = []
    
    for core in all_variants:
        # Extract indications. If none found, create at least one row for the product
        if not indications:
            # Add one empty indication row per strength
            for st in (core.strengths or [""]):
                rows.append(create_row_dict(core, "", "", st, detail_url))
            continue

        for pop, indic_list in indications.items():
            # Join all indication bullets into a single string for one cell
            full_indic = " ; ".join(indic_list)
            for st in (core.strengths or [""]):
                rows.append(create_row_dict(core, pop, full_indic, st, detail_url))
                    
    return rows

def translate_dutch_to_english(text: str) -> str:
    """
    Translate common Dutch medical terms to English.
    This is a basic translation - for production, consider using a translation API.
    """
    if not text:
        return text
    
    # Common Dutch to English translations for medical terms
    translations = {
        # Diseases and conditions
        "schizofrenie": "schizophrenia",
        "bipolaire stoornis": "bipolar disorder",
        "depressie": "depression",
        "angststoornis": "anxiety disorder",
        "paniekstoornis": "panic disorder",
        "psychose": "psychosis",
        "epilepsie": "epilepsy",
        "ziekte van parkinson": "Parkinson's disease",
        "alzheimer": "Alzheimer's disease",
        "dementie": "dementia",
        "diabetes mellitus": "diabetes mellitus",
        "hypertensie": "hypertension",
        "hartfalen": "heart failure",
        "myocardinfarct": "myocardial infarction",
        "angina pectoris": "angina pectoris",
        "atriumfibrilleren": "atrial fibrillation",
        "trombo-embolie": "thromboembolism",
        "beroerte": "stroke",
        "migraine": "migraine",
        "hoofdpijn": "headache",
        "pijn": "pain",
        "koorts": "fever",
        "infectie": "infection",
        "ontsteking": "inflammation",
        "pneumonie": "pneumonia",
        "bronchitis": "bronchitis",
        "astma": "asthma",
        "copd": "COPD",
        "longembolie": "pulmonary embolism",
        "tuberculose": "tuberculosis",
        "hepatitis": "hepatitis",
        "cirrose": "cirrhosis",
        "nierfalen": "kidney failure",
        "urineweginfectie": "urinary tract infection",
        "prostaatcarcinoom": "prostate carcinoma",
        "borstkanker": "breast cancer",
        "longkanker": "lung cancer",
        "darmkanker": "colorectal cancer",
        "melanoom": "melanoma",
        "leukemie": "leukemia",
        "lymfoom": "lymphoma",
        "multipel myeloom": "multiple myeloma",
        "kanker": "cancer",
        "reumatoide artritis": "rheumatoid arthritis",
        "artrose": "osteoarthritis",
        "multiple sclerose": "multiple sclerosis",
        
        # Populations
        "volwassenen": "adults",
        "volwassene": "adult",
        "kinderen": "children",
        "kind": "child",
        "adolescenten": "adolescents",
        "jongeren": "young people",
        "ouderen": "elderly",
        "neonaten": "neonates",
        "zuigelingen": "infants",
        
        # Treatment terms
        "behandeling": "treatment",
        "therapie": "therapy",
        "profylaxe": "prophylaxis",
        "preventie": "prevention",
        "onderhoudsbehandeling": "maintenance treatment",
        "onderhoud": "maintenance",
        "kortdurende": "short-term",
        "langdurige": "long-term",
        "chronische": "chronic",
        "acuut": "acute",
        "ernstige": "severe",
        "matige": "moderate",
        "lichte": "mild",
        
        # General terms
        "bij": "in",
        "met": "with",
        "en": "and",
        "of": "or",
        "voor": "for",
        "als": "as",
        "die": "who",
        "waarbij": "where",
        "waarvan": "of which",
    }
    
    # Perform translation (case-insensitive)
    result = text
    for dutch, english in translations.items():
        # Word boundary matching for whole words
        pattern = r'\b' + re.escape(dutch) + r'\b'
        result = re.sub(pattern, english, result, flags=re.IGNORECASE)
    
    return result


def create_row_dict(core: ProductCore, pop: str, indic: str, strength: str, url: str) -> Dict[str, str]:
    route = route_from_dosage_form(core.dosage_form)
    pack = pack_details(core.brand_name or core.generic_name, core.dosage_form, strength)
    
    # Translate indication from Dutch to English
    translated_indic = translate_dutch_to_english(indic)
    
    # Determine reimbursable status text
    reimb_text = "Reimbursed" if core.reimbursement_status == "REIMBURSED" else "Not Reimbursed"
    
    return {
        "PCID": "",
        "COUNTRY": "NETHERLANDS",
        "COMPANY": normalize_company(core.manufacturer),
        "BRAND NAME": core.brand_name.upper() if core.brand_name else "",
        "GENERIC NAME": core.generic_name.upper() if core.generic_name else "",
        "PATIENT POPULATION": pop,
        "INDICATION": translated_indic,
        "REIMBURSABLE STATUS": reimb_text,
        "Pack details": pack,
        "ROUTE OF ADMINISTRATION": route,
        "STRENGTH SIZE": strength,
        "BINDING": "NO",
        "REIMBURSEMENT BODY": "MINISTRY OF HEALTH",
        "REIMBURSEMENT DATE": "",
        "REIMBURSEMENT STATUS": core.reimbursement_status,
        "REIMBURSEMENT URL": url,
    }



def write_csv(path: str, rows: List[Dict[str, str]]) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in OUTPUT_COLUMNS})


# -------------------------
# Main
# -------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-url", default=DEFAULT_START_URL, help="Listing page for Phase 1 URL collection")
    ap.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    ap.add_argument("--headed", action="store_true", help="Run browser headed (overrides --headless)")
    ap.add_argument("--limit", type=int, default=0, help="Limit number of detail URLs collected")
    ap.add_argument("--only", default="", help="Collect only URLs containing this substring (e.g., rivaroxaban)")
    ap.add_argument("--urls-out", default="detail_urls.txt", help="Where to save collected detail URLs")
    ap.add_argument("--phase2-only", action="store_true", help="Skip Phase 1, read URLs from --urls-file")
    ap.add_argument("--urls-file", default="detail_urls.txt", help="Input URLs file for --phase2-only")
    ap.add_argument("--out-csv", default="reimbursement_rows.csv", help="Output CSV path")
    ap.add_argument("--workers", type=int, default=10, help="Number of concurrent workers for Phase 2")
    args = ap.parse_args()

    headless = True
    if args.headed:
        headless = False
    elif args.headless:
        headless = True

    detail_urls: List[str] = []
    if args.phase2_only:
        detail_urls = read_lines(args.urls_file)
        if not detail_urls:
            print(f"[ERROR] No URLs found in {args.urls_file}")
            return 2
    else:
        limit = args.limit if args.limit and args.limit > 0 else None
        only = args.only.strip() or None
        print(f"[INFO] Phase 1: Collecting detail URLs from: {args.start_url}")
        detail_urls = collect_detail_links_from_listing(
            start_url=args.start_url,
            headless=headless,
            limit=limit,
            only_contains=only,
        )
        print(f"[INFO] Phase 1: Collected {len(detail_urls)} detail URLs")
        safe_write_lines(args.urls_out, detail_urls)
        print(f"[INFO] Saved URLs to: {args.urls_out}")

    # Phase 2
    all_rows: List[Dict[str, str]] = []
    print(f"[INFO] Phase 2: Building reimbursement rows from {len(detail_urls)} URLs")
    
    # Using ThreadPoolExecutor to handle 4000+ URLs concurrently
    max_workers = args.workers if hasattr(args, 'workers') else 10
    total = len(detail_urls)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(derive_rows, url): url for url in detail_urls}
        
        processed = 0
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            processed += 1
            try:
                rows = future.result()
                all_rows.extend(rows)
            except Exception as e:
                print(f"[WARN] Failed URL ({processed}/{total}): {url}\n       {type(e).__name__}: {e}")
            
            if processed % 20 == 0:
                print(f"[INFO] Progress: {processed}/{total} URLs processed, rows so far: {len(all_rows)}")

    write_csv(args.out_csv, all_rows)
    print(f"[DONE] Wrote {len(all_rows)} rows to: {args.out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
