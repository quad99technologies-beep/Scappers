"""
Script 4: Extract Tender Award Data
===================================
Extracts supplier bid and award information from MercadoPublico award pages.

INPUTS:
  - Output/tender_redirect_urls.csv: Redirect URLs with qs parameters from Script 2
    Required columns: tender_details_url, tender_award_url

OUTPUTS:
  - Output/mercadopublico_supplier_rows.csv: Individual supplier bid rows
  - Output/mercadopublico_lot_summary.csv: Aggregated lot award summaries
"""

from __future__ import annotations

import re
import csv
import time
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

_script_dir = Path(__file__).resolve().parent
_scraper_root = _script_dir
if str(_scraper_root) not in sys.path:
    sys.path.insert(0, str(_scraper_root))

from config_loader import load_env_file, getenv, getenv_bool, getenv_int, get_output_dir

try:
    from core.chrome_pid_tracker import get_chrome_pids_from_driver, save_chrome_pids, terminate_scraper_pids
except Exception:
    get_chrome_pids_from_driver = None
    save_chrome_pids = None
    terminate_scraper_pids = None

# Constants
load_env_file()
INPUT_FILENAME = getenv("SCRIPT_02_OUTPUT_CSV", "tender_redirect_urls.csv")
SUPPLIER_OUTPUT_FILENAME = getenv("SCRIPT_04_SUPPLIER_OUTPUT_CSV", "mercadopublico_supplier_rows.csv")
LOT_SUMMARY_OUTPUT_FILENAME = getenv("SCRIPT_04_LOT_OUTPUT_CSV", "mercadopublico_lot_summary.csv")
REQUIRED_INPUT_COLUMNS = ['tender_details_url', 'tender_award_url']
HEADLESS = getenv_bool("HEADLESS", True)
WAIT_SECONDS = getenv_int("WAIT_SECONDS", 60)
DISABLE_IMAGES = getenv_bool("DISABLE_IMAGES", True)
DISABLE_CSS = getenv_bool("DISABLE_CSS", True)


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

    # handle weird "4123 20" -> "412320"
    if re.fullmatch(r"\d+\s+\d{1,2}", s):
        s = s.replace(" ", "")

    if not s:
        return None

    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    else:
        if "," in s:
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


def build_driver() -> webdriver.Chrome:
    opts = Options()
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    prefs = {}
    if DISABLE_IMAGES:
        prefs["profile.managed_default_content_settings.images"] = 2
    if DISABLE_CSS:
        prefs["profile.managed_default_content_settings.stylesheets"] = 2
    if prefs:
        opts.add_experimental_option("prefs", prefs)

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(120)
    if get_chrome_pids_from_driver and save_chrome_pids:
        try:
            pids = get_chrome_pids_from_driver(driver)
            if pids:
                save_chrome_pids("Tender_Chile", _scraper_root.parents[1], pids)
        except Exception:
            pass
    return driver


def wait_for_result_table(driver: webdriver.Chrome) -> None:
    start = time.time()
    while True:
        try:
            WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.ID, "grdItemOC")))
            return
        except Exception:
            if time.time() - start > WAIT_SECONDS:
                raise RuntimeError("Timed out waiting for #grdItemOC. Page may be blocked or slow.")
            time.sleep(1)


def extract_award_date(html: str) -> Optional[str]:
    text = clean_text(BeautifulSoup(html, "lxml").get_text(" ", strip=True))

    m = re.search(
        r"\b(?:In|En)\s+[A-Za-zÁÉÍÓÚÜÑáéíóúüñ\.\-\s]+,\s*(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})\b",
        text,
        flags=re.IGNORECASE,
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
    """
    FIXED: must not treat "Not Awarded" as awarded.
    """
    s = clean_text(state).lower()

    # negatives first
    if re.search(r"\bnot\s+awarded\b", s):
        return False
    if re.search(r"\bno\s+adjudic", s):  # e.g. No adjudicada
        return False
    if re.search(r"\bno\s+award", s):
        return False

    # positives
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


def extract_supplier_rows_and_lot_summary(html: str, award_url: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    soup = BeautifulSoup(html, "lxml")

    grd = soup.find("table", id="grdItemOC")
    if not grd:
        raise RuntimeError("Could not find Result of the Award table (id='grdItemOC').")

    award_date = extract_award_date(html)

    gv_tables = grd.find_all("table", id=lambda x: x and x.endswith("_gvLines"))
    if not gv_tables:
        raise RuntimeError("Found #grdItemOC but no bidder tables (*_gvLines).")

    lots: Dict[str, Dict[str, Any]] = {}

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
                "award_date": award_date,
                "source_url": award_url,
                "lot_number": lot_number,
                "un_classification_code": un_code,
                "item_title": item_title,
                "buyer_specifications": buyer_spec,
                "lot_quantity": lot_quantity,
                "lot_total_line_raw": lot_total_line_raw,
                "lot_total_line": lot_total_line,
                "supplier_rows": [],
            }

        tr_list = gv.find_all("tr")
        if not tr_list or len(tr_list) < 2:
            continue

        for tr in tr_list[1:]:
            tds = tr.find_all("td", recursive=False)
            if len(tds) < 6:
                continue

            supplier = clean_text(tds[0].get_text(" ", strip=True))
            supplier_specs = clean_text(tds[1].get_text(" ", strip=True))
            unit_offer_raw = clean_text(tds[2].get_text(" ", strip=True))
            awarded_qty_raw = clean_text(tds[3].get_text(" ", strip=True))
            total_net_awarded_raw = clean_text(tds[4].get_text(" ", strip=True))
            state = clean_text(tds[5].get_text(" ", strip=True))

            unit_offer_num = parse_locale_number(unit_offer_raw)
            awarded_qty_num = parse_locale_number(awarded_qty_raw)
            total_net_awarded_num = parse_locale_number(total_net_awarded_raw)

            awarded = is_awarded_state(state)

            lots[lot_number]["supplier_rows"].append({
                "award_date": award_date,
                "source_url": award_url,
                "lot_number": lot_number,
                "un_classification_code": un_code,
                "item_title": item_title,
                "buyer_specifications": buyer_spec,
                "lot_quantity": lot_quantity,
                "supplier": supplier,
                "supplier_specifications": supplier_specs,
                "unit_price_offer_raw": unit_offer_raw,
                "unit_price_offer": unit_offer_num,
                "awarded_quantity_raw": awarded_qty_raw,
                "awarded_quantity": awarded_qty_num,
                "total_net_awarded_raw": total_net_awarded_raw,
                "total_net_awarded": total_net_awarded_num,
                "state": state,
                "is_awarded": "YES" if awarded else "NO",
                "awarded_unit_price": unit_offer_num if awarded else None,
                "lot_total_line_raw": lot_total_line_raw,
                "lot_total_line": lot_total_line,
            })

    supplier_rows: List[Dict[str, Any]] = []
    lot_summary_rows: List[Dict[str, Any]] = []

    def lot_sort_key(x: str) -> tuple:
        return (x == "" or x is None, int(x) if str(x).isdigit() else 999999, str(x))

    no_award_lots: List[str] = []

    for lot_no in sorted(lots.keys(), key=lot_sort_key):
        lot = lots[lot_no]
        rows = lot["supplier_rows"]
        supplier_rows.extend(rows)

        awarded_rows = [r for r in rows if r.get("is_awarded") == "YES"]
        has_award = "YES" if awarded_rows else "NO"
        if has_award == "NO":
            no_award_lots.append(lot_no)

        # if multiple awarded rows (rare), keep first + you can expand later
        first_awarded = awarded_rows[0] if awarded_rows else {}

        lot_summary_rows.append({
            "award_date": lot["award_date"],
            "source_url": lot["source_url"],
            "lot_number": lot["lot_number"],
            "un_classification_code": lot["un_classification_code"],
            "item_title": lot["item_title"],
            "buyer_specifications": lot["buyer_specifications"],
            "lot_quantity": lot["lot_quantity"],
            "lot_total_line_raw": lot["lot_total_line_raw"],
            "lot_total_line": lot["lot_total_line"],
            "HAS_AWARD": has_award,
            "LOT_RESULT": "Awarded" if has_award == "YES" else "No Award",
            "AWARDED_SUPPLIER": first_awarded.get("supplier", ""),
            "AWARDED_UNIT_PRICE": first_awarded.get("awarded_unit_price", None),
            "AWARDED_AMOUNT": first_awarded.get("total_net_awarded", None),
        })

    print("\n================ SUMMARY ================")
    print(f"Award date (minutes header): {award_date}")
    print(f"Total lots: {len(lots)}")
    print(f"No Award lots ({len(no_award_lots)}): {sorted(no_award_lots, key=lot_sort_key)}")
    print("========================================\n")

    return supplier_rows, lot_summary_rows


def write_csv(path: Path, data: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(data)


def get_award_url_from_details_url(details_url: str) -> Optional[str]:
    """Convert DetailsAcquisition URL to PreviewAwardAct URL"""
    if 'qs=' in details_url:
        match = re.search(r'qs=([^&]+)', details_url)
        if match:
            qs = match.group(1)
            return f"https://www.mercadopublico.cl/Procurement/Modules/RFB/StepsProcessAward/PreviewAwardAct.aspx?qs={qs}"
    
    if 'idlicitacion=' in details_url:
        match = re.search(r'idlicitacion=([^&]+)', details_url)
        if match:
            lic_id = match.group(1)
            return f"https://www.mercadopublico.cl/Procurement/Modules/RFB/StepsProcessAward/PreviewAwardAct.aspx?idlicitacion={lic_id}"
    
    return None


def extract_single_award(award_url: str, headless: bool = False) -> Optional[Dict[str, Any]]:
    """Extract award data from a single award URL"""
    driver = build_driver()
    try:
        print(f"   Opening: {award_url[:60]}...")
        driver.get(award_url)

        # Award URL already has qs parameter from Script 2, so no need to get redirect again
        # Just wait for page to load
        wait_for_result_table(driver)
        time.sleep(2)

        # Use the award_url (which already has qs parameter) as the source URL
        source_url = award_url

        html = driver.page_source
        supplier_rows, lot_summary_rows = extract_supplier_rows_and_lot_summary(html, source_url)

        if not supplier_rows:
            print(f"   [WARN]  No supplier rows extracted")
            return None
        if not lot_summary_rows:
            print(f"   [WARN]  No lot summary rows built")
            return None

        return {
            "supplier_rows": supplier_rows,
            "lot_summary": lot_summary_rows,
            "award_date": lot_summary_rows[0].get("award_date") if lot_summary_rows else None
        }

    except Exception as e:
        print(f"   [ERROR] Error: {e}")
        return None
    finally:
        driver.quit()


def validate_input_file(input_path: Path) -> bool:
    """Validate that input CSV exists and has required columns"""
    if not input_path.exists():
        print(f"[ERROR] ERROR: {input_path} not found. Run Script 2 first.")
        return False
    
    try:
        with open(input_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            
            # Check for required columns
            missing_cols = [col for col in REQUIRED_INPUT_COLUMNS if col not in fieldnames]
            if missing_cols:
                print(f"[ERROR] ERROR: Missing required columns in {input_path}: {', '.join(missing_cols)}")
                print(f"   Available columns: {', '.join(fieldnames)}")
                return False
    except Exception as e:
        print(f"[ERROR] ERROR: Cannot read {input_path}: {e}")
        return False
    
    return True


def main():
    output_dir = get_output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)

    if terminate_scraper_pids:
        try:
            terminate_scraper_pids("Tender_Chile", _scraper_root.parents[1], silent=True)
        except Exception:
            pass
    
    # Check if tender_redirect_urls.csv exists (preferred - has qs parameters)
    input_path = output_dir / INPUT_FILENAME
    use_redirect_csv = input_path.exists()
    
    if use_redirect_csv:
        if not validate_input_file(input_path):
            sys.exit(1)
        
        print(f"[INFO] Reading redirect URLs from: {input_path}")
        print("=" * 80)
        
        # Read award URLs directly from redirect CSV (has qs parameters)
        tender_award_pairs = []
        with open(input_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                details_url = row.get('tender_details_url', '').strip()
                award_url = row.get('tender_award_url', '').strip()
                if details_url and award_url:
                    tender_award_pairs.append((details_url, award_url))
        
        if not tender_award_pairs:
            print("[ERROR] No tender/award URL pairs found in CSV")
            sys.exit(1)
        
        print(f"[OK] Found {len(tender_award_pairs)} tender/award URL pairs")
    else:
        # Fallback to tender_details.csv
        tender_details_path = output_dir / "tender_details.csv"
        if not tender_details_path.exists():
            print(f"[ERROR] ERROR: {tender_details_path} not found. Run Script 3 first.")
            sys.exit(1)
        
        print(f"[INFO] Reading tender details from: {tender_details_path}")
        print("=" * 80)
        
        # Read unique tender URLs from CSV
        tender_urls = set()
        with open(tender_details_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get('Source URL', '').strip()
                if url and 'DetailsAcquisition' in url:
                    tender_urls.add(url)
        
        if not tender_urls:
            print("[ERROR] No tender URLs found in CSV")
            sys.exit(1)
        
        print(f"[OK] Found {len(tender_urls)} unique tender URLs")
        
        # Convert to pairs
        tender_award_pairs = []
        for details_url in tender_urls:
            award_url = get_award_url_from_details_url(details_url)
            if award_url:
                tender_award_pairs.append((details_url, award_url))
    
    print("=" * 80)
    
    all_supplier_rows = []
    all_lot_summary_rows = []
    
    for i, (details_url, award_url) in enumerate(tender_award_pairs, 1):
        print(f"\n[{i}/{len(tender_award_pairs)}] Processing tender...")
        
        if not award_url:
            print(f"   [WARN]  No award URL for: {details_url[:60]}...")
            continue
        
        result = extract_single_award(award_url, headless=HEADLESS)
        if result:
            # Add source URL to each row
            for row in result["supplier_rows"]:
                row["source_tender_url"] = details_url
            for row in result["lot_summary"]:
                row["source_tender_url"] = details_url
            
            all_supplier_rows.extend(result["supplier_rows"])
            all_lot_summary_rows.extend(result["lot_summary"])
            print(f"   [OK] Extracted {len(result['supplier_rows'])} supplier rows, {len(result['lot_summary'])} lot summaries")
        else:
            print(f"   [WARN]  Failed to extract award data")
        
        time.sleep(2)  # Delay between requests
    
    if not all_supplier_rows:
        print("\n[ERROR] No supplier rows extracted")
        sys.exit(1)
    if not all_lot_summary_rows:
        print("\n[ERROR] No lot summary rows built")
        sys.exit(1)
    
    supplier_fields = list(all_supplier_rows[0].keys())
    summary_fields = list(all_lot_summary_rows[0].keys())
    
    supplier_csv = output_dir / SUPPLIER_OUTPUT_FILENAME
    lot_summary_csv = output_dir / LOT_SUMMARY_OUTPUT_FILENAME
    
    write_csv(supplier_csv, all_supplier_rows, supplier_fields)
    write_csv(lot_summary_csv, all_lot_summary_rows, summary_fields)
    
    print("\n" + "=" * 80)
    print(f"[OK] Supplier rows: {len(all_supplier_rows)} -> {supplier_csv}")
    print(f"[OK] Lot summary rows: {len(all_lot_summary_rows)} -> {lot_summary_csv}")
    print("=" * 80)


    if terminate_scraper_pids:
        try:
            terminate_scraper_pids("Tender_Chile", _scraper_root.parents[1], silent=True)
        except Exception:
            pass


if __name__ == "__main__":
    main()
