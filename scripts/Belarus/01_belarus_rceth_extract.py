# 01_belarus_rceth_extract.py
# Python 3.10+
# pip install requests beautifulsoup4 pandas lxml

import re
import time
import random
from datetime import datetime, timezone
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE = "https://www.rceth.by"
START_URL = "https://www.rceth.by/Refbank/reestr_drugregpricenew"
RESULTS_PATH = "/Refbank/reestr_drugregpricenew/results"

GENERIC_LIST_CSV = r"D:\quad99\Scappers\scripts\Belarus\Generic Name.csv" # input list of INNs
OUT_RAW = "belarus_rceth_raw.csv"

SESSION_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

USD_EQ_RE = re.compile(r"Equivalent price on registration date:\s*([0-9]+(?:[.,][0-9]+)?)\s*USD", re.I)
CONTRACT_CCY_RE = re.compile(r"Contract currency:\s*([A-Z]{3})", re.I)


def _sleep():
    time.sleep(random.uniform(0.6, 1.4))


def parse_price_cell(text: str):
    """
    Examples: "26,38 BYN" or "26.38 BYN"
    """
    if not text:
        return None, None
    t = " ".join(text.split())
    m = re.search(r"([0-9]+(?:[.,][0-9]+)?)\s*([A-Z]{3})", t)
    if not m:
        return None, None
    val = float(m.group(1).replace(",", "."))
    ccy = m.group(2)
    return val, ccy


def parse_import_price_usd(contract_info_text: str):
    """
    From 'Information about the maximum selling price in the contract currency' cell:
      Contract currency: USD
      Equivalent price on registration date: 8.33 USD
    """
    if not contract_info_text:
        return None, None

    t = " ".join(contract_info_text.split())
    ccy = None
    m_ccy = CONTRACT_CCY_RE.search(t)
    if m_ccy:
        ccy = m_ccy.group(1)

    m_usd = USD_EQ_RE.search(t)
    if m_usd:
        usd_val = float(m_usd.group(1).replace(",", "."))
        return usd_val, "USD"
    return None, None


def find_results_table(soup: BeautifulSoup):
    # RCETH results table usually has a standard class; fallback to first big table
    table = soup.find("table")
    if not table:
        return None
    return table


def extract_rows_from_page(html: str, search_inn: str, page_no: int, page_url: str):
    soup = BeautifulSoup(html, "lxml")
    table = find_results_table(soup)
    if not table:
        return []

    # Header -> column index mapping
    headers = []
    thead = table.find("thead")
    if thead:
        headers = [th.get_text(" ", strip=True) for th in thead.find_all("th")]
    else:
        # sometimes header is in first row
        first_tr = table.find("tr")
        if first_tr:
            headers = [td.get_text(" ", strip=True) for td in first_tr.find_all(["th", "td"])]

    # body rows
    tbody = table.find("tbody") or table
    trs = tbody.find_all("tr")
    out = []

    for tr in trs:
        tds = tr.find_all("td")
        if not tds:
            continue

        # Extract cells by position (based on screenshot ordering)
        # If site changes, we still store raw text and best-effort fields.
        cell_texts = [td.get_text("\n", strip=True) for td in tds]

        # best-effort indexes (based on visible columns)
        # 0: No p/n
        # 1: Trade Name (often link)
        # 2: INN
        # 3: Dosage form
        # 4: ATC code / Category
        # 5: MAH
        # 6: Producer
        # 7: Reg cert number (link)
        # 8: Maximum selling price
        # 9: Contract currency info (contains USD equiv)
        # 10: Registration info block
        # 11: Date of changes
        def safe(i):
            return cell_texts[i] if i < len(cell_texts) else ""

        trade_name = safe(1)
        inn = safe(2)
        dosage_form = safe(3)
        atc_cat = safe(4)
        mah = safe(5)
        producer = safe(6)
        reg_cert = safe(7)

        max_price_val, max_price_ccy = parse_price_cell(safe(8))
        import_price_usd, import_ccy = parse_import_price_usd(safe(9))

        reg_info = safe(10)
        date_changes = safe(11)

        # details url (if exists)
        details_url = ""
        a = tr.find("a", href=True)
        if a:
            href = a["href"]
            if "/details/" in href:
                details_url = urljoin(BASE, href)

        out.append({
            "search_inn_used": search_inn,
            "page_no": page_no,
            "page_url": page_url,

            "trade_name": trade_name,
            "inn": inn,
            "dosage_form": dosage_form,
            "atc_code_or_category": atc_cat,
            "marketing_authorization_holder": mah,
            "producer_raw": producer,
            "registration_certificate_number": reg_cert,

            "max_selling_price": max_price_val,
            "max_selling_price_currency": max_price_ccy,

            "import_price": import_price_usd,          # <-- NEW REQUIRED COLUMN
            "import_price_currency": import_ccy,       # usually USD

            "contract_currency_info_raw": safe(9),
            "max_price_registration_info_raw": reg_info,
            "date_of_changes_raw": date_changes,
            "details_url": details_url,

            "scraped_at_utc": datetime.now(timezone.utc).isoformat(),
            "row_raw_joined": " | ".join(cell_texts),
        })

    return out


def try_request(sess: requests.Session, method: str, url: str, **kwargs):
    # simple retry wrapper
    for attempt in range(1, 6):
        try:
            resp = sess.request(method, url, timeout=60, **kwargs)
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(2 * attempt)
                continue
            resp.raise_for_status()
            return resp
        except Exception:
            if attempt == 5:
                raise
            time.sleep(2 * attempt)
    raise RuntimeError("unreachable")


def scrape_for_inn(sess: requests.Session, inn_term: str):
    """
    Strategy:
    - Load start page to establish session/cookies
    - POST to /results with form fields (best-effort)
    - Then keep requesting next pages if pagination exists
    """
    try_request(sess, "GET", START_URL)
    _sleep()

    # Form fields are site-specific; this is best-effort for this RCETH module.
    # Common patterns:
    # - mnn = INN
    # - pageSize / size / limit = 100
    # - page / p = page number
    # We attempt several common combos. If site changes, adjust here.

    page = 1
    all_rows = []

    while True:
        form = {
            "mnn": inn_term,          # INN field per your HTML
            "page": str(page),
            "size": "100",
            "pageSize": "100",
        }

        url = urljoin(BASE, RESULTS_PATH)
        resp = try_request(sess, "POST", url, data=form)
        html = resp.text

        rows = extract_rows_from_page(html, inn_term, page, resp.url)

        # stop condition: no rows returned
        if not rows:
            break

        all_rows.extend(rows)

        # naive pagination stop:
        # if returned rows < 100 assume last page (works for most)
        if len(rows) < 100:
            break

        page += 1
        _sleep()

    return all_rows


def main():
    inn_df = pd.read_csv(GENERIC_LIST_CSV)
    # accept either "Generic Name" or first column
    if "Generic Name" in inn_df.columns:
        inns = inn_df["Generic Name"].dropna().astype(str).str.strip().tolist()
    else:
        inns = inn_df.iloc[:, 0].dropna().astype(str).str.strip().tolist()

    sess = requests.Session()
    sess.headers.update(SESSION_HEADERS)

    all_out = []
    for idx, inn_term in enumerate(inns, start=1):
        if not inn_term:
            continue
        print(f"[{idx}/{len(inns)}] Scraping INN: {inn_term}")
        try:
            rows = scrape_for_inn(sess, inn_term)
            print(f"  -> rows: {len(rows)}")
            all_out.extend(rows)
        except Exception as e:
            print(f"  !! Failed INN {inn_term}: {e}")

    df = pd.DataFrame(all_out)

    # Dedup best-effort
    if not df.empty:
        key_cols = ["registration_certificate_number", "trade_name", "dosage_form", "max_selling_price", "import_price"]
        for c in key_cols:
            if c not in df.columns:
                df[c] = ""
        df = df.drop_duplicates(subset=key_cols, keep="first")

    df.to_csv(OUT_RAW, index=False, encoding="utf-8-sig")
    print(f"\nSaved: {OUT_RAW} (rows={len(df)})")


if __name__ == "__main__":
    main()
