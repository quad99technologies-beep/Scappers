#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ontario EAP product prices scraper (ontario.ca)

Outputs a CSV with:
- Effective Start Date (from each table caption: "Effective date: ...")
- DIN, Trade name, Strength, Dosage form, DBP
- Derived pricing fields (VAT, RI)
- Reimbursement fields as per business rules

Run (live):
  python 02_ontario_eap_prices.py --out ontario_eap_prices.csv

Run (offline from saved HTML):
  python 02_ontario_eap_prices.py --html "Exceptional Access Program product prices _ ontario.ca.html" --out ontario_eap_prices.csv
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime
from typing import Optional, Tuple, List, Dict
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Add script directory to path for config_loader import
_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from config_loader import get_output_dir, EAP_PRICES_URL, EAP_PRICES_CSV_NAME


URL = EAP_PRICES_URL

VAT_MULTIPLIER = 1.08

REIMBURSABLE_STATUS = "FULLY REIMBURSABLE"
REIMBURSABLE_RATE = ""  # user didn't request any fixed numeric rate
REIMBURSABLE_NOTES = (
    "DRUGS FUNDED BY EAP. THERE EXISTS AN ADDITIONAL COPAYMENT BASED ON PATIENT'S AGE/INCOME "
    "($6.11/PRESCRIPTION FOR 65 YEARS & OLD, INCOME ABOVE $19,300; $2/PRESCRIPTION FOR 65 YEARS OLD, "
    "INCOME BELOW $19,300 & FOR POPULATION UNDER TRILLIUM DRUG PROGRAM)"
)
COPAYMENT_VALUE = 0.00


# ---------- helpers ----------

def fetch_html(url: str, timeout: int = 60) -> str:
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text


def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def parse_money_and_unit(raw: str) -> Tuple[Optional[float], Optional[str], str]:
    """
    Examples:
      "$1059.5629/mg" -> (1059.5629, "mg", "$1059.5629/mg")
      "$1424.5400/Pk" -> (1424.54, "Pk", "$1424.5400/Pk")
      "$1,555.17"     -> (1555.17, None, "$1,555.17")
    """
    raw0 = normalize_ws(raw)
    if not raw0:
        return None, None, raw0

    # Split on "/" if present
    unit = None
    money_part = raw0
    if "/" in raw0:
        parts = raw0.split("/", 1)
        money_part = parts[0].strip()
        unit = parts[1].strip()

    # Extract numeric
    m = re.search(r"([-+]?\d[\d,]*\.?\d*)", money_part.replace("$", ""))
    if not m:
        return None, unit, raw0
    val = float(m.group(1).replace(",", ""))
    return val, unit, raw0


def parse_effective_date(text: str) -> Optional[str]:
    """
    Input examples from caption:
      "Effective date: July 07, 2025"
      "Effective date February 26, 2021"  (sometimes missing colon in text sources)
    Return ISO: YYYY-MM-DD
    """
    t = normalize_ws(text)
    if not t:
        return None

    # capture the portion after "Effective date"
    m = re.search(r"Effective date\s*:?\s*(.+)$", t, flags=re.IGNORECASE)
    if not m:
        return None

    date_str = m.group(1).strip()
    # Ontario uses "Month DD, YYYY"
    for fmt in ("%B %d, %Y", "%B %d, %Y", "%b %d, %Y"):
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Some pages may have "July 07, 2025" with double space issues etc
    # Try a tolerant parse via regex (Month name, day, year)
    m2 = re.search(r"([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})", date_str)
    if m2:
        try:
            dt = datetime.strptime(f"{m2.group(1)} {int(m2.group(2)):02d}, {m2.group(3)}", "%B %d, %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return None

    return None


def has_pk_keyword(local_pack_description: str) -> bool:
    """
    User rule: "Only if Local Pack Description has PK keyword ..."
    On Ontario tables, "Pk" is used frequently (e.g., "Oral Sol-100mL Pk").
    We'll treat PK/Pk as a word token.
    """
    return bool(re.search(r"\bpk\b", local_pack_description or "", flags=re.IGNORECASE))


# ---------- scraper ----------

def scrape_from_html(html: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "lxml")

    tables = soup.select("table.table.full-width.numeric")
    rows_out: List[Dict[str, object]] = []

    for tbl in tables:
        caption = tbl.find("caption")
        cap_text = normalize_ws(caption.get_text(" ", strip=True) if caption else "")
        effective_start = parse_effective_date(cap_text)

        # Fallback: if caption missing for any reason, try to find "Effective date" near the table
        if not effective_start:
            prev_text = ""
            prev = tbl.find_previous(string=re.compile(r"Effective date", flags=re.IGNORECASE))
            if prev:
                prev_text = normalize_ws(str(prev))
            effective_start = parse_effective_date(prev_text)

        # Get header names (for safety)
        headers = [normalize_ws(th.get_text(" ", strip=True)) for th in tbl.select("thead th")]
        # Expected: DIN, Trade name, Strength, Dosage form, DBP
        # But we'll still read by column positions.

        for tr in tbl.select("tbody tr"):
            tds = tr.find_all(["td", "th"])
            if len(tds) < 5:
                continue

            din = normalize_ws(tds[0].get_text(" ", strip=True))
            trade_name = normalize_ws(tds[1].get_text(" ", strip=True))
            strength = normalize_ws(tds[2].get_text(" ", strip=True))
            dosage_form = normalize_ws(tds[3].get_text(" ", strip=True))
            dbp_raw = normalize_ws(tds[4].get_text(" ", strip=True))

            ex_factory, ex_factory_unit, _raw = parse_money_and_unit(dbp_raw)

            local_pack_description = normalize_ws(f"{strength} {dosage_form}").strip()
            pk_present = has_pk_keyword(local_pack_description)

            # Your required calculations
            public_with_vat = round(ex_factory * VAT_MULTIPLIER, 6) if ex_factory is not None else None
            # RI of the pack == Public with VAT
            ri_price = public_with_vat

            rows_out.append({
                "Effective Start Date": effective_start,          # <-- key fix
                "Effective End Date": "",                         # not provided on site
                "DIN": din,
                "Trade name": trade_name,
                "Strength": strength,
                "Dosage form": dosage_form,
                "DBP (raw)": dbp_raw,

                # Map DBP to Ex Factory Wholesale Price (your naming)
                "Ex Factory Wholesale Price": ex_factory,
                "Ex Factory Wholesale Unit": ex_factory_unit,

                "Local Pack Description": local_pack_description,
                "PK keyword present": "Yes" if pk_present else "No",

                # Your reimbursement outputs
                "Public With VAT Price": public_with_vat,
                "RI Price": ri_price,

                "Reimbursable Status": REIMBURSABLE_STATUS,
                "Reimbursable Price": ri_price,                  # per your last rule: ExFactory*1.08
                "Reimbursable Rate": REIMBURSABLE_RATE,
                "Reimbursable Notes": REIMBURSABLE_NOTES,
                "Copayment Value": float(f"{COPAYMENT_VALUE:.2f}"),
            })

    df = pd.DataFrame(rows_out)

    # If any Effective Start Date is still missing, attempt forward-fill within the same scrape order
    # (should be rare, because Ontario normally provides captions).
    if "Effective Start Date" in df.columns:
        df["Effective Start Date"] = df["Effective Start Date"].replace("", pd.NA)
        df["Effective Start Date"] = df["Effective Start Date"].ffill()

    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=None, help="Output CSV path")
    ap.add_argument("--html", default=None, help="Offline mode: path to saved HTML file")
    ap.add_argument("--url", default=URL, help="URL to scrape (live mode)")
    args = ap.parse_args()

    if args.html:
        with open(args.html, "r", encoding="utf-8", errors="ignore") as f:
            html = f.read()
    else:
        html = fetch_html(args.url)

    df = scrape_from_html(html)

    # Basic sanity checks
    if df.empty:
        raise SystemExit("No rows extracted. The page structure may have changed.")

    # Save
    output_dir = get_output_dir()
    output_path = Path(args.out) if args.out else (output_dir / EAP_PRICES_CSV_NAME)
    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(str(output_path), index=False, encoding="utf-8-sig")
    print(f"[OK] Saved {len(df):,} rows -> {output_path}")
    print("Sample:")
    print(df.head(5).to_string(index=False))


if __name__ == "__main__":

    main()
