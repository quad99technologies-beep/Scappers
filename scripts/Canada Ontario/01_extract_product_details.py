"""
Ontario Formulary scraper (end-to-end) with resume support

WHAT IT DOES
1) Loops q=a..z:
   https://www.formulary.health.gov.on.ca/formulary/results.xhtml?q=<q>&s=true&type=4
2) Extracts ALL rows from the results table (user assumption: no scroll/pagination needed).
3) Manufacturer resolution:
   - FIRST: resolve Manufacturer Name from LOCAL manufacturer master CSV by MFR code.
   - ONLY IF missing / blank / bad: open DIN detail.xhtml?drugId=... and extract Manufacturer name,
     then update BOTH product output + local master table.
4) PK / price-type logic:
   - Primary: if local_pack_code endswith 'PK' (last 2 letters) => PACK
   - Fallback (for Ontario screenshots): if Brand/Description contains token 'Pk'/'PK' => PACK
   - Else => UNIT
5) Reimbursable + copay (as per your final rule):
   - reimbursable_price = Amount MOH Pays (if numeric), else fallback to Drug Benefit Price
   - public_with_vat = exfactory_price * 1.08
   - copay = public_with_vat - reimbursable_price
   NOTE: Here exfactory_price is taken as "Drug Benefit Price or Unit Price" column because that is
   the only base price available on the results page.

RESUME SUPPORT:
- Tracks completed letters (q_letter) to skip already processed searches
- Saves data after each letter search
- Deduplicates products by local_pack_code
- Progress tracking with [PROGRESS] messages

OUTPUTS
- output/products.csv
- output/manufacturer_master.csv
- output/completed_letters.json (tracks which letters are done)

Deps:
  pip install requests beautifulsoup4 lxml pandas
"""

import os
import re
import time
import string
import json
import sys
from typing import Dict, List, Optional, Tuple, Set
from pathlib import Path

# Add script directory to path for config_loader import
_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from config_loader import get_output_dir

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE = "https://www.formulary.health.gov.on.ca/formulary/"
RESULTS_URL = BASE + "results.xhtml"
DETAIL_URL = BASE + "detail.xhtml"

# Use platform config for output directory
OUT_DIR = get_output_dir()
OUT_DIR.mkdir(parents=True, exist_ok=True)

PRODUCTS_CSV = str(OUT_DIR / "products.csv")
MFR_MASTER_CSV = str(OUT_DIR / "manufacturer_master.csv")
COMPLETED_LETTERS_JSON = str(OUT_DIR / "completed_letters.json")

# Tuning (be nice to the server)
SLEEP_BETWEEN_Q = 0.35
SLEEP_BETWEEN_DETAIL = 0.15
RETRIES = 4
TIMEOUT = 45

BAD_NAME_SET = {"", "n/a", "na", "none", "unknown", "-", "--"}


def norm(s: str) -> str:
    return (s or "").strip()


def is_bad_name(name: Optional[str]) -> bool:
    if name is None:
        return True
    return norm(name).lower() in BAD_NAME_SET


def parse_float(s: str) -> Optional[float]:
    s = norm(s)
    if not s or s.lower() in {"n/a", "na"}:
        return None
    # keep digits and dot only
    s = re.sub(r"[^\d.]+", "", s)
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def safe_get(session: requests.Session, url: str, params: dict = None) -> str:
    last = None
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    for i in range(1, RETRIES + 1):
        try:
            r = session.get(url, params=params, timeout=TIMEOUT, headers=headers)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(i * 2)
                continue
            r.raise_for_status()
            return r.text
        except Exception as e:
            last = e
            time.sleep(i * 2)
    raise RuntimeError(f"GET failed: {url} params={params} err={last}")


def load_mfr_master(path: str) -> Dict[str, str]:
    if not os.path.exists(path):
        return {}
    df = pd.read_csv(path, dtype=str).fillna("")
    master: Dict[str, str] = {}
    for _, r in df.iterrows():
        code = norm(r.get("mfr_code", ""))
        name = norm(r.get("manufacturer_name", ""))
        if code:
            master[code] = name
    return master


def save_mfr_master(path: str, master: Dict[str, str]) -> None:
    df = pd.DataFrame(
        [{"mfr_code": k, "manufacturer_name": v} for k, v in sorted(master.items(), key=lambda x: x[0])]
    )
    df.to_csv(path, index=False, encoding="utf-8-sig")


def load_completed_letters() -> Set[str]:
    """Load set of completed letters from JSON file"""
    if not os.path.exists(COMPLETED_LETTERS_JSON):
        return set()
    try:
        with open(COMPLETED_LETTERS_JSON, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return set(data.get("completed_letters", []))
    except Exception:
        return set()


def save_completed_letters(completed: Set[str]) -> None:
    """Save set of completed letters to JSON file"""
    data = {"completed_letters": sorted(list(completed))}
    with open(COMPLETED_LETTERS_JSON, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)


def detect_price_type(local_pack_code: str, brand_desc: str) -> str:
    """
    Your final instruction says: PK is last 2 alphabets of Pack ID.
    Ontario screenshots also show 'Pk' inside the brand description.

    Rule:
      1) If local_pack_code endswith PK (case-insensitive) -> PACK
      2) Else if brand_desc contains token 'pk'/'Pk' (as separate token) -> PACK
      3) Else -> UNIT
    """
    code = norm(local_pack_code)
    if len(code) >= 2 and code[-2:].upper() == "PK":
        return "PACK"

    desc = " " + norm(brand_desc) + " "
    # token match: " pk " or "-pk" or "_pk" near end
    if re.search(r"(?i)(?:\bpk\b)", desc):
        return "PACK"

    return "UNIT"


def parse_results_rows(html: str, q_letter: str) -> List[dict]:
    soup = BeautifulSoup(html, "lxml")

    tbody = soup.select_one('tbody#j_id_l\\:searchResultFull_data')
    if not tbody:
        tbody = soup.find("tbody", id=re.compile(r"searchResultFull_data$"))
    if not tbody:
        return []

    out: List[dict] = []
    for tr in tbody.select("tr"):
        tds = tr.select("td")
        if len(tds) < 9:
            continue

        din_a = tds[0].select_one("a[href*='detail.xhtml?drugId=']")
        local_pack_code = norm(din_a.get_text(strip=True)) if din_a else norm(tds[0].get_text(strip=True))
        din_href = din_a.get("href") if din_a else ""
        drug_id = ""
        m = re.search(r"drugId=([0-9A-Za-z]+)", din_href or "")
        if m:
            drug_id = m.group(1)

        generic = norm(tds[1].get_text(" ", strip=True))
        brand = norm(tds[2].get_text(" ", strip=True))
        mfr_code = norm(tds[3].get_text(strip=True))

        exfactory_raw = norm(tds[4].get_text(strip=True))  # "Drug Benefit Price or Unit Price"
        moh_raw = norm(tds[5].get_text(strip=True))        # "Amount MOH Pays"

        interchangeable = norm(tds[6].get_text(strip=True))
        limited_use = norm(tds[7].get_text(strip=True))
        therapeutic = norm(tds[8].get_text(" ", strip=True))

        price_type = detect_price_type(local_pack_code=local_pack_code, brand_desc=brand)

        out.append(
            {
                "q_letter": q_letter,
                "local_pack_code": local_pack_code,  # DIN/PIN/NPN on the site
                "drug_id": drug_id,
                "generic_name": generic,
                "brand_name_strength_dosage": brand,
                "mfr_code": mfr_code,

                # resolved later
                "manufacturer_name": "",

                # raw prices
                "exfactory_price_raw": exfactory_raw,
                "amount_moh_pays_raw": moh_raw,

                # flags
                "price_type": price_type,  # UNIT / PACK
                "interchangeable": interchangeable,
                "limited_use": limited_use,
                "therapeutic_notes_requirements": therapeutic,

                # derived later
                "exfactory_price": None,
                "reimbursable_price": None,
                "public_with_vat": None,
                "copay": None,
                "qa_notes": "",

                "detail_url": f"{DETAIL_URL}?drugId={drug_id}" if drug_id else "",
            }
        )

    return out


def parse_manufacturer_from_detail(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")

    # Typical detail table label/value pairs.
    label_td = soup.find("td", string=lambda s: isinstance(s, str) and "Manufacturer:" in s)
    if label_td:
        value_td = label_td.find_next("td")
        if value_td:
            a = value_td.select_one("a")
            if a:
                return norm(a.get_text(strip=True))
            return norm(value_td.get_text(" ", strip=True))

    # fallback regex
    m = re.search(r"Manufacturer:\s*</td>\s*<td[^>]*>\s*(?:<a[^>]*>)?([^<]+)", html, re.I)
    return norm(m.group(1)) if m else ""


def compute_prices(row: dict) -> dict:
    """
    Final calculation rule:
      reimbursable_price = Amount MOH Pays (preferred)
      public_with_vat = exfactory_price * 1.08
      copay = public_with_vat - reimbursable_price

    Here:
      exfactory_price := parsed from "Drug Benefit Price or Unit Price"
      reimbursable_price := parsed from "Amount MOH Pays" if numeric else fallback to exfactory_price
    """
    qa = []

    exf = parse_float(row.get("exfactory_price_raw", ""))
    moh = parse_float(row.get("amount_moh_pays_raw", ""))

    if exf is None:
        qa.append("exfactory_missing_or_non_numeric")

    reimb = moh if moh is not None else exf
    if reimb is None:
        qa.append("reimbursable_missing_or_non_numeric")

    public_vat = (exf * 1.08) if exf is not None else None
    copay = (public_vat - reimb) if (public_vat is not None and reimb is not None) else None

    # light QA
    if copay is not None and copay < 0:
        qa.append("copay_negative")
    if copay is not None and copay == 0:
        qa.append("copay_zero")
    if row.get("amount_moh_pays_raw", "").strip().upper() in {"N/A", "NA"}:
        qa.append("amount_moh_pays_na")

    row["exfactory_price"] = exf
    row["reimbursable_price"] = reimb
    row["public_with_vat"] = public_vat
    row["copay"] = copay
    row["qa_notes"] = ";".join(qa)

    return row


def load_existing_products(path: str) -> Tuple[pd.DataFrame, Set[str]]:
    """Load existing products and return dataframe and set of seen codes"""
    if not os.path.exists(path):
        return pd.DataFrame(), set()
    try:
        df = pd.read_csv(path, dtype=str).fillna("")
        seen = set(df["local_pack_code"].astype(str).tolist()) if "local_pack_code" in df.columns else set()
        return df, seen
    except Exception as e:
        print(f"[WARNING] Error loading existing products: {e}")
        return pd.DataFrame(), set()


def save_products_incremental(path: str, new_rows: List[dict], existing_df: pd.DataFrame, seen_codes: Set[str]) -> Tuple[pd.DataFrame, Set[str]]:
    """Save products incrementally, merging with existing data and deduplicating"""
    if not new_rows:
        return existing_df, seen_codes
    
    # Ensure stable columns + keep numeric columns as strings in CSV (safe for Excel)
    col_order = [
        "q_letter",
        "local_pack_code",
        "drug_id",
        "generic_name",
        "brand_name_strength_dosage",
        "mfr_code",
        "manufacturer_name",
        "price_type",
        "exfactory_price_raw",
        "amount_moh_pays_raw",
        "exfactory_price",
        "reimbursable_price",
        "public_with_vat",
        "copay",
        "interchangeable",
        "limited_use",
        "therapeutic_notes_requirements",
        "qa_notes",
        "detail_url",
    ]

    def finalize(df: pd.DataFrame) -> pd.DataFrame:
        for c in col_order:
            if c not in df.columns:
                df[c] = ""
        df = df[col_order].copy()

        # Convert numeric fields to consistent string format
        for c in ["exfactory_price", "reimbursable_price", "public_with_vat", "copay"]:
            df[c] = df[c].apply(lambda x: "" if x is None or str(x).strip() == "" else f"{float(x):.4f}")
        return df

    # Create DataFrame from new rows
    new_df = pd.DataFrame(new_rows)
    new_df = finalize(new_df)

    # Merge with existing data
    if not existing_df.empty:
        existing_df = finalize(existing_df)
        # Combine and deduplicate by local_pack_code (keep first occurrence)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        final_df = combined_df.drop_duplicates(subset=["local_pack_code"], keep="first")
    else:
        final_df = new_df

    # Save to CSV
    final_df.to_csv(path, index=False, encoding="utf-8-sig")
    
    # Update seen codes from final dataframe
    seen_codes = set(final_df["local_pack_code"].astype(str).tolist())
    
    return final_df, seen_codes


def main():
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }
    )

    # Load completed letters (resume support)
    completed_letters = load_completed_letters()
    print(f"[RESUME] Loaded {len(completed_letters)} completed letters: {sorted(completed_letters)}")

    # Local manufacturer cache (master)
    mfr_master = load_mfr_master(MFR_MASTER_CSV)

    # Load existing products
    existing_df, seen_codes = load_existing_products(PRODUCTS_CSV)
    print(f"[RESUME] Loaded {len(existing_df)} existing products, {len(seen_codes)} unique codes")

    # Get all letters to process
    all_letters = list(string.ascii_lowercase)
    remaining_letters = [q for q in all_letters if q not in completed_letters]
    total_letters = len(all_letters)
    completed_count = len(completed_letters)

    print(f"[RESUME] Processing {len(remaining_letters)} remaining letters out of {total_letters} total ({completed_count} already completed)")

    if not remaining_letters:
        print("[DONE] All letters already processed!")
        return

    # Process remaining letters
    for idx, q in enumerate(remaining_letters):
        current_letter_num = completed_count + idx + 1
        print(f"[Q={q}] Fetching results... (Letter {current_letter_num}/{total_letters})")
        
        # Output progress message for GUI synchronization (including current letter in progress)
        # Calculate combined progress: step 1 of 2 (50% base) + letter progress within step (50% range)
        # Progress includes the current letter being processed (completed_count + idx + 1)
        letters_in_progress = completed_count + idx + 1
        letter_progress_percent = round((letters_in_progress / total_letters) * 100, 1) if total_letters > 0 else 0
        combined_percent = round(50.0 + (letter_progress_percent * 0.5), 1)
        print(f"[PROGRESS] Pipeline Step: 1/2 ({combined_percent}%) - Letter {current_letter_num}/{total_letters}", flush=True)
        
        try:
            html = safe_get(session, RESULTS_URL, params={"q": q, "s": "true", "type": "4"})
            rows = parse_results_rows(html, q_letter=q)
            print(f"[Q={q}] Rows parsed: {len(rows)}")

            new_rows: List[dict] = []
            new_count = 0
            skipped_count = 0

            for row in rows:
                code = row["local_pack_code"]
                if not code or code in seen_codes:
                    skipped_count += 1
                    continue

                # Manufacturer resolution:
                # 1) LOCAL MASTER FIRST
                mfr_code = row.get("mfr_code", "")
                resolved_name = mfr_master.get(mfr_code, "") if mfr_code else ""

                # 2) ONLY IF missing/bad -> open DIN detail page and extract Manufacturer name
                if is_bad_name(resolved_name):
                    drug_id = row.get("drug_id", "")
                    if drug_id:
                        detail_html = safe_get(session, DETAIL_URL, params={"drugId": drug_id})
                        extracted = parse_manufacturer_from_detail(detail_html)

                        if not is_bad_name(extracted):
                            resolved_name = extracted
                            if mfr_code:
                                mfr_master[mfr_code] = extracted  # update local master

                        time.sleep(SLEEP_BETWEEN_DETAIL)

                row["manufacturer_name"] = resolved_name if not is_bad_name(resolved_name) else ""

                # Compute reimbursement/vat/copay
                row = compute_prices(row)

                new_rows.append(row)
                seen_codes.add(code)
                new_count += 1

            # Save data after each letter (incremental save)
            if new_rows:
                existing_df, seen_codes = save_products_incremental(PRODUCTS_CSV, new_rows, existing_df, seen_codes)
                print(f"[Q={q}] Saved {new_count} new products (skipped {skipped_count} duplicates)")
            else:
                print(f"[Q={q}] No new products (all {skipped_count} were duplicates)")

            # Persist manufacturer master after each letter
            save_mfr_master(MFR_MASTER_CSV, mfr_master)

            # Mark letter as completed
            completed_letters.add(q)
            save_completed_letters(completed_letters)
            print(f"[Q={q}] Letter completed and saved")
            
            # Update progress after letter completion
            completed_letter_count = len(completed_letters)
            letter_progress_percent = round((completed_letter_count / total_letters) * 100, 1) if total_letters > 0 else 0
            combined_percent = round(50.0 + (letter_progress_percent * 0.5), 1)
            print(f"[PROGRESS] Pipeline Step: 1/2 ({combined_percent}%) - Letter {completed_letter_count}/{total_letters}", flush=True)

        except Exception as e:
            print(f"[ERROR] Failed to process letter '{q}': {e}")
            print(f"[ERROR] Letter '{q}' will be retried on next run")
            # Don't mark as completed if there was an error
            continue

        time.sleep(SLEEP_BETWEEN_Q)

    # Final summary
    final_df, _ = load_existing_products(PRODUCTS_CSV)
    
    # Final progress update - step 1 complete (100% of step 1 = 50% + 50% = 100% overall for step 1)
    print(f"[PROGRESS] Pipeline Step: 1/2 (100.0%) - Letter {total_letters}/{total_letters}", flush=True)
    
    print(f"\n[DONE] All letters processed!")
    print(f"[DONE] Total products: {len(final_df)}")
    print(f"[DONE] Saved products: {PRODUCTS_CSV}")
    print(f"[DONE] Saved manufacturer master: {MFR_MASTER_CSV} (unique={len(mfr_master)})")
    print(f"[DONE] Completed letters: {len(completed_letters)}/{total_letters}")


if __name__ == "__main__":
    main()
