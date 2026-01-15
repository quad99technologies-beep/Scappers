"""
Script 2: Build Details/Award URLs (User Provided List)
=======================================================
Creates tender_redirect_urls.csv from the user-provided tender list.
By default, no browser is used; URLs are constructed from qs/id or passed through.

INPUTS:
  - output/Tender_Chile/{SCRIPT_01_OUTPUT_CSV}

OUTPUTS:
  - output/Tender_Chile/{SCRIPT_02_OUTPUT_CSV}
    Columns: original_url, redirect_url, qs_parameter, tender_details_url, tender_award_url
"""

from __future__ import annotations

import csv
import re
from typing import Dict, Optional, Any, Iterable

from config_loader import load_env_file, getenv, getenv_bool, getenv_int, get_output_dir

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
except Exception:
    webdriver = None
    Options = None


def first_value(row: Dict[str, Any], keys: Iterable[str]) -> Optional[str]:
    for key in keys:
        if key in row and row[key]:
            val = str(row[key]).strip()
            if val:
                return val
    return None


def extract_qs(url: str) -> Optional[str]:
    if not url:
        return None
    match = re.search(r"qs=([^&]+)", url)
    return match.group(1) if match else None


def extract_id(url: str) -> Optional[str]:
    if not url:
        return None
    match = re.search(r"idlicitacion=([^&]+)", url, flags=re.IGNORECASE)
    return match.group(1) if match else None


def build_details_url(qs: Optional[str], lic_id: Optional[str], url: Optional[str]) -> str:
    if qs:
        return f"https://www.mercadopublico.cl/Procurement/Modules/RFB/DetailsAcquisition.aspx?qs={qs}"
    if lic_id:
        return f"https://www.mercadopublico.cl/Procurement/Modules/RFB/DetailsAcquisition.aspx?idlicitacion={lic_id}"
    if url:
        return url
    return ""


def build_award_url(qs: Optional[str], lic_id: Optional[str], url: Optional[str], details_url: Optional[str]) -> str:
    if url:
        return url
    if qs:
        return f"https://www.mercadopublico.cl/Procurement/Modules/RFB/StepsProcessAward/PreviewAwardAct.aspx?qs={qs}"
    if lic_id:
        return f"https://www.mercadopublico.cl/Procurement/Modules/RFB/StepsProcessAward/PreviewAwardAct.aspx?idlicitacion={lic_id}"
    if details_url:
        qs_from_details = extract_qs(details_url)
        if qs_from_details:
            return build_award_url(qs_from_details, None, None, None)
        lic_from_details = extract_id(details_url)
        if lic_from_details:
            return build_award_url(None, lic_from_details, None, None)
    return ""


def resolve_redirect(url: str, headless: bool) -> str:
    if not webdriver or not Options:
        return url
    chrome_opts = Options()
    if headless:
        chrome_opts.add_argument("--headless=new")
    chrome_opts.add_argument("--disable-gpu")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-dev-shm-usage")
    chrome_opts.add_argument("--window-size=1400,900")
    driver = webdriver.Chrome(options=chrome_opts)
    try:
        driver.set_page_load_timeout(60)
        driver.get(url)
        return driver.current_url
    finally:
        driver.quit()


def main() -> None:
    load_env_file()
    output_dir = get_output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)

    input_name = getenv("SCRIPT_01_OUTPUT_CSV", "tender_list.csv")
    output_name = getenv("SCRIPT_02_OUTPUT_CSV", "tender_redirect_urls.csv")
    resolve_redirects = getenv_bool("SCRIPT_02_RESOLVE_REDIRECTS", False)
    headless = getenv_bool("HEADLESS", True)
    max_tenders = getenv_int("MAX_TENDERS", 100)

    input_path = output_dir / input_name
    if not input_path.exists():
        raise FileNotFoundError(f"Input tender list not found: {input_path}")

    output_path = output_dir / output_name
    required_cols = ["original_url", "redirect_url", "qs_parameter", "tender_details_url", "tender_award_url"]

    detail_keys = [
        "tender_details_url",
        "tender_detail_url",
        "details_url",
        "Original_Publication_Link_Notice",
        "DetailsAcquisition",
    ]
    award_keys = [
        "tender_award_url",
        "award_url",
        "Original_Publication_Link_Award",
    ]
    qs_keys = ["qs", "qs_parameter", "QS"]
    id_keys = [
        "Source Tender Id",
        "Tender ID",
        "IDLicitacion",
        "idlicitacion",
        "TenderId",
        "CN Document Number",
        "CAN Document Number",
    ]
    url_keys = ["URL", "url", "Link", "link", "Enlace", "enlace", "Detalle", "detalle"]

    rows_out = []
    with open(input_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if max_tenders and len(rows_out) >= max_tenders:
                break

            details_url = first_value(row, detail_keys)
            award_url = first_value(row, award_keys)
            qs = first_value(row, qs_keys)
            lic_id = first_value(row, id_keys)
            original_url = first_value(row, url_keys) or details_url or ""

            if not qs and details_url:
                qs = extract_qs(details_url)
            if not lic_id and details_url:
                lic_id = extract_id(details_url)
            if not qs and award_url:
                qs = extract_qs(award_url)
            if not lic_id and award_url:
                lic_id = extract_id(award_url)

            if resolve_redirects and original_url:
                redirect_url = resolve_redirect(original_url, headless=headless)
            else:
                redirect_url = details_url or original_url or ""

            if not details_url:
                details_url = build_details_url(qs, lic_id, redirect_url or original_url)

            if not award_url:
                award_url = build_award_url(qs, lic_id, None, details_url)

            rows_out.append(
                {
                    "original_url": original_url,
                    "redirect_url": redirect_url,
                    "qs_parameter": qs or "",
                    "tender_details_url": details_url,
                    "tender_award_url": award_url,
                }
            )

    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=required_cols)
        writer.writeheader()
        writer.writerows(rows_out)

    print(f"[OK] Redirect list saved: {output_path}")


if __name__ == "__main__":
    main()
