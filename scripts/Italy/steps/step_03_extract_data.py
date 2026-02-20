#!/usr/bin/env python3
"""
Step 3: Extract AIC + price data.

- PDFs are parsed from disk (output/Italy/pdfs/).
- MSF details are parsed from DB (it_determinas.detail); no JSON files are used.
"""

import concurrent.futures
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List

import pdfplumber

# Path setup
_repo_root = Path(__file__).resolve().parents[3]
_italy_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_repo_root))
sys.path.insert(0, str(_italy_dir))

from core.db.connection import CountryDB
from db.repositories import ItalyRepository
from config_loader import get_output_dir

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PDF_DIR = get_output_dir("pdfs")
_CURRENCY_RE = r"(?:€|EUR|â‚¬)"


def clean_text(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def parse_pdf(pdf_path: Path) -> List[Dict[str, Any]]:
    extracted_items: List[Dict[str, Any]] = []
    filename = pdf_path.name
    parts = filename.split("_")
    item_id = parts[0] if parts else None

    try:
        with pdfplumber.open(str(pdf_path)) as pdf:
            full_text_parts: List[str] = []
            for page in pdf.pages:
                page_text = page.extract_text() or ""
                if page_text:
                    full_text_parts.append(page_text)
            text = "\n".join(full_text_parts).replace("\n", "  ")

        aic_matches = list(re.finditer(r"AIC\s+n\.?\s*(\d{6,})", text, re.IGNORECASE))
        for match in aic_matches:
            aic = match.group(1)
            start_index = match.start()
            context_start = max(0, start_index - 500)
            context_end = min(len(text), start_index + 1000)
            context = text[context_start:context_end]

            item: Dict[str, Any] = {
                "determina_id": item_id,
                "aic": aic,
                "source_pdf": filename,
                "product_name": None,
                "pack_description": None,
                "price_ex_factory": None,
                "price_public": None,
            }

            ex_factory_match = re.search(
                rf"Prezzo\s+ex[- ]?factory.{{0,100}}?{_CURRENCY_RE}\s*([\d,.]+)",
                context,
                re.IGNORECASE | re.DOTALL,
            )
            if ex_factory_match:
                try:
                    val_str = ex_factory_match.group(1).replace(".", "").replace(",", ".")
                    item["price_ex_factory"] = float(val_str)
                except ValueError:
                    pass

            public_match = re.search(
                rf"Prezzo\s+al\s+pubblico.{{0,100}}?{_CURRENCY_RE}\s*([\d,.]+)",
                context,
                re.IGNORECASE | re.DOTALL,
            )
            if public_match:
                try:
                    val_str = public_match.group(1).replace(".", "").replace(",", ".")
                    item["price_public"] = float(val_str)
                except ValueError:
                    pass

            pre_aic_text = text[max(0, start_index - 300):start_index]
            confezione_match = re.search(r"Confezione\s+(.*)", pre_aic_text, re.IGNORECASE)
            if confezione_match:
                conf_text = confezione_match.group(1).strip()
                item["pack_description"] = clean_text(conf_text)
                item["product_name"] = conf_text.split(" ")[0] if conf_text else None

            extracted_items.append(item)

    except Exception as e:
        logger.error("Error parsing %s: %s", filename, e)

    return extracted_items


def parse_msf_detail(determina_id: str, detail: Dict[str, Any]) -> List[Dict[str, Any]]:
    extracted: List[Dict[str, Any]] = []
    if not detail:
        return extracted

    if isinstance(detail, str):
        try:
            detail = json.loads(detail)
        except Exception:
            return extracted

    text = detail.get("testo") or ""
    if not text:
        return extracted

    # Text format commonly includes: "A.I.C. n. 049930011 - Prezzo € 24,24"
    aic_matches = list(re.finditer(r"A\.?I\.?C\.?\s*n\.?\s*(\d{6,})", text, re.IGNORECASE))
    for match in aic_matches:
        aic = match.group(1)
        start_index = match.start()
        context_end = min(len(text), start_index + 220)
        context = text[start_index:context_end]

        item: Dict[str, Any] = {
            "determina_id": determina_id,
            "aic": aic,
            "source_pdf": "MSF_DETAIL",
            "product_name": None,
            "pack_description": None,
            "price_ex_factory": None,
            "price_public": None,
        }

        price_match = re.search(rf"Prezzo\s*{_CURRENCY_RE}\s*([\d,.]+)", context, re.IGNORECASE)
        if price_match:
            try:
                val_str = price_match.group(1).replace(".", "").replace(",", ".")
                item["price_public"] = float(val_str)
            except ValueError:
                pass

        pre_aic = text[max(0, start_index - 120):start_index]
        last_line = pre_aic.splitlines()[-1].strip() if pre_aic else ""
        last_line = last_line.replace("Specialita' medicinali:", "").strip()
        if last_line:
            item["product_name"] = last_line

        extracted.append(item)

    return extracted


def main() -> None:
    run_id = os.environ.get("ITALY_RUN_ID", "manual_run")
    db = CountryDB("Italy")
    repo = ItalyRepository(db, run_id)

    pdf_files = list(PDF_DIR.glob("*.pdf")) if PDF_DIR.exists() else []
    msf_rows = repo.get_determina_details_by_typology("MSF")

    logger.info(
        "Step 3: Extracting from %s PDFs and %s MSF detail payloads",
        len(pdf_files),
        len(msf_rows),
    )

    repo.clear_step_data(3)  # Clear previous extraction for this run

    extracted_batch: List[Dict[str, Any]] = []
    inserted_total = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_key = {executor.submit(parse_pdf, p): str(p) for p in pdf_files}
        future_to_key.update(
            {
                executor.submit(
                    parse_msf_detail, row.get("determina_id"), row.get("detail") or {}
                ): row.get("determina_id")
                for row in msf_rows
                if row.get("determina_id")
            }
        )

        for future in concurrent.futures.as_completed(future_to_key):
            items = future.result()
            if items:
                extracted_batch.extend(items)

            if len(extracted_batch) >= 100:
                inserted_total += repo.insert_products(extracted_batch)
                extracted_batch = []

    if extracted_batch:
        inserted_total += repo.insert_products(extracted_batch)

    try:
        repo.upsert_stat("*", 3, "products_inserted", inserted_total)
        repo.refresh_step3_product_counts_by_keyword()
    except Exception as e:
        logger.warning("Could not persist Step 3 stats: %s", e)

    logger.info("Step 3 Complete.")


if __name__ == "__main__":
    main()
