
#!/usr/bin/env python3
"""
Step 2: Download sources (PDF attachments) and persist detail JSON in DB.

Important: We do NOT write any JSON detail files to disk. All detail payloads
are stored in PostgreSQL (it_determinas.detail).
"""

import concurrent.futures
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests

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
DETAIL_URL_BASE = "https://trovanorme.aifa.gov.it/tnf-service/determina/tnf/pubblicate/"
MSF_DETAIL_URL_BASE = "https://trovanorme.aifa.gov.it/tnf-service/determina/msf"
ATTACHMENT_URL_BASE = "https://trovanorme.aifa.gov.it/tnf-service/determina/tnf/pubblicate/allegato/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://trovanorme.aifa.gov.it/",
}


def fetch_detail(item_id: str) -> Optional[Dict[str, Any]]:
    url = f"{DETAIL_URL_BASE}{item_id}"
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        if response.status_code == 200:
            return response.json()
    except Exception:
        pass
    return None


def fetch_msf_detail(item_id: str, pub_date) -> Optional[Dict[str, Any]]:
    if not pub_date:
        return None
    
    formatted_date = pub_date
    
    # Handle datetime/date objects
    if hasattr(pub_date, 'strftime'):
        # If it's just date, assume midnight
        if not hasattr(pub_date, 'hour'):
            formatted_date = pub_date.strftime("%Y-%m-%dT00:00:00.000Z")
        else:
             formatted_date = pub_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    else:
         # Assume string
         formatted_date = str(pub_date)
         # Fix potential missing milliseconds
         if "T" in formatted_date and formatted_date.endswith("Z") and ".000Z" not in formatted_date:
             formatted_date = formatted_date.replace("Z", ".000Z")

    params = {
        "dataPubblicazione": formatted_date,
        "redazionale": item_id
    }
    
    try:
        response = requests.get(MSF_DETAIL_URL_BASE, params=params, headers=HEADERS, timeout=30)
        if response.status_code == 200:
            return response.json()
        logger.warning("MSF %s failed: %s - link: %s", item_id, response.status_code, response.url)
    except Exception as e:
        logger.error("Error fetching MSF %s: %s", item_id, e)
    return None

def download_pdf(attachment_id: str, filename: str) -> bool:
    url = f"{ATTACHMENT_URL_BASE}{attachment_id}"
    filepath = PDF_DIR / filename
    if filepath.exists():
        return True
    try:
        response = requests.get(url, headers=HEADERS, stream=True, timeout=60)
        if response.status_code == 200:
            with open(filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return True
    except Exception as e:
        logger.error("Failed %s: %s", filename, e)
    return False

def _safe_filename(name: str) -> str:
    return "".join([c for c in name if c.isalnum() or c in (" ", ".", "_", "-")]).strip() or "unknown"

def main():
    run_id = os.environ.get("ITALY_RUN_ID", "manual_run")
    db = CountryDB("Italy")
    repo = ItalyRepository(db, run_id)
    
    PDF_DIR.mkdir(parents=True, exist_ok=True)

    items = repo.get_determinas()
    completed = set(repo.get_completed_keys(2))
    pending = [it for it in items if it.get("determina_id") and it.get("determina_id") not in completed]
    logger.info("Step 2: Downloading sources for %s/%s determinas (pending/total)", len(pending), len(items))
    
    completed_count = 0
    failed_count = 0
    pdf_downloaded = 0
    attachment_seen = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(process_item_download_only, item): item for item in pending}
        
        for future in concurrent.futures.as_completed(futures):
            item = futures[future]
            item_id = item.get("determina_id")
            try:
                detail, status, msg, pdf_cnt, att_cnt = future.result()
                if detail is not None and item_id:
                    repo.update_determina_detail(item_id, detail)
                if item_id:
                    repo.mark_progress(2, "Download Sources", item_id, status, error_message=msg)
                if status == "completed":
                    completed_count += 1
                else:
                    failed_count += 1
                pdf_downloaded += int(pdf_cnt or 0)
                attachment_seen += int(att_cnt or 0)
            except Exception as e:
                if item_id:
                    repo.mark_progress(2, "Download Sources", item_id, "failed", error_message=str(e))
                logger.error("Error processing %s: %s", item_id, e)
                failed_count += 1

    try:
        repo.upsert_stat("*", 2, "items_total", len(items))
        repo.upsert_stat("*", 2, "items_pending", len(pending))
        repo.upsert_stat("*", 2, "completed", completed_count)
        repo.upsert_stat("*", 2, "failed", failed_count)
        repo.upsert_stat("*", 2, "attachments_seen", attachment_seen)
        repo.upsert_stat("*", 2, "pdf_downloaded", pdf_downloaded)
        repo.refresh_step2_stats_by_keyword()
    except Exception as e:
        logger.warning("Could not persist Step 2 stats: %s", e)

    logger.info(
        "Step 2 summary: completed=%s failed=%s pdf_downloaded=%s attachments_seen=%s",
        completed_count,
        failed_count,
        pdf_downloaded,
        attachment_seen,
    )


def process_item_download_only(item):
    """
    Worker: fetch detail + download PDFs (disk I/O). No DB access here.

    Returns: (detail_json, status, message, pdf_downloaded_count, attachment_count)
    """
    item_id = item.get("determina_id")
    typology = item.get("typology") or ""
    pub_date = item.get("publish_date")

    if not item_id:
        return None, "failed", "Missing determina_id", 0, 0

    if str(typology).upper() == "MSF":
        detail = fetch_msf_detail(item_id, pub_date)
        if not detail:
            return None, "failed", "MSF detail fetch failed", 0, 0
        return detail, "completed", None, 0, 0

    detail = fetch_detail(item_id)
    if not detail:
        return None, "failed", "TNF detail fetch failed", 0, 0

    attachments = detail.get("allegati") or []
    downloaded_any = False
    downloaded_count = 0
    for att in attachments:
        att_id = att.get("id")
        att_name = att.get("nome", "unknown")
        if not att_id:
            continue
        filename = f"{item_id}_{att_id}_{_safe_filename(str(att_name))}.pdf"
        ok = download_pdf(str(att_id), filename)
        downloaded_any = downloaded_any or ok
        if ok:
            downloaded_count += 1

    if not attachments:
        return detail, "completed", "No attachments", 0, 0
    if not downloaded_any:
        return detail, "failed", "No PDFs downloaded", 0, len(attachments)
    return detail, "completed", None, downloaded_count, len(attachments)

if __name__ == "__main__":
    main()
