
#!/usr/bin/env python3
import os
import sys
import time
import logging
from pathlib import Path
import requests
import concurrent.futures


import json
from datetime import datetime

# Path setup
_repo_root = Path(__file__).resolve().parents[3]
_italy_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_repo_root))
sys.path.insert(0, str(_italy_dir))

from core.db.connection import CountryDB
from db.repositories import ItalyRepository

logger = logging.getLogger(__name__)


PDF_DIR = r"d:\quad99\Scrappers\data\Italy\pdfs"
JSON_DIR = r"d:\quad99\Scrappers\data\Italy\json"
DETAIL_URL_BASE = "https://trovanorme.aifa.gov.it/tnf-service/determina/tnf/pubblicate/"
MSF_DETAIL_URL_BASE = "https://trovanorme.aifa.gov.it/tnf-service/determina/msf"
ATTACHMENT_URL_BASE = "https://trovanorme.aifa.gov.it/tnf-service/determina/tnf/pubblicate/allegato/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
}


def fetch_detail(item_id):
    url = f"{DETAIL_URL_BASE}{item_id}"
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        if response.status_code == 200:
            return response.json()
    except Exception:
        pass
    return None


def fetch_msf_detail(item_id, pub_date):
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
        else:
            logger.warning(f"MSF {item_id} failed: {response.status_code} - Link: {response.url}")
    except Exception as e:
        logger.error(f"Error fetching MSF {item_id}: {e}")
    return None

def download_pdf(attachment_id, filename):
    url = f"{ATTACHMENT_URL_BASE}{attachment_id}"
    filepath = os.path.join(PDF_DIR, filename)
    if os.path.exists(filepath):
        return True
    try:
        response = requests.get(url, headers=HEADERS, stream=True, timeout=60)
        if response.status_code == 200:
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return True
    except Exception as e:
        logger.error(f"Failed {filename}: {e}")
    return False

def process_item(item, repo):
    item_id = item.get("determina_id")
    
    # Check progress
    if repo.is_progress_completed(2, item_id):
        return

    detail = fetch_detail(item_id)
    if not detail:
        return

    attachments = detail.get("allegati", [])
    for att in attachments:
        att_id = att.get("id")
        att_name = att.get("nome", "unknown")
        safe_name = "".join([c for c in att_name if c.isalnum() or c in (' ', '.', '_', '-')]).strip()
        filename = f"{item_id}_{att_id}_{safe_name}.pdf"
        download_pdf(att_id, filename)
    
    repo.mark_progress(2, "Download PDF", item_id, "completed")

def main():
    run_id = os.environ.get("ITALY_RUN_ID", "manual_run")
    db = CountryDB("Italy")
    repo = ItalyRepository(db, run_id)
    

    os.makedirs(PDF_DIR, exist_ok=True)
    os.makedirs(JSON_DIR, exist_ok=True)
    
    items = repo.get_determinas()
    logger.info(f"Step 2: Downloading PDFs for {len(items)} determinas")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Pass repo to worker, but beware DB connection sharing. Ideally new connection per thread or use pool.
        # Simple hack: create short-lived DB inside or just pass IDs and bulk update progress?
        # For now, let's keep it simple: progress tracking might fail concurrently with single connection.
        # We will skip DB writes inside threads for safety in this simple version, or use a lock.
        # BETTER: Just download files, then mark progress in main thread?
        
        # Refactor: parallel download, serial DB update
        futures = {executor.submit(process_item_download_only, item): item for item in items}
        
        for future in concurrent.futures.as_completed(futures):
            item = futures[future]
            try:
                future.result()
                # Mark progress here (main thread)
                repo.mark_progress(2, "Download PDF", item['determina_id'], "completed")
            except Exception as e:
                logger.error(f"Error processing {item.get('determina_id')}: {e}")


def process_item_download_only(item):
    item_id = item.get("determina_id")
    typology = item.get("typology")
    pub_date = item.get("publish_date")
    
    if typology == "MSF":
        # Handle MSF - Download JSON
        detail = fetch_msf_detail(item_id, pub_date)
        if detail:
            path = os.path.join(JSON_DIR, f"{item_id}.json")
            try:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(detail, f, indent=2, ensure_ascii=False)
            except Exception as e:
                logger.error(f"Failed saving JSON for {item_id}: {e}")
    else:
        # Handle Normal - Download PDF
        detail = fetch_detail(item_id)
        if detail:
            attachments = detail.get("allegati", [])
            for att in attachments:
                att_id = att.get("id")
                att_name = att.get("nome", "unknown")
                safe_name = "".join([c for c in att_name if c.isalnum() or c in (' ', '.', '_', '-')]).strip()
                filename = f"{item_id}_{att_id}_{safe_name}.pdf"
                download_pdf(att_id, filename)

if __name__ == "__main__":
    main()
