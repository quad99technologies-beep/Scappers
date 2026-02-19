
import sys
import os
import requests
import json
import time
import logging
import concurrent.futures
from pathlib import Path

# Add repo root to path for core imports
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Italy-specific imports
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from core.db.connection import CountryDB
from db.repositories import ItalyRepository
from config_loader import load_env_file, get_output_dir

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
PDF_DIR = r"d:\quad99\Scrappers\data\Italy\pdfs"
DETAILS_DIR = r"d:\quad99\Scrappers\data\Italy\details"
DETAIL_URL_BASE = "https://trovanorme.aifa.gov.it/tnf-service/determina/tnf/pubblicate/"
ATTACHMENT_URL_BASE = "https://trovanorme.aifa.gov.it/tnf-service/determina/tnf/pubblicate/allegato/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://trovanorme.aifa.gov.it/",
}

def fetch_detail(item_id):
    """Fetch detail metadata to find attachment IDs."""
    url = f"{DETAIL_URL_BASE}{item_id}"
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            logger.warning(f"Detail not found for {item_id}")
            return None
        else:
            logger.error(f"Error fetching detail {item_id}: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Exception fetching detail {item_id}: {e}")
        return None

def download_pdf(attachment_id, filename):
    """Download PDF attachment."""
    url = f"{ATTACHMENT_URL_BASE}{attachment_id}"
    filepath = os.path.join(PDF_DIR, filename)
    
    if os.path.exists(filepath):
        logger.info(f"File already exists: {filename}")
        return True

    try:
        response = requests.get(url, headers=HEADERS, stream=True, timeout=60)
        if response.status_code == 200:
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.info(f"Downloaded: {filename}")
            return True
        else:
            logger.error(f"Failed to download PDF {attachment_id}: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Exception downloading PDF {attachment_id}: {e}")
        return False

def process_item(item):
    # item is now a dict from DB
    item_id = item.get("determina_id")
    if not item_id:
        return
    
    # 1. Fetch Detail
    detail = fetch_detail(item_id)
    if not detail:
        return
    
    # Save detail for auditing/debugging
    detail_path = os.path.join(DETAILS_DIR, f"{item_id}.json")
    with open(detail_path, 'w', encoding='utf-8') as f:
        json.dump(detail, f, ensure_ascii=False, indent=2)
    
    # 2. Identify Attachments
    attachments = detail.get("allegati", [])
    if not attachments:
        logger.info(f"No attachments for {item_id}")
        return

    # 3. Download PDFs
    for att in attachments:
        att_id = att.get("id")
        att_name = att.get("nome", "unknown")
        
        # Filename strategy: {item_id}_{att_id}_{name}.pdf
        safe_name = "".join([c for c in att_name if c.isalnum() or c in (' ', '.', '_', '-')]).strip()
        filename = f"{item_id}_{att_id}_{safe_name}.pdf"
        
        download_pdf(att_id, filename)

def main():
    load_env_file()
    run_id = os.environ.get("ITALY_RUN_ID")
    if not run_id:
        logger.error("ITALY_RUN_ID not found in environment.")
        sys.exit(1)

    db = CountryDB("Italy")
    repo = ItalyRepository(db, run_id)

    os.makedirs(PDF_DIR, exist_ok=True)
    os.makedirs(DETAILS_DIR, exist_ok=True)
    
    # Read from DB instead of JSONL
    items = repo.get_determinas()
    logger.info(f"Loaded {len(items)} items from database to process.")
    
    if not items:
        logger.warning("No items found in database for this run.")
        db.close()
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_item, item) for item in items]
        
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            if i % 10 == 0:
                logger.info(f"Processed {i}/{len(items)} items.")
                
    logger.info("Download complete.")
    db.close()

if __name__ == "__main__":
    main()
