
import sys
import os
import time
import json
import logging
from datetime import datetime
from pathlib import Path

# Add repo root to path for core imports
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Italy-specific imports
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

import requests
from core.db.connection import CountryDB
from db.repositories import ItalyRepository
from config_loader import load_env_file, getenv, get_output_dir

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
BASE_URL = "https://trovanorme.aifa.gov.it/tnf-service/ricerca/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://trovanorme.aifa.gov.it/",
    "Origin": "https://trovanorme.aifa.gov.it"
}

def get_search_results(page_num=0, page_size=20):
    """Fetch search results for 'DET' documents."""
    params = {
        "pageSize": page_size,
        "totalElementNum": 0,
        "pageNum": page_num,
        "sortColumn": "dataPubblicazione",
        "sortOrder": "desc",
        "determinaGUSource": "true",
        "determinaTNFSource": "true",
        "documentoAIFASource": "true",
        "modificheSecondarieFarmaciSource": "true",
        "newsSource": "true",
        "tutti": "true",
        "parola": "DET"
    }
    
    try:
        response = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Error fetching page {page_num}: {e}")
        return None

def main():
    load_env_file()
    run_id = os.environ.get("ITALY_RUN_ID")
    if not run_id:
        logger.error("ITALY_RUN_ID not found in environment.")
        sys.exit(1)

    db = CountryDB("Italy")
    repo = ItalyRepository(db, run_id)
    repo.ensure_run_in_ledger()
    
    page_num = 0
    total_processed = 0
    max_pages = int(getenv("SCRIPT_01_MAX_PAGES", "5"))
    
    logger.info(f"Starting Italy Step 1: List Determinas (RunID: {run_id})")

    while page_num < max_pages:
        logger.info(f"Fetching page {page_num}...")
        data = get_search_results(page_num)
        
        if not data:
            logger.warning("Failed to retrieve data. Retrying once...")
            time.sleep(2)
            data = get_search_results(page_num)
            if not data:
                logger.error("Skipping page due to persistent error.")
                page_num += 1
                continue
        
        resource_list = data.get("resourceList", [])
        total_elements = data.get("totalElementNum", 0)
        
        if not resource_list:
            logger.info("No more items found.")
            break
            
        logger.info(f"Page {page_num}: Found {len(resource_list)} items. Total available: {total_elements}")
        
        # Save to DB
        repo.insert_determinas(resource_list)
        total_processed += len(resource_list)
        
        # Check pagination end
        if (page_num + 1) * 20 >= total_elements:
            logger.info("Reached end of results.")
            break
            
        page_num += 1
        time.sleep(1) # Be polite
            
    logger.info(f"Step 1 completed. Processed {total_processed} items. Data saved to database.")
    db.close()

if __name__ == "__main__":
    main()
