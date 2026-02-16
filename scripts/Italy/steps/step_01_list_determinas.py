
#!/usr/bin/env python3
import os
import sys
import time
import logging
from pathlib import Path
import requests

# Path setup
_repo_root = Path(__file__).resolve().parents[3]
_italy_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_repo_root))
sys.path.insert(0, str(_italy_dir))

from core.db.connection import CountryDB
from db.repositories import ItalyRepository
from config_loader import load_env_file, get_output_dir

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_URL = "https://trovanorme.aifa.gov.it/tnf-service/ricerca/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://trovanorme.aifa.gov.it/",
}




def get_search_results(page_num=0, page_size=20):
    params = {
        "pageSize": page_size,
        "pageNum": page_num,
        "parola": "Riduzione",
        "determinaGUSource": "true",
        "determinaTNFSource": "true",
        "documentoAIFASource": "true",
        "modificheSecondarieFarmaciSource": "true",
        "newsSource": "true",
        "tutti": "true"
    }
    try:
        response = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=30)
        return response.json()
    except Exception as e:
        logger.error(f"Error: {e}")
        return None

def main():
    run_id = os.environ.get("ITALY_RUN_ID", "manual_run")
    db = CountryDB("Italy")
    repo = ItalyRepository(db, run_id)
    
    logger.info(f"Starting Step 1: List Determinas (RunID: {run_id})")
    
    page_num = 0
    max_pages = 5 # config
    
    while page_num < max_pages:
        logger.info(f"Fetching page {page_num}...")
        data = get_search_results(page_num)
        
        if not data:
            break
            
        items = data.get("resourceList", [])
        if not items:
            break
            
        repo.insert_determinas(items)
        
        total_elements = data.get("totalElementNum", 0)
        if (page_num + 1) * 20 >= total_elements:
            break
            
        page_num += 1
        time.sleep(1)
        
    logger.info("Step 1 Complete.")

if __name__ == "__main__":
    main()
