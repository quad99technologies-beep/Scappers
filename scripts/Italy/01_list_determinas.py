
import requests
import json
import time
import os
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
OUTPUT_DIR = r"d:\quad99\Scrappers\data\Italy"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "determinas_list.jsonl")
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

# ...
from core.io.file_writer import DataWriter

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    page_num = 0
    total_processed = 0
    max_pages = 5 
    
    logger.info(f"Starting search scrape. Saving to {OUTPUT_FILE}")

    with DataWriter(Path(OUTPUT_DIR), "determinas_list.jsonl") as writer:
        while page_num < max_pages:
            logger.info(f"Fetching page {page_num}...")
            # ... rest of loop ...
            # Replace f.write with writer.write_jsonl(item)

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
            
            for item in resource_list:
                # We only want basic info here. Details will be fetched in step 2.
                # Adding a timestamp for when we scraped this record
                item['scraped_at'] = datetime.now().isoformat()
                
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
                total_processed += 1
            
            # Flush periodically
            f.flush()
            
            # Check pagination end
            # Use data['elementAvailableNum'] or calculate from total
            if (page_num + 1) * 20 >= total_elements:
                logger.info("Reached end of results.")
                break
                
            page_num += 1
            time.sleep(1) # Be polite
            
    logger.info(f"Search completed. Processed {total_processed} items.")

if __name__ == "__main__":
    main()
