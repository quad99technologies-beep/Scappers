
import requests
import json
import time
import os
import re
from urllib.parse import urlencode

# Configuration
OUTPUT_FILE = r"D:\quad99\Scrappers\output\Italy\price_reductions.jsonl"
BASE_URL = "https://trovanorme.aifa.gov.it/tnf-service/ricerca/"
DETAIL_URL = "https://trovanorme.aifa.gov.it/tnf-service/determina/msf"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://trovanorme.aifa.gov.it/"
}

def get_page(page_num, page_size=20, query="Riduzione"):
    """Fetch a page of search results."""
    params = {
        "pageSize": page_size,
        "totalElementNum": 0,
        "pageNum": page_num,
        "sortColumn": "dataPubblicazione",
        "determinaGUSource": "true",
        "determinaTNFSource": "true",
        "documentoAIFASource": "true",
        "modificheSecondarieFarmaciSource": "true",
        "newsSource": "true",
        "tutti": "true",
        "parola": query
    }
    
    try:
        response = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching page {page_num}: {e}")
        return None

def get_detail(item_id, pub_date, typology):
    """Fetch detail for a specific item."""
    if typology == "MSF":
        # Format date for detail API: Expects milliseconds .000Z
        formatted_date = pub_date
        if formatted_date.endswith("Z") and ".000Z" not in formatted_date:
            formatted_date = formatted_date.replace("Z", ".000Z")
        
        params = {
            "dataPubblicazione": formatted_date,
            "redazionale": item_id
        }
        
        try:
            response = requests.get(DETAIL_URL, params=params, headers=HEADERS, timeout=30)
            if response.status_code == 200:
                print(f"Fetched detail for {item_id}")
                return response.json()
            else:
                print(f"Failed to fetch detail for {item_id}: Status {response.status_code}")
                # Try fallback without modification if 500? Or maybe different typology logic needed
                # For now just return None
                return None
        except Exception as e:
            print(f"Error fetching detail for {item_id}: {e}")
            return None
    elif typology == "TNF":
        # TNF might utilize a different endpoint or parameter structure.
        # Based on user example, only MSF URL was provided explicitly for detail.
        # Searching for patterns...
        # Let's try to assume /determina/tnf exists?
        # But for safety, I will skip non-MSF detail fetching until confirmed, 
        # or implement a generic fetch if ID and date are sufficient.
        
        # NOTE: User provided example ID 'b7fdbf66...' for TNF typology. 
        # The ID format is UUID-like, unlike MSF 'TX...'.
        # I'll log a warning and skip to avoid errors.
        print(f"Skipping detail fetch for typology {typology} (ID: {item_id}) - implementation pending.")
        return None
    
    return None

def parse_text(text):
    """Extract AIC codes and prices from text."""
    # Pattern: A.I.C. n. XXXXX - ... - Prezzo € XX,XX
    # This is a basic regex, might need refinement.
    results = []
    # Normalize text
    text = text.replace("\n", " ")
    
    # Split by product entries if possible, or just find all matches
    # Regex designed to capture AIC and Price near each other
    # Example: A.I.C. n. 049930011 - Prezzo € 24,24
    
    # Simple regex to find all AICs and Prices
    # Note: Using non-greedy match for content between AIC and Price
    pattern = r"A\.I\.C\. n\.?\s*(\d+).*?Prezzo\s*€\s*([\d,]+)"
    matches = re.findall(pattern, text, re.IGNORECASE)
    
    for aic, price in matches:
        results.append({
            "aic": aic,
            "price": price.replace(",", ".")
        })
    return results

def main():
    page_num = 0
    total_processed = 0
    max_pages = 50 # limit for safety, increase as needed
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    print(f"Starting scrape. Saving to {OUTPUT_FILE}", flush=True)
    
    with open(OUTPUT_FILE, 'a', encoding='utf-8') as f:
        while page_num < max_pages:
            print(f"Fetching page {page_num}...", flush=True)
            data = get_page(page_num)
            
            if not data:
                print("Failed to get data, retrying...", flush=True)
                time.sleep(5)
                data = get_page(page_num)
                if not data:
                    print("Skipping page due to errors.", flush=True)
                    page_num += 1
                    continue
            
            resource_list = data.get("resourceList", [])
            if not resource_list:
                print("No more items found.", flush=True)
                break
                
            total_elements = data.get("totalElementNum", 0)
            print(f"Found {len(resource_list)} items (Total available: {total_elements})", flush=True)
            
            for item in resource_list:
                item_id = item.get("id")
                typology = item.get("tipologia")
                pub_date = item.get("dataPubblicazione")
                
                # Fetch detailed info
                detail = get_detail(item_id, pub_date, typology)
                
                record = {
                    "search_result": item,
                    "detail": detail,
                    "parsed_data": []
                }
                
                if detail and "testo" in detail:
                    record["parsed_data"] = parse_text(detail["testo"])
                
                # Write immediately (JSONL)
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                f.flush()
                
                total_processed += 1
                # Be polite
                time.sleep(1)
            
            # Check if we reached the end
            if (page_num + 1) * 20 >= total_elements:
                print("Reached end of results.", flush=True)
                break
                
            page_num += 1
            time.sleep(2)
            
    print(f"Scraping completed. Processed {total_processed} items.", flush=True)

if __name__ == "__main__":
    main()
