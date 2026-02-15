
import requests
import json
import time
import os
import re
import concurrent.futures
from urllib.parse import urlencode

# Configuration
OUTPUT_FILE = r"D:\quad99\Scrappers\output\Italy\price_reductions_full.jsonl"
BASE_URL = "https://trovanorme.aifa.gov.it/tnf-service/ricerca/"
DETAIL_URL_MSF = "https://trovanorme.aifa.gov.it/tnf-service/determina/msf"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://trovanorme.aifa.gov.it/"
}

# Shared state
SEEN_IDS = set()
RESULTS_BUFFER = []

def get_search_results(query, page_num=0):
    """Fetch search results for a given query and page."""
    params = {
        "pageSize": 100, # Use max valid page size
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
        if response.status_code != 200:
            print(f"Stats {response.status_code} for query '{query}'")
            return None
        return response.json()
    except Exception as e:
        print(f"Error searching '{query}': {e}")
        return None

def get_detail(item_id, pub_date, typology):
    """Fetch detail for a specific item."""
    if typology == "MSF":
        # Format date for detail API: Expects milliseconds .000Z
        formatted_date = pub_date
        if formatted_date and formatted_date.endswith("Z") and ".000Z" not in formatted_date:
            formatted_date = formatted_date.replace("Z", ".000Z")
        
        params = {
            "dataPubblicazione": formatted_date,
            "redazionale": item_id
        }
        
        try:
            response = requests.get(DETAIL_URL_MSF, params=params, headers=HEADERS, timeout=30)
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            print(f"Error fetching detail {item_id}: {e}")
    # Add other typologies if needed, currently only MSF supported/requested for price reductions
    return None

def parse_text(text):
    """Extract AIC codes and prices from text."""
    results = []
    if not text: return results
    text = text.replace("\n", " ")
    
    # Regex for AIC and Price
    pattern = r"A\.I\.C\. n\.?\s*(\d+).*?Prezzo\s*€\s*([\d,]+)"
    matches = re.findall(pattern, text, re.IGNORECASE)
    
    for aic, price in matches:
        results.append({
            "aic": aic,
            "price": price.replace(",", ".")
        })
    return results

def process_query(query):
    """Process a single search query, handling pagination (if possible) and detail fetching."""
    print(f"Processing query: '{query}'", flush=True)
    page_num = 0
    local_count = 0
    
    # We only check page 0 since pagination > 100 is broken/known issue.
    # But if API miraculously works for some queries, we could loop.
    # For now, just page 0 (limit 100).
    
    data = get_search_results(query, page_num)
    if not data: return
    
    resource_list = data.get("resourceList", [])
    total_elements = data.get("totalElementNum", 0)
    
    if total_elements >= 100:
        print(f"WARNING: Query '{query}' hit 100 limit. Data truncation possible.", flush=True)
    
    new_items_count = 0
    for item in resource_list:
        item_id = item.get("id")
        if item_id in SEEN_IDS:
            continue
        
        SEEN_IDS.add(item_id)
        new_items_count += 1
        
        # Check TITLE filter
        title = item.get("titolo", "").lower()
        if "riduzione di prezzo al pubblico" in title and "medicinal" in title:
             # Fetch detail
             typology = item.get("tipologia")
             pub_date = item.get("dataPubblicazione")
             detail = get_detail(item_id, pub_date, typology)
             
             record = {
                 "query_source": query,
                 "search_result": item,
                 "detail": detail,
                 "parsed_data": []
             }
             
             if detail and "testo" in detail:
                 record["parsed_data"] = parse_text(detail["testo"])
                 
             RESULTS_BUFFER.append(record)
    
    # Simple write to file periodically or at end? 
    # Since we use threads, appending to buffer is safer, write once or use lock.
    # We'll just write at the end or periodic flush roughly.

def main():
    start_time = time.time()
    
    # Generate Queries
    queries = set()
    queries.add("Riduzione")
    
    # Years
    for y in range(2010, 2028):
        queries.add(f"Riduzione {y}")
        
    # Months + Year (Recent years only where volume is high)
    months = ["gennaio", "febbraio", "marzo", "aprile", "maggio", "giugno", 
              "luglio", "agosto", "settembre", "ottobre", "novembre", "dicembre"]
    for y in range(2023, 2027):
        for m in months:
            queries.add(f"Riduzione {m} {y}")
            
    # Alphabet 2-char (aa..zz)
    # This might be overkill but ensures coverage of text matches "Riduzione aa..."
    # Warning: "Riduzione" followed by ANY word starting with...
    # queries like "Riduzione al" are very broad.
    # Let's try 1-char first? No, 2-char is safer.
    import string
    chars = string.ascii_lowercase
    for c1 in chars:
        for c2 in chars:
            queries.add(f"Riduzione {c1}{c2}")
            
    print(f"Generated {len(queries)} queries.", flush=True)
    
    # Ensure output directory
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    # Run in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_query, q) for q in queries]
        
        # Progress monitoring
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            if i % 10 == 0:
                print(f"Completed {i}/{len(queries)} queries. IDs found: {len(SEEN_IDS)}", flush=True)
    
    # Write results
    print(f"Writing {len(RESULTS_BUFFER)} records to {OUTPUT_FILE}", flush=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        for record in RESULTS_BUFFER:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            
    print(f"Done. Duration: {time.time() - start_time:.2f}s", flush=True)

if __name__ == "__main__":
    main()
