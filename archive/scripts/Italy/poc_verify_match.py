
import requests
import json

def verify_match(query, target_id):
    base_url = "https://trovanorme.aifa.gov.it/tnf-service/ricerca/"
    params = {
        "pageSize": 100, # Use max valid page size
        "totalElementNum": 0,
        "pageNum": 0,
        "sortColumn": "dataPubblicazione",
        "determinaGUSource": "true",
        "determinaTNFSource": "true",
        "documentoAIFASource": "true",
        "modificheSecondarieFarmaciSource": "true",
        "newsSource": "true",
        "tutti": "true",
        "parola": query
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json, text/plain, */*"
    }
    
    try:
        response = requests.get(base_url, params=params, headers=headers, timeout=10)
        data = response.json()
        items = data.get("resourceList", [])
        print(f"Query: '{query}'. Found: {len(items)}")
        
        found = False
        for item in items:
            if item.get("id") == target_id:
                print(f"MATCH! Found {target_id}")
                found = True
                break
        
        if not found:
            print(f"Missed {target_id} in results.")
            # Print first 3 IDs to see what we got
            ids = [i.get('id') for i in items[:3]]
            print(f"Top 3 IDs: {ids}")

    except Exception as e:
        print(e)

if __name__ == "__main__":
    # Target: TX25ADD10763 (from previous run)
    verify_match("Riduzione 2025", "TX25ADD10763")
    verify_match("Riduzione", "TX25ADD10763") # Baseline
