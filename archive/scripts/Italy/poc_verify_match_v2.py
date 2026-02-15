
import requests
import json

def verify_match(query, target_id):
    base_url = "https://trovanorme.aifa.gov.it/tnf-service/ricerca/"
    params = {
        "pageSize": 100,
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
            print(f"Missed {target_id}")

    except Exception as e:
        print(e)

if __name__ == "__main__":
    target = "TX25ADD10763"
    # Company is TOWA matches "T"
    verify_match("Riduzione TOWA", target)
    verify_match("Riduzione T", target) # "T" as word? Or prefix?
    verify_match("Riduzione TO*", target) # "TO" prefix?
    verify_match("Riduzione *", target) # Wildcard?
