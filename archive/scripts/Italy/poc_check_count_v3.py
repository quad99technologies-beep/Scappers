
import requests
import json
import time

def check(query, params_update={}):
    url = "https://trovanorme.aifa.gov.it/tnf-service/ricerca/"
    params = {
        "pageSize": 20,
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
    params.update(params_update)
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json, text/plain, */*"
    }
    
    try:
        t0 = time.time()
        print(f"Request: query='{query}', params={params_update} ...")
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        print(f"Response in {time.time()-t0:.2f}s")
        if resp.status_code != 200:
            print(f"Status: {resp.status_code}")
            return
            
        data = resp.json()
        print(f"totalElementNum: {data.get('totalElementNum')}")
        print(f"elementAvailableNum: {data.get('elementAvailableNum')}")
        print(f"Items: {len(data.get('resourceList', []))}")
        print("-" * 20)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    # 1. Test pagination unlock attempt
    print("Testing pagination with totalElementNum=10031...")
    check("Riduzione", {"pageNum": 5, "totalElementNum": 10031}) # Page 5 starts at item 100. Should get 20 items if unlocked.
    
    # 2. Sort direction check - maybe 'asc', 'desc'
    # Default is likely desc (newest first). Let's try to get oldest.
    print("Testing sort 'asc'...")
    check("Riduzione", {"sortDirection": "asc"})
    print("Testing sort 'ascending'...")
    check("Riduzione", {"sortDirection": "ascending"})
    
    # 3. Year query
    print("Testing 'Riduzione 2025'...")
    check("Riduzione 2025")
    
    # 4. Filter by tipologia only?
    # Maybe only MSF?
    print("Testing minimal filters (only MSF)...")
    check("Riduzione", {
        "determinaGUSource": "false",
        "determinaTNFSource": "false",
        "documentoAIFASource": "false",
        "modificheSecondarieFarmaciSource": "true", # Keep only this
        "newsSource": "false",
        "tutti": "false" 
    })
