
import requests
import json

def check_count(query, page_num=0, total_limit=0):
    base_url = "https://trovanorme.aifa.gov.it/tnf-service/ricerca/"
    params = {
        "pageSize": 20,
        "totalElementNum": total_limit,
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
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json, text/plain, */*"
    }
    
    try:
        response = requests.get(base_url, params=params, headers=headers, timeout=10)
        data = response.json()
        print(f"Query: '{query}', Page: {page_num}, ReqTotal: {total_limit}")
        print(f"totalElementNum: {data.get('totalElementNum')}")
        print(f"elementAvailableNum: {data.get('elementAvailableNum')}")
        items = data.get("resourceList", [])
        print(f"Items returned: {len(items)}")
        if items:
            print(f"First Item Title: {items[0].get('titolo', '').strip()[:50]}")
        print("-" * 20)
    except Exception as e:
        print(f"Error for '{query}': {e}")

if __name__ == "__main__":
    # Test 1: PageSize 200
    print("Testing pageSize=200...")
    check_count("Riduzione", page_num=0, total_limit=0) 
    
    # Test 2: Date filter guesses
    # Try date format YYYY-MM-DD or DD/MM/YYYY
    print("Testing Date Filters...")
    
    date_params = {
        "dataPubblicazioneDal": "2026-01-01",
        "dateFrom": "2026-01-01",
        "startDate": "2026-01-01"
    }
    
    # We need to pass these as extra params to check_count, but check_count doesn't support kwargs yet.
    # I'll just hardcode a requests call here for speed or modify check_count slightly.
    
    # Let's modify check_count to accept extra params
    pass

def check_count_v2(query, extra_params={}):
    base_url = "https://trovanorme.aifa.gov.it/tnf-service/ricerca/"
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
    params.update(extra_params)
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json, text/plain, */*"
    }
    
    try:
        response = requests.get(base_url, params=params, headers=headers, timeout=10)
        data = response.json()
        print(f"Query: '{query}', Params: {extra_params}")
        print(f"totalElementNum: {data.get('totalElementNum')}")
        print(f"elementAvailableNum: {data.get('elementAvailableNum')}")
        print("-" * 20)
    except Exception as e:
        print(f"Error for '{query}' with {extra_params}: {e}")

if __name__ == "__main__":
    # Test PageSize via check_count_v2
    check_count_v2("Riduzione", {"pageSize": 200})
    
    # Test Date Filters
    check_count_v2("Riduzione", {"dataPubblicazioneDal": "01/01/2026"})
    check_count_v2("Riduzione", {"dataPubblicazioneMin": "2026-01-01"})
    check_count_v2("Riduzione", {"from": "2026-01-01"})
