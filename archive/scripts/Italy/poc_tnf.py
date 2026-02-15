
import requests
import json
from urllib.parse import urlencode

def main():
    # Example TNF item from logs:
    # ID: b7fdbf66-e4f4-414a-a7cc-dca086330183
    # Date: 2026-02-02T10:56:34Z
    
    item_id = "b7fdbf66-e4f4-414a-a7cc-dca086330183"
    pub_date = "2026-02-02T10:56:34Z"
    
    # Try different endpoints
    endpoints = [
        "https://trovanorme.aifa.gov.it/tnf-service/determina/tnf",
        "https://trovanorme.aifa.gov.it/tnf-service/determina/generic",
        "https://trovanorme.aifa.gov.it/tnf-service/determina/" + item_id
    ]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json, text/plain, */*"
    }
    
    # Try with .000Z format if missing
    formatted_date = pub_date
    if formatted_date.endswith("Z") and ".000Z" not in formatted_date:
            formatted_date = formatted_date.replace("Z", ".000Z")
            
    params = {
        "dataPubblicazione": formatted_date, # Try with and without
        "redazionale": item_id
    }
    
    print(f"Testing TNF fetch for {item_id}")
    
    # 1. Try /determina/tnf
    url = "https://trovanorme.aifa.gov.it/tnf-service/determina/tnf"
    try:
        print(f"Trying {url} with params {params}")
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        print(f"Status: {resp.status_code}")
        if resp.status_code == 200:
            print("Success!")
            print(resp.text[:200])
    except Exception as e:
        print(e)
        
    # 2. Try generic determinas endpoint if exists (guessing)
    # The user example has "detPres/UPR 101/2026" as title.
    
if __name__ == "__main__":
    main()
