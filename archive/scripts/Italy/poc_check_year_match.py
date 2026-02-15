
import requests
import json

def check_year_match(query):
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
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json, text/plain, */*"
    }
    
    try:
        response = requests.get(base_url, params=params, headers=headers, timeout=10)
        data = response.json()
        items = data.get("resourceList", [])
        print(f"Query: '{query}'. Items: {len(items)}")
        if items:
            item = items[0]
            print(f"Title: {item.get('titolo')}")
            print(f"Date: {item.get('dataPubblicazione')}")
            # Check if 2024 is in the title or text?
            # We don't have full text here, just title.
    except Exception as e:
        print(e)

if __name__ == "__main__":
    check_year_match("Riduzione 2024")
