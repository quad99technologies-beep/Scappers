
import requests
import json

def check(query):
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
        print(f"Query: '{query}'. Total: {data.get('totalElementNum')}, Avail: {data.get('elementAvailableNum')}")
        items = data.get("resourceList", [])
        if items:
            print(f"Sample ID: {items[0].get('id')}")
    except Exception as e:
        print(e)

if __name__ == "__main__":
    check("Riduzione TX26")
    check("Riduzione TX26*") # Wildcard?
    check("Riduzione TX26ADD") # Prefix of ID?
    check("Riduzione TX26ADD1253") # Full ID?
