
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
    except Exception as e:
        print(e)

if __name__ == "__main__":
    keywords = [
        "Riduzione mg", "Riduzione ml", "Riduzione g", 
        "Riduzione compresse", "Riduzione capsule", "Riduzione fiale",
        "Riduzione soluzione", "Riduzione sospensione", "Riduzione crema",
        "Riduzione gel"
    ]
    for k in keywords:
        check(k)
