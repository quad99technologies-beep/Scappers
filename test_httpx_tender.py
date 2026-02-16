#!/usr/bin/env python3
"""Test fetching Tender Chile data using httpx"""

import httpx
import asyncio
from pathlib import Path

# Test URLs from the CSV
QS_PARAM = "My/YIhJFyhDO6ryCsB09Tg=="
TENDER_ID = "2786-47-LE21"

DETAILS_URL = f"https://www.mercadopublico.cl/Procurement/Modules/RFB/DetailsAcquisition.aspx?qs={QS_PARAM}"
AWARD_URL = f"https://www.mercadopublico.cl/Procurement/Modules/RFB/StepsProcessAward/PreviewAwardAct.aspx?qs={QS_PARAM}"

print(f"Testing Tender ID: {TENDER_ID}")
print(f"QS Parameter: {QS_PARAM}")
print()

async def fetch_url(client: httpx.AsyncClient, url: str, name: str):
    """Fetch a URL and return response info"""
    print(f"[{name}] Fetching: {url}")
    try:
        response = await client.get(url, follow_redirects=True, timeout=30.0)
        print(f"[{name}] Status: {response.status_code}")
        print(f"[{name}] Content-Type: {response.headers.get('content-type', 'unknown')}")
        print(f"[{name}] Content length: {len(response.text)} chars")
        
        # Save response for inspection
        output_file = Path(__file__).parent / f"test_{name.lower()}_response.html"
        output_file.write_text(response.text, encoding="utf-8")
        print(f"[{name}] Saved to: {output_file}")
        
        return response
    except Exception as e:
        print(f"[{name}] Error: {e}")
        return None

async def main():
    # Create client with browser-like headers
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    
    async with httpx.AsyncClient(headers=headers, http2=True) as client:
        # Fetch details page
        details_response = await fetch_url(client, DETAILS_URL, "DETAILS")
        print()
        
        # Fetch award page
        award_response = await fetch_url(client, AWARD_URL, "AWARD")
        print()
        
        # Summary
        if details_response and details_response.status_code == 200:
            print("[SUMMARY] Details page: OK")
            # Check if it contains expected content
            if "Licitaci" in details_response.text or "Tender" in details_response.text:
                print("[SUMMARY] Details page contains tender data")
            else:
                print("[SUMMARY] WARNING: Details page may be blocked or require JS")
        else:
            print("[SUMMARY] Details page: FAILED")
            
        if award_response and award_response.status_code == 200:
            print("[SUMMARY] Award page: OK")
        else:
            print("[SUMMARY] Award page: FAILED")

if __name__ == "__main__":
    asyncio.run(main())
