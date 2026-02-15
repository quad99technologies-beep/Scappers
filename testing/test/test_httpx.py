#!/usr/bin/env python3
"""Test HTTPX extraction."""

import httpx
from bs4 import BeautifulSoup
import re

url = 'https://www.mercadopublico.cl/Procurement/Modules/RFB/DetailsAcquisition.aspx?qs=My/YIhJFyhDO6ryCsB09Tg=='

resp = httpx.get(url, timeout=30)
print(f'Status: {resp.status_code}')

soup = BeautifulSoup(resp.text, 'html.parser')

# Test extraction
tid = soup.find('span', {'id': 'lblNumLicitacion'})
print(f'Tender ID: {tid.get_text() if tid else "NOT FOUND"}')

title = soup.find('span', {'id': 'lblFicha1Nombre'})
print(f'Title: {title.get_text() if title else "NOT FOUND"}')

auth = soup.find('a', {'id': 'lnkFicha2Razon'}) or soup.find('span', {'id': 'lblFicha2Razon'})
print(f'Authority: {auth.get_text() if auth else "NOT FOUND"}')

# Check if we can find lot table
lot_table = soup.find('table', {'id': re.compile(r'.*grdItem.*', re.I)})
print(f'Lot table found: {lot_table is not None}')

# Check province
contact_div = soup.find('div', {'id': 'FichaContacto'})
if contact_div:
    text = contact_div.get_text()
    m = re.search(r'(Región\s+de\s+\w+|Región\s+\w+|Región\s+del?\s+\w+)', text, re.IGNORECASE)
    if m:
        print(f'Province: {m.group(1)}')
