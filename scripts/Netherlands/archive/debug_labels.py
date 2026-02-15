"""Debug script to find why MEDIKINET Eigen risico is not found"""

import re
from lxml import html

def clean_single_line(text: str) -> str:
    t = (text or "").replace("\r", " ").replace("\n", " ").replace("\t", " ").replace("\xa0", " ")
    return re.sub(r"\s+", " ", t).strip()

# Test with MEDIKINET HTML
medikinet_html = open(r"C:\Users\Vishw\Downloads\MEDIKINET TABLET 5MG.html", "r", encoding="utf-8").read()
doc = html.fromstring(medikinet_html)

# Find all dt elements
dts = doc.xpath('//dl[contains(@class,"pat-grid-list")]/dt')
print(f"Found {len(dts)} dt elements in MEDIKINET")
print("="*60)

for i, dt_elem in enumerate(dts):
    label = clean_single_line(dt_elem.text_content())
    label_lower = label.lower()
    print(f"{i+1}. Label: '{label}'")
    print(f"   Lower: '{label_lower}'")
    print(f"   Contains 'eigen risico': {'eigen risico' in label_lower}")
    print()
