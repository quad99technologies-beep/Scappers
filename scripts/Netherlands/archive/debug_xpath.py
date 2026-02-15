"""Debug script to find all dt elements"""

from lxml import html

# Test with MEDIKINET HTML
medikinet_html = open(r"C:\Users\Vishw\Downloads\MEDIKINET TABLET 5MG.html", "r", encoding="utf-8").read()
doc = html.fromstring(medikinet_html)

# Try different XPaths
xpaths = [
    '//dl[contains(@class,"pat-grid-list")]/dt',
    '//dl[@class="pat-grid-list"]/dt',
    '//dt',
    '//dt[contains(@class,"not-reimbursed")]',
]

for xpath in xpaths:
    dts = doc.xpath(xpath)
    print(f"\nXPath: {xpath}")
    print(f"Found: {len(dts)} elements")
    for i, dt in enumerate(dts[:10]):  # Show first 10
        text = dt.text_content().strip()[:50]
        classes = dt.get('class', 'no-class')
        print(f"  {i+1}. [{classes}] {text}")
