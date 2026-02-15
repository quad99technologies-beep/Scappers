"""Find parent structure of Eigen risico dt element"""

from lxml import html

medikinet_html = open(r"C:\Users\Vishw\Downloads\MEDIKINET TABLET 5MG.html", "r", encoding="utf-8").read()
doc = html.fromstring(medikinet_html)

dt = doc.xpath('//dt[contains(@class,"not-reimbursed")]')[0]
print(f"DT element: {dt.text_content().strip()}")
print(f"DT class: {dt.get('class')}")
print()

parent = dt.getparent()
print(f"Parent tag: {parent.tag}")
print(f"Parent class: {parent.get('class', 'no-class')}")
print()

if parent is not None:
    grandparent = parent.getparent()
    if grandparent is not None:
        print(f"Grandparent tag: {grandparent.tag}")
        print(f"Grandparent class: {grandparent.get('class', 'no-class')}")
