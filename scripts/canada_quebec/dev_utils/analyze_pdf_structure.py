import pdfplumber
import re
from pathlib import Path

# Path to the PDF
pdf_path = r"D:\quad99\Scrappers\output\CanadaQuebec\split_pdf\annexe_iv2.pdf"

def analyze_pdf(path):
    print(f"Analyzing: {path}")
    try:
        with pdfplumber.open(path) as pdf:
            print(f"Total Pages: {len(pdf.pages)}")
            
            data_page_found = False
            for p_idx in range(len(pdf.pages)):
                page = pdf.pages[p_idx]
                text = page.extract_text()
                
                # Check for DINs (at least 3 to consider it a data page)
                dins = re.findall(r"\b\d{6,8}\b", text)
                if len(dins) >= 3:
                    print(f"\n--- Found Data Page at Index {p_idx} (Page {p_idx + 1}) ---")
                    print(f"DINs found: {dins[:5]}...")
                    
                    # Dump layout for this page
                    words = page.extract_words(keep_blank_chars=True)
                    print(f"Word count: {len(words)}")
                    
                    lines = {}
                    for w in words:
                        top = round(w['top'], 1)
                        if top not in lines:
                            lines[top] = []
                        lines[top].append(w) # Store full word obj for x positioning
                    
                    sorted_tops = sorted(lines.keys())
                    print("\nSample Data Lines (with x positions):")
                    for top in sorted_tops[:30]: # Print first 30 lines
                        line_words = sorted(lines[top], key=lambda x: x['x0'])
                        line_str = ""
                        for w in line_words:
                            line_str += f"{w['text']}(x={int(w['x0'])}) "
                        try:
                            print(f"[y={top}] {line_str}")
                        except UnicodeEncodeError:
                            print(f"[y={top}] {line_str.encode('ascii', 'replace').decode()}")
                    
                    data_page_found = True
                    break
            
            if not data_page_found:
                print("No data pages with >3 DINs found in the entire PDF.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if Path(pdf_path).exists():
        analyze_pdf(pdf_path)
    else:
        print(f"File not found: {pdf_path}")
