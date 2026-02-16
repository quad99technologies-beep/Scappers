
import pdfplumber
import os

pdf_dir = r"d:\quad99\Scrappers\data\Italy\pdfs"
files = [f for f in os.listdir(pdf_dir) if f.endswith(".pdf")]

if files:
    
    target_file = os.path.join(pdf_dir, files[0])
    print(f"Inspecting: {target_file}")
    
    with pdfplumber.open(target_file) as pdf:
        for i, page in enumerate(pdf.pages):
            print(f"--- Page {i+1} ---")
            text = page.extract_text()
            print(text)
            print("\n")
            
            # tables = page.extract_tables()
            # for table in tables:
            #    print("Table found:")
            #    for row in table:
            #        print(row)
            #    print("-" * 20)
else:
    print("No PDFs found yet.")
