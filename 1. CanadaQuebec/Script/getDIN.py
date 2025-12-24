
import csv
import PyPDF2
import pandas as pd
import re
import os

# Improved Python script to convert "annexe_v.pdf" (RAMQ Liste des médicaments 2025-08) to structured CSVs.
# Improvements:
# - Handles multiple fill sizes (continuation lines without code/brand/company).
# - Better detection of categories (e.g., 8:08 ANTHELMINTIQUES).
# - Improved regex and line parsing to avoid missing data (e.g., section 4:00).
# - Handles PPB flags in strength or elsewhere.
# - Fixes encoding issues (uses utf-8 strictly).
# - Captures more hierarchical info (category, sub-category).
# - No data loss: Raw lines CSV as backup.

# Requirements: pip install PyPDF2 pandas

PDF_PATH =  r"D:\quad99\Scappers\1. CanadaQuebec\output\csv\annexe_v.pdf"   # change if needed
OUTPUT_RAW = "annexe_v_raw_lines.csv"
OUTPUT_ANNEXE_V = "annexe_v_exempt_drugs.csv"
OUTPUT_MAIN_DRUGS = "annexe_v_main_drug_list.csv"

if not os.path.exists(PDF_PATH):
    raise FileNotFoundError(f"PDF not found: {PDF_PATH}")

# Helper: Clean price (comma -> dot, strip)
def clean_price(text):
    if not text or text.strip() in ["-", ""]:
        return ""
    return text.strip().replace(" ", "").replace(",", ".")

# Helper: Parse a table row line
def parse_row(line, current_code, current_brand, current_company):
    # Remove extra spaces
    line = re.sub(r'\s{2,}', ' ', line.strip())
    parts = line.split(' ')
    if len(parts) < 3:
        return None

    if re.match(r'^\d{8}$', parts[0]):
        # Full row: code brand... company format pack [unit]
        code = parts[0]
        price_idx = len(parts) - 1 if re.match(r'^\d+[.,]?\d*$', parts[-1]) else len(parts) - 2
        unit_price = clean_price(parts[-1]) if len(parts) > price_idx and re.match(r'^\d+[.,]?\d*$', parts[-1]) else ""
        pack_price = clean_price(parts[price_idx]) if unit_price else clean_price(parts[-1])
        unit_price = unit_price if unit_price else ""
        format_str = ' '.join(parts[price_idx-1:price_idx]) if unit_price else ' '.join(parts[-2:-1])  # Last before prices
        company = parts[price_idx-2]
        brand = ' '.join(parts[1:price_idx-2])
        return code, brand, company, format_str, pack_price, unit_price
    else:
        # Continuation: format pack [unit]
        code = current_code
        brand = current_brand
        company = current_company
        price_idx = len(parts) - 1 if len(parts) > 1 and re.match(r'^\d+[.,]?\d*$', parts[-1]) else len(parts) - 2
        unit_price = clean_price(parts[-1]) if len(parts) > 1 and re.match(r'^\d+[.,]?\d*$', parts[-1]) else ""
        pack_price = clean_price(parts[-1]) if not unit_price else clean_price(parts[-2])
        format_str = ' '.join(parts[:-1]) if not unit_price else ' '.join(parts[:-2])
        return code, brand, company, format_str, pack_price, unit_price

# Step 1: Extract all text lines (no loss)
print("Extracting raw text lines...")
raw_lines = []
with open(PDF_PATH, "rb") as file:
    reader = PyPDF2.PdfReader(file)
    num_pages = len(reader.pages)
    print(f"Total pages: {num_pages}")
    
    for page_num in range(num_pages):
        page = reader.pages[page_num]
        text = page.extract_text() or ""
        lines = text.split("\n")
        for line in lines:
            raw_lines.append({"page": page_num + 1, "text": line.strip()})

raw_df = pd.DataFrame(raw_lines)
raw_df.to_csv(OUTPUT_RAW, index=False, encoding="utf-8-sig")  # utf-8-sig for accented chars
print(f"Raw lines saved to {OUTPUT_RAW} ({len(raw_df)} rows)")

# Step 2: Parse Annexe V (pages 1-3)
print("\nParsing Annexe V exemptions...")
annexe_data = []
current_category_code = ""
current_category_name = ""
current_sub = ""

for _, row in raw_df[raw_df['page'] <= 3].iterrows():
    text = row["text"]
    if not text:
        continue
    
    # Category code e.g., "28:28 agents anti-maniaques"
    cat_match = re.match(r"^(\d+:\d+(\.\d+)?)\s+(.+)$", text)
    if cat_match:
        current_category_code = cat_match.group(1)
        current_category_name = cat_match.group(3)
        continue
    
    # Sub sections like "sucre" or "médicaments d'exception"
    if re.match(r"^[a-z]+$", text) or text.lower().startswith("médicaments d'exception"):
        current_sub = text
        continue
    
    # Drug names, with notes like Co. L.A.
    if text and not text.startswith("2025-08") and not text.startswith("Annexe V") and "LISTE" not in text and "Symboles" not in text:
        notes = ""
        if re.search(r"Co\. \w+\.?$", text):
            notes = re.search(r"(Co\. \w+\.?)$", text).group(1)
            drug_name = text.replace(notes, "").strip()
        else:
            drug_name = text
        annexe_data.append({
            "category_code": current_category_code,
            "category_name": current_category_name,
            "sub_section": current_sub,
            "drug_name": drug_name,
            "notes": notes
        })

annexe_df = pd.DataFrame(annexe_data)
annexe_df.to_csv(OUTPUT_ANNEXE_V, index=False, encoding="utf-8-sig")
print(f"Annexe V saved to {OUTPUT_ANNEXE_V} ({len(annexe_df)} entries)")

# Step 3: Parse main drug list (from page 7+)
print("\nParsing main drug tables...")
drug_records = []
current_category = ""
current_subcategory = ""
current_generic = ""
current_formulation = ""
current_strength = ""
current_ppb = False
current_code = ""
current_brand = ""
current_company = ""

for _, row in raw_df[raw_df['page'] >= 7].iterrows():
    text = row["text"].strip()
    if not text or text.startswith("CODE MARQUE") or text.startswith("2025-08") or text.startswith("Page"):
        continue
    
    # Category e.g., "8:00 ANTI-INFECTIEUX"
    if re.match(r"^\d+:\d+ [A-ZÉÈÀ\-]+$", text):
        current_category = text
        continue
    
    # Subcategory e.g., "anthelmintiques"
    if re.match(r"^[a-zéèà\-]+$", text):
        current_subcategory = text
        continue
    
    # Generic name e.g., "TOBRAMYCINE (SULFATE DE)"
    if re.match(r"^[A-ZÉÈÀ\(\) ']+$", text) and len(text.split()) > 1:
        current_generic = text
        current_formulation = ""
        current_strength = ""
        current_ppb = False
        continue
    
    # Formulation e.g., "Sol. Inj." or "Co."
    if re.match(r"^[A-Z][a-z]\.(\s[A-Z][a-z]\.)?$", text) or text in ["Susp. Orale", "Pd. Inj."]:
        current_formulation = text
        current_strength = ""
        current_ppb = False
        continue
    
    # Strength e.g., "40 mg/mL PPB"
    if ("mg" in text or "mL" in text or "%" in text) and len(text.split()) < 5:
        current_strength = text.replace("PPB", "").strip()
        current_ppb = "PPB" in text
        continue
    
    # Table row or continuation
    parsed = parse_row(text, current_code, current_brand, current_company)
    if parsed:
        code, brand, company, fill_size, pack_price, unit_price = parsed
        if code:  # Update currents if new row
            current_code = code
            current_brand = brand
            current_company = company
        drug_records.append({
            "category": current_category,
            "subcategory": current_subcategory,
            "generic_name": current_generic,
            "formulation": current_formulation,
            "strength": current_strength,
            "ppb": current_ppb,
            "local_code": current_code,
            "brand_name": current_brand,
            "company_name": current_company,
            "fill_size": fill_size,
            "pack_price": pack_price,
            "unit_price": unit_price,
            "page": row["page"]
        })

main_df = pd.DataFrame(drug_records)
main_df.to_csv(OUTPUT_MAIN_DRUGS, index=False, encoding="utf-8-sig")
print(f"Main drug list saved to {OUTPUT_MAIN_DRUGS} ({len(main_df)} rows)")

print("\nConversion complete!")
print("""
Outputs:
- annexe_v_raw_lines.csv: Full text backup.
- annexe_v_exempt_drugs.csv: Exempt drugs.
- annexe_v_main_drug_list.csv: Structured drug data with multiples handled.

Notes:
- If some data still missed, check raw CSV for PDF extraction quirks (e.g., accents).
- For perfect tables, consider adding tabula-py (requires Java) for direct table extraction.
- Official source: https://www.ramq.gouv.qc.ca/fr/professionnels/pharmaciens/liste-medicaments
""")
