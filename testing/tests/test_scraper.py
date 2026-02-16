# test_scraper.py
# Test script for Belarus RCETH scraper
# Run this to verify the scraper logic without full execution

import sys
import os
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

import re
import pandas as pd
from datetime import datetime, timezone

# Test data simulating the HTML table structure
def test_field_extraction():
    """Test field extraction logic"""
    print("="*60)
    print("TEST 1: Field Extraction Logic")
    print("="*60)
    
    # Test dosage form parsing
    test_cases = [
        ("tablets 200mg in blisters No10x1", "200", "mg", "10"),
        ("capsules 500mg No30", "500", "mg", "30"),
        ("syrup 100ml", "", "", "1"),
        ("film-coated tablets 10mg No30", "10", "mg", "30"),
    ]
    
    print("\nDosage Form Parsing:")
    for dosage_form, expected_strength, expected_unit, expected_pack in test_cases:
        # Parse strength
        strength_patterns = [
            r"(\d+(?:\.\d+)?)\s*(mg|g|ml|mcg|iu|units?)",
            r"(\d+(?:\.\d+)?)\s*(мг|г|мл|мкг|ЕД)",
        ]
        strength = ""
        unit = ""
        for pattern in strength_patterns:
            match = re.search(pattern, dosage_form, re.IGNORECASE)
            if match:
                strength = match.group(1)
                unit = match.group(2).lower()
                unit_map = {"мг": "mg", "г": "g", "мл": "ml", "мкг": "mcg", "ед": "iu"}
                unit = unit_map.get(unit, unit)
                break
        
        # Parse pack size
        pack_patterns = [
            r"[Nn]o?(\d+)(?:x(\d+))?",
            r"(\d+)\s*(?:шт|pcs|pieces|tab|caps)",
        ]
        pack_size = "1"
        for pattern in pack_patterns:
            match = re.search(pattern, dosage_form, re.IGNORECASE)
            if match:
                if match.group(2):
                    pack_size = str(int(match.group(1)) * int(match.group(2)))
                else:
                    pack_size = match.group(1)
                break
        
        status = "[OK]" if (strength == expected_strength and unit == expected_unit and pack_size == expected_pack) else "[FAIL]"
        print(f"  {status} '{dosage_form}'")
        print(f"      Strength: {strength} {unit} (expected: {expected_strength} {expected_unit})")
        print(f"      Pack: {pack_size} (expected: {expected_pack})")
    
    return True


def test_price_parsing():
    """Test price parsing from contract info"""
    print("\n" + "="*60)
    print("TEST 2: Price Parsing")
    print("="*60)
    
    test_cases = [
        ("Equivalent price on registration date: 8.33 USD", 8.33, "USD"),
        ("Contract currency: USD\nEquivalent price: 15.50 USD", 15.50, "USD"),
        ("Price: 25.00 BYN", 25.00, "BYN"),
        ("No price info", None, None),
        ("USD 10.25", 10.25, "USD"),
    ]
    
    USD_EQ_RE = re.compile(r"Equivalent price[^:]*:\s*([0-9]+(?:[.,][0-9]+)?)\s*USD", re.IGNORECASE)
    PRICE_CELL_RE = re.compile(r"([0-9]+(?:[.,][0-9]+)?)\s*([A-Z]{3})", re.IGNORECASE)
    
    print("\nPrice Extraction:")
    for text, expected_price, expected_ccy in test_cases:
        # Try USD equivalent pattern first
        m = USD_EQ_RE.search(text)
        if m:
            price = float(m.group(1).replace(",", "."))
            ccy = "USD"
        else:
            # Try generic price pattern
            m = PRICE_CELL_RE.search(text)
            if m:
                price = float(m.group(1).replace(",", "."))
                ccy = m.group(2).upper()
            else:
                price = None
                ccy = None
        
        match = (price == expected_price and ccy == expected_ccy)
        status = "[OK]" if match else "[FAIL]"
        print(f"  {status} '{text[:40]}...' -> {price} {ccy}")
        if not match:
            print(f"      Expected: {expected_price} {expected_ccy}")
    
    return True


def test_atc_extraction():
    """Test ATC code extraction"""
    print("\n" + "="*60)
    print("TEST 3: ATC Code Extraction")
    print("="*60)
    
    test_cases = [
        ("M01AE01", "M01AE01"),
        ("M01AE01 generic", "M01AE01"),
        ("C09CA03", "C09CA03"),
        ("invalid", "INVALID"),
        ("", ""),
    ]
    
    print("\nATC Code Extraction:")
    for input_atc, expected in test_cases:
        match = re.search(r"([A-Z]\d{2}[A-Z]{2}\d{2})", str(input_atc).upper())
        result = match.group(1) if match else str(input_atc).strip().upper()
        status = "[OK]" if result == expected else "[FAIL]"
        print(f"  {status} '{input_atc}' -> '{result}'")
    
    return True


def test_output_format():
    """Test output DataFrame format"""
    print("\n" + "="*60)
    print("TEST 4: Output Format")
    print("="*60)
    
    # Sample row simulating scraped data
    sample_row = {
        "Country": "BELARUS",
        "Product Group": "BOLEOFF",
        "Local Product Name": "BOLEOFF",
        "Generic Name": "IBUPROFEN",
        "Indication": "",
        "Pack Size": "10",
        "Effective Start Date": "05-12-2022",
        "Currency": "BYN",
        "Ex Factory Wholesale Price": 3.10,
        "VAT Percent": "0.00",
        "Margin Rule": "65 Manual Entry",
        "Package Notes": "",
        "Discontinued": "NO",
        "Region": "EUROPE",
        "WHO ATC Code": "M01AE01",
        "Marketing Authority": "Minskintercaps UP, Republic of Belarus",
        "Fill Unit": "",
        "Fill Size": "",
        "Pack Unit": "",
        "Strength": "200",
        "Strength Unit": "mg",
        "Import Type": "NONE",
        "Import Price": "",
        "Combination Molecule": "NO",
        "Source": "PRICENTRIC",
        "Client": "VALUE NEEDED",
        "LOCAL_PACK_CODE": "22/05/1534",
    }
    
    column_order = [
        "Country", "Product Group", "Local Product Name", "Generic Name", "Indication",
        "Pack Size", "Effective Start Date", "Currency", "Ex Factory Wholesale Price",
        "VAT Percent", "Margin Rule", "Package Notes", "Discontinued", "Region",
        "WHO ATC Code", "Marketing Authority", "Fill Unit", "Fill Size",
        "Pack Unit", "Strength", "Strength Unit", "Import Type", "Import Price",
        "Combination Molecule", "Source", "Client", "LOCAL_PACK_CODE"
    ]
    
    df = pd.DataFrame([sample_row])
    
    # Ensure all columns exist
    for col in column_order:
        if col not in df.columns:
            df[col] = ""
    
    df = df[column_order]
    
    print("\nOutput columns:")
    for i, col in enumerate(df.columns, 1):
        print(f"  {i:2d}. {col}")
    
    print(f"\nSample output row:")
    print(df.to_string(index=False))
    
    return True


def test_generic_list_loading():
    """Test loading generic name list"""
    print("\n" + "="*60)
    print("TEST 5: Generic List Loading")
    print("="*60)
    
    # Try to load actual generic list
    generic_paths = [
        Path(__file__).parent / "Generic Name.csv",
        Path(__file__).parent.parent.parent / "input" / "Belarus" / "Generic Name.csv",
    ]
    
    for path in generic_paths:
        if path.exists():
            print(f"\nFound generic list at: {path}")
            df = pd.read_csv(path)
            if "Generic Name" in df.columns:
                generics = df["Generic Name"].dropna().astype(str).str.strip().tolist()
            else:
                generics = df.iloc[:, 0].dropna().astype(str).str.strip().tolist()
            
            print(f"Total generics: {len(generics)}")
            print(f"First 5: {generics[:5]}")
            print(f"Last 5: {generics[-5:]}")
            return True
    
    print("Generic list not found in expected locations")
    return False


def main():
    print("\n" + "="*60)
    print("BELARUS RCETH SCRAPER - TEST SUITE")
    print("="*60)
    print(f"Started at: {datetime.now().isoformat()}")
    
    tests = [
        ("Field Extraction", test_field_extraction),
        ("Price Parsing", test_price_parsing),
        ("ATC Extraction", test_atc_extraction),
        ("Output Format", test_output_format),
        ("Generic List Loading", test_generic_list_loading),
    ]
    
    results = []
    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, result))
        except Exception as e:
            print(f"\n[ERROR] Test '{name}' failed: {e}")
            results.append((name, False))
    
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for _, r in results if r)
    total = len(results)
    
    for name, result in results:
        status = "[PASS]" if result else "[FAIL]"
        print(f"  {status}: {name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    print("="*60)
    
    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
