
import sys
import os
sys.path.append(os.path.abspath("scripts/canada_ontario"))


def norm(s: str) -> str:
    return (s or "").strip()

import re

# Copying the function here to test isolation, or import if possible.
# Ideally import to test actual code.
try:
    from extract_product_details_01 import parse_brand_details 
except ImportError:
    # It's likely named 01_extract_product_details.py which is hard to import.
    # I will rely on reading the file or just copy-paste for this test if import fails.
    # Actually, let's try to import dynamically.
    import importlib.util
    spec = importlib.util.spec_from_file_location("extract_module", "scripts/canada_ontario/01_extract_product_details.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    parse_brand_details = module.parse_brand_details

samples = [
    ("Pentasa 1g Supp", ("1g", "Supp", None)),
    ("Salofalk 500mg Tab", ("500mg", "Tab", None)),
    ("Mezera 1g", ("1g", None, None)),
    ("Tylenol 325mg Cap", ("325mg", "Cap", None)),
    ("Random Drug 5ml Inj", ("5ml", "Inj", None)),
    ("Pack test 100 PK", (None, None, "100")),
]

print("Verifying parse_brand_details...")
for s, expected in samples:
    result = parse_brand_details(s)
    print(f"Input: '{s}' -> Result: {result}")
    # loose check
    if result[0] != expected[0] and expected[0] is not None:
        print(f"  MISMATCH Strength: expected {expected[0]}, got {result[0]}")
    if result[1] != expected[1] and expected[1] is not None:
        print(f"  MISMATCH Dosage: expected {expected[1]}, got {result[1]}")
    if result[2] != expected[2] and expected[2] is not None:
        print(f"  MISMATCH Pack: expected {expected[2]}, got {result[2]}")

print("Done.")
