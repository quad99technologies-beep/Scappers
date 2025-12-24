import re

def clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def normalize_numbers_in_line(s: str) -> str:
    """
    1) Join thousands groups: "1 296,00" -> "1296,00"
    2) Replace comma with dot: "1296,00" -> "1296.00"

    Important: Only remove spaces that are thousand separators within a number,
    not spaces between separate numbers.
    """
    s = s.replace("\u00A0", " ")

    # Join multi-thousands first (most specific patterns first)
    # Pattern: digit space 3digits space 3digits , decimals
    s = re.sub(r"(\d)\s(?=\d{3}\s\d{3},\d{2,4}\b)", r"\1", s)
    s = re.sub(r"(\d)\s(?=\d{3}\s\d{3}\s\d{3},\d{2,4}\b)", r"\1", s)

    # Join single thousands: "1 296,00" -> "1296,00"
    # Only match if we have exactly 3 digits followed by comma (to avoid matching between separate numbers)
    s = re.sub(r"\b(\d{1,3})\s(?=\d{3},\d{2,4}\b)", r"\1", s)

    # Replace all decimal commas with dots
    s = s.replace(",", ".")

    return clean_spaces(s)

test_cases = [
    "02258595 Humira (seringue) AbbVie 2 1428,48 714,2400",
    "02343541 Prolia Amgen 1 330,00",
    "02368153 Xgeva Amgen 1 538,45",
    "02245619 Copaxone Teva Innov 30 1296,00 43,20",
    "01968017 Neupogen Amgen 10 1731,89 173,1890",
]

for test in test_cases:
    result = normalize_numbers_in_line(test)
    print(f"Input:  {test}")
    print(f"Output: {result}")
    print()
