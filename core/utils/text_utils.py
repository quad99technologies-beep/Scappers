
import re
import unicodedata
from typing import Optional, Tuple

def normalize_ws(s: str) -> str:
    """Normalize whitespace in string (collapse multiple spaces, strip)."""
    if not s:
        return ""
    return " ".join(s.split())

def clean_text(s: str) -> str:
    """Alias for normalize_ws."""
    return normalize_ws(s)

def strip_accents(s: str) -> str:
    """Remove accents/diacritics from a string."""
    if not s:
        return ""
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))

def nk(s: str) -> str:
    """
    Normalize key string: remove accents, normalize unicode, lowercase, remove extra whitespace.
    Commonly used for creating lookup keys.
    """
    if not s:
        return ""
    s_cleaned = strip_accents(normalize_ws(s))
    return s_cleaned.lower()

def extract_digits(s: str) -> str:
    """Extract only digits from a string."""
    if not s:
        return ""
    return "".join(filter(str.isdigit, s))

def extract_price(text: str) -> Optional[float]:
    """
    Extract the first float value from a string.
    Handles '€ 1.234,56' (European) and '$1,234.56' (US) formats heuristically if simple.
    For complex locale parsing, use specific locale libraries.
    This simple version handles dot-decimal or comma-decimal if explicit.
    """
    if not text:
        return None
    # Remove non-numeric characters except . and ,
    cleaned = re.sub(r"[^\d.,]", "", text)
    if not cleaned:
        return None
    
    # Heuristic: if ',' is last separator, it's decimal (e.g. 1.000,00)
    if ',' in cleaned and '.' in cleaned:
        if cleaned.rfind(',') > cleaned.rfind('.'):
            # European format: 1.000,00 -> 1000.00
            cleaned = cleaned.replace('.', '').replace(',', '.')
        else:
            # US format: 1,000.00 -> 1000.00
            cleaned = cleaned.replace(',', '')
    elif ',' in cleaned:
        # If only comma, assume decimal if it looks like price (e.g. 12,50)
        # OR assume separator if it looks like thousands (e.g. 1,000)
        # Detailed logic is complex, defaulting to replace comma with dot for simple prices
        cleaned = cleaned.replace(',', '.')
            
    try:
        return float(cleaned)
    except ValueError:
        return None
