import re
import math
from typing import Optional

def parse_price(val: str) -> str:
    """Extract numeric price from string"""
    if not val:
        return ""
    nums = re.findall(r"[\d\s]+", val.replace(" ", ""))
    return nums[0] if nums else val.strip()

def ar_money_to_float(s: str) -> Optional[float]:
    """Convert Argentine money format to float: '$ 1.234,56' -> 1234.56"""
    if not s:
        return None
    t = re.sub(r"[^\d\.,]", "", s.strip())
    if not t:
        return None
    # AR: dot thousands, comma decimals
    t = t.replace(".", "").replace(",", ".")
    try:
        return float(t)
    except ValueError:
        return None
