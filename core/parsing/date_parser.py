import re
from datetime import datetime
from typing import Optional

def parse_date(s: str) -> Optional[str]:
    """Parse date: '(24/07/25)' or '24/07/25' -> '2025-07-24'"""
    s = (s or "").strip()
    m = re.search(r"\((\d{2})/(\d{2})/(\d{2})\)", s) or re.search(r"\b(\d{2})/(\d{2})/(\d{2})\b", s)
    if m:
        d, mn, y = map(int, m.groups())
        y += 2000
        try:
            return datetime(y, mn, d).date().isoformat()
        except (ValueError, OverflowError):
            return None
            
    # Support DD.MM.YYYY
    m = re.search(r"\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b", s)
    if m:
        d, mn, y = map(int, m.groups())
        try:
            return datetime(y, mn, d).date().isoformat()
        except (ValueError, OverflowError):
            return None

    # Support YYYY.MM.DD
    m = re.search(r"\b(\d{4})\.(\d{1,2})\.(\d{1,2})\b", s)
    if m:
        y, mn, d = map(int, m.groups())
        try:
            return datetime(y, mn, d).date().isoformat()
        except (ValueError, OverflowError):
            return None

    return None

def russia_extract_date(cell_text: str) -> str:
    """Extract date from cell text like '531.51 \n03/15/2010'"""
    lines = [ln.strip() for ln in (cell_text or "").splitlines() if ln.strip()]
    if not lines:
        return ""
    return lines[1].strip() if len(lines) > 1 else ""
