from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException
from typing import Optional
from core.utils.text_utils import normalize_ws

def get_text_safe(root, css: str) -> Optional[str]:
    """Safely get text from element, checking presence before fetching values."""
    try:
        elements = root.find_elements(By.CSS_SELECTOR, css)
        if not elements:
            return None
        el = elements[0]
        try:
            _ = el.is_displayed()
        except StaleElementReferenceException:
            return None
        text = el.text
        return normalize_ws(text) if text else None
    except Exception:
        return None
