"""
URL Builder Utility for Netherlands Scraper
Builds search URLs with vorm (form) and sterkte (strength) parameters.
"""

from urllib.parse import quote
from typing import Optional


def build_combination_url(
    vorm: str,
    sterkte: str,
    search_term: str = "632 Medicijnkosten Drugs4",
    base_url: str = "https://www.medicijnkosten.nl/zoeken"
) -> str:
    """
    Build search URL with vorm and sterkte parameters.
    
    Args:
        vorm: Form type (e.g., "TABLETTEN EN CAPSULES")
        sterkte: Strength (e.g., "10/80MG")
        search_term: Search term to use (default: "632 Medicijnkosten Drugs4")
        base_url: Base URL for search (default: medicijnkosten.nl search)
    
    Returns:
        Full search URL with encoded parameters
    
    Example:
        >>> build_combination_url("TABLETTEN EN CAPSULES", "10/80MG")
        'https://www.medicijnkosten.nl/zoeken?searchTerm=632%20Medicijnkosten%20Drugs4&type=medicine&searchTermHandover=632%20Medicijnkosten%20Drugs4&vorm=TABLETTEN%20EN%20CAPSULES&sterkte=10%2F80MG'
    """
    params = {
        "searchTerm": search_term,
        "type": "medicine",
        "searchTermHandover": search_term,
        "vorm": vorm,
        "sterkte": sterkte
    }
    
    query_string = "&".join(f"{k}={quote(v)}" for k, v in params.items())
    return f"{base_url}?{query_string}"


def parse_combination_url(url: str) -> Optional[dict]:
    """
    Parse a combination URL to extract vorm and sterkte.
    
    Args:
        url: Full search URL
    
    Returns:
        Dict with 'vorm' and 'sterkte' keys, or None if parsing fails
    
    Example:
        >>> parse_combination_url("https://...&vorm=TABLETTEN&sterkte=10MG")
        {'vorm': 'TABLETTEN', 'sterkte': '10MG'}
    """
    from urllib.parse import urlparse, parse_qs
    
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        
        vorm = params.get('vorm', [None])[0]
        sterkte = params.get('sterkte', [None])[0]
        
        if vorm and sterkte:
            return {'vorm': vorm, 'sterkte': sterkte}
        return None
    except Exception:
        return None


if __name__ == "__main__":
    # Test the URL builder
    print("Testing URL Builder:")
    print("-" * 80)
    
    # Test 1: Basic URL building
    url1 = build_combination_url("TABLETTEN EN CAPSULES", "10/80MG")
    print(f"Test 1: {url1}")
    
    # Test 2: URL with special characters
    url2 = build_combination_url("INJECTIES & INFUSIES", "5MG/ML")
    print(f"Test 2: {url2}")
    
    # Test 3: Parse URL
    parsed = parse_combination_url(url1)
    print(f"Test 3: {parsed}")
    
    print("-" * 80)
    print("✓ URL Builder tests complete")
