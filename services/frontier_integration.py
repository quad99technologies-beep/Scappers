#!/usr/bin/env python3
"""
Frontier Queue Integration Helper

Provides easy integration of frontier queue into pipeline workflows.
"""

import sys
from pathlib import Path

# Ensure repo root in sys.path when imported from pipelines (e.g. scripts/Argentina/)
_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

import logging
from typing import Optional, List, Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)


def initialize_frontier_for_scraper(scraper_name: str, redis_host: str = "localhost", 
                                    redis_port: int = 6379):
    """
    Initialize frontier queue for a scraper.
    
    Returns:
        Frontier instance or None if Redis not available
    """
    try:
        from core.pipeline.frontier import create_frontier
        frontier = create_frontier(scraper_name, redis_host=redis_host, redis_port=redis_port)
        logger.info(f"Frontier queue initialized for {scraper_name}")
        return frontier
    except ImportError:
        logger.debug("Frontier queue not available (Redis not installed)")
        return None
    except Exception as e:
        logger.warning(f"Failed to initialize frontier queue: {e}")
        return None


def add_seed_urls(scraper_name: str, urls: List[str], priority: int = 0):
    """Add seed URLs to frontier queue."""
    try:
        from core.utils.integration_helpers import add_url_to_frontier
        added = 0
        for url in urls:
            if add_url_to_frontier(scraper_name, url, priority=priority):
                added += 1
        logger.info(f"Added {added}/{len(urls)} seed URLs to frontier for {scraper_name}")
        return added
    except Exception as e:
        logger.debug(f"Failed to add seed URLs: {e}")
        return 0


def discover_urls_from_page(html: str, base_url: str, scraper_name: str,
                           url_patterns: Optional[List[str]] = None) -> List[str]:
    """
    Discover URLs from HTML page and add to frontier.
    
    Args:
        html: HTML content
        base_url: Base URL for resolving relative URLs
        scraper_name: Scraper name
        url_patterns: Optional list of URL patterns to match
    
    Returns:
        List of discovered URLs
    """
    try:
        from bs4 import BeautifulSoup
        from urllib.parse import urljoin
        
        soup = BeautifulSoup(html, 'html.parser')
        discovered = []
        
        # Find all links
        for link in soup.find_all('a', href=True):
            href = link['href']
            full_url = urljoin(base_url, href)
            
            # Filter by patterns if provided
            if url_patterns:
                if not any(pattern in full_url for pattern in url_patterns):
                    continue
            
            discovered.append(full_url)
        
        # Add to frontier
        for url in discovered:
            try:
                from core.utils.integration_helpers import add_url_to_frontier
                add_url_to_frontier(scraper_name, url, priority=2, parent_url=base_url)
            except Exception:
                pass
        
        logger.debug(f"Discovered {len(discovered)} URLs from {base_url}")
        return discovered
        
    except Exception as e:
        logger.debug(f"URL discovery failed: {e}")
        return []


def get_frontier_stats(scraper_name: str) -> Dict[str, Any]:
    """Get frontier queue statistics. Returns zeros when Redis unavailable."""
    try:
        frontier = initialize_frontier_for_scraper(scraper_name)
        if frontier:
            return frontier.get_stats()
    except Exception:
        pass
    return {
        "queued": 0,
        "seen": 0,
        "active": 0,
        "completed": 0,
        "failed": 0,
    }
