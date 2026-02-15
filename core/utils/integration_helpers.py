#!/usr/bin/env python3
"""
Integration Helpers for Core Features

Provides helper functions to integrate Proxy Pool, Geo Router, Schema Inference,
and Frontier Queue into scrapers.
"""

import logging
from typing import Optional, Dict, Any, List
from pathlib import Path

logger = logging.getLogger(__name__)


def get_geo_config_for_scraper(scraper_name: str) -> Optional[Dict[str, Any]]:
    """
    Get geo routing configuration for a scraper.
    
    Returns:
        Dict with timezone, locale, geolocation, proxy config, or None if not available
    """
    try:
        from core.network.geo_router import get_geo_router
        router = get_geo_router()
        route_config = router.get_route(scraper_name)
        
        if not route_config:
            return None
        
        config = {
            "timezone": route_config.timezone,
            "locale": route_config.locale,
            "country_code": route_config.country_code,
        }
        
        # Add geolocation for known countries
        geolocations = {
            "MY": {"latitude": 3.139, "longitude": 101.6869},
            "AR": {"latitude": -34.6037, "longitude": -58.3816},
            "NL": {"latitude": 52.3676, "longitude": 4.9041},
            "IN": {"latitude": 28.6139, "longitude": 77.2090},
            "RU": {"latitude": 55.7558, "longitude": 37.6173},
        }
        if route_config.country_code in geolocations:
            config["geolocation"] = geolocations[route_config.country_code]
        
        # Get proxy if available
        try:
            proxy = router.proxy_pool.get_proxy(
                country_code=route_config.country_code,
                proxy_type=route_config.proxy_type
            )
            if proxy and proxy.status.value == "healthy":
                config["proxy"] = {
                    "host": proxy.host,
                    "port": proxy.port,
                    "username": proxy.username,
                    "password": proxy.password,
                    "protocol": proxy.protocol,
                }
        except Exception as e:
            logger.debug(f"Proxy not available for {scraper_name}: {e}")
        
        return config
    except ImportError:
        logger.debug("Geo router not available")
        return None
    except Exception as e:
        logger.debug(f"Geo routing failed: {e}")
        return None


def apply_proxy_to_selenium_options(opts, scraper_name: str) -> bool:
    """
    Apply proxy from proxy pool to Selenium options.
    
    Args:
        opts: Selenium Options object (Chrome or Firefox)
        scraper_name: Name of scraper for geo routing
    
    Returns:
        True if proxy was applied, False otherwise
    """
    try:
        geo_config = get_geo_config_for_scraper(scraper_name)
        if not geo_config or "proxy" not in geo_config:
            return False
        
        proxy = geo_config["proxy"]
        proxy_url = f"{proxy['protocol']}://{proxy['host']}:{proxy['port']}"
        
        # Apply proxy based on browser type
        if hasattr(opts, 'add_argument'):  # Chrome
            opts.add_argument(f"--proxy-server={proxy_url}")
            if proxy.get("username") and proxy.get("password"):
                # Chrome doesn't support auth in proxy-server, need extension or manual handling
                logger.warning("Chrome proxy authentication not supported via arguments")
        elif hasattr(opts, 'set_preference'):  # Firefox
            opts.set_preference("network.proxy.type", 1)
            opts.set_preference("network.proxy.http", proxy["host"])
            opts.set_preference("network.proxy.http_port", proxy["port"])
            opts.set_preference("network.proxy.ssl", proxy["host"])
            opts.set_preference("network.proxy.ssl_port", proxy["port"])
            if proxy.get("username") and proxy.get("password"):
                # Firefox proxy auth requires manual handling
                logger.warning("Firefox proxy authentication requires manual handling")
        
        logger.info(f"[PROXY] Applied proxy {proxy['host']}:{proxy['port']} to {scraper_name}")
        return True
    except Exception as e:
        logger.debug(f"Failed to apply proxy: {e}")
        return False


def report_proxy_success(scraper_name: str, proxy_id: Optional[str] = None, response_time_ms: float = 0):
    """Report successful proxy usage for health tracking."""
    try:
        from core.network.geo_router import get_geo_router
        router = get_geo_router()
        if proxy_id:
            router.proxy_pool.report_success(proxy_id, response_time_ms=response_time_ms)
    except Exception:
        pass


def report_proxy_failure(scraper_name: str, proxy_id: Optional[str] = None, error_type: str = "unknown"):
    """Report failed proxy usage for health tracking."""
    try:
        from core.network.geo_router import get_geo_router
        router = get_geo_router()
        if proxy_id:
            router.proxy_pool.report_failure(proxy_id, error_type=error_type)
    except Exception:
        pass


def infer_schema_for_selector(html: str, old_selector: str, expected_fields: List[str], 
                              scraper_name: str) -> Optional[Dict[str, Any]]:
    """
    Use schema inference to suggest new selectors when old ones break.
    
    Args:
        html: HTML content
        old_selector: Broken selector
        expected_fields: List of field names to extract
        scraper_name: Scraper name for context
    
    Returns:
        Dict with suggested schema and selectors, or None if inference fails
    """
    try:
        from core.data.schema_inference import LLMSchemaInference
        inference = LLMSchemaInference()
        schema = inference.infer_schema(html, expected_fields)
        
        if schema and schema.fields:
            return {
                "fields": [
                    {
                        "name": f.name,
                        "selector": f.selector,
                        "confidence": f.confidence,
                        "alternatives": f.alternatives,
                    }
                    for f in schema.fields
                ],
                "confidence": schema.confidence,
            }
    except ImportError:
        logger.debug("Schema inference not available")
    except Exception as e:
        logger.debug(f"Schema inference failed: {e}")
    return None


def add_url_to_frontier(scraper_name: str, url: str, priority: int = 2, 
                        parent_url: Optional[str] = None) -> bool:
    """
    Add URL to crawl frontier queue.
    
    Args:
        scraper_name: Scraper name
        url: URL to add
        priority: Priority (0=critical, 1=high, 2=normal, 3=low, 4=optional)
        parent_url: Parent URL that discovered this URL
    
    Returns:
        True if added successfully
    """
    try:
        from core.pipeline.frontier import create_frontier
        frontier = create_frontier(scraper_name)
        frontier.add_url(url, priority=priority, parent_url=parent_url)
        return True
    except ImportError:
        logger.debug("Frontier queue not available")
    except Exception as e:
        logger.debug(f"Failed to add URL to frontier: {e}")
    return False


def get_next_url_from_frontier(scraper_name: str) -> Optional[str]:
    """
    Get next URL from crawl frontier queue.
    
    Args:
        scraper_name: Scraper name
    
    Returns:
        URL string or None if queue is empty
    """
    try:
        from core.pipeline.frontier import create_frontier
        frontier = create_frontier(scraper_name)
        entry = frontier.get_next()
        if entry:
            return entry.url
    except ImportError:
        logger.debug("Frontier queue not available")
    except Exception as e:
        logger.debug(f"Failed to get URL from frontier: {e}")
    return None
