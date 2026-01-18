#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Health Monitor Module

Website and scraper health monitoring.
Runs independently - does NOT touch scraping logic.

Usage:
    from core.health_monitor import check_website_health, check_all_scrapers
    
    # Check a single website
    result = check_website_health("https://example.com")
    
    # Check all scraper websites
    results = check_all_scrapers()
    
    # Monitor with alerts
    monitor = HealthMonitor()
    monitor.start_monitoring(interval=300)  # Check every 5 minutes
"""

import logging
import time
import threading
from pathlib import Path
from typing import Dict, Any, List, Optional, Callable, Union
from datetime import datetime, timedelta
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Try to import requests
try:
    import requests
    from requests.exceptions import RequestException, Timeout, ConnectionError as ReqConnectionError
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    requests = None

# Try to import plyer for notifications
try:
    from plyer import notification
    PLYER_AVAILABLE = True
except ImportError:
    PLYER_AVAILABLE = False
    notification = None


class HealthCheck:
    """
    Result of a health check.
    """
    
    def __init__(
        self,
        url: str,
        status: str,
        response_time: float = None,
        status_code: int = None,
        error: str = None,
    ):
        self.url = url
        self.status = status  # "up", "down", "slow", "error"
        self.response_time = response_time
        self.status_code = status_code
        self.error = error
        self.checked_at = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "status": self.status,
            "response_time_ms": round(self.response_time * 1000, 2) if self.response_time else None,
            "status_code": self.status_code,
            "error": self.error,
            "checked_at": self.checked_at.isoformat(),
        }
    
    @property
    def is_healthy(self) -> bool:
        return self.status in ("up", "slow")


class HealthMonitor:
    """
    Health monitoring for scraper target websites.
    """
    
    # Default timeout for health checks
    DEFAULT_TIMEOUT = 30
    
    # Threshold for "slow" response (seconds)
    SLOW_THRESHOLD = 10.0
    
    # Scraper website configurations
    SCRAPER_URLS = {
        "Malaysia": {
            "name": "MyPriMe Malaysia",
            "url": "https://www.pharmacy.gov.my/v2/en/apps/myprime",
            "check_text": None,  # Optional text to verify on page
        },
        "Argentina": {
            "name": "Alfabeta Argentina",
            "url": "https://www.alfabeta.net/",
            "check_text": None,
        },
        "India": {
            "name": "NPPA India",
            "url": "https://www.nppaindia.nic.in/",
            "check_text": None,
        },
        "CanadaQuebec": {
            "name": "RAMQ Quebec",
            "url": "https://www.ramq.gouv.qc.ca/",
            "check_text": None,
        },
        "CanadaOntario": {
            "name": "Ontario Drug Benefit",
            "url": "https://www.health.gov.on.ca/",
            "check_text": None,
        },
        "Netherlands": {
            "name": "Netherlands Medicines",
            "url": "https://www.medicijnkosten.nl/",
            "check_text": None,
        },
        "Belarus": {
            "name": "Belarus RCETH",
            "url": "https://rceth.by/",
            "check_text": None,
        },
        "Russia": {
            "name": "Russia Medicines Registry",
            "url": "https://grls.rosminzdrav.ru/",
            "check_text": None,
        },
        "Taiwan": {
            "name": "Taiwan NHI Drug Database",
            "url": "https://info.nhi.gov.tw/INAE3000/INAE3000S01",
            "check_text": None,
        },
        "NorthMacedonia": {
            "name": "North Macedonia MALMED",
            "url": "https://malmed.gov.mk/",
            "check_text": None,
        },
        "Tender_Chile": {
            "name": "Chile Mercado Publico",
            "url": "https://www.mercadopublico.cl/",
            "check_text": None,
        },
    }
    
    def __init__(
        self,
        timeout: float = DEFAULT_TIMEOUT,
        slow_threshold: float = SLOW_THRESHOLD,
        on_status_change: Optional[Callable[[str, HealthCheck], None]] = None,
    ):
        """
        Initialize health monitor.
        
        Args:
            timeout: Request timeout in seconds
            slow_threshold: Response time threshold for "slow" status
            on_status_change: Callback when status changes
        """
        self.timeout = timeout
        self.slow_threshold = slow_threshold
        self.on_status_change = on_status_change
        
        self._history: Dict[str, List[HealthCheck]] = {}
        self._last_status: Dict[str, str] = {}
        self._monitoring = False
        self._monitor_thread: Optional[threading.Thread] = None
    
    def check_url(
        self,
        url: str,
        check_text: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> HealthCheck:
        """
        Check health of a URL.
        
        Args:
            url: URL to check
            check_text: Optional text to verify on page
            timeout: Request timeout (uses default if None)
        
        Returns:
            HealthCheck result
        """
        if not REQUESTS_AVAILABLE:
            return HealthCheck(
                url=url,
                status="error",
                error="requests library not available",
            )
        
        timeout = timeout or self.timeout
        start_time = time.time()
        
        try:
            response = requests.get(
                url,
                timeout=timeout,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                },
                allow_redirects=True,
            )
            
            response_time = time.time() - start_time
            
            # Check status code
            if response.status_code >= 400:
                return HealthCheck(
                    url=url,
                    status="error",
                    response_time=response_time,
                    status_code=response.status_code,
                    error=f"HTTP {response.status_code}",
                )
            
            # Check for required text
            if check_text and check_text not in response.text:
                return HealthCheck(
                    url=url,
                    status="error",
                    response_time=response_time,
                    status_code=response.status_code,
                    error=f"Expected text not found: {check_text[:50]}",
                )
            
            # Determine status based on response time
            if response_time > self.slow_threshold:
                status = "slow"
            else:
                status = "up"
            
            return HealthCheck(
                url=url,
                status=status,
                response_time=response_time,
                status_code=response.status_code,
            )
            
        except Timeout:
            return HealthCheck(
                url=url,
                status="down",
                response_time=time.time() - start_time,
                error="Request timed out",
            )
        except ReqConnectionError as e:
            return HealthCheck(
                url=url,
                status="down",
                response_time=time.time() - start_time,
                error=f"Connection error: {str(e)[:100]}",
            )
        except RequestException as e:
            return HealthCheck(
                url=url,
                status="error",
                response_time=time.time() - start_time,
                error=str(e)[:200],
            )
        except Exception as e:
            return HealthCheck(
                url=url,
                status="error",
                error=str(e)[:200],
            )
    
    def check_scraper(self, scraper_name: str) -> HealthCheck:
        """
        Check health of a scraper's target website.
        
        Args:
            scraper_name: Name of the scraper
        
        Returns:
            HealthCheck result
        """
        config = self.SCRAPER_URLS.get(scraper_name)
        if not config:
            return HealthCheck(
                url="",
                status="error",
                error=f"Unknown scraper: {scraper_name}",
            )
        
        result = self.check_url(config["url"], config.get("check_text"))
        
        # Track history
        if scraper_name not in self._history:
            self._history[scraper_name] = []
        self._history[scraper_name].append(result)
        
        # Keep only last 100 checks
        if len(self._history[scraper_name]) > 100:
            self._history[scraper_name] = self._history[scraper_name][-100:]
        
        # Check for status change
        last_status = self._last_status.get(scraper_name)
        if last_status and last_status != result.status:
            if self.on_status_change:
                self.on_status_change(scraper_name, result)
            self._notify_status_change(scraper_name, last_status, result)
        
        self._last_status[scraper_name] = result.status
        
        return result
    
    def check_all_scrapers(self) -> Dict[str, HealthCheck]:
        """
        Check health of all configured scraper websites.
        
        Returns:
            Dict mapping scraper name to HealthCheck
        """
        results = {}
        for scraper_name in self.SCRAPER_URLS:
            results[scraper_name] = self.check_scraper(scraper_name)
        return results
    
    def _notify_status_change(self, scraper_name: str, old_status: str, check: HealthCheck):
        """Send notification on status change."""
        if not PLYER_AVAILABLE:
            return
        
        if check.status == "down":
            title = f"⚠️ {scraper_name} DOWN"
            message = f"Website is not responding\n{check.error or ''}"
        elif check.status == "up" and old_status in ("down", "error"):
            title = f"✅ {scraper_name} RECOVERED"
            message = f"Website is back online\nResponse: {check.response_time*1000:.0f}ms"
        elif check.status == "slow":
            title = f"🐢 {scraper_name} SLOW"
            message = f"Response time: {check.response_time*1000:.0f}ms"
        else:
            return
        
        try:
            notification.notify(
                title=title,
                message=message,
                timeout=10,
            )
        except Exception as e:
            logger.warning(f"Failed to send notification: {e}")
    
    def get_history(self, scraper_name: str, limit: int = 10) -> List[Dict]:
        """Get health check history for a scraper."""
        history = self._history.get(scraper_name, [])
        return [h.to_dict() for h in history[-limit:]]
    
    def get_uptime(self, scraper_name: str, hours: int = 24) -> float:
        """
        Calculate uptime percentage for a scraper.
        
        Args:
            scraper_name: Scraper name
            hours: Time period in hours
        
        Returns:
            Uptime percentage (0-100)
        """
        history = self._history.get(scraper_name, [])
        if not history:
            return 100.0
        
        cutoff = datetime.now() - timedelta(hours=hours)
        recent = [h for h in history if h.checked_at >= cutoff]
        
        if not recent:
            return 100.0
        
        healthy_count = sum(1 for h in recent if h.is_healthy)
        return (healthy_count / len(recent)) * 100
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all scrapers' health status."""
        summary = {
            "checked_at": datetime.now().isoformat(),
            "scrapers": {},
            "total_up": 0,
            "total_down": 0,
            "total_slow": 0,
        }
        
        for scraper_name in self.SCRAPER_URLS:
            last_check = self._history.get(scraper_name, [None])[-1]
            if last_check:
                summary["scrapers"][scraper_name] = {
                    "status": last_check.status,
                    "last_checked": last_check.checked_at.isoformat(),
                    "response_time_ms": round(last_check.response_time * 1000, 2) if last_check.response_time else None,
                    "uptime_24h": round(self.get_uptime(scraper_name, 24), 1),
                }
                
                if last_check.status == "up":
                    summary["total_up"] += 1
                elif last_check.status == "down":
                    summary["total_down"] += 1
                elif last_check.status == "slow":
                    summary["total_slow"] += 1
            else:
                summary["scrapers"][scraper_name] = {"status": "unknown"}
        
        return summary
    
    def start_monitoring(self, interval: int = 300):
        """
        Start background monitoring.
        
        Args:
            interval: Check interval in seconds (default: 5 minutes)
        """
        if self._monitoring:
            logger.warning("Monitoring already running")
            return
        
        self._monitoring = True
        
        def monitor_loop():
            while self._monitoring:
                try:
                    logger.info("Running health checks...")
                    self.check_all_scrapers()
                    summary = self.get_summary()
                    logger.info(
                        f"Health check complete: {summary['total_up']} up, "
                        f"{summary['total_down']} down, {summary['total_slow']} slow"
                    )
                except Exception as e:
                    logger.error(f"Health check error: {e}")
                
                # Wait for next check
                for _ in range(interval):
                    if not self._monitoring:
                        break
                    time.sleep(1)
        
        self._monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        self._monitor_thread.start()
        logger.info(f"Health monitoring started (interval: {interval}s)")
    
    def stop_monitoring(self):
        """Stop background monitoring."""
        self._monitoring = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
        logger.info("Health monitoring stopped")


# Convenience functions
def check_website_health(url: str, timeout: float = 30) -> Dict[str, Any]:
    """
    Check health of a website.
    
    Args:
        url: URL to check
        timeout: Request timeout
    
    Returns:
        Health check result as dict
    """
    monitor = HealthMonitor(timeout=timeout)
    result = monitor.check_url(url)
    return result.to_dict()


def check_scraper_health(scraper_name: str) -> Dict[str, Any]:
    """
    Check health of a scraper's target website.
    
    Args:
        scraper_name: Name of the scraper
    
    Returns:
        Health check result as dict
    """
    monitor = HealthMonitor()
    result = monitor.check_scraper(scraper_name)
    return result.to_dict()


def check_all_scrapers() -> Dict[str, Dict[str, Any]]:
    """
    Check health of all configured scraper websites.
    
    Returns:
        Dict mapping scraper name to health check result
    """
    monitor = HealthMonitor()
    results = monitor.check_all_scrapers()
    return {name: result.to_dict() for name, result in results.items()}


def get_health_summary() -> Dict[str, Any]:
    """
    Get health summary for all scrapers.
    
    Note: This performs fresh checks. For cached results, use HealthMonitor.
    
    Returns:
        Summary dict with status counts and per-scraper info
    """
    monitor = HealthMonitor()
    monitor.check_all_scrapers()
    return monitor.get_summary()


# CLI interface
if __name__ == "__main__":
    import sys
    import json
    
    print(f"Requests available: {REQUESTS_AVAILABLE}")
    print(f"Plyer (notifications) available: {PLYER_AVAILABLE}")
    print()
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--all":
            print("Checking all scrapers...")
            results = check_all_scrapers()
            
            print("\nHealth Check Results:")
            print("-" * 60)
            for name, result in results.items():
                status_icon = {
                    "up": "✓",
                    "slow": "🐢",
                    "down": "✗",
                    "error": "⚠",
                }.get(result["status"], "?")
                
                time_str = f"{result['response_time_ms']:.0f}ms" if result['response_time_ms'] else "N/A"
                print(f"{status_icon} {name:20} {result['status']:8} {time_str}")
            
            print("-" * 60)
            
        elif sys.argv[1] == "--monitor":
            interval = int(sys.argv[2]) if len(sys.argv) > 2 else 60
            print(f"Starting health monitor (interval: {interval}s)...")
            print("Press Ctrl+C to stop\n")
            
            monitor = HealthMonitor()
            monitor.start_monitoring(interval=interval)
            
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                monitor.stop_monitoring()
                print("\nMonitoring stopped")
        
        else:
            # Check specific scraper or URL
            target = sys.argv[1]
            if target in HealthMonitor.SCRAPER_URLS:
                result = check_scraper_health(target)
            else:
                result = check_website_health(target)
            
            print(json.dumps(result, indent=2))
    
    else:
        print("Usage:")
        print("  python health_monitor.py --all              # Check all scrapers")
        print("  python health_monitor.py --monitor [secs]   # Start monitoring")
        print("  python health_monitor.py <scraper_name>     # Check specific scraper")
        print("  python health_monitor.py <url>              # Check specific URL")
        print()
        print("Available scrapers:", ", ".join(HealthMonitor.SCRAPER_URLS.keys()))
