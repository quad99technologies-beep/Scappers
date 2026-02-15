#!/usr/bin/env python3
"""
Geo Router - One-Click Geo Routing Feature

Automatically maps scrapers to appropriate VPN and proxy configurations
based on target country. Eliminates manual VPN switching.

Usage:
    router = GeoRouter()
    
    # Get complete routing config for scraper
    config = router.get_route("Malaysia")
    
    # Apply to scraper session
    router.apply_route("Malaysia", driver)
"""

import logging
import subprocess
import time
from dataclasses import dataclass
from typing import Dict, Optional, Any, Callable
from enum import Enum

from core.network.proxy_pool import ProxyPool, ProxyType, get_proxy_for_scraper

logger = logging.getLogger(__name__)


class VPNStatus(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    ERROR = "error"


@dataclass
class RouteConfig:
    """Complete routing configuration for a scraper"""
    scraper_name: str
    country_code: str
    vpn_profile: Optional[str]
    vpn_provider: str
    proxy_type: ProxyType
    proxy_country: str
    timezone: str
    locale: str
    preferred_dns: list
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "scraper_name": self.scraper_name,
            "country_code": self.country_code,
            "vpn_profile": self.vpn_profile,
            "vpn_provider": self.vpn_provider,
            "proxy_type": self.proxy_type.value,
            "proxy_country": self.proxy_country,
            "timezone": self.timezone,
            "locale": self.locale,
            "preferred_dns": self.preferred_dns,
        }


class VPNManager:
    """Manages VPN connections for different providers"""
    
    def __init__(self):
        self.active_vpn: Optional[str] = None
        self.status: VPNStatus = VPNStatus.DISCONNECTED
        self._providers: Dict[str, Callable] = {
            "surfshark": self._connect_surfshark,
            "nordvpn": self._connect_nordvpn,
            "expressvpn": self._connect_expressvpn,
            "openvpn": self._connect_openvpn,
            "wireguard": self._connect_wireguard,
        }
    
    def connect(self, profile: str, provider: str = "surfshark") -> bool:
        """Connect to VPN using specified profile"""
        if self.active_vpn == profile and self.status == VPNStatus.CONNECTED:
            logger.info(f"Already connected to {profile}")
            return True
        
        # Disconnect current if any
        if self.status == VPNStatus.CONNECTED:
            self.disconnect()
        
        self.status = VPNStatus.CONNECTING
        
        connector = self._providers.get(provider)
        if not connector:
            logger.error(f"Unknown VPN provider: {provider}")
            self.status = VPNStatus.ERROR
            return False
        
        try:
            success = connector(profile)
            if success:
                self.active_vpn = profile
                self.status = VPNStatus.CONNECTED
                logger.info(f"Connected to VPN: {profile}")
                time.sleep(3)  # Wait for connection to stabilize
                return True
            else:
                self.status = VPNStatus.ERROR
                return False
        except Exception as e:
            logger.error(f"VPN connection failed: {e}")
            self.status = VPNStatus.ERROR
            return False
    
    def disconnect(self) -> bool:
        """Disconnect active VPN"""
        try:
            # Try multiple VPN clients
            subprocess.run(["surfshark", "disconnect"], capture_output=True, timeout=30)
            subprocess.run(["nordvpn", "disconnect"], capture_output=True, timeout=30)
            subprocess.run(["expressvpn", "disconnect"], capture_output=True, timeout=30)
            
            self.active_vpn = None
            self.status = VPNStatus.DISCONNECTED
            logger.info("VPN disconnected")
            return True
        except Exception as e:
            logger.error(f"VPN disconnect failed: {e}")
            return False
    
    def get_status(self) -> Dict[str, Any]:
        """Get current VPN status"""
        return {
            "status": self.status.value,
            "active_profile": self.active_vpn,
        }
    
    def _connect_surfshark(self, profile: str) -> bool:
        """Connect using Surfshark CLI"""
        try:
            result = subprocess.run(
                ["surfshark", "connect", profile],
                capture_output=True,
                text=True,
                timeout=60
            )
            return result.returncode == 0 or "connected" in result.stdout.lower()
        except Exception as e:
            logger.error(f"Surfshark connection error: {e}")
            return False
    
    def _connect_nordvpn(self, profile: str) -> bool:
        """Connect using NordVPN CLI"""
        try:
            result = subprocess.run(
                ["nordvpn", "connect", profile],
                capture_output=True,
                text=True,
                timeout=60
            )
            return result.returncode == 0
        except Exception as e:
            logger.error(f"NordVPN connection error: {e}")
            return False
    
    def _connect_expressvpn(self, profile: str) -> bool:
        """Connect using ExpressVPN CLI"""
        try:
            result = subprocess.run(
                ["expressvpn", "connect", profile],
                capture_output=True,
                text=True,
                timeout=60
            )
            return result.returncode == 0
        except Exception as e:
            logger.error(f"ExpressVPN connection error: {e}")
            return False
    
    def _connect_openvpn(self, profile: str) -> bool:
        """Connect using OpenVPN"""
        try:
            config_path = f"/etc/openvpn/{profile}.ovpn"
            result = subprocess.run(
                ["sudo", "openvpn", "--config", config_path, "--daemon"],
                capture_output=True,
                text=True,
                timeout=30
            )
            return result.returncode == 0
        except Exception as e:
            logger.error(f"OpenVPN connection error: {e}")
            return False
    
    def _connect_wireguard(self, profile: str) -> bool:
        """Connect using WireGuard"""
        try:
            result = subprocess.run(
                ["sudo", "wg-quick", "up", profile],
                capture_output=True,
                text=True,
                timeout=30
            )
            return result.returncode == 0
        except Exception as e:
            logger.error(f"WireGuard connection error: {e}")
            return False


class GeoRouter:
    """
    One-click geo routing for scrapers.
    
    Automatically configures VPN and proxy based on target country.
    """
    
    # Scraper routing configuration
    COUNTRY_ROUTES: Dict[str, RouteConfig] = {
        "Malaysia": RouteConfig(
            scraper_name="Malaysia",
            country_code="MY",
            vpn_profile="singapore",
            vpn_provider="surfshark",
            proxy_type=ProxyType.RESIDENTIAL,
            proxy_country="MY",
            timezone="Asia/Kuala_Lumpur",
            locale="en-MY",
            preferred_dns=["1.1.1.1", "8.8.8.8"]
        ),
        "India": RouteConfig(
            scraper_name="India",
            country_code="IN",
            vpn_profile="india",
            vpn_provider="surfshark",
            proxy_type=ProxyType.ISP,
            proxy_country="IN",
            timezone="Asia/Kolkata",
            locale="en-IN",
            preferred_dns=["1.1.1.1", "8.8.8.8"]
        ),
        "Argentina": RouteConfig(
            scraper_name="Argentina",
            country_code="AR",
            vpn_profile="argentina",
            vpn_provider="surfshark",
            proxy_type=ProxyType.RESIDENTIAL,
            proxy_country="AR",
            timezone="America/Argentina/Buenos_Aires",
            locale="es-AR",
            preferred_dns=["1.1.1.1", "8.8.8.8"]
        ),
        "Russia": RouteConfig(
            scraper_name="Russia",
            country_code="RU",
            vpn_profile="russia",
            vpn_provider="surfshark",
            proxy_type=ProxyType.DATACENTER,
            proxy_country="RU",
            timezone="Europe/Moscow",
            locale="ru-RU",
            preferred_dns=["77.88.8.8", "77.88.8.1"]
        ),
        "CanadaQuebec": RouteConfig(
            scraper_name="CanadaQuebec",
            country_code="CA",
            vpn_profile="canada-montreal",
            vpn_provider="surfshark",
            proxy_type=ProxyType.RESIDENTIAL,
            proxy_country="CA",
            timezone="America/Montreal",
            locale="fr-CA",
            preferred_dns=["1.1.1.1", "8.8.8.8"]
        ),
        "CanadaOntario": RouteConfig(
            scraper_name="CanadaOntario",
            country_code="CA",
            vpn_profile="canada-toronto",
            vpn_provider="surfshark",
            proxy_type=ProxyType.RESIDENTIAL,
            proxy_country="CA",
            timezone="America/Toronto",
            locale="en-CA",
            preferred_dns=["1.1.1.1", "8.8.8.8"]
        ),
        "Netherlands": RouteConfig(
            scraper_name="Netherlands",
            country_code="NL",
            vpn_profile="netherlands",
            vpn_provider="surfshark",
            proxy_type=ProxyType.RESIDENTIAL,
            proxy_country="NL",
            timezone="Europe/Amsterdam",
            locale="nl-NL",
            preferred_dns=["1.1.1.1", "8.8.8.8"]
        ),
        "Belarus": RouteConfig(
            scraper_name="Belarus",
            country_code="BY",
            vpn_profile="belarus",
            vpn_provider="surfshark",
            proxy_type=ProxyType.DATACENTER,
            proxy_country="BY",
            timezone="Europe/Minsk",
            locale="be-BY",
            preferred_dns=["82.209.240.241", "82.209.240.241"]
        ),
        "Taiwan": RouteConfig(
            scraper_name="Taiwan",
            country_code="TW",
            vpn_profile="taiwan",
            vpn_provider="surfshark",
            proxy_type=ProxyType.RESIDENTIAL,
            proxy_country="TW",
            timezone="Asia/Taipei",
            locale="zh-TW",
            preferred_dns=["1.1.1.1", "8.8.8.8"]
        ),
        "NorthMacedonia": RouteConfig(
            scraper_name="NorthMacedonia",
            country_code="MK",
            vpn_profile="macedonia",
            vpn_provider="surfshark",
            proxy_type=ProxyType.RESIDENTIAL,
            proxy_country="MK",
            timezone="Europe/Skopje",
            locale="mk-MK",
            preferred_dns=["1.1.1.1", "8.8.8.8"]
        ),
        "Tender_Chile": RouteConfig(
            scraper_name="Tender_Chile",
            country_code="CL",
            vpn_profile="chile",
            vpn_provider="surfshark",
            proxy_type=ProxyType.RESIDENTIAL,
            proxy_country="CL",
            timezone="America/Santiago",
            locale="es-CL",
            preferred_dns=["1.1.1.1", "8.8.8.8"]
        ),
    }
    
    def __init__(self):
        self.vpn_manager = VPNManager()
        self.proxy_pool = ProxyPool()
        self.active_routes: Dict[str, RouteConfig] = {}
    
    def get_route(self, scraper_name: str) -> Optional[RouteConfig]:
        """Get routing configuration for a scraper"""
        return self.COUNTRY_ROUTES.get(scraper_name)
    
    def apply_route(self, scraper_name: str, driver_or_session=None, 
                    use_vpn: bool = True, use_proxy: bool = True) -> Dict[str, Any]:
        """
        Apply complete routing configuration for a scraper.
        
        Args:
            scraper_name: Name of the scraper
            driver_or_session: Selenium/Playwright driver or requests session
            use_vpn: Whether to connect VPN
            use_proxy: Whether to configure proxy
        
        Returns:
            Dictionary with applied configuration
        """
        route = self.get_route(scraper_name)
        if not route:
            logger.warning(f"No route config found for {scraper_name}")
            return {"success": False, "error": "No route config"}
        
        result = {
            "scraper": scraper_name,
            "route": route.to_dict(),
            "vpn_connected": False,
            "proxy_configured": False,
            "success": True,
        }
        
        # Connect VPN
        if use_vpn and route.vpn_profile:
            vpn_success = self.vpn_manager.connect(
                route.vpn_profile, 
                route.vpn_provider
            )
            result["vpn_connected"] = vpn_success
            if not vpn_success:
                logger.warning(f"VPN connection failed for {scraper_name}")
        
        # Get proxy
        proxy = None
        if use_proxy:
            proxy = self.proxy_pool.get_proxy(
                country_code=route.proxy_country,
                proxy_type=route.proxy_type
            )
            if proxy:
                result["proxy"] = {
                    "id": proxy.id,
                    "country": proxy.country_code,
                    "type": proxy.proxy_type.value,
                }
                result["proxy_configured"] = True
            else:
                logger.warning(f"No proxy available for {scraper_name}")
        
        # Apply to driver/session
        if driver_or_session:
            self._apply_to_driver(driver_or_session, route, proxy)
        
        self.active_routes[scraper_name] = route
        logger.info(f"Applied route for {scraper_name}: VPN={result['vpn_connected']}, Proxy={result['proxy_configured']}")
        
        return result
    
    def _apply_to_driver(self, driver, route: RouteConfig, proxy=None):
        """Apply routing configuration to Selenium/Playwright driver"""
        try:
            # Set timezone and locale via CDP (Chrome DevTools Protocol)
            if hasattr(driver, 'execute_cdp_cmd'):
                # Selenium 4+
                driver.execute_cdp_cmd('Emulation.setTimezoneOverride', {
                    'timezoneId': route.timezone
                })
                driver.execute_cdp_cmd('Emulation.setLocaleOverride', {
                    'locale': route.locale
                })
            
            # Configure proxy if available
            if proxy and hasattr(driver, 'proxy'):
                # Note: Proxy should be set when creating driver
                pass
                
        except Exception as e:
            logger.warning(f"Could not apply all route settings to driver: {e}")
    
    def release_route(self, scraper_name: str):
        """Release routing resources for a scraper"""
        if scraper_name in self.active_routes:
            del self.active_routes[scraper_name]
            logger.info(f"Released route for {scraper_name}")
    
    def get_active_routes(self) -> Dict[str, Dict]:
        """Get all currently active routes"""
        return {
            name: route.to_dict() 
            for name, route in self.active_routes.items()
        }
    
    def health_check(self) -> Dict[str, Any]:
        """Check health of routing system"""
        vpn_status = self.vpn_manager.get_status()
        proxy_stats = self.proxy_pool.get_stats()
        
        return {
            "vpn": vpn_status,
            "proxies": proxy_stats,
            "active_routes": len(self.active_routes),
        }


# Convenience functions
_default_router: Optional[GeoRouter] = None

def get_geo_router() -> GeoRouter:
    global _default_router
    if _default_router is None:
        _default_router = GeoRouter()
    return _default_router


def route_scraper(scraper_name: str, driver=None, use_vpn: bool = True, use_proxy: bool = True) -> Dict:
    """One-click routing for a scraper"""
    router = get_geo_router()
    return router.apply_route(scraper_name, driver, use_vpn, use_proxy)
