#!/usr/bin/env python3
"""
Proxy Pool Manager - High Value Feature

Built-in proxy pool for stability and scale. Rotates through multiple
proxy sources: datacenter, residential, and mobile proxies.

Features:
- Health checking and automatic failover
- Geo-targeting by country
- Session persistence (sticky sessions)
- Usage tracking and rate limiting per proxy
- Automatic proxy refresh from external APIs
"""

import json
import logging
import random
import threading
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any
from urllib.parse import urlparse
import requests
import sqlite3

logger = logging.getLogger(__name__)


class ProxyType(Enum):
    DATACENTER = "datacenter"
    RESIDENTIAL = "residential"
    MOBILE = "mobile"
    ISP = "isp"


class ProxyStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    BANNED = "banned"
    COOLDOWN = "cooldown"


@dataclass
class Proxy:
    """Represents a single proxy endpoint"""
    id: str
    host: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None
    protocol: str = "http"
    proxy_type: ProxyType = ProxyType.DATACENTER
    country_code: str = "US"
    city: Optional[str] = None
    provider: str = "unknown"
    status: ProxyStatus = ProxyStatus.HEALTHY
    last_check: Optional[datetime] = None
    success_count: int = 0
    fail_count: int = 0
    avg_response_time_ms: float = 0.0
    max_requests_per_minute: int = 60
    current_minute_requests: int = 0
    current_minute_start: Optional[datetime] = None
    session_id: Optional[str] = None
    session_expires: Optional[datetime] = None
    added_at: datetime = None
    last_used: Optional[datetime] = None
    
    def __post_init__(self):
        if self.added_at is None:
            self.added_at = datetime.utcnow()
        if self.id is None:
            self.id = f"{self.host}:{self.port}"
    
    @property
    def url(self) -> str:
        if self.username and self.password:
            return f"{self.protocol}://{self.username}:{self.password}@{self.host}:{self.port}"
        return f"{self.protocol}://{self.host}:{self.port}"
    
    @property
    def dict_format(self) -> Dict[str, str]:
        return {"http": self.url, "https": self.url}
    
    @property
    def success_rate(self) -> float:
        total = self.success_count + self.fail_count
        if total == 0:
            return 1.0
        return self.success_count / total
    
    def record_success(self, response_time_ms: float):
        self.success_count += 1
        self.last_used = datetime.utcnow()
        if self.avg_response_time_ms == 0:
            self.avg_response_time_ms = response_time_ms
        else:
            self.avg_response_time_ms = (self.avg_response_time_ms * 0.9) + (response_time_ms * 0.1)
        self._update_status()
    
    def record_failure(self, error_type: str = "unknown"):
        self.fail_count += 1
        self.last_used = datetime.utcnow()
        if self.success_rate < 0.5 and self.fail_count > 5:
            self.status = ProxyStatus.UNHEALTHY
        elif error_type == "banned":
            self.status = ProxyStatus.BANNED
    
    def _update_status(self):
        if self.success_rate > 0.9:
            self.status = ProxyStatus.HEALTHY
        elif self.success_rate > 0.7:
            self.status = ProxyStatus.DEGRADED
        if self.status == ProxyStatus.COOLDOWN:
            if self.last_used and datetime.utcnow() - self.last_used > timedelta(minutes=10):
                self.status = ProxyStatus.HEALTHY
    
    def check_rate_limit(self) -> bool:
        now = datetime.utcnow()
        if self.current_minute_start is None or (now - self.current_minute_start).seconds >= 60:
            self.current_minute_requests = 0
            self.current_minute_start = now
        return self.current_minute_requests < self.max_requests_per_minute
    
    def increment_usage(self):
        self.current_minute_requests += 1
    
    def to_dict(self) -> Dict:
        data = asdict(self)
        data['proxy_type'] = self.proxy_type.value
        data['status'] = self.status.value
        return data
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Proxy':
        data = data.copy()
        data['proxy_type'] = ProxyType(data.get('proxy_type', 'datacenter'))
        data['status'] = ProxyStatus(data.get('status', 'healthy'))
        data['added_at'] = datetime.fromisoformat(data['added_at']) if data.get('added_at') else datetime.utcnow()
        if data.get('last_check'):
            data['last_check'] = datetime.fromisoformat(data['last_check'])
        if data.get('last_used'):
            data['last_used'] = datetime.fromisoformat(data['last_used'])
        return cls(**data)


class ProxyPool:
    """
    Central proxy pool manager with health checking and geo-routing.
    
    Usage:
        pool = ProxyPool()
        proxy = pool.get_proxy(country_code="MY", proxy_type=ProxyType.RESIDENTIAL)
        response = requests.get(url, proxies=proxy.dict_format)
        pool.report_success(proxy.id, response_time_ms=500)
    """
    
    DEFAULT_HEALTH_CHECK_URL = "http://httpbin.org/ip"
    DEFAULT_HEALTH_CHECK_INTERVAL = 300
    
    def __init__(self, db_path: Optional[str] = None, config: Optional[Dict] = None):
        self.config = config or {}
        self.db_path = db_path or ".cache/proxy_pool.db"
        self._ensure_db()
        self._proxies: Dict[str, Proxy] = {}
        self._lock = threading.RLock()
        self._load_proxies()
        self._health_check_thread: Optional[threading.Thread] = None
        self._stop_health_check = threading.Event()
        self._start_health_checker()
        logger.info(f"Proxy pool initialized with {len(self._proxies)} proxies")
    
    def _ensure_db(self):
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS proxies (
                    id TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS proxy_usage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    proxy_id TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    success BOOLEAN,
                    response_time_ms REAL,
                    target_url TEXT,
                    error_type TEXT
                )
            """)
            conn.commit()
    
    def _load_proxies(self):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("SELECT data FROM proxies")
                for row in cursor.fetchall():
                    try:
                        proxy = Proxy.from_dict(json.loads(row[0]))
                        self._proxies[proxy.id] = proxy
                    except Exception as e:
                        logger.warning(f"Failed to load proxy: {e}")
        except Exception as e:
            logger.error(f"Failed to load proxies from DB: {e}")
    
    def _save_proxy(self, proxy: Proxy):
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO proxies (id, data, updated_at) VALUES (?, ?, ?)",
                    (proxy.id, json.dumps(proxy.to_dict()), datetime.utcnow().isoformat())
                )
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to save proxy: {e}")
    
    def add_proxy(self, proxy: Proxy) -> str:
        with self._lock:
            self._proxies[proxy.id] = proxy
            self._save_proxy(proxy)
        logger.info(f"Added proxy: {proxy.id} ({proxy.country_code})")
        return proxy.id
    
    def add_proxies_from_list(self, proxy_list: List[str], proxy_type: ProxyType = ProxyType.DATACENTER,
                              country_code: str = "US", provider: str = "custom") -> int:
        added = 0
        for proxy_url in proxy_list:
            try:
                parsed = urlparse(proxy_url)
                if parsed.scheme:
                    protocol = parsed.scheme
                    host = parsed.hostname
                    port = parsed.port
                    username = parsed.username
                    password = parsed.password
                else:
                    parts = proxy_url.split(":")
                    host = parts[0]
                    port = int(parts[1]) if len(parts) > 1 else 8080
                    protocol = "http"
                    username = None
                    password = None
                
                if not host or not port:
                    continue
                
                proxy = Proxy(
                    id=f"{host}:{port}",
                    host=host,
                    port=port,
                    username=username,
                    password=password,
                    protocol=protocol,
                    proxy_type=proxy_type,
                    country_code=country_code,
                    provider=provider
                )
                self.add_proxy(proxy)
                added += 1
            except Exception as e:
                logger.warning(f"Failed to parse proxy {proxy_url}: {e}")
        
        logger.info(f"Added {added} proxies from list")
        return added
    
    def get_proxy(self, country_code: Optional[str] = None,
                  proxy_type: Optional[ProxyType] = None,
                  city: Optional[str] = None,
                  session_id: Optional[str] = None,
                  exclude_ids: Optional[Set[str]] = None) -> Optional[Proxy]:
        with self._lock:
            candidates = []
            
            for proxy in self._proxies.values():
                if exclude_ids and proxy.id in exclude_ids:
                    continue
                if proxy.status in (ProxyStatus.UNHEALTHY, ProxyStatus.BANNED):
                    continue
                if proxy.status == ProxyStatus.COOLDOWN:
                    continue
                if country_code and proxy.country_code != country_code.upper():
                    continue
                if proxy_type and proxy.proxy_type != proxy_type:
                    continue
                if city and proxy.city != city:
                    continue
                if not proxy.check_rate_limit():
                    continue
                candidates.append(proxy)
            
            if not candidates:
                logger.warning(f"No available proxies for criteria: country={country_code}, type={proxy_type}")
                return None
            
            def score_proxy(p: Proxy) -> float:
                score = p.success_rate * 100
                if p.status == ProxyStatus.HEALTHY:
                    score += 20
                if p.avg_response_time_ms > 0:
                    score -= min(p.avg_response_time_ms / 100, 50)
                if p.last_used:
                    minutes_since_use = (datetime.utcnow() - p.last_used).total_seconds() / 60
                    score += min(minutes_since_use, 30)
                return score
            
            candidates.sort(key=score_proxy, reverse=True)
            top_candidates = candidates[:min(3, len(candidates))]
            selected = random.choice(top_candidates)
            selected.increment_usage()
            return selected
    
    def get_proxy_for_target(self, target_url: str, **kwargs) -> Optional[Proxy]:
        domain = urlparse(target_url).netloc.lower()
        domain_country_map = {
            "pharmacy.gov.my": "MY",
            "nppaindia.nic.in": "IN",
            "alfabeta.net": "AR",
            "farmcom.info": "RU",
            "ramq.gouv.qc.ca": "CA",
            "health.gov.on.ca": "CA",
            "medicijnkosten.nl": "NL",
            "rceth.by": "BY",
            "grls.rosminzdrav.ru": "RU",
            "info.nhi.gov.tw": "TW",
            "malmed.gov.mk": "MK",
            "mercadopublico.cl": "CL",
        }
        for domain_pattern, country in domain_country_map.items():
            if domain_pattern in domain:
                kwargs["country_code"] = kwargs.get("country_code") or country
                break
        return self.get_proxy(**kwargs)
    
    def report_success(self, proxy_id: str, response_time_ms: float):
        with self._lock:
            proxy = self._proxies.get(proxy_id)
            if proxy:
                proxy.record_success(response_time_ms)
                self._save_proxy(proxy)
    
    def report_failure(self, proxy_id: str, error_type: str = "unknown"):
        with self._lock:
            proxy = self._proxies.get(proxy_id)
            if proxy:
                proxy.record_failure(error_type)
                self._save_proxy(proxy)
                if error_type == "banned":
                    proxy.status = ProxyStatus.COOLDOWN
    
    def health_check(self, proxy_id: Optional[str] = None) -> Dict[str, Any]:
        results = {"checked": 0, "healthy": 0, "unhealthy": 0, "details": []}
        proxies_to_check = [self._proxies[proxy_id]] if proxy_id else list(self._proxies.values())
        
        for proxy in proxies_to_check:
            result = self._check_single_proxy(proxy)
            results["checked"] += 1
            if result["healthy"]:
                results["healthy"] += 1
            else:
                results["unhealthy"] += 1
            results["details"].append(result)
        return results
    
    def _check_single_proxy(self, proxy: Proxy) -> Dict:
        start_time = time.time()
        try:
            response = requests.get(
                self.DEFAULT_HEALTH_CHECK_URL,
                proxies=proxy.dict_format,
                timeout=10,
                headers={"User-Agent": "Mozilla/5.0"}
            )
            response_time_ms = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                proxy.last_check = datetime.utcnow()
                if response_time_ms < 5000:
                    if proxy.status == ProxyStatus.UNHEALTHY:
                        proxy.status = ProxyStatus.HEALTHY
                else:
                    proxy.status = ProxyStatus.DEGRADED
                self._save_proxy(proxy)
                return {"proxy_id": proxy.id, "healthy": True, "response_time_ms": response_time_ms, "status": proxy.status.value}
            else:
                proxy.record_failure("http_error")
                return {"proxy_id": proxy.id, "healthy": False, "error": f"HTTP {response.status_code}", "status": proxy.status.value}
        except Exception as e:
            proxy.record_failure("connection_error")
            return {"proxy_id": proxy.id, "healthy": False, "error": str(e), "status": proxy.status.value}
    
    def _start_health_checker(self):
        def health_check_loop():
            while not self._stop_health_check.is_set():
                try:
                    time.sleep(self.DEFAULT_HEALTH_CHECK_INTERVAL)
                    if not self._stop_health_check.is_set():
                        logger.info("Running proxy health checks...")
                        self.health_check()
                except Exception as e:
                    logger.error(f"Health check error: {e}")
        
        self._health_check_thread = threading.Thread(target=health_check_loop, daemon=True)
        self._health_check_thread.start()
        logger.info("Started proxy health checker")
    
    def stop(self):
        self._stop_health_check.set()
        if self._health_check_thread:
            self._health_check_thread.join(timeout=5)
        logger.info("Proxy pool stopped")
    
    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            total = len(self._proxies)
            healthy = sum(1 for p in self._proxies.values() if p.status == ProxyStatus.HEALTHY)
            degraded = sum(1 for p in self._proxies.values() if p.status == ProxyStatus.DEGRADED)
            unhealthy = sum(1 for p in self._proxies.values() if p.status == ProxyStatus.UNHEALTHY)
            banned = sum(1 for p in self._proxies.values() if p.status == ProxyStatus.BANNED)
            
            by_country = {}
            by_type = {}
            for proxy in self._proxies.values():
                by_country[proxy.country_code] = by_country.get(proxy.country_code, 0) + 1
                by_type[proxy.proxy_type.value] = by_type.get(proxy.proxy_type.value, 0) + 1
            
            return {
                "total": total,
                "healthy": healthy,
                "degraded": degraded,
                "unhealthy": unhealthy,
                "banned": banned,
                "by_country": by_country,
                "by_type": by_type,
                "health_rate": healthy / total if total > 0 else 0
            }


# Convenience functions for one-off usage
_default_pool: Optional[ProxyPool] = None

def get_proxy_pool() -> ProxyPool:
    global _default_pool
    if _default_pool is None:
        _default_pool = ProxyPool()
    return _default_pool


def get_proxy_for_scraper(scraper_name: str, target_url: Optional[str] = None) -> Optional[Proxy]:
    """Get appropriate proxy for a scraper based on country"""
    pool = get_proxy_pool()
    
    scraper_countries = {
        "Malaysia": "MY",
        "India": "IN",
        "Argentina": "AR",
        "Russia": "RU",
        "CanadaQuebec": "CA",
        "CanadaOntario": "CA",
        "Netherlands": "NL",
        "Belarus": "BY",
        "Taiwan": "TW",
        "NorthMacedonia": "MK",
        "Tender_Chile": "CL",
    }
    
    country_code = scraper_countries.get(scraper_name)
    
    if target_url:
        return pool.get_proxy_for_target(target_url, country_code=country_code)
    return pool.get_proxy(country_code=country_code)
