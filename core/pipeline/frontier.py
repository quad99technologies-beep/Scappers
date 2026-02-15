#!/usr/bin/env python3
"""
Crawl Frontier Queue - High Value Feature

Lightweight queue for product/detail pages discovered during scraping.
Manages crawl state, deduplication, and prioritization.

Features:
- URL deduplication
- Priority-based crawling
- Persistent storage in Redis
- Politeness delays
- Crawl state management
"""

import json
import logging
import hashlib
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Any, Iterator
from enum import Enum
from urllib.parse import urlparse, urljoin

logger = logging.getLogger(__name__)


class URLPriority(Enum):
    CRITICAL = 0   # Must crawl immediately
    HIGH = 1       # Important pages
    NORMAL = 2     # Standard pages
    LOW = 3        # Background crawl
    OPTIONAL = 4   # Nice to have


class URLStatus(Enum):
    PENDING = "pending"
    QUEUED = "queued"
    CRAWLING = "crawling"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class FrontierURL:
    """Represents a URL in the crawl frontier"""
    url: str
    priority: URLPriority
    status: URLStatus
    discovered_at: datetime
    depth: int
    referer: Optional[str] = None
    metadata: Dict[str, Any] = None
    scheduled_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    retry_count: int = 0
    max_retries: int = 3
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
    
    @property
    def url_hash(self) -> str:
        """Get unique hash for URL"""
        return hashlib.sha256(self.url.encode()).hexdigest()
    
    @property
    def domain(self) -> str:
        """Get domain from URL"""
        return urlparse(self.url).netloc
    
    def to_dict(self) -> Dict:
        data = asdict(self)
        data['priority'] = self.priority.value
        data['status'] = self.status.value
        data['discovered_at'] = self.discovered_at.isoformat()
        if self.scheduled_at:
            data['scheduled_at'] = self.scheduled_at.isoformat()
        if self.started_at:
            data['started_at'] = self.started_at.isoformat()
        if self.completed_at:
            data['completed_at'] = self.completed_at.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'FrontierURL':
        data = data.copy()
        data['priority'] = URLPriority(data.get('priority', 2))
        data['status'] = URLStatus(data.get('status', 'pending'))
        data['discovered_at'] = datetime.fromisoformat(data['discovered_at'])
        if data.get('scheduled_at'):
            data['scheduled_at'] = datetime.fromisoformat(data['scheduled_at'])
        if data.get('started_at'):
            data['started_at'] = datetime.fromisoformat(data['started_at'])
        if data.get('completed_at'):
            data['completed_at'] = datetime.fromisoformat(data['completed_at'])
        return cls(**data)


class CrawlFrontier:
    """
    Lightweight crawl frontier for managing discovered URLs.
    
    Usage:
        frontier = CrawlFrontier("Malaysia", redis_client)
        
        # Add discovered URLs
        frontier.add_url("https://example.com/product/123", priority=URLPriority.HIGH)
        
        # Get next batch to crawl
        urls = frontier.get_next_batch(size=10)
        
        # Mark completion
        frontier.mark_completed(url, success=True)
    """
    
    def __init__(self, scraper_name: str, redis_client, 
                 politeness_delay: float = 1.0,
                 max_depth: int = 3):
        self.scraper_name = scraper_name
        self.redis = redis_client
        self.politeness_delay = politeness_delay
        self.max_depth = max_depth
        
        # Redis key prefixes
        self.queue_key = f"frontier:{scraper_name}:queue"
        self.seen_key = f"frontier:{scraper_name}:seen"
        self.active_key = f"frontier:{scraper_name}:active"
        self.completed_key = f"frontier:{scraper_name}:completed"
        self.failed_key = f"frontier:{scraper_name}:failed"
        self.domain_delay_key = f"frontier:{scraper_name}:domain_delays"
        
        logger.info(f"Initialized crawl frontier for {scraper_name}")
    
    def add_url(self, url: str, priority: URLPriority = URLPriority.NORMAL,
                depth: int = 0, referer: Optional[str] = None,
                metadata: Optional[Dict] = None) -> bool:
        """
        Add URL to frontier.
        
        Returns:
            True if added, False if already seen
        """
        if depth > self.max_depth:
            logger.debug(f"Skipping {url}: exceeds max depth {self.max_depth}")
            return False
        
        url_hash = hashlib.sha256(url.encode()).hexdigest()
        
        # Check if already seen
        if self.redis.sismember(self.seen_key, url_hash):
            logger.debug(f"Skipping {url}: already seen")
            return False
        
        # Create frontier entry
        entry = FrontierURL(
            url=url,
            priority=priority,
            status=URLStatus.QUEUED,
            discovered_at=datetime.utcnow(),
            depth=depth,
            referer=referer,
            metadata=metadata or {}
        )
        
        # Add to priority queue (sorted set)
        # Score = priority value + timestamp (for FIFO within same priority)
        score = priority.value * 1000000000 + int(time.time())
        self.redis.zadd(self.queue_key, {json.dumps(entry.to_dict()): score})
        
        # Mark as seen
        self.redis.sadd(self.seen_key, url_hash)
        
        logger.debug(f"Added to frontier: {url} (priority={priority.name}, depth={depth})")
        return True
    
    def add_urls(self, urls: List[str], priority: URLPriority = URLPriority.NORMAL,
                 depth: int = 0, referer: Optional[str] = None) -> int:
        """Add multiple URLs at once"""
        added = 0
        for url in urls:
            if self.add_url(url, priority, depth, referer):
                added += 1
        logger.info(f"Added {added}/{len(urls)} URLs to frontier")
        return added
    
    def get_next(self, respect_politeness: bool = True) -> Optional[FrontierURL]:
        """
        Get next URL to crawl.
        
        Args:
            respect_politeness: If True, enforces delay between requests to same domain
        
        Returns:
            FrontierURL or None if queue empty
        """
        while True:
            # Get highest priority item
            items = self.redis.zrange(self.queue_key, 0, 0, withscores=True)
            
            if not items:
                return None
            
            item_data = items[0][0]
            entry = FrontierURL.from_dict(json.loads(item_data))
            
            # Remove from queue
            self.redis.zrem(self.queue_key, item_data)
            
            # Check politeness
            if respect_politeness:
                if not self._can_crawl_domain(entry.domain):
                    # Put back with lower priority and try next
                    self._delay_url(entry)
                    continue
            
            # Mark as active
            entry.status = URLStatus.CRAWLING
            entry.started_at = datetime.utcnow()
            self.redis.hset(self.active_key, entry.url_hash, json.dumps(entry.to_dict()))
            
            # Record domain access time
            self._record_domain_access(entry.domain)
            
            return entry
    
    def get_next_batch(self, size: int = 10, respect_politeness: bool = True) -> List[FrontierURL]:
        """Get batch of URLs to crawl"""
        urls = []
        for _ in range(size):
            url = self.get_next(respect_politeness)
            if url:
                urls.append(url)
            else:
                break
        return urls
    
    def mark_completed(self, url: str, success: bool, 
                       metadata: Optional[Dict] = None) -> bool:
        """Mark URL as completed or failed"""
        url_hash = hashlib.sha256(url.encode()).hexdigest()
        
        # Remove from active
        active_data = self.redis.hget(self.active_key, url_hash)
        if active_data:
            self.redis.hdel(self.active_key, url_hash)
            entry = FrontierURL.from_dict(json.loads(active_data))
        else:
            # Reconstruct from URL
            entry = FrontierURL(
                url=url,
                priority=URLPriority.NORMAL,
                status=URLStatus.CRAWLING,
                discovered_at=datetime.utcnow(),
                depth=0
            )
        
        entry.completed_at = datetime.utcnow()
        
        if success:
            entry.status = URLStatus.COMPLETED
            if metadata:
                entry.metadata.update(metadata)
            self.redis.hset(self.completed_key, url_hash, json.dumps(entry.to_dict()))
            logger.debug(f"Marked completed: {url}")
        else:
            entry.retry_count += 1
            if entry.retry_count < entry.max_retries:
                # Re-queue with lower priority
                entry.status = URLStatus.QUEUED
                self._delay_url(entry, minutes=5 * entry.retry_count)
                logger.debug(f"Re-queued for retry ({entry.retry_count}/{entry.max_retries}): {url}")
            else:
                entry.status = URLStatus.FAILED
                self.redis.hset(self.failed_key, url_hash, json.dumps(entry.to_dict()))
                logger.warning(f"Max retries exceeded: {url}")
        
        return success
    
    def skip_url(self, url: str, reason: str = ""):
        """Mark URL as skipped (not crawled)"""
        url_hash = hashlib.sha256(url.encode()).hexdigest()
        
        active_data = self.redis.hget(self.active_key, url_hash)
        if active_data:
            self.redis.hdel(self.active_key, url_hash)
            entry = FrontierURL.from_dict(json.loads(active_data))
            entry.status = URLStatus.SKIPPED
            entry.completed_at = datetime.utcnow()
            entry.metadata['skip_reason'] = reason
            self.redis.hset(self.completed_key, url_hash, json.dumps(entry.to_dict()))
            logger.debug(f"Skipped: {url} ({reason})")
    
    def _can_crawl_domain(self, domain: str) -> bool:
        """Check if we can crawl this domain (politeness)"""
        last_access = self.redis.hget(self.domain_delay_key, domain)
        if not last_access:
            return True
        
        last_time = datetime.fromisoformat(last_access.decode())
        elapsed = (datetime.utcnow() - last_time).total_seconds()
        
        return elapsed >= self.politeness_delay
    
    def _record_domain_access(self, domain: str):
        """Record last access time for domain"""
        self.redis.hset(self.domain_delay_key, domain, datetime.utcnow().isoformat())
    
    def _delay_url(self, entry: FrontierURL, minutes: int = 1):
        """Re-queue URL with delay"""
        entry.scheduled_at = datetime.utcnow() + timedelta(minutes=minutes)
        # Lower priority score
        score = (entry.priority.value + 1) * 1000000000 + int(time.time()) + (minutes * 60)
        self.redis.zadd(self.queue_key, {json.dumps(entry.to_dict()): score})
    
    def get_stats(self) -> Dict[str, Any]:
        """Get frontier statistics"""
        return {
            "queued": self.redis.zcard(self.queue_key),
            "seen": self.redis.scard(self.seen_key),
            "active": self.redis.hlen(self.active_key),
            "completed": self.redis.hlen(self.completed_key),
            "failed": self.redis.hlen(self.failed_key),
        }
    
    def get_progress(self) -> Dict[str, Any]:
        """Get crawl progress percentage"""
        stats = self.get_stats()
        total = stats["seen"]
        completed = stats["completed"] + stats["failed"]
        
        if total == 0:
            return {"progress": 0, "total": 0, "completed": 0}
        
        return {
            "progress": (completed / total) * 100,
            "total": total,
            "completed": completed,
            "failed": stats["failed"],
            "remaining": stats["queued"] + stats["active"]
        }
    
    def get_failed_urls(self, limit: int = 100) -> List[FrontierURL]:
        """Get list of failed URLs for retry or analysis"""
        failed = []
        for url_hash, data in self.redis.hscan_iter(self.failed_key):
            entry = FrontierURL.from_dict(json.loads(data))
            failed.append(entry)
            if len(failed) >= limit:
                break
        return failed
    
    def retry_failed(self, max_retries: Optional[int] = None) -> int:
        """Re-queue failed URLs for retry"""
        failed = self.get_failed_urls()
        retried = 0
        
        for entry in failed:
            if max_retries and entry.retry_count >= max_retries:
                continue
            
            url_hash = entry.url_hash
            self.redis.hdel(self.failed_key, url_hash)
            self.redis.srem(self.seen_key, url_hash)
            
            if self.add_url(entry.url, entry.priority, entry.depth, entry.referer, entry.metadata):
                retried += 1
        
        logger.info(f"Re-queued {retried} failed URLs")
        return retried
    
    def clear(self):
        """Clear all frontier data"""
        for key in [self.queue_key, self.seen_key, self.active_key, 
                    self.completed_key, self.failed_key, self.domain_delay_key]:
            self.redis.delete(key)
        logger.info(f"Cleared frontier for {self.scraper_name}")
    
    def export_state(self, filepath: str):
        """Export frontier state to file"""
        state = {
            "scraper_name": self.scraper_name,
            "stats": self.get_stats(),
            "queued": [],
            "active": [],
            "completed": [],
            "failed": []
        }
        
        # Export queued
        for item_data, score in self.redis.zscan_iter(self.queue_key):
            state["queued"].append(json.loads(item_data))
        
        # Export active
        for url_hash, data in self.redis.hscan_iter(self.active_key):
            state["active"].append(json.loads(data))
        
        # Export completed
        for url_hash, data in self.redis.hscan_iter(self.completed_key):
            state["completed"].append(json.loads(data))
        
        # Export failed
        for url_hash, data in self.redis.hscan_iter(self.failed_key):
            state["failed"].append(json.loads(data))
        
        with open(filepath, 'w') as f:
            json.dump(state, f, indent=2)
        
        logger.info(f"Exported frontier state to {filepath}")
    
    def import_state(self, filepath: str):
        """Import frontier state from file"""
        with open(filepath, 'r') as f:
            state = json.load(f)
        
        self.clear()
        
        # Import queued
        for item_data in state.get("queued", []):
            entry = FrontierURL.from_dict(item_data)
            score = entry.priority.value * 1000000000 + int(time.time())
            self.redis.zadd(self.queue_key, {json.dumps(entry.to_dict()): score})
            self.redis.sadd(self.seen_key, entry.url_hash)
        
        # Import other states
        for key, status_key in [("active", self.active_key),
                                ("completed", self.completed_key),
                                ("failed", self.failed_key)]:
            for item_data in state.get(key, []):
                entry = FrontierURL.from_dict(item_data)
                self.redis.hset(status_key, entry.url_hash, json.dumps(entry.to_dict()))
        
        logger.info(f"Imported frontier state from {filepath}")


# URL discovery helpers
class URLDiscovery:
    """Helper for discovering URLs in HTML content"""
    
    @staticmethod
    def extract_links(html: str, base_url: str, 
                      patterns: Optional[List[str]] = None) -> List[str]:
        """
        Extract links from HTML matching patterns.
        
        Args:
            html: HTML content
            base_url: Base URL for resolving relative URLs
            patterns: List of URL patterns to match (e.g., ["/product/", "/item/"])
        
        Returns:
            List of absolute URLs
        """
        from bs4 import BeautifulSoup
        
        soup = BeautifulSoup(html, 'html.parser')
        links = []
        
        for tag in soup.find_all('a', href=True):
            href = tag['href']
            absolute_url = urljoin(base_url, href)
            
            # Check patterns
            if patterns:
                if any(pattern in absolute_url for pattern in patterns):
                    links.append(absolute_url)
            else:
                links.append(absolute_url)
        
        # Deduplicate
        return list(set(links))
    
    @staticmethod
    def extract_pagination_urls(html: str, base_url: str) -> List[str]:
        """Extract pagination URLs"""
        patterns = ['?page=', '&page=', '/page/', '/p/', '?p=']
        all_links = URLDiscovery.extract_links(html, base_url)
        
        pagination_links = []
        for link in all_links:
            if any(pattern in link for pattern in patterns):
                pagination_links.append(link)
        
        return pagination_links
    
    @staticmethod
    def extract_product_urls(html: str, base_url: str, 
                             product_patterns: Optional[List[str]] = None) -> List[str]:
        """Extract product/detail page URLs"""
        default_patterns = [
            '/product/', '/item/', '/detail/', '/drug/', '/medicine/',
            '/medication/', '/pharma/', '/drugs/', '/view/'
        ]
        patterns = product_patterns or default_patterns
        return URLDiscovery.extract_links(html, base_url, patterns)


# Convenience functions
def create_frontier(scraper_name: str, redis_host: str = "localhost", 
                    redis_port: int = 6379) -> CrawlFrontier:
    """Create a crawl frontier with Redis connection"""
    import redis
    client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    return CrawlFrontier(scraper_name, client)
