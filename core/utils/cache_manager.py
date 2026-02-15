#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cache Manager Module

Disk-based caching layer for scraping operations.
Wraps existing functions WITHOUT changing their logic.

Usage:
    from core.utils.cache_manager import cached, get_cache, clear_cache
    
    # Cache function results
    @cached(expire=3600)  # Cache for 1 hour
    def fetch_product_details(product_id):
        return requests.get(f"https://api.example.com/product/{product_id}").json()
    
    # Manual cache operations
    cache = get_cache()
    cache.set("key", value, expire=3600)
    value = cache.get("key")
"""

import logging
import hashlib
import json
import pickle
import time
import os
from pathlib import Path
from typing import Callable, Optional, Any, Dict, Union
from functools import wraps
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Try to import diskcache, gracefully degrade if not available
try:
    from diskcache import Cache, FanoutCache
    DISKCACHE_AVAILABLE = True
except ImportError:
    DISKCACHE_AVAILABLE = False
    Cache = None
    FanoutCache = None


class SimpleFileCache:
    """
    Simple file-based cache fallback when diskcache is not available.
    
    Stores cached values as JSON files in a directory.
    """
    
    def __init__(self, directory: Union[str, Path], default_expire: int = 3600):
        """
        Initialize simple file cache.
        
        Args:
            directory: Cache directory path
            default_expire: Default expiration time in seconds
        """
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.default_expire = default_expire
        self.meta_file = self.directory / "_cache_meta.json"
        self._meta = self._load_meta()
    
    def _load_meta(self) -> Dict:
        """Load cache metadata."""
        if self.meta_file.exists():
            try:
                with open(self.meta_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def _save_meta(self):
        """Save cache metadata."""
        try:
            with open(self.meta_file, 'w') as f:
                json.dump(self._meta, f)
        except Exception as e:
            logger.warning(f"Failed to save cache meta: {e}")
    
    def _key_to_filename(self, key: str) -> Path:
        """Convert cache key to filename."""
        # Hash the key to create a safe filename
        key_hash = hashlib.md5(str(key).encode()).hexdigest()
        return self.directory / f"{key_hash}.cache"
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get value from cache."""
        cache_file = self._key_to_filename(key)
        
        if not cache_file.exists():
            return default
        
        # Check expiration
        meta = self._meta.get(str(key), {})
        expire_at = meta.get("expire_at")
        if expire_at and time.time() > expire_at:
            # Expired
            self.delete(key)
            return default
        
        try:
            with open(cache_file, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            logger.warning(f"Failed to read cache for {key}: {e}")
            return default
    
    def set(self, key: str, value: Any, expire: Optional[int] = None) -> bool:
        """Set value in cache."""
        cache_file = self._key_to_filename(key)
        expire = expire or self.default_expire
        
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump(value, f)
            
            # Update metadata
            self._meta[str(key)] = {
                "created_at": time.time(),
                "expire_at": time.time() + expire if expire else None,
            }
            self._save_meta()
            return True
        except Exception as e:
            logger.warning(f"Failed to write cache for {key}: {e}")
            return False
    
    def delete(self, key: str) -> bool:
        """Delete value from cache."""
        cache_file = self._key_to_filename(key)
        
        try:
            if cache_file.exists():
                cache_file.unlink()
            if str(key) in self._meta:
                del self._meta[str(key)]
                self._save_meta()
            return True
        except Exception as e:
            logger.warning(f"Failed to delete cache for {key}: {e}")
            return False
    
    def clear(self) -> int:
        """Clear all cached values."""
        count = 0
        try:
            for cache_file in self.directory.glob("*.cache"):
                cache_file.unlink()
                count += 1
            self._meta = {}
            self._save_meta()
        except Exception as e:
            logger.warning(f"Failed to clear cache: {e}")
        return count
    
    def __contains__(self, key: str) -> bool:
        """Check if key exists in cache."""
        return self.get(key) is not None
    
    def __getitem__(self, key: str) -> Any:
        """Get item with bracket notation."""
        value = self.get(key)
        if value is None:
            raise KeyError(key)
        return value
    
    def __setitem__(self, key: str, value: Any):
        """Set item with bracket notation."""
        self.set(key, value)
    
    def __delitem__(self, key: str):
        """Delete item with bracket notation."""
        self.delete(key)
    
    def close(self):
        """Close the cache (no-op for file cache)."""
        pass
    
    def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        cache_files = list(self.directory.glob("*.cache"))
        total_size = sum(f.stat().st_size for f in cache_files)
        
        return {
            "entries": len(cache_files),
            "size_bytes": total_size,
            "size_mb": total_size / (1024 * 1024),
            "directory": str(self.directory),
        }


class CacheManager:
    """
    Centralized cache manager for the scraper platform.
    
    Uses diskcache if available, falls back to simple file cache.
    """
    
    _instance: Optional['CacheManager'] = None
    _cache: Optional[Any] = None
    
    def __init__(self, cache_dir: Optional[Union[str, Path]] = None):
        """
        Initialize cache manager.
        
        Args:
            cache_dir: Cache directory (default: repo_root/cache)
        """
        if cache_dir is None:
            # Try to get from ConfigManager
            try:
                from core.config.config_manager import ConfigManager
                cache_dir = ConfigManager.get_cache_dir()
            except:
                # Fallback to current directory
                cache_dir = Path(__file__).parent.parent / "cache"
        
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize cache backend
        if DISKCACHE_AVAILABLE:
            self._cache = Cache(str(self.cache_dir / "diskcache"))
            logger.debug(f"Using diskcache backend at {self.cache_dir}")
        else:
            self._cache = SimpleFileCache(self.cache_dir / "filecache")
            logger.debug(f"Using simple file cache at {self.cache_dir}")
    
    @classmethod
    def get_instance(cls, cache_dir: Optional[Union[str, Path]] = None) -> 'CacheManager':
        """Get singleton instance of CacheManager."""
        if cls._instance is None:
            cls._instance = cls(cache_dir)
        return cls._instance
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get value from cache."""
        try:
            if DISKCACHE_AVAILABLE:
                return self._cache.get(key, default)
            else:
                return self._cache.get(key, default)
        except Exception as e:
            logger.warning(f"Cache get error for {key}: {e}")
            return default
    
    def set(self, key: str, value: Any, expire: Optional[int] = None) -> bool:
        """
        Set value in cache.
        
        Args:
            key: Cache key
            value: Value to cache
            expire: Expiration time in seconds (None = no expiration)
        
        Returns:
            True if successful
        """
        try:
            if DISKCACHE_AVAILABLE:
                self._cache.set(key, value, expire=expire)
            else:
                self._cache.set(key, value, expire=expire)
            return True
        except Exception as e:
            logger.warning(f"Cache set error for {key}: {e}")
            return False
    
    def delete(self, key: str) -> bool:
        """Delete value from cache."""
        try:
            if DISKCACHE_AVAILABLE:
                return self._cache.delete(key)
            else:
                return self._cache.delete(key)
        except Exception as e:
            logger.warning(f"Cache delete error for {key}: {e}")
            return False
    
    def clear(self) -> int:
        """Clear all cached values."""
        try:
            if DISKCACHE_AVAILABLE:
                return self._cache.clear()
            else:
                return self._cache.clear()
        except Exception as e:
            logger.warning(f"Cache clear error: {e}")
            return 0
    
    def memoize(self, expire: Optional[int] = None, typed: bool = False):
        """
        Decorator to memoize function results.
        
        Args:
            expire: Expiration time in seconds
            typed: If True, arguments of different types are cached separately
        
        Usage:
            @cache.memoize(expire=3600)
            def expensive_function(x, y):
                return x + y
        """
        if DISKCACHE_AVAILABLE:
            return self._cache.memoize(expire=expire, typed=typed)
        else:
            # Fallback implementation
            def decorator(func: Callable) -> Callable:
                @wraps(func)
                def wrapper(*args, **kwargs):
                    # Create cache key from function name and arguments
                    key_parts = [func.__module__, func.__name__]
                    key_parts.extend(str(arg) for arg in args)
                    key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))
                    key = hashlib.md5(":".join(key_parts).encode()).hexdigest()
                    
                    # Check cache
                    cached_value = self.get(key)
                    if cached_value is not None:
                        return cached_value
                    
                    # Compute and cache
                    result = func(*args, **kwargs)
                    self.set(key, result, expire=expire)
                    return result
                
                return wrapper
            return decorator
    
    def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        try:
            if DISKCACHE_AVAILABLE:
                return {
                    "backend": "diskcache",
                    "size_bytes": self._cache.volume(),
                    "size_mb": self._cache.volume() / (1024 * 1024),
                    "directory": str(self._cache.directory),
                }
            else:
                return self._cache.stats()
        except Exception as e:
            return {"error": str(e)}
    
    def close(self):
        """Close the cache."""
        try:
            self._cache.close()
        except:
            pass
    
    def __contains__(self, key: str) -> bool:
        return self.get(key) is not None
    
    def __getitem__(self, key: str) -> Any:
        value = self.get(key)
        if value is None:
            raise KeyError(key)
        return value
    
    def __setitem__(self, key: str, value: Any):
        self.set(key, value)
    
    def __delitem__(self, key: str):
        self.delete(key)


# Global cache instance
_global_cache: Optional[CacheManager] = None


def get_cache(cache_dir: Optional[Union[str, Path]] = None) -> CacheManager:
    """Get global cache instance."""
    global _global_cache
    if _global_cache is None:
        _global_cache = CacheManager.get_instance(cache_dir)
    return _global_cache


def cached(
    expire: Optional[int] = 3600,
    key_prefix: str = "",
    ignore_args: bool = False,
):
    """
    Decorator to cache function results.
    
    Args:
        expire: Expiration time in seconds (default: 1 hour)
        key_prefix: Prefix for cache keys
        ignore_args: If True, cache key is just function name (same result for all args)
    
    Usage:
        @cached(expire=3600)
        def fetch_data(url):
            return requests.get(url).json()
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache = get_cache()
            
            # Create cache key
            if ignore_args:
                key = f"{key_prefix}{func.__module__}.{func.__name__}"
            else:
                key_parts = [key_prefix, func.__module__, func.__name__]
                key_parts.extend(str(arg) for arg in args)
                key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))
                key = hashlib.md5(":".join(key_parts).encode()).hexdigest()
            
            # Check cache
            cached_value = cache.get(key)
            if cached_value is not None:
                logger.debug(f"Cache hit for {func.__name__}")
                return cached_value
            
            # Compute and cache
            logger.debug(f"Cache miss for {func.__name__}")
            result = func(*args, **kwargs)
            cache.set(key, result, expire=expire)
            return result
        
        # Add cache control methods
        wrapper.cache_clear = lambda: get_cache().delete(
            f"{key_prefix}{func.__module__}.{func.__name__}"
        )
        
        return wrapper
    return decorator


def cached_property(expire: Optional[int] = None):
    """
    Decorator for cached class properties.
    
    Usage:
        class MyClass:
            @cached_property(expire=3600)
            def expensive_property(self):
                return compute_expensive_value()
    """
    def decorator(func: Callable) -> property:
        @wraps(func)
        def wrapper(self):
            cache = get_cache()
            key = f"{self.__class__.__name__}.{func.__name__}.{id(self)}"
            
            cached_value = cache.get(key)
            if cached_value is not None:
                return cached_value
            
            result = func(self)
            cache.set(key, result, expire=expire)
            return result
        
        return property(wrapper)
    return decorator


def clear_cache():
    """Clear all cached values."""
    return get_cache().clear()


def cache_stats() -> Dict[str, Any]:
    """Get cache statistics."""
    return get_cache().stats()


class ScrapeCache:
    """
    Specialized cache for scraping operations.
    
    Provides scraper-specific caching with automatic key generation.
    """
    
    def __init__(self, scraper_name: str, expire: int = 86400):
        """
        Initialize scrape cache.
        
        Args:
            scraper_name: Name of the scraper
            expire: Default expiration in seconds (default: 24 hours)
        """
        self.scraper_name = scraper_name
        self.expire = expire
        self.cache = get_cache()
    
    def _make_key(self, *parts) -> str:
        """Create cache key from parts."""
        key_str = f"{self.scraper_name}:" + ":".join(str(p) for p in parts)
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def get_page(self, url: str) -> Optional[str]:
        """Get cached page content."""
        return self.cache.get(self._make_key("page", url))
    
    def set_page(self, url: str, content: str, expire: Optional[int] = None):
        """Cache page content."""
        self.cache.set(self._make_key("page", url), content, expire or self.expire)
    
    def get_product(self, product_id: str) -> Optional[Dict]:
        """Get cached product data."""
        return self.cache.get(self._make_key("product", product_id))
    
    def set_product(self, product_id: str, data: Dict, expire: Optional[int] = None):
        """Cache product data."""
        self.cache.set(self._make_key("product", product_id), data, expire or self.expire)
    
    def get_api_response(self, endpoint: str, params: Dict = None) -> Optional[Any]:
        """Get cached API response."""
        params_str = json.dumps(params or {}, sort_keys=True)
        return self.cache.get(self._make_key("api", endpoint, params_str))
    
    def set_api_response(
        self, endpoint: str, response: Any, params: Dict = None, expire: Optional[int] = None
    ):
        """Cache API response."""
        params_str = json.dumps(params or {}, sort_keys=True)
        self.cache.set(
            self._make_key("api", endpoint, params_str),
            response,
            expire or self.expire
        )
    
    def clear_scraper_cache(self):
        """Clear all cache entries for this scraper."""
        # Note: This is a simplified implementation
        # A full implementation would track all keys per scraper
        logger.info(f"Cache clear requested for {self.scraper_name}")


def get_scrape_cache(scraper_name: str, expire: int = 86400) -> ScrapeCache:
    """Get a scrape cache instance for a scraper."""
    return ScrapeCache(scraper_name, expire)


if __name__ == "__main__":
    # Demo/test
    print(f"diskcache available: {DISKCACHE_AVAILABLE}")
    
    # Test basic cache operations
    cache = get_cache()
    
    print("\nTesting cache operations:")
    cache.set("test_key", {"data": "test_value"}, expire=60)
    print(f"Set: test_key -> {{'data': 'test_value'}}")
    
    value = cache.get("test_key")
    print(f"Get: test_key -> {value}")
    
    print(f"\nCache stats: {cache.stats()}")
    
    # Test cached decorator
    @cached(expire=60)
    def expensive_function(x, y):
        print(f"  Computing {x} + {y}...")
        time.sleep(0.1)  # Simulate expensive operation
        return x + y
    
    print("\nTesting @cached decorator:")
    print(f"First call: {expensive_function(1, 2)}")
    print(f"Second call (cached): {expensive_function(1, 2)}")
    print(f"Different args: {expensive_function(3, 4)}")
    
    # Clean up
    cache.clear()
    print("\nCache cleared")
