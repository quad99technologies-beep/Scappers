#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rate Limiter Module

Rate limiting decorators for scraping operations.
Wraps existing functions WITHOUT changing their logic.

Usage:
    from core.rate_limiter import rate_limit, adaptive_rate_limit
    
    # Fixed rate limit: 10 requests per minute
    @rate_limit(calls=10, period=60)
    def fetch_page(url):
        return requests.get(url)
    
    # Adaptive rate limit that backs off on errors
    @adaptive_rate_limit(initial_delay=1.0)
    def scrape_product(product_id):
        return get_product_details(product_id)
"""

import logging
import time
import threading
from typing import Callable, Optional, Any, Dict
from functools import wraps
from collections import deque
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Try to import ratelimit, gracefully degrade if not available
try:
    from ratelimit import limits, sleep_and_retry, RateLimitException
    RATELIMIT_AVAILABLE = True
except ImportError:
    RATELIMIT_AVAILABLE = False
    RateLimitException = Exception


class TokenBucket:
    """
    Token bucket rate limiter implementation.
    
    Allows bursts while maintaining average rate.
    """
    
    def __init__(self, rate: float, capacity: int):
        """
        Initialize token bucket.
        
        Args:
            rate: Tokens added per second
            capacity: Maximum tokens (burst capacity)
        """
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.monotonic()
        self.lock = threading.Lock()
    
    def acquire(self, tokens: int = 1, blocking: bool = True) -> bool:
        """
        Acquire tokens from the bucket.
        
        Args:
            tokens: Number of tokens to acquire
            blocking: If True, wait for tokens; if False, return immediately
        
        Returns:
            True if tokens acquired, False if not (only when blocking=False)
        """
        with self.lock:
            while True:
                # Refill tokens based on time elapsed
                now = time.monotonic()
                elapsed = now - self.last_update
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                self.last_update = now
                
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return True
                
                if not blocking:
                    return False
                
                # Calculate wait time
                wait_time = (tokens - self.tokens) / self.rate
                time.sleep(min(wait_time, 1.0))  # Check every second at most


class SlidingWindowRateLimiter:
    """
    Sliding window rate limiter.
    
    More accurate than fixed window, prevents burst at window boundaries.
    """
    
    def __init__(self, calls: int, period: float):
        """
        Initialize sliding window limiter.
        
        Args:
            calls: Maximum calls allowed in the period
            period: Time period in seconds
        """
        self.calls = calls
        self.period = period
        self.timestamps: deque = deque()
        self.lock = threading.Lock()
    
    def acquire(self, blocking: bool = True) -> bool:
        """
        Acquire permission to make a call.
        
        Args:
            blocking: If True, wait until allowed; if False, return immediately
        
        Returns:
            True if allowed, False if not (only when blocking=False)
        """
        with self.lock:
            while True:
                now = time.monotonic()
                
                # Remove expired timestamps
                cutoff = now - self.period
                while self.timestamps and self.timestamps[0] < cutoff:
                    self.timestamps.popleft()
                
                if len(self.timestamps) < self.calls:
                    self.timestamps.append(now)
                    return True
                
                if not blocking:
                    return False
                
                # Wait until oldest timestamp expires
                wait_time = self.timestamps[0] + self.period - now
                if wait_time > 0:
                    time.sleep(min(wait_time + 0.01, 1.0))


class AdaptiveRateLimiter:
    """
    Adaptive rate limiter that adjusts based on response patterns.
    
    Slows down on errors, speeds up on success.
    """
    
    def __init__(
        self,
        initial_delay: float = 1.0,
        min_delay: float = 0.1,
        max_delay: float = 60.0,
        backoff_factor: float = 2.0,
        recovery_factor: float = 0.9,
    ):
        """
        Initialize adaptive rate limiter.
        
        Args:
            initial_delay: Starting delay between calls
            min_delay: Minimum delay
            max_delay: Maximum delay
            backoff_factor: Multiply delay by this on error
            recovery_factor: Multiply delay by this on success
        """
        self.initial_delay = initial_delay
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.recovery_factor = recovery_factor
        
        self.current_delay = initial_delay
        self.last_call = 0.0
        self.consecutive_errors = 0
        self.lock = threading.Lock()
    
    def wait(self):
        """Wait for the appropriate delay before next call."""
        with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_call
            
            if elapsed < self.current_delay:
                time.sleep(self.current_delay - elapsed)
            
            self.last_call = time.monotonic()
    
    def report_success(self):
        """Report a successful call (may decrease delay)."""
        with self.lock:
            self.consecutive_errors = 0
            self.current_delay = max(
                self.min_delay,
                self.current_delay * self.recovery_factor
            )
    
    def report_error(self):
        """Report a failed call (increases delay)."""
        with self.lock:
            self.consecutive_errors += 1
            self.current_delay = min(
                self.max_delay,
                self.current_delay * self.backoff_factor
            )
            logger.warning(
                f"Rate limiter backing off: delay={self.current_delay:.2f}s "
                f"(consecutive errors: {self.consecutive_errors})"
            )
    
    def reset(self):
        """Reset to initial state."""
        with self.lock:
            self.current_delay = self.initial_delay
            self.consecutive_errors = 0
    
    @property
    def stats(self) -> Dict[str, Any]:
        """Get current limiter statistics."""
        with self.lock:
            return {
                "current_delay": self.current_delay,
                "consecutive_errors": self.consecutive_errors,
                "min_delay": self.min_delay,
                "max_delay": self.max_delay,
            }


def rate_limit(
    calls: int = 10,
    period: float = 60.0,
    raise_on_limit: bool = False,
):
    """
    Rate limit decorator.
    
    Args:
        calls: Maximum calls allowed in the period
        period: Time period in seconds
        raise_on_limit: If True, raise exception when limit hit; 
                       if False (default), wait
    
    Usage:
        @rate_limit(calls=10, period=60)  # 10 calls per minute
        def fetch_data(url):
            return requests.get(url)
    """
    limiter = SlidingWindowRateLimiter(calls, period)
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not limiter.acquire(blocking=not raise_on_limit):
                raise RateLimitException(
                    f"Rate limit exceeded: {calls} calls per {period}s"
                )
            return func(*args, **kwargs)
        
        # Attach limiter for inspection
        wrapper._rate_limiter = limiter
        return wrapper
    return decorator


def rate_limit_with_retry(
    calls: int = 10,
    period: float = 60.0,
):
    """
    Rate limit decorator that automatically waits and retries.
    
    Uses the ratelimit library if available, falls back to custom implementation.
    
    Args:
        calls: Maximum calls allowed in the period
        period: Time period in seconds
    
    Usage:
        @rate_limit_with_retry(calls=5, period=60)
        def api_call(endpoint):
            return requests.get(endpoint)
    """
    if RATELIMIT_AVAILABLE:
        def decorator(func: Callable) -> Callable:
            # Use ratelimit library's decorators
            @sleep_and_retry
            @limits(calls=calls, period=period)
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper
        return decorator
    else:
        # Fall back to our implementation
        return rate_limit(calls=calls, period=period, raise_on_limit=False)


def adaptive_rate_limit(
    initial_delay: float = 1.0,
    min_delay: float = 0.1,
    max_delay: float = 60.0,
    backoff_factor: float = 2.0,
    recovery_factor: float = 0.9,
    error_exceptions: tuple = (Exception,),
):
    """
    Adaptive rate limit decorator.
    
    Automatically adjusts rate based on success/failure patterns.
    
    Args:
        initial_delay: Starting delay between calls
        min_delay: Minimum delay
        max_delay: Maximum delay
        backoff_factor: Multiply delay by this on error
        recovery_factor: Multiply delay by this on success
        error_exceptions: Exceptions that trigger backoff
    
    Usage:
        @adaptive_rate_limit(initial_delay=1.0)
        def scrape_page(url):
            return requests.get(url)
    """
    limiter = AdaptiveRateLimiter(
        initial_delay=initial_delay,
        min_delay=min_delay,
        max_delay=max_delay,
        backoff_factor=backoff_factor,
        recovery_factor=recovery_factor,
    )
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            limiter.wait()
            try:
                result = func(*args, **kwargs)
                limiter.report_success()
                return result
            except error_exceptions as e:
                limiter.report_error()
                raise
        
        # Attach limiter for inspection/control
        wrapper._rate_limiter = limiter
        wrapper.reset_rate_limiter = limiter.reset
        wrapper.get_rate_stats = lambda: limiter.stats
        return wrapper
    return decorator


def token_bucket_limit(
    rate: float = 1.0,
    capacity: int = 10,
):
    """
    Token bucket rate limit decorator.
    
    Allows bursts up to capacity while maintaining average rate.
    
    Args:
        rate: Tokens per second (average rate)
        capacity: Maximum burst size
    
    Usage:
        @token_bucket_limit(rate=2, capacity=10)  # 2/sec avg, burst of 10
        def process_item(item):
            return handle(item)
    """
    bucket = TokenBucket(rate, capacity)
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            bucket.acquire()
            return func(*args, **kwargs)
        
        wrapper._rate_limiter = bucket
        return wrapper
    return decorator


class RateLimitContext:
    """
    Context manager for rate limiting.
    
    Usage:
        limiter = RateLimitContext(calls=10, period=60)
        
        for url in urls:
            with limiter:
                response = requests.get(url)
    """
    
    def __init__(self, calls: int = 10, period: float = 60.0):
        self.limiter = SlidingWindowRateLimiter(calls, period)
    
    def __enter__(self):
        self.limiter.acquire()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class DomainRateLimiter:
    """
    Per-domain rate limiter for web scraping.
    
    Maintains separate rate limits for different domains.
    
    Usage:
        limiter = DomainRateLimiter(calls_per_domain=5, period=60)
        
        for url in urls:
            limiter.wait(url)
            response = requests.get(url)
    """
    
    def __init__(
        self,
        calls_per_domain: int = 10,
        period: float = 60.0,
        default_delay: float = 1.0,
    ):
        self.calls_per_domain = calls_per_domain
        self.period = period
        self.default_delay = default_delay
        self.limiters: Dict[str, SlidingWindowRateLimiter] = {}
        self.lock = threading.Lock()
    
    def _get_domain(self, url: str) -> str:
        """Extract domain from URL."""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            return parsed.netloc or url
        except:
            return url
    
    def _get_limiter(self, domain: str) -> SlidingWindowRateLimiter:
        """Get or create limiter for domain."""
        with self.lock:
            if domain not in self.limiters:
                self.limiters[domain] = SlidingWindowRateLimiter(
                    self.calls_per_domain, self.period
                )
            return self.limiters[domain]
    
    def wait(self, url: str):
        """Wait for rate limit before accessing URL."""
        domain = self._get_domain(url)
        limiter = self._get_limiter(domain)
        limiter.acquire()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics for all domains."""
        with self.lock:
            return {
                domain: {
                    "pending_calls": len(limiter.timestamps),
                    "calls_limit": self.calls_per_domain,
                    "period": self.period,
                }
                for domain, limiter in self.limiters.items()
            }


# Global domain rate limiter instance
_domain_limiter: Optional[DomainRateLimiter] = None


def get_domain_limiter(
    calls_per_domain: int = 10,
    period: float = 60.0,
) -> DomainRateLimiter:
    """Get or create global domain rate limiter."""
    global _domain_limiter
    if _domain_limiter is None:
        _domain_limiter = DomainRateLimiter(calls_per_domain, period)
    return _domain_limiter


def wait_for_domain(url: str):
    """Wait for rate limit before accessing URL (using global limiter)."""
    get_domain_limiter().wait(url)


# Convenience function for one-off rate limiting
def rate_limited_call(
    func: Callable,
    *args,
    calls: int = 10,
    period: float = 60.0,
    **kwargs,
) -> Any:
    """
    Make a rate-limited function call (one-off, no decorator needed).
    
    Note: This creates a new limiter each time, so it's best used
    with a shared limiter for repeated calls.
    
    Args:
        func: Function to call
        *args: Positional arguments
        calls: Rate limit calls
        period: Rate limit period
        **kwargs: Keyword arguments
    
    Returns:
        Function result
    """
    limiter = SlidingWindowRateLimiter(calls, period)
    limiter.acquire()
    return func(*args, **kwargs)


if __name__ == "__main__":
    # Demo/test
    print(f"ratelimit library available: {RATELIMIT_AVAILABLE}")
    
    # Test sliding window limiter
    @rate_limit(calls=3, period=5)
    def test_function(n):
        print(f"Call {n} at {time.strftime('%H:%M:%S')}")
        return n
    
    print("\nTesting rate limit (3 calls per 5 seconds):")
    for i in range(6):
        test_function(i)
    
    # Test adaptive limiter
    print("\nTesting adaptive rate limit:")
    
    @adaptive_rate_limit(initial_delay=0.5, min_delay=0.1, max_delay=5.0)
    def test_adaptive(n, fail=False):
        if fail:
            raise ValueError("Simulated error")
        print(f"Adaptive call {n} at {time.strftime('%H:%M:%S')}")
        return n
    
    for i in range(3):
        test_adaptive(i)
        print(f"  Stats: {test_adaptive.get_rate_stats()}")
    
    # Simulate errors
    for i in range(2):
        try:
            test_adaptive(i, fail=True)
        except ValueError:
            print(f"  Error, stats: {test_adaptive.get_rate_stats()}")
