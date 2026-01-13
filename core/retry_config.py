#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Centralized Retry and Timeout Configuration

Provides consistent timeout, retry, and backoff settings across all scrapers.
Business Logic Unchanged: Only centralizes configuration, no parsing/selectors changed.
"""

import time
from typing import Dict, Callable, Any, Optional
from functools import wraps
import logging

logger = logging.getLogger(__name__)


class RetryConfig:
    """Centralized retry and timeout configuration"""
    
    # Browser timeouts (seconds)
    PAGE_LOAD_TIMEOUT = 60
    ELEMENT_WAIT_TIMEOUT = 30
    NAVIGATION_TIMEOUT = 90
    CONNECTION_CHECK_TIMEOUT = 30
    
    # Retry settings
    MAX_RETRIES = 3
    MAX_RETRY_LOOPS = 5  # For nested retry loops
    RETRY_BACKOFF_BASE = 2.0  # Exponential backoff multiplier
    RETRY_DELAY_SECONDS = 2.0  # Base delay between retries
    RETRY_DELAY_MAX_SECONDS = 120.0  # Max delay (2 minutes)
    
    # Queue/Thread settings
    QUEUE_GET_TIMEOUT = 30
    THREAD_JOIN_TIMEOUT = 300  # 5 minutes
    
    # Connection/Network settings
    CONNECTION_RETRY_DELAY = 120  # 2 minutes for connection retries
    CONNECTION_MAX_RETRIES = 3
    
    @classmethod
    def get_timeout_config(cls) -> Dict[str, float]:
        """Get all timeout configurations as a dict"""
        return {
            "page_load": cls.PAGE_LOAD_TIMEOUT,
            "element_wait": cls.ELEMENT_WAIT_TIMEOUT,
            "navigation": cls.NAVIGATION_TIMEOUT,
            "connection_check": cls.CONNECTION_CHECK_TIMEOUT,
            "queue_get": cls.QUEUE_GET_TIMEOUT,
            "thread_join": cls.THREAD_JOIN_TIMEOUT
        }
    
    @classmethod
    def get_retry_config(cls) -> Dict[str, Any]:
        """Get all retry configurations as a dict"""
        return {
            "max_retries": cls.MAX_RETRIES,
            "max_retry_loops": cls.MAX_RETRY_LOOPS,
            "backoff_base": cls.RETRY_BACKOFF_BASE,
            "delay_seconds": cls.RETRY_DELAY_SECONDS,
            "delay_max_seconds": cls.RETRY_DELAY_MAX_SECONDS,
            "connection_delay": cls.CONNECTION_RETRY_DELAY,
            "connection_max_retries": cls.CONNECTION_MAX_RETRIES
        }
    
    @classmethod
    def calculate_backoff_delay(cls, attempt: int) -> float:
        """
        Calculate exponential backoff delay for a retry attempt.
        
        Args:
            attempt: Retry attempt number (0-indexed)
        
        Returns:
            Delay in seconds
        """
        delay = cls.RETRY_DELAY_SECONDS * (cls.RETRY_BACKOFF_BASE ** attempt)
        return min(delay, cls.RETRY_DELAY_MAX_SECONDS)


def retry_with_backoff(
    max_retries: Optional[int] = None,
    delay: Optional[float] = None,
    exceptions: tuple = (Exception,),
    on_retry: Optional[Callable[[Exception, int], None]] = None
):
    """
    Decorator for retrying a function with exponential backoff.
    
    Args:
        max_retries: Maximum number of retries (default: RetryConfig.MAX_RETRIES)
        delay: Base delay in seconds (default: RetryConfig.RETRY_DELAY_SECONDS)
        exceptions: Tuple of exceptions to catch and retry (default: all exceptions)
        on_retry: Optional callback function(exception, attempt) called before each retry
    
    Usage:
        @retry_with_backoff(max_retries=3, delay=2.0)
        def my_function():
            # code that may fail
            pass
    """
    if max_retries is None:
        max_retries = RetryConfig.MAX_RETRIES
    if delay is None:
        delay = RetryConfig.RETRY_DELAY_SECONDS
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):  # +1 for initial attempt
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        backoff_delay = RetryConfig.calculate_backoff_delay(attempt)
                        logger.warning(
                            f"Retry {attempt + 1}/{max_retries} for {func.__name__} after {backoff_delay:.2f}s: {e}"
                        )
                        if on_retry:
                            try:
                                on_retry(e, attempt + 1)
                            except:
                                pass
                        time.sleep(backoff_delay)
                    else:
                        logger.error(f"All {max_retries} retries failed for {func.__name__}: {e}")
            # If we get here, all retries failed
            raise last_exception
        return wrapper
    return decorator
