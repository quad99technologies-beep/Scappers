#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Smart Retry Module

Intelligent retry wrappers using tenacity library.
Wraps existing functions WITHOUT changing their logic.

Usage:
    from core.reliability.smart_retry import retry_request, retry_browser_action, with_retry
    
    # Wrap a requests call
    @retry_request()
    def fetch_data(url):
        return requests.get(url)
    
    # Wrap a browser action
    @retry_browser_action()
    def click_button(driver, selector):
        driver.find_element(By.CSS_SELECTOR, selector).click()
    
    # Generic retry wrapper
    @with_retry(max_attempts=5, wait_seconds=2)
    def my_function():
        pass
"""

import logging
import time
from typing import Callable, Optional, Any, Tuple, Type, Union
from functools import wraps

logger = logging.getLogger(__name__)

# Try to import tenacity, gracefully degrade if not available
try:
    from tenacity import (
        retry,
        stop_after_attempt,
        stop_after_delay,
        wait_exponential,
        wait_fixed,
        wait_random,
        retry_if_exception_type,
        retry_if_result,
        before_sleep_log,
        after_log,
        RetryError,
    )
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False
    retry = None
    RetryError = Exception

# Common exception types for different scenarios
try:
    from requests.exceptions import (
        RequestException,
        ConnectionError as RequestsConnectionError,
        Timeout as RequestsTimeout,
        HTTPError,
    )
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    RequestException = Exception
    RequestsConnectionError = Exception
    RequestsTimeout = Exception
    HTTPError = Exception

try:
    from selenium.common.exceptions import (
        WebDriverException,
        TimeoutException as SeleniumTimeout,
        StaleElementReferenceException,
        NoSuchElementException,
        ElementClickInterceptedException,
        ElementNotInteractableException,
    )
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    WebDriverException = Exception
    SeleniumTimeout = Exception
    StaleElementReferenceException = Exception
    NoSuchElementException = Exception
    ElementClickInterceptedException = Exception
    ElementNotInteractableException = Exception


class RetryConfig:
    """Configuration for retry behavior."""
    
    # Default settings
    DEFAULT_MAX_ATTEMPTS = 3
    DEFAULT_WAIT_SECONDS = 2
    DEFAULT_MAX_WAIT_SECONDS = 60
    DEFAULT_EXPONENTIAL_MULTIPLIER = 2
    
    # Request-specific settings
    REQUEST_MAX_ATTEMPTS = 5
    REQUEST_WAIT_MIN = 1
    REQUEST_WAIT_MAX = 30
    
    # Browser-specific settings
    BROWSER_MAX_ATTEMPTS = 3
    BROWSER_WAIT_MIN = 0.5
    BROWSER_WAIT_MAX = 10
    
    # Connection-specific settings
    CONNECTION_MAX_ATTEMPTS = 10
    CONNECTION_WAIT_SECONDS = 30
    CONNECTION_MAX_DELAY = 300  # 5 minutes total


def _fallback_retry(
    max_attempts: int = 3,
    wait_seconds: float = 2,
    exceptions: tuple = (Exception,),
    on_retry: Optional[Callable] = None,
):
    """
    Fallback retry decorator when tenacity is not available.
    Provides basic retry functionality.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts:
                        logger.warning(
                            f"Retry {attempt}/{max_attempts} for {func.__name__}: {e}"
                        )
                        if on_retry:
                            try:
                                on_retry(attempt, e)
                            except:
                                pass
                        time.sleep(wait_seconds * attempt)  # Simple backoff
                    else:
                        logger.error(
                            f"All {max_attempts} retries failed for {func.__name__}: {e}"
                        )
            raise last_exception
        return wrapper
    return decorator


def with_retry(
    max_attempts: int = RetryConfig.DEFAULT_MAX_ATTEMPTS,
    wait_seconds: float = RetryConfig.DEFAULT_WAIT_SECONDS,
    max_wait_seconds: float = RetryConfig.DEFAULT_MAX_WAIT_SECONDS,
    exceptions: tuple = (Exception,),
    exponential_backoff: bool = True,
    on_retry: Optional[Callable[[int, Exception], None]] = None,
):
    """
    Generic retry decorator.
    
    Args:
        max_attempts: Maximum number of retry attempts
        wait_seconds: Base wait time between retries
        max_wait_seconds: Maximum wait time (for exponential backoff)
        exceptions: Tuple of exceptions to catch and retry
        exponential_backoff: Use exponential backoff (True) or fixed wait (False)
        on_retry: Optional callback(attempt_number, exception) called before each retry
    
    Usage:
        @with_retry(max_attempts=5, wait_seconds=2)
        def my_function():
            # code that may fail
            pass
    """
    if not TENACITY_AVAILABLE:
        return _fallback_retry(max_attempts, wait_seconds, exceptions, on_retry)
    
    def decorator(func: Callable) -> Callable:
        # Build wait strategy
        if exponential_backoff:
            wait_strategy = wait_exponential(
                multiplier=wait_seconds,
                min=wait_seconds,
                max=max_wait_seconds,
            )
        else:
            wait_strategy = wait_fixed(wait_seconds)
        
        # Build retry decorator
        retry_decorator = retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_strategy,
            retry=retry_if_exception_type(exceptions),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return retry_decorator(func)(*args, **kwargs)
            except RetryError as e:
                # Re-raise the original exception
                raise e.last_attempt.exception()
        
        return wrapper
    return decorator


def retry_request(
    max_attempts: int = RetryConfig.REQUEST_MAX_ATTEMPTS,
    wait_min: float = RetryConfig.REQUEST_WAIT_MIN,
    wait_max: float = RetryConfig.REQUEST_WAIT_MAX,
    retry_on_status: Tuple[int, ...] = (429, 500, 502, 503, 504),
):
    """
    Retry decorator optimized for HTTP requests.
    
    Retries on:
    - Connection errors
    - Timeouts
    - Specific HTTP status codes (429, 5xx by default)
    
    Args:
        max_attempts: Maximum retry attempts
        wait_min: Minimum wait between retries
        wait_max: Maximum wait between retries
        retry_on_status: HTTP status codes to retry on
    
    Usage:
        @retry_request()
        def fetch_api_data(url):
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
    """
    if not REQUESTS_AVAILABLE:
        logger.warning("requests library not available, using generic retry")
        return with_retry(max_attempts=max_attempts, wait_seconds=wait_min)
    
    request_exceptions = (
        RequestsConnectionError,
        RequestsTimeout,
        HTTPError,
    )
    
    if not TENACITY_AVAILABLE:
        return _fallback_retry(max_attempts, wait_min, request_exceptions)
    
    def should_retry_response(result):
        """Check if response status code indicates retry is needed."""
        if hasattr(result, 'status_code'):
            return result.status_code in retry_on_status
        return False
    
    def decorator(func: Callable) -> Callable:
        retry_decorator = retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=1, min=wait_min, max=wait_max),
            retry=(
                retry_if_exception_type(request_exceptions) |
                retry_if_result(should_retry_response)
            ),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return retry_decorator(func)(*args, **kwargs)
            except RetryError as e:
                raise e.last_attempt.exception()
        
        return wrapper
    return decorator


def retry_browser_action(
    max_attempts: int = RetryConfig.BROWSER_MAX_ATTEMPTS,
    wait_min: float = RetryConfig.BROWSER_WAIT_MIN,
    wait_max: float = RetryConfig.BROWSER_WAIT_MAX,
    retry_stale: bool = True,
    retry_not_interactable: bool = True,
):
    """
    Retry decorator optimized for Selenium browser actions.
    
    Retries on:
    - Stale element references
    - Element not interactable
    - Click intercepted
    - Timeouts
    
    Args:
        max_attempts: Maximum retry attempts
        wait_min: Minimum wait between retries
        wait_max: Maximum wait between retries
        retry_stale: Retry on stale element references
        retry_not_interactable: Retry when element is not interactable
    
    Usage:
        @retry_browser_action()
        def click_submit_button(driver):
            button = driver.find_element(By.ID, "submit")
            button.click()
    """
    if not SELENIUM_AVAILABLE:
        logger.warning("selenium library not available, using generic retry")
        return with_retry(max_attempts=max_attempts, wait_seconds=wait_min)
    
    # Build exception list based on options
    browser_exceptions = [SeleniumTimeout, WebDriverException]
    if retry_stale:
        browser_exceptions.append(StaleElementReferenceException)
    if retry_not_interactable:
        browser_exceptions.extend([
            ElementClickInterceptedException,
            ElementNotInteractableException,
        ])
    browser_exceptions = tuple(browser_exceptions)
    
    if not TENACITY_AVAILABLE:
        return _fallback_retry(max_attempts, wait_min, browser_exceptions)
    
    def decorator(func: Callable) -> Callable:
        retry_decorator = retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=0.5, min=wait_min, max=wait_max),
            retry=retry_if_exception_type(browser_exceptions),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return retry_decorator(func)(*args, **kwargs)
            except RetryError as e:
                raise e.last_attempt.exception()
        
        return wrapper
    return decorator


def retry_connection(
    max_attempts: int = RetryConfig.CONNECTION_MAX_ATTEMPTS,
    wait_seconds: float = RetryConfig.CONNECTION_WAIT_SECONDS,
    max_delay: float = RetryConfig.CONNECTION_MAX_DELAY,
):
    """
    Retry decorator for connection-critical operations.
    
    Uses longer waits and more attempts, suitable for:
    - Database connections
    - External API connections
    - Network-dependent operations
    
    Args:
        max_attempts: Maximum retry attempts
        wait_seconds: Wait time between retries
        max_delay: Maximum total delay before giving up
    
    Usage:
        @retry_connection()
        def connect_to_database():
            return psycopg2.connect(...)
    """
    connection_exceptions = (
        ConnectionError,
        TimeoutError,
        OSError,
    )
    
    if REQUESTS_AVAILABLE:
        connection_exceptions = connection_exceptions + (
            RequestsConnectionError,
            RequestsTimeout,
        )
    
    if not TENACITY_AVAILABLE:
        return _fallback_retry(max_attempts, wait_seconds, connection_exceptions)
    
    def decorator(func: Callable) -> Callable:
        retry_decorator = retry(
            stop=(
                stop_after_attempt(max_attempts) |
                stop_after_delay(max_delay)
            ),
            wait=wait_fixed(wait_seconds) + wait_random(0, 5),
            retry=retry_if_exception_type(connection_exceptions),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return retry_decorator(func)(*args, **kwargs)
            except RetryError as e:
                raise e.last_attempt.exception()
        
        return wrapper
    return decorator


class RetryContext:
    """
    Context manager for retry operations.
    
    Usage:
        with RetryContext(max_attempts=3) as ctx:
            for attempt in ctx:
                try:
                    result = risky_operation()
                    break
                except Exception as e:
                    ctx.handle_exception(e)
    """
    
    def __init__(
        self,
        max_attempts: int = 3,
        wait_seconds: float = 2,
        exceptions: tuple = (Exception,),
    ):
        self.max_attempts = max_attempts
        self.wait_seconds = wait_seconds
        self.exceptions = exceptions
        self.attempt = 0
        self.last_exception = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        return False
    
    def __iter__(self):
        self.attempt = 0
        return self
    
    def __next__(self):
        self.attempt += 1
        if self.attempt > self.max_attempts:
            if self.last_exception:
                raise self.last_exception
            raise StopIteration
        return self.attempt
    
    def handle_exception(self, exception: Exception):
        """Handle an exception during retry loop."""
        self.last_exception = exception
        if self.attempt < self.max_attempts:
            if isinstance(exception, self.exceptions):
                logger.warning(
                    f"Retry {self.attempt}/{self.max_attempts}: {exception}"
                )
                time.sleep(self.wait_seconds * self.attempt)
            else:
                raise exception
        else:
            raise exception


def retry_on_none(
    max_attempts: int = 3,
    wait_seconds: float = 1,
):
    """
    Retry decorator that retries when function returns None.
    
    Useful for operations that may return None on transient failures.
    
    Usage:
        @retry_on_none()
        def find_element(driver, selector):
            try:
                return driver.find_element(By.CSS_SELECTOR, selector)
            except NoSuchElementException:
                return None
    """
    if not TENACITY_AVAILABLE:
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs):
                for attempt in range(1, max_attempts + 1):
                    result = func(*args, **kwargs)
                    if result is not None:
                        return result
                    if attempt < max_attempts:
                        logger.warning(
                            f"Retry {attempt}/{max_attempts} for {func.__name__}: returned None"
                        )
                        time.sleep(wait_seconds * attempt)
                return None
            return wrapper
        return decorator
    
    def decorator(func: Callable) -> Callable:
        retry_decorator = retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_fixed(wait_seconds),
            retry=retry_if_result(lambda x: x is None),
            before_sleep=before_sleep_log(logger, logging.WARNING),
        )
        
        return retry_decorator(func)
    return decorator


# Convenience function for one-off retries
def call_with_retry(
    func: Callable,
    *args,
    max_attempts: int = 3,
    wait_seconds: float = 2,
    exceptions: tuple = (Exception,),
    **kwargs,
) -> Any:
    """
    Call a function with retry logic (one-off, no decorator needed).
    
    Args:
        func: Function to call
        *args: Positional arguments for the function
        max_attempts: Maximum retry attempts
        wait_seconds: Wait between retries
        exceptions: Exceptions to catch and retry
        **kwargs: Keyword arguments for the function
    
    Returns:
        Function result
    
    Usage:
        result = call_with_retry(
            requests.get,
            "https://api.example.com/data",
            max_attempts=5,
            timeout=30
        )
    """
    last_exception = None
    
    for attempt in range(1, max_attempts + 1):
        try:
            return func(*args, **kwargs)
        except exceptions as e:
            last_exception = e
            if attempt < max_attempts:
                logger.warning(
                    f"Retry {attempt}/{max_attempts} for {func.__name__}: {e}"
                )
                time.sleep(wait_seconds * attempt)
            else:
                logger.error(
                    f"All {max_attempts} retries failed for {func.__name__}: {e}"
                )
    
    raise last_exception


# Check availability
def is_tenacity_available() -> bool:
    """Check if tenacity is available."""
    return TENACITY_AVAILABLE


if __name__ == "__main__":
    # Demo/test
    print(f"Tenacity available: {TENACITY_AVAILABLE}")
    print(f"Requests available: {REQUESTS_AVAILABLE}")
    print(f"Selenium available: {SELENIUM_AVAILABLE}")
    
    # Test basic retry
    @with_retry(max_attempts=3, wait_seconds=1)
    def test_function():
        import random
        if random.random() < 0.7:
            raise ValueError("Random failure")
        return "Success!"
    
    try:
        result = test_function()
        print(f"Result: {result}")
    except ValueError as e:
        print(f"Failed after retries: {e}")
