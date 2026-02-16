import time
import logging
from typing import Callable, Optional, TypeVar
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException

log = logging.getLogger(__name__)

T = TypeVar('T', bound=webdriver.Remote)

def generic_navigate_with_retry(
    driver: T,
    url: str,
    wait_condition: Optional[Callable[[T], None]] = None,
    retries: int = 3,
    retry_sleep: float = 2.0,
    restart_driver_func: Optional[Callable[[T], T]] = None,
    check_stop_func: Optional[Callable[[], bool]] = None,
    label: str = "Navigation"
) -> T:
    """
    Navigate to URL with retries, optional driver restart, and stop check.
    """
    last_exc = None
    
    def try_nav(d):
        if check_stop_func and check_stop_func():
            raise InterruptedError("Navigation stopped")
        d.get(url)
        if wait_condition:
            wait_condition(d)
    
    # First set of attempts
    for attempt in range(1, retries + 1):
        if check_stop_func and check_stop_func():
             raise InterruptedError("Navigation stopped")
        try:
            try_nav(driver)
            return driver
        except (TimeoutException, WebDriverException) as exc:
            last_exc = exc
            log.warning(f"{label} failed (attempt {attempt}/{retries}). Retrying in {retry_sleep}s...")
            if attempt < retries:
                time.sleep(retry_sleep)
    
    # Restart and retry if enabled
    if restart_driver_func:
        log.warning(f"{label} failed; restarting driver...")
        driver = restart_driver_func(driver)
        
        for attempt in range(1, retries + 1):
            if check_stop_func and check_stop_func():
                raise InterruptedError("Navigation stopped")
            try:
                try_nav(driver)
                return driver
            except (TimeoutException, WebDriverException) as exc:
                last_exc = exc
                if attempt < retries:
                    time.sleep(retry_sleep)
                    
    raise last_exc or RuntimeError(f"{label} failed after retries")
