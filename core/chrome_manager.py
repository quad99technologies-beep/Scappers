#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Chrome Instance Manager

Tracks and manages Chrome WebDriver instances to ensure they are properly
closed when pipelines stop or complete.
"""

import logging
import threading
import atexit
import signal
import sys
from typing import Set, Optional
from weakref import WeakSet

logger = logging.getLogger(__name__)

class ChromeManager:
    """Manages Chrome WebDriver instances across the application"""
    
    _instance: Optional['ChromeManager'] = None
    _lock = threading.Lock()
    
    def __init__(self):
        """Initialize the Chrome manager"""
        self._drivers: WeakSet = WeakSet()
        self._lock = threading.Lock()
        self._registered = False
        
    @classmethod
    def get_instance(cls) -> 'ChromeManager':
        """Get the singleton instance of ChromeManager"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
                    cls._instance._register_cleanup_handlers()
        return cls._instance
    
    def _register_cleanup_handlers(self):
        """Register cleanup handlers for process termination"""
        if self._registered:
            return
        
        self._registered = True
        
        # Register atexit handler
        atexit.register(self.cleanup_all)
        
        # Register signal handlers for graceful shutdown
        if sys.platform != "win32":
            # Unix-like systems
            signal.signal(signal.SIGTERM, self._signal_handler)
            signal.signal(signal.SIGINT, self._signal_handler)
        else:
            # Windows - use signal handlers that are available
            try:
                signal.signal(signal.SIGINT, self._signal_handler)
                signal.signal(signal.SIGTERM, self._signal_handler)
            except (ValueError, AttributeError):
                # Some signals may not be available on Windows
                pass
    
    def _signal_handler(self, signum, frame):
        """Handle termination signals"""
        logger.info(f"Received signal {signum}, cleaning up Chrome instances...")
        self.cleanup_all()
        # Re-raise the signal to allow normal termination
        signal.signal(signum, signal.SIG_DFL)
        signal.raise_signal(signum)
    
    def register_driver(self, driver):
        """
        Register a Chrome WebDriver instance for tracking.
        
        Args:
            driver: Selenium WebDriver instance to track
        """
        with self._lock:
            self._drivers.add(driver)
            logger.debug(f"Registered Chrome driver (total: {len(self._drivers)})")
    
    def unregister_driver(self, driver):
        """
        Unregister a Chrome WebDriver instance.
        
        Args:
            driver: Selenium WebDriver instance to unregister
        """
        with self._lock:
            # WeakSet automatically removes dead references, but we can try to remove explicitly
            try:
                self._drivers.discard(driver)
                logger.debug(f"Unregistered Chrome driver (remaining: {len(self._drivers)})")
            except (KeyError, TypeError):
                pass
    
    def cleanup_all(self, silent: bool = False):
        """
        Close all registered Chrome WebDriver instances.
        
        Args:
            silent: If True, suppress error messages
        """
        with self._lock:
            drivers_to_close = list(self._drivers)
            count = len(drivers_to_close)
            
            if count == 0:
                if not silent:
                    logger.debug("No Chrome instances to close")
                return
            
            if not silent:
                logger.info(f"Closing {count} Chrome instance(s)...")
            
            closed_count = 0
            for driver in drivers_to_close:
                try:
                    if driver is not None:
                        driver.quit()
                        closed_count += 1
                except Exception as e:
                    if not silent:
                        logger.warning(f"Error closing Chrome instance: {e}")
            
            # Clear the set
            self._drivers.clear()
            
            if not silent:
                logger.info(f"Closed {closed_count}/{count} Chrome instance(s)")
    
    def get_driver_count(self) -> int:
        """Get the number of currently registered drivers"""
        with self._lock:
            return len(self._drivers)
    
    def cleanup_driver(self, driver):
        """
        Close and unregister a specific Chrome WebDriver instance.
        
        Args:
            driver: Selenium WebDriver instance to close
        """
        try:
            if driver is not None:
                driver.quit()
        except Exception as e:
            logger.warning(f"Error closing Chrome instance: {e}")
        finally:
            self.unregister_driver(driver)


# Convenience functions for easy access
def register_chrome_driver(driver):
    """Register a Chrome WebDriver instance"""
    ChromeManager.get_instance().register_driver(driver)


def unregister_chrome_driver(driver):
    """Unregister a Chrome WebDriver instance"""
    ChromeManager.get_instance().unregister_driver(driver)


def cleanup_all_chrome_instances(silent: bool = False):
    """Close all registered Chrome WebDriver instances"""
    ChromeManager.get_instance().cleanup_all(silent=silent)


def get_chrome_driver_count() -> int:
    """Get the number of currently registered Chrome drivers"""
    return ChromeManager.get_instance().get_driver_count()

