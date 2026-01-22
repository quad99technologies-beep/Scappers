#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Chrome Instance Manager

Tracks and manages Chrome WebDriver instances to ensure they are properly
closed when pipelines stop or complete.

Also provides offline-capable ChromeDriver path resolution.
"""

import logging
import threading
import atexit
import signal
import sys
import os
import glob
import shutil
from pathlib import Path
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


def get_chromedriver_path() -> str:
    """
    Get ChromeDriver path with offline fallback.

    This function tries multiple strategies to find a working ChromeDriver:
    1. Check for cached ChromeDriver from webdriver_manager (works offline)
    2. Try to download using webdriver_manager (requires internet)
    3. Look for chromedriver in system PATH or common locations

    Returns:
        str: Path to the ChromeDriver executable

    Raises:
        RuntimeError: If no ChromeDriver can be found or downloaded
    """
    # Lazy import to avoid circular dependencies
    try:
        from webdriver_manager.chrome import ChromeDriverManager
    except ImportError:
        logger.warning("webdriver_manager not installed, using fallback methods only")
        ChromeDriverManager = None

    # Get the default cache directory used by webdriver_manager
    home = Path.home()
    wdm_cache_dir = home / ".wdm" / "drivers" / "chromedriver"

    def find_cached_chromedriver():
        """Find any cached chromedriver executable"""
        if wdm_cache_dir.exists():
            # Look for chromedriver executables in cache
            patterns = [
                str(wdm_cache_dir / "**" / "chromedriver.exe"),  # Windows
                str(wdm_cache_dir / "**" / "chromedriver"),      # Linux/Mac
            ]
            for pattern in patterns:
                matches = glob.glob(pattern, recursive=True)
                if matches:
                    # Sort by modification time, newest first
                    matches.sort(key=lambda x: os.path.getmtime(x), reverse=True)
                    return matches[0]
        return None

    def find_system_chromedriver():
        """Find chromedriver in system PATH or common locations"""
        # Check if chromedriver is in PATH
        chromedriver_in_path = shutil.which("chromedriver")
        if chromedriver_in_path:
            return chromedriver_in_path

        # Common installation locations on Windows
        common_paths = [
            Path(os.environ.get("LOCALAPPDATA", "")) / "Programs" / "chromedriver.exe",
            Path(os.environ.get("PROGRAMFILES", "")) / "chromedriver" / "chromedriver.exe",
            Path(os.environ.get("PROGRAMFILES(X86)", "")) / "chromedriver" / "chromedriver.exe",
            Path("C:/chromedriver/chromedriver.exe"),
            Path("C:/WebDriver/chromedriver.exe"),
        ]

        for path in common_paths:
            if path.exists():
                return str(path)

        return None

    # Strategy 1: Try cached chromedriver first (works offline)
    cached_path = find_cached_chromedriver()
    if cached_path:
        logger.info(f"[ChromeDriver] Using cached driver: {cached_path}")
        print(f"[ChromeDriver] Using cached driver: {cached_path}")
        return cached_path

    # Strategy 2: Try to download using webdriver_manager
    if ChromeDriverManager is not None:
        try:
            logger.info("[ChromeDriver] No cache found, attempting to download...")
            print("[ChromeDriver] No cache found, attempting to download...")
            driver_path = ChromeDriverManager().install()
            logger.info(f"[ChromeDriver] Downloaded and installed: {driver_path}")
            print(f"[ChromeDriver] Downloaded and installed: {driver_path}")
            return driver_path
        except Exception as e:
            error_msg = str(e).lower()
            if "offline" in error_msg or "connection" in error_msg or "resolve" in error_msg or "network" in error_msg:
                logger.warning(f"[ChromeDriver] Network unavailable: {e}")
                print(f"[ChromeDriver] Network unavailable: {e}")
            else:
                logger.warning(f"[ChromeDriver] Download failed: {e}")
                print(f"[ChromeDriver] Download failed: {e}")

    # Strategy 3: Look for system chromedriver
    system_path = find_system_chromedriver()
    if system_path:
        logger.info(f"[ChromeDriver] Using system driver: {system_path}")
        print(f"[ChromeDriver] Using system driver: {system_path}")
        return system_path

    # All strategies failed
    raise RuntimeError(
        "Could not find or download ChromeDriver.\n"
        "Options to fix:\n"
        "  1. Connect to the internet and retry\n"
        "  2. Download ChromeDriver manually from https://googlechromelabs.github.io/chrome-for-testing/\n"
        "     and place it in your PATH or C:/chromedriver/\n"
        "  3. Run with internet once to cache the driver for offline use"
    )

