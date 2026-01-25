#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Standardized Logger Module

Provides consistent logging format across all scrapers.
Format: [{level}] [{scraper}] [{step}] [thread-{id}] {message}

Business Logic Unchanged: Only standardizes logging format, no parsing/selectors changed.
"""

import logging
import os
import re
import sys
import threading
from typing import Optional, List
from pathlib import Path


def _collect_sensitive_values() -> List[str]:
    keywords = ("TOKEN", "PASSWORD", "SECRET", "API_KEY", "ACCESS_KEY", "PRIVATE_KEY")
    values = []
    for key, value in os.environ.items():
        if any(k in key.upper() for k in keywords):
            if value and isinstance(value, str) and len(value) >= 4:
                values.append(value)
    values = sorted(set(values), key=len, reverse=True)
    return values


def setup_standard_logger(
    name: str,
    scraper_name: Optional[str] = None,
    log_file: Optional[Path] = None,
    level: int = logging.INFO
) -> logging.Logger:
    """
    Setup a standardized logger with consistent format.
    
    Args:
        name: Logger name (usually __name__)
        scraper_name: Optional scraper name for log prefix
        log_file: Optional log file path (if None, only console)
        level: Logging level (default: INFO)
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Standard formatter: [{level}] [{scraper}] [{step}] [thread-{id}] {message}
    sensitive_values = _collect_sensitive_values()

    class StandardFormatter(logging.Formatter):
        """Custom formatter that includes thread ID."""
        def format(self, record):
            record.thread_id = threading.get_ident()

            prefix_parts = [f"[{record.levelname}]"]
            if getattr(record, "scraper_name", None):
                prefix_parts.append(f"[{record.scraper_name}]")
            if getattr(record, "step_name", None):
                prefix_parts.append(f"[{record.step_name}]")
            prefix_parts.append(f"[thread-{record.thread_id}]")
            prefix = " ".join(prefix_parts)

            formatted = record.getMessage()
            if sensitive_values:
                for secret in sensitive_values:
                    if secret in formatted:
                        formatted = formatted.replace(secret, "***")
            original_msg = record.msg
            original_args = record.args
            try:
                record.msg = f"{prefix} {formatted}"
                record.args = ()
                return super().format(record)
            finally:
                record.msg = original_msg
                record.args = original_args
    
    # File handler (if log_file provided)
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(level)
        file_formatter = StandardFormatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    # Console handler (always)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_formatter = StandardFormatter('%(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # Add scraper_name to logger if provided
    if scraper_name:
        logger.scraper_name = scraper_name
    
    return logger


def get_logger(name: str, scraper_name: Optional[str] = None) -> logging.Logger:
    """
    Get or create a logger instance.
    If logger doesn't exist, creates one with standard setup.
    
    Args:
        name: Logger name (usually __name__)
        scraper_name: Optional scraper name for log prefix
    
    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)
    
    # If logger has no handlers, setup standard handlers
    if not logger.handlers:
        logger = setup_standard_logger(name, scraper_name=scraper_name)
    
    return logger


# Convenience function for backward compatibility
def setup_logger(name: str, log_file: Optional[Path] = None, level: int = logging.INFO) -> logging.Logger:
    """
    Setup logger with standard format (backward compatibility).
    
    Args:
        name: Logger name
        log_file: Optional log file path
        level: Logging level
    
    Returns:
        Configured logger instance
    """
    return setup_standard_logger(name, log_file=log_file, level=level)
