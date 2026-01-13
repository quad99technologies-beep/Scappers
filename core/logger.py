#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Standardized Logger Module

Provides consistent logging format across all scrapers.
Format: [{level}] [{scraper}] [{step}] [thread-{id}] {message}

Business Logic Unchanged: Only standardizes logging format, no parsing/selectors changed.
"""

import logging
import sys
import threading
from typing import Optional
from pathlib import Path


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
    class StandardFormatter(logging.Formatter):
        """Custom formatter that includes thread ID"""
        def format(self, record):
            # Add thread ID to record
            record.thread_id = threading.get_ident()
            
            # Build prefix parts
            prefix_parts = [f"[{record.levelname}]"]
            if hasattr(record, 'scraper_name') and record.scraper_name:
                prefix_parts.append(f"[{record.scraper_name}]")
            if hasattr(record, 'step_name') and record.step_name:
                prefix_parts.append(f"[{record.step_name}]")
            prefix_parts.append(f"[thread-{record.thread_id}]")
            
            prefix = " ".join(prefix_parts)
            record.msg = f"{prefix} {record.getMessage()}"
            
            return super().format(record)
    
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
