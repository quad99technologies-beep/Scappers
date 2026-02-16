#!/usr/bin/env python3
"""
Response Validation Module for the Scraping Platform.

This module provides validation utilities for:
- HTML content validation
- Cloudflare/captcha detection
- Required element checking
- Content quality assessment
"""

import re
import logging
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)


# =============================================================================
# Validation Result
# =============================================================================

@dataclass
class ValidationResult:
    """Result of content validation."""
    is_valid: bool
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    details: Optional[Dict] = None
    
    def __bool__(self):
        return self.is_valid


# =============================================================================
# Block Detection Patterns
# =============================================================================

CLOUDFLARE_PATTERNS = [
    r"cf-browser-verification",
    r"Checking your browser",
    r"Enable JavaScript and cookies to continue",
    r"Just a moment\.\.\.",
    r"_cf_chl_opt",
    r"cf-spinner",
    r"Attention Required! \| Cloudflare",
    r"Ray ID:",
    r"cf-error-type",
    r"challenge-form",
]

CAPTCHA_PATTERNS = [
    r"class=['\"]?g-recaptcha['\"]?",
    r"class=['\"]?h-captcha['\"]?",
    r"class=['\"]?cf-turnstile['\"]?",
    r"data-sitekey=",
    r"grecaptcha\.render",
    r"hcaptcha\.render",
    r"arkose",
    r"funcaptcha",
]

BLOCK_PATTERNS = [
    r"<title>Access Denied</title>",
    r"<title>403 Forbidden</title>",
    r"<title>401 Unauthorized</title>",
    r"Request blocked",
    r"Too Many Requests",
    r"Rate limit exceeded",
    r"Your IP has been blocked",
    r"Unusual traffic detected",
    r"bot detection",
    r"automated access",
]

ERROR_PAGE_PATTERNS = [
    r"<title>404 Not Found</title>",
    r"<title>500 Internal Server Error</title>",
    r"<title>502 Bad Gateway</title>",
    r"<title>503 Service Unavailable</title>",
    r"Page not found",
    r"Server Error",
    r"Something went wrong",
]


# =============================================================================
# Validators
# =============================================================================

def detect_cloudflare(content: str) -> bool:
    """Detect Cloudflare challenge page."""
    for pattern in CLOUDFLARE_PATTERNS:
        if re.search(pattern, content, re.IGNORECASE):
            return True
    return False


def detect_captcha(content: str) -> bool:
    """Detect captcha challenge."""
    for pattern in CAPTCHA_PATTERNS:
        if re.search(pattern, content, re.IGNORECASE):
            return True
    return False


def detect_block(content: str) -> bool:
    """Detect blocked/rate-limited response."""
    for pattern in BLOCK_PATTERNS:
        if re.search(pattern, content, re.IGNORECASE):
            return True
    return False


def detect_error_page(content: str) -> bool:
    """Detect error pages."""
    for pattern in ERROR_PAGE_PATTERNS:
        if re.search(pattern, content, re.IGNORECASE):
            return True
    return False


def is_html(content: str) -> bool:
    """Check if content is HTML."""
    if not content:
        return False
    content_lower = content.lower().strip()
    return (
        content_lower.startswith("<!doctype html") or
        content_lower.startswith("<html") or
        "<html" in content_lower[:1000]
    )


def count_elements(content: str, tag: str) -> int:
    """Count occurrences of an HTML tag."""
    pattern = rf"<{tag}[\s>]"
    return len(re.findall(pattern, content, re.IGNORECASE))


def has_element(content: str, selector: str) -> bool:
    """Check if content has an element matching the selector."""
    try:
        soup = BeautifulSoup(content, "html.parser")
        return soup.select_one(selector) is not None
    except Exception:
        return False


def get_title(content: str) -> Optional[str]:
    """Extract page title."""
    try:
        soup = BeautifulSoup(content, "html.parser")
        title_tag = soup.find("title")
        return title_tag.get_text(strip=True) if title_tag else None
    except Exception:
        return None


# =============================================================================
# Main Validation Function
# =============================================================================

def validate_html(
    content: str,
    min_length: int = 1000,
    max_length: Optional[int] = None,
    required_elements: Optional[List[str]] = None,
    required_selectors: Optional[List[str]] = None,
    min_link_count: int = 0,
    check_cloudflare: bool = True,
    check_captcha: bool = True,
    check_block: bool = True,
    check_error: bool = True,
    custom_validators: Optional[List[callable]] = None
) -> ValidationResult:
    """
    Comprehensive HTML content validation.
    
    Args:
        content: HTML content to validate
        min_length: Minimum content length
        max_length: Maximum content length (optional)
        required_elements: List of required elements (e.g., ["<html", "<body"])
        required_selectors: List of required CSS selectors (e.g., ["table.results", "#main"])
        min_link_count: Minimum number of links expected
        check_cloudflare: Check for Cloudflare challenge
        check_captcha: Check for captcha
        check_block: Check for blocked response
        check_error: Check for error pages
        custom_validators: List of custom validation functions
        
    Returns:
        ValidationResult with details
    """
    details = {}
    
    # Empty content check
    if not content:
        return ValidationResult(
            is_valid=False,
            error_code="empty_content",
            error_message="Response content is empty"
        )
    
    # Length checks
    content_length = len(content)
    details["content_length"] = content_length
    
    if content_length < min_length:
        return ValidationResult(
            is_valid=False,
            error_code="content_too_short",
            error_message=f"Content length {content_length} < minimum {min_length}",
            details=details
        )
    
    if max_length and content_length > max_length:
        return ValidationResult(
            is_valid=False,
            error_code="content_too_long",
            error_message=f"Content length {content_length} > maximum {max_length}",
            details=details
        )
    
    # HTML check
    if not is_html(content):
        return ValidationResult(
            is_valid=False,
            error_code="not_html",
            error_message="Content does not appear to be HTML",
            details=details
        )
    
    # Block detection
    if check_cloudflare and detect_cloudflare(content):
        return ValidationResult(
            is_valid=False,
            error_code="cloudflare_challenge",
            error_message="Cloudflare challenge page detected",
            details=details
        )
    
    if check_captcha and detect_captcha(content):
        return ValidationResult(
            is_valid=False,
            error_code="captcha",
            error_message="Captcha challenge detected",
            details=details
        )
    
    if check_block and detect_block(content):
        return ValidationResult(
            is_valid=False,
            error_code="blocked",
            error_message="Access blocked or rate limited",
            details=details
        )
    
    if check_error and detect_error_page(content):
        return ValidationResult(
            is_valid=False,
            error_code="error_page",
            error_message="Error page detected",
            details=details
        )
    
    # Required elements check
    if required_elements:
        content_lower = content.lower()
        for element in required_elements:
            if element.lower() not in content_lower:
                return ValidationResult(
                    is_valid=False,
                    error_code="missing_element",
                    error_message=f"Required element not found: {element}",
                    details=details
                )
    
    # Required selectors check
    if required_selectors:
        for selector in required_selectors:
            if not has_element(content, selector):
                return ValidationResult(
                    is_valid=False,
                    error_code="missing_selector",
                    error_message=f"Required selector not found: {selector}",
                    details=details
                )
    
    # Link count check
    if min_link_count > 0:
        link_count = count_elements(content, "a")
        details["link_count"] = link_count
        if link_count < min_link_count:
            return ValidationResult(
                is_valid=False,
                error_code="insufficient_links",
                error_message=f"Link count {link_count} < minimum {min_link_count}",
                details=details
            )
    
    # Custom validators
    if custom_validators:
        for validator in custom_validators:
            try:
                result = validator(content)
                if not result:
                    return ValidationResult(
                        is_valid=False,
                        error_code="custom_validation_failed",
                        error_message=f"Custom validator {validator.__name__} failed",
                        details=details
                    )
            except Exception as e:
                log.warning(f"Custom validator {validator.__name__} raised exception: {e}")
    
    # All checks passed
    details["title"] = get_title(content)
    
    return ValidationResult(
        is_valid=True,
        details=details
    )


def quick_validate(content: str, min_length: int = 1000) -> Tuple[bool, Optional[str]]:
    """
    Quick validation for common use cases.
    
    Args:
        content: HTML content
        min_length: Minimum content length
        
    Returns:
        (is_valid, error_reason)
    """
    result = validate_html(content, min_length=min_length)
    return result.is_valid, result.error_code
