#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data Validation Module - Netherlands

Validates scraped data before insertion into database to ensure data quality.

Features:
- URL validation
- Price validation
- Date validation
- Text sanitization
- Data completeness checks
"""

import re
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from urllib.parse import urlparse


class ValidationError(Exception):
    """Raised when data validation fails."""
    pass


class DataValidator:
    """Validates scraped data for Netherlands scraper."""
    
    # Validation rules
    MAX_TEXT_LENGTH = 1000
    MAX_URL_LENGTH = 2000
    VALID_CURRENCIES = ["EUR", "€"]
    VALID_REIMBURSEMENT_STATUSES = [
        "Reimbursed",
        "Not reimbursed",
        "Fully reimbursed",
        "Partially reimbursed",
        "Reimbursed with conditions",
        "Unknown",
        ""
    ]
    
    def __init__(self, strict_mode: bool = False):
        """
        Initialize validator.
        
        Args:
            strict_mode: If True, raise exceptions on validation failures.
                        If False, log warnings and sanitize data.
        """
        self.strict_mode = strict_mode
        self.validation_errors: List[str] = []
    
    def reset_errors(self) -> None:
        """Reset validation error list."""
        self.validation_errors = []
    
    def get_errors(self) -> List[str]:
        """Get all validation errors."""
        return self.validation_errors.copy()
    
    def _add_error(self, error: str) -> None:
        """Add validation error."""
        self.validation_errors.append(error)
        if self.strict_mode:
            raise ValidationError(error)
    
    def validate_url(self, url: str, field_name: str = "url") -> str:
        """
        Validate and sanitize URL.
        
        Args:
            url: URL to validate
            field_name: Name of the field (for error messages)
        
        Returns:
            Sanitized URL
        """
        if not url:
            return ""
        
        url = url.strip()
        
        # Check length
        if len(url) > self.MAX_URL_LENGTH:
            self._add_error(f"{field_name}: URL too long ({len(url)} > {self.MAX_URL_LENGTH})")
            url = url[:self.MAX_URL_LENGTH]
        
        # Validate URL structure
        try:
            parsed = urlparse(url)
            if not parsed.scheme:
                self._add_error(f"{field_name}: Missing URL scheme")
            if not parsed.netloc:
                self._add_error(f"{field_name}: Missing URL domain")
        except Exception as e:
            self._add_error(f"{field_name}: Invalid URL format: {e}")
        
        return url
    
    def validate_price(self, price: str, field_name: str = "price") -> str:
        """
        Validate and sanitize price value.
        
        Args:
            price: Price string to validate
            field_name: Name of the field (for error messages)
        
        Returns:
            Sanitized price string
        """
        if not price:
            return ""
        
        price = price.strip()
        
        # Remove currency symbols for validation
        price_clean = price.replace("€", "").replace("EUR", "").strip()
        
        # Check if it's a valid number
        try:
            # Handle European format (comma as decimal separator)
            price_normalized = price_clean.replace(".", "").replace(",", ".")
            price_float = float(price_normalized)
            
            # Validate range (prices should be positive and reasonable)
            if price_float < 0:
                self._add_error(f"{field_name}: Negative price not allowed: {price}")
                return "0.00"
            
            if price_float > 100000:  # Sanity check: max 100k EUR
                self._add_error(f"{field_name}: Price too high (> 100000): {price}")
            
            # Return normalized format (2 decimal places)
            return f"{price_float:.2f}"
        
        except (ValueError, AttributeError) as e:
            self._add_error(f"{field_name}: Invalid price format: {price} ({e})")
            return ""
    
    def validate_date(self, date_str: str, field_name: str = "date") -> str:
        """
        Validate and sanitize date string.
        
        Args:
            date_str: Date string to validate (dd-mm-YYYY format)
            field_name: Name of the field (for error messages)
        
        Returns:
            Sanitized date string
        """
        if not date_str:
            return ""
        
        date_str = date_str.strip()
        
        # Try to parse date
        try:
            # Expected format: dd-mm-YYYY
            parsed_date = datetime.strptime(date_str, "%d-%m-%Y")
            
            # Validate date is not in future
            if parsed_date > datetime.now():
                self._add_error(f"{field_name}: Future date not allowed: {date_str}")
            
            # Validate date is not too old (e.g., before 2000)
            if parsed_date.year < 2000:
                self._add_error(f"{field_name}: Date too old: {date_str}")
            
            return date_str
        
        except ValueError as e:
            self._add_error(f"{field_name}: Invalid date format (expected dd-mm-YYYY): {date_str} ({e})")
            return ""
    
    def validate_text(self, text: str, field_name: str = "text", max_length: Optional[int] = None) -> str:
        """
        Validate and sanitize text field.
        
        Args:
            text: Text to validate
            field_name: Name of the field (for error messages)
            max_length: Maximum allowed length (default: MAX_TEXT_LENGTH)
        
        Returns:
            Sanitized text
        """
        if not text:
            return ""
        
        text = text.strip()
        max_len = max_length or self.MAX_TEXT_LENGTH
        
        # Check length
        if len(text) > max_len:
            self._add_error(f"{field_name}: Text too long ({len(text)} > {max_len})")
            text = text[:max_len]
        
        # Remove control characters
        text = re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]', '', text)
        
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    def validate_percentage(self, percent: str, field_name: str = "percentage") -> str:
        """
        Validate and sanitize percentage value.
        
        Args:
            percent: Percentage string to validate (e.g., "10%", "10.5%")
            field_name: Name of the field (for error messages)
        
        Returns:
            Sanitized percentage string
        """
        if not percent:
            return "0%"
        
        percent = percent.strip()
        
        # Extract numeric value
        match = re.search(r'(\d+(?:[.,]\d+)?)', percent)
        if not match:
            self._add_error(f"{field_name}: Invalid percentage format: {percent}")
            return "0%"
        
        try:
            value = float(match.group(1).replace(",", "."))
            
            # Validate range
            if value < 0:
                self._add_error(f"{field_name}: Negative percentage not allowed: {percent}")
                return "0%"
            
            if value > 100:
                self._add_error(f"{field_name}: Percentage > 100%: {percent}")
            
            return f"{value:.1f}%"
        
        except ValueError as e:
            self._add_error(f"{field_name}: Invalid percentage value: {percent} ({e})")
            return "0%"
    
    def validate_reimbursement_status(self, status: str, field_name: str = "reimbursement_status") -> str:
        """
        Validate reimbursement status.
        
        Args:
            status: Status to validate
            field_name: Name of the field (for error messages)
        
        Returns:
            Validated status
        """
        if not status:
            return ""
        
        status = status.strip()
        
        # Check if status is in valid list
        if status not in self.VALID_REIMBURSEMENT_STATUSES:
            self._add_error(f"{field_name}: Invalid reimbursement status: {status}")
            # Try to map to valid status
            status_lower = status.lower()
            if "not" in status_lower and "reimb" in status_lower:
                return "Not reimbursed"
            elif "reimb" in status_lower:
                return "Reimbursed"
            else:
                return "Unknown"
        
        return status
    
    def validate_collected_url(self, data: Dict) -> Dict:
        """
        Validate collected URL record.
        
        Args:
            data: Dictionary containing collected URL data
        
        Returns:
            Validated and sanitized data dictionary
        """
        self.reset_errors()
        
        validated = {}
        
        # Required fields
        validated['prefix'] = self.validate_text(data.get('prefix', ''), 'prefix', max_length=100)
        validated['url'] = self.validate_url(data.get('url', ''), 'url')
        validated['url_with_id'] = self.validate_url(data.get('url_with_id', ''), 'url_with_id')
        
        # Optional fields
        validated['title'] = self.validate_text(data.get('title', ''), 'title', max_length=500)
        validated['active_substance'] = self.validate_text(data.get('active_substance', ''), 'active_substance', max_length=200)
        validated['manufacturer'] = self.validate_text(data.get('manufacturer', ''), 'manufacturer', max_length=200)
        validated['document_type'] = self.validate_text(data.get('document_type', ''), 'document_type', max_length=100)
        validated['price_text'] = self.validate_text(data.get('price_text', ''), 'price_text', max_length=100)
        validated['reimbursement'] = self.validate_text(data.get('reimbursement', ''), 'reimbursement', max_length=200)
        validated['packs_scraped'] = data.get('packs_scraped', 'pending')
        validated['error'] = self.validate_text(data.get('error', ''), 'error', max_length=500)
        
        return validated
    
    def validate_pack_data(self, data: Dict) -> Dict:
        """
        Validate pack data record.
        
        Args:
            data: Dictionary containing pack data
        
        Returns:
            Validated and sanitized data dictionary
        """
        self.reset_errors()
        
        validated = {}
        
        # Dates
        validated['start_date'] = self.validate_date(data.get('start_date', ''), 'start_date')
        validated['end_date'] = self.validate_date(data.get('end_date', ''), 'end_date')
        
        # Currency
        currency = data.get('currency', 'EUR')
        if currency not in self.VALID_CURRENCIES:
            self._add_error(f"Invalid currency: {currency}")
            currency = 'EUR'
        validated['currency'] = currency
        
        # Prices
        validated['unit_price'] = self.validate_price(data.get('unit_price', ''), 'unit_price')
        validated['ppp_ex_vat'] = self.validate_price(data.get('ppp_ex_vat', ''), 'ppp_ex_vat')
        validated['ppp_vat'] = self.validate_price(data.get('ppp_vat', ''), 'ppp_vat')
        validated['copay_price'] = self.validate_price(data.get('copay_price', ''), 'copay_price')
        
        # Percentages
        validated['vat_percent'] = self.validate_percentage(data.get('vat_percent', '9'), 'vat_percent')
        validated['reimbursable_rate'] = self.validate_percentage(data.get('reimbursable_rate', '0%'), 'reimbursable_rate')
        validated['copay_percent'] = self.validate_percentage(data.get('copay_percent', '0%'), 'copay_percent')
        
        # Reimbursement
        validated['reimbursable_status'] = self.validate_reimbursement_status(
            data.get('reimbursable_status', ''), 'reimbursable_status'
        )
        
        # Text fields
        validated['margin_rule'] = self.validate_text(data.get('margin_rule', ''), 'margin_rule', max_length=200)
        validated['local_pack_description'] = self.validate_text(
            data.get('local_pack_description', ''), 'local_pack_description', max_length=500
        )
        validated['formulation'] = self.validate_text(data.get('formulation', ''), 'formulation', max_length=200)
        validated['strength_size'] = self.validate_text(data.get('strength_size', ''), 'strength_size', max_length=200)
        validated['local_pack_code'] = self.validate_text(data.get('local_pack_code', ''), 'local_pack_code', max_length=100)
        validated['reimbursement_message'] = self.validate_text(
            data.get('reimbursement_message', ''), 'reimbursement_message', max_length=1000
        )
        
        # URL
        validated['source_url'] = self.validate_url(data.get('source_url', ''), 'source_url')
        
        return validated
    
    def check_data_completeness(self, data: Dict, required_fields: List[str]) -> Tuple[bool, List[str]]:
        """
        Check if all required fields are present and non-empty.
        
        Args:
            data: Data dictionary to check
            required_fields: List of required field names
        
        Returns:
            Tuple of (is_complete, missing_fields)
        """
        missing = []
        
        for field in required_fields:
            value = data.get(field, '')
            if not value or (isinstance(value, str) and not value.strip()):
                missing.append(field)
        
        return (len(missing) == 0, missing)


# Singleton instance for easy import
validator = DataValidator(strict_mode=False)


def validate_collected_url(data: Dict) -> Dict:
    """Convenience function to validate collected URL data."""
    return validator.validate_collected_url(data)


def validate_pack_data(data: Dict) -> Dict:
    """Convenience function to validate pack data."""
    return validator.validate_pack_data(data)


def get_validation_errors() -> List[str]:
    """Get validation errors from last validation."""
    return validator.get_errors()
