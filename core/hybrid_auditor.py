"""
Dry-Run Auditor for Hybrid Scraping Architecture

Validates login, selectors, response codes, and output schemas before production runs.
"""

import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union

try:
    from selenium import webdriver
    from selenium.webdriver.remote.webdriver import WebDriver
    from selenium.webdriver.common.by import By
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    WebDriver = None

try:
    from playwright.sync_api import Page, BrowserContext, TimeoutError as PlaywrightTimeoutError
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    Page = None

logger = logging.getLogger(__name__)


class ValidationStatus(Enum):
    """Validation result status."""
    PASS = "PASS"
    FAIL = "FAIL"
    WARNING = "WARNING"
    SKIP = "SKIP"


@dataclass
class ValidationResult:
    """Result of a validation check."""
    check_name: str
    status: ValidationStatus
    message: str
    details: Optional[Dict] = None
    duration_seconds: float = 0.0
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            "check_name": self.check_name,
            "status": self.status.value,
            "message": self.message,
            "details": self.details or {},
            "duration_seconds": self.duration_seconds
        }


class HybridAuditor:
    """
    Dry-run auditor for hybrid scraping architecture.
    
    Responsibilities:
    - Validate login success
    - Validate selector presence
    - Validate HTTP response codes
    - Validate output schema
    - Generate audit reports
    """
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        self.results: List[ValidationResult] = []
    
    def validate_login(
        self,
        browser: Union[WebDriver, Page],
        login_url: str,
        success_indicators: List[str],
        timeout: int = 30
    ) -> ValidationResult:
        """
        Validate login success.
        
        Args:
            browser: Browser instance
            login_url: Login page URL
            success_indicators: List of selectors/text that indicate successful login
            timeout: Timeout in seconds
            
        Returns:
            ValidationResult
        """
        check_name = "Login Validation"
        start_time = time.time()
        
        try:
            self.logger.info(f"[AUDIT] Validating login: {login_url}")
            
            # Navigate to login page
            if isinstance(browser, Page):
                browser.goto(login_url, timeout=timeout * 1000)
                page_content = browser.content()
            else:
                browser.get(login_url)
                page_content = browser.page_source
            
            # Check for success indicators
            found_indicators = []
            for indicator in success_indicators:
                if isinstance(browser, Page):
                    try:
                        element = browser.query_selector(indicator)
                        if element and element.is_visible():
                            found_indicators.append(indicator)
                    except:
                        if indicator.lower() in page_content.lower():
                            found_indicators.append(indicator)
                else:
                    try:
                        elements = browser.find_elements(By.CSS_SELECTOR, indicator)
                        if elements and any(e.is_displayed() for e in elements):
                            found_indicators.append(indicator)
                    except:
                        if indicator.lower() in page_content.lower():
                            found_indicators.append(indicator)
            
            duration = time.time() - start_time
            
            if found_indicators:
                status = ValidationStatus.PASS
                message = f"Login successful: found {len(found_indicators)}/{len(success_indicators)} indicators"
                details = {"found_indicators": found_indicators, "all_indicators": success_indicators}
            else:
                status = ValidationStatus.FAIL
                message = f"Login failed: none of {len(success_indicators)} indicators found"
                details = {"all_indicators": success_indicators}
            
            result = ValidationResult(
                check_name=check_name,
                status=status,
                message=message,
                details=details,
                duration_seconds=duration
            )
            
            self.results.append(result)
            self.logger.info(f"[AUDIT] {check_name}: {status.value} - {message}")
            return result
            
        except Exception as e:
            duration = time.time() - start_time
            result = ValidationResult(
                check_name=check_name,
                status=ValidationStatus.FAIL,
                message=f"Login validation error: {str(e)}",
                details={"error": str(e)},
                duration_seconds=duration
            )
            self.results.append(result)
            self.logger.error(f"[AUDIT] {check_name}: FAIL - {result.message}")
            return result
    
    def validate_selectors(
        self,
        browser: Union[WebDriver, Page],
        url: str,
        selectors: Dict[str, str],  # {name: selector}
        timeout: int = 10
    ) -> ValidationResult:
        """
        Validate selector presence on page.
        
        Args:
            browser: Browser instance
            url: URL to check
            selectors: Dictionary of {name: selector} pairs
            timeout: Timeout in seconds
            
        Returns:
            ValidationResult
        """
        check_name = "Selector Validation"
        start_time = time.time()
        
        try:
            self.logger.info(f"[AUDIT] Validating selectors on: {url}")
            
            # Navigate to page
            if isinstance(browser, Page):
                browser.goto(url, timeout=timeout * 1000)
            else:
                browser.get(url)
            
            # Check each selector
            found_selectors = {}
            missing_selectors = {}
            
            for name, selector in selectors.items():
                try:
                    if isinstance(browser, Page):
                        element = browser.query_selector(selector)
                        found = element is not None and element.is_visible()
                    else:
                        elements = browser.find_elements(By.CSS_SELECTOR, selector)
                        found = len(elements) > 0 and any(e.is_displayed() for e in elements)
                    
                    if found:
                        found_selectors[name] = selector
                    else:
                        missing_selectors[name] = selector
                except Exception as e:
                    missing_selectors[name] = f"{selector} (error: {str(e)})"
            
            duration = time.time() - start_time
            
            if not missing_selectors:
                status = ValidationStatus.PASS
                message = f"All {len(selectors)} selectors found"
            elif len(found_selectors) > 0:
                status = ValidationStatus.WARNING
                message = f"Found {len(found_selectors)}/{len(selectors)} selectors, {len(missing_selectors)} missing"
            else:
                status = ValidationStatus.FAIL
                message = f"No selectors found: {len(missing_selectors)}/{len(selectors)} missing"
            
            result = ValidationResult(
                check_name=check_name,
                status=status,
                message=message,
                details={
                    "found": found_selectors,
                    "missing": missing_selectors,
                    "total": len(selectors)
                },
                duration_seconds=duration
            )
            
            self.results.append(result)
            self.logger.info(f"[AUDIT] {check_name}: {status.value} - {message}")
            return result
            
        except Exception as e:
            duration = time.time() - start_time
            result = ValidationResult(
                check_name=check_name,
                status=ValidationStatus.FAIL,
                message=f"Selector validation error: {str(e)}",
                details={"error": str(e)},
                duration_seconds=duration
            )
            self.results.append(result)
            self.logger.error(f"[AUDIT] {check_name}: FAIL - {result.message}")
            return result
    
    def validate_response_codes(
        self,
        http_client: Any,  # HybridHttpClient
        urls: List[Tuple[str, int]],  # [(url, expected_status_code), ...]
    ) -> ValidationResult:
        """
        Validate HTTP response status codes.
        
        Args:
            http_client: HybridHttpClient instance
            urls: List of (url, expected_status_code) tuples
            
        Returns:
            ValidationResult
        """
        check_name = "Response Code Validation"
        start_time = time.time()
        
        try:
            self.logger.info(f"[AUDIT] Validating {len(urls)} response codes")
            
            results = {}
            passed = 0
            failed = 0
            
            for url, expected_code in urls:
                try:
                    response = http_client.get(url, timeout=10)
                    actual_code = response.status_code if hasattr(response, 'status_code') else None
                    
                    if actual_code == expected_code:
                        results[url] = {"status": "PASS", "code": actual_code}
                        passed += 1
                    else:
                        results[url] = {
                            "status": "FAIL",
                            "expected": expected_code,
                            "actual": actual_code
                        }
                        failed += 1
                except Exception as e:
                    results[url] = {"status": "ERROR", "error": str(e)}
                    failed += 1
            
            duration = time.time() - start_time
            
            if failed == 0:
                status = ValidationStatus.PASS
                message = f"All {len(urls)} URLs returned expected status codes"
            elif passed > 0:
                status = ValidationStatus.WARNING
                message = f"{passed}/{len(urls)} URLs passed, {failed} failed"
            else:
                status = ValidationStatus.FAIL
                message = f"All {len(urls)} URLs failed validation"
            
            result = ValidationResult(
                check_name=check_name,
                status=status,
                message=message,
                details={"results": results, "passed": passed, "failed": failed},
                duration_seconds=duration
            )
            
            self.results.append(result)
            self.logger.info(f"[AUDIT] {check_name}: {status.value} - {message}")
            return result
            
        except Exception as e:
            duration = time.time() - start_time
            result = ValidationResult(
                check_name=check_name,
                status=ValidationStatus.FAIL,
                message=f"Response code validation error: {str(e)}",
                details={"error": str(e)},
                duration_seconds=duration
            )
            self.results.append(result)
            self.logger.error(f"[AUDIT] {check_name}: FAIL - {result.message}")
            return result
    
    def validate_output_schema(
        self,
        file_path: str,
        expected_columns: List[str],
        min_rows: int = 1
    ) -> ValidationResult:
        """
        Validate output CSV/JSON schema.
        
        Args:
            file_path: Path to output file
            expected_columns: List of expected column names
            min_rows: Minimum number of rows expected
            
        Returns:
            ValidationResult
        """
        check_name = "Output Schema Validation"
        start_time = time.time()
        
        try:
            from pathlib import Path
            import pandas as pd
            
            path = Path(file_path)
            if not path.exists():
                result = ValidationResult(
                    check_name=check_name,
                    status=ValidationStatus.FAIL,
                    message=f"Output file not found: {file_path}",
                    duration_seconds=time.time() - start_time
                )
                self.results.append(result)
                return result
            
            # Read file
            if path.suffix == '.csv':
                df = pd.read_csv(path)
            elif path.suffix == '.json':
                df = pd.read_json(path)
            else:
                raise ValueError(f"Unsupported file format: {path.suffix}")
            
            duration = time.time() - start_time
            
            # Check columns
            missing_columns = set(expected_columns) - set(df.columns)
            extra_columns = set(df.columns) - set(expected_columns)
            row_count = len(df)
            
            if missing_columns:
                status = ValidationStatus.FAIL
                message = f"Missing columns: {list(missing_columns)}"
            elif row_count < min_rows:
                status = ValidationStatus.WARNING
                message = f"Row count {row_count} below minimum {min_rows}"
            else:
                status = ValidationStatus.PASS
                message = f"Schema valid: {len(expected_columns)} columns, {row_count} rows"
            
            result = ValidationResult(
                check_name=check_name,
                status=status,
                message=message,
                details={
                    "expected_columns": expected_columns,
                    "actual_columns": list(df.columns),
                    "missing_columns": list(missing_columns),
                    "extra_columns": list(extra_columns),
                    "row_count": row_count,
                    "min_rows": min_rows
                },
                duration_seconds=duration
            )
            
            self.results.append(result)
            self.logger.info(f"[AUDIT] {check_name}: {status.value} - {message}")
            return result
            
        except Exception as e:
            duration = time.time() - start_time
            result = ValidationResult(
                check_name=check_name,
                status=ValidationStatus.FAIL,
                message=f"Schema validation error: {str(e)}",
                details={"error": str(e)},
                duration_seconds=duration
            )
            self.results.append(result)
            self.logger.error(f"[AUDIT] {check_name}: FAIL - {result.message}")
            return result
    
    def get_summary(self) -> Dict:
        """Get audit summary."""
        total = len(self.results)
        passed = sum(1 for r in self.results if r.status == ValidationStatus.PASS)
        failed = sum(1 for r in self.results if r.status == ValidationStatus.FAIL)
        warnings = sum(1 for r in self.results if r.status == ValidationStatus.WARNING)
        
        return {
            "total_checks": total,
            "passed": passed,
            "failed": failed,
            "warnings": warnings,
            "success_rate": (passed / total * 100) if total > 0 else 0
        }
    
    def generate_report(self) -> str:
        """Generate human-readable audit report."""
        lines = []
        lines.append("=" * 80)
        lines.append("HYBRID SCRAPER AUDIT REPORT")
        lines.append("=" * 80)
        lines.append("")
        
        summary = self.get_summary()
        lines.append(f"Total Checks: {summary['total_checks']}")
        lines.append(f"Passed: {summary['passed']}")
        lines.append(f"Failed: {summary['failed']}")
        lines.append(f"Warnings: {summary['warnings']}")
        lines.append(f"Success Rate: {summary['success_rate']:.1f}%")
        lines.append("")
        lines.append("-" * 80)
        
        for result in self.results:
            status_icon = {
                ValidationStatus.PASS: "✓",
                ValidationStatus.FAIL: "✗",
                ValidationStatus.WARNING: "⚠",
                ValidationStatus.SKIP: "⊘"
            }.get(result.status, "?")
            
            lines.append(f"{status_icon} {result.check_name}: {result.status.value}")
            lines.append(f"  {result.message}")
            if result.details:
                for key, value in result.details.items():
                    lines.append(f"    {key}: {value}")
            lines.append(f"  Duration: {result.duration_seconds:.2f}s")
            lines.append("")
        
        lines.append("=" * 80)
        return "\n".join(lines)
    
    def clear_results(self):
        """Clear all validation results."""
        self.results.clear()
