"""
Smart Locator Module for Canada Quebec Scraper

Provides intelligent, accessibility-first element location with automatic fallback.
No LLM, MCP, vision, or OCR - purely deterministic rule-based selection.

Features:
- Accessibility-first selectors (get_by_role, get_by_label, get_by_text)
- Smart fallback engine when primary selectors fail
- DOM change awareness via hash-based detection
- Rule-based anomaly detection
- Comprehensive metrics and logging
"""

import hashlib
import logging
import re
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

try:
    from playwright.sync_api import Page, Locator, TimeoutError as PlaywrightTimeoutError
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    Page = None
    Locator = None

try:
    from selenium.webdriver.remote.webdriver import WebDriver
    from selenium.webdriver.remote.webelement import WebElement
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    WebDriver = None
    WebElement = None


# Constants
MIN_TEXT_SIMILARITY_THRESHOLD = 0.6  # Minimum similarity for text matching
DOM_HASH_CHECK_INTERVAL = 1.0  # Seconds between DOM hash checks
STABLE_COUNT_REQUIRED = 3  # Number of stable checks before considering DOM stable
EMPTY_TABLE_THRESHOLD = 0  # Minimum rows for non-empty table
CSV_SIZE_THRESHOLD = 100  # Minimum bytes for valid CSV


class SelectorType(Enum):
    """Types of selectors, ordered by preference (accessibility-first)."""
    ROLE = "role"
    LABEL = "label"
    TEXT = "text"
    PLACEHOLDER = "placeholder"
    TEST_ID = "test_id"
    CSS = "css"
    XPATH = "xpath"


@dataclass
class SelectorCandidate:
    """Represents a candidate element for selection."""
    element: Any  # Playwright Locator or Selenium WebElement
    selector_type: SelectorType
    score: float
    method_used: str
    fallback_reason: Optional[str] = None


@dataclass
class LocatorMetrics:
    """Metrics for tracking locator performance."""
    primary_success: int = 0
    fallback_success: int = 0
    fallback_failures: int = 0
    dom_changes_detected: int = 0
    anomalies_detected: int = 0

    def get_summary(self) -> Dict[str, int]:
        """Get summary of metrics."""
        total_attempts = self.primary_success + self.fallback_success + self.fallback_failures
        return {
            "total_attempts": total_attempts,
            "primary_success": self.primary_success,
            "fallback_success": self.fallback_success,
            "fallback_failures": self.fallback_failures,
            "dom_changes": self.dom_changes_detected,
            "anomalies": self.anomalies_detected,
        }


class SmartLocator:
    """
    Intelligent element locator with accessibility-first approach and automatic fallback.

    Supports both Playwright and Selenium, preferring Playwright for better accessibility APIs.
    """

    def __init__(self, page_or_driver: Union[Page, WebDriver], logger: Optional[logging.Logger] = None):
        """
        Initialize SmartLocator.

        Args:
            page_or_driver: Playwright Page or Selenium WebDriver instance
            logger: Optional logger instance (creates one if not provided)
        """
        self.page = page_or_driver if PLAYWRIGHT_AVAILABLE and isinstance(page_or_driver, Page) else None
        self.driver = page_or_driver if SELENIUM_AVAILABLE and isinstance(page_or_driver, WebDriver) else None

        if not self.page and not self.driver:
            raise ValueError("Must provide either Playwright Page or Selenium WebDriver")

        self.logger = logger or logging.getLogger(__name__)
        self.metrics = LocatorMetrics()
        self._dom_hashes: Dict[str, Tuple[str, float]] = {}  # section -> (hash, timestamp)
        self._last_hash_check: float = 0.0

    def _normalize_text(self, text: str) -> str:
        """Normalize text for comparison (case-insensitive, whitespace normalized)."""
        return re.sub(r'\s+', ' ', text.strip().lower())

    def _text_similarity(self, text1: str, text2: str) -> float:
        """
        Calculate simple text similarity score (0.0 to 1.0).
        Uses word overlap and substring matching.
        """
        norm1 = self._normalize_text(text1)
        norm2 = self._normalize_text(text2)

        if norm1 == norm2:
            return 1.0

        # Exact substring match
        if norm1 in norm2 or norm2 in norm1:
            return 0.9

        # Word overlap
        words1 = set(norm1.split())
        words2 = set(norm2.split())
        if not words1 or not words2:
            return 0.0

        intersection = words1 & words2
        union = words1 | words2
        if union:
            return len(intersection) / len(union)

        return 0.0

    def _hash_dom_section(self, selector: str, section_name: str = "default") -> str:
        """
        Generate hash of DOM section for change detection.

        Args:
            selector: CSS selector for the section to hash
            section_name: Name identifier for this section
        """
        try:
            if self.page:
                # Playwright: get HTML content
                try:
                    element = self.page.query_selector(selector)
                    if element:
                        html = element.inner_html()
                    else:
                        html = self.page.content()
                except Exception:
                    html = self.page.content()
            else:
                # Selenium: get HTML source
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    html = element.get_attribute('outerHTML') if element else ""
                except Exception:
                    html = self.driver.page_source

            # Generate hash
            hash_obj = hashlib.md5(html.encode('utf-8', errors='ignore'))
            return hash_obj.hexdigest()
        except Exception as e:
            self.logger.debug(f"Error hashing DOM section {section_name}: {e}")
            return ""

    def detect_dom_change(self, selector: str, section_name: str = "default") -> bool:
        """
        Detect if DOM section has changed since last check.

        Args:
            selector: CSS selector for section to monitor
            section_name: Name identifier for this section

        Returns:
            True if DOM changed, False otherwise
        """
        current_time = time.time()

        # Throttle checks
        if current_time - self._last_hash_check < DOM_HASH_CHECK_INTERVAL:
            return False

        self._last_hash_check = current_time

        current_hash = self._hash_dom_section(selector, section_name)

        if section_name in self._dom_hashes:
            previous_hash, _ = self._dom_hashes[section_name]
            if current_hash != previous_hash:
                self.metrics.dom_changes_detected += 1
                self.logger.info(f"[LOCATOR] [DOM_CHANGE] Detected change in section '{section_name}'")
                self._dom_hashes[section_name] = (current_hash, current_time)
                return True

        self._dom_hashes[section_name] = (current_hash, current_time)
        return False

    def _find_candidates_playwright(self,
                                   role: Optional[str] = None,
                                   label: Optional[str] = None,
                                   text: Optional[str] = None,
                                   placeholder: Optional[str] = None,
                                   test_id: Optional[str] = None,
                                   css: Optional[str] = None,
                                   xpath: Optional[str] = None) -> List[SelectorCandidate]:
        """Find candidate elements using Playwright with accessibility-first approach."""
        candidates = []

        try:
            # 1. Try role-based (highest priority)
            if role:
                try:
                    locator = self.page.get_by_role(role, name=label or text, exact=False)
                    if locator.count() > 0:
                        for i in range(min(locator.count(), 10)):  # Limit to first 10
                            element = locator.nth(i)
                            if element.is_visible():
                                score = 1.0
                                if label or text:
                                    try:
                                        element_text = element.inner_text()
                                        score = self._text_similarity(label or text or "", element_text)
                                    except Exception:
                                        pass
                                candidates.append(SelectorCandidate(
                                    element=element,
                                    selector_type=SelectorType.ROLE,
                                    score=score,
                                    method_used=f"get_by_role('{role}')"
                                ))
                except Exception as e:
                    self.logger.debug(f"Role selector failed: {e}")

            # 2. Try label-based
            if label:
                try:
                    locator = self.page.get_by_label(label, exact=False)
                    if locator.count() > 0:
                        for i in range(min(locator.count(), 10)):
                            element = locator.nth(i)
                            if element.is_visible():
                                candidates.append(SelectorCandidate(
                                    element=element,
                                    selector_type=SelectorType.LABEL,
                                    score=0.95,
                                    method_used=f"get_by_label('{label}')"
                                ))
                except Exception as e:
                    self.logger.debug(f"Label selector failed: {e}")

            # 3. Try text-based
            if text:
                try:
                    locator = self.page.get_by_text(text, exact=False)
                    if locator.count() > 0:
                        for i in range(min(locator.count(), 10)):
                            element = locator.nth(i)
                            if element.is_visible():
                                element_text = element.inner_text()
                                similarity = self._text_similarity(text, element_text)
                                if similarity >= MIN_TEXT_SIMILARITY_THRESHOLD:
                                    candidates.append(SelectorCandidate(
                                        element=element,
                                        selector_type=SelectorType.TEXT,
                                        score=similarity,
                                        method_used=f"get_by_text('{text}')"
                                    ))
                except Exception as e:
                    self.logger.debug(f"Text selector failed: {e}")

            # 4. Try placeholder
            if placeholder:
                try:
                    locator = self.page.get_by_placeholder(placeholder, exact=False)
                    if locator.count() > 0:
                        for i in range(min(locator.count(), 10)):
                            element = locator.nth(i)
                            if element.is_visible():
                                candidates.append(SelectorCandidate(
                                    element=element,
                                    selector_type=SelectorType.PLACEHOLDER,
                                    score=0.85,
                                    method_used=f"get_by_placeholder('{placeholder}')"
                                ))
                except Exception as e:
                    self.logger.debug(f"Placeholder selector failed: {e}")

            # 5. Try test ID
            if test_id:
                try:
                    locator = self.page.get_by_test_id(test_id)
                    if locator.count() > 0:
                        for i in range(min(locator.count(), 10)):
                            element = locator.nth(i)
                            if element.is_visible():
                                candidates.append(SelectorCandidate(
                                    element=element,
                                    selector_type=SelectorType.TEST_ID,
                                    score=0.9,
                                    method_used=f"get_by_test_id('{test_id}')"
                                ))
                except Exception as e:
                    self.logger.debug(f"Test ID selector failed: {e}")

            # 6. Try CSS (fallback)
            if css:
                try:
                    elements = self.page.query_selector_all(css)
                    for element in elements[:10]:  # Limit to first 10
                        if element.is_visible():
                            candidates.append(SelectorCandidate(
                                element=element,
                                selector_type=SelectorType.CSS,
                                score=0.7,
                                method_used=f"CSS selector: '{css}'"
                            ))
                except Exception as e:
                    self.logger.debug(f"CSS selector failed: {e}")

            # 7. Try XPath (last resort)
            if xpath:
                try:
                    elements = self.page.query_selector_all(f"xpath={xpath}")
                    for element in elements[:10]:
                        if element.is_visible():
                            candidates.append(SelectorCandidate(
                                element=element,
                                selector_type=SelectorType.XPATH,
                                score=0.6,
                                method_used=f"XPath: '{xpath}'"
                            ))
                except Exception as e:
                    self.logger.debug(f"XPath selector failed: {e}")

        except Exception as e:
            self.logger.debug(f"Error finding candidates: {e}")

        return candidates

    def _find_candidates_selenium(self,
                                  role: Optional[str] = None,
                                  label: Optional[str] = None,
                                  text: Optional[str] = None,
                                  placeholder: Optional[str] = None,
                                  test_id: Optional[str] = None,
                                  css: Optional[str] = None,
                                  xpath: Optional[str] = None) -> List[SelectorCandidate]:
        """Find candidate elements using Selenium (limited accessibility support)."""
        candidates = []

        try:
            # Selenium has limited accessibility support, so we use attribute-based matching

            # 1. Try by label text (find label, then associated input)
            if label:
                try:
                    labels = self.driver.find_elements(By.XPATH, f"//label[contains(text(), '{label}')]")
                    for label_elem in labels[:10]:
                        try:
                            # Get associated input via 'for' attribute or parent
                            label_for = label_elem.get_attribute('for')
                            if label_for:
                                input_elem = self.driver.find_element(By.ID, label_for)
                                if input_elem.is_displayed():
                                    candidates.append(SelectorCandidate(
                                        element=input_elem,
                                        selector_type=SelectorType.LABEL,
                                        score=0.95,
                                        method_used=f"Label association: '{label}'"
                                    ))
                        except Exception:
                            pass
                except Exception as e:
                    self.logger.debug(f"Label selector failed: {e}")

            # 2. Try by text content
            if text:
                try:
                    elements = self.driver.find_elements(By.XPATH, f"//*[contains(text(), '{text}')]")
                    for elem in elements[:10]:
                        if elem.is_displayed():
                            elem_text = elem.text
                            similarity = self._text_similarity(text, elem_text)
                            if similarity >= MIN_TEXT_SIMILARITY_THRESHOLD:
                                candidates.append(SelectorCandidate(
                                    element=elem,
                                    selector_type=SelectorType.TEXT,
                                    score=similarity,
                                    method_used=f"Text content: '{text}'"
                                ))
                except Exception as e:
                    self.logger.debug(f"Text selector failed: {e}")

            # 3. Try by placeholder
            if placeholder:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, f"[placeholder*='{placeholder}']")
                    for elem in elements[:10]:
                        if elem.is_displayed():
                            candidates.append(SelectorCandidate(
                                element=elem,
                                selector_type=SelectorType.PLACEHOLDER,
                                score=0.85,
                                method_used=f"Placeholder: '{placeholder}'"
                            ))
                except Exception as e:
                    self.logger.debug(f"Placeholder selector failed: {e}")

            # 4. Try by test ID
            if test_id:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, f"[data-testid='{test_id}']")
                    for elem in elements[:10]:
                        if elem.is_displayed():
                            candidates.append(SelectorCandidate(
                                element=elem,
                                selector_type=SelectorType.TEST_ID,
                                score=0.9,
                                method_used=f"Test ID: '{test_id}'"
                            ))
                except Exception as e:
                    self.logger.debug(f"Test ID selector failed: {e}")

            # 5. Try CSS
            if css:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, css)
                    for elem in elements[:10]:
                        if elem.is_displayed():
                            candidates.append(SelectorCandidate(
                                element=elem,
                                selector_type=SelectorType.CSS,
                                score=0.7,
                                method_used=f"CSS: '{css}'"
                            ))
                except Exception as e:
                    self.logger.debug(f"CSS selector failed: {e}")

            # 6. Try XPath
            if xpath:
                try:
                    elements = self.driver.find_elements(By.XPATH, xpath)
                    for elem in elements[:10]:
                        if elem.is_displayed():
                            candidates.append(SelectorCandidate(
                                element=elem,
                                selector_type=SelectorType.XPATH,
                                score=0.6,
                                method_used=f"XPath: '{xpath}'"
                            ))
                except Exception as e:
                    self.logger.debug(f"XPath selector failed: {e}")

        except Exception as e:
            self.logger.debug(f"Error finding candidates: {e}")

        return candidates

    def find_element(self,
                    role: Optional[str] = None,
                    label: Optional[str] = None,
                    text: Optional[str] = None,
                    placeholder: Optional[str] = None,
                    test_id: Optional[str] = None,
                    css: Optional[str] = None,
                    xpath: Optional[str] = None,
                    timeout: float = 5.0,
                    required: bool = True) -> Optional[Any]:
        """
        Find element using accessibility-first approach with automatic fallback.

        Args:
            role: ARIA role (e.g., 'button', 'textbox', 'link')
            label: Label text or aria-label
            text: Visible text content
            placeholder: Placeholder text
            test_id: data-testid attribute value
            css: CSS selector (fallback)
            xpath: XPath selector (last resort)
            timeout: Maximum time to wait for element
            required: If True, raises exception when not found; if False, returns None

        Returns:
            Element (Locator or WebElement) or None if not found and not required
        """
        start_time = time.time()

        # Try primary selector first
        try:
            if self.page:
                # Playwright: try accessibility-first
                if role:
                    try:
                        locator = self.page.get_by_role(role, name=label or text, exact=False)
                        locator.wait_for(state="visible", timeout=int(timeout * 1000))
                        self.metrics.primary_success += 1
                        self.logger.debug(f"[LOCATOR] Found via primary role selector: {role}")
                        return locator.first if locator.count() > 0 else locator
                    except Exception:
                        pass

                if label:
                    try:
                        locator = self.page.get_by_label(label, exact=False)
                        locator.wait_for(state="visible", timeout=int(timeout * 1000))
                        self.metrics.primary_success += 1
                        self.logger.debug(f"[LOCATOR] Found via primary label selector: {label}")
                        return locator.first if locator.count() > 0 else locator
                    except Exception:
                        pass

                if text:
                    try:
                        locator = self.page.get_by_text(text, exact=False)
                        locator.wait_for(state="visible", timeout=int(timeout * 1000))
                        self.metrics.primary_success += 1
                        self.logger.debug(f"[LOCATOR] Found via primary text selector: {text}")
                        return locator.first if locator.count() > 0 else locator
                    except Exception:
                        pass
            else:
                # Selenium: try CSS/XPath directly (limited accessibility support)
                if css:
                    try:
                        wait = WebDriverWait(self.driver, timeout)
                        element = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, css)))
                        self.metrics.primary_success += 1
                        self.logger.debug(f"[LOCATOR] Found via primary CSS selector: {css}")
                        return element
                    except Exception:
                        pass

                if xpath:
                    try:
                        wait = WebDriverWait(self.driver, timeout)
                        element = wait.until(EC.visibility_of_element_located((By.XPATH, xpath)))
                        self.metrics.primary_success += 1
                        self.logger.debug(f"[LOCATOR] Found via primary XPath selector: {xpath}")
                        return element
                    except Exception:
                        pass

        except Exception as e:
            self.logger.debug(f"[LOCATOR] Primary selector failed: {e}")

        # Primary failed, try fallback
        # Only log at INFO if required, otherwise DEBUG to reduce noise
        if required:
            self.logger.info(f"[LOCATOR] Primary selector failed, trying fallback strategies...")
        else:
            self.logger.debug(f"[LOCATOR] Primary selector failed, trying fallback strategies...")

        elapsed = time.time() - start_time
        remaining_timeout = max(0, timeout - elapsed)

        if remaining_timeout > 0:
            # Find all candidates
            if self.page:
                candidates = self._find_candidates_playwright(
                    role=role, label=label, text=text, placeholder=placeholder,
                    test_id=test_id, css=css, xpath=xpath
                )
            else:
                candidates = self._find_candidates_selenium(
                    role=role, label=label, text=text, placeholder=placeholder,
                    test_id=test_id, css=css, xpath=xpath
                )

            # Sort by score (highest first)
            candidates.sort(key=lambda c: c.score, reverse=True)

            # Try candidates in order
            for candidate in candidates:
                try:
                    if self.page:
                        # Playwright: check if visible and enabled
                        if candidate.element.is_visible():
                            self.metrics.fallback_success += 1
                            log_level = self.logger.info if required else self.logger.debug
                            log_level(f"[LOCATOR] Found via fallback: {candidate.method_used} (score: {candidate.score:.2f})")
                            return candidate.element
                    else:
                        # Selenium: check if displayed and enabled
                        if candidate.element.is_displayed() and candidate.element.is_enabled():
                            self.metrics.fallback_success += 1
                            log_level = self.logger.info if required else self.logger.debug
                            log_level(f"[LOCATOR] Found via fallback: {candidate.method_used} (score: {candidate.score:.2f})")
                            return candidate.element
                except Exception as e:
                    self.logger.debug(f"[LOCATOR] Candidate failed: {e}")
                    continue

        # All fallbacks failed
        self.metrics.fallback_failures += 1
        error_msg = f"Could not find element with any selector strategy"

        if required:
            raise ValueError(error_msg)
        else:
            # For optional elements, log at DEBUG level to reduce noise
            self.logger.debug(f"[LOCATOR] {error_msg}")
            return None

    def detect_anomalies(self,
                        table_selector: Optional[str] = None,
                        csv_path: Optional[Path] = None,
                        error_text_patterns: Optional[List[str]] = None) -> List[str]:
        """
        Detect anomalies in page state or data.

        Args:
            table_selector: CSS selector for table to check
            csv_path: Path to CSV file to check
            error_text_patterns: List of error text patterns to search for

        Returns:
            List of detected anomaly descriptions
        """
        anomalies = []

        try:
            # Priority: Check CSV file content first (most reliable after download)
            # CSV file check is more reliable than DOM table check because:
            # - DOM table might be hidden/cleared after CSV download
            # - CSV file contains the actual data that was downloaded
            csv_has_data = False
            if csv_path and csv_path.exists():
                file_size = csv_path.stat().st_size
                # Check CSV file size first
                if file_size < CSV_SIZE_THRESHOLD:
                    anomalies.append(f"CSV file too small: {file_size} bytes (threshold: {CSV_SIZE_THRESHOLD})")
                    self.metrics.anomalies_detected += 1
                else:
                    # CSV has reasonable size - check if it has actual data rows
                    try:
                        import pandas as pd
                        # Read CSV with pandas to check row count
                        df = pd.read_csv(csv_path, nrows=10)  # Read first 10 rows to verify data exists
                        row_count = len(df)

                        # Check if we have data rows (not just header)
                        if row_count > 1:
                            # Multiple rows = has data
                            csv_has_data = True
                        elif row_count == 1:
                            # Only one row - check if it's header-only or has actual data
                            first_row = df.iloc[0]
                            non_empty_cols = sum(1 for val in first_row if pd.notna(val) and str(val).strip() and str(val).strip().lower() not in ['', 'nan', 'none'])
                            # If at least 2 columns have meaningful non-header data, consider it valid
                            if non_empty_cols >= 2:
                                # Check if values look like data (not just column headers)
                                # Simple heuristic: if values are numeric or have length > 5, likely data
                                data_like_values = sum(1 for val in first_row if pd.notna(val) and (
                                    str(val).strip().isdigit() or len(str(val).strip()) > 5
                                ))
                                if data_like_values >= 1:
                                    csv_has_data = True
                        # If row_count == 0, csv_has_data stays False
                    except Exception as e:
                        self.logger.debug(f"Error reading CSV for anomaly check: {e}")
                        # If we can't read CSV but it has size, assume it might have data
                        # Use a higher threshold to avoid false positives
                        csv_has_data = (file_size > CSV_SIZE_THRESHOLD * 5)

            # Only check DOM table if CSV not available or CSV check indicates no data
            # DOM table check is less reliable - table might be hidden after CSV download
            if table_selector and (not csv_path or not csv_path.exists() or not csv_has_data):
                try:
                    if self.page:
                        rows = self.page.query_selector_all(f"{table_selector} tbody tr, {table_selector} tr")
                        row_count = len([r for r in rows if r.is_visible()])
                    else:
                        rows = self.driver.find_elements(By.CSS_SELECTOR, f"{table_selector} tbody tr, {table_selector} tr")
                        row_count = len([r for r in rows if r.is_displayed()])

                    if row_count <= EMPTY_TABLE_THRESHOLD:
                        # Only report empty table if CSV also doesn't have data
                        # (DOM might be hidden but CSV might still have data)
                        if not csv_has_data:
                            anomalies.append(f"Empty table detected: {row_count} rows (threshold: {EMPTY_TABLE_THRESHOLD})")
                            self.metrics.anomalies_detected += 1
                except Exception as e:
                    self.logger.debug(f"Error checking table: {e}")

            # Check for error text on page
            if error_text_patterns:
                try:
                    if self.page:
                        page_text = self.page.inner_text("body").lower()
                    else:
                        page_text = self.driver.find_element(By.TAG_NAME, "body").text.lower()

                    for pattern in error_text_patterns:
                        if pattern.lower() in page_text:
                            anomalies.append(f"Error text detected on page: '{pattern}'")
                            self.metrics.anomalies_detected += 1
                except Exception as e:
                    self.logger.debug(f"Error checking page text: {e}")

        except Exception as e:
            self.logger.debug(f"Error in anomaly detection: {e}")

        return anomalies

    def get_metrics(self) -> LocatorMetrics:
        """Get current metrics."""
        return self.metrics

    def reset_metrics(self):
        """Reset metrics counters."""
        self.metrics = LocatorMetrics()
