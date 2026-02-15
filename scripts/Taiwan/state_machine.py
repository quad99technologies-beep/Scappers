"""
State Machine Module for Taiwan Scraper

Provides deterministic state-based navigation with explicit state validation.
No probabilistic behavior - all transitions are rule-based and verifiable.

Features:
- Explicit state definitions
- State validation with required elements
- Safe state transitions
- Retry logic on state validation failure
- Comprehensive logging
"""

import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from smart_locator import SmartLocator


class NavigationState(Enum):
    """Explicit states for navigation flow."""
    INITIAL = "initial"
    PAGE_LOADED = "page_loaded"
    SEARCH_READY = "search_ready"
    RESULTS_LOADING = "results_loading"
    RESULTS_READY = "results_ready"
    CSV_READY = "csv_ready"
    DETAIL_READY = "detail_ready"
    TABLE_READY = "table_ready"
    GRID_READY = "grid_ready"
    PAGER_READY = "pager_ready"
    ERROR = "error"


@dataclass
class StateCondition:
    """Defines a condition that must be met for a state to be valid."""
    element_selector: Optional[str] = None  # CSS/XPath selector
    role: Optional[str] = None  # ARIA role
    label: Optional[str] = None  # Label text
    text: Optional[str] = None  # Visible text
    min_count: int = 1  # Minimum number of elements required
    max_wait: float = 5.0  # Maximum wait time for condition
    custom_check: Optional[Callable[[Any], bool]] = None  # Custom validation function


@dataclass
class StateDefinition:
    """Defines a navigation state with its required conditions."""
    state: NavigationState
    required_conditions: List[StateCondition]
    description: str
    retry_on_failure: bool = True
    max_retries: int = 3
    retry_delay: float = 2.0


class NavigationStateMachine:
    """
    State machine for managing scraper navigation flow.

    Ensures deterministic transitions by validating state conditions before proceeding.
    """

    def __init__(self,
                 locator: SmartLocator,
                 logger: Optional[logging.Logger] = None):
        """
        Initialize state machine.

        Args:
            locator: SmartLocator instance for element finding
            logger: Optional logger instance
        """
        self.locator = locator
        self.logger = logger or logging.getLogger(__name__)
        self.current_state = NavigationState.INITIAL
        self.state_history: List[Tuple[NavigationState, float, bool]] = []  # (state, timestamp, success)
        self.state_definitions: Dict[NavigationState, StateDefinition] = {}
        self._setup_default_states()

    def _setup_default_states(self):
        """Setup default state definitions."""
        # PAGE_LOADED: Page has loaded and basic structure is present
        self.state_definitions[NavigationState.PAGE_LOADED] = StateDefinition(
            state=NavigationState.PAGE_LOADED,
            required_conditions=[
                StateCondition(element_selector="body", min_count=1, max_wait=10.0)
            ],
            description="Page has loaded with body element present",
            retry_on_failure=True,
            max_retries=3
        )

        # SEARCH_READY: Search form is ready for input
        self.state_definitions[NavigationState.SEARCH_READY] = StateDefinition(
            state=NavigationState.SEARCH_READY,
            required_conditions=[
                StateCondition(role="combobox", min_count=1, max_wait=10.0),
                StateCondition(role="textbox", min_count=1, max_wait=10.0),
                StateCondition(role="button", text="search", min_count=1, max_wait=10.0)
            ],
            description="Search form is ready (dropdown, input, button visible)",
            retry_on_failure=True,
            max_retries=3
        )

        # RESULTS_LOADING: Results are being loaded
        self.state_definitions[NavigationState.RESULTS_LOADING] = StateDefinition(
            state=NavigationState.RESULTS_LOADING,
            required_conditions=[
                StateCondition(
                    custom_check=lambda page: self._check_loading_indicator(page),
                    max_wait=30.0
                )
            ],
            description="Results are loading (loading indicator present)",
            retry_on_failure=False,
            max_retries=1
        )

        # RESULTS_READY: Results table is ready
        self.state_definitions[NavigationState.RESULTS_READY] = StateDefinition(
            state=NavigationState.RESULTS_READY,
            required_conditions=[
                StateCondition(element_selector="table", min_count=1, max_wait=15.0),
                StateCondition(
                    custom_check=lambda page: self._check_table_has_data(page),
                    max_wait=10.0
                )
            ],
            description="Results table is ready with data",
            retry_on_failure=True,
            max_retries=3
        )

        # CSV_READY: CSV download button is ready
        self.state_definitions[NavigationState.CSV_READY] = StateDefinition(
            state=NavigationState.CSV_READY,
            required_conditions=[
                StateCondition(role="button", text="csv", min_count=1, max_wait=10.0),
                StateCondition(
                    custom_check=lambda page: self._check_button_enabled(page, "csv"),
                    max_wait=5.0
                )
            ],
            description="CSV download button is visible and enabled",
            retry_on_failure=True,
            max_retries=3
        )

        # DETAIL_READY: Detail page table is ready
        self.state_definitions[NavigationState.DETAIL_READY] = StateDefinition(
            state=NavigationState.DETAIL_READY,
            required_conditions=[
                StateCondition(element_selector="table", min_count=1, max_wait=15.0),
                StateCondition(
                    custom_check=lambda page: self._check_table_has_data(page),
                    max_wait=10.0
                )
            ],
            description="Detail page table is ready with data",
            retry_on_failure=True,
            max_retries=3
        )

        # TABLE_READY: Generic table is ready
        self.state_definitions[NavigationState.TABLE_READY] = StateDefinition(
            state=NavigationState.TABLE_READY,
            required_conditions=[
                StateCondition(element_selector="table", min_count=1, max_wait=15.0),
                StateCondition(
                    custom_check=lambda page: self._check_table_has_data(page),
                    max_wait=10.0
                )
            ],
            description="Table is ready with data",
            retry_on_failure=True,
            max_retries=3
        )

        # GRID_READY: Telerik grid is ready (specific to North Macedonia)
        self.state_definitions[NavigationState.GRID_READY] = StateDefinition(
            state=NavigationState.GRID_READY,
            required_conditions=[
                StateCondition(element_selector="div#grid table", min_count=1, max_wait=20.0),
                StateCondition(element_selector="div.t-data-grid-pager", min_count=1, max_wait=20.0)
            ],
            description="Telerik grid is ready with table and pager",
            retry_on_failure=True,
            max_retries=3
        )

        # PAGER_READY: Pager controls are ready
        self.state_definitions[NavigationState.PAGER_READY] = StateDefinition(
            state=NavigationState.PAGER_READY,
            required_conditions=[
                StateCondition(element_selector="div.t-data-grid-pager", min_count=1, max_wait=15.0)
            ],
            description="Pager controls are ready",
            retry_on_failure=True,
            max_retries=3
        )

    def _check_loading_indicator(self, page_or_driver: Any) -> bool:
        """Check if loading indicator is present."""
        try:
            if hasattr(page_or_driver, 'inner_text'):  # Playwright
                body_text = page_or_driver.inner_text("body").lower()
                return "loading" in body_text or "please wait" in body_text
            else:  # Selenium
                from selenium.webdriver.common.by import By
                body_elem = page_or_driver.find_element(By.TAG_NAME, "body")
                body_text = body_elem.text.lower()
                return "loading" in body_text or "please wait" in body_text
        except Exception:
            return False

    def _check_table_has_data(self, page_or_driver: Any) -> bool:
        """Check if table has data rows."""
        try:
            if hasattr(page_or_driver, 'query_selector_all'):  # Playwright
                rows = page_or_driver.query_selector_all("table tbody tr, table tr")
                visible_rows = [r for r in rows if r.is_visible()]
                # Check if at least one row has meaningful content
                for row in visible_rows[:5]:
                    text = row.inner_text().strip()
                    if len(text) > 10:
                        return True
                return False
            else:  # Selenium
                from selenium.webdriver.common.by import By
                rows = page_or_driver.find_elements(By.CSS_SELECTOR, "table tbody tr, table tr")
                visible_rows = [r for r in rows if r.is_displayed()]
                for row in visible_rows[:5]:
                    text = row.text.strip()
                    if len(text) > 10:
                        return True
                return False
        except Exception:
            return False

    def _check_button_enabled(self, page_or_driver: Any, button_text: str) -> bool:
        """Check if button with given text is enabled."""
        try:
            if hasattr(page_or_driver, 'get_by_role'):  # Playwright
                button = page_or_driver.get_by_role("button", name=button_text, exact=False)
                if button.count() > 0:
                    return not button.first.is_disabled()
                return False
            else:  # Selenium
                from selenium.webdriver.common.by import By
                buttons = page_or_driver.find_elements(By.XPATH, f"//button[contains(text(), '{button_text}')]")
                for btn in buttons:
                    if btn.is_displayed() and btn.is_enabled():
                        return True
                return False
        except Exception:
            return False

    def validate_state(self, target_state: NavigationState,
                      custom_conditions: Optional[List[StateCondition]] = None) -> bool:
        """
        Validate that current page state matches target state.

        Args:
            target_state: State to validate
            custom_conditions: Optional custom conditions to override defaults

        Returns:
            True if state is valid, False otherwise
        """
        if target_state not in self.state_definitions:
            self.logger.warning(f"[STATE] No definition for state: {target_state}")
            return False

        state_def = self.state_definitions[target_state]
        conditions = custom_conditions or state_def.required_conditions

        # Log validation at DEBUG level to reduce noise (only log failures at INFO/WARNING)
        self.logger.debug(f"[STATE] Validating state: {target_state.value} - {state_def.description}")

        for condition in conditions:
            try:
                # Custom check
                if condition.custom_check:
                    page_or_driver = self.locator.page or self.locator.driver
                    if not condition.custom_check(page_or_driver):
                        self.logger.debug(f"[STATE] Custom check failed for {target_state.value}")
                        return False
                    continue

                # Element-based check
                element = self.locator.find_element(
                    role=condition.role,
                    label=condition.label,
                    text=condition.text,
                    css=condition.element_selector,
                    timeout=condition.max_wait,
                    required=False
                )

                if element is None:
                    self.logger.debug(f"[STATE] Element not found: {condition.element_selector or condition.role}")
                    return False

                # Check count if needed
                if condition.min_count > 1:
                    # For multiple elements, we'd need to count
                    # This is simplified - in practice, you'd query for all matching elements
                    pass

            except Exception as e:
                self.logger.debug(f"[STATE] Condition validation error: {e}")
                return False

        # Log successful validation at DEBUG level to reduce noise
        self.logger.debug(f"[STATE] State validated: {target_state.value}")
        return True

    def transition_to(self,
                     target_state: NavigationState,
                     custom_conditions: Optional[List[StateCondition]] = None,
                     reload_on_failure: bool = False) -> bool:
        """
        Transition to target state with validation and retry.

        Args:
            target_state: Target state to transition to
            custom_conditions: Optional custom conditions
            reload_on_failure: If True, reload page on validation failure

        Returns:
            True if transition successful, False otherwise
        """
        if target_state not in self.state_definitions:
            self.logger.error(f"[STATE] Invalid target state: {target_state}")
            return False

        state_def = self.state_definitions[target_state]
        retry_count = 0

        while retry_count <= state_def.max_retries:
            if retry_count > 0:
                # Retries should be logged at INFO since they indicate issues
                self.logger.info(f"[STATE] Retry {retry_count}/{state_def.max_retries} for {target_state.value}")
                time.sleep(state_def.retry_delay)

            # Validate state
            if self.validate_state(target_state, custom_conditions):
                self.current_state = target_state
                self.state_history.append((target_state, time.time(), True))
                # Only log transition at INFO if it's a meaningful state change, otherwise DEBUG
                # PAGE_LOADED is very common, so log at DEBUG
                if target_state == NavigationState.PAGE_LOADED:
                    self.logger.debug(f"[STATE] Transitioned to: {target_state.value}")
                else:
                    self.logger.info(f"[STATE] Transitioned to: {target_state.value}")
                return True

            retry_count += 1

            # Reload if configured and not last retry
            if reload_on_failure and retry_count <= state_def.max_retries:
                try:
                    page_or_driver = self.locator.page or self.locator.driver
                    if hasattr(page_or_driver, 'reload'):  # Playwright
                        self.logger.info(f"[STATE] Reloading page before retry {retry_count}")
                        page_or_driver.reload()
                        time.sleep(2.0)  # Wait for reload
                    elif hasattr(page_or_driver, 'refresh'):  # Selenium
                        self.logger.info(f"[STATE] Refreshing page before retry {retry_count}")
                        page_or_driver.refresh()
                        time.sleep(2.0)
                except Exception as e:
                    self.logger.warning(f"[STATE] Error reloading page: {e}")

        # All retries failed
        self.current_state = NavigationState.ERROR
        self.state_history.append((target_state, time.time(), False))
        self.logger.error(f"[STATE] Failed to transition to {target_state.value} after {state_def.max_retries} retries")
        return False

    def get_current_state(self) -> NavigationState:
        """Get current state."""
        return self.current_state

    def get_state_history(self) -> List[Tuple[NavigationState, float, bool]]:
        """Get state transition history."""
        return self.state_history.copy()

    def add_custom_state(self, state_def: StateDefinition):
        """Add or update a custom state definition."""
        self.state_definitions[state_def.state] = state_def
        self.logger.info(f"[STATE] Added custom state definition: {state_def.state.value}")
