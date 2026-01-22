# Malaysia Scraper Upgrade Summary

## Overview
The Malaysia scraper has been upgraded to be semi-intelligent, production-safe, and deterministic without using any LLM, MCP, vision, or OCR. All improvements are rule-based and deterministic.

## New Modules Created

### 1. `smart_locator.py`
**Purpose**: Intelligent element location with accessibility-first approach and automatic fallback.

**Key Features**:
- **Accessibility-first selectors**: Prefers `get_by_role`, `get_by_label`, `get_by_text` over CSS/XPath
- **Smart fallback engine**: Automatically tries alternative selectors when primary fails
- **DOM change awareness**: Detects structural changes via hash-based monitoring
- **Anomaly detection**: Detects empty tables, small CSV files, error text on pages
- **Comprehensive metrics**: Tracks selector success rates, fallback usage, DOM changes, anomalies
- **Dual support**: Works with both Playwright and Selenium

**Selector Priority** (highest to lowest):
1. Role-based (ARIA roles)
2. Label-based
3. Text-based
4. Placeholder-based
5. Test ID-based
6. CSS selector (fallback)
7. XPath (last resort)

### 2. `state_machine.py`
**Purpose**: Deterministic state-based navigation with explicit state validation.

**Key Features**:
- **Explicit states**: PAGE_LOADED, SEARCH_READY, RESULTS_LOADING, RESULTS_READY, CSV_READY, DETAIL_READY, TABLE_READY
- **State validation**: Each state defines required conditions that must be met
- **Safe transitions**: Transitions only occur when state conditions are validated
- **Retry logic**: Automatic retry on state validation failure with configurable retries
- **State history**: Tracks all state transitions for debugging
- **Custom states**: Allows adding custom state definitions

## Script Updates

### `01_Product_Registration_Number.py`
**Changes**:
- Integrated `SmartLocator` for intelligent element finding
- Added `NavigationStateMachine` for state-based navigation
- Replaced fixed sleeps with dynamic waits based on state validation
- Added DOM change detection
- Added anomaly detection (empty tables, small CSV files)
- Added comprehensive metrics logging
- Improved error handling with HTML snapshots on failures
- Row count stability checking before extraction

**Key Improvements**:
- "View All" button now uses accessibility-first selectors with XPath fallback
- Table extraction waits for row count to stabilize
- Anomaly detection alerts on empty tables or suspicious CSV sizes
- Metrics track selector success vs fallback usage

### `02_Product_Details.py`
**Changes**:
- Integrated `SmartLocator` throughout bulk and individual search phases
- Added `NavigationStateMachine` for state-based navigation
- Replaced brittle selectors with smart locator fallback strategies
- Added state transitions: PAGE_LOADED → SEARCH_READY → RESULTS_LOADING → RESULTS_READY → CSV_READY
- Added anomaly detection for empty results, small CSV files
- Improved wait strategies: network idle, table stability, button enabled checks
- Added HTML snapshot capture on errors
- Comprehensive metrics logging for both bulk and individual phases

**Key Improvements**:
- Search form elements use smart locator with multiple fallback strategies
- Results table waits for data stability before CSV download
- CSV button checks for enabled state before clicking
- Anomaly detection retries once if CSV is too small
- Individual detail extraction uses state machine for reliable navigation

## Technical Details

### Accessibility-First Approach
- Prefers semantic selectors (role, label, text) over structural (CSS, XPath)
- More resilient to DOM structure changes
- Better matches user-visible elements

### Smart Fallback Engine
- Scores candidates based on:
  - Selector type (role > label > text > CSS > XPath)
  - Text similarity (for text-based matching)
  - Element visibility and enabled state
- Selects highest-scoring candidate automatically
- Logs which fallback was used for debugging

### State Machine Benefits
- Explicit validation before proceeding
- Prevents race conditions
- Clear error messages when states fail
- Retry logic with configurable attempts
- Reload page option on state failure

### Dynamic Wait Strategies
- Network idle detection
- Row count stability checking
- Button enabled state verification
- Loading indicator disappearance
- No fixed sleeps (except minimal bounded delays)

### DOM Change Awareness
- Hashes key DOM sections
- Detects structural changes
- Automatically enables fallback selectors when changes detected
- Increases logging verbosity on changes

### Anomaly Detection
- Empty table detection
- CSV size threshold checking
- Error text pattern matching
- Automatic retry on anomalies
- HTML snapshot capture for debugging

### Metrics and Logging
- Selector success vs fallback usage
- State transition history
- Retry counts
- Anomaly triggers
- DOM change events
- No verbose logs in tight loops

## Configuration
No changes to existing configuration files. All new features use existing config values or sensible defaults.

## Backward Compatibility
- ✅ No changes to output CSV/Excel schemas
- ✅ No changes to downstream scripts
- ✅ All existing config variables still work
- ✅ Scripts 00, 03, 04, 05 unchanged

## Quality Improvements
- ✅ Readable code with clear inline comments
- ✅ No magic numbers (all constants defined)
- ✅ All fallbacks are explainable
- ✅ Boring, stable solutions over clever ones
- ✅ Deterministic behavior only
- ✅ No probabilistic models

## Testing Recommendations
1. Test with actual website to verify selector fallbacks work
2. Monitor metrics logs to understand selector success rates
3. Check state transition logs for navigation flow
4. Verify anomaly detection triggers appropriately
5. Confirm HTML snapshots are captured on errors

## Future Enhancements (Not Implemented)
- Could add more sophisticated text similarity algorithms
- Could add element position-based scoring (but avoided per requirements)
- Could add more state definitions for other workflows
- Could add selector caching for performance

## Notes
- All improvements are deterministic and rule-based
- No external AI services used
- No vision or OCR capabilities
- Production-safe with comprehensive error handling
- Maintains existing functionality while adding intelligence
