# Malaysia Configuration Organization Guide

## Overview

The Malaysia scraper configuration has been reorganized for better human readability. All settings are now grouped by script/category with clear section headers and descriptions.

## Configuration File Location

**Primary Config:** [config/Malaysia.env.json](config/Malaysia.env.json)

## Configuration Structure

### Section Layout

```json
{
  "scraper": {
    "id": "Malaysia",
    "enabled": true
  },
  "config": {
    "_comment_general": "========== GENERAL SETTINGS ==========",
    // General pipeline settings

    "_comment_script_01": "========== SCRIPT 01: PRODUCT REGISTRATION NUMBER ==========",
    "_comment_script_01_desc": "Scrapes drug prices from MyPriMe website",
    // Script 01 settings grouped together

    "_comment_script_02": "========== SCRIPT 02: PRODUCT DETAILS ==========",
    "_comment_script_02_desc": "Fetches detailed product information from Quest3Plus",
    // Script 02 settings grouped together

    // ... and so on for each script
  }
}
```

## Configuration Categories

### 1. General Settings
- `PIPELINE_LOG_FILE_PREFIX` - Log file naming prefix

### 2. Script 01: Product Registration Number
**Purpose:** Scrapes drug prices from MyPriMe website

**Key Settings:**
- **URL & Browser:**
  - `SCRIPT_01_URL` - Target website URL
  - `SCRIPT_01_HEADLESS` - Run browser in headless mode (⚠️ see headless mode notes below)

- **Directories & Files:**
  - `SCRIPT_01_OUT_DIR` - Output directory
  - `SCRIPT_01_OUTPUT_CSV` - Output CSV filename

- **Timing:**
  - `SCRIPT_01_WAIT_TIMEOUT` - Element wait timeout (seconds)
  - `SCRIPT_01_CLICK_DELAY` - Delay after clicking (seconds)

- **Browser Options:**
  - `SCRIPT_01_CHROME_START_MAXIMIZED` - Chrome start maximized flag
  - `SCRIPT_01_CHROME_DISABLE_AUTOMATION` - Disable automation detection

- **Selectors:**
  - `SCRIPT_01_VIEW_ALL_XPATH` - XPath for "View All" button
  - `SCRIPT_01_TABLE_SELECTOR` - Table CSS selector
  - `SCRIPT_01_HEADER_SELECTOR` - Header row selector
  - `SCRIPT_01_ROW_SELECTOR` - Data row selector
  - `SCRIPT_01_CELL_SELECTOR` - Cell selector

### 3. Script 02: Product Details
**Purpose:** Fetches detailed product information from Quest3Plus

**Key Settings:**
- **URLs & Browser:**
  - `SCRIPT_02_SEARCH_URL` - Quest3Plus search page
  - `SCRIPT_02_DETAIL_URL` - Product detail page template
  - `SCRIPT_02_HEADLESS` - Headless mode (boolean)

- **Directories & Files:**
  - `SCRIPT_02_BASE_DIR` - Base directory
  - `SCRIPT_02_OUT_DIR` - Output directory
  - `SCRIPT_02_INPUT_PRODUCTS` - Input products CSV
  - `SCRIPT_02_INPUT_MALAYSIA` - Input Malaysia drugs CSV
  - `SCRIPT_02_OUT_BULK` - Bulk search results
  - `SCRIPT_02_OUT_MISSING` - Missing registration numbers
  - `SCRIPT_02_OUT_FINAL` - Final product details
  - `SCRIPT_02_BULK_DIR_NAME` - Bulk search CSV directory
  - `SCRIPT_02_BULK_CSV_PATTERN` - Bulk CSV filename pattern

- **Timing & Performance:**
  - `SCRIPT_02_WAIT_BULK` - Wait time for bulk search
  - `SCRIPT_02_SEARCH_DELAY` - Delay between searches
  - `SCRIPT_02_DETAIL_DELAY` - Delay for detail pages
  - `SCRIPT_02_PAGE_TIMEOUT` - Page load timeout (ms)
  - `SCRIPT_02_SELECTOR_TIMEOUT` - Selector wait timeout (ms)
  - `SCRIPT_02_SAVE_INTERVAL` - Save progress interval

- **Coverage Thresholds:**
  - `SCRIPT_02_COVERAGE_HIGH_THRESHOLD` - High coverage % (95%)
  - `SCRIPT_02_COVERAGE_MEDIUM_THRESHOLD` - Medium coverage % (80%)

- **Selectors & Labels:**
  - Various CSS selectors and field labels for scraping

### 4. Script 03: Consolidate Results
**Purpose:** Combines product details from multiple sources

**Key Settings:**
- `SCRIPT_03_OUTPUT_BASE_DIR` - Output base directory
- `SCRIPT_03_QUEST3_DETAILS` - Quest3 details input file
- `SCRIPT_03_CONSOLIDATED_FILE` - Consolidated output file
- `SCRIPT_03_REQUIRED_COLUMNS` - Required column list

### 5. Script 04: Get Fully Reimbursable
**Purpose:** Scrapes fully reimbursable drugs from MOH website

**Key Settings:**
- **URL & Files:**
  - `SCRIPT_04_BASE_URL` - MOH website URL
  - `SCRIPT_04_OUT_CSV` - Output CSV filename

- **HTTP Settings:**
  - `SCRIPT_04_REQUEST_TIMEOUT` - Request timeout (seconds)
  - `SCRIPT_04_PAGE_DELAY` - Delay between pages
  - `SCRIPT_04_USER_AGENT` - User agent string
  - HTTP headers (Accept, Language, Connection)

- **Selectors:**
  - Table and row selectors

### 6. Script 05: Generate PCID Mapped
**Purpose:** Maps products to PCID and generates final output

**Key Settings:**
- **Directories & Files:**
  - Input/output directories
  - Various CSV file names

- **Data Values:**
  - `SCRIPT_05_COUNTRY_VALUE` - Country name (MALAYSIA)
  - `SCRIPT_05_REGION_VALUE` - Region name (MALAYSIA)
  - `SCRIPT_05_CURRENCY_VALUE` - Currency code (MYR)
  - `SCRIPT_05_SOURCE_VALUE` - Data source (PRICENTRIC)

- **Processing:**
  - `SCRIPT_05_DEFAULT_VAT_PERCENT` - Default VAT percentage
  - Coverage thresholds
  - Final column mappings
  - Excel sheet names

### 7. Script 00: Backup and Clean
**Purpose:** Manages output folder backup and cleanup

**Key Settings:**
- `SCRIPT_00_KEEP_FILES` - Files to preserve during cleanup
- `SCRIPT_00_KEEP_DIRS` - Directories to preserve

## Headless Mode Configuration

⚠️ **IMPORTANT:** Headless mode behavior with Cloudflare

### Current Implementation
The scraper now uses `undetected-chromedriver`'s special headless mode which is better at bypassing Cloudflare detection.

**Code:** [01_Product_Registration_Number.py](scripts/Malaysia/01_Product_Registration_Number.py)

```python
driver = uc.Chrome(
    options=options,
    version_main=chrome_version,
    headless=headless  # UC's special headless mode
)
```

### Recommended Settings

**For Local Development (Most Reliable):**
```json
"SCRIPT_01_HEADLESS": "false"
```

**For Production/Automation (May work):**
```json
"SCRIPT_01_HEADLESS": "true"
```

### Why Headless Mode Can Get Stuck

Cloudflare detects headless browsers through:
1. Missing WebGL/Canvas fingerprints
2. No mouse/keyboard events
3. Headless-specific browser properties
4. Network timing patterns

See [HEADLESS_MODE_CLOUDFLARE.md](HEADLESS_MODE_CLOUDFLARE.md) for detailed analysis.

### Solutions if Stuck in Headless

1. **Use visible browser** - Set `SCRIPT_01_HEADLESS: "false"`
2. **Use Xvfb on Linux** - Virtual display for servers
3. **Monitor execution** - Check if stuck at verification
4. **Add delays** - Increase wait timeouts

## Quick Reference

### Common Configuration Changes

**Enable/Disable Headless:**
```json
"SCRIPT_01_HEADLESS": "true"   // Headless (hidden browser)
"SCRIPT_01_HEADLESS": "false"  // Visible browser
```

**Adjust Timeouts:**
```json
"SCRIPT_01_WAIT_TIMEOUT": 30,      // Increase if slow network
"SCRIPT_02_PAGE_TIMEOUT": 90000,   // Increase for slow pages
```

**Change Output Directory:**
```json
"SCRIPT_01_OUT_DIR": "../output",  // Relative path
"OUTPUT_DIR": "C:/custom/path"     // Absolute path (override)
```

**Adjust Coverage Thresholds:**
```json
"SCRIPT_02_COVERAGE_HIGH_THRESHOLD": 95,
"SCRIPT_02_COVERAGE_MEDIUM_THRESHOLD": 80,
```

## File Organization Benefits

✅ **Easy to Find Settings:**
- Grouped by script number
- Clear section headers
- Descriptive comments

✅ **Better Maintenance:**
- Related settings together
- Easier to add new settings
- Clear ownership per script

✅ **Human Readable:**
- Visual section separators
- Purpose descriptions
- Logical grouping

## Configuration File Format

The configuration uses JSON format with special comment fields:
- `_comment_*` fields are ignored by code but provide documentation
- Standard JSON key-value pairs for actual settings
- Arrays use JSON array syntax: `["item1", "item2"]`
- Boolean values: `true` / `false` (lowercase, no quotes)
- String values: `"value"` (quoted)
- Number values: `123` (no quotes)

## Validation

The configuration is automatically loaded and validated by:
- [config_loader.py](scripts/Malaysia/config_loader.py)
- [core/config_manager.py](core/config_manager.py)

Missing required settings will raise errors with clear messages.
