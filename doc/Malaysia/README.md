# Malaysia Scraper Documentation

## Overview

The Malaysia scraper extracts pharmaceutical pricing and product information from multiple Malaysian government sources:
- MyPriMe (Ministry of Health) - Drug prices
- QUEST3+ (Pharmacy Board) - Product registration details
- Fully Reimbursable Drugs list

The scraper consolidates data from these sources and generates PCID-mapped reports.

## Workflow

The Malaysia scraper follows a 6-step pipeline:

1. **00_backup_and_clean.py** - Backup existing output and clean for fresh run
2. **01_Product_Registration_Number.py** - Scrape drug prices from MyPriMe
3. **02_Product_Details.py** - Get product details from QUEST3+
4. **03_Consolidate_Results.py** - Consolidate and standardize product data
5. **04_Get_Fully_Reimbursable.py** - Scrape fully reimbursable drugs list
6. **05_Generate_PCID_Mapped.py** - Generate final PCID-mapped report

## Configuration

All configuration is managed through `config/Malaysia.env.json`. The configuration uses script-specific prefixes:

- **SCRIPT_01_*** - MyPriMe scraping settings
- **SCRIPT_02_*** - QUEST3+ scraping settings
- **SCRIPT_03_*** - Consolidation settings
- **SCRIPT_04_*** - Fully reimbursable scraping settings
- **SCRIPT_05_*** - PCID mapping settings

### Key Configuration Values

- `SCRIPT_01_URL` - MyPriMe drug price URL
- `SCRIPT_01_WAIT_TIMEOUT` - Selenium wait timeout
- `SCRIPT_01_CLICK_DELAY` - Delay between clicks
- `SCRIPT_02_SEARCH_URL` - QUEST3+ search URL
- `SCRIPT_02_DETAIL_URL` - QUEST3+ detail URL template
- `SCRIPT_02_HEADLESS` - Browser headless mode
- `SCRIPT_02_WAIT_BULK` - Bulk search wait time
- `SCRIPT_04_BASE_URL` - Fully reimbursable URL
- `SCRIPT_05_PCID_MAPPING` - PCID mapping file name

## Input Files

Place the following files in the input directory:

- `Malaysia_PCID.csv` - PCID mapping file
- `products.csv` - Optional product list for filtering

## Output Files

The scraper generates the following output files:

- `malaysia_drug_prices_view_all.csv` - Drug prices from MyPriMe
- `quest3_product_details.csv` - Product details from QUEST3+
- `quest3_bulk_results.csv` - Bulk search results
- `quest3_missing_regnos.csv` - Missing registration numbers
- `consolidated_products.csv` - Consolidated product data
- `malaysia_fully_reimbursable_drugs.csv` - Fully reimbursable drugs
- `malaysia_pcid_mapped.csv` - Final PCID-mapped report
- `malaysia_pcid_not_mapped.csv` - Products without PCID mapping

## Running the Scraper

### Using the GUI

1. Launch `scraper_gui.py`
2. Select "Malaysia" from the scraper dropdown
3. Click "Run Pipeline" to execute all steps sequentially

### Using Command Line

Navigate to `scripts/Malaysia/` and run:

```batch
run_pipeline.bat
```

Or run individual steps:

```bash
python 00_backup_and_clean.py
python 01_Product_Registration_Number.py
python 02_Product_Details.py
python 03_Consolidate_Results.py
python 04_Get_Fully_Reimbursable.py
python 05_Generate_PCID_Mapped.py
```

## Script Details

### 01_Product_Registration_Number.py

Scrapes drug prices from MyPriMe website.

**Input:** None (scrapes from website)
**Output:** `malaysia_drug_prices_view_all.csv`

**Configuration:**
- `SCRIPT_01_URL` - MyPriMe URL
- `SCRIPT_01_WAIT_TIMEOUT` - Selenium wait timeout
- `SCRIPT_01_CLICK_DELAY` - Delay between clicks
- `SCRIPT_01_HEADLESS` - Browser headless mode
- `SCRIPT_01_CHROME_START_MAXIMIZED` - Chrome options
- `SCRIPT_01_CHROME_DISABLE_AUTOMATION` - Anti-detection options

**Features:**
- Selenium WebDriver automation
- Automatic "View All" button click
- Table extraction
- CSV export

### 02_Product_Details.py

Gets product details from QUEST3+ using registration numbers.

**Input:** 
- `products.csv` (optional)
- `malaysia_drug_prices_view_all.csv`

**Output:**
- `quest3_product_details.csv` - Product details
- `quest3_bulk_results.csv` - Bulk search results
- `quest3_missing_regnos.csv` - Missing registration numbers

**Configuration:**
- `SCRIPT_02_SEARCH_URL` - QUEST3+ search URL
- `SCRIPT_02_DETAIL_URL` - Detail URL template
- `SCRIPT_02_HEADLESS` - Browser headless mode
- `SCRIPT_02_WAIT_BULK` - Bulk search wait time
- `SCRIPT_02_SEARCH_DELAY` - Delay between searches
- `SCRIPT_02_DETAIL_DELAY` - Delay between detail fetches
- `SCRIPT_02_PAGE_TIMEOUT` - Page load timeout
- `SCRIPT_02_SELECTOR_TIMEOUT` - Selector wait timeout

**Features:**
- Bulk search capability
- Individual product detail fetching
- Progress tracking
- Error handling and retry logic
- Missing registration number tracking

### 03_Consolidate_Results.py

Consolidates and standardizes product data from multiple sources.

**Input:**
- `quest3_product_details.csv`

**Output:**
- `consolidated_products.csv`

**Features:**
- Data standardization
- Column mapping
- Data cleaning
- Duplicate handling

### 04_Get_Fully_Reimbursable.py

Scrapes fully reimbursable drugs list from MOH website.

**Input:** None (scrapes from website)
**Output:** `malaysia_fully_reimbursable_drugs.csv`

**Configuration:**
- `SCRIPT_04_BASE_URL` - Fully reimbursable URL
- `SCRIPT_04_TABLE_SELECTOR` - Table CSS selector
- `SCRIPT_04_REQUEST_TIMEOUT` - Request timeout
- `SCRIPT_04_PAGE_DELAY` - Delay between pages
- `SCRIPT_04_FAIL_FAST` - Stop on first error
- `SCRIPT_04_USER_AGENT` - User agent string

**Features:**
- Multi-page scraping
- Table extraction
- Error handling
- Progress tracking

### 05_Generate_PCID_Mapped.py

Generates final PCID-mapped report.

**Input:**
- `consolidated_products.csv`
- `malaysia_drug_prices_view_all.csv`
- `malaysia_fully_reimbursable_drugs.csv`
- `Malaysia_PCID.csv` (from input directory)

**Output:**
- `malaysia_pcid_mapped.csv` - Mapped products
- `malaysia_pcid_not_mapped.csv` - Unmapped products

**Configuration:**
- `SCRIPT_05_PCID_MAPPING` - PCID mapping file name
- `SCRIPT_05_CONSOLIDATED` - Consolidated file name
- `SCRIPT_05_PRICES` - Prices file name
- `SCRIPT_05_REIMBURSABLE` - Reimbursable file name

**Features:**
- PCID mapping
- Data merging
- Unmapped product tracking
- Final report generation

## Troubleshooting

### Common Issues

1. **Selenium WebDriver Errors**
   - Ensure ChromeDriver is installed and up to date
   - Check Chrome browser version compatibility
   - Verify `SCRIPT_01_HEADLESS` / `SCRIPT_02_HEADLESS` settings

2. **Timeout Errors**
   - Increase `WAIT_TIMEOUT`, `PAGE_TIMEOUT`, or `SELECTOR_TIMEOUT`
   - Check internet connection
   - Verify website is accessible

3. **Missing Registration Numbers**
   - Review `quest3_missing_regnos.csv`
   - Check if registration numbers are valid
   - Verify QUEST3+ website structure hasn't changed

4. **PCID Mapping Issues**
   - Verify `Malaysia_PCID.csv` format
   - Check column names match expected format
   - Review `malaysia_pcid_not_mapped.csv` for unmapped products

5. **Scraping Failures**
   - Check if website structure has changed
   - Verify selectors are still valid
   - Review error logs for specific failures

## Dependencies

- Selenium WebDriver
- ChromeDriver
- pandas
- requests
- Python 3.8+

## Notes

- The scraper uses Selenium for web automation
- ChromeDriver must be installed and in PATH
- All configuration values are in `config/Malaysia.env.json`
- The scraper handles rate limiting and delays automatically
- Progress is tracked and saved periodically

