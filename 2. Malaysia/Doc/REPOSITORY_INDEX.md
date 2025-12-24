# Malaysia Medicine Price Scraper - Repository Index

## 📋 Repository Overview

This repository contains an automated web scraping system for collecting and processing Malaysian medicine pricing data from official government sources. The system integrates multiple data sources, matches products with PCID (Product Code Identifier) mappings, and determines reimbursable status for healthcare products.

**Repository Path**: `D:\quad99\Scappers\2. Malaysia`

---

## 📁 Directory Structure

```
2. Malaysia/
├── Backup/                          # Automatic backups of previous runs
│   └── backup_YYYYMMDD_HHMMSS/     # Timestamped backup folders
│
├── Doc/                             # Documentation
│   ├── USER_MANUAL.md              # Comprehensive user manual
│   └── HYBRID_APPROACH_CHANGES.md  # Technical details on optimization
│
├── Input/                           # Input data files
│   ├── Malaysia_PCID.csv           # PCID mapping (REQUIRED - 1,300+ mappings)
│   └── products.csv                # Product type list (25 types: Tablet, Capsule, etc.)
│
├── Output/                          # Generated output files
│   ├── execution_log.txt           # Execution logs
│   ├── malaysia_drug_prices_view_all.csv
│   ├── quest3_product_details.csv
│   ├── consolidated_products.csv
│   ├── malaysia_fully_reimbursable_drugs.csv
│   ├── malaysia_pcid_mapped.csv    # Final output (WITH PCID)
│   ├── malaysia_pcid_not_mapped.csv # Final output (WITHOUT PCID)
│   └── bulk_search_csvs/           # Intermediate bulk search results
│
├── Requirement/                     # Requirements and reference files
│   ├── ForwardedMessage (3).eml
│   ├── Malaysia_PCID Mapped_ 02122025.xlsx
│   ├── Malaysia_Updated Scrapping Doc_20250521.xlsx
│   └── pdf Url.txt
│
├── Script/                          # Python scripts (executed in sequence)
│   ├── 01_Product_Registration_Number.py
│   ├── 02_Product_Details.py
│   ├── 03_Consolidate_Results.py
│   ├── 04_Get_Fully_Reimbursable.py
│   └── 05_Generate_PCID_Mapped.py
│
├── run_scripts.bat                  # Main execution file (runs all scripts)
├── setup.bat                        # Setup script (installs dependencies)
└── REPOSITORY_INDEX.md              # This file
```

---

## 🔧 Core Components

### 1. Execution Scripts

#### `run_scripts.bat`
- **Purpose**: Main orchestration script
- **Functionality**:
  - Creates timestamped backups of previous outputs
  - Clears output folder (keeps execution_log.txt)
  - Executes scripts 01-05 in sequence
  - Logs execution times and statistics
  - Handles errors and provides summary
- **Dependencies**: Python, Playwright browsers

#### `setup.bat`
- **Purpose**: Initial setup and dependency installation
- **Installs**:
  - Playwright
  - Selenium + webdriver-manager
  - pandas + openpyxl
  - requests + beautifulsoup4 + lxml
  - Playwright Chromium browser

---

### 2. Python Scripts (Execution Pipeline)

#### Script 01: `01_Product_Registration_Number.py`
- **Technology**: Selenium WebDriver
- **Source**: MyPriMe (https://pharmacy.moh.gov.my/ms/apps/drug-price)
- **Function**: Scrapes ALL drug prices from government price guide
- **Process**:
  1. Opens MyPriMe website
  2. Clicks "View All" button
  3. Extracts complete table data
  4. Saves to `malaysia_drug_prices_view_all.csv`
- **Output**: ~3,000-5,000 products with prices
- **Runtime**: ~30-60 seconds
- **Key Columns**: Registration Number, Generic Name, Brand Name, Prices, Pack Info

#### Script 02: `02_Product_Details.py`
- **Technology**: Playwright (Chromium)
- **Source**: QUEST3+ (https://quest3plus.bpfk.gov.my/pmo2/)
- **Function**: Gets company/holder information for each product
- **Hybrid Approach** (Optimized):
  - **Stage 1**: Bulk keyword-based search (fast, efficient)
    - Uses `products.csv` to search by product type
    - Downloads CSV results from search page
    - Covers ~60-80% of products
  - **Stage 2**: Individual detail page scraping (fallback)
    - For products not found in Stage 1
    - Direct access to `detail.php?type=product&id={reg_no}`
    - Extracts Holder information
- **Output**: `quest3_product_details.csv` with Holder/Company info
- **Runtime**: 1-3 hours (optimized from 2-4 hours)
- **Key Features**:
  - Resume capability (skips already processed products)
  - Rate limiting (25s between bulk searches, 3s between individual)
  - Progress tracking

#### Script 03: `03_Consolidate_Results.py`
- **Technology**: pandas
- **Function**: Standardizes and cleans product details
- **Process**:
  1. Reads `quest3_product_details.csv`
  2. Filters rows with missing Product Name or Holder
  3. Removes duplicates
  4. Standardizes column names
- **Output**: `consolidated_products.csv`
- **Runtime**: ~5-10 seconds

#### Script 04: `04_Get_Fully_Reimbursable.py`
- **Technology**: requests + BeautifulSoup
- **Source**: FUKKM (https://pharmacy.moh.gov.my/ms/apps/fukkm)
- **Function**: Scrapes fully reimbursable drugs list
- **Process**:
  1. Detects total number of pages
  2. Iterates through all pages (0 to max)
  3. Extracts table data from each page
  4. Saves Generic Names for reimbursable matching
- **Output**: `malaysia_fully_reimbursable_drugs.csv`
- **Runtime**: ~1-3 minutes
- **Output**: ~1,000-2,000 drugs

#### Script 05: `05_Generate_PCID_Mapped.py`
- **Technology**: pandas
- **Function**: Generates final PCID-mapped report
- **Process**:
  1. Loads all input files:
     - `Malaysia_PCID.csv` (PCID mappings)
     - `consolidated_products.csv` (company info)
     - `malaysia_drug_prices_view_all.csv` (prices)
     - `malaysia_fully_reimbursable_drugs.csv` (reimbursable list)
  2. Joins data on Registration Number
  3. Matches Generic Names for reimbursable status
  4. Calculates VAT (0% for Malaysia - medicines are zero-rated)
  5. Splits into two files:
     - `malaysia_pcid_mapped.csv` (WITH PCID)
     - `malaysia_pcid_not_mapped.csv` (WITHOUT PCID)
- **Output**: Two CSV files with 52 columns each
- **Runtime**: ~10-20 seconds
- **Key Calculations**:
  - Reimbursable Status: "FULLY REIMBURSABLE" if Generic Name matches FUKKM list
  - Reimbursable Rate: "100.00%" or "0.00%"
  - Copayment Percent: "0.00%" or "100.00%"
  - VAT Percent: 0.0% (Malaysia medicines are zero-rated)

---

## 📊 Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    INPUT FILES                               │
├─────────────────────────────────────────────────────────────┤
│  Input/Malaysia_PCID.csv (1,300+ mappings)                 │
│  Input/products.csv (25 product types)                     │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│  Script 01: MyPriMe Scraper                                │
│  → malaysia_drug_prices_view_all.csv                       │
│  (Registration Numbers + Prices + Generic Names)           │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│  Script 02: QUEST3+ Product Details (HYBRID)               │
│  Stage 1: Bulk search by product type                      │
│  Stage 2: Individual detail pages (fallback)               │
│  → quest3_product_details.csv                              │
│  (Company/Holder Information)                              │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│  Script 03: Consolidate                                    │
│  → consolidated_products.csv                               │
│  (Standardized format)                                      │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│  Script 04: FUKKM Reimbursable List                        │
│  → malaysia_fully_reimbursable_drugs.csv                    │
│  (Generic Names for reimbursable matching)                 │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│  Script 05: Generate Final Report                          │
│  JOIN: Prices + Company + PCID + Reimbursable             │
│  CALCULATE: VAT, Reimbursable Status, Rates                │
│  → malaysia_pcid_mapped.csv (WITH PCID)                    │
│  → malaysia_pcid_not_mapped.csv (WITHOUT PCID)             │
└─────────────────────────────────────────────────────────────┘
```

---

## 🌐 Data Sources

### 1. MyPriMe (Malaysia Medicines Price Guide)
- **URL**: https://pharmacy.moh.gov.my/ms/apps/drug-price
- **Provider**: Malaysian Ministry of Health
- **Data**: Retail prices, registration numbers, generic/brand names, pack info
- **Update Frequency**: Periodic updates by MOH

### 2. QUEST3+ Product Database
- **URL**: https://quest3plus.bpfk.gov.my/pmo2/
- **Provider**: National Pharmaceutical Regulatory Agency (NPRA)
- **Data**: Product registration details, holder/company information, manufacturer details
- **Access**: Public database with search and detail pages

### 3. FUKKM (Formulari Ubat KKM)
- **URL**: https://pharmacy.moh.gov.my/ms/apps/fukkm
- **Provider**: Ministry of Health's Formulary
- **Data**: Fully reimbursable drugs list (Category A*)
- **Purpose**: Determines reimbursable status

### 4. User-Provided PCID Mapping
- **File**: `Input/Malaysia_PCID.csv`
- **Format**: `LOCAL_PACK_CODE,PCID Mapping`
- **Size**: ~1,300+ mappings
- **Purpose**: Maps registration numbers to internal PCID values

---

## 🔑 Key Features

### 1. Hybrid Scraping Approach (Script 02)
- **Optimization**: Reduces web server hits by ~40%
- **Stage 1**: Bulk keyword searches (fast, efficient)
- **Stage 2**: Individual detail pages (fallback for 100% coverage)
- **Result**: Faster execution (1-3 hours vs 2-4 hours) while maintaining 100% coverage

### 2. Automatic Backup System
- Creates timestamped backups before each run
- Preserves previous outputs for rollback
- Location: `Backup/backup_YYYYMMDD_HHMMSS/`

### 3. Comprehensive Logging
- Execution log: `Output/execution_log.txt`
- Tracks: Start/end times, file sizes, success/failure status
- Includes statistics and summaries

### 4. Resume Capability (Script 02)
- Can resume interrupted runs
- Skips already processed products
- Saves progress incrementally

### 5. Error Handling
- Graceful failure handling
- Continues processing even if individual products fail
- Detailed error messages in logs

---

## 📦 Dependencies

### Python Packages
- `playwright` - Web automation (Script 02)
- `selenium` - Web automation (Script 01)
- `webdriver-manager` - Automatic browser driver management
- `pandas` - Data processing
- `openpyxl` - Excel file support
- `requests` - HTTP requests (Script 04)
- `beautifulsoup4` - HTML parsing (Script 04)
- `lxml` - XML/HTML processing

### System Requirements
- **OS**: Windows 10/11
- **Python**: 3.8+
- **Browser**: Chromium (installed via Playwright)
- **Disk Space**: 500MB minimum

---

## 🚀 Quick Start

### First-Time Setup
1. Run `setup.bat` to install dependencies
2. Ensure `Input/Malaysia_PCID.csv` exists
3. Run `run_scripts.bat` to execute full pipeline

### Execution Time
- **Total**: ~3-5 hours
  - Script 01: ~1 minute
  - Script 02: **2-4 hours** (longest)
  - Script 03: ~10 seconds
  - Script 04: ~2 minutes
  - Script 05: ~15 seconds

---

## 📈 Output Files

### Final Outputs (Script 05)

#### `malaysia_pcid_mapped.csv`
- Products WITH PCID mapping
- 52 columns including:
  - PCID Mapping
  - Registration No (LOCAL_PACK_CODE)
  - Company/Holder
  - Product Group (Brand Name)
  - Generic Name
  - Pack Unit, Pack Size
  - Unit Price, Public with VAT Price
  - VAT Percent (0.0%)
  - Reimbursable Status
  - Reimbursable Rate
  - Copayment Percent
  - And 40+ other fields

#### `malaysia_pcid_not_mapped.csv`
- Products WITHOUT PCID mapping
- Same structure as mapped file
- Used to identify products needing PCID assignment

### Intermediate Outputs
- `malaysia_drug_prices_view_all.csv` - Raw price data from MyPriMe
- `quest3_product_details.csv` - Company/holder information
- `consolidated_products.csv` - Standardized product details
- `malaysia_fully_reimbursable_drugs.csv` - Reimbursable drugs list

---

## 🔍 Code Architecture

### Script 01: Selenium-based
- Uses Chrome WebDriver
- Waits for dynamic content to load
- Extracts table data using CSS selectors

### Script 02: Playwright-based (Hybrid)
- **Bulk Phase**: 
  - Searches by product type keywords
  - Downloads CSV results
  - Merges bulk results
- **Individual Phase**:
  - Extracts Holder from detail pages
  - Handles missing products
  - Resume capability

### Script 03: Data Processing
- pandas DataFrame operations
- Column renaming and standardization
- Data cleaning and deduplication

### Script 04: HTTP-based Scraping
- requests library for HTTP calls
- BeautifulSoup for HTML parsing
- Pagination handling
- Error recovery

### Script 05: Data Integration
- Multi-file joins using pandas
- Complex data transformations
- Reimbursable status matching
- VAT calculations
- File splitting (mapped/not mapped)

---

## 📝 Documentation

### User Manual
- **File**: `Doc/USER_MANUAL.md`
- **Contents**:
  - Complete setup instructions
  - Detailed workflow explanation
  - Troubleshooting guide
  - FAQ section
  - Validation procedures

### Technical Documentation
- **File**: `Doc/HYBRID_APPROACH_CHANGES.md`
- **Contents**:
  - Hybrid scraping approach details
  - Performance improvements
  - Code changes explanation
  - Configuration options

---

## 🛠️ Maintenance

### Regular Tasks
- **Monthly**: Run full execution for price updates
- **Quarterly**: Review unmapped products, update PCID mappings
- **As needed**: Update `Input/Malaysia_PCID.csv` with new mappings

### Backup Management
- Backups stored in `Backup/` folder
- Consider archiving backups older than 3 months
- Each backup contains complete output from previous run

---

## ⚠️ Important Notes

1. **Script 02 is the bottleneck**: Takes 2-4 hours due to web scraping
2. **Internet required**: All scripts need active internet connection
3. **Rate limiting**: Built-in delays to respect server resources
4. **PCID mapping required**: `Input/Malaysia_PCID.csv` must exist
5. **Browser visibility**: Scripts run with visible browser windows (can be changed to headless)

---

## 🔗 Related Files

- **Requirements**: `Requirement/` folder contains reference Excel files and documentation
- **Backups**: `Backup/` folder contains timestamped backups
- **Logs**: `Output/execution_log.txt` contains detailed execution logs

---

## 📊 Statistics

### Expected Data Volumes
- **Input PCID Mappings**: ~1,300+ products
- **MyPriMe Products**: ~3,000-5,000 products
- **Reimbursable Drugs**: ~1,000-2,000 drugs
- **Final Output**: Depends on PCID coverage

### Performance Metrics
- **Total Runtime**: 3-5 hours
- **Web Requests**: ~700 bulk searches + ~1,400 detail pages (optimized)
- **Coverage**: 100% attempt rate, ~95-100% success rate
- **Stage 1 Success**: ~60-80% (keyword search)
- **Stage 2 Success**: ~20-40% (detail pages)

---

## 🎯 Use Cases

1. **Price Monitoring**: Track Malaysian medicine prices over time
2. **PCID Mapping**: Map registration numbers to internal PCID values
3. **Reimbursable Analysis**: Identify fully reimbursable vs non-reimbursable drugs
4. **Company Information**: Get complete holder/manufacturer details
5. **Data Integration**: Combine multiple government data sources

---

## 📅 Last Updated

- **Repository Index**: Created January 2025
- **User Manual**: Last updated December 24, 2025 (Version 2.0)
- **Hybrid Approach**: Implemented for Script 02 optimization

---

## 🔐 Security & Privacy

- All data sources are public government databases
- No authentication required
- No sensitive data stored
- Rate limiting implemented to respect server resources

---

**End of Repository Index**

