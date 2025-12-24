# Malaysia Medicine Price Scraper - User Manual

## Table of Contents
1. [Overview](#overview)
2. [System Requirements](#system-requirements)
3. [Installation & Setup](#installation--setup)
4. [Data Sources](#data-sources)
5. [Workflow & Execution](#workflow--execution)
6. [Output Files](#output-files)
7. [Troubleshooting & Debugging](#troubleshooting--debugging)
8. [Testing & Validation](#testing--validation)
9. [FAQ](#faq)

---

## Overview

### Purpose
This system automatically collects and processes Malaysian medicine pricing data from official government sources, matches it with PCID (Product Code Identifier) mappings, and determines reimbursable status for healthcare products.

### Key Features
- ✅ Automated data collection from multiple government sources
- ✅ Complete company/holder information for ALL products
- ✅ PCID mapping integration
- ✅ Reimbursable status determination
- ✅ Detailed execution logging
- ✅ Automatic backup system
- ✅ Separate output for mapped vs unmapped products

---

## System Requirements

### Software Requirements
- **Operating System**: Windows 10/11
- **Python**: Version 3.8 or higher
- **Internet Connection**: Required for web scraping
- **Disk Space**: Minimum 500MB free space

### Python Packages
All required packages are installed automatically via `setup.bat`:
- `playwright` - Web automation for Script 01 & 02
- `selenium` - Web automation for Script 01
- `webdriver-manager` - Automatic browser driver management
- `pandas` - Data processing
- `openpyxl` - Excel file support
- `requests` - HTTP requests for Script 04
- `beautifulsoup4` - HTML parsing for Script 04
- `lxml` - XML/HTML processing

---

## Installation & Setup

### First-Time Setup

1. **Extract the folder** to your desired location:
   ```
   Example: D:\quad99\Scappers\2. Malaysia\
   ```

2. **Run setup.bat**:
   - Double-click `setup.bat`
   - Wait for all dependencies to install (may take 5-10 minutes)
   - You should see: "Setup Complete!"

3. **Prepare input file**:
   - Navigate to `Input/` folder
   - Ensure `Malaysia_PCID.csv` exists with columns:
     - `LOCAL_PACK_CODE` (Registration Number)
     - `PCID Mapping` (PCID value)

### Folder Structure
```
2. Malaysia/
├── Backup/                          # Automatic backups
├── Input/
│   └── Malaysia_PCID.csv           # PCID mapping (REQUIRED)
├── Output/
│   └── execution_log.txt           # Execution logs
├── Script/
│   ├── 01_Product_Registration_Number.py
│   ├── 02_Product_Details.py
│   ├── 03_Consolidate_Results.py
│   ├── 04_Get_Fully_Reimbursable.py
│   └── 05_Generate_PCID_Mapped.py
├── run_scripts.bat                  # Main execution file
├── setup.bat                        # Setup file
└── USER_MANUAL.md                   # This file
```

---

## Data Sources

### Source 1: MyPriMe (Malaysia Medicines Price Guide)
**URL**: https://pharmacy.moh.gov.my/ms/apps/drug-price
**Used in**: Script 01
**Data Collected**:
- Product Registration Number (MAL...)
- Generic Name
- Brand/Trade Name
- Packaging Description
- Unit (SKU)
- Quantity per pack
- Retail Price per SKU
- Retail Price per Pack
- Year price updated

**Details**:
- Official price guide from Malaysian Ministry of Health
- Contains retail prices suggested by product registration holders
- Updated periodically by MOH
- Accessible via "View All" button on the website

### Source 2: QUEST3+ Product Database
**URL**: https://quest3plus.bpfk.gov.my/pmo2/
**Used in**: Script 02
**Data Collected**:
- Product Name (official)
- Holder/Company Name
- Holder Address
- Manufacturer Name
- Manufacturer Address
- Phone Number

**Details**:
- National Pharmaceutical Regulatory Agency (NPRA) database
- Official product registration information
- **HYBRID SCRAPING APPROACH** (optimized to reduce web hits):
  - **Stage 1**: Keyword-based search via `index.php` (fast, efficient)
    - Selects "Product Name" search criteria from dropdown
    - Enters product keyword in search box
    - Parses search results table
  - **Stage 2**: Direct detail page scraping via `detail.php?type=product&id={registration_number}` (fallback for missing data)
  - This ensures 100% coverage while minimizing server load

### Source 3: FUKKM (Formulari Ubat KKM)
**URL**: https://pharmacy.moh.gov.my/ms/apps/fukkm
**Used in**: Script 04
**Data Collected**:
- Generic Name
- MDC Code
- Category (A*, B, C)
- Indications
- Prescription Restrictions
- Dosage

**Details**:
- Ministry of Health's Formulary
- Contains fully reimbursable drugs list
- Category A* = Fully Reimbursable
- Used to determine reimbursable status

### Source 4: Malaysia_PCID.csv (User-Provided)
**Location**: Input/Malaysia_PCID.csv
**Used in**: Script 05
**Format**:
```csv
LOCAL_PACK_CODE,PCID Mapping
MAL20002297XR,1837965
MAL09102913AZ,289099
```

**Details**:
- User must provide this file
- Maps registration numbers to PCID values
- PCID = Product Code Identifier (internal reference)

---

## Workflow & Execution

### Execution Overview

```
┌─────────────────────────────────────────────────────────┐
│  run_scripts.bat - Main Execution File                 │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│  1. Backup existing outputs to Backup/backup_TIMESTAMP │
│  2. Clear Output folder (keep execution_log.txt)        │
└─────────────────────────────────────────────────────────┘
                        ↓
         ┌──────────────────────────┐
         │  Execute Scripts 1-5     │
         └──────────────────────────┘
                        ↓
         ┌──────────────────────────┐
         │  Generate Summary        │
         │  Log Statistics          │
         └──────────────────────────┘
```

### Script-by-Script Workflow

#### Script 01: Get Drug Prices from MyPriMe
**File**: `01_Product_Registration_Number.py`
**Purpose**: Get ALL medicine prices from MyPriMe
**Process**:
1. Opens https://pharmacy.moh.gov.my/ms/apps/drug-price
2. Clicks "View All" to display all products
3. Waits for full table to load
4. Extracts all rows from the table
5. Saves to `malaysia_drug_prices_view_all.csv`

**Expected Runtime**: 30-60 seconds
**Expected Output**: ~3,000-5,000 products
**Browser**: Visible Selenium window

#### Script 02: Get Product Details from QUEST3+ (HYBRID APPROACH)
**File**: `02_Product_Details.py`
**Purpose**: Get company/holder info for each product using optimized two-stage approach
**Process**:
1. Reads registration numbers and product names from Script 01 output
2. For EACH product:
   - **STAGE 1**: Tries keyword-based search first (faster)
     - Searches by product name on `search_product.php`
     - Matches registration number in search results
     - If found: extracts Product Name and Holder
   - **STAGE 2**: If Stage 1 fails, uses direct detail page (guaranteed)
     - Opens `https://quest3plus.bpfk.gov.my/pmo2/detail.php?type=product&id={reg_no}`
     - Extracts complete details: Product Name, Holder, Manufacturer, Addresses, Phone
   - Waits 1.5 seconds between requests (rate limiting)
3. Saves to `quest3_product_details.csv`

**Expected Runtime**: 1-3 hours (optimized - majority resolved in Stage 1)
**Expected Output**: Same count as Script 01 with 100% coverage
**Browser**: Visible Playwright window
**Note**: This is still the LONGEST running script, but significantly faster than pure detail scraping
**Statistics**: Script shows Stage 1 vs Stage 2 success rates at completion

#### Script 03: Consolidate Product Details
**File**: `03_Consolidate_Results.py`
**Purpose**: Standardize product details format
**Process**:
1. Reads `quest3_product_details.csv`
2. Filters out rows with missing Product Name or Holder
3. Removes duplicates
4. Standardizes column names
5. Saves to `consolidated_products.csv`

**Expected Runtime**: 5-10 seconds
**Expected Output**: Slightly less than Script 02 (filtered)

#### Script 04: Get Fully Reimbursable Drugs
**File**: `04_Get_Fully_Reimbursable.py`
**Purpose**: Get list of fully reimbursable medicines
**Process**:
1. Fetches first page of FUKKM to detect total pages
2. Iterates through all pages (0 to max)
3. Extracts Generic Name and other details from each page
4. Saves to `malaysia_fully_reimbursable_drugs.csv`

**Expected Runtime**: 1-3 minutes
**Expected Output**: ~1,000-2,000 drugs
**No Browser**: Uses requests library

#### Script 05: Generate PCID Mapped Report
**File**: `05_Generate_PCID_Mapped.py`
**Purpose**: Create final report with PCID and reimbursable status
**Process**:
1. Loads input files:
   - `Malaysia_PCID.csv` (from Input folder)
   - `consolidated_products.csv` (Script 03)
   - `malaysia_drug_prices_view_all.csv` (Script 01)
   - `malaysia_fully_reimbursable_drugs.csv` (Script 04)
2. Joins all data on Registration Number
3. Matches Generic Names for reimbursable status
4. Calculates VAT (0% for Malaysia)
5. Splits into two files based on PCID availability
6. Saves:
   - `malaysia_pcid_mapped.csv` (WITH PCID)
   - `malaysia_pcid_not_mapped.csv` (WITHOUT PCID)

**Expected Runtime**: 10-20 seconds
**Expected Output**: Two CSV files

### How to Run

1. **Double-click** `run_scripts.bat`

2. **Wait for completion**:
   - Script 01: ~1 minute
   - Script 02: **2-4 hours** ⏰
   - Script 03: ~10 seconds
   - Script 04: ~2 minutes
   - Script 05: ~15 seconds

3. **Check results**:
   - Open `Output/execution_log.txt` for detailed log
   - Check `Output/malaysia_pcid_mapped.csv` (final output)

### Execution Flow Diagram

```
┌────────────────────────────────────────────────────────────┐
│                    START: run_scripts.bat                  │
└────────────────────────────────────────────────────────────┘
                            ↓
┌────────────────────────────────────────────────────────────┐
│  Backup existing Output/ → Backup/backup_YYYYMMDD_HHMMSS  │
└────────────────────────────────────────────────────────────┘
                            ↓
┌────────────────────────────────────────────────────────────┐
│  Script 01: MyPriMe                                        │
│  → Output/malaysia_drug_prices_view_all.csv                │
│  (Registration Numbers + Prices)                           │
└────────────────────────────────────────────────────────────┘
                            ↓
┌────────────────────────────────────────────────────────────┐
│  Script 02: QUEST3+ Detail Pages                           │
│  FOR EACH Registration Number:                             │
│    → Fetch Product Name, Holder, Manufacturer             │
│  → Output/quest3_product_details.csv                       │
│  (Complete Company Info)                                   │
└────────────────────────────────────────────────────────────┘
                            ↓
┌────────────────────────────────────────────────────────────┐
│  Script 03: Consolidate                                    │
│  → Standardize column names                                │
│  → Remove missing/duplicate data                           │
│  → Output/consolidated_products.csv                        │
└────────────────────────────────────────────────────────────┘
                            ↓
┌────────────────────────────────────────────────────────────┐
│  Script 04: FUKKM                                          │
│  → Scrape fully reimbursable drugs list                    │
│  → Output/malaysia_fully_reimbursable_drugs.csv            │
│  (Generic Names for reimbursable matching)                 │
└────────────────────────────────────────────────────────────┘
                            ↓
┌────────────────────────────────────────────────────────────┐
│  Script 05: Generate PCID Mapped Report                    │
│  JOIN:                                                      │
│    - Prices (Script 01)                                    │
│    - Company Info (Script 03)                              │
│    - PCID Mapping (Input/Malaysia_PCID.csv)                │
│    - Reimbursable List (Script 04)                         │
│  ↓                                                          │
│  CALCULATE:                                                 │
│    - Reimbursable Status (match Generic Name)              │
│    - VAT (0% for Malaysia)                                 │
│  ↓                                                          │
│  SPLIT & SAVE:                                              │
│    - malaysia_pcid_mapped.csv (PCID found)                 │
│    - malaysia_pcid_not_mapped.csv (PCID not found)         │
└────────────────────────────────────────────────────────────┘
                            ↓
┌────────────────────────────────────────────────────────────┐
│  Log Summary → Output/execution_log.txt                    │
│  COMPLETE!                                                  │
└────────────────────────────────────────────────────────────┘
```

---

## Output Files

### Final Outputs (Script 05)

#### 1. malaysia_pcid_mapped.csv
**Description**: Products WITH PCID mapping
**Use**: Primary analysis file with complete pricing and PCID data
**Key Columns**:
- `PCID Mapping` - Product Code Identifier
- `Registration No` (LOCAL_PACK_CODE) - MAL... number
- `Company` / `Holder` - Manufacturer/Holder
- `Product Group` - Brand Name
- `Generic Name` - Active ingredient
- `Pack Unit` - Unit type (tablet, capsule, etc.)
- `Pack Size` - Quantity per pack
- `Unit Price` - Price per individual unit
- `Public with VAT Price` - Retail price per pack
- `VAT Percent` - 0.0% (medicines are zero-rated)
- `Reimbursable Status` - "FULLY REIMBURSABLE" or "NON REIMBURSABLE"
- `Reimbursable Rate` - "100.00%" or "0.00%"
- `Copayment Percent` - "0.00%" or "100.00%"

**Reimbursable Logic**:
- IF Generic Name matches FUKKM list → FULLY REIMBURSABLE (100% covered)
- ELSE → NON REIMBURSABLE (100% copayment)

#### 2. malaysia_pcid_not_mapped.csv
**Description**: Products WITHOUT PCID mapping
**Use**: Identify products needing PCID assignment
**Same structure** as mapped file, but `PCID Mapping` column is empty

**Action Required**:
- Review these products
- Add to `Input/Malaysia_PCID.csv` for future runs

### Intermediate Outputs

#### malaysia_drug_prices_view_all.csv (Script 01)
**Columns**:
- Nombor Pendaftaran Produk/ Product Registration Number
- Nama Generik/ Generic Name
- Nama Dagangan/ Brand Name
- Deskripsi Pembungkusan (Per Pek)/ Packaging Description
- Unit (SKU)
- Kuantiti/ Quantity (SKU)
- Harga Runcit Per Unit SKU
- Harga Runcit Per Pek
- Tahun Kemaskini Harga/ Price Updated Year

#### quest3_product_details.csv (Script 02)
**Columns**:
- Registration No
- Product Name
- Holder
- Holder Address
- Manufacturer
- Manufacturer Address
- Phone No

#### consolidated_products.csv (Script 03)
**Columns**:
- Registration No / Notification No
- Product Name
- Holder

#### malaysia_fully_reimbursable_drugs.csv (Script 04)
**Columns**:
- # (sequence number)
- Generic Name
- MDC (Medicine Data Code)
- Category
- Indications
- Pres. Restrictions
- Dosage
- _source_page (URL of source page)

### Logs & Backups

#### execution_log.txt
**Location**: `Output/execution_log.txt`
**Contents**:
```
========================================
Script Execution Log
Execution Date: 24/12/2025 14:30:22
========================================

[1/5] Running: 01_Product_Registration_Number.py
Start Time: 14:30:25
...
[SUCCESS] Script 01 completed
End Time: 14:31:15
Output file: malaysia_drug_prices_view_all.csv (approx. 3,245 lines)

[2/5] Running: 02_Product_Details.py
Start Time: 14:31:20
...
[SUCCESS] Script 02 completed
End Time: 17:45:10
Output file: quest3_product_details.csv (approx. 3,244 lines)

...

========================================
Execution Summary
========================================
Status: ALL SCRIPTS COMPLETED SUCCESSFULLY
End Time: 24/12/2025 17:48:35

Output Files:
- malaysia_drug_prices_view_all.csv
- quest3_product_details.csv
- consolidated_products.csv
- malaysia_fully_reimbursable_drugs.csv
- malaysia_pcid_mapped.csv (MAPPED records)
- malaysia_pcid_not_mapped.csv (NOT MAPPED records)
```

#### Backup Folder
**Location**: `Backup/backup_YYYYMMDD_HHMMSS/`
**Contents**: Previous run's output files
**Purpose**: Rollback in case of data loss

---

## Troubleshooting & Debugging

### Common Issues

#### Issue 1: Script fails immediately
**Error**: "Python is not installed or not in PATH"
**Solution**:
1. Install Python 3.8+ from https://python.org
2. During installation, check "Add Python to PATH"
3. Restart computer
4. Run `setup.bat` again

#### Issue 2: Playwright browser doesn't open
**Error**: "Playwright browsers not found"
**Solution**:
```batch
python -m playwright install chromium
```
Or run `setup.bat` again

#### Issue 3: Script 02 times out frequently
**Error**: "[TIMEOUT] Could not load page for MAL..."
**Possible Causes**:
- Slow internet connection
- QUEST3+ server is slow/down
- Product doesn't exist

**Solution**:
1. Check internet connection
2. Increase timeout in script (line 62): `timeout=60000` (60 seconds)
3. Re-run - script will skip failed products

#### Issue 4: Missing company names in output
**Error**: Company column is empty for some products
**Cause**: Product detail page didn't load in Script 02
**Solution**:
1. Check `quest3_product_details.csv` - find products with empty Holder
2. Manually visit: `https://quest3plus.bpfk.gov.my/pmo2/detail.php?type=product&id={REG_NO}`
3. If page exists, re-run Script 02

#### Issue 5: No PCID mapping found
**Error**: All products in `malaysia_pcid_not_mapped.csv`
**Cause**: `Input/Malaysia_PCID.csv` is missing or incorrect format
**Solution**:
1. Verify file exists: `Input/Malaysia_PCID.csv`
2. Check format:
   ```csv
   LOCAL_PACK_CODE,PCID Mapping
   MAL20002297XR,1837965
   ```
3. Ensure registration numbers match (case-sensitive)

#### Issue 6: Script 01 shows "No rows scraped"
**Error**: Table selector may have changed
**Cause**: MyPriMe website structure changed
**Solution**:
1. Open https://pharmacy.moh.gov.my/ms/apps/drug-price manually
2. Click "View All"
3. Inspect the table element
4. Update `table.tinytable` selector in script if needed
5. Contact support

#### Issue 7: Permission denied error
**Error**: "PermissionError: [WinError 32]"
**Cause**: Output file is open in Excel/another program
**Solution**:
1. Close all Excel files
2. Close any programs accessing Output folder
3. Re-run script

### Debug Mode

To run a single script for debugging:

1. **Open Command Prompt**:
   ```batch
   cd "D:\quad99\Scappers\2. Malaysia\Script"
   ```

2. **Run individual script**:
   ```batch
   python 01_Product_Registration_Number.py
   python 02_Product_Details.py
   python 03_Consolidate_Results.py
   python 04_Get_Fully_Reimbursable.py
   python 05_Generate_PCID_Mapped.py
   ```

3. **Check detailed error messages** in console

### Enable Headless Mode (Faster, No Browser Window)

**Script 01** (`01_Product_Registration_Number.py`):
```python
# Line 82: Change headless=False to headless=True
options.add_argument("--headless")
```

**Script 02** (`02_Product_Details.py`):
```python
# Line 162: Change headless=False to headless=True
browser = p.chromium.launch(headless=True)
```

**Note**: Only use headless mode after confirming scripts work correctly

---

## Testing & Validation

### Pre-Run Checklist

- [ ] Python 3.8+ installed
- [ ] `setup.bat` executed successfully
- [ ] `Input/Malaysia_PCID.csv` file exists and has correct format
- [ ] Internet connection is stable
- [ ] At least 500MB free disk space
- [ ] No files in `Output/` folder are open in other programs

### Test Run (Quick Validation)

To test without processing all products:

1. **Limit Script 01 to single page** (modify line 66-67):
   ```python
   # Comment out "View All" click
   # view_all_btn.click()
   ```
   This will test with ~20 products only

2. **Run all scripts**: `run_scripts.bat`

3. **Verify outputs**:
   - Check all 6 output files exist in `Output/`
   - Open `malaysia_pcid_mapped.csv` in Excel
   - Verify columns are populated

4. **Restore for full run**:
   - Uncomment the "View All" line
   - Delete test outputs
   - Run full execution

### Post-Execution Validation

#### Validation 1: Check Execution Log
```batch
Open: Output\execution_log.txt
Verify: Status: ALL SCRIPTS COMPLETED SUCCESSFULLY
```

#### Validation 2: Verify Output Counts
```
Expected approximate counts:
- malaysia_drug_prices_view_all.csv: 3,000-5,000 rows
- quest3_product_details.csv: Same as above
- consolidated_products.csv: Slightly less (filtered)
- malaysia_fully_reimbursable_drugs.csv: 1,000-2,000 rows
- malaysia_pcid_mapped.csv: Depends on PCID coverage
- malaysia_pcid_not_mapped.csv: Remaining products
```

#### Validation 3: Data Quality Checks

**Check 1: Company Names**
```excel
Open: malaysia_pcid_mapped.csv
Filter: Company column → (Blanks)
Expected: 0 blank rows
```

**Check 2: Price Data**
```excel
Open: malaysia_pcid_mapped.csv
Verify: "Unit Price" and "Public with VAT Price" have values
```

**Check 3: Reimbursable Status**
```excel
Open: malaysia_pcid_mapped.csv
Filter: Reimbursable Status column
Expected values: Only "FULLY REIMBURSABLE" or "NON REIMBURSABLE"
```

**Check 4: VAT Calculation**
```excel
Open: malaysia_pcid_mapped.csv
Verify: VAT Percent = 0.0 for all rows
Verify: Public without VAT Price = Public with VAT Price
```

**Check 5: PCID Mapping**
```excel
Open: malaysia_pcid_mapped.csv
Verify: PCID Mapping column has numeric values (no blanks)

Open: malaysia_pcid_not_mapped.csv
Verify: PCID Mapping column is blank for all rows
```

### Sample Data Validation

**Expected row in malaysia_pcid_mapped.csv**:
```
PCID Mapping: 1837965
Registration No: MAL04010359XZ
Company: HOVID BERHAD
Product Group: FLAVETTES VITAMIN C ORANGE 250MG
Generic Name: Ascorbic Acid 250 mg tablet
Pack Unit: tablet
Pack Size: 100
Unit Price: 0.39
Public with VAT Price: 38.50
VAT Percent: 0.0
Reimbursable Status: NON REIMBURSABLE (or FULLY REIMBURSABLE if in FUKKM)
Reimbursable Rate: 0.00% (or 100.00%)
Copayment Percent: 100.00% (or 0.00%)
```

---

## FAQ

### Q1: How long does the complete execution take?
**A**: Approximately **3-5 hours total**:
- Script 01: ~1 minute
- Script 02: **2-4 hours** (longest)
- Script 03: ~10 seconds
- Script 04: ~2 minutes
- Script 05: ~15 seconds

### Q2: Can I run scripts separately?
**A**: Yes! Run individual scripts from Command Prompt:
```batch
cd Script
python 01_Product_Registration_Number.py
```
However, each script depends on the previous script's output.

### Q3: Can I pause and resume execution?
**A**: Not recommended. If you must:
1. Stop execution (Ctrl+C)
2. Note which script was running
3. Resume by running remaining scripts manually in order

### Q4: How often should I run this?
**A**: Depends on your needs:
- **Monthly**: For price updates (MyPriMe updates periodically)
- **Quarterly**: For new product registrations
- **After PCID updates**: When new PCIDs are added to Input/Malaysia_PCID.csv

### Q5: What if a website structure changes?
**A**: The script will fail. Solutions:
1. Check `execution_log.txt` for errors
2. Contact technical support with error details
3. Script may need updating for new HTML structure

### Q6: Can I run this on Mac/Linux?
**A**: With modifications:
- Batch files (.bat) need conversion to shell scripts (.sh)
- Path separators may differ
- Python code should work as-is

### Q7: How do I add new PCIDs?
**A**:
1. Open `Input/Malaysia_PCID.csv`
2. Add new rows with format:
   ```csv
   LOCAL_PACK_CODE,PCID Mapping
   MAL20002297XR,1837965
   ```
3. Save and re-run Script 05 only

### Q8: What if I get "Rate Limited" errors?
**A**: Increase delay between requests:
- Script 02 line 185: `time.sleep(5)` (increase from 2 to 5 seconds)
- Script 04 line 190: `time.sleep(1)` (increase if needed)

### Q9: Can I export to Excel format?
**A**: Yes! Script 05 accepts `--out` parameter:
```batch
cd Script
python 05_Generate_PCID_Mapped.py --out-mapped ../Output/malaysia_pcid_mapped.xlsx
```
Or open CSV in Excel and "Save As" .xlsx

### Q10: How do I get technical support?
**A**: Contact developer with:
- `execution_log.txt` file
- Screenshot of error
- Description of what happened
- Output of: `python --version`

---

## Maintenance & Updates

### Regular Maintenance

**Monthly**:
- [ ] Run full execution to get latest prices
- [ ] Review execution log for errors
- [ ] Update `Input/Malaysia_PCID.csv` with new mappings

**Quarterly**:
- [ ] Archive old backups (delete Backup folders >3 months old)
- [ ] Review unmapped products in `malaysia_pcid_not_mapped.csv`
- [ ] Update Python packages: `pip install --upgrade playwright selenium pandas`

### Version History

**Version 2.0** (Current)
- NEW: Direct QUEST3+ detail page scraping (Script 02)
- Complete company information for ALL products
- Removed keyword-based search
- Two separate output files (mapped/not mapped)
- VAT handling (0% for Malaysia)
- Improved logging and error handling

**Version 1.0** (Legacy)
- Keyword-based QUEST3+ search
- Missing company data for some products
- Single output file

---

## Contact & Support

For questions, issues, or feature requests:
- **Email**: [Your Email]
- **Documentation**: USER_MANUAL.md (this file)
- **Logs**: Always attach `Output/execution_log.txt` when reporting issues

---

## Appendix

### File Size Expectations

| File | Approximate Size |
|------|------------------|
| malaysia_drug_prices_view_all.csv | 500KB - 1MB |
| quest3_product_details.csv | 400KB - 800KB |
| consolidated_products.csv | 200KB - 400KB |
| malaysia_fully_reimbursable_drugs.csv | 800KB - 1MB |
| malaysia_pcid_mapped.csv | 1MB - 2MB |
| malaysia_pcid_not_mapped.csv | Variable |
| execution_log.txt | 10KB - 50KB |

### Column Reference - Final Output

Complete list of columns in `malaysia_pcid_mapped.csv`:

1. PCID Mapping
2. Package Number
3. Country (MALAYSIA)
4. Company
5. Product Group
6. Local Product Name
7. Generic Name
8. Description
9. Indication
10. Pack Size
11. Effective Start Date
12. Effective End Date
13. Currency (MYR)
14. Ex Factory Wholesale Price
15. Ex Factory Wholesale Price Less Rebate
16. Ex Factory Hospital Price
17. Ex Factory Hospital Price Less Rebate
18. Ex Factory to Pharmacy Price
19. Pharmacy Purchase Price
20. Pharmacy Purchase Price Less Rebate
21. Public without VAT Price ✅
22. Public Without VAT Price Less Rebate
23. Public with VAT Price ✅
24. Public With VAT Price Less Rebate
25. VAT Percent ✅ (0.0%)
26. Reimbursable Status ✅
27. Reimbursable Price
28. Reimbursable Rate ✅
29. Reimbursable Notes
30. Copayment Value
31. Copayment Percent ✅
32. Margin Rule
33. Package Notes
34. Discontinued
35. Region (MALAYSIA)
36. WHO ATC Code
37. Therapeutic Areas
38. Presentation
39. Marketing Authority
40. Local Pack Description ✅
41. Formulation
42. Fill Unit
43. Fill Size
44. Pack Unit ✅
45. Strength
46. Strength Unit
47. Brand Type
48. Import Type
49. Combination Molecule
50. Source (PRICENTRIC)
51. LOCAL_PACK_CODE ✅
52. Unit Price ✅

✅ = Populated with actual data
Others = May be blank/NaN if not available from sources

---

**End of User Manual**

*Last Updated: December 24, 2025*
*Version: 2.0*
