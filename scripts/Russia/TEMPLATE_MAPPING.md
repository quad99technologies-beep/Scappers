# Russia Data Export Templates - Field Mapping

This document describes the field mapping from scraped Russia data to standardized export templates.

## Overview

The Russia pipeline now includes a formatting step (Step 4) that converts the scraped and translated data into standardized templates for pricing data and discontinued lists.

**Pipeline Flow:**
1. Step 0: Backup and Clean
2. Step 1: Scrape VED Pricing Data → `russia_farmcom_ved_moscow_region.csv`
3. Step 2: Scrape Excluded List → `russia_farmcom_excluded_list.csv`
4. Step 3: Process and Translate → English versions + date fixes
5. Step 4: Format for Export → Standardized templates

## Input Files (from Step 3)

- `en_russia_farmcom_ved_moscow_region.csv` - English translated VED pricing data
- `en_russia_farmcom_excluded_list.csv` - English translated excluded/discontinued drugs

### Input File Structure (both files)
```
item_id, TN, INN, Manufacturer_Country, Release_Form, EAN, Registered_Price_RUB, Start_Date_Text
```

## Output Templates (Step 4)

### 1. Pricing Data Template (from VED list)

**File:** `russia_pricing_data.csv`

**Columns:**
```
PCID, Country, Company, Product Group, Generic Name, Start Date, Currency,
Ex-Factory Wholesale Price, Local Pack Description, LOCAL_PACK_CODE
```

**Field Mapping:**

| Template Column | Source Field | Notes |
|----------------|--------------|-------|
| PCID | (empty) | To be populated by PCID mapping system |
| Country | "Russia" | Hard-coded value |
| Company | Manufacturer_Country | Manufacturer/company name |
| Product Group | TN | Trade Name (brand name) |
| Generic Name | INN | International Nonproprietary Name |
| Start Date | Start_Date_Text | Already fixed to DD.MM.YYYY format |
| Currency | "RUB" | Russian Ruble |
| Ex-Factory Wholesale Price | Registered_Price_RUB | Price in RUB |
| Local Pack Description | Release_Form | Dosage form and package description |
| LOCAL_PACK_CODE | EAN | European Article Number (barcode) |

**Example:**
```csv
PCID,Country,Company,Product Group,Generic Name,Start Date,Currency,Ex-Factory Wholesale Price,Local Pack Description,LOCAL_PACK_CODE
,Russia,Ebewe Pharma Hes.m.b.H. Nfg. CG - Austria,5-Fluorouracil-Ebewe,Fluorouracil,15.03.2010,RUB,531.51,concentrate for preparation of solution for infusion 50 mg/ml, 10 ml - ampoules (5) - cardboard boxes,9088881324836
```

### 2. Discontinued List Template (from Excluded list)

**File:** `russia_discontinued_list.csv`

**Columns:**
```
PCID, Country, Product Group, Generic Name, Start Date, End Date, Currency,
Ex-Factory Wholesale Price, Local Pack Description, LOCAL_PACK_CODE
```

**Field Mapping:**

| Template Column | Source Field | Notes |
|----------------|--------------|-------|
| PCID | (empty) | To be populated by PCID mapping system |
| Country | "Russia" | Hard-coded value |
| Product Group | TN | Trade Name (brand name) |
| Generic Name | INN | International Nonproprietary Name |
| Start Date | Start_Date_Text | Already fixed to DD.MM.YYYY format |
| End Date | (empty) | Not available in Russia data source |
| Currency | "RUB" | Russian Ruble |
| Ex-Factory Wholesale Price | Registered_Price_RUB | Price in RUB |
| Local Pack Description | Release_Form | Dosage form and package description |
| LOCAL_PACK_CODE | EAN | European Article Number (barcode) |

**Note:** The Discontinued List template includes an "End Date" column that is not available in the Russia farmcom.info excluded list. This field remains empty unless additional data sources provide exclusion/discontinuation dates.

**Example:**
```csv
PCID,Country,Product Group,Generic Name,Start Date,End Date,Currency,Ex-Factory Wholesale Price,Local Pack Description,LOCAL_PACK_CODE
,Russia,Intelence,Etravirine,26.06.2014,,RUB,17486.84,"tablets 200 mg, 60 pcs. - vials (1) - cardboard packs",4601808010282
```

## Central Export Files

The formatted files are also exported to the central exports directory:

**Location:** `exports/Russia/`

**Files:**
- `Russia_Pricing_Data.csv` - Standardized pricing data (VED list)
- `Russia_Discontinued_List.csv` - Standardized discontinued list

## Usage

### Run Full Pipeline (includes formatting)
```bash
cd scripts/Russia
python run_pipeline_resume.py
```

### Run Only Formatting Step (Step 4)
```bash
cd scripts/Russia
python 04_format_for_export.py
```

### Run Pipeline from Step 4
```bash
cd scripts/Russia
python run_pipeline_resume.py --step 4
```

## Notes

1. **PCID Field:** Left empty for PCID mapping to be performed by a separate PCID resolution system
2. **End Date:** Not available in Russia source data for discontinued items
3. **Translation:** Formatting uses English translated data (`en_*.csv` files)
4. **Currency:** Always "RUB" for Russia data
5. **Date Format:** Already normalized to DD.MM.YYYY in Step 3
6. **EAN Codes:** May be empty if barcode data not available or FETCH_EAN disabled

## Template Validation

To ensure data quality, the formatting script:
- Strips whitespace from all fields
- Preserves original data structure
- Maintains data integrity from source files
- Creates consistent column ordering per template specification
