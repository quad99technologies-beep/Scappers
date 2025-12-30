# Malaysia Scraper Documentation

## Overview

The Malaysia scraper extracts drug pricing and product information from Malaysian pharmaceutical databases. It retrieves product registration numbers, company details, pricing information, and reimbursement status from multiple sources including MyPriMe and QUEST3+.

## Purpose

- Extract drug prices from MyPriMe database
- Retrieve product and company information from QUEST3+
- Consolidate and standardize product details
- Identify fully reimbursable drugs
- Generate PCID-mapped reports for analysis

## Input Files

### Required Files

Place these files in `2. Malaysia/input/`:

1. **Malaysia_PCID.csv**
   - PCID mapping file
   - Maps product identifiers to PCID codes
   - Required columns: Product identifiers, PCID codes

2. **products.csv**
   - Product list file
   - Contains products to be processed
   - Required columns: Product identifiers, product names

### File Locations

- **Platform Input**: `{platform_root}/input/` (if using platform config)
- **Local Input**: `2. Malaysia/input/` (fallback)

## Pipeline Steps

### Step 00: Backup and Clean
**Script**: `00_backup_and_clean.py`

- Creates backup of existing output files
- Cleans output directory for fresh run
- Preserves previous results in backup folder

**Output**: Backup folder with timestamp

### Step 01: Product Registration Number
**Script**: `01_Product_Registration_Number.py`

- Retrieves product registration numbers from MyPriMe
- Searches for drug prices and registration information
- Handles bulk search operations

**Input**: `products.csv`
**Output**: Registration number data, price information

### Step 02: Product Details
**Script**: `02_Product_Details.py`

- Fetches detailed product information from QUEST3+
- Retrieves company/holder information
- Extracts product specifications

**Input**: Registration numbers from Step 01
**Output**: Detailed product and company information

### Step 03: Consolidate Results
**Script**: `03_Consolidate_Results.py`

- Standardizes and cleans product details
- Merges data from multiple sources
- Removes duplicates and normalizes formats
- Validates data consistency

**Input**: Results from Steps 01 and 02
**Output**: Consolidated product data CSV

### Step 04: Get Fully Reimbursable
**Script**: `04_Get_Fully_Reimbursable.py`

- Scrapes fully reimbursable drugs list
- Identifies products eligible for full reimbursement
- Updates product records with reimbursement status

**Input**: Consolidated data from Step 03
**Output**: Reimbursement status information

### Step 05: Generate PCID Mapped
**Script**: `05_Generate_PCID_Mapped.py`

- Maps products to PCID codes
- Generates final PCID-mapped report
- Creates output file ready for analysis

**Input**: Consolidated data and PCID mapping
**Output**: Final PCID-mapped CSV report

## Output Files

### Output Location

**Run-Specific**: `output/runs/Malaysia_{timestamp}/exports/`
**Legacy**: `2. Malaysia/output/`

### Output Files

1. **malaysia_drug_prices_view_all.csv**
   - Complete drug price listing
   - Includes all products with pricing information

2. **PCID-mapped reports**
   - Products mapped to PCID codes
   - Ready for cross-reference analysis

3. **Bulk search CSVs**
   - Intermediate search results
   - Stored in `output/bulk_search_csvs/`

### File Structure

```
output/
├── runs/
│   └── Malaysia_{timestamp}/
│       ├── exports/
│       │   └── malaysia_drug_prices_view_all.csv
│       ├── artifacts/
│       │   └── bulk_search_csvs/
│       └── logs/
│           └── run.log
└── malaysia_*.csv (final reports)
```

## Configuration

### Scraper Configuration

Location: `config/Malaysia.env.json`

```json
{
  "scraper": {
    "id": "Malaysia",
    "enabled": true
  },
  "config": {
    "timeout": 30,
    "retry_attempts": 3
  },
  "secrets": {}
}
```

### Environment Variables

No required secrets for Malaysia scraper (as of current version).

## Execution

### Using GUI

1. Launch GUI: `run_gui.bat` or `python scraper_gui.py`
2. Select "Malaysia" from scraper dropdown
3. Review pipeline steps
4. Click "Run Full Pipeline"
5. Monitor execution in logs panel
6. Check outputs in Final Output tab

### Using Command Line

```bash
cd "2. Malaysia"
python run_workflow.py
```

Or use batch file:
```bash
cd "2. Malaysia"
run_pipeline.bat
```

## Workflow Details

### Execution Flow

1. **Backup**: Previous outputs backed up automatically
2. **Validation**: Input files checked for existence
3. **Registration Lookup**: Product registration numbers retrieved
4. **Detail Extraction**: Product and company details fetched
5. **Consolidation**: Data merged and standardized
6. **Reimbursement Check**: Fully reimbursable drugs identified
7. **PCID Mapping**: Final report generated with PCID codes
8. **Output Collection**: Files organized in run directory

### Data Sources

- **MyPriMe**: Drug price and registration database
- **QUEST3+**: Product and company information database

## Troubleshooting

### Common Issues

**1. "Input file not found"**
- **Solution**: Verify `Malaysia_PCID.csv` and `products.csv` are in `input/` directory
- **Check**: File names must match exactly (case-sensitive on some systems)

**2. "No products found"**
- **Solution**: Verify input CSV files have correct format and data
- **Check**: Ensure product identifiers are valid

**3. "Network timeout"**
- **Solution**: Check internet connection, increase timeout in config
- **Check**: Verify database websites are accessible

**4. "PCID mapping failed"**
- **Solution**: Verify `Malaysia_PCID.csv` has correct mapping data
- **Check**: Ensure PCID codes match expected format

### Debugging

**View Logs:**
- Run logs: `output/runs/Malaysia_{timestamp}/logs/run.log`
- Execution logs: GUI log panel (real-time)

**Check Outputs:**
- Review intermediate files in `artifacts/` directory
- Verify each step's output before proceeding

**Validate Inputs:**
- Check CSV file formats
- Verify required columns exist
- Ensure data is properly formatted

## Data Format

### Input CSV Format

**Malaysia_PCID.csv:**
```csv
ProductID,PCID,ProductName
...
```

**products.csv:**
```csv
ProductID,ProductName,...
...
```

### Output CSV Format

**malaysia_drug_prices_view_all.csv:**
```csv
ProductID,ProductName,RegistrationNumber,Price,Company,PCID,Reimbursable,...
...
```

## Best Practices

1. **Prepare Input Files**: Ensure CSV files are properly formatted
2. **Verify Data**: Check input files contain valid product identifiers
3. **Monitor Execution**: Watch logs for warnings or errors
4. **Review Outputs**: Validate output files after completion
5. **Backup Regularly**: Automatic backups created, but manual backups recommended
6. **Check Network**: Ensure stable internet connection for database access

## Performance Considerations

- **Bulk Operations**: Large product lists may take significant time
- **Network Speed**: Database access speed affects execution time
- **Retry Logic**: Automatic retries for failed requests
- **Timeout Settings**: Configurable timeouts for network operations

## Updates and Maintenance

### Updating Input Files

1. Backup existing input files
2. Update CSV files with new data
3. Verify file formats match expected structure
4. Run pipeline to process updates

### Output Management

- Final reports stored in `output/` directory
- Run-specific outputs in `output/runs/`
- Old runs can be manually cleaned up
- Backups preserved for recovery

## Support

For issues specific to Malaysia scraper:
1. Check execution logs in `output/runs/{run_id}/logs/`
2. Review input file formats
3. Verify network connectivity to databases
4. Check scraper-specific configuration

## Related Documentation

- **Platform Overview**: `docs/PLATFORM_OVERVIEW.md`
- **User Manual**: `docs/USER_MANUAL.md`
- **Generic Documentation**: `docs/GENERIC.md`

