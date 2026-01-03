# CanadaQuebec Scraper Documentation

## Overview

The CanadaQuebec scraper extracts pharmaceutical pricing data from RAMQ (Régie de l'assurance maladie du Québec) PDF documents. The scraper processes PDFs containing multiple annexes (IV.1, IV.2, and V) and extracts structured data using AI-powered extraction.

## Workflow

The CanadaQuebec scraper follows a 7-step pipeline:

1. **00_backup_and_clean.py** - Backup existing output and clean for fresh run
2. **01_split_pdf_into_annexes.py** - Split main PDF into individual annexe PDFs
3. **02_validate_pdf_structure.py** - Validate PDF structure (optional QA step)
4. **03_extract_annexe_iv1.py** - Extract data from Annexe IV.1 using AI
5. **04_extract_annexe_iv2.py** - Extract data from Annexe IV.2 using AI
6. **05_extract_annexe_v.py** - Extract data from Annexe V using table extraction
7. **06_merge_all_annexes.py** - Merge all extracted data into final CSV

## Configuration

All configuration is managed through `config/CanadaQuebec.env.json`. The configuration follows the Malaysia format with script-specific prefixes:

- **SCRIPT_00_*** - Backup and clean configuration
- **SCRIPT_01_*** - PDF splitting settings
- **SCRIPT_02_*** - PDF validation thresholds
- **SCRIPT_03_*** - Annexe IV.1 extraction (OpenAI settings)
- **SCRIPT_04_*** - Annexe IV.2 extraction (OpenAI settings)
- **SCRIPT_05_*** - Annexe V extraction (table extraction settings)
- **SCRIPT_06_*** - Merge settings

### Key Configuration Values

- `OPENAI_API_KEY` - OpenAI API key for AI extraction (in secrets section)
- `OPENAI_MODEL` - OpenAI model to use (default: "gpt-4o-mini")
- `OPENAI_TEMPERATURE` - AI temperature setting (default: 0)
- `STATIC_CURRENCY` - Currency code (default: "CAD")
- `STATIC_REGION` - Region name (default: "NORTH AMERICA")
- `X_TOL` / `Y_TOL` - Table extraction tolerances
- `DEFAULT_BAND` - Column detection band configuration

## Input Files

Place the following files in the input directory (`input/`):

- `liste-med.pdf` - Main RAMQ PDF document containing all annexes

## Output Files

The scraper generates the following output files:

### Split PDFs (`split_pdf/`)
- `annexe_iv1.pdf` - Extracted Annexe IV.1
- `annexe_iv2.pdf` - Extracted Annexe IV.2
- `annexe_v.pdf` - Extracted Annexe V
- `index.json` - PDF splitting metadata

### Extracted Data (`csv/`)
- `annexe_iv1_extracted.csv` - Extracted data from IV.1
- `annexe_iv2_extracted.csv` - Extracted data from IV.2
- `annexe_v_extracted.csv` - Extracted data from V
- `annexe_v_extraction_log.txt` - Extraction log for V
- `merge_log.txt` - Merge operation log

### QA Reports (`qa/`)
- `annexe_IV_1_pdf_structure_report.json` - Structure validation report
- `annexe_IV_1_pdf_structure_flags.csv` - Validation flags
- Similar files for IV.2 and V

### Final Output (`exports/`)
- `canadaquebecreport_DDMMYYYY.csv` - Final merged report

## Running the Scraper

### Using the GUI

1. Launch `scraper_gui.py`
2. Select "CanadaQuebec" from the scraper dropdown
3. Click "Run Pipeline" to execute all steps sequentially

### Using Command Line

Navigate to `scripts/CanadaQuebec/` and run:

```batch
run_pipeline.bat
```

Or run individual steps:

```bash
python 00_backup_and_clean.py
python 01_split_pdf_into_annexes.py
python 02_validate_pdf_structure.py  # Optional
python 03_extract_annexe_iv1.py
python 04_extract_annexe_iv2.py
python 05_extract_annexe_v.py
python 06_merge_all_annexes.py
```

## Script Details

### 01_split_pdf_into_annexes.py

Splits the main PDF into individual annexe PDFs.

**Input:** `liste-med.pdf`
**Output:** 
- `annexe_iv1.pdf`
- `annexe_iv2.pdf`
- `annexe_v.pdf`
- `index.json`

**Configuration:**
- `SCRIPT_01_DEFAULT_INPUT_PDF_NAME` - Input PDF filename

**Features:**
- Automatic page range detection
- Metadata extraction
- JSON index generation

### 02_validate_pdf_structure.py

Validates PDF structure and generates QA reports.

**Input:** Split PDF files
**Output:** QA reports in `qa/` directory

**Configuration:**
- `SCRIPT_02_X_TOL` / `SCRIPT_02_Y_TOL` - Coordinate tolerances
- `SCRIPT_02_DEFAULT_BAND` - Column detection bands
- `SCRIPT_02_MIN_PAGES_WITH_DIN` - Minimum pages with DIN
- `SCRIPT_02_MIN_HEADERS_RATIO` - Minimum header ratio
- `SCRIPT_02_MIN_ROW_SHAPE_RATIO` - Minimum row shape ratio
- `SCRIPT_02_MAX_FLAGGED_RATIO` - Maximum flagged ratio

**Note:** This step is optional and warnings don't stop the pipeline.

### 03_extract_annexe_iv1.py

Extracts data from Annexe IV.1 using OpenAI API.

**Input:** `annexe_iv1.pdf`
**Output:** `annexe_iv1_extracted.csv`

**Configuration:**
- `SCRIPT_03_OPENAI_MODEL` - OpenAI model name
- `SCRIPT_03_OPENAI_TEMPERATURE` - Temperature setting
- `SCRIPT_03_STATIC_CURRENCY` - Currency code
- `SCRIPT_03_STATIC_REGION` - Region name

**Features:**
- AI-powered extraction
- Product Group derivation from Brand/Generic
- JSON schema validation
- Automatic retry on API errors

**Data Fields:**
- DIN (8 digits, zero-padded)
- Generic name
- Brand name
- Product Group (derived from Brand or Generic)
- Formulation
- Pack count
- Pack Price
- Unit Price

### 04_extract_annexe_iv2.py

Extracts data from Annexe IV.2 using OpenAI API.

**Input:** `annexe_iv2.pdf`
**Output:** `annexe_iv2_extracted.csv`

**Configuration:**
- `SCRIPT_04_OPENAI_MODEL` - OpenAI model name
- `SCRIPT_04_STATIC_CURRENCY` - Currency code
- `SCRIPT_04_STATIC_REGION` - Region name

**Features:**
- Similar to IV.1 extraction
- Handles "bandelette/strip" format
- Product Group derivation

### 05_extract_annexe_v.py

Extracts data from Annexe V using table extraction.

**Input:** `annexe_v.pdf`
**Output:** `annexe_v_extracted.csv`

**Configuration:**
- `SCRIPT_05_X_TOL` / `SCRIPT_05_Y_TOL` - Coordinate tolerances
- `SCRIPT_05_ANNEXE_V_START_PAGE_1IDX` - Starting page (1-indexed)
- `SCRIPT_05_ANNEXE_V_MAX_ROWS` - Maximum rows to extract (empty = all)

**Features:**
- Table-based extraction
- Coordinate-based cell detection
- Logging of extraction process

### 06_merge_all_annexes.py

Merges all extracted annexe data into final CSV.

**Input:**
- `annexe_iv1_extracted.csv`
- `annexe_iv2_extracted.csv`
- `annexe_v_extracted.csv`

**Output:** `canadaquebecreport_DDMMYYYY.csv`

**Configuration:**
- `SCRIPT_06_FINAL_REPORT_NAME_PREFIX` - Report filename prefix
- `SCRIPT_06_FINAL_REPORT_DATE_FORMAT` - Date format
- `SCRIPT_06_STATIC_CURRENCY` - Currency code
- `SCRIPT_06_STATIC_REGION` - Region name

**Features:**
- Data deduplication
- Column standardization
- Date-based filename
- Automatic export to central output directory

## Product Group Derivation

The scraper uses a deterministic approach to derive "Product Group":

1. **Primary:** Use Brand (row-level product name/presentation)
   - Examples: "Humira (seringue)", "Humira (stylo)", "NovoRapid FlexTouch"

2. **Fallback:** If Brand is empty, extract from parentheses in Generic header
   - Example: "ADALIMUMAB (HUMIRA)" → "HUMIRA"

3. **Final:** If neither available, keep empty string (no data invention)

This logic is implemented in the `infer_product_group()` helper function in scripts 03 and 04.

## Troubleshooting

### Common Issues

1. **OpenAI API Errors**
   - Verify `OPENAI_API_KEY` in config secrets section
   - Check API quota and billing
   - Verify model name is correct

2. **PDF Splitting Failures**
   - Ensure input PDF is valid and not corrupted
   - Check if PDF structure matches expected format
   - Review `index.json` for splitting metadata

3. **Extraction Errors**
   - Check OpenAI API response in logs
   - Verify PDF quality (not scanned images)
   - Review extraction prompts in scripts

4. **Table Extraction Issues (Annexe V)**
   - Adjust `X_TOL` and `Y_TOL` tolerances
   - Check `DEFAULT_BAND` configuration
   - Review extraction log for coordinate issues

5. **Merge Errors**
   - Verify all annexe CSV files exist
   - Check column names match expected schema
   - Review merge log for details

## Dependencies

- pdfplumber - PDF processing
- openai - AI extraction
- pandas - Data manipulation
- Python 3.8+

## Notes

- All configuration values are in `config/CanadaQuebec.env.json`
- Secrets (API keys) are stored in the `secrets` section
- The scraper uses AI extraction for IV.1 and IV.2, table extraction for V
- Product Group is deterministically derived, not AI-invented
- QA validation step is optional and doesn't block the pipeline

