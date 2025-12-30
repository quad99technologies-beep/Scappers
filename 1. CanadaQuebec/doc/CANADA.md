# Canada Quebec Scraper Documentation

## Overview

The Canada Quebec scraper extracts pharmaceutical data from PDF documents. It processes the official Quebec pharmaceutical list PDF (`liste-med.pdf`), splits it into structured annexes, and extracts data from multiple sections (Annexe IV.1, IV.2, and V) to generate comprehensive CSV reports.

## Purpose

- Extract pharmaceutical data from Quebec's official PDF list
- Parse structured annexes (IV.1, IV.2, V)
- Generate standardized CSV outputs
- Provide quality assurance reports
- Create merged reports combining all annexe data

## Input Files

### Required Files

Place this file in `1. CanadaQuebec/input/`:

1. **liste-med.pdf**
   - Official Quebec pharmaceutical list PDF
   - Contains multiple annexes with drug information
   - Must be the current/updated version

### File Locations

- **Platform Input**: `{platform_root}/input/` (if using platform config)
- **Local Input**: `1. CanadaQuebec/input/` (fallback)

## Pipeline Steps

### Step 00: Backup and Clean
**Script**: `00_backup_and_clean.py`

- Creates backup of existing output files
- Cleans output directory for fresh run
- Preserves previous results in backup folder

**Output**: Backup folder with timestamp in `backups/` directory

### Step 01: Split PDF into Annexes
**Script**: `01_split_pdf_into_annexes.py`

- Splits the main PDF into separate annexe files
- Identifies and extracts:
  - Annexe IV.1
  - Annexe IV.2
  - Annexe V
- Saves split PDFs for individual processing

**Input**: `liste-med.pdf`
**Output**: Split PDF files in `output/split_pdf/`

### Step 02: Validate PDF Structure
**Script**: `02_validate_pdf_structure.py`

- Validates PDF structure and format
- Checks for required annexes
- Verifies page structure
- Optional validation step

**Input**: Split PDF files
**Output**: Validation report (if enabled)

### Step 03: Extract Annexe IV.1
**Script**: `03_extract_annexe_iv1.py`

- Extracts data from Annexe IV.1
- Parses pharmaceutical information
- Uses AI/OCR for data extraction (requires OpenAI API key)
- Generates structured CSV output

**Input**: Annexe IV.1 PDF
**Output**: `output/csv/annexe_iv1.csv`

**Requirements**: OpenAI API key in configuration

### Step 04: Extract Annexe IV.2
**Script**: `04_extract_annexe_iv2.py`

- Extracts data from Annexe IV.2
- Parses pharmaceutical information
- Uses AI/OCR for data extraction (requires OpenAI API key)
- Generates structured CSV output

**Input**: Annexe IV.2 PDF
**Output**: `output/csv/annexe_iv2.csv`

**Requirements**: OpenAI API key in configuration

### Step 05: Extract Annexe V
**Script**: `05_extract_annexe_v.py`

- Extracts data from Annexe V
- Parses pharmaceutical information
- Uses AI/OCR for data extraction (requires OpenAI API key)
- Generates structured CSV output

**Input**: Annexe V PDF
**Output**: `output/csv/annexe_v.csv`

**Requirements**: OpenAI API key in configuration

### Step 06: Merge All Annexes
**Script**: `06_merge_all_annexes.py`

- Merges data from all annexes (IV.1, IV.2, V)
- Combines into single comprehensive report
- Standardizes column names and formats
- Generates final merged CSV

**Input**: All annexe CSV files
**Output**: `output/csv/canadaquebecreport_{timestamp}.csv`

## Output Files

### Output Location

**Run-Specific**: `output/runs/CanadaQuebec_{timestamp}/exports/`
**Legacy**: `1. CanadaQuebec/output/`

### Output Structure

```
output/
├── runs/
│   └── CanadaQuebec_{timestamp}/
│       ├── exports/
│       │   ├── annexe_iv1.csv
│       │   ├── annexe_iv2.csv
│       │   ├── annexe_v.csv
│       │   └── canadaquebecreport_{timestamp}.csv
│       ├── artifacts/
│       │   ├── split_pdf/
│       │   └── qa/
│       └── logs/
│           └── run.log
├── csv/
│   ├── annexe_iv1.csv
│   ├── annexe_iv2.csv
│   ├── annexe_v.csv
│   └── canadaquebecreport_{timestamp}.csv
├── split_pdf/
│   ├── annexe_iv1.pdf
│   ├── annexe_iv2.pdf
│   └── annexe_v.pdf
└── qa/
    └── (quality assurance reports)
```

### Output Files

1. **canadaquebecreport_{timestamp}.csv**
   - Final merged report combining all annexes
   - Complete pharmaceutical data
   - Ready for analysis

2. **annexe_iv1.csv, annexe_iv2.csv, annexe_v.csv**
   - Individual annexe extracts
   - Used for validation and debugging

3. **Split PDFs**
   - Individual annexe PDF files
   - Stored in `artifacts/split_pdf/`

4. **QA Reports**
   - Quality assurance reports
   - Validation and error checking
   - Stored in `artifacts/qa/`

## Configuration

### Scraper Configuration

Location: `config/CanadaQuebec.env.json`

```json
{
  "scraper": {
    "id": "CanadaQuebec",
    "enabled": true
  },
  "config": {
    "openai_model": "gpt-4o-mini",
    "extraction_timeout": 300
  },
  "secrets": {
    "OPENAI_API_KEY": "your-openai-api-key-here"
  }
}
```

### Required Secrets

- **OPENAI_API_KEY**: Required for AI-powered PDF extraction
  - Get from: https://platform.openai.com/api-keys
  - Used in steps 03, 04, 05 for data extraction

### Environment Variables

You can set `OPENAI_API_KEY` as an environment variable:
```bash
set OPENAI_API_KEY=your-key-here  # Windows
export OPENAI_API_KEY=your-key-here  # Linux/Mac
```

## Execution

### Using GUI

1. Launch GUI: `run_gui.bat` or `python scraper_gui.py`
2. Select "CanadaQuebec" from scraper dropdown
3. Review pipeline steps
4. Verify `liste-med.pdf` is in input directory
5. Click "Run Full Pipeline"
6. Monitor execution in logs panel
7. Check outputs in Final Output tab

### Using Command Line

```bash
cd "1. CanadaQuebec"
python run_workflow.py
```

Or use batch file:
```bash
cd "1. CanadaQuebec"
run_pipeline.bat
```

## Workflow Details

### Execution Flow

1. **Backup**: Previous outputs backed up automatically
2. **Validation**: Input PDF checked for existence
3. **PDF Splitting**: Main PDF split into annexe files
4. **Structure Validation**: PDF structure verified (optional)
5. **Annexe Extraction**: Each annexe processed individually
   - IV.1 extraction
   - IV.2 extraction
   - V extraction
6. **Merging**: All annexe data combined into final report
7. **Output Collection**: Files organized in run directory

### Extraction Process

- **AI-Powered**: Uses OpenAI API for intelligent extraction
- **OCR Support**: Handles scanned PDFs and images
- **Structured Parsing**: Extracts tabular data accurately
- **Error Handling**: Validates and corrects extraction results

## Troubleshooting

### Common Issues

**1. "Input PDF not found"**
- **Solution**: Verify `liste-med.pdf` is in `input/` directory
- **Check**: File name must match exactly: `liste-med.pdf`

**2. "OPENAI_API_KEY not found"**
- **Solution**: Configure API key in `config/CanadaQuebec.env.json`
- **Check**: Run `python platform_config.py config-check`

**3. "PDF splitting failed"**
- **Solution**: Verify PDF is valid and not corrupted
- **Check**: Ensure PDF contains expected annexe structure

**4. "Extraction timeout"**
- **Solution**: Increase timeout in configuration
- **Check**: Verify OpenAI API is accessible and has credits

**5. "Annexe not found in PDF"**
- **Solution**: Verify PDF contains required annexes (IV.1, IV.2, V)
- **Check**: PDF structure may have changed

### Debugging

**View Logs:**
- Run logs: `output/runs/CanadaQuebec_{timestamp}/logs/run.log`
- Execution logs: GUI log panel (real-time)

**Check Outputs:**
- Review split PDFs in `artifacts/split_pdf/`
- Verify individual annexe CSVs
- Check QA reports for validation issues

**Validate Input:**
- Ensure PDF is not password-protected
- Verify PDF is the correct version
- Check PDF is not corrupted

## Data Format

### Input PDF Structure

The PDF should contain:
- **Annexe IV.1**: First section with pharmaceutical data
- **Annexe IV.2**: Second section with pharmaceutical data
- **Annexe V**: Third section with pharmaceutical data

### Output CSV Format

**canadaquebecreport_{timestamp}.csv:**
```csv
Annexe,ProductName,DIN,Company,Price,Status,...
IV.1,Product A,12345,Company X,10.50,Active,...
IV.2,Product B,67890,Company Y,15.75,Active,...
V,Product C,11111,Company Z,20.00,Active,...
...
```

## Best Practices

1. **Use Current PDF**: Always use the latest version of `liste-med.pdf`
2. **Verify API Key**: Ensure OpenAI API key is valid and has credits
3. **Monitor Extraction**: Watch logs for extraction warnings
4. **Review QA Reports**: Check quality assurance reports for issues
5. **Validate Outputs**: Verify CSV files contain expected data
6. **Backup Regularly**: Automatic backups created, but manual backups recommended

## Performance Considerations

- **PDF Size**: Large PDFs may take longer to process
- **API Rate Limits**: OpenAI API has rate limits, may affect speed
- **Extraction Time**: Each annexe extraction may take several minutes
- **Network Speed**: API calls require stable internet connection

## OpenAI API Usage

### Cost Considerations

- API calls are made for each annexe extraction
- Costs depend on PDF size and complexity
- Monitor API usage in OpenAI dashboard

### Model Selection

Default model: `gpt-4o-mini` (cost-effective)
- Can be changed in configuration
- Other models may provide better accuracy but higher cost

### Rate Limits

- OpenAI API has rate limits
- Large PDFs may hit rate limits
- Automatic retries handle temporary failures

## Updates and Maintenance

### Updating PDF

1. Download latest `liste-med.pdf` from official source
2. Replace file in `input/` directory
3. Run pipeline to extract new data
4. Compare with previous outputs

### Output Management

- Final reports stored in `output/` directory
- Run-specific outputs in `output/runs/`
- Split PDFs preserved in `artifacts/`
- Old runs can be manually cleaned up

## Support

For issues specific to Canada Quebec scraper:
1. Check execution logs in `output/runs/{run_id}/logs/`
2. Verify OpenAI API key is configured correctly
3. Review split PDFs to ensure correct structure
4. Check QA reports for validation issues

## Related Documentation

- **Platform Overview**: `docs/PLATFORM_OVERVIEW.md`
- **User Manual**: `docs/USER_MANUAL.md`
- **Generic Documentation**: `docs/GENERIC.md`

