# Tender Chile Scraper Documentation

## Overview

The Tender Chile scraper extracts tender and award data from Chile's MercadoPublico public procurement platform (www.mercadopublico.cl). It processes tender lists provided by clients, extracts detailed tender information, bidder data, and award results, then produces a final CSV in EVERSANA format.

## Data Source

- **Website**: [MercadoPublico](https://www.mercadopublico.cl)
- **Data Type**: Public procurement tenders and awards
- **Country**: Chile

## Pipeline Steps

### Step 0: Backup and Clean
- **Script**: `00_backup_and_clean.py`
- **Description**: Creates a timestamped backup of the output folder and cleans it for a fresh run
- **Output**: Backup created in `backups/Tender_Chile/`

### Step 1: Get Redirect URLs
- **Script**: `01_get_redirect_urls.py`
- **Description**: Reads the tender list CSV (CN Document Numbers), builds DetailsAcquisition URLs, and captures redirect URLs with `qs` parameters needed for subsequent steps
- **Input**: `input/Tender_Chile/tender_list.csv` (or `TenderList.csv`)
- **Output**: `output/Tender_Chile/tender_redirect_urls.csv`
- **Required Columns in Input**: `CN Document Number` or `IDLicitacion` or URL column

### Step 2: Extract Tender Details
- **Script**: `02_extract_tender_details.py`
- **Description**: Extracts detailed tender and lot information from MercadoPublico tender pages
- **Input**: `output/Tender_Chile/tender_redirect_urls.csv`
- **Output**: `output/Tender_Chile/tender_details.csv`
- **Extracted Fields**:
  - Tender ID
  - Tender Title
  - Tendering Authority
  - Province
  - Closing Date
  - Price/Quality/Other Evaluation Ratios
  - Lot Number, Unique Lot ID
  - Generic Name, Lot Title
  - Quantity
  - Source URL

### Step 3: Extract Tender Awards
- **Script**: `03_extract_tender_awards.py`
- **Description**: Extracts supplier bid and award information from MercadoPublico award pages
- **Input**: `output/Tender_Chile/tender_redirect_urls.csv`
- **Outputs**:
  - `output/Tender_Chile/mercadopublico_supplier_rows.csv` - Individual supplier bid rows
  - `output/Tender_Chile/mercadopublico_lot_summary.csv` - Aggregated lot award summaries
- **Extracted Fields**:
  - Award Date
  - Lot Number
  - UN Classification Code
  - Item Title
  - Buyer Specifications
  - Supplier Name
  - Unit Price Offer
  - Awarded Quantity
  - Total Net Awarded
  - Award Status (Awarded/Not Awarded)

### Step 4: Merge Final CSV
- **Script**: `04_merge_final_csv.py`
- **Description**: Merges tender details, supplier data, and input metadata into the final EVERSANA-format CSV
- **Inputs**:
  - `input/Tender_Chile/tender_list.csv`
  - `output/Tender_Chile/tender_details.csv`
  - `output/Tender_Chile/mercadopublico_supplier_rows.csv`
- **Output**: `output/Tender_Chile/final_tender_data.csv`

## Configuration

Configuration is stored in `config/Tender_Chile.env.json`:

```json
{
  "scraper": {
    "id": "Tender_Chile",
    "enabled": true
  },
  "config": {
    "MAX_TENDERS": 100,
    "HEADLESS": true,
    "WAIT_SECONDS": 60
  },
  "secrets": {
    "OPENAI_API_KEY": ""
  }
}
```

### Configuration Options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `MAX_TENDERS` | int | 100 | Maximum number of tenders to process |
| `HEADLESS` | bool | true | Run Chrome in headless mode |
| `WAIT_SECONDS` | int | 60 | Page load timeout in seconds |
| `OPENAI_API_KEY` | string | "" | Optional: For Spanish-to-English translation |

## Input File Requirements

### tender_list.csv

Place this file in `input/Tender_Chile/` with one of these columns:
- `CN Document Number` - Tender ID (e.g., "1057524-8-LE24")
- `IDLicitacion` - Alternative tender ID column
- URL column containing MercadoPublico URLs

Example:
```csv
CN Document Number,Local Currency,Tender Procedure Type
1057524-8-LE24,CLP,Open
1057525-9-LE24,CLP,Open
```

## Usage

### Via GUI
1. Select "Tender_Chile" from the scraper dropdown
2. Click "Resume Pipeline" or "Run Fresh Pipeline"
3. Monitor progress in the console output

### Via Command Line

```bash
cd scripts/Tender- Chile

# Resume from last completed step
python run_pipeline_resume.py

# Start fresh (clear checkpoint)
python run_pipeline_resume.py --fresh

# Start from specific step
python run_pipeline_resume.py --step 2
```

### Via Batch File

```bash
cd scripts/Tender- Chile
run_pipeline.bat
```

## Output Format

The final output (`final_tender_data.csv`) follows the EVERSANA template with these columns:

| Column | Description |
|--------|-------------|
| COUNTRY | Always "CHILE" |
| PROVINCE | Region/Comuna from tender |
| SOURCE | Always "MERCADOPUBLICO" |
| Source Tender Id | CN Document Number |
| Tender Title | Tender name |
| Unique Lot ID | Product code |
| Lot Number | Lot sequence number |
| Sub Lot Number | (Empty) |
| Lot Title | Item description |
| Est Lot Value Local | (Empty) |
| Local Currency | From input file |
| Deadline Date | Closing date |
| TENDERING AUTHORITY | Buyer organization |
| Tendering Authority Type | (Empty) |
| Tender Procedure Type | From input file |
| CN Document Number | Tender ID |
| Original_Publication_Link_Notice | Tender details URL |
| Ceiling Unit Price | From input file |
| MEAT | From input file |
| Price Evaluation ratio | Economic criteria % |
| Quality Evaluation ratio | Technical criteria % |
| Other Evaluation ratio | Other criteria % |
| CAN Document Number | Tender ID |
| Award Date | Date of award |
| Bidder | Supplier name |
| Bid Status Award | YES/NO |
| Lot_Award_Value_Local | Total awarded amount |
| Awarded Unit Price | Unit price |
| Original_Publication_Link_Award | Award page URL |
| Status | AWARDED/PUBLISHED |

## Troubleshooting

### Common Issues

#### No tender URLs found in CSV
- Ensure your input CSV has a `CN Document Number` column
- Check CSV delimiter (auto-detected: comma, semicolon, tab, pipe)
- Verify encoding (UTF-8, UTF-8-BOM, CP1252 supported)

#### Redirect URL capture fails
- Increase `WAIT_SECONDS` in config
- Check if MercadoPublico site is accessible
- Try running with `HEADLESS: false` to debug

#### No items extracted from tender page
- The scraper tries XHR capture first, then HTML fallback
- Some tenders may have no lots/items
- Check `debug_xhr_response.json` if created

#### Award data not found
- Not all tenders have awards (may be in "Published" status)
- Award page requires valid `qs` parameter from redirect

### Lock File Issues

If the pipeline fails to start due to a lock file:
```bash
python cleanup_lock.py
```

Or use the "Clear Lock" button in the GUI.

## Dependencies

- selenium
- webdriver-manager
- beautifulsoup4
- pandas
- lxml
- python-dotenv
- openai (optional, for translation)

## Notes

- **Language**: Tender data is in Spanish. TENDERING AUTHORITY and PROVINCE remain in original language per EVERSANA requirements. Tender Title, Generic Name, and Lot Title can be translated if OpenAI API key is configured.
- **Rate Limiting**: The scraper includes delays between requests to avoid overloading the server.
- **Checkpoint System**: Progress is saved after each step. Use `--fresh` to start over.

## Version History

- **v1.0** (2025-01): Initial implementation with 4-step pipeline
