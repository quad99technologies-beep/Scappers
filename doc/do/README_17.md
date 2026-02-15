
# Canada Ontario Scraper

This scraper extracts pharmaceutical data from the Canada Ontario Formulary and EAP price lists.

## Features
- Scrapes product details from the Ontario Formulary website (`01_extract_product_details.py`).
- Extracts Exceptional Access Program (EAP) prices (`02_ontario_eap_prices.py`).
- Generates a final output report matching the standard format (`03_GenerateOutput.py`).
- Includes resume support and backup functionality.

## Prerequisites
- Database schema: Run `postgres/canada_ontario.sql` (or use `deploy_schema.py` which was run during onboarding).
- Environment variables: Configure `config/CanadaOntario.env.json` or rely on defaults.

## Usage
Run the entire pipeline:
```bash
./run_pipeline.bat
```

Or run individual steps:
1. `python 00_backup_and_clean.py`
2. `python 01_extract_product_details.py`
3. `python 02_ontario_eap_prices.py`
4. `python 03_GenerateOutput.py`

## Output
Files are saved to `output/CanadaOntario/` and final reports to `exports/CanadaOntario/`.
