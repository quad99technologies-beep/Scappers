# Documentation

This directory contains documentation for the Canada Quebec RAMQ Scraper.

## Files

- **OPTIMIZATION_SUMMARY.md** - Complete summary of security and performance optimizations, database integration, and code changes
- **DATABASE_SETUP.md** - Database schema setup instructions and table structure documentation

## Script Numbering

The pipeline uses a consistent numbering scheme:

- **step_00_*** - Utility modules (imported by other steps)
  - `step_00_utils_encoding.py` - Encoding utilities
  - `step_00_db_utils.py` - Database utilities (optional)

- **step_01** - Backup and clean output folder
- **step_02** - Extract legend section from PDF
- **step_03** - Validate PDF structure
- **step_04** - Extract DIN data to CSV
- **step_05** - Normalize CSV data
- **step_06** - Verify encoding
- **step_07** - Transform to standard format

## Quick Start

1. Run the pipeline:
   ```bash
   run_pipeline.bat
   ```

2. Configure database (optional):
   - See `DATABASE_SETUP.md` for database setup
   - Copy `env.example` to `.env` and configure

3. Check output:
   - CSV files: `output/csv/`
   - Reports: `output/qa/`

