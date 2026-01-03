# Scraper Platform

A unified platform for running multiple pharmaceutical data scrapers with a centralized GUI, configuration management, and shared utilities.

## Overview

This platform supports three scrapers:
- **Argentina** - AlfaBeta pharmaceutical data extraction
- **CanadaQuebec** - RAMQ PDF processing and AI extraction
- **Malaysia** - MyPriMe and QUEST3+ data scraping

## Features

- **Unified GUI** - Single interface for all scrapers
- **Centralized Configuration** - JSON-based config files per scraper
- **Shared Utilities** - Common code to reduce duplication
- **Platform Config** - Centralized path and config management
- **Documentation** - Comprehensive docs for each scraper

## Quick Start

### Prerequisites

- Python 3.8+
- Chrome/ChromeDriver (for Malaysia scraper)
- Required Python packages (install via pip)

### Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Configure scrapers:
   - Copy `config/*.env.example` to `config/*.env.json`
   - Fill in configuration values
   - Add API keys to `secrets` section

### Running the GUI

```bash
python scraper_gui.py
```

Or use the batch file:
```batch
run_gui.bat
```

### Running Individual Scrapers

Navigate to the scraper directory and run:
```batch
run_pipeline.bat
```

## Project Structure

```
Scappers/
├── config/                 # Configuration files
│   ├── Argentina.env.json
│   ├── CanadaQuebec.env.json
│   ├── Malaysia.env.json
│   └── platform.json
├── core/                   # Shared utilities
│   ├── __init__.py
│   ├── config_manager.py
│   └── shared_utils.py
├── scripts/                # Scraper scripts
│   ├── Argentina/
│   ├── CanadaQuebec/
│   └── Malaysia/
├── doc/                    # Documentation
│   ├── Argentina/
│   ├── CanadaQuebec/
│   └── Malaysia/
├── input/                  # Input files
├── output/                 # Output files
├── backups/               # Backup files
├── scraper_gui.py         # Main GUI
├── platform_config.py     # Platform configuration
└── README.md              # This file
```

## Configuration

All scrapers use JSON configuration files in `config/` directory. Configuration follows the Malaysia format with script-specific prefixes:

- `SCRIPT_00_*` - Backup and clean
- `SCRIPT_01_*` - First processing step
- `SCRIPT_02_*` - Second processing step
- etc.

### Configuration Structure

```json
{
  "scraper": {
    "id": "ScraperName",
    "enabled": true
  },
  "config": {
    "SCRIPT_01_SETTING": "value",
    "SCRIPT_02_SETTING": "value"
  },
  "secrets": {
    "API_KEY": "secret_value",
    "PASSWORD": "secret_value"
  }
}
```

## Scrapers

### Argentina

Extracts pharmaceutical data from AlfaBeta.

**Documentation:** [doc/Argentina/README.md](doc/Argentina/README.md)

**Steps:**
1. Backup and Clean
2. Get Product List
3. Scrape Products
4. Translate Using Dictionary
5. Generate Output
6. PCID Missing

### CanadaQuebec

Processes RAMQ PDFs and extracts data using AI.

**Documentation:** [doc/CanadaQuebec/README.md](doc/CanadaQuebec/README.md)

**Steps:**
1. Backup and Clean
2. Split PDF into Annexes
3. Validate PDF Structure (optional)
4. Extract Annexe IV.1
5. Extract Annexe IV.2
6. Extract Annexe V
7. Merge All Annexes

### Malaysia

Scrapes data from MyPriMe and QUEST3+.

**Documentation:** [doc/Malaysia/README.md](doc/Malaysia/README.md)

**Steps:**
1. Backup and Clean
2. Product Registration Number
3. Product Details
4. Consolidate Results
5. Get Fully Reimbursable
6. Generate PCID Mapped

## Shared Utilities

The `core/shared_utils.py` module provides common functionality:

- `backup_output_folder()` - Backup output directory
- `clean_output_folder()` - Clean output directory
- `get_latest_modification_time()` - Get latest file modification time

## Platform Configuration

The `platform_config.py` module provides centralized path and configuration management:

- **PathManager** - Manages input, output, backup, and export directories
- **ConfigResolver** - Resolves configuration values from JSON files
- **Scraper-specific paths** - Each scraper has its own directories

## Development

### Code Standards

- All scripts follow consistent structure
- Configuration values in JSON files (no hardcoded values)
- Shared utilities for common operations
- Comprehensive error handling
- Consistent logging

### Adding a New Scraper

1. Create scraper directory in `scripts/`
2. Create `config_loader.py` following existing pattern
3. Create configuration file in `config/`
4. Add scraper to `scraper_gui.py`
5. Create documentation in `doc/`

## Troubleshooting

### Common Issues

1. **Import Errors**
   - Ensure repo root is in Python path
   - Check `sys.path.insert()` calls in scripts
   - Verify `core/` module is accessible

2. **Configuration Errors**
   - Verify JSON files are valid
   - Check configuration keys match script expectations
   - Ensure secrets are in `secrets` section

3. **Path Errors**
   - Verify platform_config is working
   - Check directory permissions
   - Ensure directories exist or can be created

## License

Proprietary

## Support

For issues or questions, refer to individual scraper documentation:
- [Argentina Documentation](doc/Argentina/README.md)
- [CanadaQuebec Documentation](doc/CanadaQuebec/README.md)
- [Malaysia Documentation](doc/Malaysia/README.md)

