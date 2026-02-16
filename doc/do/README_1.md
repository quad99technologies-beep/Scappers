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

## Repository layout

- **Root** – Main entry points: `scraper_gui.py`, `run_gui_professional.py`, `shared_workflow_runner.py`, `run_complete_processing.py`, `stop_workflow.py`, `tools/telegram_bot.py`, `platform_config.py`, `setup_config.py`
- **doc/** – All documentation (`.md`); per-platform and general guides
- **testing/** – One-off and test scripts (DB checks, migrations, reset/fix utilities); not part of the main pipeline
- **scripts/** – Per-platform pipeline scripts (Argentina, Malaysia, Russia, India, etc.)
- **core/**, **gui/**, **config/**, **sql/** – Shared code, UI, config, and SQL

## Quick Start

### Prerequisites

- Python 3.8+
- Chrome/ChromeDriver (for Malaysia scraper)
- pip (comes with Python)

### Installation

1. Clone the repository
2. **Dependencies are automatically installed** when you first run the GUI (see below)
3. Configure scrapers:
   - Copy `config/*.env.example` to `config/*.env.json`
   - Fill in configuration values
   - Add API keys to `secrets` section

**Note:** If you prefer to install dependencies manually:
   ```bash
   pip install -r requirements.txt
   ```

### Running the GUI

When you start the GUI, it will automatically:
1. Check for and install missing dependencies from `requirements.txt`
2. Show installation progress in the console
3. Start the GUI application

```bash
python scraper_gui.py
```

Or use the batch file (recommended for Windows):
```batch
run_gui.bat
```

The batch file ensures unbuffered output so you can see dependency installation progress in real-time.

### Running Individual Scrapers

Navigate to the scraper directory and run:
```batch
run_pipeline.bat
```

## Documentation structure (doc/)

All `.md` documentation lives under `doc/` with this layout:

```
doc/
├── README.md               # This file – platform overview and quick start
├── TELEGRAM_BOT_GUIDE.md   # Telegram notifications
├── deployment/             # Deployment guides and checklists ⭐ NEW
│   ├── README.md           # Deployment documentation index
│   ├── DEPLOY_NOW.md       # Quick 3-step deployment (START HERE)
│   ├── DEPLOYMENT_GUIDE.md  # Complete deployment documentation
│   ├── DEPLOYMENT_CHECKLIST.md
│   ├── DEPLOYMENT_COMPLETE.md
│   └── VERIFICATION_REPORT.md
├── implementation/         # Implementation status and features ⭐ NEW
│   ├── README.md           # Implementation documentation index
│   ├── IMPLEMENTATION_COMPLETE.md
│   └── ALL_FEATURES_SUMMARY.md
├── project/                # Project-wide: upgrades, standardization, gap analysis
│   ├── SCRAPER_ONBOARDING_CHECKLIST.md ⭐ Master onboarding checklist
│   ├── SCRAPER_ONBOARDING_QUICK_REFERENCE.md ⭐ Quick reference
│   ├── GAP_ANALYSIS_MALAYSIA_ARGENTINA_NETHERLANDS.md ⭐ Latest gap analysis
│   ├── PROJECT_UPGRADE_DOCUMENT.md
│   ├── STANDARDIZATION_*.md
│   ├── PLATFORM.md
│   └── ...
├── general/                # Cross-cutting: audits, memory fixes, browser, PCID
│   ├── MEMORY_LEAK_FIXES*.md
│   ├── PCID_MAPPING_UNIFICATION.md
│   ├── CHROME_VERSION_FIX.md
│   └── ...
├── run_metrics/            # Run metrics and usage
│   ├── run_metrics_usage.md
│   └── RUN_METRICS_SUMMARY.md
├── gui/                    # GUI guides and enhancements
├── testing/                # Testing and one-off scripts docs
├── Argentina/              # Per-region docs
├── Belarus/
├── CanadaOntario/
├── CanadaQuebec/
├── India/
├── Malaysia/
├── Netherlands/
├── NorthMacedonia/
├── Russia/
├── Taiwan/
└── Tender_Chile/
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
├── scripts/                # Scraper scripts (Argentina, Malaysia, Russia, etc.)
├── doc/                    # All documentation (see "Documentation structure" above)
├── input/                  # Input files
├── output/                 # Output files
├── backups/                # Backup files
├── scraper_gui.py          # Main GUI
├── platform_config.py      # Platform configuration
└── README.md               # Root readme (if present)
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

**Documentation:** [Argentina/README.md](Argentina/README.md)

**Steps:**
1. Backup and Clean
2. Get Product List
3. Scrape Products
4. Translate Using Dictionary
5. Generate Output
6. PCID Missing

### CanadaQuebec

Processes RAMQ PDFs and extracts data using AI.

**Documentation:** [CanadaQuebec/README.md](CanadaQuebec/README.md)

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

**Documentation:** [Malaysia/README.md](Malaysia/README.md)

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
- [Argentina Documentation](Argentina/README.md)
- [CanadaQuebec Documentation](CanadaQuebec/README.md)
- [Malaysia Documentation](Malaysia/README.md)

