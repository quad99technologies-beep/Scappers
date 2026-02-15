# Scraper Platform

A unified platform for running multiple pharmaceutical data scrapers with a centralized GUI, configuration management, and shared utilities.

## Overview

This platform supports multiple scrapers for pharmaceutical data extraction:
- **Argentina** - AlfaBeta pharmaceutical data extraction
- **Belarus** - Belarus pharmaceutical registry scraping
- **CanadaOntario** - Ontario PDF processing
- **CanadaQuebec** - RAMQ PDF processing and AI extraction
- **India** - Indian pharmaceutical data scraping
- **Malaysia** - MyPriMe and QUEST3+ data scraping
- **Netherlands** - Dutch pharmaceutical data extraction
- **NorthMacedonia** - North Macedonia registry scraping
- **Russia** - Russian pharmaceutical registry
- **Taiwan** - Taiwan pharmaceutical data
- **Tender_Chile** - Chile tender data extraction

## Features

- **Unified GUI** - Single interface for all scrapers (`scraper_gui.py`)
- **Centralized Configuration** - JSON-based config files per scraper
- **Shared Utilities** - Common code in `core/` to reduce duplication
- **Platform Config** - Centralized path and config management
- **Workflow Runner** - Shared workflow execution (`shared_workflow_runner.py`)
- **Telegram Bot** - Notifications and remote control (`telegram_bot.py`)
- **Comprehensive Documentation** - Organized docs in `doc/`

## Quick Start

### Prerequisites

- Python 3.8+
- Chrome/ChromeDriver (for scrapers requiring browser automation)
- pip (comes with Python)

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

Or use the batch file (Windows):
```batch
run_gui.bat
```

### Running Individual Scrapers

Navigate to the scraper directory in `scripts/` and run:
```batch
run_pipeline.bat
```

## Repository Structure

```
Scrappers/
├── archive/                # Archived/one-time utility scripts
├── backups/                # Backup files
├── cache/                  # Cache directory
├── config/                 # Configuration files (.env.json)
├── core/                   # Shared utilities and modules
├── crawlee/                # Crawlee-related files
├── doc/                    # All documentation (see below)
├── exports/                # Export files
├── gui/                    # GUI components
├── input/                  # Input files for scrapers
├── logs/                   # Log files
├── monitoring/             # Monitoring scripts
├── node_modules/           # Node.js dependencies
├── output/                 # Output files from scrapers
├── requirements/           # Platform-specific requirements
├── runs/                   # Run data and checkpoints
├── scrapy_project/         # Scrapy project files
├── scripts/                # Scraper scripts (per platform)
├── sessions/               # Session data
├── sql/                    # SQL files
├── testing/                # Test scripts and utilities
├── README.md               # This file
├── requirements.txt        # Python dependencies
├── scraper_gui.py          # Main GUI application
├── platform_config.py      # Platform configuration
├── shared_workflow_runner.py  # Workflow execution
├── telegram_bot.py         # Telegram bot
├── setup_config.py         # Setup utilities
├── doctor.py               # Diagnostics tool
├── new.py                  # New scraper template
└── stop_workflow.py        # Workflow stop utility
```

## Documentation Structure (`doc/`)

All documentation is organized under `doc/`:

```
doc/
├── README.md                    # Platform overview
├── TELEGRAM_BOT_GUIDE.md        # Telegram bot setup
├── ALL_FEATURES_INTEGRATED.md   # Features summary
├── DOCUMENTATION_INDEX.md       # Documentation index
├── FEATURE_GAP_ANALYSIS.md      # Gap analysis
├── IMPLEMENTATION_CHECKLIST.md  # Implementation checklist
├── IMPLEMENTATION_STATUS.md     # Implementation status
├── INTEGRATION_COMPLETE.md      # Integration status
├── REMAINING_TASKS.md           # Remaining tasks
├── RUSSIA_BELARUS_IMPROVEMENT_SUMMARY.md  # Improvements
├── UPGRADE_SUMMARY.md           # Upgrade summary
├── Argentina/                   # Argentina scraper docs
├── Belarus/                     # Belarus scraper docs
├── CanadaOntario/               # Canada Ontario docs
├── CanadaQuebec/                # Canada Quebec docs
├── India/                       # India scraper docs
├── Malaysia/                    # Malaysia scraper docs
├── Netherlands/                 # Netherlands docs
├── NorthMacedonia/              # North Macedonia docs
├── Russia/                      # Russia scraper docs
├── Taiwan/                      # Taiwan scraper docs
├── Tender_Chile/                # Chile tender docs
├── deployment/                  # Deployment guides
├── general/                     # General/cross-cutting docs
├── gui/                         # GUI documentation
├── implementation/              # Implementation docs
├── project/                     # Project-wide docs
├── run_metrics/                 # Run metrics docs
└── testing/                     # Testing documentation
```

## Configuration

All scrapers use JSON configuration files in `config/` directory:

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

## Key Components

### Main Entry Points
- `scraper_gui.py` - Main GUI for all scrapers
- `shared_workflow_runner.py` - Workflow execution engine
- `telegram_bot.py` - Telegram notifications and control
- `platform_config.py` - Centralized configuration

### Utility Scripts
- `doctor.py` - Diagnostics and health checks
- `setup_config.py` - Configuration setup
- `new.py` - Template for new scrapers
- `stop_workflow.py` - Stop running workflows

### Archive (One-time/Utility Scripts)
The `archive/` folder contains scripts used for one-time operations:
- Database migration scripts
- Fix scripts for specific issues
- Analysis scripts
- Verification utilities

### Testing
The `testing/` folder contains:
- Test scripts for various components
- Database connection tests
- Environment configuration tests
- Validation tests

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

## License

Proprietary

## Support

For issues or questions, refer to individual scraper documentation in `doc/`.
