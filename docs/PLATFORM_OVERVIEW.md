# Scraper Platform Overview

## Introduction

The Scraper Platform is a comprehensive, unified system for managing and executing multiple web scrapers and data extraction workflows. It provides a centralized GUI interface, consistent workflow orchestration, and robust configuration management for scrapers targeting different regions and data sources.

## Architecture

### Core Components

1. **Platform Configuration System** (`platform_config.py`)
   - Centralized path management
   - Configuration resolution with precedence
   - Environment variable support
   - Secret management

2. **Shared Workflow Runner** (`shared_workflow_runner.py`)
   - Unified workflow orchestration
   - Mandatory backup-first execution
   - Deterministic run folder structure
   - Single-instance locking
   - Consistent logging

3. **GUI Interface** (`scraper_gui.py`)
   - Visual scraper management
   - Real-time execution monitoring
   - Documentation viewer
   - Configuration editor
   - Output file browser

4. **Scraper Modules**
   - **Canada Quebec**: PDF-based pharmaceutical data extraction
   - **Malaysia**: Drug price and product information scraping
   - **Argentina**: Company and product data extraction from AlfaBeta

## Directory Structure

```
Scappers/
├── 1. CanadaQuebec/          # Canada Quebec scraper
│   ├── script/               # Extraction scripts
│   ├── input/                # Input PDF files
│   ├── output/               # Output CSV files
│   └── doc/                  # Documentation
├── 2. Malaysia/              # Malaysia scraper
│   ├── scripts/              # Scraping scripts
│   ├── input/                # Input CSV files
│   ├── output/               # Output CSV files
│   └── docs/                 # Documentation
├── 3. Argentina/             # Argentina scraper
│   ├── script/               # Scraping scripts
│   ├── Input/                # Input files
│   └── doc/                  # Documentation
├── config/                   # Platform configuration
│   ├── platform.json         # Platform-wide settings
│   ├── CanadaQuebec.env.json # Canada Quebec config
│   └── Malaysia.env.json     # Malaysia config
├── output/                   # Centralized output directory
│   ├── backups/              # Automatic backups
│   └── runs/                 # Run-specific outputs
├── scraper_gui.py            # Main GUI application
├── platform_config.py        # Configuration system
└── shared_workflow_runner.py  # Workflow orchestration
```

## Key Features

### 1. Unified Workflow Execution

All scrapers follow a consistent workflow:
1. **Lock Acquisition**: Prevents concurrent runs
2. **Backup Creation**: Automatic backup of inputs, configs, and previous outputs
3. **Run Folder Creation**: Deterministic folder structure for each run
4. **Input Validation**: Verify required input files exist
5. **Step Execution**: Run scraper-specific steps
6. **Output Collection**: Gather and organize output files
7. **Lock Release**: Clean up and release execution lock

### 2. Configuration Management

- **Hierarchical Configuration**: Platform → Scraper → Environment → Runtime
- **Secret Management**: Secure storage of API keys and credentials
- **Environment Variables**: Support for runtime overrides
- **JSON-based**: Human-readable configuration files

### 3. Path Management

All paths are managed centrally through `PathManager`:
- Repository root detection
- Input/output directory resolution
- Backup and run folder creation
- Lock file management

### 4. Backup System

Automatic backups are created before each run:
- Configuration files (platform and scraper-specific)
- Input files
- Previous output files
- Final output reports
- Backup manifest with metadata

### 5. Run Organization

Each execution creates a structured run folder:
```
output/runs/{scraper_name}_{timestamp}/
├── logs/          # Execution logs
├── artifacts/     # Intermediate files
├── exports/       # Final output files
└── manifest.json  # Run metadata
```

## Supported Scrapers

### Canada Quebec
- **Purpose**: Extract pharmaceutical data from PDF documents
- **Input**: `liste-med.pdf`
- **Output**: CSV files with extracted annexe data
- **Steps**: PDF splitting, validation, extraction, merging

### Malaysia
- **Purpose**: Scrape drug prices and product information
- **Input**: PCID mapping CSV, product list CSV
- **Output**: Consolidated drug price reports
- **Steps**: Registration lookup, product details, consolidation, reimbursement status

### Argentina
- **Purpose**: Extract company and product data from AlfaBeta
- **Input**: Company list CSV, dictionary CSV, PCID mapping
- **Output**: Translated product reports
- **Steps**: Company extraction, product listing, scraping, translation, output generation

## Execution Methods

### 1. GUI Execution
- Launch: `run_gui.bat` or `python scraper_gui.py`
- Visual interface for all operations
- Real-time log viewing
- Configuration editing
- Output file browsing

### 2. Command Line Execution
- Direct: `python run_workflow.py` (from scraper directory)
- Batch file: `run_pipeline.bat` (from scraper directory)
- Platform-wide: Use GUI for centralized management

## Configuration Files

### Platform Configuration (`config/platform.json`)
```json
{
  "platform": {
    "version": "1.0.0",
    "log_level": "INFO",
    "max_concurrent_runs": 1
  },
  "paths": {
    "input_base": "input",
    "output_base": "output",
    "cache_base": "cache"
  }
}
```

### Scraper Configuration (`config/{ScraperName}.env.json`)
```json
{
  "scraper": {
    "id": "ScraperName",
    "enabled": true
  },
  "config": {
    "key": "value"
  },
  "secrets": {
    "API_KEY": "***MASKED***"
  }
}
```

## Lock System

The platform uses file-based locking to prevent concurrent executions:
- Lock files stored in `.locks/` directory
- Process ID tracking for stale lock detection
- Automatic cleanup of stale locks (>1 hour old)
- Graceful handling of interrupted processes

## Logging

- **Run Logs**: `output/runs/{run_id}/logs/run.log`
- **Platform Logs**: `logs/` directory (if configured)
- **Console Output**: Real-time during execution
- **GUI Log Viewer**: Integrated log display with search

## Error Handling

- **Input Validation**: Early detection of missing files
- **Step Failures**: Detailed error messages with context
- **Lock Management**: Automatic cleanup on errors
- **Backup Preservation**: Backups retained even on failure

## Best Practices

1. **Always use GUI or workflow runner**: Don't run scripts directly
2. **Check input files**: Ensure required inputs are in place
3. **Review configuration**: Verify settings before execution
4. **Monitor logs**: Watch for warnings and errors
5. **Backup regularly**: Automatic backups are created, but manual backups recommended for important data
6. **One run at a time**: Lock system prevents concurrent runs automatically

## Troubleshooting

### Lock File Issues
- **Problem**: "Another instance is already running"
- **Solution**: Use GUI "Clear Run Lock" button or manually delete `.locks/{scraper}.lock`

### Missing Input Files
- **Problem**: Validation fails
- **Solution**: Check `input/` directory for required files

### Configuration Errors
- **Problem**: Scraper fails to start
- **Solution**: Run `python platform_config.py config-check` to validate configuration

### Path Issues
- **Problem**: Files not found
- **Solution**: Run `python platform_config.py doctor` to check paths

## Future Enhancements

- Additional scraper modules
- Enhanced error recovery
- Parallel execution support
- Web-based interface
- API for programmatic access
- Advanced scheduling and automation

## Support

For issues or questions:
1. Check documentation in scraper-specific `doc/` or `docs/` folders
2. Review execution logs in `output/runs/{run_id}/logs/`
3. Use GUI diagnostic tools (doctor command, config-check)
4. Review platform configuration files

