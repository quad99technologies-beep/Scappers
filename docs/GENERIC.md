# Generic Scraper Documentation

## Introduction

This document provides generic information applicable to all scrapers in the platform. It covers common patterns, shared functionality, and best practices that apply across Canada Quebec, Malaysia, and Argentina scrapers.

## Common Workflow Pattern

All scrapers follow a unified workflow pattern:

```
1. Lock Acquisition
   ↓
2. Backup Creation (Mandatory)
   ↓
3. Run Folder Creation
   ↓
4. Input Validation
   ↓
5. Step Execution
   ↓
6. Output Collection
   ↓
7. Lock Release
```

## Shared Components

### 1. Workflow Runner

All scrapers use `shared_workflow_runner.py` which provides:
- Consistent execution flow
- Automatic backup creation
- Run folder organization
- Lock management
- Error handling

### 2. Configuration System

All scrapers use `platform_config.py` for:
- Path management
- Configuration resolution
- Secret management
- Environment variable support

### 3. Path Management

Standard directory structure:
```
{scraper_root}/
├── input/          # Input files
├── output/         # Output files (legacy)
├── script/         # or scripts/ - Python scripts
├── doc/           # or docs/ - Documentation
└── run_workflow.py # Workflow entry point
```

### 4. Backup System

Automatic backups include:
- Configuration files
- Input files
- Previous output files
- Final output reports
- Backup manifest (metadata)

## Common Configuration

### Platform Configuration

Location: `config/platform.json`

```json
{
  "platform": {
    "version": "1.0.0",
    "log_level": "INFO",
    "max_concurrent_runs": 1
  }
}
```

### Scraper Configuration Pattern

Location: `config/{ScraperName}.env.json`

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
    "SECRET_KEY": "***MASKED***"
  }
}
```

## Common Input Patterns

### Input File Location

Scrapers check for input files in this order:
1. Platform input directory: `{platform_root}/input/`
2. Local input directory: `{scraper_root}/input/` or `{scraper_root}/Input/`

### Input Validation

All scrapers validate inputs before execution:
- Check file existence
- Verify file formats
- Validate required fields
- Report missing files clearly

## Common Output Patterns

### Output Structure

All scrapers create outputs in:
```
output/runs/{scraper_name}_{timestamp}/
├── exports/       # Final output files (CSV/XLSX)
├── artifacts/     # Intermediate files
├── logs/          # Execution logs
└── manifest.json # Run metadata
```

### Output File Naming

Final reports follow patterns:
- **Canada Quebec**: `canadaquebecreport_{timestamp}.csv`
- **Malaysia**: `malaysia_*.csv`
- **Argentina**: `alfabeta_report_*.csv`

## Common Execution Methods

### GUI Execution

1. Launch: `run_gui.bat` or `python scraper_gui.py`
2. Select scraper from dropdown
3. Click "Run Full Pipeline"
4. Monitor in logs panel
5. Review outputs in tabs

### Command Line Execution

```bash
cd "{scraper_directory}"
python run_workflow.py
```

Or use batch file:
```bash
cd "{scraper_directory}"
run_pipeline.bat
```

## Common Error Handling

### Lock Management

- **Lock Files**: Stored in `.locks/{scraper}.lock`
- **Stale Lock Detection**: Automatically detects and removes locks >1 hour old
- **Process Tracking**: Tracks process IDs to detect dead processes
- **Graceful Cleanup**: Locks released on error or interruption

### Error Reporting

All scrapers provide:
- Detailed error messages
- Execution logs with timestamps
- Error context (which step failed)
- Recovery suggestions

## Common Logging

### Log Locations

- **Run Logs**: `output/runs/{run_id}/logs/run.log`
- **Platform Logs**: `logs/` directory (if configured)
- **Console Output**: Real-time during execution

### Log Format

```
{timestamp} - {logger_name} - {level} - {message}
```

### Log Levels

- **INFO**: Normal execution flow
- **WARNING**: Non-critical issues
- **ERROR**: Execution failures
- **DEBUG**: Detailed debugging (if enabled)

## Common Troubleshooting

### Issue: Lock File Exists

**Symptoms**: "Another instance is already running"

**Solutions**:
1. Use GUI "Clear Run Lock" button
2. Manually delete `.locks/{scraper}.lock`
3. Wait for previous run to complete
4. Check if process is actually running

### Issue: Input File Not Found

**Symptoms**: Validation fails with "file not found"

**Solutions**:
1. Verify file is in correct `input/` directory
2. Check file name matches exactly (case-sensitive)
3. Verify file exists in platform or local input directory
4. Check file permissions

### Issue: Configuration Error

**Symptoms**: Scraper fails to start or load config

**Solutions**:
1. Run `python platform_config.py config-check`
2. Verify config files exist in `config/` directory
3. Check JSON syntax is valid
4. Verify required secrets are configured

### Issue: Script Execution Failed

**Symptoms**: Step fails with Python error

**Solutions**:
1. Check execution logs for detailed error
2. Verify Python packages are installed
3. Check script file exists and is executable
4. Verify working directory is correct

## Common Best Practices

### 1. Always Use Workflow Runner

- Don't run scripts directly
- Use `run_workflow.py` or GUI
- Ensures proper backup and error handling

### 2. Verify Inputs Before Running

- Check all required files are present
- Verify file formats are correct
- Ensure data is up-to-date

### 3. Monitor Execution

- Watch logs in real-time
- Check for warnings or errors
- Verify each step completes successfully

### 4. Review Outputs

- Always check output files after execution
- Verify data completeness
- Compare with previous runs if applicable

### 5. Manage Backups

- Automatic backups are created
- Manual backups recommended for important data
- Clean up old backups periodically

### 6. Keep Configurations Updated

- Review configuration files regularly
- Update API keys and secrets as needed
- Test configuration changes with small runs

## Common Configuration Options

### Timeout Settings

```json
{
  "config": {
    "timeout": 30,
    "extraction_timeout": 300,
    "network_timeout": 60
  }
}
```

### Retry Settings

```json
{
  "config": {
    "retry_attempts": 3,
    "retry_delay": 5
  }
}
```

### Logging Settings

```json
{
  "config": {
    "log_level": "INFO",
    "log_file": "run.log",
    "console_output": true
  }
}
```

## Common Data Formats

### CSV Format

All scrapers output CSV files with:
- UTF-8 encoding
- Comma-separated values
- Header row
- Consistent column naming

### Manifest Format

Run manifests include:
```json
{
  "run_id": "ScraperName_YYYYMMDD_HHMMSS",
  "scraper_name": "ScraperName",
  "start_time": "ISO timestamp",
  "end_time": "ISO timestamp",
  "status": "completed|failed",
  "inputs": ["file1.csv", "file2.pdf"],
  "outputs": [
    {
      "type": "csv",
      "path": "full/path/to/file.csv",
      "name": "file.csv"
    }
  ],
  "backup_dir": "path/to/backup"
}
```

## Common Performance Considerations

### Execution Time

- Varies by scraper and data volume
- Network operations may add significant time
- Large files take longer to process
- API rate limits may affect speed

### Resource Usage

- Memory: Depends on file sizes
- Disk: Outputs and backups use space
- Network: API calls and web scraping
- CPU: Processing and extraction

### Optimization Tips

1. Process data in batches if possible
2. Use appropriate timeout values
3. Monitor API rate limits
4. Clean up old outputs periodically
5. Use efficient data structures

## Common Maintenance Tasks

### Regular Maintenance

1. **Clean Old Outputs**: Remove old run directories
2. **Clean Old Backups**: Archive or delete old backups
3. **Update Configurations**: Keep API keys and settings current
4. **Review Logs**: Check for recurring issues
5. **Update Dependencies**: Keep Python packages updated

### Backup Management

- Backups are automatic before each run
- Location: `output/backups/{scraper_name}_{timestamp}/`
- Each backup includes manifest.json
- Manual cleanup recommended periodically

### Configuration Updates

- Test configuration changes with small runs
- Backup configuration files before changes
- Verify changes don't break existing workflows
- Document configuration changes

## Common Integration Points

### Platform Integration

All scrapers integrate with:
- Platform configuration system
- Shared workflow runner
- GUI interface
- Path management system

### External Services

Scrapers may use:
- OpenAI API (Canada Quebec)
- Web scraping (Malaysia, Argentina)
- Database access (Malaysia)
- File processing libraries

## Common Security Considerations

### Secret Management

- Secrets stored in `secrets` section of config
- Masked in display (shown as `***MASKED***`)
- Environment variables take precedence
- Never commit secrets to version control

### File Permissions

- Input files: Read-only access sufficient
- Output files: Write access required
- Config files: Read access required
- Lock files: Automatic creation/deletion

## Common Extensibility

### Adding New Scrapers

To add a new scraper:
1. Create scraper directory
2. Implement `ScraperInterface`
3. Create `run_workflow.py`
4. Add to GUI scraper list
5. Create configuration file
6. Add documentation

### Customizing Workflows

- Override methods in `ScraperInterface`
- Add custom validation logic
- Implement custom output formatting
- Add additional processing steps

## Support and Resources

### Documentation

- Platform Overview: `docs/PLATFORM_OVERVIEW.md`
- User Manual: `docs/USER_MANUAL.md`
- Scraper-specific docs in `doc/` or `docs/` folders

### Diagnostic Tools

- `python platform_config.py doctor` - Platform health check
- `python platform_config.py config-check` - Configuration validation
- GUI diagnostic features

### Getting Help

1. Check scraper-specific documentation
2. Review execution logs
3. Run diagnostic commands
4. Check configuration files
5. Review error messages for clues

