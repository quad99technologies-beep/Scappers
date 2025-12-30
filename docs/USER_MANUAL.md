# User Manual

## Getting Started

### Prerequisites

- Python 3.7 or higher
- Required Python packages (install via `pip install -r requirements.txt` if available)
- Windows 10/11 (primary platform, Linux/Mac may work with modifications)

### Initial Setup

1. **Clone or Download** the repository
2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
3. **Configure Platform**:
   - Review `config/platform.json`
   - Create scraper-specific config files in `config/` directory
4. **Prepare Input Files**:
   - Place input files in appropriate scraper `input/` directories
   - See scraper-specific documentation for required file formats

## Launching the Application

### Method 1: GUI (Recommended)

**Windows:**
```bash
run_gui.bat
```

**Manual:**
```bash
python scraper_gui.py
```

The GUI will open in a maximized window with all features accessible.

### Method 2: Command Line

Navigate to a scraper directory and run:
```bash
python run_workflow.py
```

Or use the batch file:
```bash
run_pipeline.bat
```

## GUI Interface Guide

### Main Window Layout

The GUI is divided into several sections:

#### Left Panel - Execution Controls

1. **Scraper Selection**
   - Dropdown to select: CanadaQuebec, Malaysia, or Argentina
   - Selecting a scraper loads its steps and configuration

2. **Pipeline Control**
   - **Run Full Pipeline**: Execute all steps for selected scraper
   - **Clear Run Lock**: Remove lock file if scraper appears stuck

3. **Pipeline Steps**
   - Read-only list of all steps for selected scraper
   - Click a step to view details
   - Use "Explain This Step" button for AI-generated explanations

4. **Step Information**
   - Shows script name, description, and file path
   - Expandable explanation panel (when available)

#### Right Panel - Documentation & Outputs

**Tabs:**

1. **Final Output** (📊)
   - View final report files (CSV/XLSX)
   - Filtered by selected scraper
   - Double-click to open files
   - Search functionality

2. **Configuration** (⚙️)
   - Edit platform `.env` file
   - Load, save, and create from template
   - Shared configuration across all scrapers

3. **Output Files** (📁)
   - Browse all files in latest run directory
   - View file information
   - Open files or folders

4. **Documentation** (📚)
   - View all available documentation
   - Formatted markdown display
   - Auto-refresh on file changes

#### Bottom Panel - Execution Logs

- Real-time log output (black background, yellow text)
- Clear, copy, or save logs
- Auto-scrolls during execution
- Shows execution status and errors

### Running a Scraper

1. **Select Scraper**: Choose from dropdown (CanadaQuebec, Malaysia, Argentina)
2. **Review Steps**: Check the pipeline steps list
3. **Verify Inputs**: Ensure required input files are present
4. **Click "Run Full Pipeline"**: Confirm when prompted
5. **Monitor Progress**: Watch logs in real-time
6. **Review Outputs**: Check Final Output tab after completion

### Viewing Documentation

1. **Select Scraper**: Documentation is filtered by scraper
2. **Open Documentation Tab**: Click the 📚 tab
3. **Select Document**: Choose from dropdown
4. **Read**: Formatted markdown is displayed
5. **Refresh**: Click 🔄 to reload if files change

### Editing Configuration

1. **Open Configuration Tab**: Click ⚙️ tab
2. **Load File**: Click "Load" or file auto-loads
3. **Edit**: Modify configuration in text editor
4. **Save**: Click "Save" to write changes
5. **Create Template**: Use "Create from Template" for new configs

### Viewing Outputs

**Final Output Tab:**
- Shows only final report files (CSV/XLSX)
- Filtered by scraper name pattern
- Displays file count and total size
- Double-click to open in default application

**Output Files Tab:**
- Shows all files in run directory
- Includes logs, artifacts, and exports
- Displays file information (size, modified date)
- Navigate to parent folder if needed

## Command Line Usage

### Running a Workflow

```bash
cd "1. CanadaQuebec"
python run_workflow.py
```

### Platform Diagnostics

**Check Platform Health:**
```bash
python platform_config.py doctor
```

**Validate Configuration:**
```bash
python platform_config.py config-check
```

### Batch File Execution

Each scraper has a `run_pipeline.bat` file:
```bash
cd "2. Malaysia"
run_pipeline.bat
```

## Configuration Management

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

### Scraper Configuration

Location: `config/{ScraperName}.env.json`

**Canada Quebec:**
```json
{
  "scraper": {
    "id": "CanadaQuebec",
    "enabled": true
  },
  "config": {},
  "secrets": {
    "OPENAI_API_KEY": "your-key-here"
  }
}
```

**Malaysia:**
```json
{
  "scraper": {
    "id": "Malaysia",
    "enabled": true
  },
  "config": {},
  "secrets": {}
}
```

**Argentina:**
```json
{
  "scraper": {
    "id": "Argentina",
    "enabled": true
  },
  "config": {},
  "secrets": {
    "ALFABETA_USER": "your-email@example.com",
    "ALFABETA_PASS": "your-password"
  }
}
```

### Environment Variables

You can override configuration using environment variables:
- `OPENAI_API_KEY`: For Canada Quebec
- `ALFABETA_USER`: For Argentina
- `ALFABETA_PASS`: For Argentina

## Input File Preparation

### Canada Quebec

**Required:**
- `input/liste-med.pdf`: Source PDF file

**Location:** `1. CanadaQuebec/input/` or platform input directory

### Malaysia

**Required:**
- `input/Malaysia_PCID.csv`: PCID mapping file
- `input/products.csv`: Product list file

**Location:** `2. Malaysia/input/`

### Argentina

**Required:**
- `Input/Companylist.csv`: Company list
- `Input/Dictionary.csv`: Translation dictionary
- `Input/pcid_Mapping.csv`: PCID mapping
- `Input/ProxyList.txt`: Proxy list (optional)

**Location:** `3. Argentina/Input/`

## Output Files

### Output Locations

1. **Run-Specific Outputs**: `output/runs/{scraper_name}_{timestamp}/`
   - `exports/`: Final CSV/XLSX files
   - `artifacts/`: Intermediate files
   - `logs/`: Execution logs
   - `manifest.json`: Run metadata

2. **Scraper Outputs**: `{scraper}/output/`
   - Legacy location, preserved for compatibility
   - Files may be copied to run directory

3. **Final Reports**: `output/`
   - Centralized location for final reports
   - Named with scraper-specific patterns

### Output File Patterns

- **Canada Quebec**: `canadaquebecreport_YYYYMMDD.csv`
- **Malaysia**: `malaysia_*.csv`
- **Argentina**: `alfabeta_report_*.csv`

## Troubleshooting

### Common Issues

**1. "Another instance is already running"**
- **Cause**: Lock file exists from previous run
- **Solution**: Click "Clear Run Lock" in GUI or delete `.locks/{scraper}.lock`

**2. "Input file not found"**
- **Cause**: Missing required input files
- **Solution**: Check `input/` directory, verify file names match exactly

**3. "Configuration error"**
- **Cause**: Missing or invalid configuration
- **Solution**: Run `python platform_config.py config-check`

**4. "Script execution failed"**
- **Cause**: Python error or missing dependencies
- **Solution**: Check execution logs, verify Python packages installed

**5. "Lock file cannot be deleted"**
- **Cause**: Process still running or file locked
- **Solution**: Wait a few seconds, try again, or restart application

### Diagnostic Commands

**Check Platform Status:**
```bash
python platform_config.py doctor
```

**Validate Configuration:**
```bash
python platform_config.py config-check
```

**View Paths:**
The doctor command shows all platform paths and their status.

## Best Practices

1. **Always Use GUI**: Provides better error handling and monitoring
2. **Check Inputs First**: Verify all required files are present
3. **Review Logs**: Monitor execution for warnings or errors
4. **Backup Important Data**: Automatic backups are created, but manual backups recommended
5. **One Run at a Time**: Lock system prevents concurrent runs
6. **Keep Configurations Updated**: Review and update config files as needed
7. **Monitor Disk Space**: Outputs and backups can use significant space
8. **Review Outputs**: Always verify output files after execution

## Advanced Features

### Step Explanation

The GUI includes AI-powered step explanation:
1. Select a step in the Pipeline Steps list
2. Click "💡 Explain This Step"
3. View explanation (requires OpenAI API key in configuration)

### Custom Configuration

You can create custom configurations:
1. Use "Create from Template" in Configuration tab
2. Edit configuration values
3. Save to `.env` file
4. Configuration is shared across all scrapers

### Log Management

- **View Logs**: Real-time in GUI or in `output/runs/{run_id}/logs/`
- **Save Logs**: Use "Save Log" button in GUI
- **Copy Logs**: Use "Copy to Clipboard" for sharing
- **Clear Logs**: Use "Clear" button to reset display

## Keyboard Shortcuts

- **F5**: Refresh (in some contexts)
- **Ctrl+C**: Interrupt execution (command line)
- **Double-click**: Open files in Output tabs

## Getting Help

1. **Documentation**: Check scraper-specific docs in `doc/` or `docs/` folders
2. **Logs**: Review execution logs for error details
3. **Diagnostics**: Run `python platform_config.py doctor`
4. **Configuration Check**: Run `python platform_config.py config-check`

## Updates and Maintenance

### Updating Configuration

1. Backup existing config files
2. Review new configuration options
3. Update config files as needed
4. Test with a small run first

### Cleaning Up

- **Old Backups**: Manually delete from `output/backups/` if needed
- **Old Runs**: Manually delete from `output/runs/` if needed
- **Temp Files**: Use GUI "Clean Temp" utility (if available)

### Backup Management

- Backups are created automatically before each run
- Location: `output/backups/{scraper_name}_{timestamp}/`
- Each backup includes manifest.json with metadata
- Manual cleanup recommended periodically

