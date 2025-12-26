# Migration Guide: Platform Config Integration

**Date**: 2025-12-26
**Status**: Complete
**Migration Path**: Legacy .env → Platform Config System

---

## OVERVIEW

This guide walks through migrating from the old configuration system to the new platform config system. The migration has already been implemented - this guide helps you understand what changed and how to configure your environment.

### What Changed?

**Before (Legacy)**:
- Config files: `.env` in each scraper directory
- Paths: Relative to CWD (`./input/`, `./output/`)
- Secrets: Committed to git (security risk)
- Three different config loaders (inconsistent)

**After (Platform Config)**:
- Config files: JSON in `Documents/ScraperPlatform/config/`
- Paths: Absolute, pointing to platform root
- Secrets: Outside git, properly secured
- Unified config system (ConfigResolver + PathManager)

### What Didn't Change?

**BUSINESS LOGIC UNCHANGED**:
- All scraping logic identical
- All selectors, URLs, parsing code unchanged
- All output formats identical
- All deduplication logic identical
- All mapping logic identical

---

## MIGRATION STEPS

### Step 1: Understand New Directory Structure

**Platform Root**: `%USERPROFILE%\Documents\ScraperPlatform\`

```
Documents/ScraperPlatform/
├── config/                          # NEW: Configuration files
│   ├── platform.json               # Platform-wide settings
│   ├── CanadaQuebec.env.json       # CanadaQuebec scraper config
│   ├── Malaysia.env.json           # Malaysia scraper config
│   └── Argentina.env.json          # Argentina scraper config
├── input/                          # NEW: Centralized inputs
│   ├── CanadaQuebec/
│   ├── Malaysia/
│   └── Argentina/
├── output/                         # NEW: All outputs here
│   ├── backups/                    # Pre-run backups
│   ├── runs/                       # Run-specific outputs
│   └── exports/                    # Latest outputs
├── logs/                           # Platform logs
├── cache/                          # Cache files
├── sessions/                       # Session state
└── .locks/                         # Lock files (hidden)
```

### Step 2: Run Diagnostic Command

Check current system status:

```batch
cd d:\quad99\Scappers
python platform_config.py doctor
```

**Expected Output**:
```
=== Scraper Platform Configuration Doctor ===

Platform Paths:
  Platform Root: C:\Users\YourName\Documents\ScraperPlatform [OK]
  Config Dir: C:\Users\YourName\Documents\ScraperPlatform\config [OK]
  Input Dir: C:\Users\YourName\Documents\ScraperPlatform\input [OK]
  Output Dir: C:\Users\YourName\Documents\ScraperPlatform\output [OK]
  Logs Dir: C:\Users\YourName\Documents\ScraperPlatform\logs [OK]
  Cache Dir: C:\Users\YourName\Documents\ScraperPlatform\cache [OK]

Scraper Configs:
  CanadaQuebec: C:\Users\YourName\Documents\ScraperPlatform\config\CanadaQuebec.env.json [ X]
  Malaysia: C:\Users\YourName\Documents\ScraperPlatform\config\Malaysia.env.json [ X]
  Argentina: C:\Users\YourName\Documents\ScraperPlatform\config\Argentina.env.json [ X]

Health Status:
  Platform Root Writable: [OK]
  Config Directory Writable: [OK]
  Output Directory Writable: [OK]
```

### Step 3: Create Config Files from Templates

For each scraper you use, create config file from template:

#### CanadaQuebec

**Template Location**: `1. CanadaQuebec/.env.example`

**Create Config**:
```batch
mkdir "%USERPROFILE%\Documents\ScraperPlatform\config" 2>nul
notepad "%USERPROFILE%\Documents\ScraperPlatform\config\CanadaQuebec.env.json"
```

**Config Content** (copy from template, add your values):
```json
{
  "scraper": {
    "id": "CanadaQuebec",
    "enabled": true
  },
  "config": {
    "INPUT_DIR": "input",
    "OUTPUT_DIR": "output",
    "CSV_OUTPUT_DIR": "csv",
    "SPLIT_PDF_DIR": "split_pdf",
    "QA_OUTPUT_DIR": "qa",
    "BACKUP_DIR": "backups",
    "OPENAI_MODEL": "gpt-4o-mini",
    "STATIC_CURRENCY": "CAD",
    "STATIC_REGION": "NORTH AMERICA"
  },
  "secrets": {
    "OPENAI_API_KEY": "sk-proj-YOUR_ACTUAL_API_KEY_HERE"
  }
}
```

#### Malaysia

**Template Location**: `2. Malaysia/.env.example`

**Create Config**:
```batch
notepad "%USERPROFILE%\Documents\ScraperPlatform\config\Malaysia.env.json"
```

**Config Content**:
```json
{
  "scraper": {
    "id": "Malaysia",
    "enabled": true
  },
  "config": {
    "SCRIPT_01_URL": "https://www.pharmacy.gov.my/v2/en/apps/quest3.html",
    "SCRIPT_02_HEADLESS": "false",
    "SCRIPT_02_DOWNLOAD_DIR": "Output/bulk_search_csvs",
    "SCRIPT_02_MAX_ROWS": "0",
    "SCRIPT_03_OUTPUT_BASE_DIR": "Output",
    "SCRIPT_03_CONSOLIDATED_CSV": "consolidated_products.csv",
    "SCRIPT_04_FUKKM_URL": "https://www.fukkm.com.my/products",
    "SCRIPT_04_OUTPUT_DIR": "Output",
    "SCRIPT_04_HEADLESS": "false",
    "SCRIPT_05_INPUT_PCID_FILE": "Requirement/Malaysia_PCID Mapped_ 02122025.xlsx",
    "SCRIPT_05_INPUT_PCID_SHEET": "PCID Mapped",
    "SCRIPT_05_INPUT_CONSOLIDATED_FILE": "Output/consolidated_products.csv",
    "SCRIPT_05_OUTPUT_DIR": "Output"
  },
  "secrets": {}
}
```

#### Argentina

**Template Location**: `3. Argentina/.env.example`

**Create Config**:
```batch
notepad "%USERPROFILE%\Documents\ScraperPlatform\config\Argentina.env.json"
```

**Config Content**:
```json
{
  "scraper": {
    "id": "Argentina",
    "enabled": true
  },
  "config": {
    "HEADLESS": "false",
    "MAX_ROWS": "0",
    "INPUT_DIR": "Input",
    "OUTPUT_DIR": "Output",
    "DEBUG_DIR": "Output/debug"
  },
  "secrets": {
    "ALFABETA_USER": "your_alfabeta_username",
    "ALFABETA_PASS": "your_alfabeta_password"
  }
}
```

### Step 4: Validate Configuration

Run config check to validate required secrets:

```batch
python platform_config.py config-check
```

**Expected Output**:
```
=== Scraper Config Validation ===

CanadaQuebec:
  Required: OPENAI_API_KEY
    [ X] OPENAI_API_KEY (not set or empty)

Malaysia:
  No required secrets

Argentina:
  Required: ALFABETA_USER, ALFABETA_PASS
    [OK] ALFABETA_USER
    [OK] ALFABETA_PASS
```

Fix any `[ X]` entries by editing the config files.

### Step 5: Move Input Files

Move input files to platform input directory:

```batch
REM CanadaQuebec
mkdir "%USERPROFILE%\Documents\ScraperPlatform\input\CanadaQuebec"
copy "1. CanadaQuebec\input\*" "%USERPROFILE%\Documents\ScraperPlatform\input\CanadaQuebec\"

REM Malaysia
mkdir "%USERPROFILE%\Documents\ScraperPlatform\input\Malaysia"
copy "2. Malaysia\Requirement\*" "%USERPROFILE%\Documents\ScraperPlatform\input\Malaysia\"

REM Argentina
mkdir "%USERPROFILE%\Documents\ScraperPlatform\input\Argentina"
copy "3. Argentina\Input\*" "%USERPROFILE%\Documents\ScraperPlatform\input\Argentina\"
```

### Step 6: Test Scrapers

Run each scraper to verify configuration:

```batch
REM Test CanadaQuebec
cd "1. CanadaQuebec"
call run_pipeline.bat

REM Test Malaysia
cd "..\2. Malaysia"
call run_pipeline.bat

REM Test Argentina
cd "..\3. Argentina"
call run_pipeline.bat
```

**Expected Behavior**:
- Scripts load config from platform config
- Scripts read inputs from `Documents/ScraperPlatform/input/<scraper>/`
- Scripts write outputs to `Documents/ScraperPlatform/output/`
- All business logic unchanged

---

## TROUBLESHOOTING

### Issue 1: Config File Not Found

**Symptom**: Script runs but uses defaults

**Solution**:
1. Check config file exists: `dir "%USERPROFILE%\Documents\ScraperPlatform\config\*.env.json"`
2. Check file name matches scraper ID exactly (case-sensitive)
3. Run `python platform_config.py doctor` to verify

### Issue 2: Missing Required Secret

**Symptom**: Script fails with "API key not set" or similar

**Solution**:
1. Run `python platform_config.py config-check`
2. Edit config file to add missing secret
3. Re-run validation

### Issue 3: Input Files Not Found

**Symptom**: Script fails with "Input file not found"

**Solution**:
1. Check input files in platform input dir: `dir "%USERPROFILE%\Documents\ScraperPlatform\input\<scraper>\"`
2. Move input files if needed (see Step 5)
3. Verify scraper ID matches directory name

### Issue 4: Outputs Not Appearing

**Symptom**: Script completes but outputs not visible

**Solution**:
1. Check platform output dir: `dir "%USERPROFILE%\Documents\ScraperPlatform\output\"`
2. Check for errors in logs: `type "%USERPROFILE%\Documents\ScraperPlatform\logs\*.log"`
3. Run `python platform_config.py doctor` to verify paths writable

### Issue 5: Legacy .env File Still Used

**Symptom**: Changes to platform config not taking effect

**Solution**:
1. Check environment variables: `set | findstr "OPENAI\|ALFABETA\|SCRIPT_"`
2. Clear environment variables if set
3. Delete legacy .env files if present (they're ignored but might cause confusion)

### Issue 6: Platform Config Import Error

**Symptom**: `ImportError: cannot import name 'PathManager' from 'platform_config'`

**Solution**:
1. Verify CWD is repo root: `cd d:\quad99\Scappers`
2. Check platform_config.py exists: `dir platform_config.py`
3. Check Python path: `python -c "import sys; print('\n'.join(sys.path))"`

---

## BACKWARD COMPATIBILITY

### Legacy .env Files

**Status**: Ignored (templates only)

**Fallback**: If platform config not available, scripts use hardcoded defaults

**Migration**: Copy values from old .env to new JSON configs

### Old Batch Files

**Status**: Still functional

**Behavior**: Scripts now write to platform paths automatically

**Migration**: No action needed (batch files updated to document platform paths)

### Old Scripts

**Status**: Updated to use ConfigResolver

**Behavior**: Load config from platform config, fall back to defaults if not available

**Migration**: No action needed (config_loader.py updated)

---

## VALIDATION CHECKLIST

After migration, verify these items:

- [ ] Platform root directory created: `%USERPROFILE%\Documents\ScraperPlatform\`
- [ ] Config files created for scrapers you use
- [ ] Required secrets set in config files
- [ ] Input files moved to platform input directories
- [ ] `python platform_config.py doctor` shows all `[OK]`
- [ ] `python platform_config.py config-check` shows all `[OK]`
- [ ] Test run of each scraper completes successfully
- [ ] Outputs appear in platform output directory
- [ ] Logs created in platform logs directory
- [ ] No errors in execution logs

---

## ROLLBACK (If Needed)

If you need to rollback to legacy system:

1. **Restore old .env files** (if you saved them):
   ```batch
   copy "1. CanadaQuebec\.env.backup" "1. CanadaQuebec\.env"
   copy "2. Malaysia\.env.backup" "2. Malaysia\.env"
   copy "3. Argentina\.env.backup" "3. Argentina\.env"
   ```

2. **Checkout old config_loader.py files** (git):
   ```batch
   git checkout HEAD~5 "1. CanadaQuebec/Script/config_loader.py"
   git checkout HEAD~5 "2. Malaysia/scripts/config_loader.py"
   git checkout HEAD~5 "3. Argentina/script/config_loader.py"
   ```

3. **Note**: This will break platform config integration. Not recommended.

---

## BENEFITS OF NEW SYSTEM

### Security
- ✅ Secrets stored outside git repository
- ✅ Secrets not committed by accident
- ✅ Secrets survive git operations (pull, reset, etc.)

### Portability
- ✅ Works from any CWD
- ✅ Works in packaged EXE mode
- ✅ Consistent across all scrapers

### Maintainability
- ✅ Single source of truth for config
- ✅ Unified config loading mechanism
- ✅ Easy to validate and troubleshoot

### User Experience
- ✅ Centralized config management
- ✅ No need to edit files in repo
- ✅ Diagnostic commands for validation

---

## NEXT STEPS

After successful migration:

1. **Delete legacy .env files** (if present):
   ```batch
   del "1. CanadaQuebec\.env" 2>nul
   del "2. Malaysia\.env" 2>nul
   del "3. Argentina\.env" 2>nul
   ```

2. **Bookmark platform config directory**:
   ```batch
   explorer "%USERPROFILE%\Documents\ScraperPlatform\config"
   ```

3. **Create backup of config files**:
   ```batch
   xcopy "%USERPROFILE%\Documents\ScraperPlatform\config" "%USERPROFILE%\Documents\ScraperPlatform\config_backup\" /E /I /Y
   ```

4. **Review WIRING.md** for architecture details

5. **Review CHANGELOG.md** for complete list of changes

---

**Migration Complete!**

The platform config system is now fully integrated and ready to use.
