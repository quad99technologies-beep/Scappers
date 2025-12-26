# Platform Wiring Map

**Last Updated**: 2025-12-26  
**Purpose**: Document how all components connect, communicate, and where data flows

---

## ARCHITECTURE OVERVIEW

```
┌─────────────────────────────────────────────────────────────┐
│                    USER INTERFACE                            │
│                  (scraper_gui.py)                            │
│  - Scraper selection                                         │
│  - Config editing (Documents/ScraperPlatform/config/)        │
│  - Run triggers                                              │
│  - Output viewing                                            │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       │ Launches
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              WORKFLOW RUNNER                                 │
│         (shared_workflow_runner.py)                          │
│  - Acquires lock (platform_root/.locks/)                    │
│  - Creates backup (platform_root/output/backups/)           │
│  - Creates run folder (platform_root/output/runs/)          │
│  - Calls scraper adapter                                     │
│  - Collects outputs                                         │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       │ Calls
                       ▼
┌─────────────────────────────────────────────────────────────┐
│            SCRAPER ADAPTER                                   │
│      (*/run_workflow.py)                                     │
│  - Validates inputs                                          │
│  - Runs steps (via batch or direct script calls)             │
│  - Writes outputs to run_dir/exports/                        │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       │ Executes
                       ▼
┌─────────────────────────────────────────────────────────────┐
│            SCRAPER SCRIPTS                                   │
│      (*/Script/*.py, */scripts/*.py, etc.)                  │
│  - Load config via ConfigResolver                            │
│  - Read inputs from platform_root/input/<scraper_id>/        │
│  - Write outputs to run_dir/exports/ (via PathManager)       │
└─────────────────────────────────────────────────────────────┘
```

---

## CONFIGURATION RESOLUTION

### Precedence Order (Highest to Lowest)

1. **Runtime Overrides** (UI edits, command-line args)
2. **Process Environment Variables** (`os.environ`)
3. **Scraper Config** (`Documents/ScraperPlatform/config/<scraper_id>.env.json`)
4. **Platform Config** (`Documents/ScraperPlatform/config/platform.json`)
5. **Defaults** (hardcoded in ConfigResolver)

### Config File Locations

```
Documents/ScraperPlatform/
├── config/
│   ├── platform.json          # Global platform settings
│   ├── CanadaQuebec.env.json  # CanadaQuebec scraper config
│   ├── Malaysia.env.json      # Malaysia scraper config
│   └── Argentina.env.json      # Argentina scraper config
└── .env (DEPRECATED - templates only)
```

### Config Schema

**platform.json**:
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

**<scraper_id>.env.json**:
```json
{
  "scraper": {
    "id": "CanadaQuebec",
    "enabled": true
  },
  "config": {
    "INPUT_DIR": "input",
    "OUTPUT_DIR": "output",
    "BASE_DIR": ""
  },
  "secrets": {
    "ALFABETA_USER": "***MASKED***",
    "ALFABETA_PASS": "***MASKED***"
  }
}
```

---

## PATH MANAGEMENT

### Platform Root

**Location**: `%USERPROFILE%\Documents\ScraperPlatform\`

**Detection Order**:
1. `SCRAPER_PLATFORM_ROOT` environment variable
2. `%USERPROFILE%\Documents\ScraperPlatform\` (default)

### Directory Structure

```
Documents/ScraperPlatform/
├── config/              # Configuration files (JSON)
├── input/               # Input files (organized by scraper)
│   ├── CanadaQuebec/
│   ├── Malaysia/
│   └── Argentina/
├── output/             # All outputs
│   ├── backups/        # Pre-run backups
│   │   └── <scraper_id>_<timestamp>/
│   ├── runs/           # Run folders
│   │   └── <scraper_id>_<timestamp>/
│   │       ├── logs/   # Run-specific logs
│   │       ├── artifacts/  # Intermediate files
│   │       ├── exports/   # Final outputs (CSV, etc.)
│   │       └── manifest.json
│   └── exports/        # Symlink/copy of latest exports
├── sessions/           # Session state
├── logs/              # Platform logs
├── cache/             # Cache files
└── .locks/            # Lock files (hidden)
```

### Path Resolution

All paths are resolved via `PathManager`:
- **Absolute paths** are used everywhere
- **No relative paths** (except within platform root)
- **Platform root** is detected once and cached

---

## DATA FLOW

### Input Flow

```
User places files
    ↓
Documents/ScraperPlatform/input/<scraper_id>/
    ↓
Scraper scripts read via PathManager.get_input_dir(scraper_id)
```

### Execution Flow

```
User clicks "Run" in UI
    ↓
scraper_gui.py → run_workflow.py (subprocess)
    ↓
shared_workflow_runner.py:
    1. Acquire lock (.locks/<scraper_id>.lock)
    2. Load config (ConfigResolver)
    3. Create backup (output/backups/<run_id>/)
    4. Create run folder (output/runs/<run_id>/)
    5. Call scraper adapter
    ↓
Scraper adapter:
    1. Validate inputs (check input/<scraper_id>/)
    2. Run steps (scripts via batch or direct)
    3. Write outputs (run_dir/exports/)
    ↓
Workflow runner:
    1. Collect outputs
    2. Create manifest.json
    3. Release lock
```

### Output Flow

```
Scraper scripts write to run_dir/exports/
    ↓
Workflow runner collects outputs
    ↓
Manifest.json created with file list
    ↓
UI displays outputs from run_dir/exports/
```

---

## ENTRYPOINTS

### Primary Entrypoint

**File**: `scraper_gui.py`  
**Launcher**: `run_gui.bat`  
**Purpose**: Main UI for all operations

### Workflow Entrypoints

**Files**: `*/run_workflow.py`  
**Purpose**: CLI entrypoint for each scraper  
**Usage**: Called by GUI or directly via CLI

### Legacy Entrypoints (Deprecated)

**Files**: `*/run_pipeline.bat`  
**Status**: Still functional but bypass workflow runner  
**Migration**: Should call `run_workflow.py` instead

---

## CONFIG LOADING

### ConfigResolver Usage

```python
from platform_config import ConfigResolver

resolver = ConfigResolver()
config = resolver.get_config("Malaysia")
value = config.get("SCRIPT_03_OUTPUT_BASE_DIR", default="../output")
```

### PathManager Usage

```python
from platform_config import PathManager

pm = PathManager()
input_dir = pm.get_input_dir("Malaysia")
output_dir = pm.get_output_dir("Malaysia")
run_dir = pm.get_run_dir("Malaysia", run_id)
```

---

## LOCKING MECHANISM

### Single-Instance Locking

**Location**: `Documents/ScraperPlatform/.locks/<scraper_id>.lock`

**Purpose**: Prevent concurrent runs of same scraper

**Implementation**:
- Windows: File creation with exclusive access
- Lock file contains: PID, timestamp
- Stale lock detection: >1 hour old = stale

---

## ERROR HANDLING

### Config Errors

- **Missing config**: Use defaults, log warning
- **Invalid JSON**: Raise exception, show in UI
- **Missing required key**: Raise exception, show in UI

### Path Errors

- **Platform root not writable**: Raise exception, show in UI
- **Input directory missing**: Create if possible, else raise
- **Output directory missing**: Create automatically

### Execution Errors

- **Script failure**: Log error, save state, release lock
- **Lock acquisition failure**: Show message, don't start run
- **Backup failure**: Abort run, don't proceed

---

## MIGRATION FROM OLD SYSTEM

### Old Config Location
- `Scappers/.env` (repo root)

### New Config Location
- `Documents/ScraperPlatform/config/<scraper_id>.env.json`

### Migration Steps
1. Run `python -m platform_config migrate` (if migration script exists)
2. Or manually copy `.env` values to new JSON files
3. Update scripts to use ConfigResolver

### Backward Compatibility
- Old `.env` files are ignored (templates only)
- Scripts using old config loaders will fail gracefully
- Migration guide in `MIGRATION.md`

---

## TESTING SCENARIOS

### Scenario 1: Dev Run from Repo
- **CWD**: `D:\quad99\Scappers\`
- **Config**: `Documents/ScraperPlatform/config/`
- **Outputs**: `Documents/ScraperPlatform/output/`
- **Status**: ✅ Works

### Scenario 2: Packaged EXE Run
- **CWD**: Anywhere
- **Config**: `Documents/ScraperPlatform/config/`
- **Outputs**: `Documents/ScraperPlatform/output/`
- **Status**: ✅ Works (absolute paths)

### Scenario 3: First Run (Clean Machine)
- **Config**: Created from defaults
- **Directories**: Created automatically
- **Status**: ✅ Works

### Scenario 4: Missing Config Keys
- **Validation**: ConfigResolver validates required keys
- **Error**: Shown in UI before run starts
- **Status**: ✅ Handled

---

## COMPONENT INTERACTIONS

### UI → Runner
- **Method**: Subprocess call to `run_workflow.py`
- **Config**: Loaded by runner independently
- **Outputs**: Runner writes to `output/runs/<run_id>/`

### Runner → Scraper Adapter
- **Method**: Direct Python call
- **Config**: Passed via adapter constructor
- **Outputs**: Adapter writes to `run_dir/exports/`

### Scraper Adapter → Scripts
- **Method**: Subprocess call or direct import
- **Config**: Scripts load via ConfigResolver
- **Outputs**: Scripts write to `run_dir/exports/` (via PathManager)

---

## SECURITY CONSIDERATIONS

### Secrets Handling
- **Storage**: In `<scraper_id>.env.json` with `"secrets"` key
- **Display**: Masked in UI (`***MASKED***`)
- **Logging**: Never logged in plain text
- **Fallback**: Can use OS environment variables

### File Permissions
- **Config files**: User-readable/writable only
- **Lock files**: User-readable/writable only
- **Output files**: User-readable/writable only

---

## FUTURE ENHANCEMENTS

1. **Config UI**: Visual editor for JSON configs
2. **Config Templates**: Per-scraper templates with validation
3. **Config Versioning**: Track config changes per run
4. **Remote Config**: Support for remote config server (optional)
5. **Config Encryption**: Encrypt secrets at rest (optional)

