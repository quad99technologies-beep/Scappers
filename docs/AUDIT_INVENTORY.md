# Configuration & Entrypoint Audit - Inventory

**Date**: 2025-12-26  
**Scope**: Full repository audit of config files, entrypoints, and path references

---

## 1. ENVIRONMENT/CONFIG FILES

### Current State

#### Platform Root
- **Location**: `Scappers/.env` (if exists)
- **Status**: Currently used by GUI and some config loaders
- **References**:
  - `scraper_gui.py` (lines 410, 417-428, 482-514)
  - `1. CanadaQuebec/Script/config_loader.py` (lines 19-29)
  - `2. Malaysia/scripts/config_loader.py` (via `_find_env_file()`)
  - `3. Argentina/script/*.py` (via `load_env_file()` functions)

#### Scraper-Specific
- **CanadaQuebec**: No dedicated `.env` (uses platform root)
- **Malaysia**: No dedicated `.env` (uses platform root)
- **Argentina**: No dedicated `.env` (uses platform root)

#### Template Files
- **`.env.example`**: Not found in repo (should exist as template)

### Config Loader Implementations

1. **`1. CanadaQuebec/Script/config_loader.py`**
   - Uses `python-dotenv`
   - Searches: Platform root → Scraper root → CWD
   - Provides: `get_env()`, `get_env_int()`, `get_base_dir()`, `get_input_dir()`, `get_output_dir()`

2. **`1. CanadaQuebec/doc/config_loader.py`**
   - Similar to Script version
   - Searches: Scraper root → CWD

3. **`2. Malaysia/scripts/config_loader.py`**
   - Custom implementation (no dotenv dependency)
   - Searches: Platform root → Scraper root → CWD → Script dir
   - Provides: `load_env_file()`, `getenv()`, `getenv_int()`

4. **`3. Argentina/script/*.py`**
   - Inline `load_env_file()` functions in each script
   - Searches: Platform root → Scraper root → CWD
   - No shared loader module

### Issues Identified

1. **Multiple config loading strategies** (dotenv vs custom vs inline)
2. **No single source of truth** for config location
3. **Paths are relative** (repo-dependent, breaks in EXE mode)
4. **No centralized config validation**
5. **No precedence rules** documented
6. **Secrets handling** not standardized

---

## 2. ENTRYPOINTS

### GUI Entrypoint
- **File**: `scraper_gui.py`
- **Launcher**: `run_gui.bat`
- **Status**: ✅ Single entrypoint for UI
- **Issues**: 
  - Uses repo root for config (breaks in EXE mode)
  - Hardcoded paths relative to `__file__`

### Workflow Runners
- **File**: `shared_workflow_runner.py`
- **Adapters**:
  - `1. CanadaQuebec/run_workflow.py`
  - `2. Malaysia/run_workflow.py`
  - `3. Argentina/run_workflow.py`
- **Status**: ✅ Unified runner exists
- **Issues**:
  - Writes to `repo_root/output/` (should be Documents/ScraperPlatform/)
  - Lock files in repo root (should be in platform root)

### Batch File Entrypoints

1. **`run_gui.bat`** (Root)
   - Launches `scraper_gui.py`
   - Uses relative paths
   - ✅ Simple, but breaks if CWD != repo root

2. **`1. CanadaQuebec/run_pipeline.bat`**
   - Calls scripts directly
   - Uses `%~dp0` for script dir
   - ✅ Works from scraper root
   - ⚠️ Bypasses workflow runner (legacy)

3. **`2. Malaysia/run_pipeline.bat`**
   - Calls scripts directly
   - Uses `%~dp0` for script dir
   - ✅ Works from scraper root
   - ⚠️ Bypasses workflow runner (legacy)

4. **`3. Argentina/run_pipeline.bat`**
   - Calls scripts directly
   - Uses `%~dp0` for script dir
   - ✅ Works from scraper root
   - ⚠️ Bypasses workflow runner (legacy)

### Setup Scripts
- `1. CanadaQuebec/setup.bat`
- `2. Malaysia/setup.bat`
- **Status**: Install dependencies only, no config setup

### Issues Identified

1. **Multiple entrypoints** (GUI vs batch files vs workflow runners)
2. **Batch files bypass workflow runner** (inconsistent behavior)
3. **No absolute path resolution** (breaks in EXE mode)
4. **No platform root detection** (assumes repo structure)

---

## 3. PATH REFERENCES

### Output Paths (Current)

#### Workflow Runner (`shared_workflow_runner.py`)
- **Backups**: `repo_root/output/backups/<run_id>/`
- **Runs**: `repo_root/output/runs/<run_id>/`
- **Lock files**: `repo_root/.{scraper}_run.lock`
- **Issue**: ❌ Writes to repo root (should be Documents/ScraperPlatform/)

#### Scraper-Specific Outputs

**CanadaQuebec**:
- `1. CanadaQuebec/output/` (via config_loader)
- `1. CanadaQuebec/backups/` (via backup script)

**Malaysia**:
- `2. Malaysia/output/` (via config_loader)
- `2. Malaysia/backup/` (via backup script)

**Argentina**:
- `3. Argentina/Output/` (hardcoded in scripts)
- `3. Argentina/backups/` (via batch file)

### Input Paths

**CanadaQuebec**:
- `1. CanadaQuebec/input/` (via config_loader)

**Malaysia**:
- `2. Malaysia/input/` (hardcoded in scripts)

**Argentina**:
- `3. Argentina/Input/` (hardcoded in scripts)

### Log Paths

**Malaysia**:
- `2. Malaysia/output/execution_log.txt` (via batch file)

**Argentina**:
- `3. Argentina/logs/` (via batch file)

### Issues Identified

1. **Inconsistent casing** (output vs Output, input vs Input)
2. **Paths relative to repo** (breaks in EXE mode)
3. **No centralized path management**
4. **Multiple output locations** (scraper root + repo root)
5. **Lock files in repo root** (should be platform root)

---

## 4. CONFIG VARIABLES USED

### CanadaQuebec
- `BASE_DIR`, `INPUT_DIR`, `OUTPUT_DIR`, `SPLIT_PDF_DIR`, `CSV_OUTPUT_DIR`, `QA_OUTPUT_DIR`, `BACKUP_DIR`
- `DEFAULT_INPUT_PDF_NAME`

### Malaysia
- `SCRIPT_03_OUTPUT_BASE_DIR`, `SCRIPT_03_QUEST3_DETAILS`, `SCRIPT_03_CONSOLIDATED_FILE`
- Various script-specific env vars

### Argentina
- `ALFABETA_USER`, `ALFABETA_PASS`
- `MAX_ROWS` (via .env or command line)

### Issues Identified

1. **No schema validation** (typos cause silent failures)
2. **No required vs optional** distinction
3. **No default values** documented centrally
4. **Secrets mixed with config** (should be separate)

---

## 5. SCRIPT DEPENDENCIES

### Python Scripts Calling Config Loaders

**CanadaQuebec**:
- All scripts in `Script/` import `config_loader`
- All scripts in `doc/` import `config_loader`

**Malaysia**:
- Scripts import `config_loader` from `scripts/`

**Argentina**:
- Scripts have inline `load_env_file()` functions

### Batch Files Calling Python Scripts

All batch files use relative paths:
- `python Script\script_name.py`
- `python scripts\script_name.py`
- `python script\script_name.py`

**Issues**: Breaks if CWD != batch file directory

---

## 6. UI ↔ CONFIG ↔ RUNNER COMMUNICATION

### Current Flow

1. **UI** (`scraper_gui.py`):
   - Reads/writes `.env` at repo root
   - Launches `run_workflow.py` via subprocess
   - No config passing mechanism

2. **Runner** (`shared_workflow_runner.py`):
   - Creates backup (includes config)
   - Runs scraper adapter
   - No explicit config loading

3. **Scraper Adapters** (`*_workflow.py`):
   - Call scripts directly
   - Scripts load config independently
   - No config validation

### Issues Identified

1. **No config passing** from UI to runner
2. **No config snapshot** in run manifest
3. **No config validation** before run
4. **Stale config** possible (scripts load independently)

---

## 7. SCENARIOS TO VERIFY

### ✅ Currently Working
- Dev run from repo with .env present
- Running from scraper root directory

### ❌ Currently Broken
- Dev run with no .env (silent fallback, no validation)
- Packaged EXE run (paths relative to repo)
- Running from different CWD
- First-run on clean machine (no config setup)
- Missing config keys (no validation)

---

## SUMMARY OF ISSUES

### Critical
1. **Paths are repo-relative** → Breaks in EXE mode
2. **No centralized config** → Multiple loading strategies
3. **No platform root** → Assumes repo structure
4. **Batch files bypass runner** → Inconsistent behavior

### High Priority
5. **No config validation** → Silent failures
6. **No config passing** → UI changes don't affect runs
7. **Secrets handling** → Mixed with config, no masking

### Medium Priority
8. **Inconsistent path casing** → Windows issues possible
9. **Multiple output locations** → Confusing
10. **No migration path** → Hard to update existing installs

---

## NEXT STEPS

1. Create `platform_config.py` (ConfigResolver + PathManager)
2. Create `WIRING.md` (documentation)
3. Update all config loaders to use ConfigResolver
4. Update batch files to use absolute paths
5. Update workflow runner to use PathManager
6. Add config validation and doctor commands
7. Create migration guide

