# Repository Configuration & Wiring Inventory
**Date**: 2025-12-26
**Purpose**: Complete audit of env files, entrypoints, scripts, and path references

---

## 1. ENVIRONMENT & CONFIG FILES

### 1.1 Environment Files (.env)
| Location | Purpose | Status | Secrets Present |
|----------|---------|--------|----------------|
| `1. CanadaQuebec/.env` | CanadaQuebec scraper config | ✅ Active | ⚠️ OPENAI_API_KEY exposed |
| `1. CanadaQuebec/doc/.env.example` | Template/example | ✅ Template | No |
| `2. Malaysia/.env` | Malaysia scraper config | ✅ Active | No secrets |
| `2. Malaysia/docs/.env.example` | Template/example (missing?) | ❌ Not found | N/A |
| `3. Argentina/.env` | Argentina scraper config | ✅ Active | ⚠️ ALFABETA credentials + proxies exposed |

**CRITICAL FINDING**: `.env` files contain EXPOSED SECRETS in plain text:
- CanadaQuebec: OpenAI API key (sk-proj-...)
- Argentina: Login credentials (vishwambhar080@gmail.com / password) + proxy credentials

### 1.2 Config Loader Modules
| Location | Purpose | Implementation |
|----------|---------|----------------|
| `1. CanadaQuebec/Script/config_loader.py` | Loads .env with python-dotenv | Searches parent dirs for .env |
| `1. CanadaQuebec/doc/config_loader.py` | Duplicate/legacy | ⚠️ Duplicate file |
| `2. Malaysia/scripts/config_loader.py` | Loads .env manually (no dependencies) | Custom parser, searches parent dirs |

**CRITICAL FINDING**: Multiple config loaders with different search logic:
- CanadaQuebec uses `python-dotenv` (external dependency)
- Malaysia uses custom parser (no deps)
- Argentina: No dedicated config loader found

### 1.3 Platform Config System (NEW)
| File | Purpose | Status |
|------|---------|--------|
| `platform_config.py` | Central ConfigResolver + PathManager | ✅ Implemented |
| Documents/ScraperPlatform/config/platform.json | Platform-wide config | 🔄 Runtime-created |
| Documents/ScraperPlatform/config/{scraper}.env.json | Per-scraper config | 🔄 Runtime-created |

---

## 2. ENTRYPOINTS & LAUNCHERS

### 2.1 Batch Files (.bat)
| File | Purpose | What It Calls | CWD Dependency |
|------|---------|---------------|----------------|
| `run_gui.bat` | Launch GUI | `python scraper_gui.py` | ✅ Sets to script dir |
| `killChrome.bat` | Kill Chrome/Drive processes | `taskkill` commands | No |
| `1. CanadaQuebec/run_pipeline.bat` | Run full pipeline | Calls 6 Python scripts in sequence | ✅ Sets CWD |
| `1. CanadaQuebec/setup.bat` | Install dependencies | `pip install` from requirements.txt | ✅ Sets CWD |
| `2. Malaysia/run_pipeline.bat` | Run full pipeline | Calls 5 Python scripts in sequence | ✅ Sets CWD |
| `2. Malaysia/setup.bat` | Install dependencies | `pip install playwright selenium pandas` | ✅ Sets CWD |
| `3. Argentina/run_pipeline.bat` | Run full pipeline with state tracking | Calls 6 Python scripts with resume logic | ✅ Sets CWD |

**FINDING**: Each scraper has its own `run_pipeline.bat` with similar but inconsistent logic:
- Different backup strategies
- Different error handling
- Different logging approaches
- Argentina has advanced features (state file, loop mode, max-rows)

### 2.2 Python Entry Points
| File | Purpose | Dependencies | Import Path Issues |
|------|---------|--------------|-------------------|
| `scraper_gui.py` | Main UI (Tkinter) | ✅ None (stdlib) | No |
| `platform_config.py` | Path/config management | ✅ None (stdlib) | No |
| `shared_workflow_runner.py` | Workflow orchestration | `platform_config` (optional) | Yes - imports platform_config |
| `1. CanadaQuebec/run_workflow.py` | Workflow adapter | `shared_workflow_runner` | Yes - adds repo_root to sys.path |
| `2. Malaysia/run_workflow.py` | Workflow adapter | `shared_workflow_runner` | Yes - adds repo_root to sys.path |
| `3. Argentina/run_workflow.py` | Workflow adapter | `shared_workflow_runner` | Yes - adds repo_root to sys.path |

**FINDING**: `run_workflow.py` files exist but are NOT called by the batch files. Batch files call individual scripts directly.

### 2.3 Individual Scraper Scripts
Each scraper has numbered scripts:

**CanadaQuebec** (Script/):
- 00_backup_and_clean.py
- 01_split_pdf_into_annexes.py
- 02_validate_pdf_structure.py
- 03_extract_annexe_iv1.py
- 04_extract_annexe_iv2.py
- 05_extract_annexe_v.py
- 06_merge_all_annexes.py

**Malaysia** (scripts/):
- 00_backup_and_clean.py
- 01_Product_Registration_Number.py
- 02_Product_Details.py
- 03_Consolidate_Results.py
- 04_Get_Fully_Reimbursable.py
- 05_Generate_PCID_Mapped.py

**Argentina** (script/):
- 00_backup_and_clean.py
- 01_getCompanyList.py
- 02_getProdList.py
- 03_alfabeta_scraper_labs.py
- 04_TranslateUsingDictionary.py
- 05_GenerateOutput.py
- 06_PCIDmissing.py

---

## 3. PATH REFERENCES & WRITE LOCATIONS

### 3.1 Hardcoded Path Patterns (Found 200 occurrences across 29 files)
Common patterns:
- `input/` or `Input/` - relative paths
- `output/` or `Output/` - relative paths
- `Path(__file__).parent` - relative to script location
- `Path(__file__).parents[1]` - relative to scraper root
- `os.path.join(...)` - legacy path construction

### 3.2 Current Write Locations (INCONSISTENT)
| Scraper | Input | Output | Backups | Logs |
|---------|-------|--------|---------|------|
| CanadaQuebec | `./input/` | `./output/` | `./backups/` | `./output/csv/` (inline) |
| Malaysia | `./input/` | `./Output/` | `./Backup/` | `./Output/execution_log.txt` |
| Argentina | `./Input/` | `./Output/` | `./backups/` | `./logs/` |

**CRITICAL FINDING**: All paths are relative to scraper directory (CWD-dependent):
- ❌ No writes to `Documents/ScraperPlatform/`
- ❌ Outputs stay in repo directories
- ❌ Will break in packaged EXE mode
- ❌ Will break if CWD != scraper root

### 3.3 Target Structure (NOT YET IMPLEMENTED)
```
%USERPROFILE%/Documents/ScraperPlatform/
├── config/
│   ├── platform.json
│   ├── CanadaQuebec.env.json
│   ├── Malaysia.env.json
│   └── Argentina.env.json
├── input/
│   ├── CanadaQuebec/
│   ├── Malaysia/
│   └── Argentina/
├── output/
│   ├── exports/
│   ├── runs/
│   │   └── {run_id}/
│   │       ├── logs/
│   │       ├── artifacts/
│   │       └── exports/
│   └── backups/
├── sessions/
├── logs/
├── cache/
└── .locks/
```

---

## 4. CONFIGURATION PRECEDENCE (CURRENT STATE)

### 4.1 CanadaQuebec Config Loading
1. `config_loader.py` searches for `.env` in:
   - `Scappers/.env` (platform root, 2 levels up)
   - `1. CanadaQuebec/.env` (scraper root)
   - Current working directory
2. Uses `python-dotenv` if available, else falls back to `os.getenv`
3. Provides defaults for all values (backward compatible)

### 4.2 Malaysia Config Loading
1. `config_loader.py` searches for `.env` in 5 locations:
   - Platform root (2 levels up)
   - Scraper root (parent of scripts/)
   - CWD
   - Parent of CWD
   - Same directory as script
2. Custom parser (no external deps)
3. Only loads if key NOT already in environment

### 4.3 Argentina Config Loading
1. Uses `dotenv` directly in scripts (e.g., `03_alfabeta_scraper_labs.py`)
2. Searches for `.env` in script directory or parent
3. Less standardized than other scrapers

### 4.4 Platform Config (NEW - Not Yet Integrated)
`platform_config.py` provides:
- **PathManager**: Centralized path resolution
- **ConfigResolver**: Precedence-based config merging
  - Precedence: Runtime > Process Env > Scraper Config > Platform Config > Defaults
- Creates `Documents/ScraperPlatform/` structure
- Supports secrets masking

**CRITICAL GAP**: Platform config exists but is NOT used by individual scraper scripts yet.

---

## 5. ENTRYPOINT WIRING

### 5.1 Current Flow (GUI → Scripts)
```
run_gui.bat
  ↓
scraper_gui.py (Tkinter UI)
  ↓
User selects scraper + step
  ↓
subprocess.Popen(["python", "{scraper}/Script/{step}.py"])
  ↓
Script reads .env via config_loader
  ↓
Script writes to ./output/ (relative to scraper dir)
```

### 5.2 Current Flow (Batch → Scripts)
```
{scraper}/run_pipeline.bat
  ↓
cd /d "%~dp0"  (set CWD to scraper dir)
  ↓
python Script/00_backup_and_clean.py
python Script/01_...py
python Script/02_...py
...
  ↓
Each script reads .env via config_loader
  ↓
Each script writes to ./output/ (relative to CWD)
```

### 5.3 Intended Flow (Not Yet Active)
```
run_gui.bat (or any launcher)
  ↓
scraper_gui.py
  ↓
shared_workflow_runner.py
  ↓
{scraper}/run_workflow.py (adapter)
  ↓
Scraper implements ScraperInterface
  ↓
WorkflowRunner orchestrates:
  - Lock acquisition
  - Backup first
  - Run steps
  - Write to Documents/ScraperPlatform/output/runs/{run_id}/
```

**CRITICAL GAP**: `run_workflow.py` files exist but are NOT called by GUI or batch files.

---

## 6. DEPENDENCY INVENTORY

### 6.1 Python Dependencies
**CanadaQuebec** (doc/requirements.txt):
- PyPDF2
- pdfplumber
- pandas
- openai
- python-dotenv (optional, for config loading)

**Malaysia** (None found - requirements in setup.bat):
- playwright
- selenium
- webdriver-manager
- pandas
- openpyxl
- requests
- beautifulsoup4
- lxml

**Argentina** (No requirements.txt found):
- Dependencies installed via setup.bat:
  - playwright
  - selenium
  - webdriver-manager
  - pandas
  - openpyxl
  - requests
  - beautifulsoup4
  - lxml

**Platform** (repo root):
- No requirements.txt for platform-level deps

### 6.2 External Tools
- Python 3.8+ (required)
- pip (required)
- Playwright browsers (chromium)
- Chrome/Chromium (for Selenium)

---

## 7. CRITICAL ISSUES IDENTIFIED

### 7.1 Security Issues
1. **EXPOSED SECRETS** in .env files:
   - CanadaQuebec: OpenAI API key in plain text
   - Argentina: Login credentials + proxy credentials in plain text
   - These files are in git history (even if deleted now)

2. **No secret masking** in current config loaders

### 7.2 Path Management Issues
1. **All writes are CWD-dependent**:
   - Will break if run from different directory
   - Will break in packaged EXE mode
   - No deterministic output location

2. **Inconsistent directory naming**:
   - `input/` vs `Input/`
   - `output/` vs `Output/`
   - `backups/` vs `Backup/`

3. **No platform-wide structure**:
   - Each scraper has its own folders
   - No central output/runs/{run_id}/ pattern
   - Hard to find artifacts from specific runs

### 7.3 Entrypoint Issues
1. **Multiple pipeline runners** (inconsistent):
   - Each scraper's run_pipeline.bat is different
   - No single entrypoint to call
   - GUI calls scripts directly, not through workflow runner

2. **Unused workflow runners**:
   - `run_workflow.py` files exist but aren't called
   - `shared_workflow_runner.py` exists but isn't used by batch files

3. **Import path hacks**:
   - Multiple `sys.path.insert(0, ...)` statements
   - Fragile parent directory resolution
   - Won't work in packaged EXE mode

### 7.4 Config Management Issues
1. **No single source of truth**:
   - CanadaQuebec uses python-dotenv
   - Malaysia uses custom parser
   - Argentina uses dotenv directly in scripts
   - No consistent precedence rules

2. **Config file search is fragile**:
   - Searches multiple parent directories
   - Different search order in different scrapers
   - Will break if directory structure changes

3. **Platform config not integrated**:
   - `platform_config.py` exists but isn't used by scrapers
   - Config still loaded from .env files in scraper dirs
   - No migration from old to new config system

### 7.5 Windows/EXE Compatibility Issues
1. **Relative path dependencies**:
   - Scripts assume they're run from scraper directory
   - Use `Path(__file__).parent` patterns
   - Won't work if frozen with PyInstaller

2. **Batch file limitations**:
   - No proper error handling in some batch files
   - Inconsistent exit code handling
   - No validation of Python/pip installation

3. **No single-instance protection**:
   - Multiple runs can interfere with each other
   - No lock files (except in shared_workflow_runner, which isn't used)

---

## 8. RECOMMENDATIONS

### 8.1 Immediate (Security)
1. **Remove exposed secrets from .env files**:
   - Move to Documents/ScraperPlatform/config/{scraper}.env.json
   - Add to .gitignore
   - Rotate compromised credentials (OpenAI key, Argentina login)

2. **Create .env.example templates**:
   - With placeholder values only
   - Document required vs optional keys
   - Include in repo as reference

### 8.2 High Priority (Functionality)
1. **Migrate to Documents/ScraperPlatform/ structure**:
   - All scrapers write to common output/runs/{run_id}/
   - Input files in input/{scraper}/
   - Config in config/{scraper}.env.json

2. **Unify entrypoints**:
   - Make batch files call run_workflow.py (not individual scripts)
   - GUI calls run_workflow.py (not individual scripts)
   - Single entrypoint pattern for all scrapers

3. **Integrate platform_config**:
   - Make all scripts import from platform_config
   - Replace config_loader modules with ConfigResolver
   - Ensure precedence: Runtime > Env > Config File > Defaults

### 8.3 Medium Priority (Quality)
1. **Standardize batch files**:
   - Create template batch file
   - Consistent error handling
   - Consistent logging
   - All call same entrypoint pattern

2. **Remove duplicate/dead code**:
   - Consolidate config_loader modules
   - Remove unused run_workflow.py or make them active
   - Clean up doc/ directories with duplicate files

3. **Add validation**:
   - Config sanity check command
   - Doctor command (already partially implemented)
   - Pre-flight validation before run

### 8.4 Low Priority (Nice to Have)
1. **Add CI/linting**:
   - Detect hardcoded paths
   - Detect missing quoting in batch files
   - Detect exposed secrets

2. **Create packaging configs**:
   - PyInstaller spec files
   - Include all data files
   - Set correct entry points

3. **Documentation**:
   - WIRING.md (architecture diagram)
   - MIGRATION.md (upgrade guide)
   - API_REFERENCE.md (for ConfigResolver/PathManager)

---

## 9. NEXT STEPS

Based on this inventory, the audit should proceed as follows:

1. **Create comprehensive analysis** (This document)
2. **Design minimal changes** (WIRING.md + MIGRATION.md)
3. **Implement in phases**:
   - Phase 1: Security (move secrets, add .gitignore)
   - Phase 2: Path Management (integrate platform_config everywhere)
   - Phase 3: Entrypoint Unification (batch files → run_workflow.py)
   - Phase 4: Validation & Testing (doctor command, sanity checks)
   - Phase 5: Documentation (user guide, migration guide)

Each phase should be committed separately with clear messages documenting what changed and confirming that business logic was NOT modified.

---

**END OF INVENTORY**
