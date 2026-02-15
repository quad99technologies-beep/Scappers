# Developer Onboarding Guide: Adding a New Scraper

## Overview

This guide provides step-by-step instructions for onboarding a new scraper to the platform. Follow this checklist to ensure all features are properly integrated and the new scraper follows the same patterns as existing scrapers (Argentina, Malaysia, CanadaQuebec).

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Checklist](#checklist)
3. [Step-by-Step Instructions](#step-by-step-instructions)
4. [File Structure Requirements](#file-structure-requirements)
5. [Configuration Setup](#configuration-setup)
6. [Code Implementation](#code-implementation)
7. [Testing Checklist](#testing-checklist)
8. [Troubleshooting](#troubleshooting)

---

## Prerequisites

Before starting, ensure you understand:
- Python 3.x programming
- The existing scraper structure (review Argentina, Malaysia, or CanadaQuebec)
- Platform configuration system (`platform_config.py`)
- Pipeline checkpoint system (`core/pipeline_checkpoint.py`)
- GUI integration patterns (`scraper_gui.py`)

---

## Checklist

### Phase 1: Project Setup
- [ ] Create scraper directory: `scripts/{ScraperName}/`
- [ ] Create configuration file: `config/{ScraperName}.env.json`
- [ ] Create configuration example: `config/{ScraperName}.env.example`
- [ ] Create documentation: `doc/{ScraperName}/README.md`

### Phase 2: Core Scripts
- [ ] Create `00_backup_and_clean.py` (backup and cleanup script)
- [ ] Create all pipeline step scripts (01_*, 02_*, etc.)
- [ ] Create `config_loader.py` (configuration management)
- [ ] Create `cleanup_lock.py` (lock file cleanup)

### Phase 3: Pipeline Integration
- [ ] Create `run_pipeline_resume.py` (resume/checkpoint support)
- [ ] Create `run_pipeline.bat` (batch file runner)
- [ ] Update `scripts/create_checkpoint.py` with new scraper steps
- [ ] Test pipeline execution end-to-end

### Phase 4: GUI Integration
- [ ] Add scraper to `scraper_gui.py` scrapers dictionary
- [ ] Add all pipeline steps to GUI steps list
- [ ] Test GUI functionality (run, stop, view checkpoint, etc.)

### Phase 5: Documentation
- [ ] Create `doc/{ScraperName}/README.md`
- [ ] Document all pipeline steps
- [ ] Document configuration options
- [ ] Update main README if needed

### Phase 6: Testing & Validation
- [ ] Test backup and clean functionality
- [ ] Test checkpoint/resume functionality
- [ ] Test GUI integration
- [ ] Test configuration loading
- [ ] Verify all features work (resume, fresh run, checkpoint viewing)

---

## Step-by-Step Instructions

### Step 1: Create Directory Structure

Create the scraper directory:
```bash
mkdir scripts/NewScraper
```

### Step 2: Create Configuration Files

#### 2.1 Create `config/NewScraper.env.json`

```json
{
  "SCRAPER_ID": "NewScraper",
  "OUTPUT_DIR": "",
  "INPUT_DIR": "",
  "BACKUP_DIR": "",
  "SCRIPT_00_OUTPUT_DIR": "",
  "SCRIPT_01_OUTPUT_DIR": "",
  "SCRIPT_02_OUTPUT_DIR": "",
  "...": "..."
}
```

**Required Keys:**
- `SCRAPER_ID`: Must match directory name (e.g., "NewScraper")
- `OUTPUT_DIR`: Output directory (empty = use platform config default)
- `INPUT_DIR`: Input directory (empty = use platform config default)
- `BACKUP_DIR`: Backup directory (empty = use platform config default)
- Script-specific output directories as needed

#### 2.2 Create `config/NewScraper.env.example`

Copy from `config/NewScraper.env.json` and remove sensitive values, add comments.

### Step 3: Create Core Scripts

#### 3.1 Create `00_backup_and_clean.py`

**Template:**
```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backup Output Folder

Creates a backup of the output folder with a timestamp based on the latest
file modification date, then cleans the output folder for a fresh run.
"""

from pathlib import Path
import sys

# Add repo root to path for shared utilities
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from config_loader import get_output_dir, get_backup_dir, get_central_output_dir
from core.shared_utils import backup_output_folder, clean_output_folder

# Configuration
OUTPUT_DIR = get_output_dir()
BACKUP_DIR = get_backup_dir()
CENTRAL_OUTPUT_DIR = get_central_output_dir()


def main() -> None:
    """Main entry point."""
    print()
    print("=" * 80)
    print("BACKUP AND CLEAN OUTPUT FOLDER")
    print("=" * 80)
    print()

    # Step 1: Backup
    print("[1/2] Creating backup of output folder...")
    backup_result = backup_output_folder(
        output_dir=OUTPUT_DIR,
        backup_dir=BACKUP_DIR,
        central_output_dir=CENTRAL_OUTPUT_DIR,
        exclude_dirs=[str(BACKUP_DIR)]
    )

    if backup_result["status"] == "ok":
        print(f"[OK] Backup created successfully!")
        print(f"     Location: {backup_result['backup_folder']}")
        print(f"     Timestamp: {backup_result['timestamp']}")
        print(f"     Latest file modification: {backup_result['latest_modification']}")
        print(f"     Files backed up: {backup_result['files_backed_up']}")
    elif backup_result["status"] == "skipped":
        print(f"[SKIP] {backup_result['message']}")
    else:
        print(f"[ERROR] {backup_result['message']}")
        return

    print()

    # Step 2: Clean
    print("[2/2] Cleaning output folder...")
    clean_result = clean_output_folder(
        output_dir=OUTPUT_DIR,
        backup_dir=BACKUP_DIR,
        central_output_dir=CENTRAL_OUTPUT_DIR,
        keep_files=[],
        keep_dirs=["runs", "backups"]
    )

    if clean_result["status"] == "ok":
        print(f"[OK] Output folder cleaned successfully!")
        print(f"     Files deleted: {clean_result['files_deleted']}")
        print(f"     Directories deleted: {clean_result['directories_deleted']}")
    elif clean_result["status"] == "skipped":
        print(f"[SKIP] {clean_result['message']}")
    else:
        print(f"[ERROR] {clean_result['message']}")
        return

    print()
    print("=" * 80)
    print("Backup and cleanup complete! Ready for fresh pipeline run.")
    print("=" * 80)
    print()


if __name__ == "__main__":
    main()
```

**Rules:**
- Must use `get_output_dir()`, `get_backup_dir()`, `get_central_output_dir()` from config_loader
- Must use `backup_output_folder()` and `clean_output_folder()` from `core/shared_utils`
- Must follow the same structure as existing scrapers

#### 3.2 Create Pipeline Step Scripts

For each pipeline step, create numbered scripts:
- `01_step_name.py`
- `02_step_name.py`
- `03_step_name.py`
- etc.

**Naming Convention:**
- Use lowercase with underscores
- Start with 2-digit step number (01, 02, 03, etc.)
- Descriptive names (e.g., `01_extract_data.py`, `02_process_data.py`)

**Structure:**
```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step Description
"""

from pathlib import Path
import sys

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add scripts/{ScraperName} to path for imports
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from config_loader import get_output_dir, get_input_dir
# ... other imports

def main():
    """Main entry point."""
    # Implementation
    pass

if __name__ == "__main__":
    main()
```

#### 3.3 Create `config_loader.py`

**Template:**
```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configuration Loader (Platform Config Integration)

This module wraps platform_config.py for centralized path management.
All configuration is loaded from config/{ScraperName}.env.json.
"""

import os
import sys
from pathlib import Path

# Add repo root to path for platform_config import
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Scraper identifier
SCRAPER_ID = "NewScraper"  # MUST match directory name and SCRAPER_ID in env.json

# Try to import platform_config (preferred)
try:
    from platform_config import get_path_manager, get_config_resolver
    _PLATFORM_CONFIG_AVAILABLE = True
except ImportError:
    _PLATFORM_CONFIG_AVAILABLE = False
    get_path_manager = None
    get_config_resolver = None


def get_path_manager():
    """Get path manager instance."""
    if _PLATFORM_CONFIG_AVAILABLE:
        from platform_config import get_path_manager as _get_pm
        return _get_pm()
    raise ImportError("platform_config not available")


def get_config_resolver():
    """Get config resolver instance."""
    if _PLATFORM_CONFIG_AVAILABLE:
        from platform_config import get_config_resolver as _get_cr
        return _get_cr()
    raise ImportError("platform_config not available")


def get_env(key: str, default: str = "") -> str:
    """Get environment variable with config file fallback."""
    # Check OS environment first
    value = os.getenv(key)
    if value is not None:
        return value
    
    # Check config file
    if _PLATFORM_CONFIG_AVAILABLE:
        try:
            cr = get_config_resolver()
            value = cr.get(SCRAPER_ID, key, default)
            return str(value) if value is not None else default
        except:
            pass
    
    return default


def get_output_dir(subpath: str = None) -> Path:
    """
    Get output directory - uses Documents/ScraperPlatform/output/{ScraperName}/
    """
    # First check if OUTPUT_DIR is explicitly set
    output_dir_str = get_env("OUTPUT_DIR", "")
    if output_dir_str and Path(output_dir_str).is_absolute():
        base = Path(output_dir_str)
    else:
        # Use scraper-specific platform output directory
        if _PLATFORM_CONFIG_AVAILABLE:
            pm = get_path_manager()
            base = pm.get_output_dir(SCRAPER_ID)
            base.mkdir(parents=True, exist_ok=True)
        else:
            # Fallback: use repo root output (legacy)
            base = _repo_root / "output" / SCRAPER_ID
            base.mkdir(parents=True, exist_ok=True)

    if subpath:
        result = base / subpath
        result.mkdir(parents=True, exist_ok=True)
        return result
    return base


def get_input_dir(subpath: str = None) -> Path:
    """
    Get input directory - uses Documents/ScraperPlatform/input/{ScraperName}/
    """
    if _PLATFORM_CONFIG_AVAILABLE:
        pm = get_path_manager()
        base = pm.get_input_dir(SCRAPER_ID)
        base.mkdir(parents=True, exist_ok=True)
    else:
        base = _repo_root / "input" / SCRAPER_ID
        base.mkdir(parents=True, exist_ok=True)

    if subpath:
        return base / subpath
    return base


def get_backup_dir() -> Path:
    """Get backup directory - scraper-specific: backups/{ScraperName}/"""
    if _PLATFORM_CONFIG_AVAILABLE:
        pm = get_path_manager()
        return pm.get_backups_dir(SCRAPER_ID)
    else:
        backup_dir = _repo_root / "backups" / SCRAPER_ID
        backup_dir.mkdir(parents=True, exist_ok=True)
        return backup_dir


def get_central_output_dir() -> Path:
    """Get central exports directory for final reports"""
    if _PLATFORM_CONFIG_AVAILABLE:
        pm = get_path_manager()
        exports_dir = pm.get_exports_dir(SCRAPER_ID)
        exports_dir.mkdir(parents=True, exist_ok=True)
        return exports_dir
    else:
        repo_root = _repo_root
        central_output = repo_root / "exports" / SCRAPER_ID
        central_output.mkdir(parents=True, exist_ok=True)
        return central_output
```

**Rules:**
- `SCRAPER_ID` MUST match directory name and config file name
- Must use platform_config if available, fallback to legacy paths
- All directory functions must create directories if they don't exist
- Follow the same pattern as existing config_loaders

#### 3.4 Create `cleanup_lock.py`

**Template:**
```python
#!/usr/bin/env python3
"""
Cleanup Lock File
Removes lock files after pipeline completion
"""
import sys
import time
from pathlib import Path

# Add repo root to path
script_dir = Path(__file__).resolve().parent
repo_root = script_dir.parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

try:
    from config_loader import get_env_int, get_env_float
    MAX_RETRIES_CLEANUP = get_env_int("MAX_RETRIES_CLEANUP", 5)
    CLEANUP_RETRY_DELAY_BASE = get_env_float("CLEANUP_RETRY_DELAY_BASE", 0.3)
except ImportError:
    # Fallback if config_loader not available
    MAX_RETRIES_CLEANUP = 5
    CLEANUP_RETRY_DELAY_BASE = 0.3

SCRAPER_NAME = "NewScraper"  # MUST match SCRAPER_ID

try:
    from platform_config import get_path_manager
    pm = get_path_manager()
    lock_file = pm.get_lock_file(SCRAPER_NAME)
    
    # Try to delete lock file with retries
    for attempt in range(MAX_RETRIES_CLEANUP):
        try:
            if lock_file.exists():
                lock_file.unlink()
                if not lock_file.exists():
                    break
            else:
                break
        except Exception:
            if attempt < MAX_RETRIES_CLEANUP - 1:
                time.sleep(CLEANUP_RETRY_DELAY_BASE * (attempt + 1))
    
    # Also clean up old lock location
    old_lock = repo_root / f".{SCRAPER_NAME}_run.lock"
    for attempt in range(MAX_RETRIES_CLEANUP):
        try:
            if old_lock.exists():
                old_lock.unlink()
                if not old_lock.exists():
                    break
            else:
                break
        except Exception:
            if attempt < MAX_RETRIES_CLEANUP - 1:
                time.sleep(CLEANUP_RETRY_DELAY_BASE * (attempt + 1))
except Exception:
    pass  # Ignore errors
```

**Rules:**
- `SCRAPER_NAME` MUST match SCRAPER_ID
- Must include retry logic for lock file deletion
- Must clean up both new and old lock file locations
- Must handle errors gracefully

### Step 4: Create Pipeline Runner Scripts

#### 4.1 Create `run_pipeline_resume.py`

**Template:**
```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
{ScraperName} Pipeline Runner with Resume/Checkpoint Support

Usage:
    python run_pipeline_resume.py          # Resume from last step or start fresh
    python run_pipeline_resume.py --fresh  # Start from step 0 (clear checkpoint)
    python run_pipeline_resume.py --step N # Start from step N (0-X)
"""

import sys
import subprocess
import argparse
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add scripts/{ScraperName} to path for imports
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from core.pipeline_checkpoint import get_checkpoint_manager
from config_loader import get_output_dir

SCRAPER_NAME = "NewScraper"  # MUST match SCRAPER_ID

def run_step(step_num: int, script_name: str, step_name: str, output_files: list = None, allow_failure: bool = False):
    """Run a pipeline step and mark it complete if successful."""
    print(f"\n{'='*80}")
    print(f"Step {step_num}/{MAX_STEPS}: {step_name}")
    print(f"{'='*80}\n")
    
    script_path = Path(__file__).parent / script_name
    if not script_path.exists():
        print(f"ERROR: Script not found: {script_path}")
        return False
    
    try:
        result = subprocess.run(
            [sys.executable, "-u", str(script_path)],
            check=not allow_failure,
            capture_output=False
        )
        
        # Mark step as complete
        cp = get_checkpoint_manager(SCRAPER_NAME)
        if output_files:
            output_dir = get_output_dir()
            abs_output_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            cp.mark_step_complete(step_num, step_name, abs_output_files)
        else:
            cp.mark_step_complete(step_num, step_name)
        
        return True
    except subprocess.CalledProcessError as e:
        if allow_failure:
            print(f"\nWARNING: Step {step_num} ({step_name}) failed but continuing (allow_failure=True)")
            return True
        print(f"\nERROR: Step {step_num} ({step_name}) failed with exit code {e.returncode}")
        return False
    except Exception as e:
        print(f"\nERROR: Step {step_num} ({step_name}) failed: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description=f"{SCRAPER_NAME} Pipeline Runner with Resume Support")
    parser.add_argument("--fresh", action="store_true", help="Start from step 0 (clear checkpoint)")
    parser.add_argument("--step", type=int, help=f"Start from specific step (0-{MAX_STEPS})")
    
    args = parser.parse_args()
    
    cp = get_checkpoint_manager(SCRAPER_NAME)
    
    # Determine start step
    if args.fresh:
        cp.clear_checkpoint()
        start_step = 0
        print("Starting fresh run (checkpoint cleared)")
    elif args.step is not None:
        start_step = args.step
        print(f"Starting from step {start_step}")
    else:
        # Resume from last completed step
        info = cp.get_checkpoint_info()
        start_step = info["next_step"]
        if info["total_completed"] > 0:
            print(f"Resuming from step {start_step} (last completed: step {info['last_completed_step']})")
        else:
            print("Starting fresh run (no checkpoint found)")
    
    # Define pipeline steps with their output files
    output_dir = get_output_dir()
    steps = [
        (0, "00_backup_and_clean.py", "Backup and Clean", None),
        (1, "01_step_name.py", "Step 1 Name", ["output_file1.csv"]),
        (2, "02_step_name.py", "Step 2 Name", ["output_file2.csv"]),
        # ... add all steps
    ]
    
    # Run steps starting from start_step
    for step_info in steps:
        if len(step_info) == 4:
            step_num, script_name, step_name, output_files = step_info
            allow_failure = False
        else:
            step_num, script_name, step_name, output_files, allow_failure = step_info
        
        if step_num < start_step:
            # Skip completed steps
            if cp.is_step_complete(step_num):
                print(f"\nStep {step_num}/{MAX_STEPS}: {step_name} - SKIPPED (already completed)")
            continue
        
        success = run_step(step_num, script_name, step_name, output_files, allow_failure)
        if not success:
            print(f"\nPipeline failed at step {step_num}")
            sys.exit(1)
    
    print(f"\n{'='*80}")
    print("Pipeline completed successfully!")
    print(f"{'='*80}\n")
    
    # Clean up lock file
    try:
        cleanup_script = Path(__file__).parent / "cleanup_lock.py"
        if cleanup_script.exists():
            subprocess.run([sys.executable, str(cleanup_script)], capture_output=True)
    except:
        pass

if __name__ == "__main__":
    main()
```

**Rules:**
- `SCRAPER_NAME` MUST match SCRAPER_ID
- Define all steps in the `steps` list with output files
- Mark steps as complete after successful execution
- Support `--fresh` and `--step` flags
- Clean up lock file at the end

#### 4.2 Create `run_pipeline.bat`

**Template:**
```batch
@echo off
REM {ScraperName} Pipeline Runner
REM Runs all workflow steps in sequence with resume/checkpoint support
REM By default, resumes from last completed step
REM Use run_pipeline_resume.py --fresh to start fresh

REM Enable unbuffered output for real-time console updates
set PYTHONUNBUFFERED=1

cd /d "%~dp0"

REM Use resume script if available, otherwise fall back to original behavior
if exist "run_pipeline_resume.py" (
    python -u "run_pipeline_resume.py" %*
    exit /b %errorlevel%
)

REM Setup logging - create log file with timestamp
setlocal enabledelayedexpansion
echo Get-Date -Format 'yyyyMMdd_HHmmss' > "%TEMP%\get_timestamp.ps1"
for /f %%I in ('powershell -NoProfile -ExecutionPolicy Bypass -File "%TEMP%\get_timestamp.ps1"') do set timestamp=%%I
del "%TEMP%\get_timestamp.ps1" 2>nul
set log_file=..\..\output\{ScraperName}\{ScraperName}_run_%timestamp%.log

REM Create output directory if it doesn't exist
if not exist "..\..\output\{ScraperName}" mkdir "..\..\output\{ScraperName}"

REM Initialize log file with header
(
echo ================================================================================
echo {ScraperName} Pipeline - Starting at %date% %time%
echo ================================================================================
echo.
) > "%log_file%"

REM Step 0: Backup and Clean
echo [Step 0/X] Backup and Clean... >> "%log_file%"
echo [Step 0/X] Backup and Clean...
python -u "00_backup_and_clean.py" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%log_file%' -Append"
set PYTHON_EXIT=%errorlevel%
if %PYTHON_EXIT% neq 0 (
    echo ERROR: Backup and Clean failed >> "%log_file%"
    echo ERROR: Backup and Clean failed
    exit /b 1
)

REM ... add all steps ...

REM Clean up lock file after successful completion
python "cleanup_lock.py" 2>nul

echo. >> "%log_file%"
echo Log file saved to: %log_file% >> "%log_file%"
echo.
echo Log file saved to: %log_file%
```

**Rules:**
- MUST check for `run_pipeline_resume.py` first and use it if available
- Fall back to step-by-step execution if resume script not found
- Replace `{ScraperName}` with actual scraper name
- Update step counts (0/X where X is total steps)
- Clean up lock file at the end

### Step 5: Update Global Files

#### 5.1 Update `scripts/create_checkpoint.py`

Add the new scraper to the `PIPELINE_STEPS` dictionary:

```python
PIPELINE_STEPS = {
    # ... existing scrapers ...
    "NewScraper": [
        (0, "Backup and Clean"),
        (1, "Step 1 Name"),
        (2, "Step 2 Name"),
        # ... all steps ...
    ],
}
```

#### 5.2 Update `scraper_gui.py`

Add the new scraper to the `self.scrapers` dictionary in the `__init__` method:

```python
"NewScraper": {
    "path": self.repo_root / "scripts" / "NewScraper",
    "scripts_dir": "",
    "docs_dir": None,
    "steps": [
        {"name": "00 - Backup and Clean", "script": "00_backup_and_clean.py", "desc": "Backup output folder and clean for fresh run"},
        {"name": "01 - Step 1 Name", "script": "01_step_name.py", "desc": "Step 1 description"},
        {"name": "02 - Step 2 Name", "script": "02_step_name.py", "desc": "Step 2 description"},
        # ... all steps ...
    ],
    "pipeline_bat": "run_pipeline.bat"
}
```

**Rules:**
- Steps list MUST start with "00 - Backup and Clean"
- Step names must match script file names
- Descriptions should be clear and concise
- Steps must be in order (00, 01, 02, etc.)

### Step 6: Create Documentation

#### 6.1 Create `doc/NewScraper/README.md`

Include:
- Overview of the scraper
- Pipeline steps description
- Configuration options
- Usage instructions
- Troubleshooting tips
- Examples

**Template:**
```markdown
# {ScraperName} Scraper Documentation

## Overview
[Description of what this scraper does]

## Pipeline Steps

### Step 0: Backup and Clean
- Description: Creates backup and cleans output folder
- Script: `00_backup_and_clean.py`

### Step 1: [Step Name]
- Description: [What this step does]
- Script: `01_step_name.py`
- Output: [Output files]

[... continue for all steps ...]

## Configuration

See `config/NewScraper.env.json` for configuration options.

## Usage

### Via GUI
1. Select "{ScraperName}" from the scraper dropdown
2. Click "Resume Pipeline" or "Run Fresh Pipeline"

### Via Command Line
```bash
cd scripts/NewScraper
python run_pipeline_resume.py          # Resume from last step
python run_pipeline_resume.py --fresh  # Start fresh
python run_pipeline_resume.py --step 2 # Start from step 2
```

### Via Batch File
```bash
cd scripts/NewScraper
run_pipeline.bat
```

## Troubleshooting

[Common issues and solutions]
```

---

## File Structure Requirements

```
scripts/NewScraper/
├── 00_backup_and_clean.py          # REQUIRED: Backup and cleanup
├── 01_step_name.py                  # Pipeline step scripts
├── 02_step_name.py
├── ...
├── config_loader.py                 # REQUIRED: Configuration management
├── cleanup_lock.py                  # REQUIRED: Lock file cleanup
├── run_pipeline_resume.py           # REQUIRED: Resume/checkpoint runner
└── run_pipeline.bat                 # REQUIRED: Batch file runner

config/
├── NewScraper.env.json              # REQUIRED: Configuration file
└── NewScraper.env.example           # REQUIRED: Example configuration

doc/
└── NewScraper/
    └── README.md                    # REQUIRED: Documentation
```

---

## Configuration Setup

### Required Configuration Keys

All scrapers MUST have these keys in their `{ScraperName}.env.json`:

```json
{
  "SCRAPER_ID": "NewScraper",
  "OUTPUT_DIR": "",
  "INPUT_DIR": "",
  "BACKUP_DIR": ""
}
```

### Optional Configuration Keys

- Script-specific output directories
- API keys (if needed)
- Rate limits
- Retry settings
- Any scraper-specific settings

---

## Code Implementation Rules

### Naming Conventions

1. **Directory Name**: PascalCase (e.g., `NewScraper`)
2. **SCRAPER_ID**: Must match directory name exactly
3. **Script Files**: `##_descriptive_name.py` (e.g., `01_extract_data.py`)
4. **Functions**: snake_case
5. **Constants**: UPPER_SNAKE_CASE

### Code Standards

1. **All scripts MUST:**
   - Include shebang: `#!/usr/bin/env python3`
   - Include encoding: `# -*- coding: utf-8 -*-`
   - Include docstring at module and function level
   - Add repo root and script dir to sys.path
   - Use config_loader for paths and configuration
   - Handle errors gracefully

2. **Path Handling:**
   - Always use `Path` from `pathlib`
   - Use functions from `config_loader` (get_output_dir, get_input_dir, etc.)
   - Create directories if they don't exist

3. **Error Handling:**
   - Use try/except blocks
   - Log errors clearly
   - Exit with appropriate codes (0 = success, 1 = error)

4. **Logging:**
   - Use print statements for user feedback
   - Include step numbers and progress indicators
   - Use consistent formatting (e.g., `[OK]`, `[ERROR]`, `[SKIP]`)

---

## Testing Checklist

### Functional Testing

- [ ] Backup and clean script creates backups correctly
- [ ] Backup and clean script cleans output folder correctly
- [ ] All pipeline steps execute successfully
- [ ] Pipeline can be run end-to-end
- [ ] Checkpoint system creates checkpoint files
- [ ] Resume functionality works (resume from last step)
- [ ] Fresh run works (--fresh flag)
- [ ] Starting from specific step works (--step flag)
- [ ] Lock file cleanup works
- [ ] GUI integration works (run, stop, view checkpoint, clear checkpoint)

### Integration Testing

- [ ] Configuration loading works (env.json and platform_config)
- [ ] Path resolution works (output, input, backup directories)
- [ ] Checkpoint files are created in correct location
- [ ] GUI displays all steps correctly
- [ ] GUI buttons work (Resume, Fresh Run, View Checkpoint, Clear Checkpoint)
- [ ] Batch file executes resume script correctly

### Edge Case Testing

- [ ] Pipeline handles crashes gracefully
- [ ] Resume works after crash
- [ ] Lock file cleanup handles errors gracefully
- [ ] Configuration fallbacks work when platform_config unavailable
- [ ] Empty output directories are handled correctly

---

## Troubleshooting

### Common Issues

#### Issue: Scraper not appearing in GUI
**Solution:** Check that scraper is added to `scraper_gui.py` scrapers dictionary

#### Issue: Steps not visible in GUI
**Solution:** Check that all steps are listed in the steps array, starting with step 00

#### Issue: Checkpoint not working
**Solution:** 
- Verify `run_pipeline_resume.py` exists and is correct
- Check that checkpoint directory is created
- Verify SCRAPER_NAME matches SCRAPER_ID

#### Issue: Configuration not loading
**Solution:**
- Check that `config/{ScraperName}.env.json` exists
- Verify SCRAPER_ID matches directory name
- Check config_loader.py imports and functions

#### Issue: Lock file not cleaned up
**Solution:**
- Verify `cleanup_lock.py` exists
- Check that SCRAPER_NAME matches SCRAPER_ID
- Verify cleanup_lock.py is called in run_pipeline_resume.py

---

## Important Reminders

1. **SCRAPER_ID Consistency**: The SCRAPER_ID must match:
   - Directory name (`scripts/{ScraperName}/`)
   - Config file name (`config/{ScraperName}.env.json`)
   - SCRAPER_ID in config_loader.py
   - SCRAPER_NAME in cleanup_lock.py
   - SCRAPER_NAME in run_pipeline_resume.py
   - Entry in scraper_gui.py

2. **Step Numbering**: Steps MUST start at 00 (Backup and Clean) and be sequential

3. **File Naming**: Script files MUST follow the pattern `##_name.py` where ## is the step number

4. **Checkpoint Integration**: All steps MUST be defined in `run_pipeline_resume.py` with output files

5. **GUI Integration**: All steps MUST be listed in `scraper_gui.py` with descriptions

6. **Documentation**: MUST create documentation in `doc/{ScraperName}/README.md`

---

## Quick Reference: File Checklist

When onboarding a new scraper, ensure these files exist and are correct:

### Required Files
- [ ] `scripts/{ScraperName}/00_backup_and_clean.py`
- [ ] `scripts/{ScraperName}/config_loader.py`
- [ ] `scripts/{ScraperName}/cleanup_lock.py`
- [ ] `scripts/{ScraperName}/run_pipeline_resume.py`
- [ ] `scripts/{ScraperName}/run_pipeline.bat`
- [ ] `config/{ScraperName}.env.json`
- [ ] `config/{ScraperName}.env.example`
- [ ] `doc/{ScraperName}/README.md`

### Required Updates
- [ ] `scripts/create_checkpoint.py` - Add to PIPELINE_STEPS
- [ ] `scraper_gui.py` - Add to scrapers dictionary

### Pipeline Step Files
- [ ] All step scripts (01_*, 02_*, etc.) created and functional

---

## Support and Questions

If you encounter issues or have questions:
1. Review existing scrapers (Argentina, Malaysia, CanadaQuebec) as reference
2. Check this guide for the specific section
3. Review error messages and logs
4. Consult with the development team

---

## Version History

- **v1.0** (2025-01-03): Initial guide created based on Argentina, Malaysia, and CanadaQuebec implementations

