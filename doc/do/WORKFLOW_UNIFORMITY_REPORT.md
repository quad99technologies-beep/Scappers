# Workflow Uniformity Report

## Summary
All three scrapers (Argentina, Malaysia, CanadaQuebec) now have uniform workflow implementations with all features enabled.

## Features Enabled for All Scrapers

### ✅ Resume/Checkpoint System
- **Argentina**: ✅ `run_pipeline_resume.py` - Steps 0-6
- **Malaysia**: ✅ `run_pipeline_resume.py` - Steps 0-5  
- **CanadaQuebec**: ✅ `run_pipeline_resume.py` - Steps 0-6

All resume scripts support:
- `--fresh` flag to start from step 0
- `--step N` flag to start from specific step
- Automatic resume from last completed step
- Checkpoint file storage in `.checkpoints/pipeline_checkpoint.json`

### ✅ Batch File Integration
- **Argentina**: ✅ `run_pipeline.bat` - Checks for resume script first
- **Malaysia**: ✅ `run_pipeline.bat` - Checks for resume script first
- **CanadaQuebec**: ✅ `run_pipeline.bat` - Checks for resume script first

All batch files:
- Use `run_pipeline_resume.py` if available (default behavior)
- Fall back to legacy step-by-step execution if resume script not found
- Support passing arguments to resume script

### ✅ Lock File Cleanup
- **Argentina**: ✅ `cleanup_lock.py` - With retry logic
- **Malaysia**: ✅ `cleanup_lock.py` - With retry logic
- **CanadaQuebec**: ✅ `cleanup_lock.py` - With retry logic

All cleanup scripts:
- Use platform_config for lock file location
- Fall back to old location if needed
- Include retry logic with configurable delays
- Called from both batch files and resume scripts

### ✅ GUI Integration
All scrapers are integrated into the GUI with:
- Resume Pipeline button (resumes from checkpoint)
- Run Fresh Pipeline button (starts from step 0)
- View Checkpoint button (shows checkpoint status)
- Clear Checkpoint button (resets checkpoint)
- Checkpoint status label (shows last completed step)

### ✅ Configuration Management
- **Argentina**: ✅ `config_loader.py` with env.json support
- **Malaysia**: ✅ `config_loader.py` with env.json support
- **CanadaQuebec**: ✅ `config_loader.py` with env.json support

All config loaders support:
- Environment variables
- JSON configuration files
- Platform config integration
- Fallback to defaults

## File Structure Comparison

### Argentina
```
scripts/Argentina/
├── 00_backup_and_clean.py
├── 01_getProdList.py
├── 02_prepare_urls.py
├── 03_alfabeta_api_scraper.py
├── 04_alfabeta_selenium_scraper.py
├── 05_TranslateUsingDictionary.py
├── 06_GenerateOutput.py
├── 06_PCIDmissing.py
├── config_loader.py
├── run_pipeline.bat          ✅ Resume script integration
├── run_pipeline_resume.py    ✅ 7 steps (0-6)
└── cleanup_lock.py           ✅ With retry logic
```

### Malaysia
```
scripts/Malaysia/
├── 00_backup_and_clean.py
├── 01_Product_Registration_Number.py
├── 02_Product_Details.py
├── 03_Consolidate_Results.py
├── 04_Get_Fully_Reimbursable.py
├── 05_Generate_PCID_Mapped.py
├── config_loader.py
├── run_pipeline.bat          ✅ Resume script integration
├── run_pipeline_resume.py    ✅ 6 steps (0-5)
└── cleanup_lock.py           ✅ With retry logic
```

### CanadaQuebec
```
scripts/CanadaQuebec/
├── 00_backup_and_clean.py
├── 01_split_pdf_into_annexes.py
├── 02_validate_pdf_structure.py
├── 03_extract_annexe_iv1.py
├── 04_extract_annexe_iv2.py
├── 05_extract_annexe_v.py
├── 06_merge_all_annexes.py
├── config_loader.py
├── run_pipeline.bat          ✅ Resume script integration
├── run_pipeline_resume.py    ✅ 7 steps (0-6, step 2 optional)
└── cleanup_lock.py           ✅ With retry logic
```

## Workflow Execution Flow

### Standard Execution (via batch file)
1. User runs `run_pipeline.bat` or clicks "Run Pipeline" in GUI
2. Batch file checks for `run_pipeline_resume.py`
3. If found, executes resume script with checkpoint support
4. If not found, falls back to legacy step-by-step execution

### Resume Script Flow
1. Check for checkpoint file
2. Determine start step (fresh, specific step, or resume)
3. Execute steps starting from determined step
4. Mark each completed step in checkpoint
5. Skip already completed steps
6. Clean up lock file on completion

## Checkpoint System

### Checkpoint File Location
All scrapers store checkpoints at:
```
{output_dir}/{scraper_name}/.checkpoints/pipeline_checkpoint.json
```

### Checkpoint File Format
```json
{
  "scraper": "ScraperName",
  "last_run": "ISO timestamp",
  "completed_steps": [0, 1, 2, ...],
  "step_outputs": {
    "step_0": {
      "step_number": 0,
      "step_name": "Step Name",
      "completed_at": "ISO timestamp",
      "output_files": ["file1.csv", ...]
    },
    ...
  },
  "metadata": {}
}
```

## Utilities

### Checkpoint Management Script
- Location: `scripts/create_checkpoint.py`
- Supports all three scrapers
- Can create, view, clear, and list checkpoints
- Interactive mode available

## Documentation

All documentation moved to `doc/` folder:
- `doc/README.md` - Main platform documentation
- `doc/Argentina/README.md` - Argentina scraper docs
- `doc/Malaysia/README.md` - Malaysia scraper docs
- `doc/CanadaQuebec/README.md` - CanadaQuebec scraper docs
- `doc/CREATE_CHECKPOINT_README.md` - Checkpoint utility docs

## Status: ✅ All Features Uniform Across All Scrapers

