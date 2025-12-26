# Configuration & Entrypoint Audit - Summary

**Date**: 2025-12-26  
**Status**: Phase 1 Complete - Foundation Laid

---

## COMPLETED WORK

### 1. Inventory & Documentation ✅
- **AUDIT_INVENTORY.md**: Comprehensive inventory of all config files, entrypoints, and path references
- **WIRING.md**: Complete wiring map showing component interactions and data flow
- **AUDIT_SUMMARY.md**: This document

### 2. Centralized Configuration System ✅
- **platform_config.py**: 
  - `PathManager`: Centralized path management (all paths under `Documents/ScraperPlatform/`)
  - `ConfigResolver`: Configuration resolution with precedence (defaults → platform → scraper → runtime → env)
  - Backward compatibility functions for existing code

### 3. Workflow Runner Updates ✅
- **shared_workflow_runner.py**: Updated to use `PathManager` and `ConfigResolver`
  - Uses platform root (`Documents/ScraperPlatform/`) instead of repo root
  - Backward compatible (falls back to repo-relative paths if `platform_config` not available)
  - Lock files now in `.locks/` directory
  - Config backup includes new JSON config files

---

## REMAINING WORK

### 4. Batch File Updates ⏳
**Files to Update**:
- `run_gui.bat` - Use absolute paths, detect platform root
- `1. CanadaQuebec/run_pipeline.bat` - Should call `run_workflow.py` instead of direct scripts
- `2. Malaysia/run_pipeline.bat` - Should call `run_workflow.py` instead of direct scripts  
- `3. Argentina/run_pipeline.bat` - Should call `run_workflow.py` instead of direct scripts

**Changes Needed**:
- Use `SCRAPER_PLATFORM_ROOT` env var or detect `%USERPROFILE%\Documents\ScraperPlatform`
- Call `python <absolute_path_to_run_workflow.py>` instead of relative scripts
- Pass args correctly
- Write logs to platform logs directory

### 5. GUI Updates ⏳
**File**: `scraper_gui.py`

**Changes Needed**:
- Use `ConfigResolver` instead of direct `.env` file access
- Read/write config from `Documents/ScraperPlatform/config/<scraper_id>.env.json`
- Use `PathManager` for all path operations
- Display config with secrets masked
- Validate config before runs

### 6. Config Loader Updates ⏳
**Files to Update**:
- `1. CanadaQuebec/Script/config_loader.py` - Use `ConfigResolver`
- `1. CanadaQuebec/doc/config_loader.py` - Use `ConfigResolver`
- `2. Malaysia/scripts/config_loader.py` - Use `ConfigResolver`
- `3. Argentina/script/*.py` - Replace inline `load_env_file()` with `ConfigResolver`

**Changes Needed**:
- Import `platform_config`
- Use `get_config_resolver().get_config(scraper_id)`
- Use `get_path_manager()` for paths
- Maintain backward compatibility during transition

### 7. Doctor & Sanity Check Commands ⏳
**File**: `platform_config.py` (extend existing `doctor` command)

**Features Needed**:
- `python platform_config.py doctor` - Print platform info (✅ exists)
- `python platform_config.py sanity <scraper_id>` - Validate config keys
- `python platform_config.py migrate` - Migrate old .env to new JSON format

### 8. Migration Documentation ⏳
**File**: `MIGRATION.md`

**Content Needed**:
- How to migrate existing installations
- How to add a new scraper
- Backward compatibility notes
- Troubleshooting guide

### 9. Changelog ⏳
**File**: `CHANGELOG.md`

**Content Needed**:
- Explicit confirmation that business logic was not changed
- List of wiring/config changes only
- Migration steps for users

---

## TESTING CHECKLIST

### Scenarios to Verify
- [ ] Dev run from repo (python/node) with no .env present
- [ ] Dev run with .env present (should still prefer Documents config)
- [ ] First-run on a clean machine (Documents folder empty)
- [ ] Packaged EXE run (PyInstaller/etc.) with no repo access
- [ ] Running from a different working directory (CWD not repo root)
- [ ] Multiple scrapers installed/enabled
- [ ] Missing config keys (UI validation + runner error)
- [ ] Invalid paths / missing input files
- [ ] Non-admin Windows user permissions
- [ ] One-instance rule (avoid recursive self-launching)

---

## IMPLEMENTATION PRIORITY

### Phase 1: Foundation ✅ (COMPLETE)
1. Inventory & documentation
2. Centralized config system
3. Workflow runner updates

### Phase 2: Integration (NEXT)
4. Batch file updates
5. GUI updates
6. Config loader updates

### Phase 3: Tooling & Docs
7. Doctor & sanity check commands
8. Migration documentation
9. Changelog

---

## BACKWARD COMPATIBILITY

### Current State
- Old system still works (repo-relative paths, .env files)
- New system available but not enforced
- Gradual migration path

### Migration Path
1. **Phase 1**: New system available, old system still works
2. **Phase 2**: New system preferred, old system fallback
3. **Phase 3**: Old system deprecated, new system required

### Breaking Changes
- None in Phase 1 (backward compatible)
- Phase 2 may require config migration
- Phase 3 will require full migration

---

## NOTES

- All business logic remains unchanged (scraping, parsing, output schemas)
- Only wiring/config/path management changed
- Platform root detection is automatic (no manual setup required)
- Config files are JSON for better structure and validation
- Secrets are separated from regular config

---

## NEXT STEPS

1. Update batch files to use absolute paths and call `run_workflow.py`
2. Update GUI to use `ConfigResolver` and `PathManager`
3. Update config loaders to use new system
4. Add doctor/sanity check commands
5. Create migration guide
6. Test all scenarios
7. Update changelog

