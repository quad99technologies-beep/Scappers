# Wiring Audit - Analysis & Proposed Changes
**Date**: 2025-12-26
**Status**: Proposal for Review
**Risk Level**: 🟡 Medium (No business logic changes, but infrastructure refactor)

---

## EXECUTIVE SUMMARY

The repository has THREE parallel configuration systems that don't communicate:
1. **Legacy system**: `.env` files in each scraper directory + `config_loader.py` modules
2. **Platform system**: `platform_config.py` (ConfigResolver + PathManager) - exists but unused
3. **Workflow system**: `run_workflow.py` + `shared_workflow_runner.py` - exists but unused

**Current State**: ❌ Scrapers write to CWD-relative paths, not platform structure
**Desired State**: ✅ All scrapers write to `Documents/ScraperPlatform/`

**Root Cause**: The new systems exist but aren't integrated. Batch files bypass them and call individual scripts directly.

---

## FINDINGS SUMMARY

### ✅ What's Working
1. **Platform config infrastructure exists**: `platform_config.py` has ConfigResolver + PathManager
2. **Workflow infrastructure exists**: `shared_workflow_runner.py` with ScraperInterface
3. **Adapters exist**: Each scraper has `run_workflow.py` implementing ScraperInterface
4. **GUI exists**: `scraper_gui.py` provides UI
5. **Individual scripts work**: Each scraper's numbered scripts execute correctly

### ❌ What's Broken
1. **Entrypoints bypass new infrastructure**:
   - Batch files call scripts directly (not through run_workflow.py)
   - GUI calls scripts directly (not through run_workflow.py)
   - Result: PathManager and ConfigResolver are never used

2. **Config scattered across files**:
   - `.env` files in each scraper directory
   - EXPOSED SECRETS in version control
   - Three different loading mechanisms (python-dotenv, custom parser, direct dotenv)

3. **Paths are CWD-dependent**:
   - All writes to `./input/`, `./output/`, `./backups/`
   - Won't work in packaged EXE
   - Won't work if run from different directory

4. **No deterministic output structure**:
   - Each run overwrites previous (after backup)
   - No `output/runs/{run_id}/` pattern in use
   - Hard to trace outputs to specific runs

### ⚠️ Security Concerns
1. **Exposed API keys**: CanadaQuebec `.env` has OpenAI key in plain text
2. **Exposed credentials**: Argentina `.env` has login + proxy credentials
3. **No secret masking**: Config loaders don't mask secrets in logs

---

## ROOT CAUSE ANALYSIS

### Why is the platform config unused?

1. **Batch files** call scripts directly:
   ```batch
   python Script\00_backup_and_clean.py
   python Script\01_split_pdf_into_annexes.py
   ...
   ```
   Should call:
   ```batch
   python run_workflow.py
   ```

2. **GUI** calls scripts directly (see scraper_gui.py:200-250):
   ```python
   script_path = scraper_path / scripts_dir / step["script"]
   subprocess.Popen(["python", str(script_path)])
   ```
   Should call:
   ```python
   workflow_path = scraper_path / "run_workflow.py"
   subprocess.Popen(["python", str(workflow_path), "--step", step_id])
   ```

3. **Individual scripts** import their own config_loader:
   ```python
   from config_loader import get_input_dir, get_output_dir
   ```
   Should import:
   ```python
   from platform_config import get_path_manager
   pm = get_path_manager()
   input_dir = pm.get_input_dir(scraper_id)
   ```

### Why do we have three config loaders?

Historical evolution:
1. **First**: Hardcoded paths in each script
2. **Second**: Added config_loader.py per scraper (inconsistent implementations)
3. **Third**: Added platform_config.py (central, but not integrated)

No one removed the old layers when adding new ones.

---

## PROPOSED SOLUTION

### Strategy: **Bridge Pattern** (Minimal Disruption)

Instead of rewriting everything, we'll:
1. **Keep existing scripts as-is** (no business logic changes)
2. **Update entrypoints only** (batch files, GUI, run_workflow.py)
3. **Make config_loader a thin wrapper** around platform_config
4. **Preserve backward compatibility** (fallback to old paths if needed)

### Implementation Phases

#### PHASE 1: Security (Immediate)
**Goal**: Remove exposed secrets from repo

Actions:
1. Create `.env.example` templates (no real values)
2. Add actual `.env` files to `.gitignore`
3. Move secrets to `Documents/ScraperPlatform/config/{scraper}.env.json`
4. Update config_loader to read from both locations (fallback chain)

**Risk**: Low
**Business Logic Impact**: ✅ None
**Rollback**: Easy (revert config_loader change)

#### PHASE 2: Path Integration
**Goal**: Make scripts write to Documents/ScraperPlatform/

Actions:
1. Update `config_loader.py` modules to proxy to PathManager:
   ```python
   # config_loader.py (updated)
   from platform_config import get_path_manager, get_config_resolver

   pm = get_path_manager()
   cr = get_config_resolver()

   def get_input_dir():
       return pm.get_input_dir(SCRAPER_ID)

   def get_output_dir():
       return pm.get_output_dir()
   ```

2. Add scraper_id constant to each config_loader:
   ```python
   SCRAPER_ID = "CanadaQuebec"  # or "Malaysia", "Argentina"
   ```

3. Test each scraper individually

**Risk**: Low-Medium (changes output locations)
**Business Logic Impact**: ✅ None (same data, different location)
**Rollback**: Medium (need to restore old config_loader)

#### PHASE 3: Entrypoint Unification
**Goal**: Make batch files and GUI call run_workflow.py

Actions:
1. Update `run_workflow.py` to accept CLI args:
   ```python
   if __name__ == "__main__":
       parser = argparse.ArgumentParser()
       parser.add_argument("--step", help="Run specific step (01, 02, etc.)")
       parser.add_argument("--all", action="store_true", help="Run all steps")
       args = parser.parse_args()
   ```

2. Update batch files to call run_workflow.py:
   ```batch
   python run_workflow.py --all
   ```

3. Update GUI to call run_workflow.py:
   ```python
   workflow_path = scraper_path / "run_workflow.py"
   subprocess.Popen(["python", str(workflow_path), "--step", step_id])
   ```

4. Make run_workflow.py call individual scripts (preserve existing logic)

**Risk**: Medium (changes execution flow)
**Business Logic Impact**: ✅ None (same scripts run in same order)
**Rollback**: Easy (revert batch files and GUI)

#### PHASE 4: Run ID Structure
**Goal**: Implement output/runs/{run_id}/ pattern

Actions:
1. Update shared_workflow_runner.py to create run directories
2. Make run_workflow.py use WorkflowRunner properly
3. Copy outputs to run directory after each script completes
4. Update GUI to show run history

**Risk**: Low (additive, doesn't remove old outputs)
**Business Logic Impact**: ✅ None
**Rollback**: Easy (just ignore run directories)

#### PHASE 5: Validation & Polish
**Goal**: Add safety checks and documentation

Actions:
1. Implement `doctor` command (already partially done in platform_config.py)
2. Add `config-check` command per scraper
3. Add pre-flight validation in run_workflow.py
4. Create MIGRATION.md for users
5. Update CHANGELOG.md

**Risk**: Low (additive only)
**Business Logic Impact**: ✅ None
**Rollback**: Not needed (no breaking changes)

---

## MINIMAL CHANGES APPROACH

To achieve objectives with minimal risk, we'll change:

### Files to MODIFY (High Priority)
1. **config_loader.py** (3 files):
   - Wrap platform_config instead of reimplementing
   - Preserve same API (get_input_dir, get_output_dir, etc.)
   - Add fallback to old paths if platform config fails

2. **run_pipeline.bat** (3 files):
   - Change from calling scripts directly to calling run_workflow.py
   - Preserve same CLI interface (--max-rows, --loop, etc.)
   - Keep same error handling and logging

3. **scraper_gui.py** (1 file):
   - Change script execution to call run_workflow.py
   - Add run_id to UI
   - Keep same UI layout

4. **run_workflow.py** (3 files):
   - Add CLI argument parsing
   - Make it call individual scripts (preserve sequence)
   - Add WorkflowRunner integration (optional, for run directories)

### Files to CREATE
1. **.env.example** (3 files):
   - Template with placeholder values
   - Document required vs optional keys
   - No secrets

2. **.gitignore** updates:
   - Ignore *.env (except .env.example)
   - Ignore Documents/ScraperPlatform/ (if in repo)

3. **MIGRATION.md**:
   - How to update existing installations
   - How to migrate secrets
   - How to verify new paths

4. **CHANGELOG.md**:
   - Document all changes
   - Confirm business logic unchanged
   - List new features (run directories, config validation)

### Files to LEAVE UNCHANGED
- ✅ All numbered scripts (00_*.py, 01_*.py, ...)
- ✅ platform_config.py (already correct)
- ✅ shared_workflow_runner.py (already correct)
- ✅ All scraping logic, selectors, parsers, etc.

---

## TESTING STRATEGY

### Per-Phase Testing

**Phase 1 (Security)**:
- [ ] Verify .env.example has no secrets
- [ ] Verify actual .env is gitignored
- [ ] Verify config loads from Documents/ScraperPlatform/config/
- [ ] Verify fallback to old .env works
- [ ] Verify secrets are masked in logs

**Phase 2 (Paths)**:
- [ ] Run each scraper, verify outputs in Documents/ScraperPlatform/output/
- [ ] Verify old inputs still work (backward compat)
- [ ] Verify from different CWD (not scraper root)
- [ ] Verify no writes to repo directory

**Phase 3 (Entrypoints)**:
- [ ] Run via batch file, verify calls run_workflow.py
- [ ] Run via GUI, verify calls run_workflow.py
- [ ] Run with --step flag, verify single step works
- [ ] Run with --all flag, verify full pipeline works
- [ ] Verify error handling preserved

**Phase 4 (Run Directories)**:
- [ ] Verify output/runs/{run_id}/ created
- [ ] Verify run.json metadata file created
- [ ] Verify logs in run directory
- [ ] Verify final exports in run directory
- [ ] Verify GUI shows run history

**Phase 5 (Validation)**:
- [ ] Run doctor command, verify paths shown
- [ ] Run config-check, verify required keys validated
- [ ] Trigger validation error, verify clear message
- [ ] Verify MIGRATION.md is accurate

### Scenario Coverage (ALL 10 from requirements)

1. ✅ **Dev run from repo (no .env)**:
   - Should use defaults from config_loader
   - Should write to Documents/ScraperPlatform/

2. ✅ **Dev run with .env present**:
   - Should prefer Documents/ScraperPlatform/config/
   - Should fall back to .env if needed

3. ✅ **First-run on clean machine**:
   - PathManager creates Documents/ScraperPlatform/ structure
   - ConfigResolver creates default configs
   - GUI shows initial setup wizard (optional)

4. ✅ **Packaged EXE run**:
   - No access to repo root (frozen)
   - Must use Documents/ScraperPlatform/ exclusively
   - No sys.path hacks needed

5. ✅ **Different CWD**:
   - run_workflow.py resolves paths absolutely
   - No dependency on `os.getcwd()`

6. ✅ **Multiple scrapers enabled**:
   - Each has own input/output directories
   - Shared platform config
   - Per-scraper config files

7. ✅ **Missing config keys**:
   - ConfigResolver provides defaults
   - Optional: config-check command validates required keys
   - Clear error message if critical key missing

8. ✅ **Invalid paths / missing input files**:
   - run_workflow.py validates inputs before run
   - Clear error message with path resolution
   - Doctor command helps debug

9. ✅ **Non-admin Windows user**:
   - Documents/ScraperPlatform/ in user profile (no admin needed)
   - No writes to Program Files or system dirs

10. ✅ **One-instance rule**:
    - WorkflowRunner creates lock file
    - run_workflow.py checks lock before run
    - Clear error if already running

---

## ROLLBACK PLAN

Each phase is independently reversible:

**Phase 1**:
```bash
git revert <commit>  # Restore old .env files
```

**Phase 2**:
```bash
git revert <commit>  # Restore old config_loader.py
# Old outputs still in repo directories
```

**Phase 3**:
```bash
git revert <commit>  # Restore old batch files and GUI
# Scripts still work individually
```

**Phase 4**:
```bash
# No rollback needed - run directories are additive
# Just ignore them if not wanted
```

**Phase 5**:
```bash
# No rollback needed - documentation and validation are additive
```

---

## RISK ASSESSMENT

| Phase | Risk Level | Impact if Failed | Mitigation |
|-------|-----------|------------------|------------|
| Phase 1: Security | 🟢 Low | Secrets in logs | Test secret masking thoroughly |
| Phase 2: Paths | 🟡 Medium | Outputs in wrong location | Add fallback to old paths, extensive testing |
| Phase 3: Entrypoints | 🟡 Medium | Scripts don't run | Preserve old batch files as `run_pipeline_legacy.bat` |
| Phase 4: Run Dirs | 🟢 Low | Extra disk usage | Additive only, doesn't break existing |
| Phase 5: Validation | 🟢 Low | False positives in validation | Make validation warnings, not errors |

**Overall Risk**: 🟡 Medium (manageable with phased rollout and testing)

---

## SUCCESS CRITERIA

### Must Have (Phase 1-3)
- [ ] No secrets in git repo
- [ ] All scrapers write to Documents/ScraperPlatform/
- [ ] Batch files and GUI call run_workflow.py
- [ ] All 10 scenarios work
- [ ] Business logic unchanged (same outputs for same inputs)

### Should Have (Phase 4)
- [ ] Run directories created with {run_id}/
- [ ] run.json metadata file per run
- [ ] GUI shows run history
- [ ] Logs and artifacts organized by run

### Nice to Have (Phase 5)
- [ ] Doctor command works
- [ ] Config validation works
- [ ] MIGRATION.md complete
- [ ] CHANGELOG.md confirms no business logic changes

---

## NEXT ACTIONS

1. **Review this proposal** with stakeholders
2. **Get approval** for phased approach
3. **Start Phase 1** (security) immediately:
   - Create .env.example files
   - Update .gitignore
   - Test secret migration
4. **Commit Phase 1** with message: `security: move secrets to platform config (no business logic changes)`
5. **Proceed to Phase 2** after Phase 1 validated

---

**END OF ANALYSIS**
