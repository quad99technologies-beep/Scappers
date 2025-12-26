# Documentation Index

**Last Updated**: 2025-12-26
**Purpose**: Central documentation for the multi-scraper platform

---

## Quick Start

**New to the platform?** Start here:
1. [MIGRATION.md](MIGRATION.md) - How to migrate to platform config system
2. [WIRING.md](WIRING.md) - Architecture and component wiring
3. [CHANGELOG.md](CHANGELOG.md) - What changed in platform config integration

**Troubleshooting?**
- Run `python platform_config.py doctor` for diagnostics
- Run `python platform_config.py config-check` for config validation
- See [MIGRATION.md](MIGRATION.md) troubleshooting section

---

## Documentation Files

### Core Documentation

#### [WIRING.md](WIRING.md)
**Architecture and System Wiring**

Complete system architecture documentation including:
- Architecture diagram (text-based)
- Configuration resolution precedence
- Path management system
- Data flow diagrams
- Component interaction maps
- Entrypoint documentation
- Locking mechanism
- Error handling
- Testing scenarios

**Use this when**: Understanding how components connect and communicate

---

#### [MIGRATION.md](MIGRATION.md)
**Migration Guide: Legacy → Platform Config**

Step-by-step guide for migrating to the new platform config system:
- Directory structure overview
- Configuration file creation
- Secret management
- Validation steps
- Troubleshooting guide
- Rollback instructions

**Use this when**: Setting up the platform or migrating from old system

---

#### [CHANGELOG.md](CHANGELOG.md)
**Complete Change History**

Comprehensive changelog of platform config integration:
- All 5 phases documented
- File-by-file modifications
- Business logic confirmation (UNCHANGED)
- Commit history
- Breaking changes (NONE)
- Deprecations

**Use this when**: Understanding what changed and why

---

### Migration & Setup

#### [ENV_MIGRATION_NOTICE.md](ENV_MIGRATION_NOTICE.md)
**Quick Migration Notice**

Quick reference for .env file migration:
- What changed (before/after)
- Where secrets are now stored
- How to migrate (both methods)
- Configuration precedence
- FAQ

**Use this when**: Quick reference for env file migration

---

### Audit & Analysis

#### [AUDIT_ANALYSIS.md](AUDIT_ANALYSIS.md)
**Root Cause Analysis**

Detailed analysis of original system issues:
- Problem identification
- Root causes
- Impact assessment
- 5-phase solution plan
- Risk assessment

**Use this when**: Understanding why platform config was needed

---

#### [INVENTORY.md](INVENTORY.md)
**Complete File Inventory**

Comprehensive catalog of all config files and entrypoints:
- All .env files
- All config_loader modules
- All batch files
- All path references
- Platform config structure

**Use this when**: Finding specific config files or understanding scope

---

#### [AUDIT_INVENTORY.md](AUDIT_INVENTORY.md)
**Audit Inventory Snapshot**

Initial inventory snapshot from audit phase:
- Pre-migration file list
- Environment file locations
- Script file locations

**Use this when**: Comparing before/after states

---

#### [AUDIT_SUMMARY.md](AUDIT_SUMMARY.md)
**Audit Summary**

High-level summary of audit findings:
- Key issues discovered
- Recommendations
- Priority ranking

**Use this when**: Quick overview of audit results

---

#### [CLEANUP_SUMMARY.md](CLEANUP_SUMMARY.md)
**Cleanup Summary**

Summary of cleanup operations performed:
- Files removed
- Files consolidated
- Rationale for changes

**Use this when**: Understanding cleanup decisions

---

## Documentation by Task

### I want to...

**Understand the architecture**
→ Read [WIRING.md](WIRING.md)

**Set up the platform for first time**
→ Read [MIGRATION.md](MIGRATION.md) Steps 1-6

**Troubleshoot config issues**
→ See [MIGRATION.md](MIGRATION.md) Troubleshooting section
→ Run `python platform_config.py doctor`

**Understand what changed**
→ Read [CHANGELOG.md](CHANGELOG.md)

**Find a specific config file**
→ Check [INVENTORY.md](INVENTORY.md)

**Understand why we migrated**
→ Read [AUDIT_ANALYSIS.md](AUDIT_ANALYSIS.md)

**Migrate my secrets**
→ Follow [ENV_MIGRATION_NOTICE.md](ENV_MIGRATION_NOTICE.md)

---

## Platform Structure

```
Scappers/                                   # Git repository
├── docs/                                   # THIS FOLDER
│   ├── README.md                          # This file
│   ├── WIRING.md                          # Architecture
│   ├── MIGRATION.md                       # Migration guide
│   ├── CHANGELOG.md                       # Change history
│   ├── ENV_MIGRATION_NOTICE.md            # Env migration
│   ├── INVENTORY.md                       # File inventory
│   └── AUDIT_*.md                         # Audit docs
├── platform_config.py                      # Core config system
├── 1. CanadaQuebec/
│   ├── .env.example                       # Config template
│   └── Script/config_loader.py            # Wraps platform_config
├── 2. Malaysia/
│   ├── .env.example                       # Config template
│   └── scripts/config_loader.py           # Wraps platform_config
└── 3. Argentina/
    ├── .env.example                       # Config template
    └── script/config_loader.py            # Wraps platform_config

%USERPROFILE%\Documents\ScraperPlatform\   # Platform root
├── config/                                 # Config files (NOT in git)
│   ├── platform.json
│   ├── CanadaQuebec.env.json
│   ├── Malaysia.env.json
│   └── Argentina.env.json
├── input/                                  # Input files
├── output/                                 # All outputs
├── logs/                                   # Platform logs
├── cache/                                  # Cache files
├── sessions/                               # Session state
└── .locks/                                 # Lock files (hidden)
```

---

## Configuration Precedence

1. **Runtime Overrides** (command-line args, GUI)
2. **Environment Variables** (OS-level)
3. **Scraper Config** (`Documents/ScraperPlatform/config/<scraper>.env.json`)
4. **Platform Config** (`Documents/ScraperPlatform/config/platform.json`)
5. **Legacy .env** (fallback, deprecated)
6. **Defaults** (hardcoded)

---

## Diagnostic Commands

### Show Platform Configuration
```batch
python platform_config.py doctor
```

Shows:
- Platform paths (with status)
- Config file locations
- Health status

### Validate Required Secrets
```batch
python platform_config.py config-check
```

Shows:
- Required secrets per scraper
- Which secrets are set
- Which secrets are missing

---

## Key Principles

### Security
- ✅ Secrets stored outside git repository
- ✅ Secrets in `Documents/ScraperPlatform/config/` (user-only access)
- ✅ .env files gitignored
- ✅ Automated validation

### Portability
- ✅ Absolute paths (CWD-independent)
- ✅ Works in packaged EXE mode
- ✅ Platform root auto-detected

### Maintainability
- ✅ Single source of truth (platform_config.py)
- ✅ Unified config system
- ✅ Comprehensive documentation
- ✅ Diagnostic tools

### Backward Compatibility
- ✅ Legacy .env files still work (fallback)
- ✅ All existing APIs unchanged
- ✅ Zero business logic changes

---

## Related Files

### Scraper-Specific Documentation

**CanadaQuebec**:
- [1. CanadaQuebec/doc/README.md](../1.%20CanadaQuebec/doc/README.md)
- [1. CanadaQuebec/doc/ANNEXE_V_ANALYSIS.md](../1.%20CanadaQuebec/doc/ANNEXE_V_ANALYSIS.md)
- [1. CanadaQuebec/doc/OPTIMIZATION_SUMMARY.md](../1.%20CanadaQuebec/doc/OPTIMIZATION_SUMMARY.md)

**Malaysia**:
- [2. Malaysia/Doc/USER_MANUAL.md](../2.%20Malaysia/Doc/USER_MANUAL.md)
- [2. Malaysia/Doc/REPOSITORY_INDEX.md](../2.%20Malaysia/Doc/REPOSITORY_INDEX.md)
- [2. Malaysia/Doc/FIXES_APPLIED.md](../2.%20Malaysia/Doc/FIXES_APPLIED.md)

**Argentina**:
- No scraper-specific docs (uses platform docs)

---

## Maintenance

### Keeping Documentation Updated

When making changes to the platform:

1. **Update CHANGELOG.md** with new changes
2. **Update WIRING.md** if architecture changes
3. **Update MIGRATION.md** if migration steps change
4. **Update this README.md** if new docs are added

### Documentation Standards

- **Format**: Markdown (GitHub-flavored)
- **Line Length**: No hard limit (readable is priority)
- **Headers**: Use ATX style (`#`, `##`, `###`)
- **Code Blocks**: Always specify language (```batch, ```python, etc.)
- **Links**: Use relative paths within repo
- **Date Format**: YYYY-MM-DD (ISO 8601)

---

## Questions?

**Platform Issues**: Check [MIGRATION.md](MIGRATION.md) troubleshooting
**Architecture Questions**: Read [WIRING.md](WIRING.md)
**What Changed**: See [CHANGELOG.md](CHANGELOG.md)
**Run Diagnostics**: `python platform_config.py doctor`

---

**Documentation Complete** ✅
