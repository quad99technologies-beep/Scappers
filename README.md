# Scraper Platform - Repository Structure

## 📂 Directory Layout

```
Scrappers/
├── 📂 core/               # Core Infrastructure (v3.0 - Reorganized)
│   ├── browser/           # Chrome/Firefox automation, stealth
│   ├── config/            # Configuration management
│   ├── data/              # Validation, quality checks
│   ├── db/                # Database connections (Postgres)
│   ├── monitoring/        # Health checks, alerts
│   ├── network/           # Proxy, Tor management
│   ├── pipeline/          # Orchestration base classes
│   ├── progress/          # Tracking & reporting
│   ├── reliability/       # Retry logic, rate limiting
│   └── utils/             # Shared utilities (logging, caching)
│
├── 📂 scripts/            # Scraper Implementations
│   ├── Argentina/         # Selenium Scrapers
│   ├── Malaysia/
│   ├── India/             # Scrapy + Wrapper
│   ├── Russia/
│   ├── Belarus/
│   ├── ... (12+ countries)
│
├── 📂 gui/                # Desktop GUI Application
│   ├── tabs/
│   ├── themes/
│   └── scraper_gui.py     # Main Entry Point
│
├── 📂 internal_tools/     # Helper Scripts
│   ├── database_setup.py
│   └── migration_tools.py
│
├── 📂 doc/                # Documentation
│   ├── PRODUCTION_READINESS_AUDIT.md
│   ├── SCRAPER_STATUS_FINAL.md
│   └── ... (Technical Guides)
│
├── 📂 tests/              # Test Suite
│   ├── test_production_code.py
│   └── smoke_test.py
│
├── Dockerfile             # Container definition
├── docker-compose.yml     # Orchestration
└── requirements.txt       # Python dependencies
```

---

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Environment
Enable the `.env` file with your database credentials:
```bash
cp .env.example .env
# Edit .env with your DB_HOST, DB_USER, etc.
```

### 3. Run a Scraper
```bash
# Standardized entry point for all active scrapers:
python scripts/Argentina/run_pipeline_resume.py --fresh
```

### 4. Launch GUI
```bash
python scraper_gui.py
# or simply double-click run_gui.bat
```

---

## 📊 Project Status

**Version**: 3.1.0 (Production Ready)
**Date**: Feb 15, 2026

*   ✅ **Core Platform**: 100% Ready (Modular, Secure, Scalable)
*   ✅ **Active Scrapers**: 9 Country Modules (Fully Operational)
*   ⚠️ **Legacy Scrapers**: 3 Country Modules (Excluded/Broken - see Audit)

For detailed status, read [**PRODUCTION_READINESS_AUDIT.md**](doc/PRODUCTION_READINESS_AUDIT.md).

---

## 🛠Key Components

### Core Framework (`core/`)
The heart of the system.
*   **ConfigManager**: Single source of truth for all settings.
*   **SmartRetry**: Intelligent retry logic for network resilience.
*   **ChromeInstanceTracker**: Manages browser processes to prevent leaks.
*   **PipelineCheckpoint**: Ensures robust resume capabilities after crashes.

### Database
*   **PostgreSQL**: Primary data store.
*   **Schemas**: Manage via `core/db/schema_registry.py`.

---

## 🤝 Contributing

1.  **New Scrapers**: Create a new folder in `scripts/CountryName`. Use `run_pipeline_resume.py` as the entry point.
2.  **Core Changes**: Add new modules to the appropriate subdirectory in `core/` (e.g., `core/network/new_proxy.py`).
3.  **Documentation**: Keep `doc/` updated with implementation notes.

---
**Maintained by**: Quad99 Technologies
