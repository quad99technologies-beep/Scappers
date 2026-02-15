# Scraper Platform

A unified platform for running multiple pharmaceutical data scrapers with centralized GUI, configuration management, and shared utilities.

---

## 🚀 Quick Start

### For New Users

1. **Platform Overview:** Read [`doc/README.md`](doc/README.md)
2. **Deploy Features:** Follow [`doc/deployment/DEPLOY_NOW.md`](doc/deployment/DEPLOY_NOW.md) (5 minutes)

### For Developers

1. **Developer Guide:** [`doc/general/DEVELOPER_ONBOARDING_GUIDE.md`](doc/general/DEVELOPER_ONBOARDING_GUIDE.md)
2. **Documentation Index:** [`doc/DOCUMENTATION_INDEX.md`](doc/DOCUMENTATION_INDEX.md)

---

## 📚 Documentation

All documentation is organized in the `doc/` directory:

- **Deployment:** [`doc/deployment/`](doc/deployment/) - Deployment guides and checklists
- **Implementation:** [`doc/implementation/`](doc/implementation/) - Feature implementation status
- **Project:** [`doc/project/`](doc/project/) - Project-wide documentation
- **General:** [`doc/general/`](doc/general/) - Cross-cutting concerns
- **Regions:** [`doc/Argentina/`](doc/Argentina/), [`doc/Malaysia/`](doc/Malaysia/), etc. - Region-specific docs

**Complete Index:** [`doc/DOCUMENTATION_INDEX.md`](doc/DOCUMENTATION_INDEX.md)

---

## 🎯 Key Features

- ✅ **Unified GUI** - Single interface for all scrapers
- ✅ **Postgres-Only** - PostgreSQL as single source of truth
- ✅ **Step Tracking** - Complete lifecycle tracking (duration, metrics, errors)
- ✅ **Foundation Contracts** - Standardized hooks, checks, alerting
- ✅ **23 High-Value Features** - Dashboard, benchmarking, scheduling, API, etc.

---

## 📖 Getting Started

### Prerequisites

- Python 3.8+
- PostgreSQL database
- Chrome/ChromeDriver (for browser-based scrapers)

### Installation

```bash
pip install -r requirements.txt
```

### Running the GUI

```bash
python scraper_gui.py
```

Or on Windows:
```bash
run_gui.bat
```

---

## 📁 Project Structure

```
Scrappers/
├── config/              # Configuration files (.env.json)
├── core/               # Shared utilities and contracts
├── scripts/            # Per-region pipeline scripts
├── doc/                # All documentation
│   ├── deployment/     # Deployment guides ⭐
│   ├── implementation/ # Implementation status ⭐
│   ├── project/        # Project-wide docs
│   ├── general/        # Cross-cutting docs
│   └── [regions]/      # Region-specific docs
├── sql/                # Database schemas and migrations
├── gui/                # GUI components
└── requirements.txt    # Python dependencies
```

---

## 🔗 Quick Links

- **Deploy Now:** [`doc/deployment/DEPLOY_NOW.md`](doc/deployment/DEPLOY_NOW.md)
- **Implementation Status:** [`doc/implementation/IMPLEMENTATION_COMPLETE.md`](doc/implementation/IMPLEMENTATION_COMPLETE.md)
- **Gap Analysis:** [`doc/project/GAP_ANALYSIS_MALAYSIA_ARGENTINA_NETHERLANDS.md`](doc/project/GAP_ANALYSIS_MALAYSIA_ARGENTINA_NETHERLANDS.md)
- **Platform Guide:** [`doc/README.md`](doc/README.md)

---

## 📞 Support

For detailed documentation, see [`doc/DOCUMENTATION_INDEX.md`](doc/DOCUMENTATION_INDEX.md)

---

**Last Updated:** February 6, 2026
