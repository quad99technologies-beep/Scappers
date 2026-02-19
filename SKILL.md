# Scraper Platform Skill

This skill provides a comprehensive interface for managing the Quad99 Scraper Platform. It includes capabilities for running existing scrapers, creating new ones, and maintaining the core infrastructure.

## skill_functions

### `run_scraper`
Executes the data collection pipeline for a specified country or market.
- **Usage**: `python scripts/<Country>/run_pipeline_resume.py`
- **Context**: Use this when you need to update data for a region.
- **Args**:
  - `country`: The directory name in `scripts/` (e.g., `Argentina`).
  - `fresh`: Boolean. If true, adds `--fresh` to restart the pipeline.

### `add_scraper`
Scaffolds a new scraper module for a new country.
- **Usage**: Follows the [Add Scraper Workflow](.agent/workflows/add_scraper.md).
- **Context**: Use this when the user needs to target a new website or country.
- **Process**:
  1. Create `scripts/<Country>`.
  2. Implement `run_pipeline_resume.py`.
  3. Implement step scripts using `core` modules.

### `debug_scraper`
Investigates and resolves issues in failing scrapers.
- **Usage**: Analyze logs in `logs/` and `core.monitoring`.
- **Context**: Use checks like `check_memory_limit` or `SmartRetry` logs to diagnose failures.

### `manage_database`
Handles schema updates and data integrity.
- **Location**: `sql/` contains schema definitions.
- **Context**: Use `core.db` for connections.

## Core Documentation
For detailed API usage of the infrastructure, refer to **`core/README.md`**.

### Key Modules
- **`core.browser`**: Webdriver management.
- **`core.config`**: Environment and path management.
- **`core.db`**: Database connectivity.
- **`core.utils.logger`**: Centralized logging.

## Rules & Standards
All operations must adhere to the rules defined in **`.agent/rules.md`**.
- Do not hardcode paths.
- Use the `core` framework.
- Ensure idempotency.
