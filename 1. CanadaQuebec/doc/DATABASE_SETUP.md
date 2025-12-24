# Database Schema Setup

This document describes the database schema setup for the Canada Quebec RAMQ scraper.

## Files

The SQL scripts are located in the `schema/` directory:
- `create_tables.sql` - Creates all required tables, indexes, and triggers
- `insert_queries.sql` - Parameterized query templates (for reference only, queries are embedded in Python code)

## Quick Setup

1. **Create PostgreSQL database:**
   ```bash
   createdb scraper_db
   ```

2. **Run schema creation:**
   ```bash
   psql -d scraper_db -f schema/create_tables.sql
   ```

3. **Verify tables were created:**
   ```sql
   \dt
   ```
   Should show: `scraper_runs`, `extracted_data`, `standard_format_data`

4. **Verify indexes:**
   ```sql
   \di
   ```
   Should show indexes on `run_id`, `scraper_id`, `din`, `local_pack_code`, etc.

## Table Structure

### scraper_runs
Tracks each execution of the scraper with metadata.

### extracted_data
Stores raw extracted data from PDF processing (step_04).

### standard_format_data
Stores transformed data in standard format (step_07).

## Notes

- All tables include automatic `updated_at` timestamp management via triggers
- Foreign key constraints ensure data integrity
- Indexes are optimized for common query patterns
- All timestamps use `TIMESTAMP WITH TIME ZONE` for proper timezone handling

