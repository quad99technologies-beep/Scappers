# Deployment Guide: Foundation Contracts & Features

**Date:** February 6, 2026  
**Purpose:** Step-by-step deployment guide for all implemented features

---

## ✅ Pre-Deployment Checklist

### 1. Verify Prerequisites

- [ ] PostgreSQL database is running and accessible
- [ ] Python 3.8+ installed
- [ ] All dependencies installed (`pip install -r requirements.txt`)
- [ ] Environment variables configured (see below)
- [ ] Backup of existing database (recommended)

### 2. Required Dependencies

```bash
# Core dependencies (should already be installed)
pip install psycopg2-binary python-dotenv

# For API endpoints (optional)
pip install flask flask-cors

# For webhook notifications (optional)
pip install requests

# For scheduler (optional)
pip install python-daemon  # or use systemd/supervisor
```

---

## 🚀 Deployment Steps

### Step 1: Database Migration (REQUIRED)

**⚠️ CRITICAL: Run this first before using any new features**

```bash
# Connect to your PostgreSQL database
psql -d your_database_name -U your_username

# Or use connection string
psql "postgresql://user:password@host:port/database"

# Run the migration
\i sql/migrations/postgres/005_add_step_tracking_columns.sql

# Verify migration
SELECT version FROM _schema_versions WHERE version = 5;
```

**Expected Output:**
- Enhanced columns added to all `*_step_progress` tables
- Enhanced columns added to `run_ledger` table
- `step_retries` table created
- Schema version 5 recorded

**Rollback (if needed):**
```sql
-- Remove enhanced columns (if migration fails)
-- Note: This will lose data in enhanced columns
ALTER TABLE {prefix}_step_progress DROP COLUMN IF EXISTS duration_seconds;
ALTER TABLE {prefix}_step_progress DROP COLUMN IF EXISTS rows_read;
-- ... (repeat for all enhanced columns)
```

---

### Step 2: Configure Environment Variables

Create or update `.env` file in repository root:

```bash
# Database Configuration (REQUIRED)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=scrapers
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password

# Telegram Alerts (OPTIONAL but recommended)
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_ALLOWED_CHAT_IDS=123456789,987654321

# API Endpoints (OPTIONAL)
PIPELINE_API_KEYS=key1,key2,key3
PIPELINE_API_HOST=0.0.0.0
PIPELINE_API_PORT=5000

# Webhook Notifications (OPTIONAL)
# Configure via database: INSERT INTO webhook_configs (event_type, webhook_url) VALUES (...)
```

---

### Step 3: Test Foundation Contracts

```bash
# Test step hooks
python -c "from core.step_hooks import StepHookRegistry; print('Step hooks OK')"

# Test preflight checks
python -c "from core.preflight_checks import PreflightChecker; print('Preflight checks OK')"

# Test alerting contract
python -c "from core.alerting_contract import AlertRuleRegistry; print('Alerting contract OK')"

# Test PCID mapping contract
python -c "from core.pcid_mapping_contract import get_pcid_mapping; print('PCID contract OK')"
```

---

### Step 4: Test Malaysia Pipeline (Already Integrated)

```bash
cd scripts/Malaysia

# Test with fresh run
python run_pipeline_resume.py --fresh

# Verify:
# - Preflight checks run
# - Step hooks emit
# - Enhanced metrics logged
# - Alerts sent (if configured)
```

---

### Step 5: Integrate Argentina Pipeline (Copy Malaysia Pattern)

```bash
# 1. Backup current Argentina pipeline
cp scripts/Argentina/run_pipeline_resume.py scripts/Argentina/run_pipeline_resume.py.backup

# 2. Apply integration (see core/integration_example.py)
# Copy the integration pattern from Malaysia pipeline

# 3. Test Argentina pipeline
cd scripts/Argentina
python run_pipeline_resume.py --fresh
```

**Integration Points:**
- Add foundation imports at top of `run_pipeline_resume.py`
- Add preflight checks in `main()` function
- Update `run_step()` to emit hooks and log enhanced metrics
- Add post-run processing (aggregation, data quality checks)

---

### Step 6: Integrate Netherlands Pipeline (Copy Malaysia Pattern)

Same as Step 5, but for Netherlands:

```bash
cd scripts/Netherlands
# Apply same integration pattern
python run_pipeline_resume.py --fresh
```

---

### Step 7: Start Optional Services (OPTIONAL)

#### A. Pipeline Scheduler (for automated runs)

```bash
# As daemon (requires python-daemon)
python scripts/common/scheduler.py --daemon

# Or use systemd (create service file)
# Or use supervisor (create config)
```

**Systemd Service Example:**
```ini
[Unit]
Description=Pipeline Scheduler
After=network.target postgresql.service

[Service]
Type=simple
User=your_user
WorkingDirectory=/path/to/Scrappers
ExecStart=/usr/bin/python3 scripts/common/scheduler.py
Restart=always

[Install]
WantedBy=multi-user.target
```

#### B. API Server (for external integrations)

```bash
# Start API server
python scripts/common/pipeline_api.py

# Test API
curl -H "X-API-Key: your_key" http://localhost:5000/api/v1/health
curl -H "X-API-Key: your_key" http://localhost:5000/api/v1/pipelines/Malaysia/status
```

**Production Deployment:**
- Use gunicorn/uWSGI for production
- Set up reverse proxy (nginx)
- Configure SSL/TLS
- Set up authentication properly

---

### Step 8: Configure Webhooks (OPTIONAL)

```sql
-- Add webhook configuration
INSERT INTO webhook_configs (scraper_name, event_type, webhook_url, enabled)
VALUES 
    ('Malaysia', 'pipeline.completed', 'https://your-webhook-url.com/complete', true),
    ('Malaysia', 'pipeline.failed', 'https://your-webhook-url.com/failed', true);
```

---

### Step 9: Set Up Automated Backups (OPTIONAL)

```bash
# Add to crontab
crontab -e

# Daily backup at 2 AM
0 2 * * * /usr/bin/python3 /path/to/Scrappers/scripts/common/backup_archive.py --strategy daily

# Weekly backup on Sunday at 1 AM
0 1 * * 0 /usr/bin/python3 /path/to/Scrappers/scripts/common/backup_archive.py --strategy weekly

# Monthly backup on 1st at midnight
0 0 1 * * /usr/bin/python3 /path/to/Scrappers/scripts/common/backup_archive.py --strategy monthly
```

---

## ✅ Post-Deployment Verification

### 1. Verify Database Schema

```sql
-- Check enhanced columns exist
SELECT column_name 
FROM information_schema.columns 
WHERE table_name = 'my_step_progress' 
AND column_name IN ('duration_seconds', 'rows_read', 'log_file_path');

-- Check run_ledger enhancements
SELECT column_name 
FROM information_schema.columns 
WHERE table_name = 'run_ledger' 
AND column_name IN ('total_runtime_seconds', 'slowest_step_number');

-- Check step_retries table exists
SELECT EXISTS (
    SELECT FROM information_schema.tables 
    WHERE table_name = 'step_retries'
);
```

### 2. Test Pipeline Run

```bash
cd scripts/Malaysia
python run_pipeline_resume.py --fresh

# Verify output includes:
# - [PREFLIGHT] Health Checks: ✅
# - [SETUP] Alerting hooks registered
# - [POST-RUN] Data quality checks completed
```

### 3. Verify Enhanced Metrics

```sql
-- Check step progress has enhanced data
SELECT step_number, duration_seconds, rows_processed, log_file_path
FROM my_step_progress
WHERE run_id = 'latest_run_id'
ORDER BY step_number;

-- Check run aggregation
SELECT run_id, total_runtime_seconds, slowest_step_number, slowest_step_name
FROM run_ledger
WHERE scraper_name = 'Malaysia'
ORDER BY started_at DESC
LIMIT 1;
```

### 4. Test Alerting (if configured)

```bash
# Trigger a test failure (or wait for real failure)
# Verify Telegram alert received
```

### 5. Test API (if deployed)

```bash
# Health check
curl http://localhost:5000/api/v1/health

# Get pipeline status
curl -H "X-API-Key: your_key" http://localhost:5000/api/v1/pipelines/Malaysia/status
```

---

## 🔧 Troubleshooting

### Issue: Migration Fails

**Error:** `column already exists` or `table already exists`

**Solution:**
```sql
-- Migration is idempotent (uses IF NOT EXISTS)
-- If errors occur, check:
SELECT * FROM _schema_versions WHERE version = 5;
-- If version 5 exists, migration already ran
```

### Issue: Preflight Checks Fail

**Error:** `Database connection failed`

**Solution:**
- Verify PostgreSQL is running
- Check connection credentials in `.env`
- Test connection: `psql -h host -U user -d database`

### Issue: Step Hooks Not Emitting

**Error:** No hook output in logs

**Solution:**
- Verify `_FOUNDATION_AVAILABLE = True` in pipeline runner
- Check imports are correct
- Verify `setup_alerting_hooks()` is called

### Issue: Enhanced Metrics Not Logged

**Error:** `duration_seconds` is NULL in database

**Solution:**
- Verify migration ran successfully
- Check `log_step_progress()` is called with `duration_seconds` parameter
- Verify table has enhanced columns

---

## 📋 Deployment Checklist

### Pre-Deployment
- [ ] Database backup created
- [ ] Dependencies installed
- [ ] Environment variables configured
- [ ] Test environment verified

### Deployment
- [ ] Schema migration executed
- [ ] Foundation contracts tested
- [ ] Malaysia pipeline tested
- [ ] Argentina pipeline integrated (optional)
- [ ] Netherlands pipeline integrated (optional)

### Post-Deployment
- [ ] Database schema verified
- [ ] Pipeline run successful
- [ ] Enhanced metrics populated
- [ ] Alerts working (if configured)
- [ ] API accessible (if deployed)
- [ ] Scheduler running (if deployed)
- [ ] Backups configured (if desired)

---

## 🎯 Quick Deployment (Minimal)

For quick deployment with minimal setup:

```bash
# 1. Run migration
psql -d your_database -f sql/migrations/postgres/005_add_step_tracking_columns.sql

# 2. Test Malaysia pipeline
cd scripts/Malaysia
python run_pipeline_resume.py --fresh

# Done! Foundation contracts are now active.
```

---

## 📊 Feature Activation Status

| Feature | Activation Required | Status After Deployment |
|---------|-------------------|------------------------|
| **Schema Migration** | Run SQL script | ✅ Active |
| **Step Hooks** | Integrated in pipeline | ✅ Active (Malaysia) |
| **Preflight Checks** | Integrated in pipeline | ✅ Active (Malaysia) |
| **Alerting** | Call `setup_alerting_hooks()` | ✅ Active (Malaysia) |
| **Enhanced Metrics** | Use enhanced `log_step_progress()` | ✅ Active (Malaysia) |
| **Data Quality Checks** | Integrated in pipeline | ✅ Active (Malaysia) |
| **Audit Logging** | Integrated in pipeline | ✅ Active (Malaysia) |
| **Benchmarking** | Integrated in pipeline | ✅ Active (Malaysia) |
| **Scheduler** | Start daemon | ⏳ Manual start |
| **API** | Start server | ⏳ Manual start |
| **Webhooks** | Configure in DB | ⏳ Manual config |
| **Backups** | Configure cron | ⏳ Manual config |

---

## 🚨 Important Notes

1. **Backward Compatibility**: All features are backward compatible. Existing pipelines will continue to work even if new features aren't used.

2. **Gradual Rollout**: You can deploy features gradually:
   - Week 1: Schema migration + Malaysia integration
   - Week 2: Argentina integration
   - Week 3: Netherlands integration
   - Week 4: Optional services (scheduler, API)

3. **No Breaking Changes**: All new code is additive. No existing functionality is modified.

4. **Testing**: Test in a development environment first before production deployment.

---

## 📞 Support

If you encounter issues:

1. Check `IMPLEMENTATION_COMPLETE.md` for feature details
2. Review `core/integration_example.py` for integration patterns
3. Check database logs for migration issues
4. Verify environment variables are set correctly

---

**Deployment Status:** Ready for Production ✅
