# Deployment Checklist

**Date:** February 6, 2026  
**Use this checklist to ensure complete deployment**

---

## ✅ Pre-Deployment

- [ ] **Backup Database**
  ```bash
  pg_dump -h localhost -U postgres scrapers > backup_before_deployment.sql
  ```

- [ ] **Verify Dependencies**
  ```bash
  pip install psycopg2-binary python-dotenv flask flask-cors requests
  ```

- [ ] **Configure Environment**
  - [ ] Create/update `.env` file
  - [ ] Set `POSTGRES_*` variables
  - [ ] Set `TELEGRAM_*` variables (optional)
  - [ ] Set `PIPELINE_API_*` variables (optional)

---

## 🚀 Deployment

- [ ] **Run Schema Migration**
  ```bash
  psql -d your_database -f sql/migrations/postgres/005_add_step_tracking_columns.sql
  ```

- [ ] **Verify Migration**
  ```sql
  SELECT version FROM _schema_versions WHERE version = 5;
  ```

- [ ] **Test Foundation Contracts**
  ```bash
  python -c "from core.step_hooks import StepHookRegistry; print('OK')"
  python -c "from core.preflight_checks import PreflightChecker; print('OK')"
  python -c "from core.alerting_contract import AlertRuleRegistry; print('OK')"
  ```

- [ ] **Test Malaysia Pipeline**
  ```bash
  cd scripts/Malaysia
  python run_pipeline_resume.py --fresh
  ```
  - [ ] Preflight checks run
  - [ ] Step hooks emit
  - [ ] Enhanced metrics logged
  - [ ] Post-run checks complete

---

## 🔧 Integration (Optional)

- [ ] **Integrate Argentina Pipeline**
  - [ ] Copy integration pattern from Malaysia
  - [ ] Test pipeline run
  - [ ] Verify enhanced metrics

- [ ] **Integrate Netherlands Pipeline**
  - [ ] Copy integration pattern from Malaysia
  - [ ] Test pipeline run
  - [ ] Verify enhanced metrics

---

## 🎛️ Optional Services

- [ ] **Start Scheduler** (if needed)
  ```bash
  python services/scheduler.py --daemon
  ```

- [ ] **Start API Server** (if needed)
  ```bash
  python services/pipeline_api.py
  ```

- [ ] **Configure Webhooks** (if needed)
  ```sql
  INSERT INTO webhook_configs (event_type, webhook_url) VALUES (...);
  ```

- [ ] **Set Up Backups** (if needed)
  ```bash
  # Add to crontab
  0 2 * * * python services/backup_archive.py --strategy daily
  ```

---

## ✅ Post-Deployment Verification

- [ ] **Database Schema**
  ```sql
  -- Verify enhanced columns
  SELECT column_name FROM information_schema.columns 
  WHERE table_name = 'my_step_progress' 
  AND column_name IN ('duration_seconds', 'rows_read');
  ```

- [ ] **Pipeline Run**
  - [ ] Run completes successfully
  - [ ] Enhanced metrics populated
  - [ ] Alerts sent (if configured)
  - [ ] Audit logs created

- [ ] **Features Working**
  - [ ] Dashboard data accessible
  - [ ] Run comparison works
  - [ ] Trend analysis works
  - [ ] API endpoints respond (if deployed)

---

## 📊 Deployment Status

**Foundation Contracts:** ✅ Ready  
**Features:** ✅ Ready  
**Malaysia Integration:** ✅ Complete  
**Argentina Integration:** ⏳ Pending  
**Netherlands Integration:** ⏳ Pending  
**Optional Services:** ⏳ Pending  

---

**Ready to deploy!** Follow the checklist above step by step.
