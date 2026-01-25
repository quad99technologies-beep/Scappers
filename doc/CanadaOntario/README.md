Canada Ontario Pipeline Notes

Overview
- Scraper steps: backup/clean -> products scrape -> EAP prices -> final output.
- Progress and checkpoints live under `output/CanadaOntario/.checkpoints/`.
- Run logs live under `runs/<run_id>/logs/` (pipeline, backup, EAP, output, health).

Output Schema
- `output/CanadaOntario/products.csv`
  - Required: `local_pack_code`, `generic_name`
  - Optional: `brand_name_strength_dosage`, `manufacturer_name`, `mfr_code`, `drug_id`, `price_type`
  - Pricing: `exfactory_price_raw`, `amount_moh_pays_raw`, `exfactory_price`, `reimbursable_price`, `public_with_vat`, `copay`
  - Flags: `interchangeable`, `limited_use`, `therapeutic_notes_requirements`, `qa_notes`
  - Trace: `q_letter`, `detail_url`
- `output/CanadaOntario/ontario_eap_prices.csv`
  - Required: `Effective Start Date`, `DIN`, `Trade name`, `Strength`, `Dosage form`, `DBP (raw)`
  - Derived: `Ex Factory Wholesale Price`, `Ex Factory Wholesale Unit`, `Public With VAT Price`, `RI Price`
  - Reimbursement: `Reimbursable Status`, `Reimbursable Price`, `Reimbursable Rate`, `Reimbursable Notes`, `Copayment Value`
  - Pack info: `Local Pack Description`, `PK keyword present`
- `exports/CanadaOntario/canadaontarioreport_<ddmmyyyy>.csv`
  - Required: `PCID`, `Country`, `Company`, `Generic Name`, `Public With VAT Price`, `LOCAL_PACK_CODE`
  - Reimbursement: `Reimbursement Category`, `Reimbursement Amount`, `Co-Pay Amount`
  - Additional: `Local Pack Description`, `Price Type`, `Interchangeable`, `Limited Use`, `Therapeutic Notes`, `Currency`, `Region`

Dedup Keys
- Products: `local_pack_code` (primary).
- Final output: `LOCAL_PACK_CODE` (derived from `local_pack_code`).

Progress/Resume
- Pipeline checkpoints: `output/CanadaOntario/.checkpoints/pipeline_checkpoint.json`
- Item-level resume: `completed_letters.json` plus checkpoint metadata for `completed_letters`.
- Pipeline lock: `.locks/CanadaOntario.lock` (cleared on completion; cleanup script available).
- Step timing is stored in checkpoint outputs; total pipeline timing in checkpoint metadata.
  - Lock override: set `LOCK_FORCE=true` or run `cleanup_lock.py --force`.

Runbook
- CLI: `python scripts/Canada Ontario/run_pipeline_resume.py` (add `--fresh` or `--step N`).
- Batch: `scripts/Canada Ontario/run_pipeline.bat`
- Cleanup lock: `python scripts/Canada Ontario/cleanup_lock.py` if a stale lock blocks runs.

QA/Health
- Health check: `scripts/Canada Ontario/health_check.py` (optional in pipeline).
- Data validation: `core/data_validator.py` for `CanadaOntario` products.
- QA toggle: `QA_CHECKS_ENABLED` (defaults true).

Proxy (Optional)
- Set `PROXY_URL` in config to route HTTP requests.
- Health check will use proxy when configured.
- Browser error capture: `CAPTURE_BROWSER_ERRORS=true` to save screenshots/page HTML on Selenium failures.

Config Notes
- `config/CanadaOntario.env.json` holds non-secret defaults; secrets go in `secrets`.
- `config/CanadaOntario.env.example` lists optional overrides.
