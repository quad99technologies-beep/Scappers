-- Update run_ledger status from 'stopped' to 'running' for specific run_id
UPDATE run_ledger 
SET status = 'running'
WHERE run_id = '20260206_160604_5d97a684'
  AND scraper_name = 'Argentina';

-- Verify the update
SELECT run_id, scraper_name, status, started_at, ended_at
FROM run_ledger
WHERE run_id = '20260206_160604_5d97a684';
