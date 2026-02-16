#!/usr/bin/env python3
"""
Reset checkpoint to start from step 4 (company search).
"""
import os
import sys
import json
from pathlib import Path

# Ensure Argentina directory is at the front of sys.path to prioritize local 'db' package
# This fixes conflict with core/db which might be in sys.path
_script_dir = Path(__file__).resolve().parent
_argentina_dir = _script_dir.parent

sys.path = [p for p in sys.path if not Path(p).name == 'core']
if str(_argentina_dir) in sys.path:
    sys.path.remove(str(_argentina_dir))
sys.path.insert(0, str(_argentina_dir))

# Force re-import of db module if it was incorrectly loaded from core/db
if 'db' in sys.modules:
    del sys.modules['db']

from config_loader import get_output_dir
from core.db.connection import CountryDB
from db.schema import apply_argentina_schema
from core.pipeline.pipeline_checkpoint import get_checkpoint_manager

# Get run_id
output_dir = get_output_dir()
run_id_file = output_dir / '.current_run_id'
if run_id_file.exists():
    run_id = run_id_file.read_text(encoding='utf-8').strip()
    print(f'Run ID: {run_id}')
else:
    print('No run_id found')
    sys.exit(1)

# Update scrape_source for products with total_records > 0
db = CountryDB('Argentina')
apply_argentina_schema(db)

with db.cursor() as cur:
    # Update scrape_source
    cur.execute('''
        UPDATE ar_product_index
        SET scrape_source = 'selenium_product'
        WHERE run_id = %s
          AND total_records > 0
          AND (scrape_source IS NULL OR scrape_source = '')
    ''', (run_id,))
    updated = cur.rowcount
    print(f'Updated {updated} products with scrape_source = selenium_product')

db.commit()

# Reset checkpoint to only have steps 0-3 completed
cp = get_checkpoint_manager('Argentina')
checkpoint_data = cp._load_checkpoint()
print(f"Before: completed_steps = {checkpoint_data.get('completed_steps', [])}")

# Keep only steps 0, 1, 2, 3
completed = checkpoint_data.get('completed_steps', [])
new_completed = [s for s in completed if s <= 3]

# Update the internal checkpoint data
cp._checkpoint_data['completed_steps'] = new_completed

# Also update step_info to only keep steps 0-3
step_info = cp._checkpoint_data.get('step_info', {})
new_step_info = {k: v for k, v in step_info.items() if int(k) <= 3}
cp._checkpoint_data['step_info'] = new_step_info

# Save checkpoint
cp._save_checkpoint()
print(f"After: completed_steps = {cp._checkpoint_data.get('completed_steps', [])}")

info = cp.get_checkpoint_info()
print(f"Next step: {info['next_step']}")
