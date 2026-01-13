# Checkpoint Creation Utility

## Overview
The `create_checkpoint.py` script allows you to manually create or edit checkpoint files for any scraper. This is useful when you need to resume from a specific step after a crash or manual intervention.

## Location
The script is located at: `scripts/create_checkpoint.py`

## Usage Examples

### 1. Mark steps 0-3 as complete (resume from step 4)
```bash
python scripts/create_checkpoint.py Argentina --step 3
```
This will mark steps 0, 1, 2, and 3 as complete, so the pipeline will resume from step 4.

### 2. Mark specific steps as complete
```bash
python scripts/create_checkpoint.py Argentina --steps 0,1,2,3
```
This marks only the specified steps as complete.

### 3. View current checkpoint status
```bash
python scripts/create_checkpoint.py Argentina --view
```
Shows the current checkpoint file location, completed steps, and next step to run.

### 4. Clear checkpoint (start fresh)
```bash
python scripts/create_checkpoint.py Argentina --clear
```
Clears the checkpoint so the pipeline starts from step 0.

### 5. List all scrapers and their checkpoint status
```bash
python scripts/create_checkpoint.py --list
```
Shows checkpoint status for Argentina, Malaysia, and CanadaQuebec.

### 6. Interactive mode
```bash
python scripts/create_checkpoint.py Argentina --interactive
```
Provides an interactive menu to create, view, or clear checkpoints.

## Argentina Pipeline Steps
- Step 0: Backup and Clean
- Step 1: Get Product List
- Step 2: Prepare URLs
- Step 3: Scrape Products (Selenium)
- Step 4: Scrape Products (API)
- Step 5: Translate Using Dictionary
- Step 6: Generate Output

## Checkpoint File Location
Checkpoint files are stored at:
```
output/Argentina/.checkpoints/pipeline_checkpoint.json
```
Or with platform config:
```
Documents/ScraperPlatform/output/Argentina/.checkpoints/pipeline_checkpoint.json
```

## Example: Create checkpoint to resume from step 4
If you want to resume from step 4 (Translation), mark steps 0-3 as complete:
```bash
python scripts/create_checkpoint.py Argentina --step 3
```

## Viewing Checkpoints in GUI
You can also view checkpoints in the GUI:
1. Select the scraper (Argentina, Malaysia, or CanadaQuebec)
2. Click the "View Checkpoint" button in the Pipeline Control section
3. This opens a window showing checkpoint details and file location

