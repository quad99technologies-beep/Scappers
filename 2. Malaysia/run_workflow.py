#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Malaysia Workflow Runner
Uses shared workflow runner with backup-first and deterministic run folders.
"""

import sys
import os
import subprocess
import shutil
from pathlib import Path

# Add shared runner to path
repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(repo_root))

from shared_workflow_runner import WorkflowRunner, ScraperInterface


class MalaysiaScraper(ScraperInterface):
    """Malaysia scraper adapter"""
    
    def __init__(self, scraper_root: Path):
        self.scraper_root = scraper_root
        self.scripts_dir = scraper_root / "scripts"
        self.input_dir = scraper_root / "input"
        self.output_dir = scraper_root / "output"
        
    def validate_inputs(self) -> dict:
        """Validate input files"""
        inputs = []
        
        # Check for expected input files
        input_files = [
            "Malaysia_PCID.csv",
            "products.csv"
        ]
        
        for input_file in input_files:
            input_path = self.input_dir / input_file
            if input_path.exists():
                inputs.append(str(input_path))
        
        if not inputs:
            return {
                "status": "warning",
                "message": "No expected input files found, but continuing..."
            }
        
        return {
            "status": "ok",
            "inputs": inputs
        }
    
    def run_steps(self, run_dir: Path) -> dict:
        """Run all Malaysia steps"""
        # Step 1: Run backup script FIRST
        backup_script = self.scripts_dir / "00_backup_and_clean.py"
        if backup_script.exists():
            print("Running backup script...")
            result = subprocess.run(
                [sys.executable, str(backup_script)],
                cwd=str(self.scraper_root),
                capture_output=False
            )
            if result.returncode != 0:
                return {
                    "status": "error",
                    "message": f"Backup script failed (exit code: {result.returncode})"
                }
        else:
            print("Warning: Backup script not found, skipping backup...")
        
        # Step 2: Run the existing batch file (which calls all scripts in order)
        batch_file = self.scraper_root / "run_pipeline.bat"
        
        if not batch_file.exists():
            return {
                "status": "error",
                "message": f"Pipeline batch file not found: {batch_file}"
            }
        
        print("Running Malaysia pipeline...")
        result = subprocess.run(
            [str(batch_file)],
            cwd=str(self.scraper_root),
            shell=True
        )
        
        if result.returncode != 0:
            return {
                "status": "error",
                "message": f"Pipeline failed (exit code: {result.returncode})"
            }
        
        # Copy outputs to run folder structure (preserve original outputs)
        exports_dir = run_dir / "exports"
        if self.output_dir.exists():
            for output_file in self.output_dir.glob("*.csv"):
                shutil.copy2(output_file, exports_dir / output_file.name)
            
            # Copy subdirectories as artifacts
            for subdir in self.output_dir.iterdir():
                if subdir.is_dir():
                    shutil.copytree(
                        subdir,
                        run_dir / "artifacts" / subdir.name,
                        dirs_exist_ok=True
                    )
        
        return {"status": "ok"}
    
    def write_outputs(self, run_dir: Path) -> dict:
        """Collect output files"""
        exports_dir = run_dir / "exports"
        outputs = []
        
        # Find all CSV files in exports
        for csv_file in exports_dir.glob("*.csv"):
            outputs.append({
                "type": "csv",
                "path": str(csv_file),
                "name": csv_file.name
            })
        
        return {
            "status": "ok",
            "outputs": outputs
        }


def main():
    """Main entry point"""
    repo_root = Path(__file__).resolve().parent.parent
    scraper_root = Path(__file__).resolve().parent
    
    runner = WorkflowRunner("Malaysia", scraper_root, repo_root)
    scraper = MalaysiaScraper(scraper_root)
    
    try:
        result = runner.run(scraper)
        
        if result["status"] == "ok":
            print(f"\n[OK] Run completed successfully!")
            print(f"  Run ID: {result['run_id']}")
            print(f"  Run directory: {result['run_dir']}")
            print(f"  Backup: {result['backup_dir']}")
            print(f"  Outputs: {len(result.get('outputs', []))} files")
            return 0
        else:
            print(f"\n[FAILED] Run failed: {result.get('message', 'Unknown error')}")
            return 1
    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] Pipeline interrupted by user")
        return 130
    finally:
        # Ensure lock is always released, even if there's an error
        try:
            runner.release_lock()
            print("[OK] Lock released")
        except Exception as e:
            print(f"[WARNING] Could not release lock: {e}")


if __name__ == "__main__":
    exit_code = main()

    # Force cleanup and exit
    # Flush all output streams
    sys.stdout.flush()
    sys.stderr.flush()

    # Force exit to ensure process terminates
    # (required because subprocess spawned by GUI may have lingering resources)
    os._exit(exit_code)

