#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Canada Quebec Workflow Runner
Uses shared workflow runner with backup-first and deterministic run folders.
"""

import sys
import os
import subprocess
import shutil
import signal
from pathlib import Path

# Add shared runner to path
repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(repo_root))

from shared_workflow_runner import WorkflowRunner, ScraperInterface

# Add Script directory to path for config_loader
script_dir = Path(__file__).resolve().parent / "Script"
sys.path.insert(0, str(script_dir))

try:
    from config_loader import get_input_dir, get_output_dir
except ImportError:
    # Fallback
    def get_input_dir():
        return Path(__file__).resolve().parent / "input"
    def get_output_dir():
        return Path(__file__).resolve().parent / "output"


class CanadaQuebecScraper(ScraperInterface):
    """Canada Quebec scraper adapter"""
    
    def __init__(self, scraper_root: Path):
        self.scraper_root = scraper_root
        self.scripts_dir = scraper_root / "Script"
        self.input_dir = get_input_dir()
        self.output_dir = get_output_dir()
        
    def validate_inputs(self) -> dict:
        """Validate input files"""
        # Try platform config path first
        input_pdf = self.input_dir / "liste-med.pdf"
        
        # If not found, try local input directory as fallback
        if not input_pdf.exists():
            local_input_dir = self.scraper_root / "input"
            local_input_pdf = local_input_dir / "liste-med.pdf"
            if local_input_pdf.exists():
                # Update input_dir to point to local directory for subsequent operations
                self.input_dir = local_input_dir
                input_pdf = local_input_pdf
            else:
                return {
                    "status": "error",
                    "message": f"Input PDF not found in platform location ({self.input_dir / 'liste-med.pdf'}) or local location ({local_input_pdf})"
                }
        
        return {
            "status": "ok",
            "inputs": [str(input_pdf)]
        }
    
    def run_steps(self, run_dir: Path) -> dict:
        """Run all Canada Quebec steps"""
        import shutil
        
        # Step 1: Run backup script FIRST (00_backup_and_clean.py is already in the batch file)
        # The batch file already calls it first, so we just run the batch file
        # But we verify the backup script exists
        backup_script = self.scripts_dir / "00_backup_and_clean.py"
        if not backup_script.exists():
            return {
                "status": "error",
                "message": f"Backup script not found: {backup_script}"
            }
        
        # Step 2: Run the existing batch file (which calls backup first, then all scripts in order)
        batch_file = self.scraper_root / "run_pipeline.bat"
        
        if not batch_file.exists():
            return {
                "status": "error",
                "message": f"Pipeline batch file not found: {batch_file}"
            }
        
        print("Running Canada Quebec pipeline...")
        
        # Use Popen for better control over process termination
        process = None
        try:
            process = subprocess.Popen(
                [str(batch_file)],
                cwd=str(self.scraper_root),
                shell=True,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
            )
            
            # Wait for process to complete
            result_code = process.wait()
            
            if result_code != 0:
                return {
                    "status": "error",
                    "message": f"Pipeline failed (exit code: {result_code})"
                }
        except KeyboardInterrupt:
            # User pressed Ctrl+C - terminate the process and all children
            if process:
                print("\n[INTERRUPTED] Stopping pipeline...")
                try:
                    if sys.platform == "win32":
                        # On Windows, terminate the process and all child processes
                        import ctypes
                        try:
                            # Try to send Ctrl+C to the process group first (graceful)
                            ctypes.windll.kernel32.GenerateConsoleCtrlEvent(1, process.pid)
                        except:
                            pass
                        
                        # Terminate the process
                        process.terminate()
                        
                        # Wait a bit, then kill if still running
                        try:
                            process.wait(timeout=3)
                        except subprocess.TimeoutExpired:
                            # Force kill if still running
                            try:
                                process.kill()
                            except:
                                pass
                            
                            # Also kill any child Python processes that might be running
                            try:
                                # Use taskkill to kill the process tree
                                subprocess.run(
                                    ['taskkill', '/F', '/T', '/PID', str(process.pid)],
                                    timeout=2,
                                    capture_output=True,
                                    stderr=subprocess.DEVNULL,
                                    stdout=subprocess.DEVNULL
                                )
                            except:
                                pass
                    else:
                        # On Unix, send SIGTERM to process group
                        os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                        try:
                            process.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                except Exception as e:
                    print(f"Warning: Error terminating process: {e}")
                    if process:
                        process.kill()
            return {
                "status": "error",
                "message": "Pipeline interrupted by user"
            }
        except Exception as e:
            # Any other error - try to clean up
            if process:
                try:
                    process.terminate()
                    process.wait(timeout=2)
                except:
                    try:
                        process.kill()
                    except:
                        pass
            raise
        
        # Copy outputs to run folder structure (preserve original outputs)
        # Artifacts: split_pdf, qa
        artifacts_dir = run_dir / "artifacts"
        if (self.output_dir / "split_pdf").exists():
            shutil.copytree(
                self.output_dir / "split_pdf",
                artifacts_dir / "split_pdf",
                dirs_exist_ok=True
            )
        if (self.output_dir / "qa").exists():
            shutil.copytree(
                self.output_dir / "qa",
                artifacts_dir / "qa",
                dirs_exist_ok=True
            )
        
        # Exports: CSV files
        exports_dir = run_dir / "exports"
        exports_dir.mkdir(parents=True, exist_ok=True)  # Ensure exports directory exists
        csv_dir = self.output_dir / "csv"
        if csv_dir.exists():
            csv_files = list(csv_dir.glob("*.csv"))
            if csv_files:
                for csv_file in csv_files:
                    shutil.copy2(csv_file, exports_dir / csv_file.name)
            else:
                print(f"Warning: No CSV files found in {csv_dir}")
        else:
            print(f"Warning: CSV directory does not exist: {csv_dir}")
        
        return {"status": "ok"}
    
    def write_outputs(self, run_dir: Path) -> dict:
        """Collect output files"""
        exports_dir = run_dir / "exports"
        outputs = []
        
        # Find all CSV files in exports
        if exports_dir.exists():
            csv_files = list(exports_dir.glob("*.csv"))
            for csv_file in csv_files:
                outputs.append({
                    "type": "csv",
                    "path": str(csv_file),
                    "name": csv_file.name
                })
            
            # Also check the original CSV directory for the final report if not found
            if not any("canadaquebecreport" in out["name"].lower() for out in outputs):
                csv_dir = self.output_dir / "csv"
                if csv_dir.exists():
                    final_reports = list(csv_dir.glob("canadaquebecreport_*.csv"))
                    for report_file in final_reports:
                        # Copy to exports if not already there
                        dest_file = exports_dir / report_file.name
                        if not dest_file.exists():
                            shutil.copy2(report_file, dest_file)
                        outputs.append({
                            "type": "csv",
                            "path": str(dest_file),
                            "name": report_file.name
                        })
        else:
            print(f"Warning: Exports directory does not exist: {exports_dir}")
        
        return {
            "status": "ok",
            "outputs": outputs
        }


def main():
    """Main entry point"""
    import os
    
    repo_root = Path(__file__).resolve().parent.parent
    scraper_root = Path(__file__).resolve().parent
    
    # Set up signal handler for graceful shutdown
    def signal_handler(sig, frame):
        print("\n\n[INTERRUPTED] Received interrupt signal. Cleaning up...")
        sys.exit(130)  # Standard exit code for Ctrl+C
    
    signal.signal(signal.SIGINT, signal_handler)
    if hasattr(signal, 'SIGTERM'):
        signal.signal(signal.SIGTERM, signal_handler)
    
    runner = WorkflowRunner("CanadaQuebec", scraper_root, repo_root)
    scraper = CanadaQuebecScraper(scraper_root)
    
    result = None
    try:
        result = runner.run(scraper)
        
        if result["status"] == "ok":
            print(f"\n[OK] Run completed successfully!")
            print(f"  Run ID: {result['run_id']}")
            print(f"  Run directory: {result['run_dir']}")
            print(f"  Backup: {result['backup_dir']}")
            print(f"\n  Output Files ({len(result.get('outputs', []))} files):")
            
            # Show output locations
            outputs = result.get('outputs', [])
            if outputs:
                for output in outputs:
                    print(f"    - {output['name']}")
                    print(f"      Location: {output['path']}")
            else:
                print("    (No output files found)")
            
            # Also show platform output directory
            print(f"\n  Platform Output Directory:")
            print(f"    CSV files: {scraper.output_dir / 'csv'}")
            print(f"    Split PDFs: {scraper.output_dir / 'split_pdf'}")
            print(f"    QA Reports: {scraper.output_dir / 'qa'}")
            
            return 0
        else:
            print(f"\n[FAILED] Run failed: {result.get('message', 'Unknown error')}")
            return 1
    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] Pipeline interrupted by user")
        return 130
    # Note: Lock is automatically released by WorkflowRunner's internal cleanup
    # No manual release needed here


if __name__ == "__main__":
    exit_code = main()

    # Force cleanup and exit
    # Flush all output streams
    sys.stdout.flush()
    sys.stderr.flush()

    # Force exit to ensure process terminates
    # (required because subprocess spawned by GUI may have lingering resources)
    os._exit(exit_code)

