#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline Checkpoint System

Provides checkpoint/resume functionality for all scrapers.
Tracks completed steps and allows resuming from the last completed step.
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Set

log = logging.getLogger("checkpoint")


class PipelineCheckpoint:
    """Manages pipeline checkpoint/resume functionality for scrapers."""
    
    def __init__(self, scraper_name: str, checkpoint_dir: Optional[Path] = None):
        """
        Initialize checkpoint manager.
        
        Args:
            scraper_name: Name of the scraper (e.g., "Argentina", "Malaysia", "CanadaQuebec")
            checkpoint_dir: Directory to store checkpoint files (default: output/scraper_name/.checkpoints)
        """
        self.scraper_name = scraper_name
        
        if checkpoint_dir:
            self.checkpoint_dir = Path(checkpoint_dir)
        else:
            # Default: use output directory
            from platform_config import get_path_manager
            try:
                pm = get_path_manager()
                output_dir = pm.get_output_dir(scraper_name)
            except:
                # Fallback
                output_dir = Path(__file__).parent.parent / "output" / scraper_name
            self.checkpoint_dir = output_dir / ".checkpoints"
        
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_file = self.checkpoint_dir / "pipeline_checkpoint.json"
        self._checkpoint_data = None
    
    def _load_checkpoint(self) -> Dict:
        """Load checkpoint data from file."""
        if self._checkpoint_data is not None:
            return self._checkpoint_data
        
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    self._checkpoint_data = json.load(f)
                return self._checkpoint_data
            except Exception as e:
                log.warning(f"Failed to load checkpoint file: {e}")
                self._checkpoint_data = {}
                return self._checkpoint_data
        else:
            self._checkpoint_data = {
                "scraper": self.scraper_name,
                "last_run": None,
                "completed_steps": [],
                "step_outputs": {},
                "metadata": {}
            }
            return self._checkpoint_data
    
    def _save_checkpoint(self):
        """Save checkpoint data to file."""
        try:
            checkpoint_data = self._load_checkpoint()
            checkpoint_data["last_run"] = datetime.now().isoformat()
            
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            log.error(f"Failed to save checkpoint: {e}")
    
    def mark_step_complete(self, step_number: int, step_name: str, output_files: List[str] = None):
        """
        Mark a step as completed.
        
        Args:
            step_number: Step number (0, 1, 2, ...)
            step_name: Name/description of the step
            output_files: List of output file paths (relative or absolute) created by this step
        """
        checkpoint_data = self._load_checkpoint()
        
        step_key = f"step_{step_number}"
        if step_number not in checkpoint_data["completed_steps"]:
            checkpoint_data["completed_steps"].append(step_number)
            checkpoint_data["completed_steps"].sort()
        
        checkpoint_data["step_outputs"][step_key] = {
            "step_number": step_number,
            "step_name": step_name,
            "completed_at": datetime.now().isoformat(),
            "output_files": output_files or []
        }
        
        self._save_checkpoint()
        log.info(f"[CHECKPOINT] Marked step {step_number} ({step_name}) as complete")
    
    def is_step_complete(self, step_number: int) -> bool:
        """Check if a step has been completed."""
        checkpoint_data = self._load_checkpoint()
        return step_number in checkpoint_data["completed_steps"]
    
    def get_last_completed_step(self) -> Optional[int]:
        """Get the last completed step number, or None if no steps completed."""
        checkpoint_data = self._load_checkpoint()
        completed = checkpoint_data["completed_steps"]
        return max(completed) if completed else None
    
    def get_next_step(self) -> int:
        """Get the next step number to run (first incomplete step)."""
        last_completed = self.get_last_completed_step()
        return (last_completed + 1) if last_completed is not None else 0
    
    def clear_checkpoint(self):
        """Clear all checkpoint data (start fresh)."""
        self._checkpoint_data = {
            "scraper": self.scraper_name,
            "last_run": None,
            "completed_steps": [],
            "step_outputs": {},
            "metadata": {}
        }
        self._save_checkpoint()
        log.info(f"[CHECKPOINT] Cleared checkpoint data for {self.scraper_name}")
    
    def get_checkpoint_info(self) -> Dict:
        """Get checkpoint information."""
        checkpoint_data = self._load_checkpoint()
        last_completed = self.get_last_completed_step()
        next_step = self.get_next_step()
        
        return {
            "scraper": self.scraper_name,
            "last_run": checkpoint_data.get("last_run"),
            "completed_steps": checkpoint_data["completed_steps"],
            "last_completed_step": last_completed,
            "next_step": next_step,
            "total_completed": len(checkpoint_data["completed_steps"])
        }
    
    def verify_output_files(self, step_number: int) -> bool:
        """
        Verify that output files for a step still exist.
        
        Args:
            step_number: Step number to check
            
        Returns:
            True if all output files exist, False otherwise
        """
        checkpoint_data = self._load_checkpoint()
        step_key = f"step_{step_number}"
        
        if step_key not in checkpoint_data["step_outputs"]:
            return False
        
        output_files = checkpoint_data["step_outputs"][step_key].get("output_files", [])
        if not output_files:
            # No output files recorded, assume step is valid if marked complete
            return step_number in checkpoint_data["completed_steps"]
        
        # Check if all output files exist
        for file_path in output_files:
            file_obj = Path(file_path)
            if not file_obj.exists():
                log.warning(f"[CHECKPOINT] Output file missing for step {step_number}: {file_path}")
                return False
        
        return True
    
    def should_skip_step(self, step_number: int, step_name: str, verify_outputs: bool = True) -> bool:
        """
        Determine if a step should be skipped (already completed).
        
        Args:
            step_number: Step number to check
            step_name: Step name (for logging)
            verify_outputs: If True, also verify output files exist
            
        Returns:
            True if step should be skipped, False if it should run
        """
        if not self.is_step_complete(step_number):
            return False
        
        if verify_outputs:
            if not self.verify_output_files(step_number):
                log.warning(f"[CHECKPOINT] Step {step_number} ({step_name}) marked complete but output files missing. Will re-run.")
                return False
        
        log.info(f"[CHECKPOINT] Step {step_number} ({step_name}) already completed. Skipping.")
        return True


def get_checkpoint_manager(scraper_name: str, checkpoint_dir: Optional[Path] = None) -> PipelineCheckpoint:
    """Get or create a checkpoint manager for a scraper."""
    return PipelineCheckpoint(scraper_name, checkpoint_dir)

