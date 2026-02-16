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
from typing import Any, Dict, List, Optional, Set

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
            from core.config.config_manager import ConfigManager
            try:
                # Migrated: get_path_manager() -> ConfigManager
                output_dir = ConfigManager.get_output_dir(scraper_name)
            except:
                # Fallback
                output_dir = Path(__file__).parent.parent / "output" / scraper_name
            self.checkpoint_dir = output_dir / ".checkpoints"
        
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_file = self.checkpoint_dir / "pipeline_checkpoint.json"
        self._checkpoint_data = None

    def _default_checkpoint_data(self) -> Dict:
        return {
            "scraper": self.scraper_name,
            "last_run": None,
            "completed_steps": [],
            "step_outputs": {},
            "metadata": {},
            "events": [],
            "event_seq": 0,
        }

    def _validate_checkpoint_data(self, data: Dict) -> bool:
        if not isinstance(data, dict):
            return False
        if data.get("scraper") != self.scraper_name:
            return False
        if "completed_steps" not in data or not isinstance(data["completed_steps"], list):
            return False
        if "step_outputs" not in data or not isinstance(data["step_outputs"], dict):
            return False
        if "metadata" not in data or not isinstance(data["metadata"], dict):
            return False
        if "events" not in data:
            data["events"] = []
        if not isinstance(data.get("events"), list):
            data["events"] = []
        if "event_seq" not in data or not isinstance(data.get("event_seq"), int):
            data["event_seq"] = 0
        return True

    def _event_signature(self, event: Dict[str, Any]) -> tuple:
        return (
            event.get("event_type"),
            event.get("run_id"),
            event.get("status"),
            event.get("step_number"),
            event.get("step_name"),
            event.get("source"),
            event.get("message"),
        )

    def _json_safe(self, value: Any) -> Any:
        """
        Convert values to JSON-serializable forms for timeline event payloads.
        """
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, dict):
            return {str(k): self._json_safe(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self._json_safe(v) for v in value]
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception:
                pass
        return str(value)

    def _append_event(
        self,
        checkpoint_data: Dict[str, Any],
        event_type: str,
        run_id: Optional[str] = None,
        status: Optional[str] = None,
        step_number: Optional[int] = None,
        step_name: Optional[str] = None,
        source: str = "checkpoint",
        message: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Append an event to checkpoint timeline. Returns True when appended.
        """
        event = {
            "timestamp": datetime.now().isoformat(),
            "event_type": event_type,
            "run_id": run_id,
            "status": status,
            "step_number": step_number,
            "step_name": step_name,
            "source": source,
            "message": message,
            "details": self._json_safe(details or {}),
        }

        # Drop empty fields to keep timeline compact
        event = {k: v for k, v in event.items() if v not in (None, "", {}, [])}

        events = checkpoint_data.setdefault("events", [])
        if events:
            last = events[-1]
            if self._event_signature(last) == self._event_signature(event):
                return False

        checkpoint_data["event_seq"] = int(checkpoint_data.get("event_seq", 0)) + 1
        event["sequence"] = checkpoint_data["event_seq"]
        events.append(event)

        max_events = 2000
        if len(events) > max_events:
            checkpoint_data["events"] = events[-max_events:]

        return True

    def record_event(
        self,
        event_type: str,
        run_id: Optional[str] = None,
        status: Optional[str] = None,
        step_number: Optional[int] = None,
        step_name: Optional[str] = None,
        source: str = "checkpoint",
        message: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Persist a structured pipeline event in checkpoint timeline.
        """
        checkpoint_data = self._load_checkpoint()
        appended = self._append_event(
            checkpoint_data=checkpoint_data,
            event_type=event_type,
            run_id=run_id,
            status=status,
            step_number=step_number,
            step_name=step_name,
            source=source,
            message=message,
            details=details,
        )
        if appended:
            self._save_checkpoint()
        return appended
    
    def _load_checkpoint(self) -> Dict:
        """Load checkpoint data from file."""
        if self._checkpoint_data is not None:
            return self._checkpoint_data
        
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if not self._validate_checkpoint_data(data):
                    raise ValueError("Invalid checkpoint structure or scraper mismatch")
                self._checkpoint_data = data
                return self._checkpoint_data
            except (json.JSONDecodeError, IOError, OSError, ValueError) as e:
                log.warning(f"Failed to load checkpoint file: {e}")
                try:
                    backup_path = self.checkpoint_file.with_suffix(".invalid.json")
                    if backup_path.exists():
                        backup_path.unlink()
                    self.checkpoint_file.replace(backup_path)
                    log.warning(f"Checkpoint file moved to: {backup_path}")
                except (OSError, IOError):
                    pass
                self._checkpoint_data = self._default_checkpoint_data()
                return self._checkpoint_data
        else:
            self._checkpoint_data = self._default_checkpoint_data()
            return self._checkpoint_data
    
    def _save_checkpoint(self):
        """Save checkpoint data to file (atomic write)."""
        try:
            checkpoint_data = self._load_checkpoint()
            checkpoint_data["last_run"] = datetime.now().isoformat()
            # Step 0 cleanup may remove output/.checkpoints while process is running.
            # Recreate directory before each write so checkpointing remains durable.
            self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
            self.checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Atomic write: write to temp file, then rename
            import tempfile
            import shutil
            temp_file = self.checkpoint_file.with_suffix('.tmp')
            try:
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)
                # Atomic rename (Windows requires replace)
                if self.checkpoint_file.exists():
                    self.checkpoint_file.replace(self.checkpoint_file.with_suffix('.backup'))
                temp_file.replace(self.checkpoint_file)
            except (OSError, IOError) as e:
                # If atomic write fails, try direct write as fallback
                log.warning(f"Atomic write failed, using fallback: {e}")
                try:
                    if temp_file.exists():
                        temp_file.unlink()
                except (OSError, IOError):
                    pass
                with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                    json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            log.error(f"Failed to save checkpoint: {e}")
    
    def mark_step_complete(self, step_number: int, step_name: str, output_files: List[str] = None, duration_seconds: float = None):
        """
        Mark a step as completed.
        
        Args:
            step_number: Step number (0, 1, 2, ...)
            step_name: Name/description of the step
            output_files: List of output file paths (relative or absolute) created by this step
            duration_seconds: Optional duration in seconds for this step
        """
        checkpoint_data = self._load_checkpoint()
        
        step_key = f"step_{step_number}"
        if step_number not in checkpoint_data["completed_steps"]:
            checkpoint_data["completed_steps"].append(step_number)
            checkpoint_data["completed_steps"].sort()
        
        step_output = {
            "step_number": step_number,
            "step_name": step_name,
            "completed_at": datetime.now().isoformat(),
            "output_files": output_files or []
        }
        if duration_seconds is not None:
            step_output["duration_seconds"] = duration_seconds
        
        checkpoint_data["step_outputs"][step_key] = step_output

        run_id = checkpoint_data.get("metadata", {}).get("run_id")
        self._append_event(
            checkpoint_data=checkpoint_data,
            event_type="step_completed",
            run_id=run_id,
            status="completed",
            step_number=step_number,
            step_name=step_name,
            source="checkpoint",
            details={
                "duration_seconds": duration_seconds,
                "output_count": len(output_files or []),
            },
        )
        
        self._save_checkpoint()
        if duration_seconds is not None:
            log.info(f"[CHECKPOINT] Marked step {step_number} ({step_name}) as complete (duration: {duration_seconds:.2f}s)")
        else:
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
        self._checkpoint_data = self._default_checkpoint_data()
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

    def get_metadata(self) -> Dict:
        """Get checkpoint metadata."""
        checkpoint_data = self._load_checkpoint()
        return dict(checkpoint_data.get("metadata", {}))

    def update_metadata(self, updates: Dict = None, replace: bool = False, **kwargs) -> None:
        """
        Update checkpoint metadata.

        Args:
            updates: Metadata updates to apply.
            replace: If True, replaces metadata entirely.
        """
        checkpoint_data = self._load_checkpoint()
        merged_updates = {}
        if updates:
            merged_updates.update(updates)
        if kwargs:
            merged_updates.update(kwargs)

        previous = dict(checkpoint_data.get("metadata", {}))
        if replace:
            checkpoint_data["metadata"] = dict(merged_updates)
        else:
            checkpoint_data.setdefault("metadata", {})
            checkpoint_data["metadata"].update(merged_updates)

        current = checkpoint_data.get("metadata", {})

        if current.get("run_id") and current.get("run_id") != previous.get("run_id"):
            self._append_event(
                checkpoint_data=checkpoint_data,
                event_type="run_id_set",
                run_id=current.get("run_id"),
                source="metadata",
                details={
                    "previous_run_id": previous.get("run_id"),
                },
            )

        status_old = previous.get("status")
        status_new = current.get("status")
        if status_new and status_new != status_old:
            self._append_event(
                checkpoint_data=checkpoint_data,
                event_type="status_changed",
                run_id=current.get("run_id"),
                status=status_new,
                step_number=current.get("current_step"),
                step_name=current.get("current_step_name"),
                source="metadata",
                details={"from": status_old, "to": status_new},
            )

        step_old = (previous.get("current_step"), previous.get("current_step_name"))
        step_new = (current.get("current_step"), current.get("current_step_name"))
        if step_new != step_old and (step_new[0] is not None or step_new[1]):
            self._append_event(
                checkpoint_data=checkpoint_data,
                event_type="step_pointer_changed",
                run_id=current.get("run_id"),
                status=current.get("status"),
                step_number=current.get("current_step"),
                step_name=current.get("current_step_name"),
                source="metadata",
                details={
                    "previous_step_number": step_old[0],
                    "previous_step_name": step_old[1],
                },
            )
        self._save_checkpoint()

    def get_events(self, limit: int = 200, run_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Return latest pipeline events from checkpoint timeline.
        """
        checkpoint_data = self._load_checkpoint()
        events = checkpoint_data.get("events", []) or []
        if run_id:
            events = [e for e in events if e.get("run_id") == run_id]
        if limit <= 0:
            return []
        return events[-limit:]
    
    def get_pipeline_timing(self) -> Dict:
        """
        Get timing information for the pipeline.
        
        Returns:
            Dict with total duration and step durations:
            {
                "total_duration_seconds": float,
                "step_durations": {step_num: duration_seconds, ...},
                "pipeline_started_at": str (ISO format),
                "pipeline_completed_at": str (ISO format)
            }
        """
        checkpoint_data = self._load_checkpoint()
        step_outputs = checkpoint_data.get("step_outputs", {})
        
        step_durations = {}
        total_duration = 0.0
        pipeline_started_at = None
        pipeline_completed_at = checkpoint_data.get("last_run")
        
        for step_key, step_info in step_outputs.items():
            step_num = step_info.get("step_number")
            duration = step_info.get("duration_seconds")
            completed_at = step_info.get("completed_at")
            
            if duration is not None:
                step_durations[step_num] = duration
                total_duration += duration
            
            # Track earliest completion as start (approximation)
            if completed_at:
                if pipeline_started_at is None or completed_at < pipeline_started_at:
                    # Approximate start time by subtracting duration
                    if duration is not None:
                        try:
                            from datetime import datetime, timedelta
                            completed_dt = datetime.fromisoformat(completed_at.replace('Z', '+00:00'))
                            started_dt = completed_dt - timedelta(seconds=duration)
                            pipeline_started_at = started_dt.isoformat()
                        except:
                            pass
        
        return {
            "total_duration_seconds": total_duration,
            "step_durations": step_durations,
            "pipeline_started_at": pipeline_started_at,
            "pipeline_completed_at": pipeline_completed_at
        }
    
    def verify_output_files(self, step_number: int, expected_output_files: Optional[List[str]] = None) -> bool:
        """
        Verify that output files for a step still exist.
        
        Args:
            step_number: Step number to check
            expected_output_files: Optional list of expected output files for this step.
                                   If provided, these files will be checked for existence.
                                   If None, it will check files recorded in the checkpoint.
                                   If empty list, it means no output files are expected.
            
        Returns:
            True if all output files exist (or no files to verify), False otherwise
        """
        checkpoint_data = self._load_checkpoint()
        
        # If step is not marked as complete, it shouldn't be skipped
        if step_number not in checkpoint_data["completed_steps"]:
            return False
        
        step_key = f"step_{step_number}"
        
        # Determine which output files to check
        files_to_check = []
        if expected_output_files is not None:
            # If expected files are explicitly provided, use them (even if empty list)
            files_to_check = expected_output_files
        elif step_key in checkpoint_data["step_outputs"]:
            # Otherwise, use files recorded in the checkpoint
            files_to_check = checkpoint_data["step_outputs"][step_key].get("output_files", [])
        
        # If expected_output_files was explicitly provided as empty list, that means no files expected
        if expected_output_files is not None and len(expected_output_files) == 0:
            return True
        
        # If no files to check and expected_output_files was not explicitly provided, assume valid
        if not files_to_check and expected_output_files is None:
            # No output files recorded, assume step is valid if marked complete
            # This handles cases where steps are manually marked via manage checkpoint without output files
            return True
        
        # If expected_output_files was provided but is empty, or files_to_check is empty, 
        # and we're here, it means we should check but there are no files - this is an error case
        if not files_to_check:
            # This shouldn't happen, but if it does, assume valid
            return True
        
        # Check if all output files exist
        for file_path in files_to_check:
            file_obj = Path(file_path)
            if not file_obj.exists():
                log.warning(f"[CHECKPOINT] Output file missing for step {step_number}: {file_path}")
                return False
        
        return True
    
    def should_skip_step(self, step_number: int, step_name: str, verify_outputs: bool = True, expected_output_files: List[str] = None) -> bool:
        """
        Determine if a step should be skipped (already completed).
        
        Args:
            step_number: Step number to check
            step_name: Step name (for logging)
            verify_outputs: If True, also verify output files exist
            expected_output_files: List of expected output file paths (relative or absolute).
                                  If provided and step has no recorded output files, these will be checked.
            
        Returns:
            True if step should be skipped, False if it should run
        """
        if not self.is_step_complete(step_number):
            return False
        
        if verify_outputs:
            # Check output files - use expected_output_files if provided, otherwise check recorded files
            if not self.verify_output_files(step_number, expected_output_files):
                log.warning(f"[CHECKPOINT] Step {step_number} ({step_name}) marked complete but expected output files missing. Will re-run.")
                return False
        
        log.info(f"[CHECKPOINT] Step {step_number} ({step_name}) already completed. Skipping.")
        return True


    def get_status(self) -> str:
        """
        Get the current pipeline status from metadata.

        Returns:
            Status string: 'running', 'completed', 'resume', 'stopped', or 'idle'
        """
        metadata = self.get_metadata()
        return metadata.get("status", "idle")

    def set_status(self, status: str) -> None:
        """
        Set the pipeline status in metadata.

        Args:
            status: One of 'running', 'completed', 'resume', 'stopped', 'idle'
        """
        self.update_metadata({"status": status})
        log.info(f"[CHECKPOINT] Set status to '{status}' for {self.scraper_name}")

    def mark_as_resumable(self) -> None:
        """Mark this checkpoint as resumable (for crash recovery or stop)."""
        self.set_status("resume")

    def mark_as_stopped(self) -> None:
        """Mark this checkpoint as stopped (cannot be resumed)."""
        self.set_status("stopped")

    def mark_as_running(self, step_num: int = None, step_name: str = None) -> None:
        """Mark this checkpoint as running."""
        updates = {"status": "running"}
        if step_num is not None:
            updates["current_step"] = step_num
        if step_name is not None:
            updates["current_step_name"] = step_name
        self.update_metadata(updates)

    def mark_as_completed(self) -> None:
        """Mark this checkpoint as completed."""
        self.set_status("completed")

    def is_resumable(self) -> bool:
        """Check if this checkpoint can be resumed."""
        status = self.get_status()
        # Can resume if status is 'resume' OR 'running' (crash recovery)
        return status in ("resume", "running")

    def recover_if_stale(self) -> bool:
        """
        Check if checkpoint was left in 'running' state and recover it.

        Called on app startup to handle crash scenarios.
        If status is 'running', marks it as 'resume' so it can be resumed.

        Returns:
            True if recovery was performed, False otherwise
        """
        status = self.get_status()
        if status == "running":
            log.warning(f"[CHECKPOINT] Found stale 'running' checkpoint for {self.scraper_name}. Marking as 'resume'.")
            self.mark_as_resumable()
            return True
        return False


def get_checkpoint_manager(scraper_name: str, checkpoint_dir: Optional[Path] = None) -> PipelineCheckpoint:
    """Get or create a checkpoint manager for a scraper."""
    return PipelineCheckpoint(scraper_name, checkpoint_dir)


def recover_all_stale_checkpoints(scraper_names: List[str] = None) -> Dict[str, bool]:
    """
    Recover all stale checkpoints on app startup.

    Args:
        scraper_names: List of scraper names to check. If None, checks common scrapers.

    Returns:
        Dict mapping scraper name to whether recovery was performed
    """
    if scraper_names is None:
        # Default list of scrapers (include Russia for checkpoint recovery)
        scraper_names = [
            "India", "Malaysia", "Argentina", "Belarus", "Russia", "CanadaOntario", "CanadaQuebec",
            "Chile", "Korea", "NewZealand", "SouthAfrica", "Thailand"
        ]

    results = {}
    for scraper_name in scraper_names:
        try:
            cp = get_checkpoint_manager(scraper_name)
            results[scraper_name] = cp.recover_if_stale()
        except Exception as e:
            log.warning(f"[CHECKPOINT] Failed to recover checkpoint for {scraper_name}: {e}")
            results[scraper_name] = False

    return results
