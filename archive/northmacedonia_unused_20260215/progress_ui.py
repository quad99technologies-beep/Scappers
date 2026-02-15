#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Progress UI Module for North Macedonia Scraper

Provides rich progress bars, checkpoint visualization, and status panels.
Integrates with core.rich_progress and core.progress_tracker.
"""

import sys
from pathlib import Path
from typing import Optional, Dict, List, Any
from datetime import datetime

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.rich_progress import (
    create_progress, console, print_status, print_table,
    ScraperProgress, ProgressContext
)
from core.progress_tracker import StandardProgress


class NorthMacedoniaProgressUI:
    """Rich progress UI for North Macedonia scraper with checkpoint visualization."""
    
    def __init__(self, run_id: str, total_steps: int = 4):
        self.run_id = run_id
        self.total_steps = total_steps
        self.current_step = 0
        self.step_names = {
            0: "Backup & Clean",
            1: "Collect URLs",
            2: "Drug Register Data",
            3: "Max Prices",
        }
        self.checkpoint_data = {}
        
    def show_pipeline_start(self):
        """Display pipeline start banner."""
        console.rule(f"[bold blue]North Macedonia Scraper - Run: {self.run_id}")
        console.print(f"[dim]Started at: {datetime.now().isoformat()}[/dim]")
        console.print()
        
    def show_step_header(self, step_num: int, step_name: str):
        """Display step header with progress."""
        self.current_step = step_num
        pct = (step_num / self.total_steps) * 100
        console.rule(
            f"[bold green]Step {step_num + 1}/{self.total_steps}[/bold green] "
            f"({pct:.0f}%) - [cyan]{step_name}[/cyan]"
        )
        
    def create_step_progress(self, task_name: str, total: int) -> ScraperProgress:
        """Create a progress context for a step."""
        return ScraperProgress(f"nm_{task_name}")
        
    def show_checkpoint_status(self, checkpoint_info: Dict[str, Any]):
        """Display checkpoint status table."""
        from rich.table import Table
        from rich.panel import Panel
        
        table = Table(title="Checkpoint Status", show_header=True)
        table.add_column("Step", style="cyan")
        table.add_column("Name", style="green")
        table.add_column("Status", style="bold")
        table.add_column("Duration", style="dim")
        table.add_column("Outputs", style="blue")
        
        completed = checkpoint_info.get("completed_steps", [])
        for i, name in self.step_names.items():
            status = "[green]✓ Complete[/green]" if i in completed else "[dim]○ Pending[/dim]"
            duration = ""
            outputs = ""
            if i in completed:
                step_data = checkpoint_info.get("step_data", {}).get(str(i), {})
                duration_sec = step_data.get("duration_seconds", 0)
                if duration_sec:
                    duration = f"{duration_sec:.1f}s"
                output_files = step_data.get("output_files", [])
                if output_files:
                    outputs = f"{len(output_files)} files"
                    
            table.add_row(
                str(i + 1),
                name,
                status,
                duration,
                outputs
            )
            
        console.print(Panel(table, border_style="blue"))
        console.print()
        
    def show_resume_info(self, last_completed: int, next_step: int):
        """Display resume information."""
        from rich.panel import Panel
        
        if last_completed >= 0:
            msg = (
                f"[yellow]Resuming from Step {next_step + 1}[/yellow]\n"
                f"Last completed: Step {last_completed + 1} - {self.step_names.get(last_completed, 'Unknown')}"
            )
        else:
            msg = "[green]Starting fresh run[/green]"
            
        console.print(Panel(msg, title="Resume Status", border_style="yellow"))
        console.print()
        
    def show_completion_summary(self, timing_info: Dict[str, Any]):
        """Display pipeline completion summary."""
        from rich.table import Table
        from rich.panel import Panel
        
        total_duration = timing_info.get("total_duration_seconds", 0)
        hours = int(total_duration // 3600)
        minutes = int((total_duration % 3600) // 60)
        seconds = int(total_duration % 60)
        
        if hours > 0:
            duration_str = f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            duration_str = f"{minutes}m {seconds}s"
        else:
            duration_str = f"{seconds}s"
            
        table = Table(show_header=False)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Total Duration", duration_str)
        table.add_row("Steps Completed", str(self.total_steps))
        table.add_row("Run ID", self.run_id)
        table.add_row("Completed At", datetime.now().isoformat())
        
        console.print()
        console.print(Panel(table, title="[bold green]Pipeline Completed Successfully", border_style="green"))
        console.print()
        
    def show_db_stats(self, stats: Dict[str, int]):
        """Display database statistics."""
        from rich.table import Table
        from rich.panel import Panel
        
        table = Table(title="Database Statistics", show_header=True)
        table.add_column("Table", style="cyan")
        table.add_column("Count", style="green", justify="right")
        
        for table_name, count in stats.items():
            table.add_row(table_name, str(count))
            
        console.print(Panel(table, border_style="blue"))
        console.print()


class NorthMacedoniaCheckpointVisualizer:
    """Visualize checkpoint and resume state for North Macedonia."""
    
    STEP_ICONS = {
        0: "🧹",  # Backup & Clean
        1: "🔗",  # Collect URLs
        2: "💊",  # Drug Register
        3: "💰",  # Max Prices
    }
    
    def __init__(self, checkpoint_manager):
        self.cp = checkpoint_manager
        
    def render_checkpoint_flow(self):
        """Render a visual flow of checkpoint steps."""
        from rich.console import Group
        from rich.panel import Panel
        from rich.text import Text
        
        info = self.cp.get_checkpoint_info()
        completed = set(info.get("completed_steps", []))
        current = info.get("next_step", 0)
        
        steps_text = Text()
        
        for i in range(4):
            icon = self.STEP_ICONS.get(i, "⚪")
            name = ["Backup", "URLs", "Drugs", "Prices"][i]
            
            if i in completed:
                steps_text.append(f"{icon} {name} ", style="green")
                if i < 3:
                    steps_text.append("→ ", style="dim")
            elif i == current:
                steps_text.append(f"{icon} {name} ", style="bold yellow")
                if i < 3:
                    steps_text.append("→ ", style="dim")
            else:
                steps_text.append(f"○ {name} ", style="dim")
                if i < 3:
                    steps_text.append("→ ", style="dim")
                    
        console.print(Panel(steps_text, title="Pipeline Flow", border_style="blue"))
        console.print()
        
    def render_step_detail(self, step_num: int):
        """Render detailed information about a step."""
        from rich.table import Table
        from rich.panel import Panel
        
        step_data = self.cp.get_step_data(step_num)
        if not step_data:
            console.print(f"[dim]No data for step {step_num}[/dim]")
            return
            
        table = Table(show_header=False)
        table.add_column("Field", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Step Number", str(step_data.get("step_number", "N/A")))
        table.add_row("Step Name", step_data.get("step_name", "N/A"))
        table.add_row("Completed At", step_data.get("completed_at", "N/A"))
        
        duration = step_data.get("duration_seconds", 0)
        if duration:
            table.add_row("Duration", f"{duration:.1f}s")
            
        output_files = step_data.get("output_files", [])
        if output_files:
            table.add_row("Output Files", str(len(output_files)))
            
        console.print(Panel(table, title=f"Step {step_num} Details", border_style="green"))


def create_standard_progress(task_id: str, total: int, run_dir: Path = None) -> StandardProgress:
    """Create a StandardProgress tracker for North Macedonia."""
    state_path = None
    if run_dir:
        state_path = run_dir / "logs" / f"progress_{task_id}.json"
        
    return StandardProgress(
        task_id=task_id,
        total=total,
        unit="items",
        state_path=state_path,
        log_every=10
    )


# Convenience functions for quick progress display
def show_progress_bar(current: int, total: int, description: str = "Processing"):
    """Show a simple progress bar."""
    pct = (current / total * 100) if total else 0
    bar_width = 40
    filled = int(bar_width * current / total) if total else 0
    bar = "█" * filled + "░" * (bar_width - filled)
    console.print(f"\r{description}: [{bar}] {current}/{total} ({pct:.1f}%)", end="")
    if current >= total:
        console.print()  # New line when complete


def log_step_progress(step_num: int, step_name: str, status: str, message: str = ""):
    """Log step progress in standardized format."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    status_emoji = {
        "started": "▶️",
        "completed": "✅",
        "failed": "❌",
        "skipped": "⏭️",
        "retry": "🔄",
    }.get(status, "➡️")
    
    console.print(
        f"[{timestamp}] {status_emoji} Step {step_num}: {step_name} - {status.upper()}"
        f"{f' ({message})' if message else ''}"
    )


if __name__ == "__main__":
    # Demo the progress UI
    ui = NorthMacedoniaProgressUI("demo_run_001")
    ui.show_pipeline_start()
    ui.show_resume_info(-1, 0)
    
    # Simulate checkpoint status
    checkpoint_info = {
        "completed_steps": [0, 1],
        "step_data": {
            "0": {"duration_seconds": 5.2, "output_files": []},
            "1": {"duration_seconds": 15.8, "output_files": ["urls.csv"]},
        }
    }
    ui.show_checkpoint_status(checkpoint_info)
    
    console.print("[dim]Demo complete[/dim]")
