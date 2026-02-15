#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rich Progress Module

Enhanced progress display using the rich library.
Provides beautiful terminal output WITHOUT changing scraping logic.

Usage:
    from core.progress.rich_progress import (
        create_progress, console, print_status, print_table,
        ProgressContext, ScraperProgress
    )
    
    # Simple progress bar
    with create_progress() as progress:
        task = progress.add_task("Processing...", total=100)
        for i in range(100):
            progress.update(task, advance=1)
    
    # Scraper-specific progress
    with ScraperProgress("Malaysia") as sp:
        sp.start_step("Fetching products", total=500)
        for product in products:
            sp.advance()
"""

import logging
import sys
from typing import Optional, Any, Dict, List, Callable
from contextlib import contextmanager
from datetime import datetime
import time

logger = logging.getLogger(__name__)

# Try to import rich, gracefully degrade if not available
try:
    from rich.console import Console
    from rich.progress import (
        Progress,
        SpinnerColumn,
        BarColumn,
        TextColumn,
        TimeElapsedColumn,
        TimeRemainingColumn,
        MofNCompleteColumn,
        TaskProgressColumn,
    )
    from rich.table import Table
    from rich.panel import Panel
    from rich.live import Live
    from rich.layout import Layout
    from rich.text import Text
    from rich.style import Style
    from rich import box
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    Console = None
    Progress = None
    Table = None
    Panel = None


class FallbackConsole:
    """Fallback console when rich is not available."""
    
    def print(self, *args, **kwargs):
        """Print to stdout."""
        # Remove rich-specific kwargs
        kwargs.pop('style', None)
        kwargs.pop('highlight', None)
        kwargs.pop('markup', None)
        print(*args, **kwargs)
    
    def log(self, *args, **kwargs):
        """Log message."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        kwargs.pop('style', None)
        print(f"[{timestamp}]", *args, **kwargs)
    
    def status(self, message: str, **kwargs):
        """Show status (returns context manager)."""
        return FallbackStatus(message)
    
    def rule(self, title: str = "", **kwargs):
        """Print a horizontal rule."""
        if title:
            print(f"\n{'='*20} {title} {'='*20}\n")
        else:
            print("=" * 60)


class FallbackStatus:
    """Fallback status context manager."""
    
    def __init__(self, message: str):
        self.message = message
    
    def __enter__(self):
        print(f"⏳ {self.message}...")
        return self
    
    def __exit__(self, *args):
        print(f"✓ {self.message} done")
    
    def update(self, message: str):
        print(f"  → {message}")


class FallbackProgress:
    """Fallback progress bar when rich is not available."""
    
    def __init__(self):
        self.tasks: Dict[int, Dict] = {}
        self._task_counter = 0
    
    def add_task(self, description: str, total: Optional[int] = None, **kwargs) -> int:
        """Add a new task."""
        task_id = self._task_counter
        self._task_counter += 1
        self.tasks[task_id] = {
            "description": description,
            "total": total or 100,
            "completed": 0,
            "start_time": time.time(),
        }
        return task_id
    
    def update(self, task_id: int, advance: int = 0, completed: int = None, **kwargs):
        """Update task progress."""
        if task_id not in self.tasks:
            return
        
        task = self.tasks[task_id]
        if completed is not None:
            task["completed"] = completed
        else:
            task["completed"] += advance
        
        # Print progress
        pct = (task["completed"] / task["total"] * 100) if task["total"] > 0 else 0
        bar_width = 30
        filled = int(bar_width * task["completed"] / task["total"]) if task["total"] > 0 else 0
        bar = "█" * filled + "░" * (bar_width - filled)
        
        print(f"\r{task['description']}: [{bar}] {task['completed']}/{task['total']} ({pct:.1f}%)", 
              end="", flush=True)
        
        if task["completed"] >= task["total"]:
            print()  # Newline when complete
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        pass
    
    def start(self):
        pass
    
    def stop(self):
        pass


# Global console instance
console = Console() if RICH_AVAILABLE else FallbackConsole()


def create_progress(**kwargs) -> Any:
    """
    Create a progress bar instance.
    
    Returns:
        Progress instance (rich or fallback)
    
    Usage:
        with create_progress() as progress:
            task = progress.add_task("Processing", total=100)
            for i in range(100):
                progress.update(task, advance=1)
    """
    if RICH_AVAILABLE:
        return Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
            **kwargs
        )
    else:
        return FallbackProgress()


def print_status(message: str, status: str = "info", **kwargs):
    """
    Print a status message with appropriate styling.
    
    Args:
        message: Message to print
        status: Status type - "info", "success", "warning", "error"
    """
    if RICH_AVAILABLE:
        styles = {
            "info": "blue",
            "success": "green",
            "warning": "yellow",
            "error": "red bold",
        }
        icons = {
            "info": "ℹ",
            "success": "✓",
            "warning": "⚠",
            "error": "✗",
        }
        style = styles.get(status, "white")
        icon = icons.get(status, "•")
        console.print(f"[{style}]{icon} {message}[/{style}]", **kwargs)
    else:
        icons = {
            "info": "[INFO]",
            "success": "[OK]",
            "warning": "[WARN]",
            "error": "[ERROR]",
        }
        icon = icons.get(status, "[INFO]")
        print(f"{icon} {message}")


def print_table(
    data: List[Dict],
    title: str = "",
    columns: Optional[List[str]] = None,
    max_rows: int = 50,
):
    """
    Print data as a formatted table.
    
    Args:
        data: List of dictionaries
        title: Table title
        columns: Column names (auto-detect if None)
        max_rows: Maximum rows to display
    """
    if not data:
        print("No data to display")
        return
    
    # Auto-detect columns
    if columns is None:
        columns = list(data[0].keys())
    
    if RICH_AVAILABLE:
        table = Table(title=title, box=box.ROUNDED)
        
        for col in columns:
            table.add_column(col, style="cyan")
        
        for i, row in enumerate(data[:max_rows]):
            table.add_row(*[str(row.get(col, "")) for col in columns])
        
        if len(data) > max_rows:
            table.add_row(*["..." for _ in columns])
        
        console.print(table)
    else:
        # Simple text table
        if title:
            print(f"\n{title}")
            print("-" * len(title))
        
        # Print headers
        header = " | ".join(f"{col:15}" for col in columns)
        print(header)
        print("-" * len(header))
        
        # Print rows
        for i, row in enumerate(data[:max_rows]):
            row_str = " | ".join(f"{str(row.get(col, '')):15}" for col in columns)
            print(row_str)
        
        if len(data) > max_rows:
            print(f"... and {len(data) - max_rows} more rows")


def print_panel(content: str, title: str = "", style: str = "blue"):
    """Print content in a panel."""
    if RICH_AVAILABLE:
        console.print(Panel(content, title=title, style=style))
    else:
        if title:
            print(f"\n╔{'═' * (len(title) + 2)}╗")
            print(f"║ {title} ║")
            print(f"╚{'═' * (len(title) + 2)}╝")
        print(content)


class ScraperProgress:
    """
    Scraper-specific progress tracker.
    
    Provides a unified interface for tracking scraper progress.
    
    Usage:
        with ScraperProgress("Malaysia") as sp:
            sp.start_step("Fetching products", total=500)
            for product in products:
                sp.advance()
                sp.log(f"Processing {product['name']}")
    """
    
    def __init__(self, scraper_name: str, show_spinner: bool = True):
        """
        Initialize scraper progress.
        
        Args:
            scraper_name: Name of the scraper
            show_spinner: Show spinner for indeterminate tasks
        """
        self.scraper_name = scraper_name
        self.show_spinner = show_spinner
        self._progress = None
        self._current_task = None
        self._step_count = 0
        self._start_time = None
        self._step_times: List[float] = []
    
    def __enter__(self):
        self._start_time = time.time()
        self._progress = create_progress()
        if hasattr(self._progress, 'start'):
            self._progress.start()
        
        if RICH_AVAILABLE:
            console.rule(f"[bold blue]{self.scraper_name} Scraper")
        else:
            print(f"\n{'='*20} {self.scraper_name} Scraper {'='*20}\n")
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self._progress, 'stop'):
            self._progress.stop()
        
        elapsed = time.time() - self._start_time if self._start_time else 0
        
        if exc_type is None:
            print_status(
                f"{self.scraper_name} completed in {elapsed:.1f}s",
                "success"
            )
        else:
            print_status(
                f"{self.scraper_name} failed after {elapsed:.1f}s: {exc_val}",
                "error"
            )
        
        return False
    
    def start_step(self, description: str, total: Optional[int] = None):
        """
        Start a new step with progress tracking.
        
        Args:
            description: Step description
            total: Total items (None for indeterminate)
        """
        self._step_count += 1
        step_desc = f"Step {self._step_count}: {description}"
        
        if self._progress:
            self._current_task = self._progress.add_task(step_desc, total=total or 0)
        
        self._step_start_time = time.time()
    
    def advance(self, amount: int = 1):
        """Advance the current step progress."""
        if self._progress and self._current_task is not None:
            self._progress.update(self._current_task, advance=amount)
    
    def update(self, completed: int = None, total: int = None, description: str = None):
        """Update the current step."""
        if self._progress and self._current_task is not None:
            kwargs = {}
            if completed is not None:
                kwargs['completed'] = completed
            if total is not None:
                kwargs['total'] = total
            if description is not None:
                kwargs['description'] = description
            self._progress.update(self._current_task, **kwargs)
    
    def complete_step(self, message: str = None):
        """Mark the current step as complete."""
        if self._progress and self._current_task is not None:
            task = self._progress.tasks[self._current_task] if RICH_AVAILABLE else None
            if task:
                self._progress.update(self._current_task, completed=task.total)
        
        step_time = time.time() - getattr(self, '_step_start_time', time.time())
        self._step_times.append(step_time)
        
        if message:
            print_status(message, "success")
    
    def log(self, message: str, level: str = "info"):
        """Log a message during progress."""
        if RICH_AVAILABLE:
            console.log(message)
        else:
            print(f"  {message}")
    
    def warn(self, message: str):
        """Log a warning."""
        print_status(message, "warning")
    
    def error(self, message: str):
        """Log an error."""
        print_status(message, "error")


class MultiScraperProgress:
    """
    Progress tracker for multiple scrapers running in parallel.
    
    Usage:
        with MultiScraperProgress(["Malaysia", "Argentina", "India"]) as mp:
            mp.update("Malaysia", completed=50, total=100)
            mp.update("Argentina", completed=30, total=80)
    """
    
    def __init__(self, scraper_names: List[str]):
        self.scraper_names = scraper_names
        self._progress = None
        self._tasks: Dict[str, int] = {}
    
    def __enter__(self):
        self._progress = create_progress()
        if hasattr(self._progress, 'start'):
            self._progress.start()
        
        for name in self.scraper_names:
            self._tasks[name] = self._progress.add_task(f"[cyan]{name}", total=100)
        
        return self
    
    def __exit__(self, *args):
        if hasattr(self._progress, 'stop'):
            self._progress.stop()
    
    def update(self, scraper_name: str, completed: int = None, total: int = None, **kwargs):
        """Update progress for a specific scraper."""
        if scraper_name in self._tasks and self._progress:
            update_kwargs = {}
            if completed is not None:
                update_kwargs['completed'] = completed
            if total is not None:
                update_kwargs['total'] = total
            update_kwargs.update(kwargs)
            self._progress.update(self._tasks[scraper_name], **update_kwargs)
    
    def advance(self, scraper_name: str, amount: int = 1):
        """Advance progress for a specific scraper."""
        if scraper_name in self._tasks and self._progress:
            self._progress.update(self._tasks[scraper_name], advance=amount)


@contextmanager
def progress_context(description: str, total: Optional[int] = None):
    """
    Simple context manager for progress tracking.
    
    Usage:
        with progress_context("Processing items", total=100) as update:
            for item in items:
                process(item)
                update()  # or update(advance=5)
    """
    progress = create_progress()
    
    if hasattr(progress, 'start'):
        progress.start()
    
    task_id = progress.add_task(description, total=total or 0)
    
    def update_func(advance: int = 1, **kwargs):
        progress.update(task_id, advance=advance, **kwargs)
    
    try:
        yield update_func
    finally:
        if hasattr(progress, 'stop'):
            progress.stop()


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def print_summary(
    scraper_name: str,
    records_processed: int,
    duration_seconds: float,
    errors: int = 0,
    warnings: int = 0,
):
    """Print a summary of scraper execution."""
    if RICH_AVAILABLE:
        table = Table(title=f"{scraper_name} Summary", box=box.ROUNDED)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Records Processed", f"{records_processed:,}")
        table.add_row("Duration", format_duration(duration_seconds))
        table.add_row("Rate", f"{records_processed/duration_seconds:.1f}/sec" if duration_seconds > 0 else "N/A")
        
        if errors > 0:
            table.add_row("Errors", f"[red]{errors}[/red]")
        else:
            table.add_row("Errors", "0")
        
        if warnings > 0:
            table.add_row("Warnings", f"[yellow]{warnings}[/yellow]")
        else:
            table.add_row("Warnings", "0")
        
        console.print(table)
    else:
        print(f"\n{'='*40}")
        print(f"{scraper_name} Summary")
        print(f"{'='*40}")
        print(f"Records Processed: {records_processed:,}")
        print(f"Duration: {format_duration(duration_seconds)}")
        print(f"Rate: {records_processed/duration_seconds:.1f}/sec" if duration_seconds > 0 else "Rate: N/A")
        print(f"Errors: {errors}")
        print(f"Warnings: {warnings}")
        print(f"{'='*40}\n")


# Check availability
def is_rich_available() -> bool:
    """Check if rich is available."""
    return RICH_AVAILABLE


if __name__ == "__main__":
    # Demo
    print(f"Rich available: {RICH_AVAILABLE}\n")
    
    # Test basic progress
    print("Testing basic progress bar:")
    with create_progress() as progress:
        task = progress.add_task("Processing items...", total=50)
        for i in range(50):
            time.sleep(0.02)
            progress.update(task, advance=1)
    
    # Test status messages
    print("\nTesting status messages:")
    print_status("This is an info message", "info")
    print_status("This is a success message", "success")
    print_status("This is a warning message", "warning")
    print_status("This is an error message", "error")
    
    # Test table
    print("\nTesting table:")
    sample_data = [
        {"Name": "Product A", "Price": 10.99, "Stock": 100},
        {"Name": "Product B", "Price": 25.50, "Stock": 50},
        {"Name": "Product C", "Price": 5.00, "Stock": 200},
    ]
    print_table(sample_data, title="Sample Products")
    
    # Test scraper progress
    print("\nTesting ScraperProgress:")
    with ScraperProgress("TestScraper") as sp:
        sp.start_step("Fetching data", total=20)
        for i in range(20):
            time.sleep(0.05)
            sp.advance()
        sp.complete_step("Data fetched successfully")
        
        sp.start_step("Processing data", total=10)
        for i in range(10):
            time.sleep(0.05)
            sp.advance()
        sp.complete_step("Data processed")
    
    # Test summary
    print_summary("TestScraper", records_processed=1000, duration_seconds=45.5, errors=2, warnings=5)
