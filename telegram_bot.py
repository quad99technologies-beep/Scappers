#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram bot controller for the Scraper platform.

Commands:
  /help, /start - Show help message
  /whoami - Show your chat ID
  /ping - Health check
  /list - List available scrapers
  /status <scraper|all> - Check pipeline status with DB summary
  /run <scraper> [fresh] - Start pipeline if idle
  /runfresh <scraper> - Start a fresh pipeline
  /resume <scraper> - Resume pipeline
  /stop <scraper> - Stop running pipeline
  /clear <scraper> - Clear stale lock file
  /summary <scraper> - Show database table summary
"""

from __future__ import annotations

import os
import sys

# Fix Windows console encoding
if sys.platform == "win32":
    import codecs
    try:
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    except Exception:
        pass

import time
import threading
import subprocess
import re
import signal
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Tuple, List, Any
from contextlib import contextmanager

import requests
from dotenv import load_dotenv

# Add repo root to path for imports
REPO_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT))

from core.pipeline_start_lock import (
    claim_pipeline_start_lock,
    get_lock_paths as _shared_get_lock_paths,
    is_pid_running as _shared_is_pid_running,
    read_lock_info as _shared_read_lock_info,
    release_pipeline_lock,
    update_pipeline_lock,
)

# Database imports (PostgreSQL only)
try:
    from core.db.postgres_connection import PostgresDB, COUNTRY_PREFIX_MAP
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False
    PostgresDB = None
    COUNTRY_PREFIX_MAP = {}

try:
    from scripts.common.db import get_country_db, get_cursor, get_connection
    DB_UTILS_AVAILABLE = True
except ImportError:
    DB_UTILS_AVAILABLE = False

# Shared scraper registry (single source of truth for GUI, API, and Telegram bot)
try:
    from scripts.common.scraper_registry import (
        SCRAPER_CONFIGS,
        get_scraper_names,
        get_scraper_path,
        get_pipeline_script,
        resolve_country_name,
    )
    REGISTRY_AVAILABLE = True
except ImportError:
    REGISTRY_AVAILABLE = False
    SCRAPER_CONFIGS = {}

# =============================================================================
# Configuration
# =============================================================================

# Build SCRAPERS dict from registry (backward-compatible format)
if REGISTRY_AVAILABLE:
    SCRAPERS: Dict[str, Dict[str, Path]] = {
        name: {"path": REPO_ROOT / cfg["path"]}
        for name, cfg in SCRAPER_CONFIGS.items()
    }
else:
    # Fallback if registry import fails
    SCRAPERS: Dict[str, Dict[str, Path]] = {
        "canada_quebec": {"path": REPO_ROOT / "scripts" / "canada_quebec"},
        "Malaysia": {"path": REPO_ROOT / "scripts" / "Malaysia"},
        "Argentina": {"path": REPO_ROOT / "scripts" / "Argentina"},
        "CanadaOntario": {"path": REPO_ROOT / "scripts" / "canada_ontario"},
        "Netherlands": {"path": REPO_ROOT / "scripts" / "Netherlands"},
        "Belarus": {"path": REPO_ROOT / "scripts" / "Belarus"},
        "Russia": {"path": REPO_ROOT / "scripts" / "Russia"},
        "Taiwan": {"path": REPO_ROOT / "scripts" / "Taiwan"},
        "NorthMacedonia": {"path": REPO_ROOT / "scripts" / "north_macedonia"},
        "tender_chile": {"path": REPO_ROOT / "scripts" / "tender_chile"},
        "India": {"path": REPO_ROOT / "scripts" / "India"},
    }

# Status emojis for better UX
STATUS_EMOJIS = {
    "running": "🟢",
    "idle": "⚪",
    "stale": "🟡",
    "completed": "✅",
    "failed": "❌",
    "stopped": "🛑",
    "cancelled": "⛔",
    "resume": "⏸️",
    "partial": "⚠️",
}

# =============================================================================
# Helper Functions
# =============================================================================

def _normalize_name(value: str) -> str:
    return "".join(ch.lower() for ch in value if ch.isalnum())


SCRAPER_ALIASES = {_normalize_name(name): name for name in SCRAPERS.keys()}


def resolve_scraper_name(raw_name: Optional[str]) -> Optional[str]:
    if not raw_name:
        return None
    # Try registry resolver first (handles display names and aliases)
    if REGISTRY_AVAILABLE:
        result = resolve_country_name(raw_name)
        if result:
            return result
    # Fallback to local alias map
    key = _normalize_name(raw_name)
    return SCRAPER_ALIASES.get(key)


def _get_path_manager():
    try:
        from core.config_manager import ConfigManager
        return get_path_manager()
    except Exception:
        return None


def get_lock_paths(scraper_name: str) -> Tuple[Path, Path]:
    return _shared_get_lock_paths(scraper_name, repo_root=REPO_ROOT)


def read_lock_info(lock_file: Path) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    return _shared_read_lock_info(lock_file)


def is_pid_running(pid: Optional[int]) -> bool:
    return _shared_is_pid_running(pid)


def terminate_pid(pid: Optional[int]) -> bool:
    if not pid:
        return False
    try:
        import psutil
        proc = psutil.Process(pid)
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
        return True
    except Exception:
        if sys.platform == "win32":
            try:
                result = subprocess.run(
                    ["taskkill", "/PID", str(pid), "/T", "/F"],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                return result.returncode == 0
            except Exception:
                return False
        try:
            os.kill(pid, signal.SIGTERM)
            return True
        except Exception:
            return False


def ensure_lock_file(lock_file: Path, pid: int, log_path: Optional[Path] = None) -> None:
    update_pipeline_lock(lock_file, pid, log_path=log_path)


def remove_lock_file(lock_file: Path) -> None:
    release_pipeline_lock(lock_file)


def get_logs_dir() -> Path:
    pm = _get_path_manager()
    if pm:
        logs_dir = pm.get_logs_dir()
    else:
        logs_dir = REPO_ROOT / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    telegram_dir = logs_dir / "telegram"
    telegram_dir.mkdir(parents=True, exist_ok=True)
    return telegram_dir


def _safe_read_last_lines(path: Path, max_lines: int = 200) -> List[str]:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            lines = handle.readlines()
        if len(lines) > max_lines:
            return lines[-max_lines:]
        return lines
    except Exception:
        return []


def find_latest_log(scraper_name: str, status: Dict[str, Optional[str]]) -> Optional[Path]:
    if status.get("log_file"):
        log_path = Path(status["log_file"])
        if log_path.exists():
            return log_path

    candidates: List[Path] = []

    # Telegram bot logs (if bot started the pipeline)
    telegram_dir = get_logs_dir()
    candidates.extend(telegram_dir.glob(f"{scraper_name}_pipeline_*.log"))

    # Output logs from pipeline batch scripts
    try:
        from core.config_manager import ConfigManager
        # Migrated: get_path_manager() -> ConfigManager
        output_dir = ConfigManager.get_output_dir(scraper_name)
    except Exception:
        output_dir = REPO_ROOT / "output" / scraper_name
    if output_dir.exists():
        candidates.extend(output_dir.glob("*.log"))

    # Generic logs dir (fallback)
    generic_logs = REPO_ROOT / "logs"
    if generic_logs.exists():
        candidates.extend(generic_logs.glob("*.log"))

    # Scraper-local logs folder
    scraper_path = SCRAPERS.get(scraper_name, {}).get("path")
    if scraper_path:
        local_logs = scraper_path / "logs"
        if local_logs.exists():
            candidates.extend(local_logs.glob("*.log"))

    if not candidates:
        return None

    candidates = [p for p in candidates if p.exists()]
    if not candidates:
        return None

    return max(candidates, key=lambda p: p.stat().st_mtime)


def extract_latest_progress(log_path: Path) -> Optional[str]:
    lines = _safe_read_last_lines(log_path, max_lines=300)
    if not lines:
        return None

    progress_patterns = [
        r"\[PROGRESS\]\s+Pipeline\s+Step\s*:\s*(\d+)\s*/\s*(\d+)\s*\(([\d.]+)%\)\s*-\s*(.+)",
        r"\[PROGRESS\]\s+Pipeline\s+Step\s*:\s*(\d+)\s*/\s*(\d+)\s*\(([\d.]+)%\)",
        r"\[Step\s+(\d+)\s*/\s*(\d+)\]\s*(.+)",
        r"Step\s+(\d+)\s*/\s*(\d+)\s*:\s*(.+)",
    ]

    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        if "Pipeline completed" in line or "Execution completed" in line:
            return "✅ Pipeline completed"
        for pattern in progress_patterns:
            match = re.search(pattern, line)
            if not match:
                continue
            step = match.group(1)
            total = match.group(2)
            if len(match.groups()) >= 4:
                percent = match.group(3)
                desc = match.group(4).strip()
                return f"📍 Step {step}/{total} ({percent}%) - {desc}"
            if len(match.groups()) == 3:
                percent = match.group(3)
                return f"📍 Step {step}/{total} ({percent}%)"
            if len(match.groups()) == 2:
                return f"📍 Step {step}/{total}"
        if "Pipeline stopped" in line or "[STOPPED]" in line:
            return "🛑 Pipeline stopped"
    return None


def get_pipeline_status(scraper_name: str) -> Dict[str, Optional[str]]:
    new_lock, old_lock = get_lock_paths(scraper_name)
    lock_file = new_lock if new_lock.exists() else old_lock if old_lock.exists() else None
    if not lock_file:
        return {"state": "idle", "pid": None, "started": None, "lock_file": None, "log_file": None}

    pid, started, log_path = read_lock_info(lock_file)
    running = is_pid_running(pid)
    if running:
        return {
            "state": "running",
            "pid": str(pid) if pid else None,
            "started": started,
            "lock_file": str(lock_file),
            "log_file": log_path
        }
    return {
        "state": "stale",
        "pid": str(pid) if pid else None,
        "started": started,
        "lock_file": str(lock_file),
        "log_file": log_path
    }


# =============================================================================
# Database Summary Functions (PostgreSQL Only)
# =============================================================================

def get_db_summary(scraper_name: str) -> Optional[Dict[str, Any]]:
    """
    Get database summary for a scraper from PostgreSQL.
    Returns table counts and latest run info.
    """
    if not POSTGRES_AVAILABLE:
        return None
    
    try:
        db = PostgresDB(scraper_name)
        prefix = db.prefix
        
        summary = {
            "scraper_name": scraper_name,
            "prefix": prefix,
            "tables": {},
            "tables_all_runs": {},  # Total across all runs (for reference)
            "latest_run": None,
            "total_entities": 0,
        }
        
        with db.cursor() as cur:
            # First, get the latest run_id
            latest_run_id = None
            try:
                cur.execute("""
                    SELECT run_id FROM run_ledger 
                    WHERE scraper_name = %s 
                    ORDER BY started_at DESC 
                    LIMIT 1
                """, (scraper_name,))
                row = cur.fetchone()
                if row:
                    latest_run_id = row[0]
            except Exception:
                pass
            
            # Get all tables with this country's prefix
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE %s
                AND table_type = 'BASE TABLE'
            """, (f"{prefix}%",))
            
            tables = [row[0] for row in cur.fetchall()]
            
            # Check which tables have run_id column
            tables_with_run_id = set()
            for table in tables:
                try:
                    cur.execute("""
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = %s AND column_name = 'run_id'
                    """, (table,))
                    if cur.fetchone():
                        tables_with_run_id.add(table)
                except Exception:
                    pass
            
            # Get row counts for each table (filtered by latest run_id if applicable)
            for table in tables:
                try:
                    # Remove prefix for display
                    display_name = table.replace(prefix, "")
                    
                    # Get total count (all runs)
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    total_count = cur.fetchone()[0]
                    summary["tables_all_runs"][display_name] = total_count
                    
                    # Get count for latest run only (if table has run_id column)
                    if table in tables_with_run_id and latest_run_id:
                        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (latest_run_id,))
                        count = cur.fetchone()[0]
                    else:
                        count = total_count
                    
                    summary["tables"][display_name] = count
                    
                    # Track total entities (common entity tables) - use latest run count
                    if any(x in table for x in ['product', 'tender', 'drug', 'sku', 'entity']):
                        summary["total_entities"] += count
                except Exception:
                    pass
            
            # Store latest run_id in summary
            summary["latest_run_id"] = latest_run_id
            
            # Get latest run from run_ledger
            try:
                cur.execute("""
                    SELECT run_id, started_at, ended_at, status, 
                           items_scraped, items_exported, error_message
                    FROM run_ledger 
                    WHERE scraper_name = %s 
                    ORDER BY started_at DESC 
                    LIMIT 1
                """, (scraper_name,))
                
                row = cur.fetchone()
                if row:
                    summary["latest_run"] = {
                        "run_id": row[0],
                        "started_at": row[1],
                        "ended_at": row[2],
                        "status": row[3],
                        "items_scraped": row[4] or 0,
                        "items_exported": row[5] or 0,
                        "error_message": row[6],
                    }
            except Exception:
                pass
            
            # Also try pipeline_runs table
            try:
                cur.execute("""
                    SELECT run_id, created_at, started_at, ended_at, status,
                           current_step, current_step_num, total_steps, error_message
                    FROM pipeline_runs 
                    WHERE country = %s 
                    ORDER BY created_at DESC 
                    LIMIT 1
                """, (scraper_name,))
                
                row = cur.fetchone()
                if row:
                    summary["pipeline_run"] = {
                        "run_id": row[0],
                        "created_at": row[1],
                        "started_at": row[2],
                        "ended_at": row[3],
                        "status": row[4],
                        "current_step": row[5],
                        "current_step_num": row[6],
                        "total_steps": row[7],
                        "error_message": row[8],
                    }
            except Exception:
                pass
        
        db.close()
        return summary
        
    except Exception as e:
        return {"error": str(e)}


def format_db_summary(summary: Optional[Dict[str, Any]], human_readable: bool = True) -> str:
    """Format database summary for Telegram display.
    
    Args:
        summary: The database summary dict
        human_readable: If True, format numbers in human-readable format (K, M, B)
    """
    if summary is None:
        return "📊 <i>Database summary not available</i>"
    
    if "error" in summary:
        return f"❌ <i>DB Error: {summary['error']}</i>"
    
    def fmt_num(n: int) -> str:
        """Format number in human-readable format."""
        if not human_readable:
            return f"{n:,}"
        if n >= 1_000_000_000:
            return f"{n/1_000_000_000:.2f}B"
        elif n >= 1_000_000:
            return f"{n/1_000_000:.2f}M"
        elif n >= 1_000:
            return f"{n/1_000:.1f}K"
        return f"{n:,}"
    
    lines = ["📊 <b>Database Summary</b>"]
    
    # Add note about current run filtering
    latest_run_id = summary.get("latest_run_id")
    if latest_run_id:
        short_run_id = latest_run_id[:8] + "..." if len(latest_run_id) > 8 else latest_run_id
        lines.append(f"<i>(Filtered by latest run: {short_run_id})</i>")
    
    # Intelligent table categorization
    entity_tables = {}
    metadata_tables = {}
    other_tables = {}
    
    if summary.get("tables"):
        for name, count in summary["tables"].items():
            lower_name = name.lower()
            # Categorize tables
            if any(x in lower_name for x in ['product', 'tender', 'drug', 'sku', 'entity', 'item', 'record', 'data']):
                entity_tables[name] = count
            elif any(x in lower_name for x in ['log', 'run', 'ledger', 'meta', 'config', 'setting', 'session']):
                metadata_tables[name] = count
            else:
                other_tables[name] = count
    
    # Entity tables (main data)
    if entity_tables:
        lines.append("")
        lines.append("📦 <b>Data Tables (Current Run):</b>")
        for name, count in sorted(entity_tables.items(), key=lambda x: -x[1]):
            lines.append(f"  • {name}: <code>{fmt_num(count)}</code>")
    
    # Other tables
    if other_tables:
        lines.append("")
        lines.append("📋 <b>Other Tables:</b>")
        for name, count in sorted(other_tables.items(), key=lambda x: -x[1]):
            lines.append(f"  • {name}: <code>{fmt_num(count)}</code>")
    
    # Metadata tables (collapsed if too many)
    if metadata_tables:
        lines.append("")
        lines.append("⚙️ <b>System Tables:</b>")
        for name, count in sorted(metadata_tables.items(), key=lambda x: -x[1]):
            lines.append(f"  • {name}: <code>{fmt_num(count)}</code>")
    
    # Total entities
    if summary.get("total_entities"):
        lines.append(f"\n📊 <b>Total Records:</b> <code>{fmt_num(summary['total_entities'])}</code>")
    
    # Latest run info
    latest = summary.get("latest_run")
    if latest:
        lines.append("")
        lines.append("<b>Latest Run:</b>")
        status_emoji = STATUS_EMOJIS.get(latest.get("status", ""), "⚪")
        lines.append(f"  {status_emoji} Status: <code>{latest.get('status', 'N/A')}</code>")
        
        started = latest.get("started_at")
        if started:
            if isinstance(started, datetime):
                started = started.strftime("%Y-%m-%d %H:%M")
            lines.append(f"  🕐 Started: <code>{started}</code>")
        
        if latest.get("items_scraped"):
            lines.append(f"  📥 Scraped: <code>{fmt_num(latest['items_scraped'])}</code>")
        if latest.get("items_exported"):
            lines.append(f"  📤 Exported: <code>{fmt_num(latest['items_exported'])}</code>")
        
        if latest.get("error_message"):
            lines.append(f"  ⚠️ Error: <code>{latest['error_message'][:100]}</code>")
    
    # Pipeline run info (more detailed)
    pipeline = summary.get("pipeline_run")
    if pipeline:
        lines.append("")
        lines.append("<b>Pipeline Run:</b>")
        status_emoji = STATUS_EMOJIS.get(pipeline.get("status", ""), "⚪")
        lines.append(f"  {status_emoji} Status: <code>{pipeline.get('status', 'N/A')}</code>")
        
        current_step = pipeline.get("current_step_num") or 0
        total_steps = pipeline.get("total_steps") or 0
        if total_steps > 0:
            progress = (current_step / total_steps) * 100
            lines.append(f"  📍 Progress: <code>{current_step}/{total_steps} ({progress:.1f}%)</code>")
            
            # Progress bar
            filled = int(progress / 10)
            bar = "█" * filled + "░" * (10 - filled)
            lines.append(f"  <code>[{bar}]</code>")
        
        if pipeline.get("current_step"):
            lines.append(f"  📝 Step: <code>{pipeline['current_step']}</code>")
    
    return "\n".join(lines)


def get_all_scrapers_summary(human_readable: bool = True) -> str:
    """Get a summary of all scrapers from the database."""
    if not POSTGRES_AVAILABLE:
        return "📊 <i>PostgreSQL not available</i>"
    
    def fmt_num(n: int) -> str:
        """Format number in human-readable format."""
        if not human_readable:
            return f"{n:,}"
        if n >= 1_000_000_000:
            return f"{n/1_000_000_000:.2f}B"
        elif n >= 1_000_000:
            return f"{n/1_000_000:.2f}M"
        elif n >= 1_000:
            return f"{n/1_000:.1f}K"
        return f"{n:,}"
    
    lines = ["📊 <b>All Scrapers Summary</b>\n"]
    
    try:
        # Use first available scraper for DB connection (prefix doesn't matter
        # since run_ledger is a shared table without prefix)
        first_scraper = next(iter(SCRAPERS.keys()), "Malaysia")
        db = PostgresDB(first_scraper)
        
        with db.cursor() as cur:
            # Get latest run for each scraper
            cur.execute("""
                SELECT DISTINCT ON (scraper_name)
                    scraper_name, status, started_at, ended_at,
                    items_scraped, items_exported
                FROM run_ledger
                ORDER BY scraper_name, started_at DESC
            """)
            
            runs = cur.fetchall()
            
            if runs:
                lines.append("<b>Latest Runs:</b>")
                for row in runs:
                    name, status, started, ended, scraped, exported = row
                    emoji = STATUS_EMOJIS.get(status, "⚪")
                    
                    # Format time nicely
                    if started and isinstance(started, datetime):
                        time_str = started.strftime("%m-%d %H:%M")
                    else:
                        time_str = str(started)[:16] if started else "N/A"
                    
                    status_line = f"  {emoji} <b>{name}</b>: {status}"
                    if scraped:
                        status_line += f" | 📦 {fmt_num(scraped)}"
                    status_line += f" | 🕐 {time_str}"
                    lines.append(status_line)
            else:
                lines.append("<i>No runs found in database</i>")
        
        db.close()
        
    except Exception as e:
        lines.append(f"<i>Error: {str(e)[:100]}</i>")
    
    return "\n".join(lines)


# =============================================================================
# Pipeline Operations
# =============================================================================

def build_pipeline_command(scraper_name: str, fresh: bool = False) -> Tuple[List[str], Path]:
    scraper_path = SCRAPERS[scraper_name]["path"]
    resume_script = scraper_path / "run_pipeline_resume.py"
    if resume_script.exists():
        cmd = [sys.executable, "-u", str(resume_script)]
        if fresh:
            cmd.append("--fresh")
        return cmd, scraper_path

    pipeline_bat = scraper_path / "run_pipeline.bat"
    if pipeline_bat.exists():
        if sys.platform == "win32":
            cmd = ["cmd", "/c", str(pipeline_bat)]
        else:
            cmd = ["sh", str(pipeline_bat)]
        return cmd, scraper_path

    raise FileNotFoundError(f"No pipeline runner found for {scraper_name}")


def launch_pipeline(scraper_name: str, fresh: bool = False) -> Tuple[int, Path]:
    acquired, lock_file, reason = claim_pipeline_start_lock(
        scraper_name, owner="telegram", repo_root=REPO_ROOT
    )
    if not acquired:
        raise RuntimeError(f"{scraper_name} is already running ({reason})")

    cmd, cwd = build_pipeline_command(scraper_name, fresh=fresh)
    log_dir = get_logs_dir()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = log_dir / f"{scraper_name}_pipeline_{timestamp}.log"

    log_handle = log_path.open("a", encoding="utf-8", errors="replace")
    log_handle.write(f"[BOT] Starting {scraper_name} at {datetime.now().isoformat()}\n")
    log_handle.write(f"[BOT] Command: {' '.join(cmd)}\n\n")
    log_handle.flush()

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    
    # Generate and pass run_id to pipeline so all components (GUI/API/Telegram) are in sync
    if fresh:
        # Check if pipeline is already running (started from elsewhere)
        existing_run_id = None
        try:
            pm = _get_path_manager()
            if pm:
                output_dir = ConfigManager.get_output_dir(scraper_name)
                run_id_file = output_dir / ".current_run_id"
                if run_id_file.exists():
                    existing_run_id = run_id_file.read_text(encoding='utf-8').strip()
                    # Check if lock file exists (confirming it's running)
                    new_lock, _old_lock = get_lock_paths(scraper_name)
                    if not new_lock.exists():
                        existing_run_id = None  # Not actually running
        except Exception:
            pass
        
        if existing_run_id:
            run_id = existing_run_id
            log_handle.write(f"[BOT] Using existing run_id from running pipeline: {run_id}\n")
        else:
            # Fresh run: generate new run_id
            run_id = f"{scraper_name}_{timestamp}"
            log_handle.write(f"[BOT] Generated fresh run_id: {run_id}\n")
        
        env_var_name = f"{scraper_name.upper().replace(' ', '_').replace('-', '_')}_RUN_ID"
        env[env_var_name] = run_id
        log_handle.flush()

    try:
        process = subprocess.Popen(
            cmd,
            cwd=str(cwd),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            env=env
        )
    except Exception:
        release_pipeline_lock(lock_file)
        try:
            log_handle.close()
        except Exception:
            pass
        raise

    ensure_lock_file(lock_file, process.pid, log_path=log_path)

    def _monitor() -> None:
        exit_code = process.wait()
        try:
            with log_path.open("a", encoding="utf-8", errors="replace") as handle:
                handle.write(f"\n[BOT] {scraper_name} exited with code {exit_code} at {datetime.now().isoformat()}\n")
        finally:
            remove_lock_file(lock_file)
            try:
                log_handle.close()
            except Exception:
                pass

    threading.Thread(target=_monitor, daemon=True).start()
    return process.pid, log_path


# =============================================================================
# Formatting Functions (Improved UI/UX)
# =============================================================================

def format_status_line(scraper_name: str, status: Dict[str, Optional[str]]) -> str:
    state = status.get("state", "idle")
    emoji = STATUS_EMOJIS.get(state, "⚪")
    
    if state == "running":
        line = f"{emoji} <b>{scraper_name}</b>: RUNNING"
        if status.get("pid"):
            line += f" <code>(pid {status['pid']})</code>"
        return line
    if state == "stale":
        line = f"{emoji} <b>{scraper_name}</b>: STALE LOCK"
        if status.get("pid"):
            line += f" <code>(pid {status['pid']})</code>"
        return line
    return f"{emoji} <b>{scraper_name}</b>: IDLE"


def format_status_details(scraper_name: str, status: Dict[str, Optional[str]]) -> str:
    lines = [format_status_line(scraper_name, status)]
    
    if status.get("state") == "running":
        log_path = find_latest_log(scraper_name, status)
        if log_path:
            progress = extract_latest_progress(log_path)
            if progress:
                lines.append(f"\n{progress}")
            lines.append(f"\n📝 <b>Log:</b> <code>{log_path.name}</code>")
        else:
            lines.append("\n<i>Current Step: (log not available)</i>")
    
    if status.get("started"):
        lines.append(f"\n🕐 <b>Started:</b> <code>{status['started']}</code>")
    
    # Add DB summary
    lines.append("")
    summary = get_db_summary(scraper_name)
    lines.append(format_db_summary(summary))
    
    return "\n".join(lines)


def format_help_text() -> str:
    """Format help text with emojis and better layout."""
    return (
        "🤖 <b>Scraper Bot Commands</b>\n\n"
        "<b>General:</b>\n"
        "  /help, /start - Show this help message\n"
        "  /whoami - Show your chat ID\n"
        "  /ping - Health check\n"
        "  /list - List available scrapers\n\n"
        "<b>Status & Monitoring:</b>\n"
        "  /status &lt;scraper|all&gt; - Check pipeline status with DB summary\n"
        "  /summary &lt;scraper&gt; - Show detailed database table summary\n"
        "  allincome - Show all scrapers data summary (human-readable)\n\n"
        "<b>Control:</b>\n"
        "  /run &lt;scraper&gt; [fresh] - Start pipeline if idle\n"
        "  /resume &lt;scraper&gt; - Resume pipeline\n"
        "  /runfresh &lt;scraper&gt; - Start a fresh pipeline\n"
        "  /stop &lt;scraper&gt; - Stop running pipeline\n"
        "  /clear &lt;scraper&gt; - Clear stale lock file"
    )


def parse_allowed_chat_ids(raw_value: Optional[str]) -> Optional[set]:
    if not raw_value:
        return None
    ids = set()
    for part in raw_value.split(","):
        part = part.strip()
        if not part:
            continue
        if part.lstrip("-").isdigit():
            ids.add(int(part))
    return ids or None


# =============================================================================
# Telegram Bot Class
# =============================================================================

class TelegramBot:
    def __init__(self, token: str, allowed_chat_ids: Optional[set], default_scraper: Optional[str] = None):
        self.token = token
        self.allowed_chat_ids = allowed_chat_ids
        self.default_scraper = default_scraper
        self.base_url = f"https://api.telegram.org/bot{self.token}"
        self.last_update_id = None

    def send_message(self, chat_id: int, text: str, parse_mode: str = "HTML") -> None:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
            "parse_mode": parse_mode
        }
        try:
            requests.post(f"{self.base_url}/sendMessage", data=payload, timeout=10)
        except Exception:
            pass

    def is_authorized(self, chat_id: int) -> bool:
        if not self.allowed_chat_ids:
            return True
        return chat_id in self.allowed_chat_ids

    def handle_allincome(self, chat_id: int) -> None:
        """Handle 'allincome' message - show all scrapers data summary."""
        if not POSTGRES_AVAILABLE:
            self.send_message(chat_id, "📊 <i>PostgreSQL not available</i>")
            return
        
        self.send_message(chat_id, "⏳ Fetching all scrapers data summary...")
        
        lines = ["📊 <b>All Scrapers Data Summary</b>\n"]
        
        def fmt_num(n: int) -> str:
            """Format number in human-readable format."""
            if n >= 1_000_000_000:
                return f"{n/1_000_000_000:.2f}B"
            elif n >= 1_000_000:
                return f"{n/1_000_000:.2f}M"
            elif n >= 1_000:
                return f"{n/1_000:.1f}K"
            return f"{n:,}"
        
        try:
            total_all_records = 0
            scraper_data = []
            
            for scraper_name in sorted(SCRAPERS.keys()):
                try:
                    db = PostgresDB(scraper_name)
                    prefix = db.prefix
                    
                    with db.cursor() as cur:
                        # Get all tables with this country's prefix
                        cur.execute("""
                            SELECT table_name 
                            FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name LIKE %s
                            AND table_type = 'BASE TABLE'
                        """, (f"{prefix}%",))
                        
                        tables = [row[0] for row in cur.fetchall()]
                        
                        # Get row counts for entity tables only
                        entity_count = 0
                        table_counts = {}
                        for table in tables:
                            try:
                                cur.execute(f"SELECT COUNT(*) FROM {table}")
                                count = cur.fetchone()[0]
                                display_name = table.replace(prefix, "")
                                table_counts[display_name] = count
                                
                                # Track total entities (common entity tables)
                                if any(x in table for x in ['product', 'tender', 'drug', 'sku', 'entity', 'item', 'record']):
                                    entity_count += count
                            except Exception:
                                pass
                        
                        # Get latest run info
                        latest_scraped = 0
                        try:
                            cur.execute("""
                                SELECT items_scraped
                                FROM run_ledger 
                                WHERE scraper_name = %s 
                                ORDER BY started_at DESC 
                                LIMIT 1
                            """, (scraper_name,))
                            row = cur.fetchone()
                            if row and row[0]:
                                latest_scraped = row[0]
                        except Exception:
                            pass
                        
                        scraper_data.append({
                            'name': scraper_name,
                            'entity_count': entity_count,
                            'tables': table_counts,
                            'latest_scraped': latest_scraped
                        })
                        total_all_records += entity_count
                    
                    db.close()
                except Exception:
                    pass
            
            # Sort by entity count (descending)
            scraper_data.sort(key=lambda x: -x['entity_count'])
            
            # Build output
            for data in scraper_data:
                if data['entity_count'] > 0 or data['latest_scraped'] > 0:
                    lines.append(f"📌 <b>{data['name']}</b>")
                    lines.append(f"   📦 Records: <code>{fmt_num(data['entity_count'])}</code>")
                    if data['latest_scraped'] > 0:
                        lines.append(f"   📥 Last Run: <code>{fmt_num(data['latest_scraped'])}</code>")
                    
                    # Show top 3 tables
                    top_tables = sorted(data['tables'].items(), key=lambda x: -x[1])[:3]
                    if top_tables:
                        table_str = " | ".join([f"{n}: {fmt_num(c)}" for n, c in top_tables if c > 0])
                        if table_str:
                            lines.append(f"   📋 {table_str}")
                    lines.append("")
            
            lines.append(f"📊 <b>Grand Total:</b> <code>{fmt_num(total_all_records)}</code> records")
            
        except Exception as e:
            lines.append(f"<i>Error: {str(e)[:100]}</i>")
        
        self.send_message(chat_id, "\n".join(lines))

    def handle_command(self, chat_id: int, text: str) -> None:
        parts = text.strip().split()
        if not parts:
            return
        cmd = parts[0].split("@")[0].lower()
        args = parts[1:]
        
        # Handle 'allincome' message
        if cmd.lower() == "allincome":
            self.handle_allincome(chat_id)
            return

        if cmd in ("/start", "/help"):
            self.send_message(chat_id, format_help_text())
            return
            
        if cmd == "/whoami":
            self.send_message(chat_id, f"🆔 <b>Your Chat ID:</b> <code>{chat_id}</code>")
            return
            
        if cmd == "/ping":
            self.send_message(chat_id, "🏓 <b>pong</b>")
            return

        if cmd == "/list":
            names = sorted(SCRAPERS.keys())
            lines = ["📋 <b>Available Scrapers:</b>\n"]
            for i, name in enumerate(names, 1):
                lines.append(f"  {i}. {name}")
            self.send_message(chat_id, "\n".join(lines))
            return

        if cmd == "/summary":
            scraper_name = resolve_scraper_name(args[0]) if args else self.default_scraper
            if not scraper_name:
                self.send_message(chat_id, "⚠️ <b>Usage:</b> <code>/summary &lt;scraper&gt;</code>")
                return
            
            self.send_message(chat_id, f"⏳ Fetching summary for <b>{scraper_name}</b>...")
            summary = get_db_summary(scraper_name)
            self.send_message(chat_id, format_db_summary(summary))
            return

        if cmd == "/status":
            if args and args[0].lower() in ("all", "*"):
                # Show all scrapers status
                lines = ["📊 <b>All Scrapers Status</b>\n"]
                for name in sorted(SCRAPERS.keys()):
                    status = get_pipeline_status(name)
                    line = format_status_line(name, status)
                    if status.get("state") == "running":
                        log_path = find_latest_log(name, status)
                        if log_path:
                            progress = extract_latest_progress(log_path)
                            if progress:
                                line += f"\n   └─ {progress}"
                    lines.append(line)
                
                # Add DB summary for all
                lines.append("\n" + "─" * 30)
                lines.append(get_all_scrapers_summary())
                
                self.send_message(chat_id, "\n".join(lines))
                return
            
            scraper_name = resolve_scraper_name(args[0]) if args else self.default_scraper
            if not scraper_name:
                self.send_message(chat_id, "⚠️ <b>Usage:</b> <code>/status &lt;scraper|all&gt;</code>")
                return
            
            status = get_pipeline_status(scraper_name)
            self.send_message(chat_id, format_status_details(scraper_name, status))
            return

        if cmd in ("/run", "/runfresh", "/resume"):
            fresh = cmd == "/runfresh" or any(arg.lower() in ("fresh", "--fresh", "-f") for arg in args)
            scraper_arg = None
            for arg in args:
                if arg.lower() in ("fresh", "--fresh", "-f"):
                    continue
                scraper_arg = arg
                break
            scraper_name = resolve_scraper_name(scraper_arg) if scraper_arg else self.default_scraper
            if not scraper_name:
                if cmd == "/resume":
                    self.send_message(chat_id, "⚠️ <b>Usage:</b> <code>/resume &lt;scraper&gt;</code>")
                else:
                    self.send_message(chat_id, "⚠️ <b>Usage:</b> <code>/run &lt;scraper&gt; [fresh]</code>")
                return
            
            status = get_pipeline_status(scraper_name)
            if status["state"] == "running":
                self.send_message(chat_id, format_status_details(scraper_name, status))
                return
            
            try:
                self.send_message(chat_id, f"⏳ Starting <b>{scraper_name}</b> ({'fresh' if fresh else 'resume'})...")
                pid, log_path = launch_pipeline(scraper_name, fresh=fresh)
                mode = "🆕 fresh" if fresh else "▶️ resume"
                self.send_message(
                    chat_id,
                    f"✅ <b>Started {scraper_name}</b> ({mode})\n"
                    f"🆔 <b>PID:</b> <code>{pid}</code>\n"
                    f"📝 <b>Log:</b> <code>{log_path.name}</code>"
                )
            except Exception as exc:
                self.send_message(chat_id, f"❌ <b>Failed to start {scraper_name}:</b> <code>{exc}</code>")
            return

        if cmd == "/stop":
            scraper_name = resolve_scraper_name(args[0]) if args else self.default_scraper
            if not scraper_name:
                self.send_message(chat_id, "⚠️ <b>Usage:</b> <code>/stop &lt;scraper&gt;</code>")
                return
            
            status = get_pipeline_status(scraper_name)
            if status["state"] != "running":
                self.send_message(chat_id, f"⚠️ <b>{scraper_name}</b> is not running.")
                return
            
            pid = None
            if status.get("pid"):
                try:
                    pid = int(str(status["pid"]))
                except Exception:
                    pid = None
            
            if not terminate_pid(pid):
                self.send_message(chat_id, f"❌ <b>Failed to stop {scraper_name}.</b>")
                return
            
            lock_path = status.get("lock_file")
            if lock_path:
                remove_lock_file(Path(lock_path))
            
            self.send_message(chat_id, f"🛑 <b>Stopped {scraper_name}</b> <code>(pid {pid})</code>")
            return

        if cmd == "/clear":
            scraper_name = resolve_scraper_name(args[0]) if args else self.default_scraper
            if not scraper_name:
                self.send_message(chat_id, "⚠️ <b>Usage:</b> <code>/clear &lt;scraper&gt;</code>")
                return
            
            status = get_pipeline_status(scraper_name)
            if status["state"] == "running":
                self.send_message(chat_id, "⚠️ Pipeline is running; lock not cleared.")
                return
            
            lock_path = status.get("lock_file")
            if lock_path:
                remove_lock_file(Path(lock_path))
                self.send_message(chat_id, f"🧹 <b>Cleared lock for {scraper_name}</b>")
            else:
                self.send_message(chat_id, f"ℹ️ <b>No lock file found for {scraper_name}</b>")
            return

        self.send_message(chat_id, "❓ Unknown command. Use /help for options.")

    def poll(self) -> None:
        while True:
            try:
                params = {"timeout": 60}
                if self.last_update_id is not None:
                    params["offset"] = self.last_update_id + 1
                response = requests.get(f"{self.base_url}/getUpdates", params=params, timeout=70)
                response.raise_for_status()
                payload = response.json()
                if not payload.get("ok"):
                    time.sleep(2)
                    continue

                new_last_update_id = self.last_update_id
                for update in payload.get("result", []):
                    update_id = update.get("update_id")
                    if update_id is None:
                        continue
                    if new_last_update_id is not None and update_id <= new_last_update_id:
                        continue
                    new_last_update_id = update_id

                    message = update.get("message") or update.get("edited_message")
                    if not message:
                        continue
                    chat_id = message.get("chat", {}).get("id")
                    text = message.get("text", "")
                    if not chat_id or not text:
                        continue
                    
                    # Always respond to messages (bot is always ON)
                    text_stripped = text.strip()
                    cmd = text_stripped.split()[0].split("@")[0].lower()
                    
                    # Handle /whoami without authorization check
                    if cmd == "/whoami":
                        try:
                            self.handle_command(chat_id, text)
                        except Exception:
                            pass
                        continue
                    
                    # Check authorization for other commands
                    if not self.is_authorized(chat_id):
                        try:
                            self.send_message(
                                chat_id,
                                "🚫 <b>Not authorized.</b>\nSend /whoami to get your chat ID."
                            )
                        except Exception:
                            pass
                        continue
                    
                    # Handle all commands and messages
                    try:
                        self.handle_command(chat_id, text)
                    except Exception:
                        pass
                    continue

                if new_last_update_id is not None:
                    self.last_update_id = new_last_update_id
            except Exception:
                time.sleep(3)


# =============================================================================
# Main Entry Point
# =============================================================================

def main() -> None:
    load_dotenv(REPO_ROOT / ".env")
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        print("[ERROR] Missing TELEGRAM_BOT_TOKEN in environment.")
        sys.exit(1)
    
    allowed_chat_ids = parse_allowed_chat_ids(os.getenv("TELEGRAM_ALLOWED_CHAT_IDS"))
    default_scraper = resolve_scraper_name(os.getenv("TELEGRAM_DEFAULT_SCRAPER"))

    # Check database availability
    if POSTGRES_AVAILABLE:
        print("[OK] PostgreSQL support enabled")
    else:
        print("[WARN] PostgreSQL support not available (core.db.postgres_connection)")

    if REGISTRY_AVAILABLE:
        print(f"[OK] Scraper registry loaded ({len(SCRAPER_CONFIGS)} scrapers)")
    else:
        print(f"[WARN] Registry not available, using fallback ({len(SCRAPERS)} scrapers)")

    bot = TelegramBot(token, allowed_chat_ids, default_scraper=default_scraper)
    print("[OK] Telegram bot started. Waiting for commands...")
    bot.poll()


if __name__ == "__main__":
    main()
