#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline API Server (FastAPI)

REST API that mirrors GUI pipeline operations.  When called from an external
application every request is logged to the server console with an [API] prefix
so the operator sees exactly what is happening — just like using the GUI.

Usage (standalone):
    pip install fastapi uvicorn
    python services/api_server.py                    # default 0.0.0.0:8000
    python services/api_server.py --port 9000        # custom port

Usage (embedded — started from GUI):
    from services.api_server import start_embedded, stop_embedded

Endpoints:
    GET  /docs                                        — Swagger UI
    GET  /api/v1/health                               — Health check
    GET  /api/v1/scrapers                             — List all scrapers + steps
    GET  /api/v1/scrapers/{country}/status             — Current pipeline status
    POST /api/v1/scrapers/{country}/run                — Start pipeline
    POST /api/v1/scrapers/{country}/stop               — Stop running pipeline
    GET  /api/v1/scrapers/{country}/steps              — Steps with completion status
    GET  /api/v1/scrapers/{country}/logs               — Latest log tail
    GET  /api/v1/scrapers/{country}/runs               — Run history
    GET  /api/v1/scrapers/{country}/runs/{rid}/metrics  — Run metrics
    GET  /api/v1/scrapers/{country}/tables             — DB tables + row counts
    POST /api/v1/scrapers/{country}/health-check       — Run health check script
    GET  /api/v1/scrapers/{country}/progress           — Live progress
"""

import os
import sys
import signal
import subprocess
import threading
import time
import logging
import re
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from core.pipeline.pipeline_start_lock import (
    claim_pipeline_start_lock,
    release_pipeline_lock,
    update_pipeline_lock,
)

# ---------------------------------------------------------------------------
# Path wiring
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# ---------------------------------------------------------------------------
# Logging — every API action appears on the server console
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[API] %(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("api_server")

# ---------------------------------------------------------------------------
# FastAPI imports (fail gracefully with install hint)
# ---------------------------------------------------------------------------
try:
    from fastapi import FastAPI, HTTPException, Request
    from fastapi.middleware.cors import CORSMiddleware
    from pydantic import BaseModel
    import uvicorn
    _FASTAPI_AVAILABLE = True
except ImportError:
    _FASTAPI_AVAILABLE = False
    logger.error("FastAPI not installed.  Run:  pip install fastapi uvicorn")

# ---------------------------------------------------------------------------
# Internal imports
# ---------------------------------------------------------------------------
from services.scraper_registry import (
    SCRAPER_CONFIGS,
    REPO_ROOT as _REG_ROOT,
    get_scraper_names,
    get_scraper_config,
    get_scraper_path,
    get_pipeline_script,
    resolve_country_name,
)

try:
    from core.pipeline.pipeline_checkpoint import get_checkpoint_manager
    _CHECKPOINT_AVAILABLE = True
except ImportError:
    _CHECKPOINT_AVAILABLE = False

try:
    from core.db.postgres_connection import get_db
    _DB_AVAILABLE = True
except ImportError:
    _DB_AVAILABLE = False

# ---------------------------------------------------------------------------
# In-memory process & log tracker  (mirrors GUI's self.running_processes)
# ---------------------------------------------------------------------------
_running_processes: Dict[str, subprocess.Popen] = {}
_process_logs: Dict[str, deque] = {}          # last N lines per scraper
_process_start_times: Dict[str, datetime] = {}
_process_lock_files: Dict[str, Path] = {}
_last_runtime_event_key: Dict[str, tuple] = {}
_LOG_BUFFER_SIZE = 2000                        # keep last 2 000 lines

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
if _FASTAPI_AVAILABLE:
    class RunRequest(BaseModel):
        fresh: bool = False
        step: Optional[int] = None

    class StopResponse(BaseModel):
        status: str
        scraper: str
        message: str

    class HealthResponse(BaseModel):
        status: str
        timestamp: str
        scrapers_running: List[str]

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Scraper Pipeline API",
    description="REST API that mirrors GUI pipeline operations.  Console shows [API] logs.",
    version="1.0.0",
) if _FASTAPI_AVAILABLE else None

if app:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_or_404(country: str) -> str:
    """Resolve country name or raise 404."""
    key = resolve_country_name(country)
    if not key:
        raise HTTPException(404, detail=f"Scraper '{country}' not found. Available: {get_scraper_names()}")
    return key


def _tail_log(scraper: str, lines: int = 200) -> List[str]:
    """Return last *lines* from in-memory log buffer."""
    buf = _process_logs.get(scraper, deque())
    return list(buf)[-lines:]


def _get_checkpoint(scraper: str):
    """Return checkpoint manager or None when unavailable."""
    if not _CHECKPOINT_AVAILABLE:
        return None
    try:
        return get_checkpoint_manager(scraper)
    except Exception:
        return None


def _record_timeline_event(
    scraper: str,
    event_type: str,
    run_id: Optional[str] = None,
    status: Optional[str] = None,
    step_number: Optional[int] = None,
    step_name: Optional[str] = None,
    source: str = "api",
    message: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    """Persist timeline event in checkpoint metadata with light dedupe."""
    cp = _get_checkpoint(scraper)
    if not cp:
        return

    if not run_id:
        try:
            run_id = (cp.get_metadata() or {}).get("run_id")
        except Exception:
            run_id = None

    dedupe_key = (
        event_type,
        run_id,
        status,
        step_number,
        step_name,
        source,
        message,
    )
    if _last_runtime_event_key.get(scraper) == dedupe_key:
        return

    _last_runtime_event_key[scraper] = dedupe_key
    try:
        cp.record_event(
            event_type=event_type,
            run_id=run_id,
            status=status,
            step_number=step_number,
            step_name=step_name,
            source=source,
            message=message,
            details=details or {},
        )
    except Exception:
        pass


_STEP_NUM_RE = re.compile(r"\bstep\s+(\d+)\b", re.IGNORECASE)
_STEP_WITH_NAME_RE = re.compile(r"\bstep\s+(\d+)\s*\(([^)]{1,200})\)", re.IGNORECASE)
_RUN_ID_RE = re.compile(r"\brun\s*id\s*[:=]\s*([A-Za-z0-9._:-]+)", re.IGNORECASE)
_PIPELINE_PROGRESS_RE = re.compile(
    r"pipeline\s*step\s*:\s*(\d+)\s*/\s*(\d+)\s*\(([\d.]+)%\)\s*-\s*(.+)",
    re.IGNORECASE,
)


def _extract_step_context_from_line(line: str) -> tuple:
    """Try to extract step number and step name from a log line."""
    step_number = None
    step_name = None

    m_name = _STEP_WITH_NAME_RE.search(line)
    if m_name:
        try:
            step_number = int(m_name.group(1))
        except Exception:
            step_number = None
        step_name = m_name.group(2).strip()
        return step_number, step_name

    m_num = _STEP_NUM_RE.search(line)
    if m_num:
        try:
            step_number = int(m_num.group(1))
        except Exception:
            step_number = None
    return step_number, step_name


def _classify_log_line_event(line: str) -> Optional[Dict[str, Any]]:
    """Map known log patterns into structured timeline events."""
    lower = line.lower().strip()
    if not lower:
        return None
    step_number, step_name = _extract_step_context_from_line(line)

    run_id_match = _RUN_ID_RE.search(line)
    if run_id_match:
        return {
            "event_type": "run_id_detected",
            "run_id": run_id_match.group(1).strip(),
            "status": "running",
            "message": line,
            "source": "log",
        }

    if "[recovery]" in lower:
        return {
            "event_type": "pipeline_recovery",
            "status": "resume",
            "step_number": step_number,
            "step_name": step_name,
            "message": line,
            "source": "log",
        }

    if "[pause]" in lower and "resuming pipeline" in lower:
        return {
            "event_type": "pipeline_resumed",
            "status": "running",
            "message": line,
            "source": "log",
        }

    if "[checkpoint]" in lower and "marked step" in lower and "as complete" in lower:
        return {
            "event_type": "step_completed",
            "status": "completed",
            "step_number": step_number,
            "step_name": step_name,
            "message": line,
            "source": "log",
        }
    if "[checkpoint]" in lower and "marked complete but" in lower and "will re-run" in lower:
        return {
            "event_type": "step_reopened",
            "status": "pending",
            "step_number": step_number,
            "step_name": step_name,
            "message": line,
            "source": "log",
        }
    if "[checkpoint]" in lower and "will skip" in lower:
        return {
            "event_type": "step_skipped",
            "status": "skipped",
            "step_number": step_number,
            "step_name": step_name,
            "message": line,
            "source": "log",
        }
    if "running step" in lower or "starting step" in lower or "executing step" in lower:
        return {
            "event_type": "step_started",
            "status": "in_progress",
            "step_number": step_number,
            "step_name": step_name,
            "message": line,
            "source": "log",
        }
    if "skipping" in lower and "step" in lower:
        return {
            "event_type": "step_skipped",
            "status": "skipped",
            "step_number": step_number,
            "step_name": step_name,
            "message": line,
            "source": "log",
        }
    if "error" in lower or "failed" in lower:
        return {
            "event_type": "step_failed",
            "status": "failed",
            "step_number": step_number,
            "step_name": step_name,
            "message": line,
            "source": "log",
        }
    if "[timing]" in lower and "total pipeline duration" in lower:
        return {
            "event_type": "pipeline_timing",
            "status": "completed",
            "message": line,
            "source": "log",
        }
    if "[progress]" in lower:
        progress_match = _PIPELINE_PROGRESS_RE.search(line)
        if progress_match:
            current_step_display = int(progress_match.group(1))
            total_steps = int(progress_match.group(2))
            percent = float(progress_match.group(3))
            description = progress_match.group(4).strip()
            inferred_step = max(0, current_step_display - 1)
            if step_number is None:
                step_number = inferred_step
            return {
                "event_type": "pipeline_progress",
                "status": "completed" if percent >= 100.0 else "in_progress",
                "step_number": step_number,
                "step_name": step_name,
                "message": line,
                "source": "log",
                "details": {
                    "display_step": current_step_display,
                    "total_steps": total_steps,
                    "percent": percent,
                    "description": description,
                },
            }
        return {
            "event_type": "progress",
            "step_number": step_number,
            "step_name": step_name,
            "message": line,
            "source": "log",
        }
    return None


def _build_step_snapshot(scraper: str, run_id: Optional[str]) -> List[Dict[str, Any]]:
    """
    Build best-effort step states by combining registry + checkpoint + DB.
    """
    cfg = get_scraper_config(scraper) or {}
    steps_cfg = cfg.get("steps", [])
    snapshot: List[Dict[str, Any]] = []
    idx_map: Dict[int, int] = {}

    for i, step in enumerate(steps_cfg):
        row = {
            "step_number": i,
            "name": step.get("name"),
            "script": step.get("script"),
            "description": step.get("desc"),
            "status": "pending",
            "source": "registry",
        }
        snapshot.append(row)
        idx_map[i] = len(snapshot) - 1

    cp = _get_checkpoint(scraper)
    if cp:
        try:
            cp_info = cp.get_checkpoint_info()
            cp_data = cp._load_checkpoint()  # already used elsewhere in this codebase
            completed_steps = set(cp_info.get("completed_steps", []))
            step_outputs = cp_data.get("step_outputs", {})
            meta = cp.get_metadata() or {}

            for sn in completed_steps:
                if sn in idx_map:
                    snapshot[idx_map[sn]]["status"] = "completed"
                    snapshot[idx_map[sn]]["source"] = "checkpoint"

            for step_key, step_info in step_outputs.items():
                sn = step_info.get("step_number")
                if sn in idx_map:
                    row = snapshot[idx_map[sn]]
                    row["completed_at"] = step_info.get("completed_at")
                    row["duration_seconds"] = step_info.get("duration_seconds")
                    row["output_files_count"] = len(step_info.get("output_files", []))

            current_step = meta.get("current_step")
            if isinstance(current_step, int) and current_step in idx_map:
                current_row = snapshot[idx_map[current_step]]
                if current_row.get("status") != "completed":
                    current_row["status"] = "in_progress"
                    current_row["source"] = "checkpoint"
                if meta.get("current_step_name"):
                    current_row["name"] = meta.get("current_step_name")
        except Exception:
            pass

    if not (_DB_AVAILABLE and run_id):
        return snapshot

    try:
        db = get_db(scraper)
        prefix = cfg.get("db_prefix", scraper.lower()[:2])
        table_name = f"{prefix}_step_progress"

        exists_cur = db.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s)",
            (table_name,),
        )
        if not exists_cur.fetchone()[0]:
            return snapshot

        candidate_columns = [
            "step_number",
            "step_name",
            "status",
            "started_at",
            "completed_at",
            "duration_seconds",
            "rows_read",
            "rows_processed",
            "rows_inserted",
            "rows_updated",
            "rows_rejected",
            "browser_instances_spawned",
            "error_message",
            "log_file_path",
            "progress_key",
        ]
        cols_cur = db.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name = %s",
            (table_name,),
        )
        available = {r[0] for r in cols_cur.fetchall()}
        select_cols = [c for c in candidate_columns if c in available]
        if not select_cols:
            return snapshot

        order_fragments = ["step_number ASC"]
        if "completed_at" in select_cols:
            order_fragments.append("completed_at DESC NULLS LAST")
        elif "started_at" in select_cols:
            order_fragments.append("started_at DESC NULLS LAST")
        order_clause = ", ".join(order_fragments)

        step_cur = db.execute(
            f"SELECT {', '.join(select_cols)} FROM {table_name} WHERE run_id = %s ORDER BY {order_clause}",
            (run_id,),
        )
        for raw in step_cur.fetchall():
            row = dict(zip(select_cols, raw))
            step_number = row.get("step_number")
            if not isinstance(step_number, int):
                continue

            if step_number not in idx_map:
                idx_map[step_number] = len(snapshot)
                snapshot.append(
                    {
                        "step_number": step_number,
                        "name": row.get("step_name"),
                        "script": None,
                        "description": None,
                        "status": "pending",
                        "source": "db",
                    }
                )

            target = snapshot[idx_map[step_number]]
            target["source"] = "db"
            for col in select_cols:
                value = row.get(col)
                if value is None:
                    continue
                if hasattr(value, "isoformat"):
                    value = value.isoformat()
                if col == "step_name" and value:
                    target["name"] = value
                elif col == "status" and value:
                    target["status"] = value
                else:
                    target[col] = value
    except Exception:
        return snapshot

    snapshot.sort(key=lambda x: (x.get("step_number") is None, x.get("step_number", 10**9)))
    return snapshot


def _stream_output(scraper: str, process: subprocess.Popen):
    """Background thread: reads subprocess stdout and appends to log buffer."""
    buf = _process_logs.setdefault(scraper, deque(maxlen=_LOG_BUFFER_SIZE))
    try:
        for line_number, raw_line in enumerate(process.stdout, start=1):
            line = raw_line.rstrip("\n").rstrip("\r")
            buf.append(line)
            # Echo to server console so operator sees everything
            print(f"[API][{scraper}] {line}", flush=True)
            event = _classify_log_line_event(line)
            if event:
                details = event.get("details") or {}
                if not isinstance(details, dict):
                    details = {"raw_details": str(details)}
                details["line_number"] = line_number
                event["details"] = details
                _record_timeline_event(scraper=scraper, **event)
    except Exception:
        pass
    process.wait()
    rc = process.returncode
    status = "completed" if rc == 0 else f"failed (exit {rc})"
    logger.info(f"Pipeline {scraper} {status}")
    _record_timeline_event(
        scraper=scraper,
        event_type="pipeline_process_exited",
        status="completed" if rc == 0 else "failed",
        source="api",
        message=f"Process exited with code {rc}",
        details={"exit_code": rc},
    )
    _running_processes.pop(scraper, None)
    _process_start_times.pop(scraper, None)
    lock_file = _process_lock_files.pop(scraper, None)
    release_pipeline_lock(lock_file)

# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
if app:

    # -- Health -----------------------------------------------------------------
    @app.get("/api/v1/health", response_model=HealthResponse, tags=["System"])
    async def health():
        return HealthResponse(
            status="ok",
            timestamp=datetime.now().isoformat(),
            scrapers_running=list(_running_processes.keys()),
        )

    # -- List scrapers ----------------------------------------------------------
    @app.get("/api/v1/scrapers", tags=["Scrapers"])
    async def list_scrapers():
        """List all scrapers with their steps."""
        out = []
        for key, cfg in SCRAPER_CONFIGS.items():
            out.append({
                "key": key,
                "display_name": cfg.get("display_name", key),
                "path": cfg["path"],
                "total_steps": len(cfg["steps"]),
                "steps": cfg["steps"],
                "running": key in _running_processes,
            })
        return out

    # -- Status -----------------------------------------------------------------
    @app.get("/api/v1/scrapers/{country}/status", tags=["Pipeline"])
    async def scraper_status(country: str, request: Request):
        key = _resolve_or_404(country)
        logger.info(f"GET /scrapers/{key}/status — from {request.client.host}")

        result: Dict[str, Any] = {
            "scraper": key,
            "running": key in _running_processes,
        }

        cp = _get_checkpoint(key)
        if cp:
            try:
                info = cp.get_checkpoint_info()
                meta = cp.get_metadata() or {}
                result["checkpoint"] = {
                    "next_step": info.get("next_step", 0),
                    "total_completed": info.get("total_completed", 0),
                    "last_completed_step": info.get("last_completed_step"),
                }
                result["run_id"] = meta.get("run_id")
                result["current_step"] = meta.get("current_step")
                result["current_step_name"] = meta.get("current_step_name")
                result["pipeline_status"] = meta.get("status", "idle")
            except Exception as e:
                result["checkpoint_error"] = str(e)

        # DB run_ledger latest
        last_run = None
        if _DB_AVAILABLE:
            try:
                db = get_db(key)
                cur = db.execute(
                    "SELECT run_id, status, started_at, ended_at, step_count "
                    "FROM run_ledger WHERE scraper_name = %s "
                    "ORDER BY started_at DESC LIMIT 1",
                    (key,),
                )
                row = cur.fetchone()
                if row:
                    last_run = {
                        "run_id": row[0],
                        "status": row[1],
                        "started_at": row[2].isoformat() if row[2] else None,
                        "ended_at": row[3].isoformat() if row[3] else None,
                        "step_count": row[4],
                    }
                    result["last_run"] = last_run
            except Exception:
                pass

        active_run_id = result.get("run_id") or (last_run or {}).get("run_id")
        if active_run_id and not result.get("run_id"):
            result["run_id"] = active_run_id

        if cp:
            try:
                result["timeline"] = cp.get_events(limit=80, run_id=active_run_id)
            except Exception:
                result["timeline"] = []
        else:
            result["timeline"] = []

        result["step_snapshot"] = _build_step_snapshot(key, active_run_id)
        return result

    # -- Run pipeline -----------------------------------------------------------
    @app.post("/api/v1/scrapers/{country}/run", tags=["Pipeline"])
    async def run_pipeline(country: str, body: RunRequest, request: Request):
        key = _resolve_or_404(country)
        active_run_id: Optional[str] = None
        logger.info(
            f"POST /scrapers/{key}/run - fresh={body.fresh}, step={body.step} - from {request.client.host}"
        )

        if key in _running_processes:
            proc = _running_processes[key]
            if proc.poll() is None:
                raise HTTPException(409, detail=f"Pipeline {key} is already running (PID {proc.pid})")
            _running_processes.pop(key, None)

        lock_acquired, lock_file, lock_reason = claim_pipeline_start_lock(
            key, owner="api", repo_root=REPO_ROOT
        )
        if not lock_acquired:
            raise HTTPException(409, detail=f"Pipeline {key} is already running ({lock_reason})")

        script_path = get_pipeline_script(key)
        if not script_path or not script_path.exists():
            raise HTTPException(404, detail=f"Pipeline script not found for {key}")

        cmd = [sys.executable, "-u", str(script_path)]
        if body.fresh:
            cmd.append("--fresh")
        elif body.step is not None:
            cmd.extend(["--step", str(body.step)])

        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        env["PIPELINE_RUNNER"] = "1"
        env["API_TRIGGERED"] = "1"

        if body.fresh:
            existing_run_id = None
            try:
                from core.config.config_manager import ConfigManager
                # Migrated: get_path_manager() -> ConfigManager
                output_dir = ConfigManager.get_output_dir(key)
                run_id_file = output_dir / ".current_run_id"
                if run_id_file.exists():
                    existing_run_id = run_id_file.read_text(encoding="utf-8").strip()
                    lock_file = pm.get_lock_file(key)
                    if not lock_file.exists():
                        existing_run_id = None
            except Exception:
                pass

            if existing_run_id:
                run_id = existing_run_id
                logger.info(f"Using existing run_id from running pipeline: {run_id}")
            else:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                run_id = f"{key}_{timestamp}"

            env_var_name = f"{key.upper().replace(' ', '_').replace('-', '_')}_RUN_ID"
            env[env_var_name] = run_id
            logger.info(f"Set {env_var_name}={run_id}")
            active_run_id = run_id
        else:
            cp = _get_checkpoint(key)
            if cp:
                try:
                    active_run_id = (cp.get_metadata() or {}).get("run_id")
                except Exception:
                    active_run_id = None

        _record_timeline_event(
            scraper=key,
            event_type="run_requested",
            run_id=active_run_id,
            status="running",
            source="api",
            message="Run request accepted",
            details={
                "fresh": body.fresh,
                "step": body.step,
                "client_host": request.client.host if request.client else None,
            },
        )

        scraper_dir = get_scraper_path(key)
        _process_logs[key] = deque(maxlen=_LOG_BUFFER_SIZE)

        try:
            process = subprocess.Popen(
                cmd,
                cwd=str(scraper_dir),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env=env,
                text=True,
                bufsize=1,
            )
        except Exception as exc:
            release_pipeline_lock(lock_file)
            raise HTTPException(500, detail=f"Failed to start pipeline: {exc}")

        update_pipeline_lock(lock_file, process.pid)
        _process_lock_files[key] = lock_file

        _running_processes[key] = process
        _process_start_times[key] = datetime.now()
        _record_timeline_event(
            scraper=key,
            event_type="pipeline_process_started",
            run_id=active_run_id,
            status="running",
            source="api",
            message=f"Pipeline process started (pid={process.pid})",
            details={
                "pid": process.pid,
                "fresh": body.fresh,
                "step": body.step,
                "script": str(script_path),
            },
        )

        t = threading.Thread(target=_stream_output, args=(key, process), daemon=True)
        t.start()

        logger.info(f"Pipeline {key} started (PID {process.pid})")

        return {
            "status": "started",
            "scraper": key,
            "pid": process.pid,
            "fresh": body.fresh,
            "step": body.step,
            "run_id": active_run_id,
            "started_at": _process_start_times[key].isoformat(),
        }
    # -- Stop pipeline ----------------------------------------------------------
    @app.post("/api/v1/scrapers/{country}/stop", tags=["Pipeline"])
    async def stop_pipeline(country: str, request: Request):
        key = _resolve_or_404(country)
        logger.info(f"POST /scrapers/{key}/stop - from {request.client.host}")

        cp = _get_checkpoint(key)
        run_id = None
        if cp:
            try:
                run_id = (cp.get_metadata() or {}).get("run_id")
            except Exception:
                run_id = None

        proc = _running_processes.get(key)
        if not proc or proc.poll() is not None:
            _running_processes.pop(key, None)
            release_pipeline_lock(_process_lock_files.pop(key, None))
            _record_timeline_event(
                scraper=key,
                event_type="stop_requested",
                run_id=run_id,
                status="not_running",
                source="api",
                message="Stop requested but pipeline was not running",
            )
            return {"status": "not_running", "scraper": key, "message": "Pipeline is not running"}

        _record_timeline_event(
            scraper=key,
            event_type="stop_requested",
            run_id=run_id,
            status="stopping",
            source="api",
            message="Stop requested",
            details={"pid": proc.pid},
        )

        try:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        except Exception as e:
            logger.warning(f"Error stopping {key}: {e}")

        _running_processes.pop(key, None)
        _process_start_times.pop(key, None)
        release_pipeline_lock(_process_lock_files.pop(key, None))
        logger.info(f"Pipeline {key} stopped")
        _record_timeline_event(
            scraper=key,
            event_type="pipeline_stopped",
            run_id=run_id,
            status="stopped",
            source="api",
            message="Pipeline stopped successfully",
        )

        return {"status": "stopped", "scraper": key, "message": "Pipeline stopped successfully"}
    # -- Steps ------------------------------------------------------------------
    @app.get("/api/v1/scrapers/{country}/steps", tags=["Pipeline"])
    async def get_steps(country: str):
        key = _resolve_or_404(country)
        cfg = get_scraper_config(key)
        steps = cfg["steps"]

        # Add completion info from checkpoint
        completed_steps = set()
        cp = _get_checkpoint(key)
        if cp:
            try:
                info = cp.get_checkpoint_info()
                for i in range(info.get("total_completed", 0)):
                    completed_steps.add(i)
                cp._load_checkpoint()
                completed_steps = set(cp._checkpoint_data.get("completed_steps", []))
            except Exception:
                pass

        result = []
        for i, step in enumerate(steps):
            result.append({
                "step_number": i,
                "name": step["name"],
                "script": step["script"],
                "description": step["desc"],
                "completed": i in completed_steps,
                "skip_by_default": step.get("skip_by_default", False),
            })
        return result

    # -- Logs -------------------------------------------------------------------
    @app.get("/api/v1/scrapers/{country}/logs", tags=["Pipeline"])
    async def get_logs(country: str, lines: int = 200):
        key = _resolve_or_404(country)
        log_lines = _tail_log(key, lines)
        return {
            "scraper": key,
            "running": key in _running_processes,
            "line_count": len(log_lines),
            "lines": log_lines,
        }

    # -- Run history ------------------------------------------------------------
    @app.get("/api/v1/scrapers/{country}/runs", tags=["History"])
    async def get_runs(country: str, limit: int = 10):
        key = _resolve_or_404(country)
        if not _DB_AVAILABLE:
            raise HTTPException(503, detail="Database not available")

        try:
            db = get_db(key)
            cur = db.execute(
                "SELECT run_id, status, started_at, ended_at, step_count, "
                "items_scraped, total_runtime_seconds "
                "FROM run_ledger WHERE scraper_name = %s "
                "ORDER BY started_at DESC LIMIT %s",
                (key, limit),
            )
            rows = cur.fetchall()
            return [
                {
                    "run_id": r[0],
                    "status": r[1],
                    "started_at": r[2].isoformat() if r[2] else None,
                    "ended_at": r[3].isoformat() if r[3] else None,
                    "step_count": r[4],
                    "items_scraped": r[5],
                    "total_runtime_seconds": r[6],
                }
                for r in rows
            ]
        except Exception as e:
            raise HTTPException(500, detail=str(e))

    # -- Run metrics ------------------------------------------------------------
    @app.get("/api/v1/scrapers/{country}/runs/{run_id}/metrics", tags=["History"])
    async def get_run_metrics(country: str, run_id: str):
        key = _resolve_or_404(country)
        if not _DB_AVAILABLE:
            raise HTTPException(503, detail="Database not available")

        cfg = get_scraper_config(key)
        prefix = cfg.get("db_prefix", key.lower()[:2])
        table_name = f"{prefix}_step_progress"

        try:
            db = get_db(key)
            # Run info
            cur = db.execute(
                "SELECT run_id, status, started_at, ended_at, step_count, "
                "total_runtime_seconds, items_scraped "
                "FROM run_ledger WHERE run_id = %s AND scraper_name = %s",
                (run_id, key),
            )
            run_row = cur.fetchone()
            if not run_row:
                raise HTTPException(404, detail=f"Run {run_id} not found")

            # Step metrics
            steps = []
            try:
                cur = db.execute(
                    f"SELECT step_number, step_name, status, duration_seconds, "
                    f"rows_read, rows_processed, rows_inserted, rows_updated, "
                    f"rows_rejected, error_message "
                    f"FROM {table_name} WHERE run_id = %s ORDER BY step_number",
                    (run_id,),
                )
                for row in cur.fetchall():
                    steps.append({
                        "step_number": row[0],
                        "step_name": row[1],
                        "status": row[2],
                        "duration_seconds": row[3],
                        "rows_read": row[4],
                        "rows_processed": row[5],
                        "rows_inserted": row[6],
                        "rows_updated": row[7],
                        "rows_rejected": row[8],
                        "error_message": row[9],
                    })
            except Exception:
                pass  # step_progress table may not exist for this scraper

            cp = _get_checkpoint(key)
            timeline = []
            if cp:
                try:
                    timeline = cp.get_events(limit=150, run_id=run_id)
                except Exception:
                    timeline = []

            return {
                "run_id": run_row[0],
                "status": run_row[1],
                "started_at": run_row[2].isoformat() if run_row[2] else None,
                "ended_at": run_row[3].isoformat() if run_row[3] else None,
                "step_count": run_row[4],
                "total_runtime_seconds": run_row[5],
                "items_scraped": run_row[6],
                "steps": steps,
                "step_snapshot": _build_step_snapshot(key, run_id),
                "timeline": timeline,
            }
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(500, detail=str(e))

    # -- DB tables --------------------------------------------------------------
    @app.get("/api/v1/scrapers/{country}/tables", tags=["Database"])
    async def get_tables(country: str):
        key = _resolve_or_404(country)
        if not _DB_AVAILABLE:
            raise HTTPException(503, detail="Database not available")

        cfg = get_scraper_config(key)
        prefix = cfg.get("db_prefix", key.lower()[:2])

        try:
            db = get_db(key)
            cur = db.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public' "
                "AND tablename LIKE %s ORDER BY tablename",
                (f"{prefix}_%",),
            )
            tables = []
            for (tbl,) in cur.fetchall():
                try:
                    cnt_cur = db.execute(f"SELECT COUNT(*) FROM {tbl}")
                    count = cnt_cur.fetchone()[0]
                except Exception:
                    count = -1
                tables.append({"table": tbl, "row_count": count})

            # Also include run_ledger count for this scraper
            try:
                cnt_cur = db.execute(
                    "SELECT COUNT(*) FROM run_ledger WHERE scraper_name = %s", (key,)
                )
                tables.append({"table": "run_ledger (filtered)", "row_count": cnt_cur.fetchone()[0]})
            except Exception:
                pass

            return {"scraper": key, "db_prefix": prefix, "tables": tables}
        except Exception as e:
            raise HTTPException(500, detail=str(e))

    # -- Health check -----------------------------------------------------------
    @app.post("/api/v1/scrapers/{country}/health-check", tags=["Pipeline"])
    async def run_health_check(country: str, request: Request):
        key = _resolve_or_404(country)
        logger.info(f"POST /scrapers/{key}/health-check — from {request.client.host}")

        scraper_dir = get_scraper_path(key)
        hc_script = scraper_dir / "health_check.py"
        if not hc_script.exists():
            raise HTTPException(404, detail=f"No health_check.py for {key}")

        try:
            result = subprocess.run(
                [sys.executable, "-u", str(hc_script)],
                cwd=str(scraper_dir),
                capture_output=True,
                text=True,
                timeout=120,
            )
            return {
                "scraper": key,
                "exit_code": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }
        except subprocess.TimeoutExpired:
            raise HTTPException(504, detail="Health check timed out (120s)")
        except Exception as e:
            raise HTTPException(500, detail=str(e))

    # -- Progress ---------------------------------------------------------------
    @app.get("/api/v1/scrapers/{country}/progress", tags=["Pipeline"])
    async def get_progress(country: str):
        key = _resolve_or_404(country)
        cfg = get_scraper_config(key)
        total_steps = len(cfg["steps"])

        result: Dict[str, Any] = {
            "scraper": key,
            "running": key in _running_processes,
            "total_steps": total_steps,
        }

        cp = _get_checkpoint(key)
        if cp:
            try:
                info = cp.get_checkpoint_info()
                completed = info.get("total_completed", 0)
                pct = round((completed / total_steps) * 100, 1) if total_steps > 0 else 0

                meta = cp.get_metadata() or {}
                result["completed_steps"] = completed
                result["next_step"] = info.get("next_step", 0)
                result["percent"] = pct
                result["current_step"] = meta.get("current_step")
                result["current_step_name"] = meta.get("current_step_name")
                result["run_id"] = meta.get("run_id")
                result["pipeline_status"] = meta.get("status", "idle")
            except Exception:
                result["percent"] = 0

        log_lines = _tail_log(key, 500)
        for line in reversed(log_lines):
            if "[PROGRESS]" in line:
                result["last_progress_line"] = line
                break

        if key in _process_start_times:
            elapsed = (datetime.now() - _process_start_times[key]).total_seconds()
            result["elapsed_seconds"] = round(elapsed, 1)

        run_id = result.get("run_id")
        if not run_id and _DB_AVAILABLE:
            try:
                db = get_db(key)
                cur = db.execute(
                    "SELECT run_id FROM run_ledger WHERE scraper_name = %s ORDER BY started_at DESC LIMIT 1",
                    (key,),
                )
                row = cur.fetchone()
                if row:
                    run_id = row[0]
                    result["run_id"] = run_id
            except Exception:
                pass

        result["step_snapshot"] = _build_step_snapshot(key, run_id)
        if cp:
            try:
                result["timeline"] = cp.get_events(limit=120, run_id=run_id)
            except Exception:
                result["timeline"] = []
        else:
            result["timeline"] = []

        return result

    # -- Timeline ---------------------------------------------------------------
    @app.get("/api/v1/scrapers/{country}/timeline", tags=["Pipeline"])
    async def get_timeline(country: str, limit: int = 200, run_id: Optional[str] = None):
        key = _resolve_or_404(country)
        if limit < 1:
            limit = 1
        if limit > 2000:
            limit = 2000

        cp = _get_checkpoint(key)
        if not cp:
            return {
                "scraper": key,
                "run_id": run_id,
                "events": [],
                "step_snapshot": _build_step_snapshot(key, run_id),
                "running": key in _running_processes,
            }

        active_run_id = run_id
        if not active_run_id:
            try:
                active_run_id = (cp.get_metadata() or {}).get("run_id")
            except Exception:
                active_run_id = None

        events = cp.get_events(limit=limit, run_id=active_run_id)
        return {
            "scraper": key,
            "run_id": active_run_id,
            "event_count": len(events),
            "events": events,
            "step_snapshot": _build_step_snapshot(key, active_run_id),
            "running": key in _running_processes,
        }
# ---------------------------------------------------------------------------
# Embedded server support (for GUI integration)
# ---------------------------------------------------------------------------

_embedded_server_thread: Optional[threading.Thread] = None
_embedded_uvicorn_server = None


def start_embedded(host: str = "0.0.0.0", port: int = 8000) -> bool:
    """Start the API server in a background thread (called from GUI)."""
    global _embedded_server_thread, _embedded_uvicorn_server

    if not _FASTAPI_AVAILABLE:
        logger.error("Cannot start embedded API: FastAPI not installed")
        return False

    if _embedded_server_thread and _embedded_server_thread.is_alive():
        logger.warning("Embedded API server is already running")
        return False

    config = uvicorn.Config(app, host=host, port=port, log_level="info")
    _embedded_uvicorn_server = uvicorn.Server(config)

    def _run():
        logger.info(f"Embedded API server starting on {host}:{port}")
        _embedded_uvicorn_server.run()
        logger.info("Embedded API server stopped")

    _embedded_server_thread = threading.Thread(target=_run, daemon=True, name="api-server")
    _embedded_server_thread.start()
    logger.info(f"Embedded API server started on http://{host}:{port}/docs")
    return True


def stop_embedded() -> bool:
    """Stop the embedded API server."""
    global _embedded_server_thread, _embedded_uvicorn_server

    if _embedded_uvicorn_server:
        _embedded_uvicorn_server.should_exit = True
        logger.info("Embedded API server shutting down...")
        return True
    return False


# ---------------------------------------------------------------------------
# Standalone entry-point
# ---------------------------------------------------------------------------

def main():
    """Run API server standalone."""
    if not _FASTAPI_AVAILABLE:
        print("ERROR: FastAPI not installed.  Run:  pip install fastapi uvicorn")
        sys.exit(1)

    import argparse
    parser = argparse.ArgumentParser(description="Scraper Pipeline API Server")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8000, help="Bind port (default: 8000)")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload (dev mode)")
    args = parser.parse_args()

    print(f"""
================================================================================
  SCRAPER PIPELINE API SERVER
  Listening on http://{args.host}:{args.port}
  Swagger docs: http://localhost:{args.port}/docs
================================================================================
  [API] logs will appear below for every request.
  Pipeline output is streamed live with [API][ScraperName] prefix.
================================================================================
""")

    uvicorn.run(
        "scripts.common.api_server:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level="info",
    )


if __name__ == "__main__":
    main()



