#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline API

REST API for pipeline operations.

Usage:
    python services/pipeline_api.py
    
    Endpoints:
    - GET /api/v1/pipelines/{country}/status
    - POST /api/v1/pipelines/{country}/run
    - POST /api/v1/pipelines/{country}/stop
    - GET /api/v1/pipelines/{country}/runs/{run_id}/metrics
"""

import os
import sys
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime

# Add repo root to path
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

try:
    from flask import Flask, request, jsonify
    from flask_cors import CORS
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False
    Flask = None
    jsonify = None
    CORS = None

from core.db.postgres_connection import get_db
from core.monitoring.audit_logger import audit_log

logger = logging.getLogger(__name__)

app = Flask(__name__) if FLASK_AVAILABLE else None
if app and CORS:
    CORS(app)  # Enable CORS for all routes

# API Key authentication (simple - use OAuth in production)
API_KEYS = os.getenv("PIPELINE_API_KEYS", "").split(",")


def require_api_key(f):
    """Decorator to require API key authentication."""
    def decorated_function(*args, **kwargs):
        api_key = request.headers.get("X-API-Key")
        if not api_key or api_key not in API_KEYS:
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    decorated_function.__name__ = f.__name__
    return decorated_function


@app.route("/api/v1/pipelines/<country>/status", methods=["GET"])
@require_api_key
def get_pipeline_status(country: str):
    """Get current pipeline status."""
    try:
        db = get_db(country)
        with db.cursor() as cur:
            cur.execute("""
                SELECT run_id, status, started_at, ended_at, step_count, error_message
                FROM run_ledger
                WHERE scraper_name = %s
                ORDER BY started_at DESC
                LIMIT 1
            """, (country,))
            
            row = cur.fetchone()
            if row:
                return jsonify({
                    "scraper_name": country,
                    "run_id": row[0],
                    "status": row[1],
                    "started_at": row[2].isoformat() if row[2] else None,
                    "ended_at": row[3].isoformat() if row[3] else None,
                    "step_count": row[4],
                    "error_message": row[5]
                })
            return jsonify({
                "scraper_name": country,
                "status": "idle",
                "run_id": None
            })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/v1/pipelines/<country>/run", methods=["POST"])
@require_api_key
def start_pipeline(country: str):
    """Start a pipeline run."""
    try:
        import subprocess
        from datetime import datetime
        
        script_path = REPO_ROOT / "scripts" / country / "run_pipeline_resume.py"
        if not script_path.exists():
            return jsonify({"error": f"Pipeline not found for {country}"}), 404
        
        fresh = request.json.get("fresh", False)
        args = [sys.executable, str(script_path)]
        if fresh:
            args.append("--fresh")
        
        # Set up environment with run_id for sync across GUI/API/Telegram
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        
        if fresh:
            # Check if pipeline is already running (started from elsewhere)
            existing_run_id = None
            try:
                from core.config.config_manager import ConfigManager
                # Migrated: get_path_manager() -> ConfigManager
                output_dir = ConfigManager.get_output_dir(country)
                run_id_file = output_dir / ".current_run_id"
                if run_id_file.exists():
                    existing_run_id = run_id_file.read_text(encoding='utf-8').strip()
                    # Check if lock file exists (confirming it's running)
                    lock_file = pm.get_lock_file(country)
                    if not lock_file.exists():
                        existing_run_id = None  # Not actually running
            except Exception:
                pass
            
            if existing_run_id:
                run_id = existing_run_id
            else:
                # Fresh run: generate new run_id
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                run_id = f"{country}_{timestamp}"
            
            env_var_name = f"{country.upper().replace(' ', '_').replace('-', '_')}_RUN_ID"
            env[env_var_name] = run_id
        
        process = subprocess.Popen(
            args,
            cwd=str(script_path.parent),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env
        )
        
        audit_log(
            action="run_started",
            scraper_name=country,
            user=request.headers.get("X-User", "api"),
            details={"fresh": fresh, "pid": process.pid}
        )
        
        return jsonify({
            "status": "started",
            "scraper_name": country,
            "pid": process.pid
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/v1/pipelines/<country>/stop", methods=["POST"])
@require_api_key
def stop_pipeline(country: str):
    """Stop a running pipeline."""
    try:
        from shared_workflow_runner import stop_pipeline as stop_pipeline_func
        
        result = stop_pipeline_func(country)
        
        audit_log(
            action="run_stopped",
            scraper_name=country,
            user=request.headers.get("X-User", "api"),
            details=result
        )
        
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/v1/pipelines/<country>/runs/<run_id>/metrics", methods=["GET"])
@require_api_key
def get_run_metrics(country: str, run_id: str):
    """Get metrics for a specific run."""
    try:
        db = get_db(country)
        
        # Get run info
        with db.cursor() as cur:
            cur.execute("""
                SELECT run_id, status, started_at, ended_at, step_count,
                       total_runtime_seconds, slowest_step_number, slowest_step_name,
                       failure_step_number, failure_step_name
                FROM run_ledger
                WHERE run_id = %s AND scraper_name = %s
            """, (run_id, country))
            
            run_row = cur.fetchone()
            if not run_row:
                return jsonify({"error": "Run not found"}), 404
            
            # Get step metrics
            table_prefix_map = {
                "Argentina": "ar",
                "Malaysia": "my",
                "Netherlands": "nl",
            }
            prefix = table_prefix_map.get(country, country.lower()[:2])
            table_name = f"{prefix}_step_progress"
            
            cur.execute(f"""
                SELECT step_number, step_name, status, duration_seconds,
                       rows_read, rows_processed, rows_inserted, rows_updated, rows_rejected,
                       browser_instances_spawned, error_message
                FROM {table_name}
                WHERE run_id = %s
                ORDER BY step_number
            """, (run_id,))
            
            steps = []
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
                    "browser_instances_spawned": row[9],
                    "error_message": row[10]
                })
            
            return jsonify({
                "run_id": run_row[0],
                "status": run_row[1],
                "started_at": run_row[2].isoformat() if run_row[2] else None,
                "ended_at": run_row[3].isoformat() if run_row[3] else None,
                "step_count": run_row[4],
                "total_runtime_seconds": run_row[5],
                "slowest_step_number": run_row[6],
                "slowest_step_name": run_row[7],
                "failure_step_number": run_row[8],
                "failure_step_name": run_row[9],
                "steps": steps
            })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/v1/health", methods=["GET"])
def health_check():
    """Health check endpoint."""
    return jsonify({"status": "ok", "timestamp": datetime.now().isoformat()})


def main():
    """Run API server."""
    if not FLASK_AVAILABLE:
        logger.error("Flask not available. Install with: pip install flask flask-cors")
        return
    
    host = os.getenv("PIPELINE_API_HOST", "0.0.0.0")
    port = int(os.getenv("PIPELINE_API_PORT", "5000"))
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    
    logger.info(f"Starting Pipeline API on {host}:{port}")
    app.run(host=host, port=port, debug=False)


if __name__ == "__main__":
    main()
