#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper Management GUI
A comprehensive user interface for running scrapers and viewing documentation
"""

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog, simpledialog
import subprocess
import sys
import threading
import queue
import os
import shutil
from pathlib import Path
import webbrowser
from datetime import datetime
import json
import time
import traceback
from typing import Optional

# Try to import requests for Prometheus metrics
try:
    import requests
    _REQUESTS_AVAILABLE = True
except ImportError:
    _REQUESTS_AVAILABLE = False

# CRITICAL: Initialize ConfigManager FIRST before any other imports
# Add repo root to path for core.config_manager
repo_root = Path(__file__).resolve().parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

# Auto-install missing dependencies on GUI startup
def _check_and_install_dependencies():
    """Check for missing dependencies and offer to install them."""
    missing_packages = []
    
    # Check critical packages (GUI functionality)
    critical_packages = {
        'requests': 'requests',
        'psycopg2': 'psycopg2-binary',
        'prometheus_client': 'prometheus-client',
    }
    
    # Check optional packages (features work without them)
    optional_packages = {
        'redis': 'redis',
    }
    
    for module_name, package_name in critical_packages.items():
        try:
            __import__(module_name)
        except ImportError:
            missing_packages.append(package_name)
    
    if missing_packages:
        print(f"\n[GUI] Missing dependencies detected: {', '.join(missing_packages)}")
        print("[GUI] To install missing packages, run:")
        print(f"      pip install {' '.join(missing_packages)}")
        print("      Or install all dependencies:")
        print(f"      pip install -r {repo_root / 'requirements.txt'}\n")
        
        # Try to auto-install if enabled via environment variable
        auto_install_env = os.getenv('AUTO_INSTALL_DEPS', 'false').lower()
        if auto_install_env == 'true':
            try:
                import subprocess
                print(f"[GUI] Auto-installing missing packages...")
                result = subprocess.run(
                    [sys.executable, '-m', 'pip', 'install'] + missing_packages,
                    capture_output=True,
                    text=True,
                    timeout=300,
                    check=False
                )
                if result.returncode == 0:
                    print("[GUI] Successfully installed missing packages!")
                    print("[GUI] Please restart the GUI for changes to take effect.")
                else:
                    print(f"[GUI] Installation failed: {result.stderr}")
                    print("[GUI] Please install manually using the command above.")
            except Exception as e:
                print(f"[GUI] Auto-installation failed: {e}")
                print("[GUI] Please install manually using the command above.")
        else:
            print("[GUI] Tip: Set AUTO_INSTALL_DEPS=true environment variable to auto-install missing packages")
    
    # Check optional packages (just inform, don't require)
    missing_optional = []
    for module_name, package_name in optional_packages.items():
        try:
            __import__(module_name)
        except ImportError:
            missing_optional.append(package_name)
    
    if missing_optional:
        print(f"[GUI] Optional packages not installed (features will be disabled): {', '.join(missing_optional)}")

# Check dependencies on import (before GUI starts)
_check_and_install_dependencies()

try:
    from core.config_manager import ConfigManager

    # Ensure directories exist
    ConfigManager.ensure_dirs()

    # STARTUP RECOVERY: Recover stale "running" pipelines from crashes
    # This handles the scenario where the system crashed while a pipeline was running
    _total_recovered = 0

    # 1. Recover checkpoint/run_ledger (file-based)
    try:
        from shared_workflow_runner import recover_stale_pipelines
        recovery_result = recover_stale_pipelines()
        if recovery_result.get("total_recovered", 0) > 0:
            _total_recovered += recovery_result["total_recovered"]
            print(f"[STARTUP RECOVERY] Recovered {recovery_result['total_recovered']} stale checkpoint(s)")
            for scraper, recovered in recovery_result.get("checkpoint_recovery", {}).items():
                if recovered:
                    print(f"  - {scraper}: checkpoint marked as 'resume'")
    except ImportError:
        pass  # shared_workflow_runner not available
    except Exception as e:
        print(f"[STARTUP RECOVERY] Checkpoint recovery warning: {e}")

    # 2. Recover database run_ledger tables (Malaysia, India, etc.)
    try:
        from core.db.models import recover_stale_db_runs
        from core.db.postgres_connection import PostgresDB

        # Check each scraper's database (always close connection to avoid resource leak)
        for scraper_name in ["Malaysia", "India", "Argentina", "Russia"]:
            db = None
            try:
                db = PostgresDB(scraper_name)
                db.connect()
                db_result = recover_stale_db_runs(db, scraper_name)
                if db_result["resumed"] or db_result["stopped"]:
                    count = len(db_result["resumed"]) + len(db_result["stopped"])
                    _total_recovered += count
                    print(f"[STARTUP RECOVERY] {scraper_name} DB: {len(db_result['resumed'])} resumed, {len(db_result['stopped'])} stopped")
            except Exception as e:
                print(f"[STARTUP RECOVERY] {scraper_name} DB warning: {e}")
            finally:
                if db is not None:
                    try:
                        db.close()
                    except Exception:
                        pass
    except ImportError:
        pass  # DB modules not available
    except Exception as e:
        print(f"[STARTUP RECOVERY] DB recovery warning: {e}")

    if _total_recovered > 0:
        print(f"[STARTUP RECOVERY] Total recovered: {_total_recovered} stale pipeline state(s)")

    # Try to acquire single-instance lock (prevents EXE runaway sessions)
    # If lock exists, show warning but allow GUI to start (user can clear stale locks)
    _app_lock_acquired = False
    if ConfigManager:
        try:
            _app_lock_acquired = ConfigManager.acquire_lock()
            if not _app_lock_acquired:
                print("WARNING: Another instance may be running, or stale lock file exists.")
                print("You can clear stale locks using the 'Clear Lock' button in the GUI.")
                print("Continuing anyway...")
                # Don't exit - allow GUI to start so user can clear the lock
        except Exception as e:
            print(f"WARNING: Could not acquire app lock: {e}")
            print("Continuing anyway...")
            # Continue anyway - lock is for preventing duplicate instances, not critical
except ImportError as e:
    print(f"WARNING: Failed to import ConfigManager: {e}")
    print("Continuing with legacy mode...")
    ConfigManager = None
except Exception as e:
    print(f"WARNING: ConfigManager initialization failed: {e}")
    print("Continuing with legacy mode...")
    ConfigManager = None

# Try to import OpenAI
try:
    from openai import OpenAI
    _OPENAI_AVAILABLE = True
except ImportError:
    _OPENAI_AVAILABLE = False
    OpenAI = None

class ScraperGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Scraper Management Interface")
        
        # Get repository root
        self.repo_root = Path(__file__).resolve().parent
        
        # Professional color palette with subtle borders
        self.colors = {
            # Gray colors
            'dark_gray': '#1f2937',
            'medium_gray': '#4b5563',
            'light_gray': '#9ca3af',
            'background_gray': '#f4f5f7',
            'border_gray': '#e3e6ea',  # Softer, more subtle gray for borders
            'border_light': '#f4f5f7',  # Very subtle for minimal borders
            # White
            'white': '#fdfdfd',
            # Console colors (critical - must be black/yellow)
            'console_black': '#000000',
            'console_yellow': '#ffd700',
            # Tab colors
            'tab_bg': '#f6f7f9',  # Very light gray for unselected tabs
            'tab_selected': '#fdfdfd',  # Off-white for selected tab
            'tab_border': '#e3e6ea',  # Subtle border for tabs
            # Legacy compatibility
            'bg_main': '#f4f5f7',      # background_gray
            'bg_panel': '#fdfdfd',     # white
            'bg_dark': '#1f2937',      # dark_gray
            'bg_console': '#000000',   # console_black
            'text_black': '#1f2937',   # dark_gray
            'text_green': '#10b981',   # green (exception for JSON strings)
            'text_red': '#ef4444',     # red
            'text_console': '#ffd700', # console_yellow
            'border': '#e5e7eb',      # Softer gray for borders
        }
        
        # Standard font configuration - uniform across platform
        self.fonts = {
            'standard': ('Segoe UI', 9, 'normal'),
            'bold': ('Segoe UI', 9, 'bold'),
            'italic': ('Segoe UI', 9, 'italic'),
            'monospace': ('Consolas', 9),  # Only for code/console content
            'header': ('Segoe UI', 9, 'bold'),
            'small': ('Segoe UI', 8, 'normal'),
        }
        
        # Set window background to light gray
        self.root.configure(bg=self.colors['background_gray'])
        
        # Open in fullscreen/maximized window
        if sys.platform == "win32":
            self.root.state('zoomed')  # Maximized on Windows
        else:
            # On Unix-like systems, use screen dimensions
            self.root.attributes('-zoomed', True)  # Fullscreen on Linux
        
        self.root.minsize(1000, 700)
        self.root.resizable(False, False)  # Disable window resizing
        
        # Log size caps to prevent memory bloat and GUI hang after long runs (2–3+ hours)
        self.MAX_LOG_CHARS = 1_500_000   # ~1.5 MB in-memory per scraper
        self.MAX_DISPLAY_LOG_CHARS = 500_000  # Show last 500k in widget for responsive UI

        # Current scraper and step
        self.current_scraper = None
        self.current_step = None
        self.running_processes = {}  # Track processes per scraper: {scraper_name: process}
        self.running_scrapers = set()  # Track which scrapers are running from GUI
        self.scraper_logs = {}  # Store logs per scraper: {scraper_name: log_text}
        self._pipeline_lock_files = {}  # Track lock files created for pipeline runs: {scraper_name: lock_file_path}
        self._stopped_by_user = set()  # Track scrapers that were stopped by user: {scraper_name}
        self._stopping_scrapers = set()  # Track scrapers currently being stopped to prevent multiple simultaneous stop attempts
        self._stopping_started_at = {}  # Timestamp when each scraper stop began (for 30s stale state timeout)
        self.scraper_progress = {}  # Store progress state per scraper: {scraper_name: {"percent": float, "description": str}}
        self._last_completed_logs = {}  # Store last run log content per scraper for archive/save
        self._log_stream_state = {}  # Track external log stream offsets per scraper
        self._scraper_active_state = {}  # Track lock-based run activity per scraper
        self._external_log_files = {}  # Track external log files for pipelines started outside GUI
        self._last_known_lock_states = {}  # Track last known lock states to detect external starts/stops
        self._pending_table_refresh_after_id = None  # Debounce Output/Input table refreshes on scraper switch
        self._output_async_tokens = {"tables": 0, "runs": 0, "data": 0}  # Drop stale async Output tab updates
        self.MONITOR_COMBO_WIDTH = 14

        # Auto-restart timer: stop pipeline every 20 min, pause 30s, then resume to clear cache/memory
        self._auto_restart_timers = {}       # {scraper_name: after_id} - active Tkinter timer IDs
        self._auto_restart_pausing = set()   # scrapers currently in the 30s pause before resume
        self._auto_restart_resume_ids = {}   # {scraper_name: after_id} - pending resume timer IDs
        self._auto_restart_cycle_count = {}  # {scraper_name: int} - number of restart cycles completed

        # Network stats tracking for rate calculation
        self._prev_net_sent = 0
        self._prev_net_recv = 0
        self._prev_net_time = None

        # Ticker tape tracking
        self.ticker_label = None
        self.ticker_text = ""
        self.ticker_offset = 0
        self.ticker_running = False

        # Start periodic cleanup task to check for stale locks
        self.start_periodic_lock_cleanup()
        self.start_periodic_network_info_update()
        
        # Step explanations cache (key: script_path, value: explanation text)
        self.step_explanations = {}
        # Explanation cache file
        self.explanation_cache_file = self.repo_root / ".step_explanations_cache.json"
        self.load_explanation_cache()
        
        # API server state
        self._api_server_thread = None
        self._api_server_running = False
        self._api_server_port = 8099

        # Build scrapers from shared registry (single source of truth for GUI + API)
        self.scrapers = self._build_scrapers_from_registry()

        self.health_check_scripts = self._discover_health_check_scripts()
        self.health_check_json_path = None
        self.health_check_running = False
        
        self.setup_ui()
        self.load_documentation()
        # Load first documentation if available (after UI is set up)
        self.root.after(100, self.load_first_documentation)
        # Install dependencies and show progress in GUI console
        self.root.after(200, self.install_dependencies_in_gui)
    
    def _build_scrapers_from_registry(self):
        """Build self.scrapers dict from the shared scraper_registry.

        The registry (scripts/common/scraper_registry.py) is the single source of truth
        used by both this GUI and the FastAPI api_server.py.
        """
        try:
            from scripts.common.scraper_registry import SCRAPER_CONFIGS
        except ImportError:
            print("[GUI] WARNING: Could not import scraper_registry, falling back to empty config")
            return {}

        scrapers = {}
        for key, cfg in SCRAPER_CONFIGS.items():
            entry = {
                "path": self.repo_root / cfg["path"],
                "scripts_dir": "",
                "docs_dir": None,
                "steps": list(cfg["steps"]),  # shallow copy
                "pipeline_bat": cfg.get("pipeline_bat", "run_pipeline.bat"),
            }
            # Preserve optional keys that some scrapers use
            if "pipeline_script" in cfg:
                entry["pipeline_script"] = cfg["pipeline_script"]
            if "resume_options" in cfg:
                entry["resume_options"] = cfg["resume_options"]
            scrapers[key] = entry
        return scrapers

    def setup_styles(self):
        """Configure professional ttk styles with gray, white, black, and yellow color scheme"""
        style = ttk.Style()
        
        # Use modern theme if available
        try:
            style.theme_use('clam')
        except:
            pass
        
        # Main window background
        style.configure('TFrame', background=self.colors['background_gray'])
        style.configure('TLabel', background=self.colors['background_gray'], foreground='#111827')
        
        # Notebook tabs styling - professional, uniform design
        style.configure('TNotebook', 
                       borderwidth=0, 
                       background=self.colors['background_gray'], 
                       relief='flat')
        style.configure('TNotebook.Tab', 
                      borderwidth=0, 
                      relief='flat',
                      padding=[12, 8],
                      background=self.colors['tab_bg'],
                      foreground='#374151',
                      font=self.fonts['standard'])
        style.map('TNotebook.Tab',
                 background=[('selected', self.colors['tab_selected']), 
                            ('!selected', self.colors['tab_bg']),
                            ('active', '#f3f4f6')],
                 foreground=[('selected', '#000000'),
                           ('!selected', '#6b7280'),
                           ('active', '#374151')],
                 borderwidth=[('selected', 0), ('!selected', 0)],
                 expand=[('selected', [1, 1, 1, 0])])  # Subtle padding for selected
        
        # LabelFrames - white background, no borders
        style.configure('TLabelframe', 
                       borderwidth=0, 
                       relief='flat',
                       background=self.colors['white'])
        style.configure('TLabelframe.Label', 
                       background=self.colors['white'], 
                       foreground='#000000',
                       font=self.fonts['bold'])
        style.map('TLabelframe', background=[('', self.colors['white'])])
        
        # Title LabelFrame (for section headers) - no borders
        style.configure('Title.TLabelframe',
                      borderwidth=0,
                      relief='flat',
                      background=self.colors['white'])
        style.configure('Title.TLabelframe.Label',
                      background=self.colors['white'],
                      foreground='#000000',
                      font=self.fonts['bold'])
        
        # Buttons - Primary (subtle, professional look)
        style.configure('Primary.TButton',
                       background='#f3f4f6',  # Very light grey background
                       foreground='#1f2937',  # Dark text
                       borderwidth=0,  # No visible border
                       relief='flat',  # Flat look
                       padding=[8, 6],  # Comfortable padding
                       font=self.fonts['standard'])
        style.map('Primary.TButton',
                 background=[('active', '#e5e7eb'),  # Subtle hover
                            ('pressed', '#d1d5db'),
                            ('disabled', '#f9fafb'),  # Very light when disabled
                            ('!disabled', '#f3f4f6')],
                 foreground=[('active', '#111827'),
                           ('pressed', '#000000'),
                           ('disabled', '#9ca3af'),  # Grey text when disabled
                           ('!disabled', '#1f2937')],
                 bordercolor=[('', ''),  # No border
                             ('disabled', '')],
                 focuscolor=[('', '')])
        
        # Buttons - Secondary (same subtle style as Primary)
        style.configure('Secondary.TButton',
                       background='#f3f4f6',  # Very light grey background
                       foreground='#1f2937',  # Dark text
                       borderwidth=0,  # No visible border
                       relief='flat',  # Flat look
                       padding=[8, 6],  # Comfortable padding
                       font=self.fonts['standard'])
        style.map('Secondary.TButton',
                 background=[('active', '#e5e7eb'),  # Subtle hover
                            ('pressed', '#d1d5db'),
                            ('disabled', '#f9fafb'),  # Very light when disabled
                            ('!disabled', '#f3f4f6')],
                 foreground=[('active', '#111827'),
                           ('pressed', '#000000'),
                           ('disabled', '#9ca3af'),  # Grey text when disabled
                           ('!disabled', '#1f2937')],
                 bordercolor=[('', ''),  # No border
                             ('disabled', '')],
                 focuscolor=[('', '')])
        
        # Buttons - Danger (same subtle style, slightly different color)
        style.configure('Danger.TButton',
                       background='#fef2f2',  # Very light red tint
                       foreground='#991b1b',  # Dark red text
                       borderwidth=0,  # No visible border
                       relief='flat',  # Flat look
                       padding=[8, 6],  # Comfortable padding
                       font=self.fonts['standard'])
        style.map('Danger.TButton',
                 background=[('active', '#fee2e2'),  # Subtle hover
                            ('pressed', '#fecaca'),
                            ('disabled', '#fef2f2'),  # Very light when disabled
                            ('!disabled', '#fef2f2')],
                 foreground=[('active', '#7f1d1d'),
                           ('pressed', '#000000'),
                           ('disabled', '#9ca3af'),  # Grey text when disabled
                           ('!disabled', '#991b1b')],
                 bordercolor=[('', ''),  # No border
                             ('disabled', '')],
                 focuscolor=[('', '')])
        
        # Entry/Input fields - subtle borders
        style.configure('TEntry',
                       fieldbackground=self.colors['white'],
                       foreground='#000000',
                       borderwidth=0,
                       relief='flat',
                       padding=[6, 8],
                       font=self.fonts['standard'])
        style.map('TEntry',
                 fieldbackground=[('focus', self.colors['white']),
                                 ('!focus', self.colors['white'])],
                 bordercolor=[('focus', self.colors['border_light']),
                             ('!focus', self.colors['border_light'])],
                 focuscolor=[('', '')],
                 highlightcolor=[('focus', self.colors['border_gray']),
                                ('!focus', self.colors['border_light'])],
                 highlightthickness=[('focus', 1),
                                    ('!focus', 0)])
        
        style.configure('Modern.TEntry',
                       fieldbackground=self.colors['white'],
                       foreground='#000000',
                       borderwidth=0,
                       relief='flat',
                       padding=[6, 8],
                       font=self.fonts['standard'])
        style.map('Modern.TEntry',
                 fieldbackground=[('focus', self.colors['white']),
                                 ('!focus', self.colors['white'])],
                 bordercolor=[('focus', self.colors['border_light']),
                             ('!focus', self.colors['border_light'])],
                 focuscolor=[('', '')],
                 highlightcolor=[('focus', self.colors['border_gray']),
                                ('!focus', self.colors['border_light'])],
                 highlightthickness=[('focus', 1),
                                    ('!focus', 0)])
        
        # Combobox - subtle borders
        style.configure('TCombobox',
                       fieldbackground=self.colors['white'],
                       foreground='#000000',
                       borderwidth=0,
                       relief='flat',
                       padding=[4, 6],
                       font=self.fonts['standard'],
                       arrowcolor='#000000')
        style.map('TCombobox',
                 fieldbackground=[('readonly', self.colors['white']),
                                ('!readonly', self.colors['white']),
                                ('focus', self.colors['white']),
                                ('!focus', self.colors['white'])],
                 foreground=[('readonly', '#000000'),
                            ('!readonly', '#000000')],
                 bordercolor=[('focus', self.colors['border_light']),
                             ('!focus', self.colors['border_light'])],
                 arrowcolor=[('', '#000000')],
                 focuscolor=[('', '')],
                 highlightcolor=[('focus', self.colors['border_gray']),
                                ('!focus', self.colors['border_light'])],
                 highlightthickness=[('focus', 1),
                                    ('!focus', 0)])
        
        style.configure('Modern.TCombobox',
                       fieldbackground=self.colors['white'],
                       foreground='#000000',
                       borderwidth=0,
                       relief='flat',
                       padding=[4, 6],
                       font=self.fonts['standard'],
                       arrowcolor='#000000')
        style.map('Modern.TCombobox',
                 fieldbackground=[('readonly', self.colors['white']),
                                ('!readonly', self.colors['white']),
                                ('focus', self.colors['white']),
                                ('!focus', self.colors['white'])],
                 foreground=[('readonly', '#000000'),
                            ('!readonly', '#000000')],
                 bordercolor=[('focus', self.colors['border_light']),
                             ('!focus', self.colors['border_light'])],
                 arrowcolor=[('', '#000000')],
                 focuscolor=[('', '')],
                 highlightcolor=[('focus', self.colors['border_gray']),
                                ('!focus', self.colors['border_light'])],
                 highlightthickness=[('focus', 1),
                                    ('!focus', 0)])
        
        # Progress bar - green color
        style.configure('TProgressbar',
                       background='#10b981',  # Green color
                       troughcolor=self.colors['light_gray'],
                       borderwidth=0,
                       relief='flat',
                       thickness=6)
        
        style.configure('Modern.Horizontal.TProgressbar',
                       background='#10b981',  # Green color
                       troughcolor=self.colors['light_gray'],
                       borderwidth=0,
                       relief='flat',
                       thickness=6)
        
        # Scrollbar
        style.configure('TScrollbar',
                       background=self.colors['background_gray'],
                       troughcolor=self.colors['background_gray'],
                       borderwidth=0,
                       arrowcolor=self.colors['medium_gray'],
                       darkcolor=self.colors['background_gray'],
                       lightcolor=self.colors['background_gray'])
        style.map('TScrollbar',
                 background=[('active', self.colors['light_gray']),
                            ('!active', self.colors['light_gray'])],
                 arrowcolor=[('active', self.colors['medium_gray']),
                            ('!active', self.colors['medium_gray'])],
                 darkcolor=[('', self.colors['background_gray'])],
                 lightcolor=[('', self.colors['background_gray'])])
        
        # PanedWindow
        style.configure('TPanedwindow', background=self.colors['background_gray'])
        style.map('TPanedwindow', background=[('', self.colors['background_gray'])])
    
    def load_first_documentation(self):
        """Load first available documentation file"""
        if hasattr(self, 'docs') and self.docs and hasattr(self, 'doc_var'):
            first_doc = sorted(self.docs.keys())[0]
            self.doc_var.set(first_doc)
            self.on_doc_selected()
        
    def setup_ui(self):
        """Setup the user interface"""
        # Setup styles first
        self.setup_styles()
        
        # Create header bar - dark gray background
        header_frame = tk.Frame(self.root, bg=self.colors['dark_gray'], height=50)
        header_frame.pack(fill=tk.X, side=tk.TOP)
        header_frame.pack_propagate(False)
        
        # Header title - white text on dark gray
        title_label = tk.Label(header_frame,
                              text="Scraper Management System",
                              bg=self.colors['dark_gray'],
                              fg=self.colors['white'],
                              font=self.fonts['bold'],
                              pady=8,
                              padx=15)
        title_label.pack(side=tk.LEFT)
        
        # Auto-restart status icon in top right (always ON)
        auto_restart_frame = tk.Frame(header_frame, bg=self.colors['dark_gray'])
        auto_restart_frame.pack(side=tk.RIGHT, padx=15, pady=8)
        
        # Auto-restart icon and label (always ON - no toggle)
        self.auto_restart_icon_label = tk.Label(
            auto_restart_frame,
            text="🔄",  # Refresh/reload icon
            bg=self.colors['dark_gray'],
            fg=self.colors['console_yellow'],
            font=('Segoe UI', 14, 'normal'),
            cursor="hand2"
        )
        self.auto_restart_icon_label.pack(side=tk.LEFT, padx=(0, 5))
        self.auto_restart_icon_label.bind("<Button-1>", lambda e: self._toggle_auto_restart_from_header())

        # Auto-restart status text
        self.auto_restart_status_label = tk.Label(
            auto_restart_frame,
            text="Auto-restart: ON (20 min)",
            bg=self.colors['dark_gray'],
            fg=self.colors['white'],
            font=self.fonts['small'],
            cursor="hand2"
        )
        self.auto_restart_status_label.pack(side=tk.LEFT)
        self.auto_restart_status_label.bind("<Button-1>", lambda e: self._toggle_auto_restart_from_header())
        
        # Telegram Bot Link next to auto-restart
        telegram_link = tk.Label(
            auto_restart_frame,
            text="📱 Telegram Bot",
            bg=self.colors['dark_gray'],
            fg='#10b981',  # Emerald green color
            font=self.fonts['small'],
            cursor='hand2'
        )
        telegram_link.pack(side=tk.LEFT, padx=(15, 0))
        
        # Bind click to open Telegram bot
        telegram_link.bind("<Button-1>", lambda e: self._open_telegram_bot())

        # API Server toggle
        self.api_toggle_label = tk.Label(
            auto_restart_frame,
            text="API: OFF",
            bg=self.colors['dark_gray'],
            fg=self.colors['light_gray'],
            font=self.fonts['small'],
            cursor='hand2'
        )
        self.api_toggle_label.pack(side=tk.LEFT, padx=(15, 0))
        self.api_toggle_label.bind("<Button-1>", lambda e: self._toggle_api_server())

        # Note: Icon state will be updated after all UI setup is complete

        # Create ticker tape frame - positioned below header
        ticker_frame = tk.Frame(self.root, bg=self.colors['medium_gray'], height=30)
        ticker_frame.pack(fill=tk.X, side=tk.TOP)
        ticker_frame.pack_propagate(False)

        # Ticker tape label for scrolling text
        self.ticker_label = tk.Label(ticker_frame,
                                     text="",
                                     bg=self.colors['medium_gray'],
                                     fg=self.colors['console_yellow'],
                                     font=self.fonts['monospace'],
                                     anchor='w')
        self.ticker_label.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        # Start ticker animation
        self.start_ticker_animation()

        # Create main container for the top-level pages
        main_container = tk.Frame(self.root, bg=self.colors['white'])
        main_container.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        # Top-level notebook pages
        self.main_notebook = ttk.Notebook(main_container)
        self.main_notebook.pack(fill=tk.BOTH, expand=True)

        # Dashboard page (execution + console)
        dashboard_frame = ttk.Frame(self.main_notebook)
        self.main_notebook.add(dashboard_frame, text="Dashboard")
        self.setup_dashboard_page(dashboard_frame)

        # Input Management page (CSV to PostgreSQL input tables + PCID mapping)
        input_mgmt_frame = ttk.Frame(self.main_notebook)
        self.main_notebook.add(input_mgmt_frame, text="Input")
        self.setup_input_management_tab(input_mgmt_frame)

        # Output page (DB table browser)
        output_frame = ttk.Frame(self.main_notebook)
        self.main_notebook.add(output_frame, text="Output")
        self.setup_output_tab_db(output_frame)

        # Configuration page
        config_frame = ttk.Frame(self.main_notebook)
        self.main_notebook.add(config_frame, text="Configuration")
        self.setup_config_tab(config_frame)

        # Documentation page
        documentation_frame = ttk.Frame(self.main_notebook)
        self.main_notebook.add(documentation_frame, text="Documentation")
        self.setup_documentation_tab(documentation_frame)

        # Monitoring page (Pipeline, Health Check, Run Metrics, Prometheus, Proxy Pool, Frontier Queue, Geo Router, Selector Healer)
        monitoring_frame = ttk.Frame(self.main_notebook)
        self.main_notebook.add(monitoring_frame, text="Monitoring")
        self.setup_monitoring_tab(monitoring_frame)
        
        # Initialize auto-restart header icon state (after all UI is set up)
        # Use after() to ensure this runs after the method is fully defined
        self.root.after(100, self._initialize_auto_restart_icon)
        
        # Initialize Prometheus metrics server (if available)
        self.root.after(200, self._init_prometheus_server)

    def _discover_health_check_scripts(self) -> dict[str, Path]:
        """Locate health_check scripts for enabled scrapers."""
        result = {}
        for scraper_name, scraper_info in self.scrapers.items():
            script_path = scraper_info["path"] / "health_check.py"
            if script_path.exists():
                result[scraper_name] = script_path
        return result

    def setup_dashboard_page(self, parent):
        """Setup dashboard page with execution controls and console"""
        # Create main container with fixed widths (no resizing)
        # Fixed widths: 17% (exec controls) + 43% (log status controls) = 60% left, 40% right
        screen_width = self.root.winfo_screenwidth()
        dashboard_container = tk.Frame(parent, bg=self.colors['white'])
        dashboard_container.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        # Left panel - Execution controls + log status controls - 60% total (17% + 43%)
        left_panel = ttk.Frame(dashboard_container)
        left_panel.configure(style='TFrame')
        left_panel.pack(side=tk.LEFT, fill=tk.BOTH, expand=False)
        left_panel.config(width=int(screen_width * 0.60))
        left_panel.pack_propagate(False)

        # Right panel - Console (full height) - 40%
        right_panel = ttk.Frame(dashboard_container)
        right_panel.configure(style='TFrame')
        right_panel.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Setup left panel (execution)
        self.setup_left_panel(left_panel)

        # Setup right panel (console only)
        self.setup_console_panel(right_panel)

    # setup_outputs_page removed — Output and Configuration are now top-level tabs
        
    def setup_left_panel(self, parent):
        """Setup left panel with execution controls and log status controls"""
        # Create fixed-width split for execution and log status controls (no resizing)
        # Fixed widths: 17% (exec controls) + 43% (log status controls) = 60% total of window
        exec_split = tk.Frame(parent, bg=self.colors['white'])
        exec_split.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Get screen width for fixed width calculations
        screen_width = self.root.winfo_screenwidth()
        
        # Left side - Execution controls - 17% of total window (fixed width)
        exec_controls_frame = ttk.Frame(exec_split)
        exec_controls_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=False)
        exec_controls_frame.config(width=int(screen_width * 0.17))
        exec_controls_frame.pack_propagate(False)
        self.setup_execution_tab(exec_controls_frame)
        
        # Right side - System Status + Execution Status - 43% of total window (fixed width)
        log_controls_frame = tk.Frame(exec_split, bg=self.colors['white'])
        log_controls_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=False)
        log_controls_frame.config(width=int(screen_width * 0.43))
        log_controls_frame.pack_propagate(False)
        self.setup_log_status_panel(log_controls_frame)
    
    def setup_execution_tab(self, parent):
        """Setup execution control panel"""
        # Scraper selection - light gray border
        scraper_section = tk.Frame(parent, bg=self.colors['white'],
                                   highlightbackground=self.colors['border_gray'],
                                   highlightthickness=1)
        scraper_section.pack(fill=tk.X, padx=8, pady=(6, 6))
        
        tk.Label(scraper_section, text="Select Scraper", bg=self.colors['white'], fg='#000000', 
                font=self.fonts['bold']).pack(anchor=tk.W, padx=8, pady=(6, 3))
        
        self.scraper_var = tk.StringVar()
        scraper_combo = ttk.Combobox(scraper_section, textvariable=self.scraper_var, 
                                     values=list(self.scrapers.keys()), state="readonly",
                                     style='Modern.TCombobox')
        scraper_combo.pack(fill=tk.X, expand=True, padx=8, pady=(0, 3))
        scraper_combo.bind("<<ComboboxSelected>>", self.on_scraper_selected)
        # Ensure UI state updates even on programmatic changes
        try:
            self.scraper_var.trace_add("write", lambda *_: self.refresh_run_button_state())
        except Exception:
            pass
        
        # Network info label - shows Tor/VPN/Direct and IP
        self.network_info_label = tk.Label(scraper_section, 
                                          text="Network: Select a scraper",
                                          bg=self.colors['white'],
                                          fg='#666666',
                                          font=self.fonts['small'],
                                          anchor=tk.W)
        self.network_info_label.pack(fill=tk.X, padx=8, pady=(0, 6))

        # Pipeline control - light gray border
        pipeline_section = tk.Frame(parent, bg=self.colors['white'],
                                    highlightbackground=self.colors['border_gray'],
                                    highlightthickness=1)
        pipeline_section.pack(fill=tk.X, padx=8, pady=(0, 6))
        
        tk.Label(pipeline_section, text="Pipeline Control", bg=self.colors['white'], fg='#000000', 
                font=self.fonts['bold']).pack(anchor=tk.W, padx=8, pady=(6, 3))

        # Status labels - in pipeline section
        status_frame = tk.Frame(pipeline_section, bg=self.colors['white'])
        status_frame.pack(fill=tk.X, padx=8, pady=(0, 8))
        
        # Checkpoint status label (Line 1) - black text
        self.checkpoint_status_label = tk.Label(status_frame, 
                                               text="Checkpoint: Not checked",
                                               bg=self.colors['white'],
                                               fg='#000000',
                                               font=self.fonts['standard'],
                                               anchor=tk.W)
        self.checkpoint_status_label.pack(fill=tk.X, pady=(0, 3), padx=0)
        
        # Checkpoint resume info label (Line 2)
        self.checkpoint_resume_label = tk.Label(status_frame, 
                                               text="",
                                               bg=self.colors['white'],
                                               fg='#000000',
                                               font=self.fonts['standard'],
                                               anchor=tk.W)
        self.checkpoint_resume_label.pack(fill=tk.X, pady=(0, 3), padx=0)
        
        # Chrome instance count label (Line 3)
        self.chrome_count_label = tk.Label(status_frame, 
                                         text="Chrome Instances: 0",
                                         bg=self.colors['white'],
                                         fg='#000000',
                                         font=self.fonts['standard'],
                                         anchor=tk.W)
        self.chrome_count_label.pack(fill=tk.X, pady=(0, 3), padx=0)

        # Timeline status label (Line 4)
        self.timeline_status_label = tk.Label(
            status_frame,
            text="Timeline: Not checked",
            bg=self.colors['white'],
            fg='#000000',
            font=self.fonts['standard'],
            anchor=tk.W
        )
        self.timeline_status_label.pack(fill=tk.X, pady=(0, 3), padx=0)

        # Checkpoint tools
        checkpoint_tools = tk.Frame(pipeline_section, bg=self.colors['white'])
        checkpoint_tools.pack(fill=tk.X, padx=8, pady=(0, 8))
        ttk.Button(
            checkpoint_tools,
            text="View Checkpoint",
            command=self.view_checkpoint_file,
            style='Secondary.TButton'
        ).pack(fill=tk.X, pady=(0, 3))
        ttk.Button(
            checkpoint_tools,
            text="Manage Checkpoint",
            command=self.manage_checkpoint,
            style='Secondary.TButton'
        ).pack(fill=tk.X, pady=(0, 3))
        ttk.Button(
            checkpoint_tools,
            text="Clear Checkpoint",
            command=self.clear_checkpoint,
            style='Danger.TButton'
        ).pack(fill=tk.X)

        # Actions - flat container (avoid dark border)
        actions_section = tk.Frame(parent, bg=self.colors['white'],
                                   highlightthickness=0,
                                   bd=0)
        actions_section.pack(fill=tk.X, padx=8, pady=(0, 6))
        
        tk.Label(actions_section, text="Actions", bg=self.colors['white'], fg='#000000', 
                font=self.fonts['bold']).pack(anchor=tk.W, padx=8, pady=(6, 3))

        self.run_button = ttk.Button(actions_section, text="Resume Pipeline",
                  command=lambda: self.run_full_pipeline(resume=True), width=23, 
                  state=tk.NORMAL, style='Primary.TButton')
        self.run_button.pack(pady=(0, 3), padx=8, fill=tk.X, expand=True)

        self.run_fresh_button = ttk.Button(actions_section, text="Run Fresh Pipeline",
                  command=lambda: self.run_full_pipeline(resume=False), width=23, 
                  state=tk.NORMAL, style='Primary.TButton')
        self.run_fresh_button.pack(pady=(0, 3), padx=8, fill=tk.X, expand=True)

        self.stop_button = ttk.Button(actions_section, text="Stop Pipeline",
                  command=self.stop_pipeline, width=23, state=tk.DISABLED, style='Danger.TButton')
        self.stop_button.pack(pady=(0, 3), padx=8, fill=tk.X, expand=True)

        ttk.Button(actions_section, text="Clear Run Lock",
                  command=self.clear_run_lock, width=23, style='Secondary.TButton').pack(pady=(0, 3), padx=8, fill=tk.X, expand=True)
        
        self.kill_all_chrome_button = ttk.Button(actions_section, text="Kill All Chrome Instances",
                  command=self.kill_all_chrome_instances, width=23, state=tk.NORMAL, style='Secondary.TButton')
        self.kill_all_chrome_button.pack(pady=(0, 3), padx=8, fill=tk.X, expand=True)

        # Run Validation Table button (shows detailed progress for current scraper)
        self.validation_table_button = ttk.Button(actions_section, text="Run Validation Table",
                  command=self.show_validation_table, width=23, state=tk.NORMAL, style='Secondary.TButton')
        self.validation_table_button.pack(pady=(0, 6), padx=8, fill=tk.X, expand=True)

        # Detailed run/state timeline viewer (API-backed with local fallback)
        self.timeline_view_button = ttk.Button(
            actions_section,
            text="View State Timeline",
            command=self.show_state_timeline,
            width=23,
            state=tk.NORMAL,
            style='Secondary.TButton'
        )
        self.timeline_view_button.pack(pady=(0, 6), padx=8, fill=tk.X, expand=True)

        # Auto-restart is always ON - no toggle needed
        # The auto-restart feature runs automatically every 20 minutes to clear cache/memory
        self._auto_restart_enabled = tk.BooleanVar(value=True)  # Always True

    def _get_selected_scraper_for_data_reset(self) -> Optional[str]:
        """Return the scraper name for data reset actions."""
        if getattr(self, "_data_reset_use_output", False) and hasattr(self, "output_scraper_var"):
            scraper = self.output_scraper_var.get()
            if scraper:
                return scraper
        if hasattr(self, "scraper_var"):
            return self.scraper_var.get()
        return None

    def setup_data_reset_section(self, parent):
        """Setup data reset controls."""
        reset_frame = tk.Frame(parent, bg=self.colors['white'])
        reset_frame.pack(fill=tk.X, padx=8, pady=(0, 6))

        tk.Label(reset_frame, text="Data Reset", bg=self.colors['white'],
                 fg='#000000', font=self.fonts['bold']).pack(anchor=tk.W, pady=(0, 3))

        chooser = tk.Frame(reset_frame, bg=self.colors['white'])
        chooser.pack(fill=tk.X, pady=(0, 4))

        tk.Label(chooser, text="Step:", bg=self.colors['white'],
                 fg='#000000', font=self.fonts['standard']).pack(side=tk.LEFT)

        self.clear_step_var = tk.StringVar(value="1")
        self.clear_step_combo = ttk.Combobox(chooser, textvariable=self.clear_step_var,
                                             values=["1", "2", "3", "4", "5"],
                                             state="readonly", width=5,
                                             style='Modern.TCombobox')
        self.clear_step_combo.pack(side=tk.LEFT, padx=(6, 12))

        self.clear_downstream_var = tk.BooleanVar(value=False)
        self.clear_downstream_check = ttk.Checkbutton(
            chooser, text="Include downstream",
            variable=self.clear_downstream_var,
            style='Secondary.TCheckbutton')
        self.clear_downstream_check.pack(side=tk.LEFT)

        self.clear_step_button = ttk.Button(reset_frame, text="Clear Step Data",
                  command=self.clear_step_data_action, width=23,
                  style='Danger.TButton')
        self.clear_step_button.pack(pady=(4, 0), fill=tk.X)
        # Disabled by default; enabled when a supported scraper is selected
        self.clear_step_combo.config(state=tk.DISABLED)
        self.clear_step_button.config(state=tk.DISABLED)
        self.clear_downstream_check.state(["disabled"])

    def update_reset_controls(self, scraper_name: str):
        """Enable data-reset controls if clear_step_data.py exists for the scraper."""
        if not hasattr(self, "clear_step_combo"):
            return
        scraper_info = self.scrapers.get(scraper_name, {})
        script_path = scraper_info.get("path", self.repo_root / "scripts" / scraper_name) / "clear_step_data.py"

        # Populate step combo values based on defined steps (1-based)
        steps = scraper_info.get("steps", [])
        step_numbers = [str(i + 1) for i, _ in enumerate(steps)] or ["1", "2", "3", "4", "5"]
        self.clear_step_combo["values"] = step_numbers
        if step_numbers:
            self.clear_step_var.set(step_numbers[0])

        if script_path.exists():
            self.clear_step_combo.config(state="readonly")
            self.clear_step_button.config(state=tk.NORMAL)
            self.clear_downstream_check.state(["!disabled"])
        else:
            self.clear_step_combo.config(state=tk.DISABLED)
            self.clear_step_button.config(state=tk.DISABLED)
            self.clear_downstream_check.state(["disabled"])
    
    def setup_pipeline_steps_tab(self, parent, compact: bool = False):
        """Setup Pipeline Steps tab with step list, info, and explanation"""
        selector_frame = tk.Frame(parent, bg=self.colors['white'])
        selector_frame.pack(fill=tk.X, padx=8, pady=(8, 0))

        tk.Label(selector_frame, text="Select Scraper for Pipeline Steps",
                 bg=self.colors['white'], fg='#000000',
                 font=self.fonts['standard']).pack(side=tk.LEFT, padx=(0, 8))

        self.pipeline_steps_scraper_var = tk.StringVar()
        self.pipeline_steps_combo = ttk.Combobox(
            selector_frame,
            textvariable=self.pipeline_steps_scraper_var,
            values=list(self.scrapers.keys()),
            state="readonly",
            style='Modern.TCombobox',
            width=(self.MONITOR_COMBO_WIDTH if compact else 26)
        )
        if compact:
            self.pipeline_steps_combo.pack(side=tk.LEFT, padx=(0, 0))
        else:
            self.pipeline_steps_combo.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.pipeline_steps_combo.bind("<<ComboboxSelected>>", self.on_pipeline_steps_scraper_selected)
        if self.scrapers:
            first_scraper = next(iter(self.scrapers))
            self.pipeline_steps_scraper_var.set(first_scraper)

        # Steps listbox with scrollbar - container frame with light gray border
        listbox_container = tk.Frame(parent, bg=self.colors['white'],
                                     highlightbackground=self.colors['border_gray'],
                                     highlightthickness=1)
        listbox_container.pack(fill=tk.BOTH, expand=False, padx=8, pady=(8, 8))
        
        scrollbar = ttk.Scrollbar(listbox_container)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.steps_listbox = tk.Listbox(listbox_container, yscrollcommand=scrollbar.set,
                                        height=15, font=self.fonts['monospace'],  # Monospace
                                        bg=self.colors['white'],
                                        fg='#000000',
                                        selectbackground=self.colors['white'],
                                        selectforeground='#000000',
                                        borderwidth=0,
                                        relief='flat',
                                        highlightthickness=0,
                                        activestyle='none')
        self.steps_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.steps_listbox.yview)

        self.steps_listbox.bind("<<ListboxSelect>>", self.on_step_selected)

        # Step info text container - with light gray border
        step_info_container = tk.Frame(parent, bg=self.colors['white'],
                                       highlightbackground=self.colors['border_gray'],
                                       highlightthickness=1)
        step_info_container.pack(fill=tk.BOTH, expand=False, padx=8, pady=(0, 8))
        
        # Step info text - below listbox, vertically stacked
        self.step_info_text = tk.Text(step_info_container, height=5, wrap=tk.WORD,
                                     font=self.fonts['monospace'], state=tk.DISABLED,
                                     bg=self.colors['white'],
                                     fg='#000000',
                                     borderwidth=0,
                                     relief='flat',
                                     highlightthickness=0,
                                     padx=12,
                                     pady=12)
        self.step_info_text.pack(fill=tk.BOTH, expand=True)
        
        self.reset_step_info_text()
        
        # Explain button - below step info, vertically stacked
        button_container = tk.Frame(parent, bg=self.colors['white'])
        button_container.pack(fill=tk.X, padx=8, pady=(0, 8))
        
        self.explain_button = ttk.Button(button_container, 
                                         text="Explain This Step",
                                         command=self.explain_step, 
                                         state=tk.NORMAL,
                                         style='Primary.TButton')
        self.explain_button.pack(side=tk.LEFT, padx=(0, 8), pady=0)
        
        ttk.Button(button_container, 
                   text="Refresh Status",
                   command=lambda: self.refresh_pipeline_steps_list(self.pipeline_steps_scraper_var.get()),
                   style='Secondary.TButton').pack(side=tk.LEFT, padx=0, pady=0)

        # Explanation panel - with light gray border
        self.exec_controls_parent = parent
        self.explanation_frame = tk.Frame(parent, bg=self.colors['white'],
                                          highlightbackground=self.colors['border_gray'],
                                          highlightthickness=1)
        # Don't pack initially - will pack when explanation is shown

        self.explanation_text = scrolledtext.ScrolledText(
            self.explanation_frame,
            wrap=tk.WORD,
            font=self.fonts['standard'],
            state=tk.DISABLED,
            height=8,  # Initial height, will expand
            bg=self.colors['white'],
            fg='#000000',
            borderwidth=0,
            relief='flat',
            highlightthickness=0,
            padx=16,
            pady=16
        )
        self.explanation_text.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        
        self.explanation_visible = False
        self.refresh_pipeline_steps_list(self.pipeline_steps_scraper_var.get())
    
    def on_pipeline_steps_scraper_selected(self, event=None):
        """Handle scraper selection specifically for the pipeline steps tab."""
        scraper_name = self.pipeline_steps_scraper_var.get()
        self.refresh_pipeline_steps_list(scraper_name)

    def refresh_pipeline_steps_list(self, scraper_name: str):
        """Populate the pipeline steps listbox for the selected scraper."""
        if not scraper_name:
            self.steps_listbox.delete(0, tk.END)
            self.reset_step_info_text()
            return

        scraper_info = self.scrapers.get(scraper_name)
        if not scraper_info:
            self.steps_listbox.delete(0, tk.END)
            self.reset_step_info_text("No pipeline steps defined for this scraper.")
            return

        self.pipeline_steps_scraper_var.set(scraper_name)
        self.steps_listbox.delete(0, tk.END)
        
        # Get status from DB for Netherlands and NorthMacedonia
        statuses = {}
        if scraper_name in ["Netherlands", "NorthMacedonia"]:
            try:
                from core.db.postgres_connection import get_db
                db = get_db(scraper_name)

                # Determine table prefix
                prefix = "nl_" if scraper_name == "Netherlands" else "nm_"

                with db.cursor() as cur:
                    # Get latest run with progress
                    cur.execute(f"SELECT run_id FROM {prefix}step_progress ORDER BY id DESC LIMIT 1")
                    run_id_row = cur.fetchone()
                    if run_id_row:
                        run_id = run_id_row[0]
                        cur.execute(f"SELECT step_number, status FROM {prefix}step_progress WHERE run_id = %s", (run_id,))
                        statuses = {row[0]: row[1] for row in cur.fetchall()}
            except Exception:
                pass

        for i, step in enumerate(scraper_info["steps"]):
            name = step["name"]
            if scraper_name in ["Netherlands", "NorthMacedonia"] and i in statuses:
                status = statuses[i]
                icon = "○"
                if status == 'completed': icon = "✓"
                elif status == 'failed': icon = "✗"
                elif status == 'in_progress': icon = "↻"
                elif status == 'skipped': icon = "→"
                name = f"[{icon}] {name}"
            self.steps_listbox.insert(tk.END, name)
            
        self.current_step = None
        self.reset_step_info_text()

    def reset_step_info_text(self, message: Optional[str] = None):
        """Reset the step info text area to the default placeholder."""
        default_msg = message or "Select a step from the list above to see details and get AI explanation."
        self.step_info_text.config(state=tk.NORMAL)
        self.step_info_text.delete(1.0, tk.END)
        self.step_info_text.insert(1.0, default_msg)
        self.step_info_text.config(state=tk.DISABLED)
        
    def setup_health_check_tab(self, parent):
        """Setup the health check manual tab for scraper diagnostics"""
        container = tk.Frame(parent, bg=self.colors['white'])
        container.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        tk.Label(
            container,
            text="Manual Health Check",
            bg=self.colors['white'],
            fg='#111827',
            font=self.fonts['bold']
        ).pack(anchor=tk.W, pady=(0, 4))

        tk.Label(
            container,
            text="Use this tab to confirm the key configuration and website selectors before running a pipeline.",
            bg=self.colors['white'],
            fg='#1f2937',
            font=self.fonts['standard'],
            wraplength=600,
            justify=tk.LEFT
        ).pack(anchor=tk.W, pady=(0, 8))

        controls_frame = tk.Frame(container, bg=self.colors['white'])
        controls_frame.pack(fill=tk.X, pady=(0, 8))

        tk.Label(controls_frame, text="Select Scraper", bg=self.colors['white'], fg='#111827',
                 font=self.fonts['standard']).grid(row=0, column=0, sticky=tk.W, padx=(0, 8), pady=4)

        self.health_check_scraper_var = tk.StringVar()
        values = sorted(self.health_check_scripts.keys())
        self.health_check_combo = ttk.Combobox(
            controls_frame,
            textvariable=self.health_check_scraper_var,
            values=values,
            state="readonly",
            width=self.MONITOR_COMBO_WIDTH,
            style='Modern.TCombobox'
        )
        self.health_check_combo.grid(row=0, column=1, sticky=tk.W, pady=4)
        controls_frame.columnconfigure(1, weight=0)
        if values:
            self.health_check_scraper_var.set(values[0])

        self.health_check_status_var = tk.StringVar(value="Select a scraper above and press Run Health Check.")
        tk.Label(
            container,
            textvariable=self.health_check_status_var,
            bg=self.colors['white'],
            fg='#1f2937',
            font=self.fonts['standard'],
            wraplength=700,
            justify=tk.LEFT
        ).pack(anchor=tk.W, padx=(0, 0), pady=(0, 8))

        actions_frame = tk.Frame(container, bg=self.colors['white'])
        actions_frame.pack(fill=tk.X, pady=(0, 8))

        self.health_check_run_button = ttk.Button(
            actions_frame,
            text="Run Health Check",
            command=self.start_health_check,
            style='Primary.TButton'
        )
        self.health_check_run_button.pack(side=tk.LEFT, padx=(0, 8))

        ttk.Button(
            actions_frame,
            text="Clear Log",
            command=self.clear_health_check_log,
            style='Secondary.TButton'
        ).pack(side=tk.LEFT)

        table_frame = tk.Frame(container, bg=self.colors['white'],
                               highlightbackground=self.colors['border_gray'],
                               highlightthickness=1)
        table_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 8))

        self.health_check_table = ttk.Treeview(
            table_frame,
            columns=("step", "check", "status", "detail"),
            show="headings",
            selectmode="none",
            height=6
        )
        self.health_check_table.pack(fill=tk.BOTH, expand=True)
        self.health_check_table.heading("step", text="Step")
        self.health_check_table.heading("check", text="Check")
        self.health_check_table.heading("status", text="Status")
        self.health_check_table.heading("detail", text="Detail")
        self.health_check_table.column("step", width=100, anchor=tk.W)
        self.health_check_table.column("check", width=220, anchor=tk.W)
        self.health_check_table.column("status", width=80, anchor=tk.CENTER)
        self.health_check_table.column("detail", width=420, anchor=tk.W)
        self.health_check_table.tag_configure("PASS", background="#dcfce7")
        self.health_check_table.tag_configure("FAIL", background="#fee2e2")

        log_frame = tk.Frame(container, bg=self.colors['white'],
                             highlightbackground=self.colors['border_gray'],
                             highlightthickness=1)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 8))

        self.health_check_log_text = scrolledtext.ScrolledText(
            log_frame,
            wrap=tk.WORD,
            font=self.fonts['standard'],
            state=tk.DISABLED,
            bg=self.colors['white'],
            fg='#000000',
            padx=12,
            pady=12,
            borderwidth=0,
            relief='flat'
        )
        self.health_check_log_text.pack(fill=tk.BOTH, expand=True)
        self.health_check_log_text.insert(tk.END, "Health check logs will appear here.\n")
        self.health_check_log_text.config(state=tk.DISABLED)

    def start_health_check(self):
        """Trigger the health check script for the selected scraper."""
        if self.health_check_running:
            messagebox.showwarning("Health Check Running", "Health check already in progress.")
            return
        scraper_name = self.health_check_scraper_var.get()
        script_path = self.health_check_scripts.get(scraper_name)
        if not script_path:
            messagebox.showwarning("Select Scraper", "Choose a scraper that supports health checks.")
            return

        self.health_check_running = True
        self.health_check_json_path = None
        self.health_check_run_button.config(state=tk.DISABLED)
        self.health_check_status_var.set(f"Running health check for {scraper_name}...")
        self.clear_health_check_log()
        self.clear_health_check_table()

        def worker():
            cmd = [sys.executable, str(script_path)]
            process = subprocess.Popen(
                cmd,
                cwd=str(script_path.parent),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            for line in process.stdout or []:
                self.append_health_check_log(line.rstrip())
            return_code = process.wait()
            self.root.after(0, lambda: self.health_check_complete(scraper_name, return_code))

        threading.Thread(target=worker, daemon=True).start()

    def append_health_check_log(self, message):
        """Append a line to the health check log text field."""
        def update():
            self.health_check_log_text.config(state=tk.NORMAL)
            self.health_check_log_text.insert(tk.END, message + "\n")
            self.health_check_log_text.see(tk.END)
            self.health_check_log_text.config(state=tk.DISABLED)
        self.root.after(0, update)

        if "[HEALTH CHECK] JSON summary saved:" in message:
            # Path may contain ":" (e.g. Windows C:\...), so take text after "JSON summary saved: "
            prefix = "[HEALTH CHECK] JSON summary saved: "
            path = message[message.find(prefix) + len(prefix):].strip()
            try:
                self.health_check_json_path = Path(path)
            except Exception:
                self.health_check_json_path = None

    def health_check_complete(self, scraper_name, return_code):
        """Update UI once the health check script finishes."""
        self.health_check_running = False
        self.health_check_run_button.config(state=tk.NORMAL)
        status_text = "PASSED" if return_code == 0 else f"FAILED (exit {return_code})"
        self.health_check_status_var.set(f"Health check finished for {scraper_name}: {status_text}")
        if self.health_check_json_path:
            self.populate_health_check_table(self.health_check_json_path)

    def populate_health_check_table(self, json_path: Path):
        """Load JSON summary and show in table."""
        if not json_path.exists():
            self.append_health_check_log(f"[HEALTH CHECK UI] Summary file missing: {json_path}")
            return
        try:
            with open(json_path, "r", encoding="utf-8") as fh:
                items = json.load(fh)
        except Exception as exc:
            self.append_health_check_log(f"[HEALTH CHECK UI] Failed to read summary: {exc}")
            return
        self.clear_health_check_table()
        for row in items:
            status = row.get("status", "").upper()
            tag = "PASS" if status == "PASS" else "FAIL"
            self.health_check_table.insert(
                "",
                "end",
                values=(
                    row.get("step", ""),
                    row.get("check", ""),
                    row.get("status", ""),
                    row.get("detail", ""),
                ),
                tags=(tag,),
            )

    def clear_health_check_log(self):
        """Clear the health check log pane."""
        self.health_check_log_text.config(state=tk.NORMAL)
        self.health_check_log_text.delete(1.0, tk.END)
        self.health_check_log_text.config(state=tk.DISABLED)

    def clear_health_check_table(self):
        """Clear the health check results table."""
        for item in self.health_check_table.get_children():
            self.health_check_table.delete(item)
    def setup_right_panel(self, parent, include_pipeline_steps=True, include_final_output=True,
                          include_config=True, include_output=True, include_docs=True):
        """Setup right panel with tabs for outputs/config and optional pipeline/docs"""
        # Create notebook for right panel tabs
        notebook = ttk.Notebook(parent)
        notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Pipeline Steps tab
        if include_pipeline_steps:
            pipeline_steps_frame = ttk.Frame(notebook)
            notebook.add(pipeline_steps_frame, text="Pipeline Steps")
            self.setup_pipeline_steps_tab(pipeline_steps_frame)
        
        # Final Output tab
        if include_final_output:
            final_output_frame = ttk.Frame(notebook)
            notebook.add(final_output_frame, text="Final Output")
            self.setup_final_output_tab(final_output_frame)

        # Configuration tab
        if include_config:
            config_frame = ttk.Frame(notebook)
            notebook.add(config_frame, text="Configuration")
            self.setup_config_tab(config_frame)

        # Input Data tab
        input_frame = ttk.Frame(notebook)
        notebook.add(input_frame, text="Input Data")
        self.setup_input_management_tab(input_frame)

        # Output Files tab
        if include_output:
            output_frame = ttk.Frame(notebook)
            notebook.add(output_frame, text="Output Files")
            self.setup_output_tab(output_frame)

        # Documentation tab
        if include_docs:
            doc_frame = ttk.Frame(notebook)
            notebook.add(doc_frame, text="Documentation")
            self.setup_documentation_tab(doc_frame)
    
    def setup_documentation_tab(self, parent):
        """Setup documentation viewer tab (read-only, formatted)"""
        # Documentation header - white background with border
        doc_header = ttk.LabelFrame(parent, text="Documentation", padding=15, style='Title.TLabelframe')
        doc_header.pack(fill=tk.X, padx=8, pady=8)

        # Documentation selector - white background
        doc_selector_frame = tk.Frame(doc_header, bg=self.colors['white'])
        doc_selector_frame.pack(fill=tk.X, pady=5)

        tk.Label(doc_selector_frame, text="Select Document:",
                bg=self.colors['white'],
                fg='#000000',
                font=self.fonts['standard']).pack(side=tk.LEFT, padx=5)

        self.doc_var = tk.StringVar()
        self.doc_combo = ttk.Combobox(doc_selector_frame, textvariable=self.doc_var,
                                      state="readonly", width=18, style='Modern.TCombobox')
        self.doc_combo.pack(side=tk.LEFT, padx=12, fill=tk.X, expand=True)
        self.doc_combo.bind("<<ComboboxSelected>>", self.on_doc_selected)

        ttk.Button(doc_selector_frame, text="Refresh", command=self.load_documentation, 
                  style='Secondary.TButton').pack(side=tk.LEFT, padx=5)
        
        # Documentation viewer (read-only, formatted) - white background with light gray border
        doc_viewer_frame = tk.Frame(parent, bg=self.colors['white'],
                                     highlightbackground=self.colors['border_gray'],
                                     highlightthickness=1)
        doc_viewer_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        
        # Use Text widget with better formatting for markdown/readable docs
        self.doc_text = scrolledtext.ScrolledText(
            doc_viewer_frame,
            wrap=tk.WORD,
            font=self.fonts['standard'],  # Body text
            state=tk.DISABLED,  # Read-only
            bg=self.colors['white'],
            fg='#000000',
            padx=24,  # Padding
            pady=24,  # Padding
            spacing1=3,  # Space above paragraphs
            spacing3=8,  # Space below paragraphs
            borderwidth=0,
            relief='flat',
            highlightthickness=0
        )
        self.doc_text.pack(fill=tk.BOTH, expand=True)

        # Configure text tags for professional formatting with black text
        self.doc_text.tag_configure("heading1", font=self.fonts['bold'], foreground='#000000', spacing1=10, spacing3=6)
        self.doc_text.tag_configure("heading2", font=self.fonts['bold'], foreground='#000000', spacing1=12, spacing3=6)
        self.doc_text.tag_configure("heading3", font=self.fonts['bold'], foreground='#000000', spacing1=8, spacing3=5)
        self.doc_text.tag_configure("heading4", font=self.fonts['bold'], foreground='#000000', spacing1=7, spacing3=4)
        self.doc_text.tag_configure("heading5", font=self.fonts['bold'], foreground='#000000', spacing1=6, spacing3=3)
        self.doc_text.tag_configure("heading6", font=self.fonts['bold'], foreground='#000000', spacing1=5, spacing3=2)
        self.doc_text.tag_configure("code", font=self.fonts['monospace'], background=self.colors['white'], foreground='#000000',
                                    relief=tk.FLAT, borderwidth=0, lmargin1=20, lmargin2=20, rmargin=20,
                                    spacing1=8, spacing3=8)
        self.doc_text.tag_configure("code_inline", font=self.fonts['monospace'], background=self.colors['white'], foreground='#000000',
                                    relief=tk.FLAT)
        self.doc_text.tag_configure("bold", font=self.fonts['bold'], foreground='#000000')
        self.doc_text.tag_configure("italic", font=self.fonts['italic'], foreground='#000000')
        self.doc_text.tag_configure("link", foreground='#000000', underline=True, font=self.fonts['standard'])
        self.doc_text.tag_configure("blockquote", foreground='#000000', lmargin1=20, lmargin2=20,
                                    background=self.colors['white'], font=self.fonts['italic'],
                                    spacing1=4, spacing3=4)
        self.doc_text.tag_configure("hr", background=self.colors['border_gray'], lmargin1=0, lmargin2=0, rmargin=0)
        self.doc_text.tag_configure("list", lmargin1=20, lmargin2=40, font=self.fonts['standard'])
        self.doc_text.tag_configure("list_item", lmargin1=20, lmargin2=40, font=self.fonts['standard'],
                                    spacing1=4, spacing3=4)
    
    # ==================================================================
    # RUN METRICS TAB
    # ==================================================================

    def setup_run_metrics_tab(self, parent):
        """Setup Run Metrics tab to view network consumption and execution time."""
        # Header
        header = ttk.LabelFrame(parent, text="Run Metrics (Network & Time)", padding=10, style='Title.TLabelframe')
        header.pack(fill=tk.X, padx=8, pady=(8, 4))

        # Controls frame
        controls = tk.Frame(header, bg=self.colors['white'])
        controls.pack(fill=tk.X)

        # Scraper selector
        tk.Label(controls, text="Scraper:", bg=self.colors['white'], fg='#000000',
                 font=self.fonts['standard']).pack(side=tk.LEFT, padx=(0, 5))

        self.metrics_scraper_var = tk.StringVar()
        self.metrics_scraper_combo = ttk.Combobox(controls, textvariable=self.metrics_scraper_var,
                                                   state="readonly", width=self.MONITOR_COMBO_WIDTH, style='Modern.TCombobox')
        self.metrics_scraper_combo['values'] = ["All"] + list(self.scrapers.keys())
        self.metrics_scraper_combo.pack(side=tk.LEFT, padx=5)
        self.metrics_scraper_combo.set("All")
        self.metrics_scraper_combo.bind("<<ComboboxSelected>>", self.on_metrics_scraper_changed)

        # Refresh button
        ttk.Button(controls, text="Refresh", command=self.load_run_metrics,
                  style='Secondary.TButton').pack(side=tk.LEFT, padx=5)

        # Export button
        ttk.Button(controls, text="Export CSV", command=self.export_run_metrics,
                  style='Secondary.TButton').pack(side=tk.LEFT, padx=5)

        # Summary frame
        summary_frame = ttk.LabelFrame(parent, text="Summary", padding=10, style='Title.TLabelframe')
        summary_frame.pack(fill=tk.X, padx=8, pady=(4, 4))

        self.metrics_summary_text = tk.Text(summary_frame, height=6, wrap=tk.WORD,
                                           font=self.fonts['standard'],
                                           bg=self.colors['white'],
                                           fg='#000000',
                                           state=tk.DISABLED,
                                           borderwidth=0,
                                           relief='flat',
                                           highlightthickness=0)
        self.metrics_summary_text.pack(fill=tk.X, expand=True)

        # Treeview frame
        tree_frame = ttk.LabelFrame(parent, text="Run History", padding=5, style='Title.TLabelframe')
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=(4, 8))

        # Create treeview with scrollbars
        tree_container = tk.Frame(tree_frame, bg=self.colors['white'])
        tree_container.pack(fill=tk.BOTH, expand=True)

        # Scrollbars
        vsb = ttk.Scrollbar(tree_container, orient="vertical")
        hsb = ttk.Scrollbar(tree_container, orient="horizontal")

        # Treeview columns
        columns = ('run_id', 'scraper', 'status', 'duration', 'network', 'started')
        self.metrics_tree = ttk.Treeview(tree_container, columns=columns, show='headings',
                                         yscrollcommand=vsb.set, xscrollcommand=hsb.set,
                                         height=15)

        vsb.config(command=self.metrics_tree.yview)
        hsb.config(command=self.metrics_tree.xview)

        # Define column headings
        self.metrics_tree.heading('run_id', text='Run ID')
        self.metrics_tree.heading('scraper', text='Scraper')
        self.metrics_tree.heading('status', text='Status')
        self.metrics_tree.heading('duration', text='Duration')
        self.metrics_tree.heading('network', text='Network (GB)')
        self.metrics_tree.heading('started', text='Started At')

        # Define column widths
        self.metrics_tree.column('run_id', width=250, minwidth=150)
        self.metrics_tree.column('scraper', width=100, minwidth=80)
        self.metrics_tree.column('status', width=80, minwidth=60)
        self.metrics_tree.column('duration', width=100, minwidth=80)
        self.metrics_tree.column('network', width=100, minwidth=80)
        self.metrics_tree.column('started', width=180, minwidth=150)

        # Grid layout
        self.metrics_tree.grid(row=0, column=0, sticky='nsew')
        vsb.grid(row=0, column=1, sticky='ns')
        hsb.grid(row=1, column=0, sticky='ew')

        tree_container.grid_rowconfigure(0, weight=1)
        tree_container.grid_columnconfigure(0, weight=1)

        # Bind double-click to show details
        self.metrics_tree.bind('<Double-1>', self.on_metrics_item_double_click)

        # Status bar
        self.metrics_status_var = tk.StringVar(value="Ready")
        status_label = tk.Label(parent, textvariable=self.metrics_status_var,
                               bg=self.colors['white'], fg='#666666',
                               font=self.fonts['small'], anchor='w')
        status_label.pack(fill=tk.X, padx=8, pady=(0, 4))

        # Load initial data
        self.load_run_metrics()

    def on_metrics_scraper_changed(self, event=None):
        """Handle scraper selection change in metrics tab."""
        self.load_run_metrics()

    def load_run_metrics(self):
        """Load and display run metrics."""
        try:
            from core.run_metrics_tracker import RunMetricsTracker
        except ImportError:
            self.metrics_status_var.set("Error: Run metrics tracker not available")
            return

        scraper_filter = self.metrics_scraper_var.get()
        if scraper_filter == "All":
            scraper_filter = None

        token = self._next_monitor_async_token("run_metrics")
        self.metrics_status_var.set("Loading run metrics...")

        def worker():
            try:
                tracker = RunMetricsTracker()
                metrics_list = tracker.list_metrics(scraper_name=scraper_filter, limit=100)
                summary = tracker.get_summary(scraper_name=scraper_filter)
                rows = []
                for m in metrics_list:
                    rows.append((
                        m.run_id,
                        m.scraper_name,
                        m.status,
                        self._format_duration(m.active_duration_seconds),
                        f"{m.network_total_gb:.4f}",
                        m.started_at[:19] if m.started_at else "N/A",
                    ))
                return {"rows": rows, "summary": summary, "count": len(metrics_list)}
            except Exception as exc:
                return {"error": f"Failed to load run metrics: {exc}"}

        def apply(payload):
            if not self._is_monitor_async_token_current("run_metrics", token):
                return
            if payload.get("error"):
                self.metrics_status_var.set(payload["error"])
                return

            for item in self.metrics_tree.get_children():
                self.metrics_tree.delete(item)
            for row in payload.get("rows", []):
                self.metrics_tree.insert('', 'end', values=row)

            summary = payload.get("summary", {})
            COST_PER_GB = 5.0
            total_network_gb = float(summary.get("total_network_gb", 0.0))
            total_cost = total_network_gb * COST_PER_GB

            summary_text = (
                f"Total Runs: {summary.get('total_runs', 0)} | "
                f"Total Duration: {self._format_duration(float(summary.get('total_duration_seconds', 0.0)))} | "
                f"Total Network: {total_network_gb:.4f} GB | "
                f"Avg Duration: {self._format_duration(float(summary.get('avg_duration_seconds', 0.0)))} | "
                f"Avg Network: {float(summary.get('avg_network_gb', 0.0)):.4f} GB\n\n"
                f"{'='*60}\n"
                f"TOTAL NETWORK CONSUMED: {total_network_gb:.4f} GB\n"
                f"ESTIMATED COST (@ ${COST_PER_GB}/GB): ${total_cost:.2f}\n"
                f"{'='*60}"
            )

            self.metrics_summary_text.config(state=tk.NORMAL)
            self.metrics_summary_text.delete('1.0', tk.END)
            self.metrics_summary_text.insert('1.0', summary_text)
            self.metrics_summary_text.config(state=tk.DISABLED)
            self.metrics_status_var.set(f"Loaded {payload.get('count', 0)} runs | Total Cost: ${total_cost:.2f}")

        def run_async():
            payload = worker()
            self.root.after(0, lambda: apply(payload))

        threading.Thread(target=run_async, daemon=True).start()

    def _format_duration(self, seconds):
        """Format duration in human-readable format."""
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f}m"
        else:
            hours = seconds / 3600
            return f"{hours:.2f}h"

    def on_metrics_item_double_click(self, event):
        """Handle double-click on metrics item - show details."""
        selection = self.metrics_tree.selection()
        if not selection:
            return

        item = selection[0]
        values = self.metrics_tree.item(item, 'values')
        run_id = values[0]

        try:
            from core.run_metrics_tracker import RunMetricsTracker
            tracker = RunMetricsTracker()
            metrics = tracker.get_metrics(run_id)

            if metrics:
                # Calculate cost for this run
                COST_PER_GB = 5.0
                run_cost = metrics.network_total_gb * COST_PER_GB
                
                # Show details in message box
                details = (f"Run ID: {metrics.run_id}\n"
                          f"Scraper: {metrics.scraper_name}\n"
                          f"Status: {metrics.status}\n\n"
                          f"Active Duration: {self._format_duration(metrics.active_duration_seconds)}\n"
                          f"Network Sent: {metrics.network_sent_mb:.2f} MB\n"
                          f"Network Received: {metrics.network_received_mb:.2f} MB\n"
                          f"Network Total: {metrics.network_total_gb:.4f} GB\n"
                          f"Estimated Cost: ${run_cost:.2f} (@ ${COST_PER_GB}/GB)\n\n"
                          f"Started: {metrics.started_at}\n"
                          f"Ended: {metrics.ended_at or 'N/A'}")

                messagebox.showinfo("Run Details", details)
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load run details: {e}")

    def export_run_metrics(self):
        """Export run metrics to CSV file."""
        try:
            from core.run_metrics_tracker import RunMetricsTracker
            import csv
        except ImportError:
            messagebox.showerror("Error", "Run metrics tracker not available")
            return

        # Ask for file path
        file_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            title="Export Run Metrics"
        )

        if not file_path:
            return

        try:
            tracker = RunMetricsTracker()
            scraper_filter = self.metrics_scraper_var.get()
            if scraper_filter == "All":
                scraper_filter = None

            metrics_list = tracker.list_metrics(scraper_name=scraper_filter, limit=10000)

            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'run_id', 'scraper_name', 'status', 'started_at', 'ended_at',
                    'active_duration_seconds', 'active_duration_minutes',
                    'network_sent_bytes', 'network_received_bytes',
                    'network_total_gb', 'network_sent_mb', 'network_received_mb'
                ])

                for m in metrics_list:
                    writer.writerow([
                        m.run_id,
                        m.scraper_name,
                        m.status,
                        m.started_at,
                        m.ended_at,
                        m.active_duration_seconds,
                        round(m.active_duration_seconds / 60, 2),
                        m.network_sent_bytes,
                        m.network_received_bytes,
                        round(m.network_total_gb, 6),
                        round(m.network_sent_mb, 2),
                        round(m.network_received_mb, 2),
                    ])

            self.metrics_status_var.set(f"Exported {len(metrics_list)} runs to {file_path}")
            messagebox.showinfo("Export Complete", f"Exported {len(metrics_list)} runs to:\n{file_path}")
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export: {e}")

    # MONITORING TAB
    # ==================================================================

    def setup_monitoring_tab(self, parent):
        """Setup Monitoring tab with Pipeline, Health Check, Run Metrics, Prometheus, Proxy Pool, Frontier Queue, Geo Router, and Selector Healer."""
        # Create notebook for sub-tabs
        monitoring_notebook = ttk.Notebook(parent)
        monitoring_notebook.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        # Pipeline sub-tab
        pipeline_steps_frame = ttk.Frame(monitoring_notebook)
        monitoring_notebook.add(pipeline_steps_frame, text="Pipeline")
        self.setup_pipeline_steps_tab(pipeline_steps_frame, compact=True)

        # Health Check sub-tab
        health_check_frame = ttk.Frame(monitoring_notebook)
        monitoring_notebook.add(health_check_frame, text="Health Check")
        self.setup_health_check_tab(health_check_frame)

        # Run Metrics sub-tab
        run_metrics_frame = ttk.Frame(monitoring_notebook)
        monitoring_notebook.add(run_metrics_frame, text="Run Metrics")
        self.setup_run_metrics_tab(run_metrics_frame)

        # Prometheus Metrics sub-tab
        prometheus_frame = ttk.Frame(monitoring_notebook)
        monitoring_notebook.add(prometheus_frame, text="Prometheus Metrics")
        self.setup_prometheus_tab(prometheus_frame)

        # Proxy Pool sub-tab
        proxy_frame = ttk.Frame(monitoring_notebook)
        monitoring_notebook.add(proxy_frame, text="Proxy Pool")
        self.setup_proxy_pool_tab(proxy_frame)

        # Frontier Queue sub-tab
        frontier_frame = ttk.Frame(monitoring_notebook)
        monitoring_notebook.add(frontier_frame, text="Frontier Queue")
        self.setup_frontier_queue_tab(frontier_frame)

        # Geo Router sub-tab
        geo_frame = ttk.Frame(monitoring_notebook)
        monitoring_notebook.add(geo_frame, text="Geo Router")
        self.setup_geo_router_tab(geo_frame)

        # Selector Healer sub-tab
        healer_frame = ttk.Frame(monitoring_notebook)
        monitoring_notebook.add(healer_frame, text="Selector Healer")
        self.setup_selector_healer_tab(healer_frame)

    def setup_prometheus_tab(self, parent):
        """Setup Prometheus metrics display."""
        # Header
        header = ttk.LabelFrame(parent, text="Prometheus Metrics", padding=10, style='Title.TLabelframe')
        header.pack(fill=tk.X, padx=8, pady=(8, 4))

        # Controls
        controls = tk.Frame(header, bg=self.colors['white'])
        controls.pack(fill=tk.X)

        scraper_label = tk.Label(
            controls,
            text="Scraper:",
            bg=self.colors['white'],
            fg='#000000',
            font=self.fonts['standard'],
        )

        self.prometheus_scraper_var = tk.StringVar()
        self.prometheus_scraper_combo = ttk.Combobox(
            controls,
            textvariable=self.prometheus_scraper_var,
            state="readonly",
            width=self.MONITOR_COMBO_WIDTH,
            style='Modern.TCombobox',
        )
        self.prometheus_scraper_combo['values'] = ["All"] + list(self.scrapers.keys())
        self.prometheus_scraper_combo.set("All")
        self.prometheus_scraper_combo.bind("<<ComboboxSelected>>", self.on_prometheus_scraper_changed)

        refresh_btn = ttk.Button(
            controls,
            text="Refresh",
            command=self.load_prometheus_metrics,
            style='Secondary.TButton',
        )

        open_endpoint_btn = ttk.Button(
            controls,
            text="Open Metrics Endpoint",
            command=self.open_prometheus_endpoint,
            style='Secondary.TButton',
        )

        start_server_btn = ttk.Button(
            controls,
            text="Start Server",
            command=self.start_prometheus_server,
            style='Secondary.TButton',
        )

        # Status
        self.prometheus_status_var = tk.StringVar(value="Ready")
        status_label = tk.Label(
            controls,
            textvariable=self.prometheus_status_var,
            bg=self.colors['white'],
            fg='#666666',
            font=self.fonts['small'],
            anchor='w',
            justify=tk.LEFT,
            wraplength=420,
        )

        def apply_controls_layout(compact: bool):
            for w in (scraper_label, self.prometheus_scraper_combo, refresh_btn, open_endpoint_btn, start_server_btn, status_label):
                w.grid_forget()
            for col in range(6):
                controls.grid_columnconfigure(col, weight=0)

            if compact:
                scraper_label.grid(row=0, column=0, sticky='w', padx=(0, 5), pady=(0, 4))
                self.prometheus_scraper_combo.grid(row=0, column=1, sticky='w', padx=5, pady=(0, 4))
                refresh_btn.grid(row=0, column=2, sticky='w', padx=5, pady=(0, 4))
                open_endpoint_btn.grid(row=1, column=0, sticky='w', padx=(0, 5))
                start_server_btn.grid(row=1, column=1, sticky='w', padx=5)
                status_label.grid(row=1, column=2, columnspan=3, sticky='w', padx=(12, 0))
                controls.grid_columnconfigure(5, weight=1)
            else:
                scraper_label.grid(row=0, column=0, sticky='w', padx=(0, 5))
                self.prometheus_scraper_combo.grid(row=0, column=1, sticky='w', padx=5)
                refresh_btn.grid(row=0, column=2, sticky='w', padx=5)
                open_endpoint_btn.grid(row=0, column=3, sticky='w', padx=5)
                start_server_btn.grid(row=0, column=4, sticky='w', padx=5)
                status_label.grid(row=0, column=5, sticky='w', padx=(20, 0))
                controls.grid_columnconfigure(5, weight=1)

        controls_layout_state = {"compact": None}

        def on_controls_resize(_event=None):
            compact = controls.winfo_width() < 980
            if controls_layout_state["compact"] == compact:
                return
            controls_layout_state["compact"] = compact
            apply_controls_layout(compact)

        controls.bind("<Configure>", on_controls_resize, add="+")
        controls.after(0, on_controls_resize)

        # Metrics display
        metrics_frame = ttk.LabelFrame(parent, text="Metrics", padding=10, style='Title.TLabelframe')
        metrics_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=(4, 8))

        # Create treeview
        tree_container = tk.Frame(metrics_frame, bg=self.colors['white'])
        tree_container.pack(fill=tk.BOTH, expand=True)

        vsb = ttk.Scrollbar(tree_container, orient="vertical")
        hsb = ttk.Scrollbar(tree_container, orient="horizontal")

        columns = ('metric', 'value', 'labels', 'description')
        self.prometheus_tree = ttk.Treeview(tree_container, columns=columns, show='headings',
                                           yscrollcommand=vsb.set, xscrollcommand=hsb.set,
                                           height=20)

        vsb.config(command=self.prometheus_tree.yview)
        hsb.config(command=self.prometheus_tree.xview)

        self.prometheus_tree.heading('metric', text='Metric Name')
        self.prometheus_tree.heading('value', text='Value')
        self.prometheus_tree.heading('labels', text='Labels')
        self.prometheus_tree.heading('description', text='Description')

        self.prometheus_tree.column('metric', width=250, minwidth=150)
        self.prometheus_tree.column('value', width=120, minwidth=80)
        self.prometheus_tree.column('labels', width=200, minwidth=150)
        self.prometheus_tree.column('description', width=300, minwidth=200)

        self.prometheus_tree.grid(row=0, column=0, sticky='nsew')
        vsb.grid(row=0, column=1, sticky='ns')
        hsb.grid(row=1, column=0, sticky='ew')

        tree_container.grid_rowconfigure(0, weight=1)
        tree_container.grid_columnconfigure(0, weight=1)

        # Load initial data
        self.load_prometheus_metrics()

    def setup_proxy_pool_tab(self, parent):
        """Setup Proxy Pool status display."""
        # Header
        header = ttk.LabelFrame(parent, text="Proxy Pool Status", padding=10, style='Title.TLabelframe')
        header.pack(fill=tk.X, padx=8, pady=(8, 4))

        # Controls
        controls = tk.Frame(header, bg=self.colors['white'])
        controls.pack(fill=tk.X)

        scraper_label = tk.Label(
            controls,
            text="Scraper:",
            bg=self.colors['white'],
            fg='#000000',
            font=self.fonts['standard'],
        )

        self.proxy_scraper_var = tk.StringVar()
        self.proxy_scraper_combo = ttk.Combobox(
            controls,
            textvariable=self.proxy_scraper_var,
            state="readonly",
            width=self.MONITOR_COMBO_WIDTH,
            style='Modern.TCombobox',
        )
        self.proxy_scraper_combo['values'] = ["All"] + list(self.scrapers.keys())
        self.proxy_scraper_combo.set("All")
        self.proxy_scraper_combo.bind("<<ComboboxSelected>>", self.on_proxy_scraper_changed)

        refresh_btn = ttk.Button(
            controls,
            text="Refresh",
            command=self.load_proxy_pool_status,
            style='Secondary.TButton',
        )

        # Status
        self.proxy_status_var = tk.StringVar(value="Ready")
        status_label = tk.Label(
            controls,
            textvariable=self.proxy_status_var,
            bg=self.colors['white'],
            fg='#666666',
            font=self.fonts['small'],
            anchor='w',
            justify=tk.LEFT,
            wraplength=420,
        )

        def apply_controls_layout(compact: bool):
            for w in (scraper_label, self.proxy_scraper_combo, refresh_btn, status_label):
                w.grid_forget()
            for col in range(4):
                controls.grid_columnconfigure(col, weight=0)

            if compact:
                scraper_label.grid(row=0, column=0, sticky='w', padx=(0, 5), pady=(0, 4))
                self.proxy_scraper_combo.grid(row=0, column=1, sticky='w', padx=5, pady=(0, 4))
                refresh_btn.grid(row=0, column=2, sticky='w', padx=5, pady=(0, 4))
                status_label.grid(row=1, column=0, columnspan=4, sticky='w', padx=(0, 0))
            else:
                scraper_label.grid(row=0, column=0, sticky='w', padx=(0, 5))
                self.proxy_scraper_combo.grid(row=0, column=1, sticky='w', padx=5)
                refresh_btn.grid(row=0, column=2, sticky='w', padx=5)
                status_label.grid(row=0, column=3, sticky='w', padx=(20, 0))
                controls.grid_columnconfigure(3, weight=1)

        controls_layout_state = {"compact": None}

        def on_controls_resize(_event=None):
            compact = controls.winfo_width() < 860
            if controls_layout_state["compact"] == compact:
                return
            controls_layout_state["compact"] = compact
            apply_controls_layout(compact)

        controls.bind("<Configure>", on_controls_resize, add="+")
        controls.after(0, on_controls_resize)

        # Proxy list
        proxy_frame = ttk.LabelFrame(parent, text="Proxy List", padding=10, style='Title.TLabelframe')
        proxy_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=(4, 8))

        tree_container = tk.Frame(proxy_frame, bg=self.colors['white'])
        tree_container.pack(fill=tk.BOTH, expand=True)

        vsb = ttk.Scrollbar(tree_container, orient="vertical")
        hsb = ttk.Scrollbar(tree_container, orient="horizontal")

        columns = ('proxy_id', 'host', 'port', 'type', 'country', 'status', 'health_score', 'success_rate')
        self.proxy_tree = ttk.Treeview(tree_container, columns=columns, show='headings',
                                      yscrollcommand=vsb.set, xscrollcommand=hsb.set,
                                      height=20)

        vsb.config(command=self.proxy_tree.yview)
        hsb.config(command=self.proxy_tree.xview)

        self.proxy_tree.heading('proxy_id', text='Proxy ID')
        self.proxy_tree.heading('host', text='Host')
        self.proxy_tree.heading('port', text='Port')
        self.proxy_tree.heading('type', text='Type')
        self.proxy_tree.heading('country', text='Country')
        self.proxy_tree.heading('status', text='Status')
        self.proxy_tree.heading('health_score', text='Health Score')
        self.proxy_tree.heading('success_rate', text='Success Rate')

        self.proxy_tree.column('proxy_id', width=150, minwidth=100)
        self.proxy_tree.column('host', width=150, minwidth=100)
        self.proxy_tree.column('port', width=80, minwidth=60)
        self.proxy_tree.column('type', width=120, minwidth=80)
        self.proxy_tree.column('country', width=100, minwidth=80)
        self.proxy_tree.column('status', width=100, minwidth=80)
        self.proxy_tree.column('health_score', width=100, minwidth=80)
        self.proxy_tree.column('success_rate', width=100, minwidth=80)

        self.proxy_tree.grid(row=0, column=0, sticky='nsew')
        vsb.grid(row=0, column=1, sticky='ns')
        hsb.grid(row=1, column=0, sticky='ew')

        tree_container.grid_rowconfigure(0, weight=1)
        tree_container.grid_columnconfigure(0, weight=1)

        # Load initial data
        self.load_proxy_pool_status()

    def setup_frontier_queue_tab(self, parent):
        """Setup Frontier Queue stats display."""
        # Header
        header = ttk.LabelFrame(parent, text="Frontier Queue Statistics", padding=10, style='Title.TLabelframe')
        header.pack(fill=tk.X, padx=8, pady=(8, 4))

        # Controls
        controls = tk.Frame(header, bg=self.colors['white'])
        controls.pack(fill=tk.X)

        tk.Label(controls, text="Scraper:", bg=self.colors['white'], fg='#000000',
                 font=self.fonts['standard']).pack(side=tk.LEFT, padx=(0, 5))

        self.frontier_scraper_var = tk.StringVar()
        self.frontier_scraper_combo = ttk.Combobox(controls, textvariable=self.frontier_scraper_var,
                                                   state="readonly", width=self.MONITOR_COMBO_WIDTH, style='Modern.TCombobox')
        self.frontier_scraper_combo['values'] = list(self.scrapers.keys())
        self.frontier_scraper_combo.pack(side=tk.LEFT, padx=5)
        if self.scrapers:
            self.frontier_scraper_combo.set(list(self.scrapers.keys())[0])
        self.frontier_scraper_combo.bind("<<ComboboxSelected>>", self.on_frontier_scraper_changed)

        ttk.Button(controls, text="Refresh", command=self.load_frontier_queue_stats,
                  style='Secondary.TButton').pack(side=tk.LEFT, padx=5)

        # Stats display
        stats_frame = ttk.LabelFrame(parent, text="Queue Statistics", padding=10, style='Title.TLabelframe')
        stats_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=(4, 8))

        self.frontier_stats_text = tk.Text(stats_frame, height=15, wrap=tk.WORD,
                                          font=self.fonts['monospace'],
                                          bg=self.colors['white'],
                                          fg='#000000',
                                          state=tk.DISABLED,
                                          borderwidth=0,
                                          relief='flat',
                                          highlightthickness=0)
        self.frontier_stats_text.pack(fill=tk.BOTH, expand=True)

        # Status
        self.frontier_status_var = tk.StringVar(value="Ready")
        status_label = tk.Label(parent, textvariable=self.frontier_status_var,
                               bg=self.colors['white'], fg='#666666',
                               font=self.fonts['small'], anchor='w')
        status_label.pack(fill=tk.X, padx=8, pady=(0, 4))

        # Load initial data
        self.load_frontier_queue_stats()

    def setup_geo_router_tab(self, parent):
        """Setup Geo Router configuration display."""
        # Header
        header = ttk.LabelFrame(parent, text="Geo Router Configuration", padding=10, style='Title.TLabelframe')
        header.pack(fill=tk.X, padx=8, pady=(8, 4))

        # Controls
        controls = tk.Frame(header, bg=self.colors['white'])
        controls.pack(fill=tk.X)

        tk.Label(controls, text="Scraper:", bg=self.colors['white'], fg='#000000',
                 font=self.fonts['standard']).pack(side=tk.LEFT, padx=(0, 5))

        self.geo_scraper_var = tk.StringVar()
        self.geo_scraper_combo = ttk.Combobox(controls, textvariable=self.geo_scraper_var,
                                             state="readonly", width=self.MONITOR_COMBO_WIDTH, style='Modern.TCombobox')
        self.geo_scraper_combo['values'] = list(self.scrapers.keys())
        self.geo_scraper_combo.pack(side=tk.LEFT, padx=5)
        if self.scrapers:
            self.geo_scraper_combo.set(list(self.scrapers.keys())[0])
        self.geo_scraper_combo.bind("<<ComboboxSelected>>", self.on_geo_scraper_changed)

        ttk.Button(controls, text="Refresh", command=self.load_geo_router_config,
                  style='Secondary.TButton').pack(side=tk.LEFT, padx=5)

        # Config display
        config_frame = ttk.LabelFrame(parent, text="Routing Configuration", padding=10, style='Title.TLabelframe')
        config_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=(4, 8))

        self.geo_config_text = tk.Text(config_frame, height=15, wrap=tk.WORD,
                                      font=self.fonts['monospace'],
                                      bg=self.colors['white'],
                                      fg='#000000',
                                      state=tk.DISABLED,
                                      borderwidth=0,
                                      relief='flat',
                                      highlightthickness=0)
        self.geo_config_text.pack(fill=tk.BOTH, expand=True)

        # Status
        self.geo_status_var = tk.StringVar(value="Ready")
        status_label = tk.Label(parent, textvariable=self.geo_status_var,
                               bg=self.colors['white'], fg='#666666',
                               font=self.fonts['small'], anchor='w')
        status_label.pack(fill=tk.X, padx=8, pady=(0, 4))

        # Load initial data
        self.load_geo_router_config()

    def setup_selector_healer_tab(self, parent):
        """Setup Selector Healer stats display."""
        # Header
        header = ttk.LabelFrame(parent, text="Selector Healer Statistics", padding=10, style='Title.TLabelframe')
        header.pack(fill=tk.X, padx=8, pady=(8, 4))

        # Controls
        controls = tk.Frame(header, bg=self.colors['white'])
        controls.pack(fill=tk.X)

        tk.Label(controls, text="Scraper:", bg=self.colors['white'], fg='#000000',
                 font=self.fonts['standard']).pack(side=tk.LEFT, padx=(0, 5))

        self.healer_scraper_var = tk.StringVar()
        self.healer_scraper_combo = ttk.Combobox(controls, textvariable=self.healer_scraper_var,
                                                 state="readonly", width=self.MONITOR_COMBO_WIDTH, style='Modern.TCombobox')
        self.healer_scraper_combo['values'] = ["All"] + list(self.scrapers.keys())
        self.healer_scraper_combo.pack(side=tk.LEFT, padx=5)
        self.healer_scraper_combo.set("All")
        self.healer_scraper_combo.bind("<<ComboboxSelected>>", self.on_healer_scraper_changed)

        ttk.Button(controls, text="Refresh", command=self.load_selector_healer_stats,
                  style='Secondary.TButton').pack(side=tk.LEFT, padx=5)

        # Stats display
        stats_frame = ttk.LabelFrame(parent, text="Healing Statistics", padding=10, style='Title.TLabelframe')
        stats_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=(4, 8))

        self.healer_stats_text = tk.Text(stats_frame, height=15, wrap=tk.WORD,
                                         font=self.fonts['monospace'],
                                         bg=self.colors['white'],
                                         fg='#000000',
                                         state=tk.DISABLED,
                                         borderwidth=0,
                                         relief='flat',
                                         highlightthickness=0)
        self.healer_stats_text.pack(fill=tk.BOTH, expand=True)

        # Status
        self.healer_status_var = tk.StringVar(value="Ready")
        status_label = tk.Label(parent, textvariable=self.healer_status_var,
                               bg=self.colors['white'], fg='#666666',
                               font=self.fonts['small'], anchor='w')
        status_label.pack(fill=tk.X, padx=8, pady=(0, 4))

        # Load initial data
        self.load_selector_healer_stats()

    def on_prometheus_scraper_changed(self, event=None):
        """Handle scraper selection change in Prometheus tab."""
        self.load_prometheus_metrics()

    def load_prometheus_metrics(self):
        """Load Prometheus metrics."""
        if not _REQUESTS_AVAILABLE:
            self.prometheus_status_var.set("Error: requests library not available")
            return

        token = self._next_monitor_async_token("prometheus")
        self.prometheus_status_var.set("Loading metrics...")

        def worker():
            try:
                response = requests.get("http://localhost:9090/metrics", timeout=5)
                if response.status_code != 200:
                    return {"error": f"Prometheus endpoint returned status {response.status_code}"}

                rows = []
                metric_count = 0
                for line in response.text.split('\n'):
                    if not line or line.startswith('#'):
                        continue
                    parts = line.split()
                    if len(parts) < 2:
                        continue
                    metric_name = parts[0]
                    value = parts[1]
                    labels = ""
                    description = ""
                    if '{' in metric_name:
                        name_part = metric_name.split('{')[0]
                        labels_part = metric_name.split('{')[1].rstrip('}')
                        metric_name = name_part
                        labels = labels_part
                    rows.append((metric_name, value, labels, description))
                    metric_count += 1
                    if metric_count >= 100:
                        break
                return {"rows": rows, "count": metric_count}
            except requests.exceptions.ConnectionError:
                return {"not_running": True}
            except Exception as exc:
                return {"error": f"Error loading metrics: {exc}"}

        def apply(payload):
            if not self._is_monitor_async_token_current("prometheus", token):
                return
            for item in self.prometheus_tree.get_children():
                self.prometheus_tree.delete(item)

            if payload.get("error"):
                self.prometheus_status_var.set(payload["error"])
                return
            if payload.get("not_running"):
                self.prometheus_status_var.set("Prometheus not running - Click 'Start Server' to start it")
                self.prometheus_tree.insert('', 'end', values=(
                    "INFO",
                    "N/A",
                    "",
                    "Prometheus server not running. Click 'Start Server' button to start it."
                ))
                return

            for row in payload.get("rows", []):
                self.prometheus_tree.insert('', 'end', values=row)
            self.prometheus_status_var.set(f"Loaded {payload.get('count', 0)} metrics from Prometheus")

        def run_async():
            payload = worker()
            self.root.after(0, lambda: apply(payload))

        threading.Thread(target=run_async, daemon=True).start()

    def open_prometheus_endpoint(self):
        """Open Prometheus metrics endpoint in browser."""
        webbrowser.open("http://localhost:9090/metrics")

    def start_prometheus_server(self):
        """Start Prometheus metrics server manually."""
        try:
            from core.prometheus_exporter import init_prometheus_metrics
            success = init_prometheus_metrics(port=9090)
            if success:
                self.prometheus_status_var.set("Prometheus server started on port 9090")
                messagebox.showinfo("Success", "Prometheus metrics server started successfully!\n\nMetrics available at: http://localhost:9090/metrics")
                # Refresh metrics after starting
                self.root.after(500, self.load_prometheus_metrics)
            else:
                self.prometheus_status_var.set("Prometheus client not available - install prometheus_client")
                messagebox.showwarning("Not Available", "Prometheus client not installed.\n\nInstall with: pip install prometheus-client")
        except Exception as e:
            self.prometheus_status_var.set(f"Error starting server: {e}")
            messagebox.showerror("Error", f"Failed to start Prometheus server:\n{e}")

    def on_proxy_scraper_changed(self, event=None):
        """Handle scraper selection change in Proxy Pool tab."""
        self.load_proxy_pool_status()

    def load_proxy_pool_status(self):
        """Load Proxy Pool status."""
        token = self._next_monitor_async_token("proxy_pool")
        self.proxy_status_var.set("Loading proxy pool...")

        scraper_filter = self.proxy_scraper_var.get()
        if scraper_filter == "All":
            scraper_filter = None

        def worker():
            try:
                from core.proxy_pool import get_proxy_pool
            except ImportError:
                return {"error": "Proxy Pool not available"}

            try:
                pool = get_proxy_pool()
                stats = pool.get_stats()
                proxies = []
                rows = []
                with pool._lock:
                    for proxy in pool._proxies.values():
                        if not scraper_filter:
                            proxies.append(proxy)
                        else:
                            scraper_countries = {
                                "Malaysia": "MY",
                                "India": "IN",
                                "Argentina": "AR",
                                "Russia": "RU",
                                "Netherlands": "NL",
                                "Belarus": "BY",
                            }
                            country_code = scraper_countries.get(scraper_filter)
                            if country_code and proxy.country_code == country_code:
                                proxies.append(proxy)

                for proxy in proxies:
                    proxy_id = getattr(proxy, 'id', 'N/A')
                    host = getattr(proxy, 'host', 'N/A')
                    port = getattr(proxy, 'port', 'N/A')
                    proxy_type_val = getattr(proxy.proxy_type, 'value', 'N/A') if hasattr(proxy, 'proxy_type') else 'N/A'
                    country_code = getattr(proxy, 'country_code', 'N/A')
                    status_val = getattr(proxy.status, 'value', 'N/A') if hasattr(proxy, 'status') else 'N/A'
                    health_score = getattr(proxy, 'health_score', None)
                    success_rate = getattr(proxy, 'success_rate', None)
                    rows.append((
                        proxy_id,
                        host,
                        port,
                        proxy_type_val,
                        country_code,
                        status_val,
                        f"{health_score:.2f}" if health_score is not None else "N/A",
                        f"{success_rate:.2%}" if success_rate is not None else "N/A",
                    ))

                return {
                    "rows": rows,
                    "loaded": len(rows),
                    "total": stats.get("total_proxies", 0),
                }
            except AttributeError as exc:
                return {"error": f"Error accessing proxy pool: {exc}"}
            except Exception as exc:
                return {"error": f"Error loading proxy pool: {exc}"}

        def apply(payload):
            if not self._is_monitor_async_token_current("proxy_pool", token):
                return
            for item in self.proxy_tree.get_children():
                self.proxy_tree.delete(item)

            if payload.get("error"):
                self.proxy_status_var.set(payload["error"])
                return

            for row in payload.get("rows", []):
                self.proxy_tree.insert('', 'end', values=row)
            self.proxy_status_var.set(
                f"Loaded {payload.get('loaded', 0)} proxies | Total: {payload.get('total', 0)}"
            )

        def run_async():
            payload = worker()
            self.root.after(0, lambda: apply(payload))

        threading.Thread(target=run_async, daemon=True).start()

    def on_frontier_scraper_changed(self, event=None):
        """Handle scraper selection change in Frontier Queue tab."""
        self.load_frontier_queue_stats()

    def load_frontier_queue_stats(self):
        """Load Frontier Queue statistics."""
        scraper_name = self.frontier_scraper_var.get()
        if not scraper_name:
            self.frontier_status_var.set("Please select a scraper")
            return

        token = self._next_monitor_async_token("frontier")
        self.frontier_status_var.set("Loading frontier stats...")

        def worker():
            try:
                from scripts.common.frontier_integration import get_frontier_stats
            except ImportError:
                return {"error": "Frontier Queue not available (Redis required)"}

            try:
                stats = get_frontier_stats(scraper_name)
                stats_text = f"""Frontier Queue Statistics for {scraper_name}
{'='*60}

Queue Status:
  Queued URLs:     {stats.get('queued', 0):,}
  Seen URLs:      {stats.get('seen', 0):,}
  Active URLs:    {stats.get('active', 0):,}
  Completed URLs: {stats.get('completed', 0):,}
  Failed URLs:    {stats.get('failed', 0):,}

Total Processed:  {stats.get('queued', 0) + stats.get('completed', 0) + stats.get('failed', 0):,}
Success Rate:     {(stats.get('completed', 0) / max(1, stats.get('completed', 0) + stats.get('failed', 0))) * 100:.1f}%

{'='*60}
"""
                return {"text": stats_text}
            except Exception as exc:
                return {"error": f"Error loading frontier stats: {exc}"}

        def apply(payload):
            if not self._is_monitor_async_token_current("frontier", token):
                return
            if payload.get("error"):
                self.frontier_status_var.set(payload["error"])
                return

            self.frontier_stats_text.config(state=tk.NORMAL)
            self.frontier_stats_text.delete('1.0', tk.END)
            self.frontier_stats_text.insert('1.0', payload.get("text", ""))
            self.frontier_stats_text.config(state=tk.DISABLED)
            self.frontier_status_var.set(f"Loaded stats for {scraper_name}")

        def run_async():
            payload = worker()
            self.root.after(0, lambda: apply(payload))

        threading.Thread(target=run_async, daemon=True).start()

    def on_geo_scraper_changed(self, event=None):
        """Handle scraper selection change in Geo Router tab."""
        self.load_geo_router_config()

    def load_geo_router_config(self):
        """Load Geo Router configuration."""
        scraper_name = self.geo_scraper_var.get()
        if not scraper_name:
            self.geo_status_var.set("Please select a scraper")
            return

        token = self._next_monitor_async_token("geo_router")
        self.geo_status_var.set("Loading geo router config...")

        def worker():
            try:
                from core.integration_helpers import get_geo_config_for_scraper
            except ImportError:
                return {"error": "Geo Router not available"}

            try:
                geo_config = get_geo_config_for_scraper(scraper_name)

                if geo_config:
                    config_text = f"""Geo Router Configuration for {scraper_name}
{'='*60}

Basic Settings:
  Country Code:   {geo_config.get('country_code', 'N/A')}
  Timezone:       {geo_config.get('timezone', 'N/A')}
  Locale:         {geo_config.get('locale', 'N/A')}

Geolocation:
  Latitude:       {geo_config.get('geolocation', {}).get('latitude', 'N/A')}
  Longitude:      {geo_config.get('geolocation', {}).get('longitude', 'N/A')}

Proxy Configuration:
"""
                    if geo_config.get('proxy'):
                        proxy = geo_config['proxy']
                        config_text += f"""  Host:           {proxy.get('host', 'N/A')}
  Port:           {proxy.get('port', 'N/A')}
  Protocol:       {proxy.get('protocol', 'N/A')}
  Username:       {proxy.get('username', 'N/A') or 'None'}
"""
                    else:
                        config_text += "  Proxy:          Not configured\n"

                    config_text += f"""
{'='*60}
"""
                else:
                    config_text = f"No geo configuration found for {scraper_name}\n"

                return {"text": config_text}
            except Exception as exc:
                return {"error": f"Error loading geo config: {exc}"}

        def apply(payload):
            if not self._is_monitor_async_token_current("geo_router", token):
                return
            if payload.get("error"):
                self.geo_status_var.set(payload["error"])
                return

            self.geo_config_text.config(state=tk.NORMAL)
            self.geo_config_text.delete('1.0', tk.END)
            self.geo_config_text.insert('1.0', payload.get("text", ""))
            self.geo_config_text.config(state=tk.DISABLED)
            self.geo_status_var.set(f"Loaded config for {scraper_name}")

        def run_async():
            payload = worker()
            self.root.after(0, lambda: apply(payload))

        threading.Thread(target=run_async, daemon=True).start()

    def on_healer_scraper_changed(self, event=None):
        """Handle scraper selection change in Selector Healer tab."""
        self.load_selector_healer_stats()

    def load_selector_healer_stats(self):
        """Load Selector Healer statistics."""
        token = self._next_monitor_async_token("selector_healer")
        self.healer_status_var.set("Loading selector healer stats...")

        def worker():
            try:
                from core.selector_healer import get_selector_healer
            except ImportError:
                return {"error": "Selector Healer not available"}

            try:
                healer = get_selector_healer()
                stats_text = f"""Selector Healer Statistics
{'='*60}

Status:
  Inference Available: {'Yes' if healer._inference_available else 'No'}

Note: Selector healing statistics are tracked per-scraper during runtime.
Healing attempts are logged when selectors fail and are automatically healed.

To view detailed healing logs, check the scraper console output or logs.

{'='*60}
"""
                # This would require per-scraper runtime healing counters.
                stats_text += "\nHealing is automatically attempted when selectors fail.\n"
                stats_text += "Check scraper logs for healing details.\n"
                return {"text": stats_text}
            except Exception as exc:
                return {"error": f"Error loading healer stats: {exc}"}

        def apply(payload):
            if not self._is_monitor_async_token_current("selector_healer", token):
                return
            if payload.get("error"):
                self.healer_status_var.set(payload["error"])
                return

            self.healer_stats_text.config(state=tk.NORMAL)
            self.healer_stats_text.delete('1.0', tk.END)
            self.healer_stats_text.insert('1.0', payload.get("text", ""))
            self.healer_stats_text.config(state=tk.DISABLED)
            self.healer_status_var.set("Selector Healer ready")

        def run_async():
            payload = worker()
            self.root.after(0, lambda: apply(payload))

        threading.Thread(target=run_async, daemon=True).start()

    # ==================================================================
    # INPUT DATA MANAGEMENT TAB
    # ==================================================================

    def setup_input_management_tab(self, parent):
        """Setup Input Data management tab: browse, upload, preview input tables + mappings."""
        import tkinter.filedialog as filedialog
        import tkinter.messagebox as messagebox

        # --- Header ---
        header = ttk.LabelFrame(parent, text="Input Data & Mappings", padding=10, style='Title.TLabelframe')
        header.pack(fill=tk.X, padx=8, pady=(8, 4))

        controls = tk.Frame(header, bg=self.colors['white'])
        controls.pack(fill=tk.X)

        # Country selector
        tk.Label(controls, text="Country:", bg=self.colors['white'], fg='#000000',
                 font=self.fonts['standard']).pack(side=tk.LEFT, padx=(0, 5))

        self.input_scraper_var = tk.StringVar()
        input_scraper_combo = ttk.Combobox(controls, textvariable=self.input_scraper_var,
                                           state="readonly", width=18, style='Modern.TCombobox')
        input_scraper_combo['values'] = list(self.scrapers.keys())
        input_scraper_combo.pack(side=tk.LEFT, padx=5)
        if self.scrapers:
            self.input_scraper_var.set(list(self.scrapers.keys())[0])
        input_scraper_combo.bind("<<ComboboxSelected>>", lambda e: (self._refresh_input_tables(), self._refresh_io_lock_states()))

        # Table selector
        tk.Label(controls, text="Table:", bg=self.colors['white'], fg='#000000',
                 font=self.fonts['standard']).pack(side=tk.LEFT, padx=(15, 5))

        self.input_table_var = tk.StringVar()
        self.input_table_combo = ttk.Combobox(controls, textvariable=self.input_table_var,
                                              state="readonly", width=22, style='Modern.TCombobox')
        self.input_table_combo.pack(side=tk.LEFT, padx=5)
        self.input_table_combo.bind("<<ComboboxSelected>>", lambda e: self._preview_input_table())

        # Buttons (store references for lock/unlock during pipeline runs)
        self._input_upload_btn = ttk.Button(controls, text="Upload CSV", command=self._upload_input_csv,
                   style='Secondary.TButton')
        self._input_upload_btn.pack(side=tk.LEFT, padx=(15, 5))
        self._input_export_btn = ttk.Button(controls, text="Export CSV", command=self._export_input_csv,
                   style='Secondary.TButton')
        self._input_export_btn.pack(side=tk.LEFT, padx=5)
        self._input_schema_btn = ttk.Button(controls, text="Schema Info", command=self._show_schema_info,
                   style='Secondary.TButton')
        self._input_schema_btn.pack(side=tk.LEFT, padx=5)
        self._input_refresh_btn = ttk.Button(controls, text="Refresh", command=self._refresh_input_tables,
                   style='Secondary.TButton')
        self._input_refresh_btn.pack(side=tk.LEFT, padx=5)

        # Lock warning label (hidden by default)
        self._input_lock_label = tk.Label(controls, text="  Locked (scraper running)",
                                          bg=self.colors['white'], fg='#ef4444',
                                          font=self.fonts['standard'])
        self._input_lock_label.pack(side=tk.LEFT, padx=(10, 0))
        self._input_lock_label.pack_forget()

        # --- Info bar ---
        self.input_info_var = tk.StringVar(value="Select a country and table to view data")
        info_bar = tk.Label(parent, textvariable=self.input_info_var,
                            bg=self.colors['background_gray'], fg=self.colors['medium_gray'],
                            font=self.fonts['standard'], anchor='w', padx=10, pady=4)
        info_bar.pack(fill=tk.X, padx=8, pady=(0, 4))

        # --- Data preview (Treeview table) ---
        preview_frame = tk.Frame(parent, bg=self.colors['white'],
                                 highlightbackground=self.colors['border_gray'], highlightthickness=1)
        preview_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))

        # Treeview with scrollbars
        tree_container = tk.Frame(preview_frame, bg=self.colors['white'])
        tree_container.pack(fill=tk.BOTH, expand=True)

        y_scroll = ttk.Scrollbar(tree_container, orient=tk.VERTICAL)
        x_scroll = ttk.Scrollbar(tree_container, orient=tk.HORIZONTAL)

        self.input_tree = ttk.Treeview(tree_container,
                                       yscrollcommand=y_scroll.set,
                                       xscrollcommand=x_scroll.set,
                                       show='headings')
        y_scroll.config(command=self.input_tree.yview)
        x_scroll.config(command=self.input_tree.xview)

        y_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        x_scroll.pack(side=tk.BOTTOM, fill=tk.X)
        self.input_tree.pack(fill=tk.BOTH, expand=True)

        # Initialize
        self._refresh_input_tables()

    def _get_country_db(self, country: str):
        """Get or create PostgresDB for input tables."""
        try:
            self._last_input_db_error = None
            from core.db.postgres_connection import PostgresDB
            from core.db.schema_registry import SchemaRegistry
            from pathlib import Path

            db = PostgresDB(country)
            db.connect()

            # Apply common + inputs schema
            from core.db.models import apply_common_schema
            apply_common_schema(db)

            repo_root = Path(__file__).resolve().parent
            sql_dir = repo_root / "sql" / "schemas" / "postgres"
            registry = SchemaRegistry(db)
            inputs_schema = sql_dir / "inputs.sql"
            if inputs_schema.exists():
                registry.apply_schema(inputs_schema)
            # Apply country-specific schema so input tables exist (e.g. in_input_formulations, my_input_products)
            country_schemas = {
                "India": "india.sql",
                "Malaysia": "malaysia.sql",
                "Argentina": "argentina.sql",
                "Belarus": "belarus.sql",
                "Russia": "russia.sql",
                "Netherlands": "netherlands.sql",
                "Taiwan": "taiwan.sql",
                "tender_chile": "tender_chile.sql",
                "CanadaOntario": "canada_ontario.sql",
                "canada_quebec": "canada_quebec.sql",
                "North_Macedonia": "north_macedonia.sql",
            }
            schema_file = country_schemas.get(country)
            if schema_file and (sql_dir / schema_file).exists():
                try:
                    registry.apply_schema(sql_dir / schema_file)
                except Exception as e:
                    # Non-fatal: input tables already created by inputs.sql above.
                    # Country schema may fail if data tables have constraint issues.
                    print(f"[GUI] Country schema {schema_file} partially failed (input tables still available): {e}")

            return db
        except Exception as e:
            self._last_input_db_error = str(e)
            return None

    def _lock_input_controls(self):
        """Disable input modification buttons while a scraper is running."""
        for btn in (self._input_upload_btn, self._input_export_btn, self._input_schema_btn, self._input_refresh_btn):
            try:
                if btn:
                    btn.config(state=tk.DISABLED)
            except Exception:
                pass
        if hasattr(self, '_input_lock_label'):
            self._input_lock_label.pack(side=tk.LEFT, padx=(10, 0))

    def _unlock_input_controls(self):
        """Re-enable input modification buttons after scraper finishes."""
        for btn in (self._input_upload_btn, self._input_export_btn, self._input_schema_btn, self._input_refresh_btn):
            try:
                if btn:
                    btn.config(state=tk.NORMAL)
            except Exception:
                pass
        if hasattr(self, '_input_lock_label'):
            self._input_lock_label.pack_forget()

    def _update_output_lock_state(self):
        """Enable/disable output controls based on selected scraper running state."""
        scraper = self.output_scraper_var.get() if hasattr(self, "output_scraper_var") else None
        running = scraper in self.running_scrapers
        state = tk.DISABLED if running else tk.NORMAL
        # Keep scraper combo always enabled so user can switch markets
        # Leave dropdowns enabled; disable buttons only.
        controls = [
            getattr(self, "_output_refresh_btn", None),
            getattr(self, "_output_export_btn", None),
            getattr(self, "_output_delete_table_btn", None),
            getattr(self, "_output_delete_all_btn", None),
            getattr(self, "_output_delete_market_btn", None),
        ]
        for ctrl in controls:
            try:
                if ctrl:
                    ctrl.config(state=state)
            except Exception:
                pass

        # Data reset controls should follow running state
        try:
            if running:
                if hasattr(self, "clear_step_combo"):
                    self.clear_step_combo.config(state=tk.DISABLED)
                if hasattr(self, "clear_step_button"):
                    self.clear_step_button.config(state=tk.DISABLED)
                if hasattr(self, "clear_downstream_check"):
                    self.clear_downstream_check.state(["disabled"])
            else:
                if scraper:
                    self.update_reset_controls(scraper)
        except Exception:
            pass

    def _update_input_lock_state(self):
        """Enable/disable input controls based on selected scraper running state."""
        scraper = self.input_scraper_var.get() if hasattr(self, "input_scraper_var") else None
        running = scraper in self.running_scrapers
        if running:
            self._lock_input_controls()
        else:
            self._unlock_input_controls()

    def _refresh_io_lock_states(self):
        """Refresh both input and output lock states."""
        self._update_input_lock_state()
        self._update_output_lock_state()

    def _schedule_market_table_refresh(self, delay_ms: int = 80):
        """Debounce table refresh while switching scrapers to keep dropdown responsive."""
        if self._pending_table_refresh_after_id:
            try:
                self.root.after_cancel(self._pending_table_refresh_after_id)
            except Exception:
                pass
            self._pending_table_refresh_after_id = None

        def _run_refresh():
            self._pending_table_refresh_after_id = None
            try:
                if hasattr(self, 'output_table_combo'):
                    self._refresh_output_tables()
            except Exception:
                pass
            try:
                if hasattr(self, '_refresh_input_tables'):
                    self._refresh_input_tables()
            except Exception:
                pass

        self._pending_table_refresh_after_id = self.root.after(max(0, int(delay_ms)), _run_refresh)

    def _refresh_input_tables(self):
        """Refresh the table dropdown and info for the selected country."""
        country = self.input_scraper_var.get()
        if not country:
            return

        try:
            from core.db.csv_importer import INPUT_TABLE_REGISTRY, PCID_MAPPING_CONFIG
        except ImportError:
            self.input_info_var.set("Error: core.db.csv_importer not found")
            return

        # Build table list: country-specific + PCID mapping (if applicable)
        tables = []
        configs = INPUT_TABLE_REGISTRY.get(country, [])
        for cfg in configs:
            tables.append(cfg["display"])
        
        # Only add PCID mapping for countries that use it
        countries_with_pcid = {"Argentina", "Malaysia", "Belarus", "Netherlands", "Taiwan", "tender_chile", "NorthMacedonia"}
        if country in countries_with_pcid:
            tables.append(PCID_MAPPING_CONFIG["display"])

        self.input_table_combo['values'] = tables
        if tables:
            self.input_table_var.set(tables[0])
            self._preview_input_table()
        else:
            self.input_table_var.set("")
            self.input_info_var.set(f"No input tables configured for {country}")

        # Store configs for lookup
        self._input_configs = {cfg["display"]: cfg for cfg in configs}
        if country in countries_with_pcid:
            self._input_configs[PCID_MAPPING_CONFIG["display"]] = PCID_MAPPING_CONFIG

    def _get_selected_table_config(self):
        """Get the config dict for the currently selected table."""
        display = self.input_table_var.get()
        return getattr(self, '_input_configs', {}).get(display)

    def _preview_input_table(self):
        """Load and display the selected input table in the Treeview."""
        country = self.input_scraper_var.get()
        config = self._get_selected_table_config()
        if not config:
            return

        table = config["table"]
        db = self._get_country_db(country)
        if not db:
            err = getattr(self, "_last_input_db_error", None)
            suffix = f": {err}" if err else ""
            self.input_info_var.set(f"Could not open database for {country}{suffix}")
            return

        try:
            from core.db.csv_importer import CSVImporter
            importer = CSVImporter(db)

            # Get table info
            info = importer.get_table_info(table, country=country)
            rows = importer.get_table_rows(table, limit=500, country=country)

            # Update info bar
            upload_info = ""
            if info.get("last_upload"):
                lu = info["last_upload"]
                upload_info = f" | Last upload: {lu['source_file']} ({lu['uploaded_at']})"
            self.input_info_var.set(f"{table}: {info['row_count']} rows{upload_info}")

            # Update Treeview
            self.input_tree.delete(*self.input_tree.get_children())

            if rows:
                # Filter out internal columns from display
                skip_cols = {"id", "uploaded_at", "created_at"}
                columns = [k for k in rows[0].keys() if k not in skip_cols]
                self.input_tree['columns'] = columns
                for col in columns:
                    self.input_tree.heading(col, text=col)
                    self.input_tree.column(col, width=120, minwidth=60)

                for row in rows:
                    values = [row.get(c, "") for c in columns]
                    self.input_tree.insert("", tk.END, values=values)
            else:
                self.input_tree['columns'] = ["(empty)"]
                self.input_tree.heading("(empty)", text="No data — upload a CSV to populate this table")

        except Exception as e:
            self.input_info_var.set(f"Error loading {table}: {e}")
        finally:
            db.close()

    def _upload_input_csv(self):
        """Open file dialog, import CSV into the selected input table."""
        import tkinter.filedialog as filedialog
        import tkinter.messagebox as messagebox

        country = self.input_scraper_var.get()
        config = self._get_selected_table_config()
        if not config:
            messagebox.showwarning("No Table", "Select a table first.")
            return

        csv_path = filedialog.askopenfilename(
            title=f"Upload CSV for {config['display']} ({country})",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialdir=str(self.repo_root / "input" / country) if (self.repo_root / "input" / country).exists() else str(self.repo_root / "input"),
        )
        if not csv_path:
            return

        from pathlib import Path
        csv_path = Path(csv_path)

        # Ask replace or append
        mode = "replace"
        if messagebox.askyesno("Import Mode",
                               f"Replace all existing data in '{config['display']}'?\n\n"
                               f"Yes = Replace (delete old, import new)\n"
                               f"No = Append (add to existing)"):
            mode = "replace"
        else:
            mode = "append"

        db = self._get_country_db(country)
        if not db:
            err = getattr(self, "_last_input_db_error", None)
            suffix = f"\n\nDetails: {err}" if err else ""
            messagebox.showerror("Error", f"Could not open database for {country}.{suffix}")
            return

        try:
            from core.db.csv_importer import CSVImporter
            importer = CSVImporter(db)

            # Validate schema first
            validation = importer.validate_csv(
                csv_path, config.get("column_map", {}), config.get("required", [])
            )
            if not validation["valid"]:
                messagebox.showerror("Validation Failed",
                    "Cannot import — schema validation failed:\n\n" +
                    "\n".join(validation["errors"]))
                return
            if validation["warnings"]:
                warn_msg = "\n".join(validation["warnings"])
                if not messagebox.askyesno("Validation Warnings",
                    f"Schema validation passed with warnings:\n\n{warn_msg}\n\nProceed with import?"):
                    return

            result = importer.import_csv(
                csv_path=csv_path,
                table=config["table"],
                column_map=config.get("column_map", {}),
                mode=mode,
                country=country,
            )

            if result.status == "ok":
                msg = (f"Imported {result.rows_imported} rows into {config['display']}\n"
                       f"Source: {result.source_file}\n"
                       f"Columns mapped: {', '.join(result.columns_mapped)}")
                if result.columns_unmapped:
                    msg += f"\nColumns skipped: {', '.join(result.columns_unmapped)}"
                if result.rows_skipped:
                    msg += f"\nRows skipped (duplicates): {result.rows_skipped}"
                messagebox.showinfo("Import Complete", msg)
            elif result.status == "warning":
                messagebox.showwarning("Import Warning", result.message)
            else:
                messagebox.showerror("Import Error", result.message)

            # Refresh preview
            self._preview_input_table()

        except Exception as e:
            messagebox.showerror("Import Error", f"Failed to import: {e}")
        finally:
            db.close()

    def _export_input_csv(self):
        """Export the selected input table to a CSV file."""
        import tkinter.filedialog as filedialog
        import tkinter.messagebox as messagebox

        country = self.input_scraper_var.get()
        config = self._get_selected_table_config()
        if not config:
            messagebox.showwarning("No Table", "Select a table first.")
            return

        save_path = filedialog.asksaveasfilename(
            title=f"Export {config['display']} ({country})",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
            initialfile=f"{config['table']}_{country}.csv",
        )
        if not save_path:
            return

        from pathlib import Path
        db = self._get_country_db(country)
        if not db:
            err = getattr(self, "_last_input_db_error", None)
            suffix = f"\n\nDetails: {err}" if err else ""
            messagebox.showerror("Error", f"Could not open database for {country}.{suffix}")
            return

        try:
            from core.db.csv_importer import CSVImporter
            importer = CSVImporter(db)
            msg = importer.export_table_csv(config["table"], Path(save_path), country=country)
            messagebox.showinfo("Export", msg)
        except Exception as e:
            messagebox.showerror("Export Error", str(e))
        finally:
            db.close()

    def _show_schema_info(self):
        """Show expected schema / column mapping for the selected table."""
        import tkinter.messagebox as messagebox

        country = self.input_scraper_var.get()
        config = self._get_selected_table_config()
        if not config:
            messagebox.showwarning("No Table", "Select a table first.")
            return

        lines = [f"Table: {config['table']}", f"Display: {config['display']}", ""]
        lines.append("CSV Column → DB Column mapping:")
        for csv_col, db_col in config.get("column_map", {}).items():
            lines.append(f"  {csv_col} → {db_col}")
        lines.append("")
        req = config.get("required", [])
        lines.append(f"Required DB columns: {', '.join(req) if req else '(none)'}")

        messagebox.showinfo(f"Schema: {config['display']}", "\n".join(lines))

    def setup_logs_tab(self, parent):
        """Setup logs viewer panel"""
        self.setup_log_status_panel(parent)
        self.setup_console_panel(parent)
    
    def setup_log_status_panel(self, parent):
        """Setup system status and execution controls (without console)"""
        # System Status frame (FIRST - at the top) - white background with light gray border
        stats_frame = tk.Frame(parent, bg=self.colors['white'],
                               highlightthickness=1,
                               highlightbackground=self.colors['border_gray'],
                               bd=0)
        stats_frame.pack(fill=tk.X, padx=8, pady=(8, 12))
        
        # Label for the section
        tk.Label(stats_frame, text="System Status",
                font=self.fonts['bold'],
                bg=self.colors['white'],
                fg='#000000').pack(anchor=tk.W, padx=16, pady=(16, 4))
        
        # Subtle horizontal separator below label
        separator = tk.Frame(stats_frame, height=1, bg=self.colors['border_light'])
        separator.pack(fill=tk.X, padx=16, pady=(0, 8))
        
        # Create container for formatted layout - white background
        stats_container = tk.Frame(stats_frame, bg=self.colors['white'])
        stats_container.pack(fill=tk.X, padx=5, pady=8)
        
        # Line 1: Chrome, Tor, RAM, CPU
        stats_line1 = tk.Frame(stats_container, bg=self.colors['white'])
        stats_line1.pack(fill=tk.X, pady=3)
        
        self.system_stats_label_line1 = tk.Label(stats_line1,
                                                 text="Chrome Instances (tracked): 0  |  Tor Instances: 0  |  RAM Usage: --  |  CPU Usage: --",
                                                 bg=self.colors['white'],
                                                 fg='#000000',
                                                 font=self.fonts['standard'],
                                                 anchor=tk.W)
        self.system_stats_label_line1.pack(side=tk.LEFT, padx=8)
        
        # Line 2: GPU, Network
        stats_line2 = tk.Frame(stats_container, bg=self.colors['white'])
        stats_line2.pack(fill=tk.X, pady=3)
        
        self.system_stats_label_line2 = tk.Label(stats_line2,
                                                 text="GPU Usage: --  |  Network: --",
                                                 bg=self.colors['white'],
                                                 fg='#000000',
                                                 font=self.fonts['standard'],
                                                 anchor=tk.W)
        self.system_stats_label_line2.pack(side=tk.LEFT, padx=8)
        
        # Keep old label for backward compatibility (will be updated but not displayed)
        self.system_stats_label = self.system_stats_label_line1
        
        # Start periodic system stats update
        self.update_system_stats()
        
        # Execution Status section (BELOW System Status) - white background with light border
        execution_status_frame = tk.Frame(parent, bg=self.colors['white'],
                                          highlightthickness=1,
                                          highlightbackground=self.colors['border_gray'],
                                          bd=0)
        execution_status_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))
        
        # Label for the section
        tk.Label(execution_status_frame, text="Execution Status", 
                font=self.fonts['bold'],
                bg=self.colors['white'],
                fg='#000000').pack(anchor=tk.W, padx=16, pady=(16, 4))
        
        # Subtle horizontal separator below label
        separator = tk.Frame(execution_status_frame, height=1, bg=self.colors['border_light'])
        separator.pack(fill=tk.X, padx=16, pady=(0, 8))
        
        # Progress frame - white background (2 lines)
        progress_frame = tk.Frame(execution_status_frame, bg=self.colors['white'])
        progress_frame.pack(fill=tk.X, padx=5, pady=(5, 10))
        
        # Line 1: Current status label
        status_line = tk.Frame(progress_frame, bg=self.colors['white'])
        status_line.pack(fill=tk.X, pady=(0, 2))
        
        tk.Label(
            status_line,
            text="Current status:",
            bg=self.colors['white'],
            fg='#000000',
            font=self.fonts['standard']
        ).pack(side=tk.LEFT, padx=(0, 5))

        # Lines 2-3: Current status text (wrapped)
        status_text_line = tk.Frame(progress_frame, bg=self.colors['white'])
        status_text_line.pack(fill=tk.X, pady=(0, 6))

        self.progress_label = tk.Label(
            status_text_line,
            text="Ready",
            bg=self.colors['white'],
            fg='#000000',
            font=self.fonts['standard'],
            justify=tk.LEFT,
            anchor='w',
            wraplength=520,
            height=2
        )
        self.progress_label.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        # Line 2: Progress bar
        bar_line = tk.Frame(progress_frame, bg=self.colors['white'])
        bar_line.pack(fill=tk.X)
        
        self.progress_bar = ttk.Progressbar(
            bar_line,
            mode='determinate',
            maximum=100,
            value=0,
            style='Modern.Horizontal.TProgressbar',
            length=480
        )
        self.progress_bar.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        # Progress percentage label (on same line as bar, right side)
        self.progress_percent = tk.Label(
            bar_line,
            text="0%",
            width=5,
            anchor=tk.E,
            bg=self.colors['white'],
            fg='#000000',
            font=self.fonts['standard']
        )
        self.progress_percent.pack(side=tk.RIGHT, padx=(10, 0))

        # DB Activity panel (below progress bar, fills remaining space)
        db_activity_frame = tk.Frame(execution_status_frame, bg=self.colors['white'])
        db_activity_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=(0, 5))

        tk.Label(
            db_activity_frame,
            text="DB Activity:",
            bg=self.colors['white'],
            fg='#666666',
            font=self.fonts['standard'],
            anchor='w'
        ).pack(anchor=tk.W)

        self.db_activity_text = tk.Text(
            db_activity_frame,
            height=20,
            wrap=tk.WORD,
            font=("Consolas", 8),
            bg='#1e1e1e',
            fg='#00cc66',
            insertbackground='#00cc66',
            relief=tk.FLAT,
            bd=1,
            highlightthickness=1,
            highlightbackground=self.colors['border_light'],
            state=tk.DISABLED,
        )
        self.db_activity_text.pack(fill=tk.BOTH, expand=True, pady=(2, 0))

        # Tag for different DB activity types
        self.db_activity_text.tag_configure("claim", foreground="#66ccff")
        self.db_activity_text.tag_configure("upsert", foreground="#ffcc00")
        self.db_activity_text.tag_configure("ok", foreground="#00cc66")
        self.db_activity_text.tag_configure("fail", foreground="#ff5555")
        self.db_activity_text.tag_configure("seed", foreground="#cc99ff")
        self.db_activity_text.tag_configure("finish", foreground="#ffffff")

    def setup_console_panel(self, parent):
        """Setup console viewer panel"""
        # Execution Log header + actions (above console)
        console_header = tk.Frame(parent, bg=self.colors['white'],
                                  highlightthickness=0,
                                  bd=0)
        console_header.pack(fill=tk.X, padx=8, pady=(8, 0))

        tk.Label(console_header, text="Execution Log", 
                 font=self.fonts['bold'],
                 bg=self.colors['white'],
                 fg='#000000').pack(anchor=tk.W, padx=16, pady=(8, 4))

        separator = tk.Frame(console_header, height=1, bg=self.colors['border_light'])
        separator.pack(fill=tk.X, padx=16, pady=(0, 8))

        toolbar = tk.Frame(console_header, bg=self.colors['white'])
        toolbar.pack(fill=tk.X, padx=16, pady=(0, 8))

        ttk.Button(toolbar, text="Clear", command=self.clear_logs,
                   style='Secondary.TButton').pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(toolbar, text="Copy to Clipboard", command=self.copy_logs_to_clipboard,
                   style='Secondary.TButton').pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(toolbar, text="Save Log", command=self.save_log,
                   style='Secondary.TButton').pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(toolbar, text="Archive Log", command=self.archive_current_log,
                   style='Secondary.TButton').pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(toolbar, text="Open in Cursor", command=self.open_console_in_cursor,
                   style='Secondary.TButton').pack(side=tk.LEFT, padx=(0, 8))

        # Log viewer - CRITICAL: Black background with yellow text
        log_viewer_frame = tk.Frame(
            parent,
            bg=self.colors['dark_gray'],
            highlightthickness=0,
            bd=0,
            relief='flat'
        )
        log_viewer_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))

        self.log_text = scrolledtext.ScrolledText(
            log_viewer_frame,
            wrap=tk.WORD,
            font=self.fonts['monospace'],  # Monospace font
            state=tk.DISABLED,
            bg=self.colors['console_black'],  # Pure black background
            fg=self.colors['console_yellow'],  # Yellow/gold text
            insertbackground=self.colors['console_yellow'],  # Yellow cursor
            selectbackground="#333333",  # Dark gray selection
            selectforeground=self.colors['console_yellow'],  # Yellow selected text
            borderwidth=0,
            relief='flat',
            highlightthickness=0,
            padx=16,
            pady=16
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
    def setup_output_tab(self, parent):
        """Alias for setup_output_tab_db - sets up the DB-backed output browser."""
        self.setup_output_tab_db(parent)

    def setup_output_tab_db(self, parent):
        """Setup DB-backed output browser tab - queries PostgreSQL tables directly."""
        container = tk.Frame(parent, bg=self.colors['white'])
        container.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        screen_width = self.root.winfo_screenwidth()

        # Left panel (controls)
        left_panel = ttk.Frame(container)
        left_panel.configure(style='TFrame')
        left_panel.pack(side=tk.LEFT, fill=tk.Y, expand=False)
        left_panel.config(width=int(screen_width * 0.17))
        left_panel.pack_propagate(False)

        # Right panel (data table)
        right_panel = tk.Frame(container, bg=self.colors['white'])
        right_panel.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # --- Selection section ---
        selection_section = tk.Frame(left_panel, bg=self.colors['white'],
                                     highlightbackground=self.colors['border_gray'],
                                     highlightthickness=1)
        selection_section.pack(fill=tk.X, padx=8, pady=(6, 4))

        tk.Label(selection_section, text="Output Browser", bg=self.colors['white'], fg='#000000',
                 font=self.fonts['bold']).pack(anchor=tk.W, padx=8, pady=(6, 4))

        form = tk.Frame(selection_section, bg=self.colors['white'])
        form.pack(fill=tk.X, padx=8, pady=(0, 8))

        tk.Label(form, text="Scraper:", bg=self.colors['white'], fg='#000000',
                 font=self.fonts['standard']).pack(anchor=tk.W)

        self.output_scraper_var = tk.StringVar()
        self.output_scraper_combo = ttk.Combobox(
            form, textvariable=self.output_scraper_var,
            state="readonly", width=18, style='Modern.TCombobox')
        self.output_scraper_combo['values'] = list(self.scrapers.keys())
        self.output_scraper_combo.pack(fill=tk.X, pady=(2, 8))
        if self.scrapers:
            self.output_scraper_var.set(list(self.scrapers.keys())[0])
        self.output_scraper_combo.bind("<<ComboboxSelected>>", lambda e: self._on_output_scraper_selected())

        tk.Label(form, text="Table:", bg=self.colors['white'], fg='#000000',
                 font=self.fonts['standard']).pack(anchor=tk.W)

        self.output_table_var = tk.StringVar()
        self.output_table_combo = ttk.Combobox(
            form, textvariable=self.output_table_var,
            state="readonly", width=18, style='Modern.TCombobox')
        # Ensure dropdown is visible and clickable
        self.output_table_combo.config(state="readonly")
        self.output_table_combo.pack(fill=tk.X, pady=(2, 8))
        self.output_table_combo.bind("<<ComboboxSelected>>", lambda e: self._on_output_table_selected())
        # Use postcommand so opening the dropdown never gets blocked by click handlers.
        self.output_table_combo.configure(postcommand=self._ensure_output_tables_populated)

        tk.Label(form, text="Run ID:", bg=self.colors['white'], fg='#000000',
                 font=self.fonts['standard']).pack(anchor=tk.W)

        self.output_run_var = tk.StringVar()
        self.output_run_combo = ttk.Combobox(
            form, textvariable=self.output_run_var,
            state="readonly", width=18, style='Modern.TCombobox')
        self.output_run_combo.pack(fill=tk.X, pady=(2, 8))
        self.output_run_combo.bind("<<ComboboxSelected>>", lambda e: self._load_output_data())

        tk.Label(form, text="Status Filter:", bg=self.colors['white'], fg='#000000',
                 font=self.fonts['standard']).pack(anchor=tk.W)

        self.output_status_filter_var = tk.StringVar(value="All")
        self.output_status_filter_combo = ttk.Combobox(
            form, textvariable=self.output_status_filter_var,
            values=["All", "running", "completed", "failed", "cancelled", "resume", "stopped", "partial"],
            state="readonly", width=18, style='Modern.TCombobox')
        self.output_status_filter_combo.pack(fill=tk.X, pady=(2, 8))
        self.output_status_filter_combo.bind("<<ComboboxSelected>>", lambda e: self._on_status_filter_changed())

        # --- Actions section ---
        actions_section = tk.Frame(left_panel, bg=self.colors['white'],
                                   highlightbackground=self.colors['border_gray'],
                                   highlightthickness=1)
        actions_section.pack(fill=tk.X, padx=8, pady=(0, 4))

        tk.Label(actions_section, text="Actions", bg=self.colors['white'], fg='#000000',
                 font=self.fonts['bold']).pack(anchor=tk.W, padx=8, pady=(6, 3))

        self._output_refresh_btn = ttk.Button(actions_section, text="Refresh", command=self._refresh_output_tables,
                   style='Secondary.TButton')
        self._output_refresh_btn.pack(fill=tk.X, padx=8, pady=(0, 4))
        self._output_export_btn = ttk.Button(actions_section, text="Export CSV", command=self._export_output_csv,
                   style='Primary.TButton')
        self._output_export_btn.pack(fill=tk.X, padx=8, pady=(0, 4))
        self._output_delete_table_btn = ttk.Button(actions_section, text="Delete run_id (table)", command=self._delete_run_id_in_selected_table,
                   style='Secondary.TButton')
        self._output_delete_table_btn.pack(fill=tk.X, padx=8, pady=(0, 4))
        self._output_delete_all_btn = ttk.Button(actions_section, text="Delete run_id (all tables)", command=self._delete_run_id_all_tables,
                   style='Secondary.TButton')
        self._output_delete_all_btn.pack(fill=tk.X, padx=8, pady=(0, 4))
        self._output_delete_market_btn = ttk.Button(actions_section, text="Delete market records", command=self._delete_all_records,
                   style='Secondary.TButton')
        self._output_delete_market_btn.pack(fill=tk.X, padx=8, pady=(0, 4))
        self._output_set_resume_btn = ttk.Button(actions_section, text="Set as Resume", command=self._set_run_id_as_resume,
                   style='Primary.TButton')
        self._output_set_resume_btn.pack(fill=tk.X, padx=8, pady=(0, 8))

        # --- Data reset section (moved from Dashboard) ---
        self._data_reset_use_output = True
        data_reset_section = tk.Frame(left_panel, bg=self.colors['white'],
                                      highlightbackground=self.colors['border_gray'],
                                      highlightthickness=1)
        data_reset_section.pack(fill=tk.X, padx=8, pady=(0, 6))
        self.setup_data_reset_section(data_reset_section)

        # --- Right panel header ---
        header = tk.Frame(right_panel, bg=self.colors['white'])
        header.pack(fill=tk.X, padx=8, pady=(6, 0))

        tk.Label(header, text="Data Preview", bg=self.colors['white'], fg='#000000',
                 font=self.fonts['bold']).pack(side=tk.LEFT)

        self.output_row_count_label = tk.Label(
            header, text="", bg=self.colors['white'], fg='#666666',
            font=self.fonts['standard'])
        self.output_row_count_label.pack(side=tk.RIGHT)

        # --- Treeview data grid ---
        tree_frame = tk.Frame(right_panel, bg=self.colors['white'],
                              highlightbackground=self.colors['border_gray'],
                              highlightthickness=1)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=(6, 8))

        # Scrollbars
        tree_scroll_y = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL)
        tree_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
        tree_scroll_x = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL)
        tree_scroll_x.pack(side=tk.BOTTOM, fill=tk.X)

        self.output_tree = ttk.Treeview(
            tree_frame, show="headings",
            yscrollcommand=tree_scroll_y.set,
            xscrollcommand=tree_scroll_x.set)
        self.output_tree.pack(fill=tk.BOTH, expand=True)
        tree_scroll_y.config(command=self.output_tree.yview)
        tree_scroll_x.config(command=self.output_tree.xview)

        # --- Status bar ---
        self.output_status_label = tk.Label(
            right_panel, text="Select a table to view data",
            bg=self.colors['white'], fg='#666666',
            font=self.fonts['standard'], anchor='w')
        self.output_status_label.pack(fill=tk.X, padx=8)

        # Initialize
        self._on_output_scraper_selected()

    # --- DB Output helpers ---

    def _get_output_db(self):
        """Get PostgresDB connection for the scraper selected in the Output tab."""
        scraper_name = self.output_scraper_var.get() if hasattr(self, 'output_scraper_var') else None
        if not scraper_name:
            scraper_name = self.scraper_var.get() if hasattr(self, 'scraper_var') else None
        if not scraper_name:
            return None
        scraper_info = self.scrapers.get(scraper_name, {})
        country = scraper_info.get("country", scraper_name)
        return self._get_output_db_for_country(country)

    def _get_output_db_for_country(self, country):
        """Get PostgresDB connection for explicit country/scraper key."""
        if not country:
            return None
        try:
            from core.db.postgres_connection import PostgresDB
            db = PostgresDB(country)
            db.connect()
            return db
        except Exception:
            return None

    def _get_market_context(self):
        """Return (scraper_name, country) for the current Output tab selection."""
        scraper_name = self.output_scraper_var.get() if hasattr(self, 'output_scraper_var') else None
        if not scraper_name:
            scraper_name = self.scraper_var.get() if hasattr(self, 'scraper_var') else None
        if not scraper_name:
            return None, None
        scraper_info = self.scrapers.get(scraper_name, {})
        country = scraper_info.get("country", scraper_name)
        return scraper_name, country

    def _ensure_output_tables_populated(self):
        """Ensure tables are populated when dropdown is clicked."""
        if not hasattr(self, 'output_table_combo'):
            return
        # If dropdown is empty, refresh tables
        current_values = self.output_table_combo['values']
        if not current_values or len(current_values) == 0:
            # Defer refresh so the combobox interaction itself stays responsive.
            self.root.after(0, self._refresh_output_tables)
    
    def _on_output_scraper_selected(self):
        """Handle Output tab scraper selection changes."""
        scraper_name = self.output_scraper_var.get() if hasattr(self, "output_scraper_var") else None
        if scraper_name:
            self.update_reset_controls(scraper_name)
        # Defer DB-heavy refresh to keep dropdown interaction smooth.
        self.root.after(0, self._refresh_output_tables)
        self._update_output_lock_state()

    def _get_input_table_basenames(self):
        """Return base input table names from the CSV importer registry."""
        basenames = set()
        try:
            from core.db.csv_importer import INPUT_TABLE_REGISTRY, PCID_MAPPING_CONFIG
            for configs in INPUT_TABLE_REGISTRY.values():
                for cfg in configs:
                    base = cfg.get("table")
                    if base:
                        basenames.add(base)
            if PCID_MAPPING_CONFIG.get("table"):
                basenames.add(PCID_MAPPING_CONFIG["table"])
        except Exception:
            pass
        return basenames

    def _is_input_table(self, table_name: str, prefix: str) -> bool:
        """Return True if table is an input table or input tracking table."""
        if table_name in {"input_uploads", "pcid_mapping"}:
            return True
        # Protect any table matching {prefix}input_* pattern (from inputs.sql)
        if prefix and table_name.startswith(f"{prefix}input_"):
            return True
        # Protect tables that match the INPUT_TABLE_REGISTRY basenames
        basenames = self._get_input_table_basenames()
        for base in basenames:
            if prefix and table_name == f"{prefix}{base}":
                return True
        # Also protect dictionary tables (Argentina uses ar_dictionary)
        if prefix and table_name in {f"{prefix}dictionary"}:
            return True
        return False

    # Tables to exclude from output dropdown (deprecated/removed tables)
    EXCLUDED_OUTPUT_TABLES = {
        'ar_pcid_mappings',      # Deprecated: now uses CSV exports only
        'ar_pcid_reference',     # Deprecated: removed from schema
        # Note: nl_input_search_terms is correctly filtered as input table
    }

    LEGACY_RUN_TABLES = {
        "Argentina": {"ar_pcid_mappings"},
    }

    def _cleanup_legacy_run_tables(self, db, run_id: str, scraper_name: Optional[str]) -> None:
        """Remove rows from legacy tables that still reference run_id."""
        if not run_id or not scraper_name:
            return
        tables = self.LEGACY_RUN_TABLES.get(scraper_name, set())
        if not tables:
            return
        for table in tables:
            if self._table_has_column(db, table, "run_id"):
                db.execute(f'DELETE FROM "{table}" WHERE run_id = %s', (run_id,))

    def _filter_output_tables(self, all_tables, prefix, scraper_name=None):
        """Filter tables for Output tab: exclude input tables, keep shared + market tables."""
        from core.db.postgres_connection import SHARED_TABLES
        tables = []
        for table in all_tables:
            if table in self.EXCLUDED_OUTPUT_TABLES:
                continue
            if self._is_input_table(table, prefix):
                continue
            if scraper_name == "Argentina" and table == "http_requests":
                continue
            if table in SHARED_TABLES or (prefix and table.startswith(prefix)):
                tables.append(table)
        tables.sort(key=lambda t: (0 if t in SHARED_TABLES else 1, t))
        return tables

    def _get_shared_table_filter(self, table: str, scraper_name: str):
        """Return (where_sql, params) to filter shared tables by selected market."""
        if not scraper_name:
            return "", ()
        if table == "run_ledger":
            return "scraper_name = %s", (scraper_name,)
        if table in ("http_requests", "scraped_items"):
            return "run_id IN (SELECT run_id FROM run_ledger WHERE scraper_name = %s)", (scraper_name,)
        if table == "chrome_instances":
            return "scraper_name = %s", (scraper_name,)
        if table == "step_retries":
            return "run_id IN (SELECT run_id FROM run_ledger WHERE scraper_name = %s)", (scraper_name,)
        return "", ()

    def _next_output_async_token(self, kind: str) -> int:
        """Return a monotonically increasing token for Output tab async tasks."""
        current = int(self._output_async_tokens.get(kind, 0)) + 1
        self._output_async_tokens[kind] = current
        return current

    def _is_output_async_token_current(self, kind: str, token: int) -> bool:
        """Check whether async response is still the latest for given task kind."""
        return int(self._output_async_tokens.get(kind, 0)) == int(token)

    def _next_monitor_async_token(self, kind: str) -> int:
        """Return a monotonic token for monitoring tab async tasks."""
        if not hasattr(self, "_monitor_async_tokens"):
            self._monitor_async_tokens = {}
        current = int(self._monitor_async_tokens.get(kind, 0)) + 1
        self._monitor_async_tokens[kind] = current
        return current

    def _is_monitor_async_token_current(self, kind: str, token: int) -> bool:
        """Check whether monitoring async response is still current."""
        if not hasattr(self, "_monitor_async_tokens"):
            return False
        return int(self._monitor_async_tokens.get(kind, 0)) == int(token)

    def _refresh_output_tables(self):
        """Populate table dropdown with only tables for the selected market + global tables."""
        if not hasattr(self, 'output_table_combo'):
            return

        scraper_name, country = self._get_market_context()
        if not scraper_name or not country:
            self.output_table_combo['values'] = []
            self.output_table_var.set("")
            if hasattr(self, "output_run_combo"):
                self.output_run_combo['values'] = ["(All)"]
                self.output_run_var.set("(All)")
            self.output_status_label.config(text="Select a scraper to view tables")
            return

        selected_table = self.output_table_var.get() if hasattr(self, "output_table_var") else ""
        token = self._next_output_async_token("tables")
        self.output_status_label.config(text=f"Loading tables for {scraper_name}...")

        def worker():
            db = self._get_output_db_for_country(country)
            if not db:
                return {"error": "DB: Cannot connect to PostgreSQL", "scraper_name": scraper_name}
            try:
                cur = db.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name NOT LIKE '\\_%' ESCAPE '\\' "
                    "ORDER BY table_name"
                )
                all_tables = [row[0] for row in cur.fetchall()]
                prefix = getattr(db, '_prefix', '') or getattr(db, 'prefix', '')
                tables = self._filter_output_tables(all_tables, prefix, scraper_name=scraper_name)
                return {
                    "scraper_name": scraper_name,
                    "tables": tables,
                    "selected_table": selected_table,
                }
            except Exception as exc:
                return {
                    "error": f"Error loading tables: {str(exc)[:120]}",
                    "scraper_name": scraper_name,
                }
            finally:
                try:
                    db.close()
                except Exception:
                    pass

        def apply(payload):
            if not self._is_output_async_token_current("tables", token):
                return
            current_scraper = self.output_scraper_var.get() if hasattr(self, "output_scraper_var") else None
            if current_scraper != payload.get("scraper_name"):
                return

            if payload.get("error"):
                self.output_table_combo['values'] = []
                self.output_table_var.set("")
                if hasattr(self, "output_run_combo"):
                    self.output_run_combo['values'] = ["(All)"]
                    self.output_run_var.set("(All)")
                self.output_status_label.config(text=payload["error"])
                return

            tables = payload.get("tables", []) or []
            self.output_table_combo['values'] = tables
            self.output_table_combo.config(state="readonly")

            if tables:
                target = payload.get("selected_table")
                if target not in tables:
                    target = tables[0]
                self.output_table_var.set(target)
            else:
                self.output_table_var.set("")
                if hasattr(self, "output_run_combo"):
                    self.output_run_combo['values'] = ["(All)"]
                    self.output_run_var.set("(All)")
                try:
                    self.output_tree.delete(*self.output_tree.get_children())
                    self.output_tree['columns'] = []
                except Exception:
                    pass

            status_text = f"DB: PostgreSQL | {len(tables)} tables for this market"
            if len(tables) == 0:
                status_text += " (no tables found)"
            self.output_status_label.config(text=status_text)

            if tables:
                self._on_output_table_selected()

        def run_async():
            payload = worker()
            self.root.after(0, lambda: apply(payload))

        threading.Thread(target=run_async, daemon=True).start()

    def _get_market_tables(self, db):
        """Return list of tables for selected market (shared + market-prefixed)."""
        cur = db.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name NOT LIKE '\\_%' ESCAPE '\\' "
            "ORDER BY table_name")
        all_tables = [row[0] for row in cur.fetchall()]
        prefix = getattr(db, '_prefix', '') or getattr(db, 'prefix', '')
        scraper_name, _ = self._get_market_context()
        return self._filter_output_tables(all_tables, prefix, scraper_name=scraper_name)

    def _table_exists(self, db, table):
        """Check if a table exists in the database."""
        try:
            cur = db.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = %s",
                (table,))
            return cur.fetchone() is not None
        except Exception:
            return False

    def _table_has_column(self, db, table, column):
        try:
            cur = db.execute(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = %s AND column_name = %s",
                (table, column))
            return cur.fetchone() is not None
        except Exception:
            return False

    def _get_fk_safe_delete_order(self, db, tables):
        """Sort tables so FK children are deleted before parents.

        Queries PostgreSQL's information_schema to find FK relationships
        and returns tables in topological order (children first).
        """
        try:
            cur = db.execute("""
                SELECT
                    tc.table_name AS child_table,
                    ccu.table_name AS parent_table
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.constraint_column_usage AS ccu
                    ON tc.constraint_name = ccu.constraint_name
                    AND tc.table_schema = ccu.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                  AND tc.table_schema = 'public'
            """)
            fk_pairs = cur.fetchall()
        except Exception:
            return tables  # Can't query FKs, return original order

        # Build dependency graph: parent -> set of children
        table_set = set(tables)
        children_of = {}  # parent -> [children]
        for child, parent in fk_pairs:
            if child in table_set and parent in table_set:
                children_of.setdefault(parent, []).append(child)

        # Topological sort: children before parents
        visited = set()
        ordered = []

        def visit(t):
            if t in visited:
                return
            visited.add(t)
            for child in children_of.get(t, []):
                visit(child)
            ordered.append(t)

        for t in tables:
            visit(t)

        # ordered has children before parents (topological order)
        return ordered

    def _delete_shared_table_market_rows(self, db, table, scraper_name, prefix):
        """Delete only the selected market's rows from shared tables."""
        if table == "http_requests":
            db.execute(
                "DELETE FROM http_requests WHERE run_id IN "
                "(SELECT run_id FROM run_ledger WHERE scraper_name = %s)",
                (scraper_name,))
            return
        if table == "scraped_items":
            db.execute(
                "DELETE FROM scraped_items WHERE run_id IN "
                "(SELECT run_id FROM run_ledger WHERE scraper_name = %s)",
                (scraper_name,))
            return
        if table == "data_quality_checks":
            # Delete data_quality_checks for this scraper's runs
            # This must be deleted before run_ledger due to FK constraint
            db.execute(
                "DELETE FROM data_quality_checks WHERE run_id IN "
                "(SELECT run_id FROM run_ledger WHERE scraper_name = %s)",
                (scraper_name,))
            return
        if table == "run_ledger":
            # Increase lock timeout temporarily for run_ledger deletion to avoid lock timeouts
            # This is needed because foreign key checks from deleted tables can cause locks
            try:
                db.execute("SET lock_timeout = '30s'")
                db.execute("DELETE FROM run_ledger WHERE scraper_name = %s", (scraper_name,))
                db.execute("SET lock_timeout = '5s'")  # Reset to default
            except Exception as e:
                # Reset timeout even on error
                try:
                    db.execute("SET lock_timeout = '5s'")
                except Exception:
                    pass
                raise
            return
        if table == "pcid_mapping":
            db.execute("DELETE FROM pcid_mapping WHERE source_country = %s", (scraper_name,))
            return
        if table == "input_uploads":
            if prefix:
                db.execute("DELETE FROM input_uploads WHERE table_name LIKE %s", (f"{prefix}%",))
            return

    def _delete_all_records(self):
        """Delete all records for the selected market (shared + market tables). Confirmation required."""
        db = self._get_output_db()
        if not db:
            messagebox.showerror("Error", "Cannot connect to PostgreSQL.")
            return
        try:
            tables = self._get_market_tables(db)
            if not tables:
                messagebox.showinfo("Delete market records", "No tables found for this market.")
                db.close()
                return
            scraper_name, _ = self._get_market_context()
            prefix = getattr(db, '_prefix', '') or getattr(db, 'prefix', '')
            protected = {"_schema_versions"}
            legacy_tables = []
            if scraper_name:
                for legacy in self.LEGACY_RUN_TABLES.get(scraper_name, set()):
                    if self._table_has_column(db, legacy, "run_id"):
                        legacy_tables.append(legacy)
            display_tables = [t for t in tables if t not in protected]
            for legacy in legacy_tables:
                if legacy not in display_tables:
                    display_tables.append(legacy)
            ok = messagebox.askyesno(
                "Delete market records",
                f"This will permanently delete all rows for '{scraper_name}' from {len(display_tables)} table(s):\n\n"
                + ", ".join(display_tables[:15])
                + (" ..." if len(display_tables) > 15 else "")
                + "\n\nAre you sure?",
                icon="warning",
                default="no",
            )
            if not ok:
                db.close()
                return
            # Delete market-prefixed tables first to avoid FK issues with run_ledger
            shared_tables = {"run_ledger", "http_requests", "scraped_items", "_schema_versions"}
            
            # For Netherlands, delete in correct order to respect foreign key constraints
            # Order: child tables first, then parent tables, then run_ledger
            if scraper_name == "Netherlands":
                # Define deletion order for Netherlands tables (child -> parent)
                nl_deletion_order = [
                    "nl_costs",              # References nl_details
                    "nl_packs",              # References nl_collected_urls
                    "nl_details",            # Parent of nl_costs
                    "nl_collected_urls",     # Parent of nl_packs
                    "nl_consolidated",       # Standalone
                    "nl_chrome_instances",   # Standalone
                    "nl_input_search_terms", # Input table
                    "nl_step_progress",      # Standalone
                    "nl_export_reports",  # Standalone
                    "nl_errors",          # Standalone
                    "nl_products",        # Legacy
                    "nl_reimbursement",   # Legacy
                ]
                
                # Delete Netherlands tables in order
                # NOTE: db.execute() auto-commits each statement, so each
                # TRUNCATE/DELETE runs in its own transaction. No SAVEPOINTs needed.
                deleted_count = 0
                errors = []
                for table_name in nl_deletion_order:
                    if table_name in tables:
                        try:
                            db.execute(f'TRUNCATE TABLE "{table_name}" CASCADE')
                            deleted_count += 1
                            print(f"[DELETE] Truncated {table_name}", flush=True)
                        except Exception as e1:
                            # TRUNCATE failed — try DELETE as fallback
                            try:
                                db.execute(f'DELETE FROM "{table_name}"')
                                deleted_count += 1
                                print(f"[DELETE] Deleted all rows from {table_name}", flush=True)
                            except Exception as e2:
                                error_msg = f"{table_name}: {str(e2)}"
                                errors.append(error_msg)
                                print(f"[WARNING] Skipped {table_name}: {e2}", flush=True)

                # Delete any remaining nl_ tables not in the ordered list
                for table in tables:
                    if table.startswith("nl_") and table not in nl_deletion_order and table not in shared_tables and table not in protected:
                        try:
                            db.execute(f'TRUNCATE TABLE "{table}" CASCADE')
                            deleted_count += 1
                            print(f"[DELETE] Truncated {table}", flush=True)
                        except Exception as e:
                            try:
                                db.execute(f'DELETE FROM "{table}"')
                                deleted_count += 1
                                print(f"[DELETE] Deleted all rows from {table}", flush=True)
                            except Exception as e2:
                                error_msg = f"{table}: {str(e2)}"
                                errors.append(error_msg)
                                print(f"[WARNING] Skipped {table}: {e2}", flush=True)
                
                if deleted_count == 0:
                    error_summary = "\n".join(errors) if errors else "No tables found or all deletions failed"
                    raise Exception(f"No Netherlands tables were deleted.\n\nErrors:\n{error_summary}")
                elif errors:
                    print(f"[WARNING] Some tables had errors but {deleted_count} table(s) were deleted successfully.", flush=True)
            else:
                # For other scrapers, use FK-safe ordering
                # db.execute() auto-commits each statement — no SAVEPOINTs needed
                safe_tables = self._get_fk_safe_delete_order(db, [t for t in tables if t not in shared_tables and t not in protected])
                for table in safe_tables:
                    try:
                        db.execute(f'TRUNCATE TABLE "{table}" CASCADE')
                    except Exception as e:
                        try:
                            db.execute(f'DELETE FROM "{table}"')
                        except Exception as e2:
                            print(f"[WARNING] Failed to delete {table}: {e2}", flush=True)

            for legacy in legacy_tables:
                try:
                    db.execute(f'TRUNCATE TABLE "{legacy}" CASCADE')
                except Exception as e:
                    try:
                        db.execute(f'DELETE FROM "{legacy}"')
                    except Exception as e2:
                        print(f"[WARNING] Failed to delete {legacy}: {e2}", flush=True)

            # Then delete market rows in shared tables
            # IMPORTANT: Delete run_ledger LAST after all market tables are deleted
            # This avoids foreign key constraint lock timeouts
            if scraper_name:
                # Delete dependent shared tables first (in FK order: children before parents)
                # data_quality_checks references run_ledger, so delete it before run_ledger
                for shared in ("http_requests", "scraped_items", "data_quality_checks"):
                    try:
                        self._delete_shared_table_market_rows(db, shared, scraper_name, prefix)
                    except Exception as e:
                        # If table doesn't exist, that's OK - continue
                        if "does not exist" not in str(e).lower() and "relation" not in str(e).lower():
                            print(f"[WARNING] Failed to delete {shared}: {e}", flush=True)
                
                # Delete from run_ledger last (this can timeout if FK checks are still pending)
                # We increase lock timeout in _delete_shared_table_market_rows for this
                try:
                    self._delete_shared_table_market_rows(db, "run_ledger", scraper_name, prefix)
                except Exception as e:
                    if "lock timeout" in str(e).lower() or "canceling statement due to lock timeout" in str(e).lower():
                        # If lock timeout, try one more time with even longer timeout
                        print(f"[WARNING] Lock timeout on run_ledger, retrying with longer timeout...", flush=True)
                        try:
                            db.execute("SET lock_timeout = '60s'")
                            db.execute("DELETE FROM run_ledger WHERE scraper_name = %s", (scraper_name,))
                            db.execute("SET lock_timeout = '5s'")
                            print(f"[DELETE] Successfully deleted run_ledger rows on retry", flush=True)
                        except Exception as e2:
                            try:
                                db.execute("SET lock_timeout = '5s'")
                            except Exception:
                                pass
                            raise Exception(f"Failed to delete run_ledger even with extended timeout: {e2}")
                    else:
                        raise
            
            db.close()
            messagebox.showinfo("Delete market records", f"All records deleted for '{scraper_name}'.")
            self._refresh_output_tables()
        except Exception as e:
            try:
                db.close()
            except Exception:
                pass
            import traceback
            error_details = traceback.format_exc()
            print(f"[ERROR] Delete failed: {error_details}", flush=True)
            messagebox.showerror("Error", f"Failed to delete records:\n\n{str(e)}\n\nCheck console for details.")

    def _delete_run_id_in_selected_table(self):
        """Delete rows for the selected run_id in the selected table."""
        table = self.output_table_var.get()
        run_id = self.output_run_var.get()
        if not table:
            messagebox.showwarning("Delete run_id (table)", "Select a table first.")
            return
        if not run_id or run_id == "(All)":
            messagebox.showwarning("Delete run_id (table)", "Select a specific run_id.")
            return
        db = self._get_output_db()
        if not db:
            messagebox.showerror("Error", "Cannot connect to PostgreSQL.")
            return
        try:
            if not self._table_has_column(db, table, "run_id"):
                messagebox.showinfo("Delete run_id (table)", f"Table '{table}' has no run_id column.")
                db.close()
                return
            ok = messagebox.askyesno(
                "Delete run_id (table)",
                f"Delete rows from '{table}' for run_id = {run_id}?\n\nThis cannot be undone.",
                icon="warning",
                default="no",
            )
            if not ok:
                db.close()
                return
            scraper_name, _ = self._get_market_context()
            if table == "run_ledger":
                self._cleanup_legacy_run_tables(db, run_id, scraper_name)
            
            # For run_ledger, increase lock timeout to avoid lock timeout errors
            if table == "run_ledger":
                try:
                    db.execute("SET lock_timeout = '30s'")
                    db.execute(f'DELETE FROM "{table}" WHERE run_id = %s', (run_id,))
                    db.execute("SET lock_timeout = '5s'")
                except Exception as e:
                    try:
                        db.execute("SET lock_timeout = '5s'")
                    except Exception:
                        pass
                    if "lock timeout" in str(e).lower() or "canceling statement due to lock timeout" in str(e).lower():
                        # Retry with longer timeout
                        try:
                            print(f"[WARNING] Lock timeout on run_ledger, retrying with longer timeout...", flush=True)
                            db.execute("SET lock_timeout = '60s'")
                            db.execute(f'DELETE FROM "{table}" WHERE run_id = %s', (run_id,))
                            db.execute("SET lock_timeout = '5s'")
                            print(f"[DELETE] Successfully deleted run_ledger row on retry", flush=True)
                        except Exception as e2:
                            try:
                                db.execute("SET lock_timeout = '5s'")
                            except Exception:
                                pass
                            raise Exception(f"Failed to delete run_ledger even with extended timeout: {e2}")
                    else:
                        raise
            else:
                db.execute(f'DELETE FROM "{table}" WHERE run_id = %s', (run_id,))
            
            db.close()
            messagebox.showinfo("Delete run_id (table)", f"Deleted rows in '{table}' for run_id = {run_id}.")
            self._load_output_data()
        except Exception as e:
            try:
                db.close()
            except Exception:
                pass
            messagebox.showerror("Error", f"Failed to delete run_id rows: {e}")

    def _set_run_id_as_resume(self):
        """Set the selected run_id status to 'running' in the database."""
        run_id = self.output_run_var.get()
        if not run_id or run_id == "(All)":
            messagebox.showwarning("Set as Resume", "Select a specific run_id first.")
            return
        
        scraper_name, _ = self._get_market_context()
        if not scraper_name:
            messagebox.showerror("Error", "Could not determine scraper name.")
            return
        
        db = self._get_output_db()
        if not db:
            messagebox.showerror("Error", "Cannot connect to PostgreSQL.")
            return
        
        try:
            # Update run_ledger status to 'running'
            db.execute(
                "UPDATE run_ledger SET status = 'running' WHERE run_id = %s AND scraper_name = %s",
                (run_id, scraper_name)
            )
            db.commit()
            db.close()
            
            messagebox.showinfo("Set as Resume", f"Successfully set run_id = {run_id} status to 'running'.")
            
            # Refresh the data to show updated status
            self._load_output_data()
        except Exception as e:
            try:
                db.close()
            except Exception:
                pass
            error_details = traceback.format_exc()
            print(f"[ERROR] Set as resume failed: {error_details}", flush=True)
            messagebox.showerror("Error", f"Failed to set run_id as resume:\n\n{str(e)}\n\nCheck console for details.")

    def _delete_run_id_all_tables(self):
        """Delete rows for the selected run_id across all market tables (and shared tables that have run_id)."""
        run_id = self.output_run_var.get()
        if not run_id or run_id == "(All)":
            messagebox.showwarning("Delete run_id (all tables)", "Select a specific run_id.")
            return
        db = self._get_output_db()
        if not db:
            messagebox.showerror("Error", "Cannot connect to PostgreSQL.")
            return
        try:
            scraper_name, _ = self._get_market_context()
            tables = self._get_market_tables(db)
            if not tables:
                messagebox.showinfo("Delete run_id (all tables)", "No tables found for this market.")
                db.close()
                return
            legacy_tables = []
            if scraper_name:
                for legacy in self.LEGACY_RUN_TABLES.get(scraper_name, set()):
                    if self._table_has_column(db, legacy, "run_id"):
                        legacy_tables.append(legacy)
            effective_tables = list(tables)
            for legacy in legacy_tables:
                if legacy not in effective_tables:
                    effective_tables.append(legacy)
            ok = messagebox.askyesno(
                "Delete run_id (all tables)",
                f"Delete rows for run_id = {run_id} across {len(effective_tables)} table(s)?\n\nThis cannot be undone.",
                icon="warning",
                default="no",
            )
            if not ok:
                db.close()
                return

            # Delete from all tables with run_id, but do run_ledger last (FKs)
            # Sort tables so FK children are deleted before parents
            # db.execute() auto-commits each statement — no SAVEPOINTs needed
            
            # First, delete from shared tables that reference run_ledger
            # These must be deleted before run_ledger due to FK constraints
            shared_tables_with_fk = ["http_requests", "scraped_items", "data_quality_checks"]
            for shared_table in shared_tables_with_fk:
                if self._table_exists(db, shared_table) and self._table_has_column(db, shared_table, "run_id"):
                    try:
                        db.execute(f'DELETE FROM "{shared_table}" WHERE run_id = %s', (run_id,))
                        print(f"[DELETE] Deleted from {shared_table} for run_id {run_id}", flush=True)
                    except Exception as del_err:
                        print(f"[DELETE] Warning: Could not delete from {shared_table}: {del_err}", flush=True)
            
            # Then delete from market-specific tables
            ordered_tables = self._get_fk_safe_delete_order(db, effective_tables)
            for table in ordered_tables:
                if table == "_schema_versions" or table == "run_ledger" or table in shared_tables_with_fk:
                    continue
                if self._table_has_column(db, table, "run_id"):
                    try:
                        db.execute(f'DELETE FROM "{table}" WHERE run_id = %s', (run_id,))
                    except Exception as del_err:
                        print(f"[DELETE] Warning: Could not delete from {table}: {del_err}", flush=True)

            if self._table_has_column(db, "run_ledger", "run_id"):
                self._cleanup_legacy_run_tables(db, run_id, scraper_name)
                # Delete from run_ledger LAST (after all FK children are deleted)
                # Use increased lock timeout to avoid lock timeout errors
                try:
                    db.execute("SET lock_timeout = '30s'")
                    db.execute('DELETE FROM "run_ledger" WHERE run_id = %s', (run_id,))
                    db.execute("SET lock_timeout = '5s'")
                except Exception as e:
                    try:
                        db.execute("SET lock_timeout = '5s'")
                    except Exception:
                        pass
                    if "lock timeout" in str(e).lower() or "canceling statement due to lock timeout" in str(e).lower():
                        # Retry with longer timeout
                        try:
                            print(f"[WARNING] Lock timeout on run_ledger, retrying with longer timeout...", flush=True)
                            db.execute("SET lock_timeout = '60s'")
                            db.execute('DELETE FROM "run_ledger" WHERE run_id = %s', (run_id,))
                            db.execute("SET lock_timeout = '5s'")
                            print(f"[DELETE] Successfully deleted run_ledger row on retry", flush=True)
                        except Exception as e2:
                            try:
                                db.execute("SET lock_timeout = '5s'")
                            except Exception:
                                pass
                            raise Exception(f"Failed to delete run_ledger even with extended timeout: {e2}")
                    else:
                        raise

            db.close()
            messagebox.showinfo("Delete run_id (all tables)", f"Deleted run_id = {run_id} across tables.")
            self._refresh_output_tables()
        except Exception as e:
            try:
                db.close()
            except Exception:
                pass
            messagebox.showerror("Error", f"Failed to delete run_id rows: {e}")

    def _delete_selected_table_records(self):
        """Delete records from the selected table (market-only for shared tables)."""
        table = self.output_table_var.get()
        if not table:
            messagebox.showwarning("Delete table data", "Select a table first.")
            return
        db = self._get_output_db()
        if not db:
            messagebox.showerror("Error", "Cannot connect to PostgreSQL.")
            return
        try:
            from core.db.postgres_connection import SHARED_TABLES
            scraper_name, _ = self._get_market_context()
            prefix = getattr(db, '_prefix', '') or getattr(db, 'prefix', '')

            if table == "_schema_versions":
                messagebox.showinfo("Delete table data", "Schema versions are shared and cannot be deleted per market.")
                db.close()
                return

            ok = messagebox.askyesno(
                "Delete table data",
                f"This will permanently delete data from '{table}' for '{scraper_name}'.\n\nAre you sure?",
                icon="warning",
                default="no",
            )
            if not ok:
                db.close()
                return

            if table in SHARED_TABLES:
                if table == "run_ledger":
                    db.close()
                    messagebox.showinfo(
                        "Delete table data",
                        "Run ledger has dependencies. Use 'Delete market records' to clear this market safely."
                    )
                    return
                if not scraper_name:
                    db.close()
                    messagebox.showerror("Delete table data", "No market selected.")
                    return
                self._delete_shared_table_market_rows(db, table, scraper_name, prefix)
            else:
                # For Netherlands tables, handle foreign key constraints properly
                if scraper_name == "Netherlands" and table.startswith("nl_"):
                    # Check if this table has child tables that reference it
                    child_tables_map = {
                        "nl_collected_urls": ["nl_packs"],
                        "nl_details": ["nl_costs"],
                    }
                    
                    # Delete child tables first if this is a parent table
                    if table in child_tables_map:
                        for child_table in child_tables_map[table]:
                            try:
                                db.execute(f'TRUNCATE TABLE "{child_table}" CASCADE')
                                print(f"[DELETE] Deleted child table {child_table} first", flush=True)
                            except Exception as e:
                                try:
                                    db.execute(f'DELETE FROM "{child_table}"')
                                    print(f"[DELETE] Deleted rows from child table {child_table}", flush=True)
                                except Exception as e2:
                                    print(f"[WARNING] Could not delete child table {child_table}: {e2}", flush=True)
                    
                    # Now delete the requested table
                    try:
                        db.execute(f'TRUNCATE TABLE "{table}" CASCADE')
                        print(f"[DELETE] Truncated {table}", flush=True)
                    except Exception as e:
                        # Try DELETE if TRUNCATE fails
                        try:
                            db.execute(f'DELETE FROM "{table}"')
                            print(f"[DELETE] Deleted all rows from {table}", flush=True)
                        except Exception as e2:
                            raise Exception(f"Failed to delete {table}: {e2}")
                else:
                    # For other tables, use standard deletion
                    try:
                        db.execute(f'TRUNCATE TABLE "{table}" CASCADE')
                    except Exception as e:
                        # Fallback to DELETE if TRUNCATE fails
                        try:
                            db.execute(f'DELETE FROM "{table}"')
                        except Exception as e2:
                            raise Exception(f"Failed to delete {table}: {e2}")
            db.close()
            messagebox.showinfo("Delete table data", f"Deleted data from '{table}'.")
            self._refresh_output_tables()
        except Exception as e:
            try:
                db.close()
            except Exception:
                pass
            import traceback
            error_details = traceback.format_exc()
            print(f"[ERROR] Delete table failed: {error_details}", flush=True)
            messagebox.showerror("Error", f"Failed to delete table data:\n\n{str(e)}\n\nCheck console for details.")

    def _on_status_filter_changed(self):
        """When status filter changes, repopulate run_id dropdown."""
        self._populate_run_ids_with_status_filter()

    def _populate_run_ids_with_status_filter(self):
        """Populate run_id dropdown filtered by selected status from run_ledger."""
        table = self.output_table_var.get()
        scraper_name, country = self._get_market_context()
        if not table or not scraper_name or not country:
            if hasattr(self, "output_run_combo"):
                self.output_run_combo['values'] = ["(All)"]
                self.output_run_var.set("(All)")
            return

        status_filter = self.output_status_filter_var.get() if hasattr(self, 'output_status_filter_var') else "All"
        current_run = self.output_run_var.get() if hasattr(self, "output_run_var") else "(All)"
        token = self._next_output_async_token("runs")
        self.output_status_label.config(text=f"Loading run IDs for {table}...")

        def worker():
            db = self._get_output_db_for_country(country)
            if not db:
                return {"error": "DB: Cannot connect to PostgreSQL", "scraper_name": scraper_name, "table": table}
            try:
                cur = db.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = %s ORDER BY ordinal_position",
                    (table,),
                )
                columns = [row[0] for row in cur.fetchall()]
                if 'run_id' not in columns:
                    return {
                        "scraper_name": scraper_name,
                        "table": table,
                        "runs": ["(All)"],
                        "selected_run": "(All)",
                    }

                from core.db.postgres_connection import SHARED_TABLES
                filter_sql, filter_params = ("", ())
                if table in SHARED_TABLES:
                    filter_sql, filter_params = self._get_shared_table_filter(table, scraper_name)

                if status_filter != "All":
                    cur = db.execute(
                        "SELECT run_id FROM run_ledger WHERE scraper_name = %s AND status = %s ORDER BY run_id DESC",
                        (scraper_name, status_filter),
                    )
                    filtered_run_ids = {row[0] for row in cur.fetchall()}
                    if filter_sql:
                        cur = db.execute(
                            f"SELECT DISTINCT run_id FROM \"{table}\" WHERE {filter_sql} ORDER BY run_id DESC",
                            filter_params,
                        )
                    else:
                        cur = db.execute(f"SELECT DISTINCT run_id FROM \"{table}\" ORDER BY run_id DESC")
                    table_run_ids = [row[0] for row in cur.fetchall()]
                    runs = [rid for rid in table_run_ids if rid in filtered_run_ids]
                else:
                    if filter_sql:
                        cur = db.execute(
                            f"SELECT DISTINCT run_id FROM \"{table}\" WHERE {filter_sql} ORDER BY run_id DESC",
                            filter_params,
                        )
                    else:
                        cur = db.execute(f"SELECT DISTINCT run_id FROM \"{table}\" ORDER BY run_id DESC")
                    runs = [row[0] for row in cur.fetchall()]

                runs.insert(0, "(All)")
                selected_run = current_run if current_run in runs else "(All)"
                return {
                    "scraper_name": scraper_name,
                    "table": table,
                    "runs": runs,
                    "selected_run": selected_run,
                }
            except Exception as exc:
                return {
                    "error": f"Error loading run IDs: {str(exc)[:120]}",
                    "scraper_name": scraper_name,
                    "table": table,
                }
            finally:
                try:
                    db.close()
                except Exception:
                    pass

        def apply(payload):
            if not self._is_output_async_token_current("runs", token):
                return
            current_scraper = self.output_scraper_var.get() if hasattr(self, "output_scraper_var") else None
            current_table = self.output_table_var.get() if hasattr(self, "output_table_var") else None
            if current_scraper != payload.get("scraper_name") or current_table != payload.get("table"):
                return

            if payload.get("error"):
                self.output_status_label.config(text=payload["error"])
                if hasattr(self, "output_run_combo"):
                    self.output_run_combo['values'] = ["(All)"]
                    self.output_run_var.set("(All)")
                return

            runs = payload.get("runs", ["(All)"])
            self.output_run_combo['values'] = runs
            self.output_run_var.set(payload.get("selected_run", "(All)"))
            self._load_output_data()

        def run_async():
            payload = worker()
            self.root.after(0, lambda: apply(payload))

        threading.Thread(target=run_async, daemon=True).start()

    def _on_output_table_selected(self):
        """When a table is selected, populate the run_id dropdown."""
        self._populate_run_ids_with_status_filter()

    def _load_output_data(self):
        """Load data from selected table/run into the Treeview."""
        table = self.output_table_var.get()
        run_id = self.output_run_var.get()
        scraper_name, country = self._get_market_context()
        if not table or not scraper_name or not country:
            return
        token = self._next_output_async_token("data")
        self.output_status_label.config(text=f"Loading data for {table}...")

        def worker():
            db = self._get_output_db_for_country(country)
            if not db:
                return {"error": "DB: Cannot connect to PostgreSQL", "scraper_name": scraper_name, "table": table, "run_id": run_id}
            try:
                cur = db.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = %s ORDER BY ordinal_position",
                    (table,),
                )
                col_names = [row[0] for row in cur.fetchall()]

                from core.db.postgres_connection import SHARED_TABLES
                where_parts = []
                params = []
                if table in SHARED_TABLES:
                    filter_sql, filter_params = self._get_shared_table_filter(table, scraper_name)
                    if filter_sql:
                        where_parts.append(filter_sql)
                        params.extend(filter_params)

                if run_id and run_id != "(All)" and 'run_id' in col_names:
                    where_parts.append("run_id = %s")
                    params.append(run_id)

                where_clause = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
                cur = db.execute(f"SELECT * FROM \"{table}\"{where_clause} LIMIT 1000", tuple(params))
                rows = cur.fetchall()
                count_cur = db.execute(f"SELECT COUNT(*) FROM \"{table}\"{where_clause}", tuple(params))
                total_count = count_cur.fetchone()[0]

                return {
                    "scraper_name": scraper_name,
                    "table": table,
                    "run_id": run_id,
                    "col_names": col_names,
                    "rows": rows,
                    "total_count": total_count,
                }
            except Exception as exc:
                return {
                    "error": f"Error loading {table}: {str(exc)[:140]}",
                    "scraper_name": scraper_name,
                    "table": table,
                    "run_id": run_id,
                }
            finally:
                try:
                    db.close()
                except Exception:
                    pass

        def apply(payload):
            if not self._is_output_async_token_current("data", token):
                return
            current_scraper = self.output_scraper_var.get() if hasattr(self, "output_scraper_var") else None
            current_table = self.output_table_var.get() if hasattr(self, "output_table_var") else None
            current_run = self.output_run_var.get() if hasattr(self, "output_run_var") else None
            if current_scraper != payload.get("scraper_name") or current_table != payload.get("table") or current_run != payload.get("run_id"):
                return

            if payload.get("error"):
                self.output_status_label.config(text=payload["error"])
                return

            col_names = payload.get("col_names", [])
            rows = payload.get("rows", [])
            total_count = payload.get("total_count", 0)

            self.output_tree.delete(*self.output_tree.get_children())
            self.output_tree['columns'] = col_names
            for col in col_names:
                self.output_tree.heading(col, text=col, anchor='w')
                self.output_tree.column(col, width=120, minwidth=60, anchor='w')

            for row in rows:
                display = [str(v) if v is not None else "" for v in row]
                self.output_tree.insert("", tk.END, values=display)

            showing = min(len(rows), 1000)
            suffix = f" (showing {showing}/{total_count})" if total_count > 1000 else f" ({total_count} rows)"
            self.output_row_count_label.config(text=f"{table}{suffix}")
            run_label = f" | run_id={run_id}" if run_id and run_id != "(All)" else ""
            self.output_status_label.config(text=f"Loaded {table}{run_label} | {total_count} total rows")

        def run_async():
            payload = worker()
            self.root.after(0, lambda: apply(payload))

        threading.Thread(target=run_async, daemon=True).start()

    def _export_output_csv(self):
        """Export currently viewed table to CSV file."""
        table = self.output_table_var.get()
        run_id = self.output_run_var.get()
        db = self._get_output_db()
        if not table or not db:
            return
        from tkinter import filedialog
        out_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
            initialfile=f"{table}.csv")
        if not out_path:
            db.close()
            return
        try:
            import csv
            cur = db.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = %s ORDER BY ordinal_position",
                (table,))
            col_names = [row[0] for row in cur.fetchall()]
            from core.db.postgres_connection import SHARED_TABLES
            scraper_name, _ = self._get_market_context()
            where_parts = []
            params = []
            if table in SHARED_TABLES:
                filter_sql, filter_params = self._get_shared_table_filter(table, scraper_name)
                if filter_sql:
                    where_parts.append(filter_sql)
                    params.extend(filter_params)
            if run_id and run_id != "(All)" and 'run_id' in col_names:
                where_parts.append("run_id = %s")
                params.append(run_id)
            where_clause = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
            cur = db.execute(f"SELECT * FROM \"{table}\"{where_clause}", tuple(params))
            rows = cur.fetchall()
            db.close()
            with open(out_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(col_names)
                writer.writerows(rows)
            self.output_status_label.config(text=f"Exported {len(rows)} rows to {out_path}")
        except Exception as e:
            from tkinter import messagebox
            messagebox.showerror("Export Error", str(e))

    def setup_output_files_tab_legacy(self, parent):
        """Setup output files viewer tab"""
        # Toolbar - white background
        toolbar = tk.Frame(parent, bg=self.colors['white'])
        toolbar.pack(fill=tk.X, padx=8, pady=8)
        
        # Left side: Label and entry
        left_frame = tk.Frame(toolbar, bg=self.colors['white'])
        left_frame.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))
        
        tk.Label(left_frame, text="Output Directory:", 
                font=self.fonts['standard'],
                bg=self.colors['white'],
                fg='#000000').pack(side=tk.LEFT, padx=(0, 12))
        
        self.output_path_var = tk.StringVar()
        output_path_entry = ttk.Entry(left_frame, textvariable=self.output_path_var, width=40, style='Modern.TEntry')
        output_path_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 8))
        
        # Right side: Buttons (always visible)
        button_frame = tk.Frame(toolbar, bg=self.colors['white'])
        button_frame.pack(side=tk.RIGHT, padx=5)
        
        ttk.Button(button_frame, text="Refresh", command=self.refresh_output_files, 
                  style='Secondary.TButton').pack(side=tk.LEFT, padx=3)
        self.open_folder_button = ttk.Button(button_frame, text="Open Folder", 
                                           command=self.open_output_folder, style='Primary.TButton')
        self.open_folder_button.pack(side=tk.LEFT, padx=3)
        
        # File list - white background with light gray border
        file_list_frame = tk.Frame(parent, bg=self.colors['white'],
                                   highlightbackground=self.colors['border_gray'],
                                   highlightthickness=1)
        file_list_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        
        # Label for the section
        tk.Label(file_list_frame, text="Output Files", 
                font=self.fonts['bold'],
                bg=self.colors['white'],
                fg='#000000').pack(anchor=tk.W, pady=(0, 8))
        
        # Listbox with scrollbar - white background
        list_container = tk.Frame(file_list_frame, bg=self.colors['white'])
        list_container.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        scrollbar = ttk.Scrollbar(list_container)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.output_listbox = tk.Listbox(list_container, yscrollcommand=scrollbar.set,
                                        font=self.fonts['monospace'],  # Monospace
                                        bg=self.colors['white'],
                                        fg='#000000',
                                        selectbackground=self.colors['white'],
                                        selectforeground='#000000',
                                        borderwidth=0,
                                        relief='flat',
                                        highlightthickness=0,
                                        activestyle='none')
        self.output_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.output_listbox.yview)

        self.output_listbox.bind("<Double-Button-1>", self.open_output_file)

        # File info - white background with light gray border
        file_info_frame = tk.Frame(parent, bg=self.colors['white'],
                                   highlightbackground=self.colors['border_gray'],
                                   highlightthickness=1)
        file_info_frame.pack(fill=tk.X, padx=8, pady=8)

        # Label for the section
        tk.Label(file_info_frame, text="File Information", 
                font=self.fonts['bold'],
                bg=self.colors['white'],
                fg='#000000').pack(anchor=tk.W, pady=(0, 8))

        self.file_info_text = tk.Text(file_info_frame, height=8, wrap=tk.WORD,
                                      font=self.fonts['standard'], state=tk.DISABLED,
                                      bg=self.colors['white'],
                                      fg='#000000',
                                      borderwidth=0,
                                      relief='flat',
                                      highlightthickness=0,
                                      padx=16,
                                      pady=16)
        self.file_info_text.pack(fill=tk.BOTH, expand=True)
    
    def setup_final_output_tab(self, parent):
        """Setup final output viewer tab"""
        # Toolbar - white background
        toolbar = tk.Frame(parent, bg=self.colors['white'])
        toolbar.pack(fill=tk.X, padx=8, pady=8)
        
        tk.Label(toolbar, text="Final Output Directory:", 
                font=self.fonts['standard'],
                bg=self.colors['white'],
                fg='#000000').pack(side=tk.LEFT, padx=(0, 12))
        
        self.final_output_path_var = tk.StringVar()
        final_output_path_entry = ttk.Entry(toolbar, textvariable=self.final_output_path_var, width=50, style='Modern.TEntry')
        final_output_path_entry.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        
        # Set default to exports directory (will be updated when scraper is selected)
        # Default to repo root/exports for now, will be updated per scraper
        try:
            from core.config_manager import ConfigManager
            default_exports = ConfigManager.get_exports_dir()
            self.final_output_path_var.set(str(default_exports))
        except Exception:
            # Fallback to repo root/exports
            default_exports = self.repo_root / "exports"
            self.final_output_path_var.set(str(default_exports))
        
        ttk.Button(toolbar, text="Search", command=self.search_final_output, 
                  style='Primary.TButton').pack(side=tk.LEFT, padx=5)
        
        # File list - white background with light gray border
        file_list_frame = tk.Frame(parent, bg=self.colors['white'],
                                   highlightbackground=self.colors['border_gray'],
                                   highlightthickness=1)
        file_list_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        
        # Label for the section
        tk.Label(file_list_frame, text="Final Output Files", 
                font=self.fonts['bold'],
                bg=self.colors['white'],
                fg='#000000').pack(anchor=tk.W, pady=(0, 8))
        
        # Listbox with scrollbar - white background
        list_container = tk.Frame(file_list_frame, bg=self.colors['white'])
        list_container.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        scrollbar = ttk.Scrollbar(list_container)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.final_output_listbox = tk.Listbox(list_container, yscrollcommand=scrollbar.set,
                                               font=self.fonts['monospace'],  # Monospace
                                               bg=self.colors['white'],
                                               fg='#000000',
                                               selectbackground=self.colors['white'],
                                               selectforeground='#000000',
                                               borderwidth=0,
                                               relief='flat',
                                               highlightthickness=0,
                                               activestyle='none')
        self.final_output_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.final_output_listbox.yview)

        self.final_output_listbox.bind("<Double-Button-1>", self.open_final_output_file)
        self.final_output_listbox.bind("<<ListboxSelect>>", self.on_final_output_file_selected)

        # File preview/info - white background with light gray border
        file_info_frame = tk.Frame(parent, bg=self.colors['white'],
                                   highlightbackground=self.colors['border_gray'],
                                   highlightthickness=1)
        file_info_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        # Label for the section
        tk.Label(file_info_frame, text="Final Output Information", 
                font=self.fonts['bold'],
                bg=self.colors['white'],
                fg='#000000').pack(anchor=tk.W, pady=(0, 8))

        self.final_output_info_text = tk.Text(file_info_frame, wrap=tk.WORD,
                                              font=self.fonts['standard'], state=tk.DISABLED,
                                              bg=self.colors['white'],
                                              fg='#000000',
                                              borderwidth=0,
                                              relief='flat',
                                              highlightthickness=0,
                                              padx=16,
                                              pady=16)
        self.final_output_info_text.pack(fill=tk.BOTH, expand=True)
        
        # Buttons below the information table - white background
        button_frame = tk.Frame(parent, bg=self.colors['white'])
        button_frame.pack(fill=tk.X, padx=8, pady=8)
        
        ttk.Button(button_frame, text="Refresh", command=self.refresh_final_output_files, 
                  style='Secondary.TButton').pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Push to DB", command=self.push_to_database, 
                  style='Primary.TButton').pack(side=tk.LEFT, padx=5)
    
    def setup_config_tab(self, parent):
        """Setup configuration/environment editing tab"""
        # Use extracted ConfigTab module
        from gui.tabs import ConfigTab
        self.config_tab_instance = ConfigTab(parent, self)
        # Store reference to current_config_file at instance level for compatibility
        self.current_config_file = None
    
    def setup_rest_tab(self, parent):
        """Setup rest/utilities tab"""
        # Toolbar
        toolbar = ttk.Frame(parent)
        toolbar.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(toolbar, text="Utilities & Tools:").pack(side=tk.LEFT, padx=5)
        
        ttk.Button(toolbar, text="Refresh All", command=self.refresh_all_outputs).pack(side=tk.LEFT, padx=5)
        ttk.Button(toolbar, text="Statistics", command=self.show_statistics).pack(side=tk.LEFT, padx=5)
        ttk.Button(toolbar, text="Clean Temp", command=self.clean_temp_files).pack(side=tk.LEFT, padx=5)
        
        # Main content area
        content_frame = ttk.Frame(parent)
        content_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Left side - Quick actions
        left_frame = ttk.LabelFrame(content_frame, text="Quick Actions", padding=10)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=False, padx=5)
        
        ttk.Button(left_frame, text="Open Root Output",
                  command=lambda: os.startfile(str(self.repo_root / "output"))).pack(fill=tk.X, pady=2)
        ttk.Button(left_frame, text="View All Logs",
                  command=self.view_all_logs).pack(fill=tk.X, pady=2)
        ttk.Button(left_frame, text="Organize Outputs",
                  command=self.organize_outputs).pack(fill=tk.X, pady=2)
        ttk.Button(left_frame, text="Export Summary",
                  command=self.export_summary).pack(fill=tk.X, pady=2)
        
        # Right side - Info/Status
        right_frame = ttk.LabelFrame(content_frame, text="System Information", padding=10)
        right_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5)
        
        self.rest_info_text = scrolledtext.ScrolledText(right_frame, wrap=tk.WORD,
                                                       font=self.fonts['standard'], state=tk.DISABLED)
        self.rest_info_text.pack(fill=tk.BOTH, expand=True)
        
        # Initialize with system info
        self.update_rest_info()
        
    def load_documentation(self):
        """Load all documentation files from unified doc/ folder"""
        self.docs = {}
        
        # Load from unified doc/ folder at root
        doc_root = self.repo_root / "doc"
        if doc_root.exists():
            # Load platform-level docs (root of doc/)
            for doc_file in doc_root.glob("*.md"):
                if doc_file.is_file():
                    key = f"Platform - {doc_file.stem}"
                    self.docs[key] = doc_file
            
            # Load scraper-specific docs from doc/scrapers/ or doc/{scraper_name}/
            scrapers_doc_dir = doc_root / "scrapers"
            if scrapers_doc_dir.exists():
                for doc_file in scrapers_doc_dir.glob("*"):
                    if doc_file.is_file() and doc_file.suffix in [".md", ".txt"]:
                        # Extract scraper name from filename or use filename
                        scraper_name = doc_file.stem.upper()
                        if "CANADA" in scraper_name:
                            scraper_name = "canada_quebec"
                        elif "MALAYSIA" in scraper_name:
                            scraper_name = "Malaysia"
                        elif "ARGENTINA" in scraper_name or "PIPELINE" in scraper_name:
                            scraper_name = "Argentina"
                        elif "TAIWAN" in scraper_name:
                            scraper_name = "Taiwan"
                        key = f"{scraper_name} - {doc_file.name}"
                        self.docs[key] = doc_file
            
            # Also check for scraper-specific doc directories (doc/canada_quebec/, doc/Malaysia/, etc.)
            for scraper_name in ["canada_quebec", "Malaysia", "Argentina", "Taiwan"]:
                scraper_doc_dir = doc_root / scraper_name
                if scraper_doc_dir.exists():
                    for doc_file in scraper_doc_dir.glob("*.md"):
                        if doc_file.is_file():
                            key = f"{scraper_name} - {doc_file.name}"
                            self.docs[key] = doc_file
        
        # Update combo box
        self.doc_combo["values"] = sorted(self.docs.keys())
    
    def on_scraper_selected(self, event=None):
        """Handle scraper selection"""
        scraper_name = self.scraper_var.get()
        if not scraper_name:
            return
            
        self.current_scraper = scraper_name
        scraper_info = self.scrapers[scraper_name]
        
        # Clear explanation when scraper changes
        self.clear_explanation()

        # Enable/disable data reset controls based on script availability
        self.update_reset_controls(scraper_name)
        self._refresh_io_lock_states()
        
        # Update output path to scraper output directory (not runs directory)
        if hasattr(self, 'output_path_var'):
            # Use scraper-specific output directory
            try:
                from core.config_manager import ConfigManager
                # Migrated: pm = get_path_manager()
                output_dir = ConfigManager.get_output_dir(scraper_name)
                self.output_path_var.set(str(output_dir))
            except Exception:
                # Fallback to scraper output directory
                output_dir = scraper_info["path"] / "output"
                self.output_path_var.set(str(output_dir))
            
            # Refresh output files
            self.refresh_output_files()
        
        # Update Final Output path to scraper-specific exports directory
        if hasattr(self, 'final_output_path_var'):
            try:
                from core.config_manager import ConfigManager
                # Migrated: pm = get_path_manager()
                exports_dir = ConfigManager.get_exports_dir(scraper_name)
                self.final_output_path_var.set(str(exports_dir))
            except Exception:
                # Fallback to repo root/exports/{scraper_name}
                exports_dir = self.repo_root / "exports" / scraper_name
                self.final_output_path_var.set(str(exports_dir))
            
            # Refresh final output files (filtered by scraper)
            if hasattr(self, 'refresh_final_output_files'):
                self.refresh_final_output_files()

        # Sync Output and Input tabs to selected market so DB activities show only this market (like console)
        if hasattr(self, 'output_scraper_var') and scraper_name:
            self.output_scraper_var.set(scraper_name)
        if hasattr(self, 'input_scraper_var') and scraper_name:
            self.input_scraper_var.set(scraper_name)
        self._schedule_market_table_refresh(delay_ms=80)

        # Reset progress state when switching scrapers (to avoid showing stale data from previous runs)
        # Only reset if scraper is not currently running
        if scraper_name not in self.running_scrapers and scraper_name not in self.running_processes:
            # Check if scraper was stopped - keep stopped state, otherwise reset to ready
            if scraper_name in self.scraper_logs:
                log_content = self.scraper_logs[scraper_name]
                if "[STOPPED]" in log_content or "Pipeline stopped" in log_content:
                    # Keep stopped state
                    self.scraper_progress[scraper_name] = {"percent": 0, "description": "Pipeline stopped"}
                else:
                    # Reset to ready state (no stale progress from previous runs)
                    self.scraper_progress[scraper_name] = {"percent": 0, "description": f"Ready: {scraper_name}"}
            else:
                # No logs - initialize to ready
                self.scraper_progress[scraper_name] = {"percent": 0, "description": f"Ready: {scraper_name}"}
        
        # Load scraper-specific config file
        if hasattr(self, 'load_config_file'):
            self.load_config_file()
        
        # Always update log display when scraper selection changes
        # This ensures the user sees the selected scraper's progress
        self.update_log_display(scraper_name)
        
        # Update progress bar for the newly selected scraper
        self.update_progress_for_scraper(scraper_name)
        
        # Refresh button/checkpoint state once (update_checkpoint_status also refreshes timeline label)
        # Avoid duplicate refresh calls here because they can block UI while switching scrapers.
        self.refresh_run_button_state()
        
        # Update Chrome instance count
        self.update_chrome_count()
        
        # Update kill all Chrome button state
        self.update_kill_all_chrome_button_state()
        
        # Update network info display (force refresh so Tor/Direct reflects this scraper's config)
        self.update_network_info(scraper_name, force_refresh=True)
    
    def on_step_selected(self, event=None):
        """Handle step selection"""
        selection = self.steps_listbox.curselection()
        if not selection:
            return
        scraper_name = self.pipeline_steps_scraper_var.get() or self.scraper_var.get()
        if not scraper_name:
            return

        step_index = selection[0]
        scraper_info = self.scrapers.get(scraper_name)
        if not scraper_info:
            return
        step = scraper_info["steps"][step_index]
        
        self.current_step = step
        
        # Update step info first (always show step info)
        self.step_info_text.config(state=tk.NORMAL)
        self.step_info_text.delete(1.0, tk.END)
        info = f"Script: {step['script']}\n"
        info += f"Description: {step['desc']}\n"
        # Scripts are directly in the scraper path, not in a subdirectory
        if scraper_info.get("scripts_dir"):
            script_path = scraper_info["path"] / scraper_info["scripts_dir"] / step["script"]
        else:
            script_path = scraper_info["path"] / step["script"]
        info += f"Path: {script_path}"
        self.step_info_text.insert(1.0, info)
        self.step_info_text.config(state=tk.DISABLED)
        
        # Ensure widgets are visible (now directly in parent, no frames to check)
        if hasattr(self, 'explain_button') and not self.explain_button.winfo_viewable():
            self.explain_button.pack(side=tk.LEFT, padx=8, pady=(0, 8))
        
        # Clear explanation when step changes (so user sees explanation for new step)
        self.clear_explanation()
        
        # Enable explain button if script exists (button is always visible and enabled)
        if script_path.exists():
            self.explain_button.config(state=tk.NORMAL, text="Explain This Step (OpenAI API)")
            if hasattr(self, 'explain_info_label'):
                self.explain_info_label.config(text="Get AI-powered explanation using OpenAI")
        else:
            # Keep button visible and enabled, but show warning when clicked
            self.explain_button.config(state=tk.NORMAL, text="Explain This Step (Script not found)")
            if hasattr(self, 'explain_info_label'):
                self.explain_info_label.config(text="Script file not found - explanation unavailable")
    
    def on_doc_selected(self, event=None):
        """Handle documentation selection and format for display"""
        doc_key = self.doc_var.get()
        if not doc_key or doc_key not in self.docs:
            return
            
        doc_file = self.docs[doc_key]
        
        try:
            with open(doc_file, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
            
            self.doc_text.config(state=tk.NORMAL)
            self.doc_text.delete(1.0, tk.END)
            
            # Format content for better readability
            formatted_content = self.format_documentation(content, doc_file.suffix)
            self.doc_text.insert(1.0, formatted_content)
            
            # Apply formatting tags if markdown
            if doc_file.suffix in [".md", ".MD"]:
                self.apply_markdown_formatting()
            
            self.doc_text.config(state=tk.DISABLED)
            
            self.update_status(f"Loaded: {doc_file.name}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load document:\n{str(e)}")
    
    def load_explanation_cache(self):
        """Load cached explanations from file"""
        if self.explanation_cache_file.exists():
            try:
                with open(self.explanation_cache_file, 'r', encoding='utf-8') as f:
                    self.step_explanations = json.load(f)
            except Exception:
                self.step_explanations = {}
        else:
            self.step_explanations = {}
    
    def save_explanation_cache(self):
        """Save cached explanations to file"""
        try:
            with open(self.explanation_cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.step_explanations, f, indent=2, ensure_ascii=False)
        except Exception:
            pass  # Silently fail if cache can't be saved
    
    def get_file_mtime(self, file_path: Path) -> float:
        """Get file last modified timestamp to detect changes"""
        try:
            return file_path.stat().st_mtime
        except Exception:
            return 0.0
    
    def explain_step(self):
        """Explain the currently selected step using OpenAI"""
        if not self.current_step:
            messagebox.showinfo("Info", 
                "Please select a step from the 'Pipeline Steps' list above to get an AI explanation.\n\n"
                "The explanation will use OpenAI API to analyze the step's script and provide a detailed explanation of what it does.")
            return
        
        scraper_name = self.scraper_var.get()
        if not scraper_name:
            return
        
        scraper_info = self.scrapers[scraper_name]
        # Scripts are directly in the scraper path, not in a subdirectory
        if scraper_info.get("scripts_dir"):
            script_path = scraper_info["path"] / scraper_info["scripts_dir"] / self.current_step["script"]
        else:
            script_path = scraper_info["path"] / self.current_step["script"]
        
        if not script_path.exists():
            messagebox.showerror("Error", f"Script file not found:\n{script_path}")
            return
        
        # Disable button temporarily while fetching (will re-enable after)
        self.explain_button.config(state=tk.DISABLED, text="⏳ Generating explanation...")
        
        # Check if we have cached explanation and if file hasn't changed (using modification time)
        cache_key = str(script_path)
        file_mtime = self.get_file_mtime(script_path)
        
        cached_explanation = self.step_explanations.get(cache_key, {})
        cached_mtime = cached_explanation.get("file_mtime", 0.0)
        
        # If file modification time hasn't changed, use cached explanation
        if cached_explanation.get("explanation") and abs(cached_mtime - file_mtime) < 1.0:  # Allow 1 second tolerance
            # Use cached explanation
            self.show_explanation(cached_explanation["explanation"])
            # Re-enable button (already disabled above)
            self.explain_button.config(state=tk.NORMAL, text="Explain This Step (OpenAI API)")
            return
        
        # Need to get explanation from OpenAI
        if not _OPENAI_AVAILABLE:
            messagebox.showerror("Error", "OpenAI library not available. Install using: pip install openai")
            self.explain_button.config(state=tk.NORMAL, text="Explain This Step (OpenAI API)")
            return
        
        # Get OpenAI API key
        api_key = os.getenv("OPENAI_API_KEY", "")
        if not api_key:
            # Try to load from .env file
            try:
                from dotenv import load_dotenv
                env_file = self.repo_root / ".env"
                if env_file.exists():
                    try:
                        load_dotenv(env_file)
                    except Exception as e:
                        # Silently ignore dotenv parse errors - API key might be in environment or config files
                        print(f"Warning: Could not parse .env file: {e}")
                api_key = os.getenv("OPENAI_API_KEY", "")
            except ImportError:
                pass
        
        if not api_key:
            messagebox.showerror("Error", "OPENAI_API_KEY not found. Configure it in environment or .env file.")
            self.explain_button.config(state=tk.NORMAL, text="Explain This Step (OpenAI API)")
            return
        
        # Read script content
        try:
            with open(script_path, 'r', encoding='utf-8', errors='ignore') as f:
                script_content = f.read()
        except Exception as e:
            messagebox.showerror("Error", f"Failed to read script file:\n{str(e)}")
            self.explain_button.config(state=tk.NORMAL, text="Explain This Step (OpenAI API)")
            return
        
        # Show loading message
        self.show_explanation("Loading explanation from OpenAI...")
        
        # Get explanation in a separate thread
        def get_explanation():
            try:
                client = OpenAI(api_key=api_key)
                prompt = f"""Explain what this Python script does. Format your response as follows:

**TLDR:** (One sentence summary - maximum 100 words)

**Full Explanation:**
(Detailed explanation covering:)
1. Main purpose and functionality
2. Key steps and workflow
3. Important functions and their roles
4. Input/output expectations

Script name: {script_path.name}
Step description: {self.current_step['desc']}

Code:
```python
{script_content}
```

Provide a clear, concise explanation suitable for users who want to understand what this step does. Start with a brief TLDR, then provide the full explanation."""
                
                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "You are a helpful assistant that explains Python code clearly and concisely."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.3,
                    max_tokens=1000
                )
                
                # Extract explanation from response
                if not response.choices or len(response.choices) == 0:
                    raise ValueError("OpenAI API returned no choices in response")
                
                explanation = response.choices[0].message.content
                
                if not explanation:
                    raise ValueError("OpenAI API returned empty explanation")
                
                # Get current file modification time
                current_file_mtime = self.get_file_mtime(script_path)
                
                # Cache the explanation with file modification time
                self.step_explanations[cache_key] = {
                    "file_mtime": current_file_mtime,
                    "explanation": explanation,
                    "cache_timestamp": datetime.now().isoformat(),
                    "file_mtime_str": datetime.fromtimestamp(current_file_mtime).isoformat()
                }
                self.save_explanation_cache()
                
                # Update UI in main thread
                self.root.after(0, lambda: self.show_explanation(explanation))
                # Re-enable button after showing explanation
                self.root.after(0, lambda: self.explain_button.config(state=tk.NORMAL, text="Explain This Step"))
                
            except Exception as e:
                error_msg = f"Failed to get explanation from OpenAI:\n{str(e)}"
                self.root.after(0, lambda: messagebox.showerror("Error", error_msg))
                self.root.after(0, lambda: self.hide_explanation())
                # Re-enable button on error
                self.root.after(0, lambda: self.explain_button.config(state=tk.NORMAL, text="Explain This Step"))
        
        thread = threading.Thread(target=get_explanation, daemon=True)
        thread.start()
    
    def show_explanation(self, explanation: str):
        """Show explanation in the explanation panel with proper formatting"""
        self.explanation_text.config(state=tk.NORMAL)
        self.explanation_text.delete(1.0, tk.END)
        
        # Parse and format explanation with TLDR first
        formatted = self.format_explanation(explanation)
        self.explanation_text.insert(1.0, formatted)
        
        # Apply formatting tags
        self.apply_explanation_formatting()
        
        self.explanation_text.config(state=tk.DISABLED)
        
        # Show explanation panel below step information
        if not self.explanation_visible:
            # Pack explanation frame below step info
            self.explanation_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
            self.explanation_visible = True
        
        # Increase height when showing
        self.explanation_text.config(height=10)
    
    def format_explanation(self, explanation: str) -> str:
        """Format explanation to show TLDR first, then full explanation"""
        import re
        
        # Try to extract TLDR section
        tldr_match = re.search(r'\*\*TLDR:\*\*(.*?)(?=\*\*Full Explanation:\*\*|\*\*|$)', explanation, re.DOTALL | re.IGNORECASE)
        full_explanation_match = re.search(r'\*\*Full Explanation:\*\*(.*?)$', explanation, re.DOTALL | re.IGNORECASE)
        
        formatted_parts = []
        
        # Extract and format TLDR
        if tldr_match:
            tldr_text = tldr_match.group(1).strip()
            # Clean up markdown formatting
            tldr_text = re.sub(r'\*\*', '', tldr_text)
            formatted_parts.append("SUMMARY:\n" + tldr_text.strip() + "\n")

        # Extract full explanation
        if full_explanation_match:
            full_text = full_explanation_match.group(1).strip()
            # Clean up markdown formatting
            full_text = re.sub(r'\*\*([^*]+)\*\*', r'\1', full_text)  # Remove bold
            full_text = re.sub(r'^#+\s*', '', full_text, flags=re.MULTILINE)  # Remove heading markers
            formatted_parts.append("\nDETAILED EXPLANATION:\n" + full_text.strip())
        elif not tldr_match:
            # No TLDR found, use entire explanation
            formatted_parts.append(explanation.strip())
        
        return "\n".join(formatted_parts) if formatted_parts else explanation
    
    def apply_explanation_formatting(self):
        """Apply formatting tags to explanation text"""
        # Configure tags
        self.explanation_text.tag_config("tldr_header", font=self.fonts['bold'], foreground='#000000')
        self.explanation_text.tag_config("full_header", font=self.fonts['bold'], foreground='#000000')

        content = self.explanation_text.get(1.0, tk.END)
        lines = content.split('\n')

        for i, line in enumerate(lines, 1):
            if line.startswith("SUMMARY:"):
                self.explanation_text.tag_add("tldr_header", f"{i}.0", f"{i}.end")
            elif line.startswith("DETAILED EXPLANATION:"):
                self.explanation_text.tag_add("full_header", f"{i}.0", f"{i}.end")
    
    def hide_explanation(self):
        """Hide explanation panel (step information remains visible)"""
        if self.explanation_visible:
            self.explanation_frame.pack_forget()
            self.explanation_visible = False
    
    def clear_explanation(self):
        """Clear explanation text and hide explanation panel (step information remains visible)"""
        if self.explanation_visible:
            self.explanation_frame.pack_forget()
            self.explanation_visible = False
        
        # Step information is always visible (no frame to check)
        
        self.explanation_text.config(state=tk.NORMAL)
        self.explanation_text.delete(1.0, tk.END)
        self.explanation_text.config(state=tk.DISABLED)
    
    def _truncate_log_tail(self, content: str, max_chars: int = None) -> str:
        """Return content or its tail to keep UI responsive and limit memory."""
        if not content:
            return content
        limit = max_chars if max_chars is not None else self.MAX_DISPLAY_LOG_CHARS
        if len(content) <= limit:
            return content
        return "... [earlier output truncated] ...\n\n" + content[-limit:]

    def _cap_scraper_log(self, scraper_name: str) -> None:
        """Keep in-memory log for scraper under MAX_LOG_CHARS to prevent bloat."""
        if scraper_name not in self.scraper_logs:
            return
        s = self.scraper_logs[scraper_name]
        if len(s) <= self.MAX_LOG_CHARS:
            return
        self.scraper_logs[scraper_name] = "... [earlier output truncated] ...\n\n" + s[-self.MAX_LOG_CHARS:]

    def update_log_display(self, scraper_name: str):
        """Update log display with the selected scraper's log"""
        if self._is_scraper_active(scraper_name):
            self._sync_external_log_if_running(scraper_name)
        log_content = self.scraper_logs.get(scraper_name, "")
        if not log_content and not self._is_scraper_active(scraper_name):
            log_content = self._load_latest_log_for_scraper(scraper_name)
        display_content = self._truncate_log_tail(log_content)
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.insert(1.0, display_content)
        self.log_text.see(tk.END)
        self.log_text.config(state=tk.DISABLED)
        self._sync_db_activity(log_content)
        
        # Schedule periodic refresh if this scraper is running
        # This ensures we see updates even if they come in while viewing another scraper
        if self._is_scraper_active(scraper_name):
            self.schedule_log_refresh(scraper_name)
    
    def schedule_log_refresh(self, scraper_name: str):
        """Schedule periodic refresh of log display for running scraper"""
        # Always update progress state for running scrapers (even if not selected)
        if not self._is_scraper_active(scraper_name):
            return
        self._sync_external_log_if_running(scraper_name)
        log_content = self.scraper_logs.get(scraper_name, "")
        # Always update progress state (stored state)
        self.update_progress_from_log(log_content, scraper_name, update_display=False)
        
        # Only refresh display if this scraper is still selected
        if scraper_name == self.scraper_var.get():
            display_content = self._truncate_log_tail(log_content)
            current_content = self.log_text.get(1.0, tk.END)
            if display_content.rstrip() != current_content.rstrip('\n'):
                # Log has been updated, refresh display (tail only to avoid GUI freeze)
                self.log_text.config(state=tk.NORMAL)
                self.log_text.delete(1.0, tk.END)
                self.log_text.insert(1.0, display_content)
                self.log_text.see(tk.END)
                self.log_text.config(state=tk.DISABLED)
            
            # Update display with stored progress state
            progress_state = self.scraper_progress.get(scraper_name, {"percent": 0, "description": f"Running {scraper_name}..."})
            self.progress_label.config(text=progress_state["description"])
            self.progress_bar['value'] = progress_state["percent"]
            self.progress_percent.config(text=f"{progress_state['percent']:.1f}%")

        # Sync DB activity from log content only for selected scraper
        if scraper_name == self.scraper_var.get():
            self._sync_db_activity(log_content)

        # Schedule next refresh in 500ms
        self.root.after(500, lambda sn=scraper_name: self.schedule_log_refresh(sn))
    
    def update_progress_for_scraper(self, scraper_name: str):
        """Update progress bar for a specific scraper based on its current log content"""
        # Always update stored progress state (even if not selected)
        if scraper_name not in self.scraper_logs:
            # No logs yet - initialize stored progress if not exists
            if scraper_name not in self.scraper_progress:
                self.scraper_progress[scraper_name] = {"percent": 0, "description": f"Ready: {scraper_name}"}
        else:
            # Update progress from scraper's log content (updates stored state)
            log_content = self.scraper_logs[scraper_name]
            self.update_progress_from_log(log_content, scraper_name, update_display=False)
        
        # Always update display when this function is called (it's called when scraper is selected)
        progress_state = self.scraper_progress.get(scraper_name, {"percent": 0, "description": f"Ready: {scraper_name}"})
        self.progress_label.config(text=progress_state["description"])
        self.progress_bar['value'] = progress_state["percent"]
        self.progress_percent.config(text=f"{progress_state['percent']:.1f}%")
    
    def update_progress_from_log(self, log_content: str, scraper_name: str, update_display: bool = True):
        """Parse log content for progress indicators and update progress state (and optionally display)"""
        import re
        
        # Determine if we should update the display
        is_selected = scraper_name == self.scraper_var.get()
        should_update_display = update_display and is_selected
        
        # Check if scraper is currently running - if not, don't parse old progress
        is_running = self._is_scraper_active(scraper_name)
        
        # Reset progress if pipeline stopped - check this FIRST before parsing progress
        if "[STOPPED]" in log_content or "Pipeline stopped" in log_content:
            # Find the position of the stop message
            stop_idx = max(
                log_content.rfind("[STOPPED]"),
                log_content.rfind("Pipeline stopped")
            )
            # Only parse progress messages that come AFTER the stop message
            if stop_idx >= 0:
                # If there are progress messages after stop, use them (scraper restarted)
                # Otherwise, reset to 0
                content_after_stop = log_content[stop_idx:]
                if "[PROGRESS]" not in content_after_stop:
                    # No progress after stop - reset to stopped state
                    progress_state = {"percent": 0, "description": "Pipeline stopped"}
                    self.scraper_progress[scraper_name] = progress_state
                    if should_update_display:
                        self.progress_label.config(text=progress_state["description"])
                        self.progress_bar['value'] = progress_state["percent"]
                        self.progress_percent.config(text="0%")
                    return
                # If progress exists after stop, continue parsing (scraper was restarted)
                log_content = content_after_stop
            else:
                # Stop message found but position unclear - reset to stopped state
                progress_state = {"percent": 0, "description": "Pipeline stopped"}
                self.scraper_progress[scraper_name] = progress_state
                if should_update_display:
                    self.progress_label.config(text=progress_state["description"])
                    self.progress_bar['value'] = progress_state["percent"]
                    self.progress_percent.config(text="0%")
                return
        
        # If scraper is not running and no stop message, check if it should show stale progress
        # Don't show old progress from previous runs - only show if scraper is actively running
        if not is_running:
            # Check if this is a fresh start (no progress state exists) - initialize to 0
            if scraper_name not in self.scraper_progress:
                progress_state = {"percent": 0, "description": f"Ready: {scraper_name}"}
                self.scraper_progress[scraper_name] = progress_state
                if should_update_display:
                    self.progress_label.config(text=progress_state["description"])
                    self.progress_bar['value'] = progress_state["percent"]
                    self.progress_percent.config(text="0%")
                return
            # If scraper has old progress state but is not running, don't re-parse old log
            # Just use the existing progress state (which should be 0 if it was stopped)
            existing_progress = self.scraper_progress.get(scraper_name)
            if existing_progress and existing_progress.get("percent", 0) == 0:
                # Already at 0 (stopped state) - don't re-parse old log
                if should_update_display:
                    self.progress_label.config(text=existing_progress.get("description", f"Ready: {scraper_name}"))
                    self.progress_bar['value'] = 0
                    self.progress_percent.config(text="0%")
                return
        
        # Check if pipeline completed (only if the scraper is no longer running)
        if (not is_running) and ("Pipeline completed" in log_content or "Execution completed" in log_content or "Finished" in log_content):
            progress_state = {"percent": 100, "description": "Pipeline completed"}
            self.scraper_progress[scraper_name] = progress_state
            if should_update_display:
                self.progress_label.config(text=progress_state["description"])
                self.progress_bar['value'] = progress_state["percent"]
                self.progress_percent.config(text="100%")
            return
        
        # Try to extract progress from various patterns
        # Search from the end (most recent) to get latest progress
        progress_percent = None
        progress_desc = None
        
        # Split log into lines and search from the end
        lines = log_content.split('\n')
        
        # If scraper is not running, only check for progress in lines after the last stop message
        # This prevents showing stale progress from previous runs when switching scrapers
        if not is_running:
            # Find the last stop message position
            last_stop_line_idx = -1
            for idx, line in enumerate(lines):
                if "[STOPPED]" in line or "Pipeline stopped" in line:
                    last_stop_line_idx = idx
            # If stop message found, only parse lines after it (ignore old progress before stop)
            if last_stop_line_idx >= 0:
                lines = lines[last_stop_line_idx + 1:]
                # If no lines after stop, no progress to show - already handled above, but check again
                if not lines or not any("[PROGRESS]" in line for line in lines):
                    # No progress messages after stop - keep stopped state
                    progress_state = self.scraper_progress.get(scraper_name, {"percent": 0, "description": "Pipeline stopped"})
                    if progress_state.get("percent", 0) > 0:
                        # Reset if somehow it has non-zero progress
                        progress_state = {"percent": 0, "description": "Pipeline stopped"}
                        self.scraper_progress[scraper_name] = progress_state
                    if should_update_display:
                        self.progress_label.config(text=progress_state.get("description", "Pipeline stopped"))
                        self.progress_bar['value'] = 0
                        self.progress_percent.config(text="0%")
                    return
        
        # Pattern 1: "[PROGRESS] {step}: X/Y (Z%)" (highest priority - most specific, includes step details)
        # Check the recent portion of the log for the most up-to-date progress message
        recent_limit = 200
        search_start_idx = max(0, len(lines) - recent_limit)

        pipeline_candidate = None
        pipeline_line_idx = -1
        # Look for the most recent Pipeline Step message
        for idx in range(len(lines) - 1, search_start_idx - 1, -1):
            line = lines[idx]
            pipeline_match = re.search(
                r'\[PROGRESS\]\s+Pipeline\s+Step\s*:\s*(\d+)\s*/\s*(\d+)\s*\(([\d.]+)%\)(?:\s*-\s*(.+))?',
                line,
                re.IGNORECASE
            )
            if pipeline_match:
                current = int(pipeline_match.group(1))
                total = int(pipeline_match.group(2))
                percent = float(pipeline_match.group(3))
                description = pipeline_match.group(4).strip() if pipeline_match.group(4) else None
                if total > 0:
                    desc_text = description if description else f"Pipeline Step {current}/{total}"
                    pipeline_candidate = {
                        "percent": percent,
                        "description": desc_text,
                        "current": current,
                        "total": total
                    }
                    pipeline_line_idx = idx
                    break

        general_candidate = None
        general_line_idx = -1

        # Priority 1: page/row progress lines (e.g., "Max Prices: page 1/14 row 10/200 (5.0%)")
        for idx in range(len(lines) - 1, search_start_idx - 1, -1):
            line = lines[idx]
            page_row_match = re.search(
                r'\[PROGRESS\]\s+(.+?)\s*:\s*page\s+(\d+)(?:/\d+)?\s+row\s+(\d+)\s*/\s*(\d+)\s*\(([\d.]+)%\)',
                line,
                re.IGNORECASE
            )
            if page_row_match:
                step_desc = page_row_match.group(1).strip()
                page_num = int(page_row_match.group(2))
                row_num = int(page_row_match.group(3))
                row_total = int(page_row_match.group(4))
                percent = float(page_row_match.group(5))
                if row_total > 0:
                    general_candidate = {
                        "percent": percent,
                        "description": f"{step_desc}: page {page_num} row {row_num}/{row_total}"
                    }
                    general_line_idx = idx
                    break

        # Priority 1.5: "left" progress lines (e.g., "Searching: X - 7546 left: 0/7546 (0.0%)")
        if general_candidate is None:
            for idx in range(len(lines) - 1, search_start_idx - 1, -1):
                line = lines[idx]
                left_match = re.search(
                    r'\[PROGRESS\]\s+(.+?)\s*-\s*(\d+)\s*left:\s*(\d+)\s*/\s*(\d+)\s*\(([\d.]+)%\)',
                    line,
                    re.IGNORECASE
                )
                if left_match:
                    step_desc = left_match.group(1).strip()
                    current = int(left_match.group(3))
                    total = int(left_match.group(4))
                    percent = float(left_match.group(5))
                    if total > 0:
                        general_candidate = {
                            "percent": percent,
                            "description": f"{step_desc} ({current}/{total})"
                        }
                        general_line_idx = idx
                        break

        # Priority 2: general "[PROGRESS] Step": percentage format
        for idx in range(len(lines) - 1, search_start_idx - 1, -1):
            line = lines[idx]
            progress_match = re.search(
                r'\[PROGRESS\]\s+(.+?)\s*:\s*(\d+)\s*/\s*(\d+)\s*\(([\d.]+)%\)\s*(?:-\s*(.+))?',
                line,
                re.IGNORECASE
            )
            if progress_match:
                step_desc = progress_match.group(1).strip()
                current = int(progress_match.group(2))
                total = int(progress_match.group(3))
                percent = float(progress_match.group(4))
                suffix = progress_match.group(5).strip() if progress_match.group(5) else None
                if is_running and step_desc.lower().startswith("pipeline step") and percent >= 100.0:
                    continue
                if total > 0:
                    desc_text = None
                    if ':' in step_desc:
                        parts = step_desc.split(':', 1)
                        step_name = parts[0].strip()
                        product_name = parts[1].strip()
                        if len(product_name) > 30:
                            product_name = product_name[:27] + "..."
                        desc_text = f"{step_name}: {product_name} ({current}/{total})"
                    else:
                        desc_text = f"{step_desc} ({current}/{total})"
                    if suffix:
                        desc_text = f"{desc_text} - {suffix}"
                    general_candidate = {
                        "percent": percent,
                        "description": desc_text
                    }
                    general_line_idx = idx
                    break
            else:
                fraction_match = re.search(
                    r'\[PROGRESS\]\s+(.+?)\s*:\s*(.+?)\s*\((\d+)\s*/\s*(\d+)\)\s*(?:-\s*(.+))?',
                    line,
                    re.IGNORECASE
                )
                if fraction_match:
                    step_desc = fraction_match.group(1).strip()
                    current = int(fraction_match.group(3))
                    total = int(fraction_match.group(4))
                    suffix = fraction_match.group(5).strip() if fraction_match.group(5) else None
                    if is_running and step_desc.lower().startswith("pipeline step") and total > 0 and current >= total:
                        continue
                    if total > 0:
                        percent = (current / total) * 100
                        desc_text = f"{step_desc} ({current}/{total})"
                        if suffix:
                            desc_text = f"{desc_text} - {suffix}"
                        general_candidate = {
                            "percent": percent,
                            "description": desc_text
                        }
                        general_line_idx = idx
                        break

        # Pattern 2: "[PROGRESS] X/Y (Z%) - Worker N" (numeric-only progress)
        if general_candidate is None:
            secondary_limit = 80
            search_start_idx_secondary = max(0, len(lines) - secondary_limit)
            for idx in range(len(lines) - 1, search_start_idx_secondary - 1, -1):
                line = lines[idx]
                numeric_match = re.search(
                    r'\[PROGRESS\]\s*(\d+)\s*/\s*(\d+)\s*\(([\d.]+)%\)\s*(?:-\s*(.+))?',
                    line,
                    re.IGNORECASE
                )
                if numeric_match:
                    current = int(numeric_match.group(1))
                    total = int(numeric_match.group(2))
                    percent = float(numeric_match.group(3))
                    suffix = numeric_match.group(4).strip() if numeric_match.group(4) else None
                    if total > 0:
                        desc_text = f"Processing {current}/{total}"
                        if suffix:
                            desc_text = f"{desc_text} - {suffix}"
                        general_candidate = {
                            "percent": percent,
                            "description": desc_text
                        }
                        general_line_idx = idx
                        break

        # Pattern 3: "[a][T#] Done: X | Skipped: Y | Failed: Z" (Netherlands format - high priority)
        if general_candidate is None:
            search_start_idx_thread = max(0, len(lines) - 200)  # Check more lines for thread progress
            total_done = 0
            total_skipped = 0
            total_failed = 0
            latest_thread_line_idx = -1
            
            # Find all thread progress entries and collect them
            thread_entries = []  # Store (done, skipped, failed, line_idx) for each thread
            for idx in range(len(lines) - 1, search_start_idx_thread - 1, -1):
                line = lines[idx]
                thread_match = re.search(
                    r'\[a\]\[T\d+\]\s+Done:\s*(\d+)\s*\|\s*Skipped:\s*(\d+)\s*\|\s*Failed:\s*(\d+)',
                    line,
                    re.IGNORECASE
                )
                if thread_match:
                    done = int(thread_match.group(1))
                    skipped = int(thread_match.group(2))
                    failed = int(thread_match.group(3))
                    thread_entries.append((done, skipped, failed, idx))
                    if latest_thread_line_idx == -1:
                        latest_thread_line_idx = idx
            
            # Calculate totals: use maximum values across threads (each thread reports its own progress)
            # This is more accurate than summing, as threads may process different items
            if thread_entries:
                total_done = max(entry[0] for entry in thread_entries)
                total_skipped = max(entry[1] for entry in thread_entries)
                total_failed = max(entry[2] for entry in thread_entries)
            
            # If we found thread progress entries, try to find total count
            if latest_thread_line_idx >= 0 and total_done > 0:
                # Look for total count in log entries (check both earlier and recent entries)
                total_count = None
                
                # First, look for explicit progress messages with total (e.g., "[PROGRESS] ... X/Y")
                for idx in range(len(lines) - 1, max(0, len(lines) - 500) - 1, -1):
                    line = lines[idx]
                    # Look for "[PROGRESS] ... X/Y" format
                    progress_total_match = re.search(
                        r'\[PROGRESS\].*?(\d+)\s*/\s*(\d+)\s*\([\d.]+%\)',
                        line,
                        re.IGNORECASE
                    )
                    if progress_total_match:
                        try:
                            current_val = int(progress_total_match.group(1))
                            total_val = int(progress_total_match.group(2))
                            if total_val > total_done:  # Only use if it's larger than done count
                                total_count = total_val
                                break
                        except ValueError:
                            pass
                
                # If not found, look for patterns like "[TOTAL]", "[PENDING]", "Total: X", etc.
                if total_count is None:
                    for idx in range(len(lines)):
                        line = lines[idx]
                        # Look for patterns like "[TOTAL] X", "[PENDING] X", "Total: X", "Total URLs: X", etc.
                        total_match = re.search(
                            r'(?:\[(?:TOTAL|PENDING|TOTAL\s+URLS?)\]\s*)?(?:Total|total|TOTAL|PENDING|pending)[:\s]+(\d+)|Processing\s+(\d+)\s+items?|(\d+)\s+items?\s+to\s+process|detail\s+URLs?\s*:\s*(\d+)',
                            line,
                            re.IGNORECASE
                        )
                        if total_match:
                            # Try to extract the number from any matching group
                            for group_idx in [1, 2, 3, 4]:
                                if total_match.group(group_idx):
                                    try:
                                        candidate_total = int(total_match.group(group_idx))
                                        if candidate_total > total_done:  # Only use if it's larger than done count
                                            total_count = candidate_total
                                            break
                                    except ValueError:
                                        pass
                            if total_count and total_count > total_done:
                                break
                
                # Calculate progress
                if total_count and total_count > 0:
                    # Use total_count as denominator
                    processed = total_done + total_skipped + total_failed
                    percent = min(100.0, round((processed / total_count) * 100, 1))
                    general_candidate = {
                        "percent": percent,
                        "description": f"Done: {total_done} | Skipped: {total_skipped} | Failed: {total_failed} ({processed}/{total_count})"
                    }
                    general_line_idx = latest_thread_line_idx
                else:
                    # No total found, but we have progress - use done count as indicator
                    # Calculate based on done + skipped + failed (assuming that's the total processed so far)
                    processed = total_done + total_skipped + total_failed
                    if processed > 0:
                        # Estimate progress: if we've processed items, show at least some progress
                        # Use a conservative estimate: assume we're at least 1% if we've done work
                        percent = max(0.1, min(99.0, round((total_done / max(processed, 1)) * 100, 1)))
                        general_candidate = {
                            "percent": percent,
                            "description": f"Done: {total_done} | Skipped: {total_skipped} | Failed: {total_failed}"
                        }
                        general_line_idx = latest_thread_line_idx

        # Pattern 4: "Scraping products: X/Y" (legacy format, second priority)
        if general_candidate is None:
            secondary_limit = 50
            search_start_idx_secondary = max(0, len(lines) - secondary_limit)
            for idx in range(len(lines) - 1, search_start_idx_secondary - 1, -1):
                line = lines[idx]
                scraping_match = re.search(
                    r'\[PROGRESS\]\s+Scraping\s+products?\s*:\s*(\d+)\s*/\s*(\d+)\s*\(([\d.]+)%\)',
                    line,
                    re.IGNORECASE
                )
                if scraping_match:
                    current = int(scraping_match.group(1))
                    total = int(scraping_match.group(2))
                    percent = float(scraping_match.group(3))
                    if total > 0:
                        general_candidate = {
                            "percent": percent,
                            "description": f"Scraping products: {current}/{total}"
                        }
                        general_line_idx = idx
                        break

        # Pattern 4: "Scraping products: X/Y" (without [PROGRESS] tag)
        if general_candidate is None:
            search_start_idx_secondary = max(0, len(lines) - 50)
            for idx in range(len(lines) - 1, search_start_idx_secondary - 1, -1):
                line = lines[idx]
                scraping_match = re.search(
                    r'Scraping\s+products?\s*:\s*(\d+)\s*/\s*(\d+)',
                    line,
                    re.IGNORECASE
                )
                if scraping_match:
                    current = int(scraping_match.group(1))
                    total = int(scraping_match.group(2))
                    if total > 0:
                        general_candidate = {
                            "percent": int((current / total) * 100),
                            "description": f"Scraping products: {current}/{total}"
                        }
                        general_line_idx = idx
                        break

        # Pattern 4: "Step X/Y" or "Step X of Y"
        if general_candidate is None:
            search_start_idx_secondary = max(0, len(lines) - 50)
            for idx in range(len(lines) - 1, search_start_idx_secondary - 1, -1):
                line = lines[idx]
                step_match = re.search(
                    r'Step\s+(\d+)\s*(?:of|/)\s*(\d+)',
                    line,
                    re.IGNORECASE
                )
                if step_match:
                    current = int(step_match.group(1))
                    total = int(step_match.group(2))
                    if total > 0:
                        general_candidate = {
                            "percent": int((current / total) * 100),
                            "description": f"Step {current}/{total}"
                        }
                        general_line_idx = idx
                        break

        # Pattern 5: "Processing X/Y" or "Processing X of Y"
        if general_candidate is None:
            search_start_idx_secondary = max(0, len(lines) - 50)
            for idx in range(len(lines) - 1, search_start_idx_secondary - 1, -1):
                line = lines[idx]
                process_match = re.search(
                    r'Processing\s+(\d+)\s*(?:of|/)\s*(\d+)',
                    line,
                    re.IGNORECASE
                )
                if process_match:
                    current = int(process_match.group(1))
                    total = int(process_match.group(2))
                    if total > 0:
                        general_candidate = {
                            "percent": int((current / total) * 100),
                            "description": f"Processing {current}/{total}"
                        }
                        general_line_idx = idx
                        break

        # Pattern 6: "X/Y products" or "X of Y products"
        if general_candidate is None:
            search_start_idx_secondary = max(0, len(lines) - 50)
            for idx in range(len(lines) - 1, search_start_idx_secondary - 1, -1):
                line = lines[idx]
                product_match = re.search(
                    r'(\d+)\s*(?:of|/)\s*(\d+)\s+product',
                    line,
                    re.IGNORECASE
                )
                if product_match:
                    current = int(product_match.group(1))
                    total = int(product_match.group(2))
                    if total > 0:
                        general_candidate = {
                            "percent": int((current / total) * 100),
                            "description": f"Products: {current}/{total}"
                        }
                        general_line_idx = idx
                        break

        # Pattern 7: "Progress: X%" or "X% complete"
        if general_candidate is None:
            search_start_idx_secondary = max(0, len(lines) - 50)
            for idx in range(len(lines) - 1, search_start_idx_secondary - 1, -1):
                line = lines[idx]
                percent_match = re.search(
                    r'(?:Progress|Complete)[:\s]+(\d+)%',
                    line,
                    re.IGNORECASE
                )
                if percent_match:
                    general_candidate = {
                        "percent": int(percent_match.group(1)),
                        "description": f"{percent_match.group(1)}% complete"
                    }
                    general_line_idx = idx
                    break

        if is_running and pipeline_candidate and pipeline_candidate.get("percent", 0) >= 100 and general_candidate is None:
            pipeline_candidate = None

        chosen_candidate = None
        if general_candidate and pipeline_candidate:
            # Always prefer the most recent progress line
            if general_line_idx >= pipeline_line_idx:
                chosen_candidate = general_candidate
            else:
                chosen_candidate = pipeline_candidate
        elif general_candidate and pipeline_candidate is None:
            chosen_candidate = general_candidate
        elif pipeline_candidate:
            chosen_candidate = pipeline_candidate

        if chosen_candidate:
            progress_percent = chosen_candidate["percent"]
            progress_desc = chosen_candidate["description"]

        stats_summary = None
        stats_match = re.search(
            r'\[STATS\]\s*Success:\s*(\d+)\s*\|\s*Zero-records:\s*(\d+)\s*\|\s*Detail rows:\s*(\d+)',
            log_content
        )
        if stats_match:
            success = stats_match.group(1)
            zero = stats_match.group(2)
            rows = stats_match.group(3)
            stats_summary = f"Success {success} | Zero {zero} | Rows {rows}"

        # Pattern 7: Look for current step name in log (e.g., "Running step: X")
        if not progress_desc:
            step_name_match = re.search(r'(?:Running|Executing|Step)\s*:?\s*([^\n]+)', log_content, re.IGNORECASE)
            if step_name_match:
                step_name = step_name_match.group(1).strip()[:50]  # Limit length
                progress_desc = f"Running: {step_name}"
        
        # Pattern 7: Look for last meaningful line (not empty, not just separators)
        if not progress_desc:
            lines = log_content.split('\n')
            for line in reversed(lines[-10:]):  # Check last 10 lines
                line = line.strip()
                if line and not line.startswith('=') and len(line) > 5:
                    # Skip common log prefixes
                    if not re.match(r'^\[?\d{4}-\d{2}-\d{2}', line):  # Skip timestamps
                        progress_desc = line[:60]  # Limit length
                        break
        
        # Calculate final progress percent if we have description but no percent
        if progress_desc and progress_percent is None:
            # If we have description but no percent, try to extract from description
            # Look for pattern like "Step: Product (X/Y)" in description
            desc_match = re.search(r'\((\d+)/(\d+)\)', progress_desc)
            if desc_match:
                current = int(desc_match.group(1))
                total = int(desc_match.group(2))
                if total > 0:
                    progress_percent = round((current / total) * 100, 1)
                    if current > 0 and progress_percent < 0.1:
                        progress_percent = 0.1
        
        # Determine final description
        if not progress_desc:
            if scraper_name in self.running_scrapers or scraper_name in self.running_processes:
                progress_desc = f"Running {scraper_name}..."
            else:
                progress_desc = f"Ready: {scraper_name}"

        if stats_summary:
            progress_desc = f"{progress_desc} | {stats_summary}"
        
        # Store progress state for this scraper
        if progress_percent is None and is_running:
            existing_progress = self.scraper_progress.get(scraper_name, {})
            progress_percent = existing_progress.get("percent", 0) or 0
        final_percent = progress_percent if progress_percent is not None else 0
        progress_state = {"percent": final_percent, "description": progress_desc}
        self.scraper_progress[scraper_name] = progress_state
        
        # Update display only if this scraper is selected
        if should_update_display:
            self.progress_bar['value'] = final_percent
            self.progress_percent.config(text=f"{final_percent:.1f}%")
            self.progress_label.config(text=progress_desc)
    
    def append_to_log_display(self, line: str):
        """Append a line to the log display (if scraper is selected). Cap widget size to avoid hang."""
        self.log_text.config(state=tk.NORMAL)
        self.log_text.insert(tk.END, line)
        # Keep widget responsive: drop oldest lines if over ~8k lines
        try:
            line_count = int(self.log_text.index("end-1c").split(".")[0])
            if line_count > 8000:
                self.log_text.delete("1.0", f"{line_count - 6000}.0")
        except Exception:
            pass
        self.log_text.see(tk.END)
        self.log_text.config(state=tk.DISABLED)
    
    def append_to_log_if_selected(self, line: str, scraper_name: str):
        """Append a line to the log display only if this scraper is currently selected"""
        if scraper_name == self.scraper_var.get():
            self.append_to_log_display(line)
        # Route [DB] lines to activity panel only for selected scraper
        if "[DB]" in line and scraper_name == self.scraper_var.get():
            self._append_db_activity(line)

    def _append_db_activity(self, line: str):
        """Append a [DB] tagged line to the DB activity panel with color coding."""
        if not hasattr(self, 'db_activity_text'):
            return
        # Skip noisy zero-record lines
        upper = line.upper()
        if "| ZERO_RECORDS |" in upper:
            return
        # Determine tag from content
        tag = None
        if "| CLAIM |" in upper:
            tag = "claim"
        elif "| UPSERT |" in upper:
            tag = "upsert"
        elif "| OK |" in upper or "| COMPLETED |" in upper:
            tag = "ok"
        elif "| FAIL" in upper:
            tag = "fail"
        elif "| SEED |" in upper:
            tag = "seed"
        elif "| FINISH |" in upper:
            tag = "finish"

        # Strip the [DB] prefix for cleaner display
        display = line.strip()
        if display.startswith("[DB] "):
            display = display[5:]

        self.db_activity_text.config(state=tk.NORMAL)
        if tag:
            self.db_activity_text.insert(tk.END, display + "\n", tag)
        else:
            self.db_activity_text.insert(tk.END, display + "\n")
        # Keep only last 100 lines
        line_count = int(self.db_activity_text.index('end-1c').split('.')[0])
        if line_count > 100:
            self.db_activity_text.delete('1.0', f'{line_count - 100}.0')
        self.db_activity_text.see(tk.END)
        self.db_activity_text.config(state=tk.DISABLED)

    def _sync_db_activity(self, log_content: str):
        """Extract [DB] lines from full log and refresh the DB activity panel."""
        if not hasattr(self, 'db_activity_text'):
            return
        db_lines = [l for l in log_content.split('\n') if '[DB]' in l]
        # Only show last 50 lines
        db_lines = db_lines[-50:]
        if not db_lines:
            return
        # Build the content with tags
        self.db_activity_text.config(state=tk.NORMAL)
        self.db_activity_text.delete('1.0', tk.END)
        for line in db_lines:
            upper = line.upper()
            if "| ZERO_RECORDS |" in upper:
                continue  # Skip noisy zero-record lines
            display = line.strip()
            if "[DB] " in display:
                display = display[display.index("[DB] ") + 5:]
            tag = None
            if "| CLAIM |" in upper:
                tag = "claim"
            elif "| UPSERT |" in upper:
                tag = "upsert"
            elif "| OK |" in upper or "| COMPLETED |" in upper:
                tag = "ok"
            elif "| FAIL" in upper:
                tag = "fail"
            elif "| SEED |" in upper:
                tag = "seed"
            elif "| FINISH |" in upper:
                tag = "finish"
            if tag:
                self.db_activity_text.insert(tk.END, display + "\n", tag)
            else:
                self.db_activity_text.insert(tk.END, display + "\n")
        self.db_activity_text.see(tk.END)
        self.db_activity_text.config(state=tk.DISABLED)

    def _get_lock_paths(self, scraper_name: str):
        try:
            from core.config_manager import ConfigManager
            # Migrated: pm = get_path_manager()
            new_lock = pm.get_lock_file(scraper_name)
        except Exception:
            new_lock = self.repo_root / ".locks" / f"{scraper_name}.lock"
        old_lock = self.repo_root / f".{scraper_name}_run.lock"
        return new_lock, old_lock

    def _get_lock_status(self, scraper_name: str):
        """Return (is_active, pid, log_path, lock_file) for lock-based runs."""
        new_lock, old_lock = self._get_lock_paths(scraper_name)
        lock_file = new_lock if new_lock.exists() else old_lock if old_lock.exists() else None
        if not lock_file or not lock_file.exists():
            return False, None, None, None

        pid = None
        log_path = None
        try:
            with open(lock_file, "r", encoding="utf-8", errors="replace") as f:
                lock_content = f.read().strip().split("\n")
            if lock_content and lock_content[0].isdigit():
                pid = int(lock_content[0])
            if len(lock_content) > 2 and lock_content[2].strip():
                log_path = lock_content[2].strip()
        except Exception:
            return False, None, None, None

        if pid:
            try:
                if sys.platform == "win32":
                    result = subprocess.run(
                        ["tasklist", "/FI", f"PID eq {pid}"],
                        capture_output=True,
                        text=True,
                        timeout=2
                    )
                    if str(pid) not in result.stdout:
                        try:
                            lock_file.unlink()
                        except Exception:
                            pass
                        return False, None, None, None
                else:
                    os.kill(pid, 0)
            except Exception:
                try:
                    lock_file.unlink()
                except Exception:
                    pass
                return False, None, None, None

            # Extra safety: ensure PID command line matches scraper name; otherwise treat as stale lock.
            if not self._pid_matches_scraper(pid, scraper_name):
                try:
                    lock_file.unlink()
                except Exception:
                    pass
                return False, None, None, None

        return True, pid, log_path, lock_file

    def _pid_matches_scraper(self, pid: int, scraper_name: str) -> bool:
        """Check if a PID command line appears to belong to the given scraper."""
        try:
            if sys.platform == "win32":
                result = subprocess.run(
                    ["wmic", "process", "where", f"processid={pid}", "get", "CommandLine"],
                    capture_output=True,
                    text=True,
                    timeout=2
                )
                cmdline = (result.stdout or "").lower()
            else:
                # Best-effort on non-Windows
                result = subprocess.run(
                    ["ps", "-p", str(pid), "-o", "command="],
                    capture_output=True,
                    text=True,
                    timeout=2
                )
                cmdline = (result.stdout or "").lower()
        except Exception:
            return True  # If unsure, keep lock active

        if not cmdline:
            return True

        name = scraper_name.lower()
        # Match by scraper name or scripts/<scraper> path segment
        if name in cmdline:
            return True
        if f"scripts\\{name}\\" in cmdline or f"scripts/{name}/" in cmdline:
            return True
        return False

    def _read_log_tail(self, scraper_name: str, log_path: Path) -> str:
        """Read new log data since the last offset for this scraper."""
        state = self._log_stream_state.get(scraper_name)
        offset = 0
        if state and state.get("path") == log_path:
            offset = state.get("offset", 0)
        try:
            with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                try:
                    size = log_path.stat().st_size
                    if offset > size:
                        offset = 0
                except Exception:
                    pass
                f.seek(offset)
                data = f.read()
                new_offset = f.tell()
        except Exception:
            return ""
        self._log_stream_state[scraper_name] = {"path": log_path, "offset": new_offset}
        return data

    def _read_log_tail_bytes(self, log_path: Path, max_bytes: int = 200000) -> str:
        """Read the tail of a log file (by size) for fast initial display."""
        try:
            with open(log_path, "rb") as f:
                f.seek(0, os.SEEK_END)
                size = f.tell()
                start = max(0, size - max_bytes)
                f.seek(start)
                data = f.read()
            text = data.decode("utf-8", errors="replace")
            if start > 0:
                nl = text.find("\n")
                if nl != -1:
                    text = text[nl + 1:]
            return text
        except Exception:
            return ""

    def _find_latest_external_log(self, scraper_name: str, include_archive: bool = False) -> Optional[Path]:
        candidates = []
        # Telegram bot logs
        try:
            from core.config_manager import ConfigManager
            # Migrated: pm = get_path_manager()
            logs_dir = pm.get_logs_dir()
        except Exception:
            logs_dir = self.repo_root / "logs"
        telegram_dir = logs_dir / "telegram"
        if telegram_dir.exists():
            candidates.extend(list(telegram_dir.glob(f"{scraper_name}_pipeline_*.log")))

        # Output logs
        try:
            from core.config_manager import ConfigManager
            # Migrated: pm = get_path_manager()
            output_dir = ConfigManager.get_output_dir(scraper_name)
        except Exception:
            output_dir = self.repo_root / "output" / scraper_name
        if output_dir.exists():
            candidates.extend(list(output_dir.glob("*.log")))

        # Scraper-specific logs (live or automatically saved)
        scraper_logs_dir = self._get_scraper_logs_dir(scraper_name)
        archive_dir = self._get_scraper_archive_dir(scraper_name)
        if scraper_logs_dir.exists():
            for log_path in scraper_logs_dir.rglob("*.log"):
                if archive_dir in log_path.parents and not include_archive:
                    continue
                candidates.append(log_path)

        # Scraper-local logs
        scraper_path = self.scrapers.get(scraper_name, {}).get("path")
        if scraper_path:
            local_logs = scraper_path / "logs"
            if local_logs.exists():
                candidates.extend(list(local_logs.glob("*.log")))

        candidates = [p for p in candidates if p.exists()]
        if not candidates:
            return None
        return max(candidates, key=lambda p: p.stat().st_mtime)

    def _load_latest_log_for_scraper(self, scraper_name: str) -> str:
        """Load the most recent log for a scraper (including archives) into memory."""
        log_path = self._find_latest_external_log(scraper_name, include_archive=True)
        if not log_path:
            return ""
        content = self._read_log_tail_bytes(log_path)
        if content:
            self.scraper_logs[scraper_name] = content
        return content

    def _sync_external_log_if_running(self, scraper_name: str):
        if scraper_name in self.running_scrapers:
            return
        lock_active, _pid, lock_log_path, _lock_file = self._get_lock_status(scraper_name)
        prev_active = self._scraper_active_state.get(scraper_name, False)
        if not lock_active:
            if prev_active:
                self._scraper_active_state[scraper_name] = False
            return
        if not prev_active:
            self.scraper_logs[scraper_name] = ""
            self._log_stream_state.pop(scraper_name, None)
        self._scraper_active_state[scraper_name] = True
        log_path = None
        if lock_log_path:
            log_path = Path(lock_log_path)
            if not log_path.exists():
                log_path = None
        if log_path is None:
            log_path = self._find_latest_external_log(scraper_name)
        if not log_path:
            return

        prev_path = self._external_log_files.get(scraper_name)
        if prev_path and prev_path != log_path:
            self.scraper_logs[scraper_name] = ""
            self._log_stream_state.pop(scraper_name, None)
        self._external_log_files[scraper_name] = log_path
        content = self._read_log_tail(scraper_name, log_path)
        if content:
            self.scraper_logs[scraper_name] = self.scraper_logs.get(scraper_name, "") + content
            self._cap_scraper_log(scraper_name)

    def _is_scraper_active(self, scraper_name: str) -> bool:
        if scraper_name in self.running_scrapers or scraper_name in self.running_processes:
            return True
        lock_active, _pid, _log_path, _lock_file = self._get_lock_status(scraper_name)
        return lock_active
    
    def refresh_run_button_state(self):
        """Refresh run button and stop button state based on current scraper selection and lock status"""
        scraper_name = self.scraper_var.get()
        if not scraper_name or not hasattr(self, 'run_button'):
            return

        try:
            lock_active, _lock_pid, lock_log_path, _lock_file = self._get_lock_status(scraper_name)
            if lock_active:
                if lock_log_path:
                    self._external_log_files[scraper_name] = Path(lock_log_path)
                self._sync_external_log_if_running(scraper_name)
                self.update_status(f"Selected scraper: {scraper_name} (RUNNING - lock file exists)")
                # Disable run button, enable stop button if lock exists
                self.run_button.config(state=tk.DISABLED, text="Running...")
                self.stop_button.config(state=tk.NORMAL)
            elif scraper_name in self.running_scrapers:
                # This scraper is running from GUI
                self.update_status(f"Selected scraper: {scraper_name} (RUNNING)")
                self.run_button.config(state=tk.DISABLED, text="Running...")
                self.stop_button.config(state=tk.NORMAL)
            else:
                # No lock and not running - enable run button, disable stop button
                self.update_status(f"Selected scraper: {scraper_name}")
                self.run_button.config(state=tk.NORMAL, text="Resume Pipeline")
                if hasattr(self, 'run_fresh_button'):
                    self.run_fresh_button.config(state=tk.NORMAL)
                self.stop_button.config(state=tk.DISABLED)
        except Exception:
            # On error, enable run button and disable stop button if not running from GUI
            if scraper_name not in self.running_scrapers:
                self.run_button.config(state=tk.NORMAL, text="Resume Pipeline")
                if hasattr(self, 'run_fresh_button'):
                    self.run_fresh_button.config(state=tk.NORMAL)
                self.stop_button.config(state=tk.DISABLED)
            self.update_status(f"Selected scraper: {scraper_name}")

        # Update checkpoint status
        self.update_checkpoint_status()

        # Update kill all Chrome button state
        self.update_kill_all_chrome_button_state()
    def update_kill_all_chrome_button_state(self):
        """Update the state of the 'Kill All Chrome Instances' button based on running scrapers"""
        if not hasattr(self, 'kill_all_chrome_button'):
            return
        
        # Check if any scraper is running
        any_running = False
        
        # Check if any scraper is in running_scrapers set
        if self.running_scrapers:
            any_running = True
        else:
            # Check if any scraper has a lock file (might be running from outside GUI)
            try:
                from core.config_manager import ConfigManager
                # Migrated: pm = get_path_manager()
                for scraper_name in self.scrapers.keys():
                    lock_file = pm.get_lock_file(scraper_name)
                    if lock_file.exists():
                        # Check if lock is stale
                        try:
                            with open(lock_file, 'r', encoding='utf-8') as f:
                                lock_content = f.read().strip().split('\n')
                                if lock_content and lock_content[0].isdigit():
                                    lock_pid = int(lock_content[0])
                                    # Check if process is still running
                                    import subprocess
                                    if sys.platform == "win32":
                                        result = subprocess.run(
                                            ['tasklist', '/FI', f'PID eq {lock_pid}'],
                                            capture_output=True,
                                            text=True,
                                            timeout=2
                                        )
                                        if str(lock_pid) in result.stdout:
                                            any_running = True
                                            break
                        except:
                            # If we can't read the lock, assume it might be active
                            any_running = True
                            break
            except Exception:
                pass
        
        # Enable button only if no scrapers are running
        if any_running:
            self.kill_all_chrome_button.config(state=tk.DISABLED)
        else:
            self.kill_all_chrome_button.config(state=tk.NORMAL)
    
    def kill_all_chrome_instances(self):
        """Kill all Chrome instances for all scrapers"""
        # Confirm action
        if not messagebox.askyesno("Confirm", 
            "Kill all Chrome instances for all scrapers?\n\n"
            "This will terminate all Chrome processes tracked by the scrapers.\n"
            "Make sure no scrapers are currently running."):
            return
        
        try:
            total_terminated = 0
            
            # Kill Chrome instances for each scraper
            for scraper_name in self.scrapers.keys():
                try:
                    from core.chrome_pid_tracker import terminate_scraper_pids
                    count = terminate_scraper_pids(scraper_name, self.repo_root, silent=True)
                    total_terminated += count
                except Exception:
                    pass
            
            # Also use fallback method to catch any remaining Chrome instances with automation flags
            try:
                from core.chrome_pid_tracker import terminate_chrome_by_flags
                fallback_count = terminate_chrome_by_flags(silent=True)
                total_terminated += fallback_count
            except Exception:
                pass
            
            # Also try to kill ChromeDriver processes
            try:
                import psutil
                chromedriver_count = 0
                for proc in psutil.process_iter(['pid', 'name']):
                    try:
                        proc_name = (proc.info.get('name') or '').lower()
                        if 'chromedriver' in proc_name:
                            proc.kill()
                            chromedriver_count += 1
                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                        pass
                total_terminated += chromedriver_count
            except Exception:
                pass
            
            # Update Chrome count display
            self.update_chrome_count()
            
            # Show result
            if total_terminated > 0:
                messagebox.showinfo("Success", 
                    f"Terminated {total_terminated} Chrome process(es) across all scrapers.")
                self.append_to_log_display(f"[KILL ALL] Terminated {total_terminated} Chrome process(es)\n")
            else:
                messagebox.showinfo("Information", 
                    "No Chrome instances found to terminate.")
                
        except Exception as e:
            messagebox.showerror("Error", f"Failed to kill Chrome instances:\n{e}")

    def open_tor_browser(self):
        """Launch Tor Browser from common install paths or TOR_BROWSER_PATH."""
        tor_path = self._resolve_tor_browser_path()
        if not tor_path:
            messagebox.showwarning(
                "Warning",
                "Tor Browser not found. Install it or set TOR_BROWSER_PATH."
            )
            return

        try:
            if sys.platform == "win32":
                os.startfile(str(tor_path))
            else:
                subprocess.Popen([str(tor_path)])
        except Exception as e:
            messagebox.showerror("Error", f"Failed to launch Tor Browser:\n{str(e)}")

    def _resolve_tor_browser_path(self):
        env_path = os.environ.get("TOR_BROWSER_PATH", "").strip()
        if env_path:
            env_candidate = Path(env_path)
            if env_candidate.is_dir():
                tor_exe = self._find_tor_exe_in_dir(env_candidate)
                if tor_exe:
                    return tor_exe
            elif env_candidate.exists():
                return env_candidate

        if sys.platform != "win32":
            for cmd in ("torbrowser-launcher", "tor-browser", "torbrowser"):
                found = shutil.which(cmd)
                if found:
                    return Path(found)
            return None

        user_profile = Path(os.environ.get("USERPROFILE", str(Path.home())))
        local_app_data = os.environ.get("LOCALAPPDATA")
        program_files = os.environ.get("ProgramFiles")
        program_files_x86 = os.environ.get("ProgramFiles(x86)")

        candidates = [
            user_profile / "Desktop" / "Tor Browser" / "Start Tor Browser.exe",
            user_profile / "Desktop" / "Tor Browser" / "Browser" / "firefox.exe",
            user_profile / "OneDrive" / "Desktop" / "Tor Browser" / "Start Tor Browser.exe",
            user_profile / "OneDrive" / "Desktop" / "Tor Browser" / "Browser" / "firefox.exe",
            user_profile / "Tor Browser" / "Start Tor Browser.exe",
            user_profile / "Tor Browser" / "Browser" / "firefox.exe",
        ]

        if local_app_data:
            local_app_data = Path(local_app_data)
            candidates.extend([
                local_app_data / "Tor Browser" / "Start Tor Browser.exe",
                local_app_data / "Tor Browser" / "Browser" / "firefox.exe",
            ])

        for pf in (program_files, program_files_x86):
            if pf:
                pf_path = Path(pf)
                candidates.extend([
                    pf_path / "Tor Browser" / "Start Tor Browser.exe",
                    pf_path / "Tor Browser" / "Browser" / "firefox.exe",
                ])

        for candidate in candidates:
            if candidate.exists():
                return candidate
        return None

    def _find_tor_exe_in_dir(self, base_dir):
        candidates = [
            base_dir / "Start Tor Browser.exe",
            base_dir / "Browser" / "firefox.exe",
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return None
    
    def start_periodic_lock_cleanup(self):
        """Start a periodic task to check for and clean up stale lock files"""
        def periodic_check():
            try:
                # Check all scrapers for stale locks and external starts
                for scraper_name in self.scrapers.keys():
                    try:
                        from core.config_manager import ConfigManager
                        # Migrated: pm = get_path_manager()
                        lock_file = pm.get_lock_file(scraper_name)
                        
                        # Check current lock state
                        current_lock_exists = lock_file.exists()
                        last_lock_exists = self._last_known_lock_states.get(scraper_name, False)
                        
                        # Update stored state
                        self._last_known_lock_states[scraper_name] = current_lock_exists
                        
                        # Detect external start (lock appeared, not started from GUI)
                        if current_lock_exists and not last_lock_exists and scraper_name not in self.running_scrapers:
                            # External process started - refresh UI if this is the selected scraper
                            if scraper_name == self.scraper_var.get():
                                self.root.after(0, self.refresh_run_button_state)
                                self.root.after(0, lambda sn=scraper_name: self.schedule_log_refresh(sn))
                            # Also update ticker to show running status
                            self.root.after(0, self.update_ticker_content)
                            continue
                        
                        # Detect external stop (lock disappeared, not stopped from GUI)
                        if not current_lock_exists and last_lock_exists and scraper_name not in self.running_scrapers:
                            # External process stopped - refresh UI if this is the selected scraper
                            if scraper_name == self.scraper_var.get():
                                self.root.after(0, self.refresh_run_button_state)
                            # Update ticker to reflect stopped status
                            self.root.after(0, self.update_ticker_content)
                            continue
                        
                        # Skip if scraper is actually running from GUI (stale lock check not needed)
                        if scraper_name in self.running_scrapers:
                            continue
                        
                        if lock_file.exists():
                            # Check if lock is stale
                            try:
                                with open(lock_file, 'r', encoding='utf-8') as f:
                                    lock_content = f.read().strip().split('\n')
                                    if lock_content and lock_content[0].isdigit():
                                        lock_pid = int(lock_content[0])
                                        # Check if process is still running
                                        if sys.platform == "win32":
                                            result = subprocess.run(
                                                ['tasklist', '/FI', f'PID eq {lock_pid}'],
                                                capture_output=True,
                                                text=True,
                                                timeout=2,
                                                creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, 'CREATE_NO_WINDOW') else 0
                                            )
                                            # If PID not found, process is dead - remove stale lock
                                            if str(lock_pid) not in result.stdout:
                                                try:
                                                    lock_file.unlink()
                                                    self._last_known_lock_states[scraper_name] = False
                                                    # Refresh button state if this is the selected scraper
                                                    if scraper_name == self.scraper_var.get():
                                                        self.root.after(0, self.refresh_run_button_state)
                                                except:
                                                    pass
                            except:
                                # If we can't read the lock file, it might be corrupted - try to remove it
                                try:
                                    lock_file.unlink()
                                    self._last_known_lock_states[scraper_name] = False
                                    if scraper_name == self.scraper_var.get():
                                        self.root.after(0, self.refresh_run_button_state)
                                except:
                                    pass
                    except:
                        pass
            except:
                pass
            
            # Update Chrome instance count for selected scraper
            try:
                self.root.after(0, self.update_chrome_count)
            except:
                pass
            
            # Update kill all Chrome button state
            try:
                self.root.after(0, self.update_kill_all_chrome_button_state)
            except:
                pass
            
            # Schedule next check in 5 seconds
            self.root.after(5000, periodic_check)
        
        # Start the periodic check after 5 seconds
        self.root.after(5000, periodic_check)

    def start_periodic_network_info_update(self):
        """Start a periodic task to refresh network info (every 30 seconds)"""
        def periodic_update():
            try:
                scraper_name = self.scraper_var.get()
                if scraper_name:
                    self.update_network_info(scraper_name)
            except:
                pass
            self.root.after(30000, periodic_update)  # 30 seconds
        self.root.after(30000, periodic_update)  # First update after 30 seconds

    def start_ticker_animation(self):
        """Start the ticker tape animation"""
        self.ticker_running = True
        self.update_ticker_content()
        self.animate_ticker()

    def update_ticker_content(self):
        """Update ticker tape content with running scraper details"""
        running_info = []

        # Collect information about running scrapers
        for scraper_name in self.scrapers.keys():
            # Check if scraper is running
            is_running = (scraper_name in self.running_scrapers or
                         scraper_name in self.running_processes)

            if is_running:
                # Get progress info if available
                progress_info = self.scraper_progress.get(scraper_name, {})
                percent = progress_info.get('percent', 0)
                description = progress_info.get('description', 'Running')

                # Format scraper status with emoji indicators
                status_text = f"▶ {scraper_name}: {description} ({percent:.1f}%)"
                running_info.append(status_text)

        # Create ticker text with better spacing
        if running_info:
            self.ticker_text = "     ●●●     ".join(running_info) + "     ●●●     "
        else:
            self.ticker_text = "⏸ No scrapers currently running     ●●●     Ready for execution     ●●●     "

        # Schedule next content update (every 2 seconds)
        if self.ticker_running:
            self.root.after(2000, self.update_ticker_content)

    def animate_ticker(self):
        """Animate the ticker tape scrolling effect (right to left)"""
        if not self.ticker_running or not self.ticker_label:
            return

        if not self.ticker_text:
            self.ticker_label.config(text="")
            self.root.after(150, self.animate_ticker)
            return

        # Get the width of the label to calculate character capacity
        # Approximate 8 pixels per character for monospace font
        try:
            label_width = self.ticker_label.winfo_width()
            if label_width <= 1:  # Not yet rendered
                label_width = 1000  # Default fallback
            display_length = max(int(label_width / 8), 80)  # Calculate based on actual width
        except:
            display_length = 120  # Fallback

        text_length = len(self.ticker_text)

        # Calculate display text with wrapping
        if text_length > 0:
            # Add padding spaces at the end for smooth looping
            padded_text = self.ticker_text + " " * 20
            padded_length = len(padded_text)

            # Duplicate text to create seamless loop
            extended_text = padded_text * 3

            # Right to left scrolling: increment offset moves text left
            start_pos = self.ticker_offset % padded_length
            display_text = extended_text[start_pos:start_pos + display_length]

            self.ticker_label.config(text=display_text)

            # Move offset for next frame (right to left)
            self.ticker_offset += 1
            if self.ticker_offset >= padded_length:
                self.ticker_offset = 0

        # Schedule next animation frame (150ms = slower, smoother scrolling)
        if self.ticker_running:
            self.root.after(150, self.animate_ticker)

    def stop_ticker_animation(self):
        """Stop the ticker tape animation"""
        self.ticker_running = False

    def _get_env_value_from_dotenv(self, key: str):
        env_path = self.repo_root / ".env"
        if not env_path.exists():
            return None
        try:
            for line in env_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                k, v = line.split("=", 1)
                if k.strip() != key:
                    continue
                v = v.strip().strip("'").strip('"')
                return v
        except Exception:
            return None
        return None

    def finish_scraper_run(self, scraper_name: str, return_code: int, stopped: bool = False):
        """Finish scraper run and update display if selected"""
        # Ensure scraper is removed from running sets
        self.running_scrapers.discard(scraper_name)
        # Unlock input controls if no scrapers running
        self._refresh_io_lock_states()
        self.refresh_run_button_state()
        # Update kill all Chrome button state
        self.update_kill_all_chrome_button_state()

        # Final cleanup of any remaining lock files (safety net with retries)
        import time
        max_retries = 5
        for attempt in range(max_retries):
            try:
                from core.config_manager import ConfigManager
                # Migrated: pm = get_path_manager()
                lock_file = pm.get_lock_file(scraper_name)
                if lock_file.exists():
                    lock_file.unlink()
                    # Verify deletion
                    if lock_file.exists() and attempt < max_retries - 1:
                        time.sleep(0.2 * (attempt + 1))
                        continue
                
                # Also check old lock location
                old_lock = self.repo_root / f".{scraper_name}_run.lock"
                if old_lock.exists():
                    old_lock.unlink()
                    # Verify deletion
                    if old_lock.exists() and attempt < max_retries - 1:
                        time.sleep(0.2 * (attempt + 1))
                        continue
                
                # If we get here, cleanup was successful
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(0.2 * (attempt + 1))
                else:
                    print(f"Warning: Could not remove lock files in finish_scraper_run: {e}")
        
        log_content = self.scraper_logs.get(scraper_name, "")
        if scraper_name == self.scraper_var.get():
            self.update_log_display(scraper_name)
            if stopped:
                self.update_status(f"{scraper_name} execution stopped by user")
            elif return_code == 0:
                self.update_status(f"{scraper_name} execution completed")
                # Save log automatically after successful completion
                self.save_log_automatically(scraper_name, log_content=log_content)
            else:
                self.update_status(f"{scraper_name} execution failed")
            self.refresh_output_files()

        if log_content.strip():
            self._last_completed_logs[scraper_name] = log_content
        
        # Refresh button state - use a longer delay to ensure lock files are gone
        def refresh_with_delay():
            """Refresh button state after ensuring cleanup is complete"""
            # Double-check lock files are gone before refreshing
            try:
                from core.config_manager import ConfigManager
                # Migrated: pm = get_path_manager()
                lock_file = pm.get_lock_file(scraper_name)
                if lock_file.exists():
                    # Lock still exists, try to remove it one more time
                    try:
                        lock_file.unlink()
                    except:
                        pass
            except:
                pass
            self.refresh_run_button_state()
        
        self.root.after(500, refresh_with_delay)  # 500ms delay to ensure file system updates

        # Auto-restart: if pipeline was stopped by the 20-min timer, schedule resume after 30s pause
        if scraper_name in self._auto_restart_pausing:
            self._auto_restart_pausing.discard(scraper_name)
            if return_code == 0:
                # Pipeline completed successfully - no need to restart
                self._cancel_auto_restart(scraper_name)
                self.append_to_log_display(f"[AUTO-RESTART] Pipeline completed successfully, auto-restart cycle ended.\n")
            else:
                # Schedule resume after 30 seconds
                cycle = self._auto_restart_cycle_count.get(scraper_name, 0)
                self.append_to_log_display(
                    f"\n[AUTO-RESTART] Cycle #{cycle}: Pipeline stopped. Resuming in 30 seconds...\n")
                resume_id = self.root.after(30000, lambda sn=scraper_name: self._auto_restart_resume_pipeline(sn))
                self._auto_restart_resume_ids[scraper_name] = resume_id
        elif return_code == 0:
            # Natural successful completion - cancel any pending auto-restart
            self._cancel_auto_restart(scraper_name)

    def handle_scraper_error(self, scraper_name: str, error: str):
        """Handle scraper execution error and update display if selected"""
        if scraper_name == self.scraper_var.get():
            self.update_log_display(scraper_name)
            self.update_status(f"Error: {error}")
            messagebox.showerror("Error", f"Failed to run script:\n{error}")
    
    def format_documentation(self, content: str, file_ext: str) -> str:
        """Format documentation content - keep original markdown for proper parsing"""
        # Return original content - we'll parse and format it in apply_markdown_formatting
        return content
    
    def apply_markdown_formatting(self):
        """Apply GitHub-like markdown formatting to the text widget"""
        import re
        
        self.doc_text.config(state=tk.NORMAL)
        content = self.doc_text.get(1.0, tk.END)
        self.doc_text.delete(1.0, tk.END)
        
        lines = content.split('\n')
        in_code_block = False
        code_block_start = None
        
        i = 1  # Line counter (1-indexed for tkinter)
        
        for line in lines:
            # Handle code blocks
            if line.strip().startswith('```'):
                if in_code_block:
                    # End code block
                    if code_block_start:
                        end_pos = f"{i}.0"
                        self.doc_text.tag_add("code", code_block_start, end_pos)
                    in_code_block = False
                    code_block_start = None
                else:
                    # Start code block
                    in_code_block = True
                    code_block_start = f"{i}.0"
                i += 1
                continue
            
            if in_code_block:
                # Inside code block - insert as-is with code formatting
                self.doc_text.insert(tk.END, line + "\n")
                i += 1
                continue
            
            # Process markdown syntax
            # Headings
            if line.startswith('# '):
                text = line[2:].strip()
                self.doc_text.insert(tk.END, text + "\n")
                self.doc_text.tag_add("heading1", f"{i}.0", f"{i}.end")
                i += 1
            elif line.startswith('## '):
                text = line[3:].strip()
                self.doc_text.insert(tk.END, text + "\n")
                self.doc_text.tag_add("heading2", f"{i}.0", f"{i}.end")
                i += 1
            elif line.startswith('### '):
                text = line[4:].strip()
                self.doc_text.insert(tk.END, text + "\n")
                self.doc_text.tag_add("heading3", f"{i}.0", f"{i}.end")
                i += 1
            elif line.startswith('#### '):
                text = line[5:].strip()
                self.doc_text.insert(tk.END, text + "\n")
                self.doc_text.tag_add("heading4", f"{i}.0", f"{i}.end")
                i += 1
            elif line.startswith('##### '):
                text = line[6:].strip()
                self.doc_text.insert(tk.END, text + "\n")
                self.doc_text.tag_add("heading5", f"{i}.0", f"{i}.end")
                i += 1
            elif line.startswith('###### '):
                text = line[7:].strip()
                self.doc_text.insert(tk.END, text + "\n")
                self.doc_text.tag_add("heading6", f"{i}.0", f"{i}.end")
                i += 1
            # Horizontal rule
            elif re.match(r'^[-*_]{3,}$', line.strip()):
                self.doc_text.insert(tk.END, "─" * 60 + "\n")
                self.doc_text.tag_add("hr", f"{i}.0", f"{i}.end")
                i += 1
            # Blockquote
            elif line.startswith('> '):
                text = line[2:].strip()
                self.doc_text.insert(tk.END, text + "\n")
                self.doc_text.tag_add("blockquote", f"{i}.0", f"{i}.end")
                i += 1
            # Lists
            elif re.match(r'^[\s]*[-*+]\s+', line):
                text = re.sub(r'^[\s]*[-*+]\s+', '', line)
                self.doc_text.insert(tk.END, "  • " + text + "\n")
                self.doc_text.tag_add("list_item", f"{i}.0", f"{i}.end")
                i += 1
            elif re.match(r'^[\s]*\d+\.\s+', line):
                match = re.match(r'^[\s]*(\d+)\.\s+(.*)', line)
                if match:
                    num, text = match.groups()
                    self.doc_text.insert(tk.END, f"  {num}. {text}\n")
                    self.doc_text.tag_add("list_item", f"{i}.0", f"{i}.end")
                    i += 1
            else:
                # Regular text - insert as-is and apply inline formatting
                self._insert_formatted_text_simple(line, i)
                i += 1
        
        # Close any open code block
        if in_code_block and code_block_start:
            end_pos = f"{i}.0"
            self.doc_text.tag_add("code", code_block_start, end_pos)
        
        self.doc_text.config(state=tk.DISABLED)
    
    def _insert_formatted_text_simple(self, line: str, line_num: int):
        """Insert text with simple markdown formatting - just insert text, formatting applied via tags"""
        import re
        
        if not line.strip():
            self.doc_text.insert(tk.END, "\n")
            return
        
        # Simple approach: insert text and apply tags without modifying content
        start_pos = self.doc_text.index(tk.END)
        self.doc_text.insert(tk.END, line + "\n")
        
        # Apply inline code tags (replace backticks)
        for match in list(re.finditer(r'`([^`]+)`', line))[::-1]:  # Process reverse
            start_idx = match.start()
            end_idx = match.end()
            tag_start = f"{start_pos}+{start_idx}c"
            tag_end = f"{start_pos}+{end_idx}c"
            self.doc_text.delete(tag_start, tag_end)
            self.doc_text.insert(tag_start, match.group(1))
            new_end = self.doc_text.index(f"{tag_start}+{len(match.group(1))}c")
            self.doc_text.tag_add("code_inline", tag_start, new_end)
            # Update line for next matches
            line = line[:start_idx] + match.group(1) + line[end_idx:]
        
        # Apply link tags
        for match in list(re.finditer(r'\[([^\]]+)\]\(([^\)]+)\)', line))[::-1]:
            start_idx = match.start()
            end_idx = match.end()
            tag_start = f"{start_pos}+{start_idx}c"
            tag_end = f"{start_pos}+{end_idx}c"
            link_text = match.group(1)
            url = match.group(2)
            self.doc_text.delete(tag_start, tag_end)
            self.doc_text.insert(tag_start, link_text)
            new_end = self.doc_text.index(f"{tag_start}+{len(link_text)}c")
            self.doc_text.tag_add("link", tag_start, new_end)
            self.doc_text.tag_bind("link", "<Button-1>", lambda e, u=url: webbrowser.open(u))
            line = line[:start_idx] + link_text + line[end_idx:]
        
        # Apply bold tags
        for match in list(re.finditer(r'\*\*([^*]+)\*\*|__([^_]+)__', line))[::-1]:
            start_idx = match.start()
            end_idx = match.end()
            tag_start = f"{start_pos}+{start_idx}c"
            tag_end = f"{start_pos}+{end_idx}c"
            text = match.group(1) or match.group(2)
            self.doc_text.delete(tag_start, tag_end)
            self.doc_text.insert(tag_start, text)
            new_end = self.doc_text.index(f"{tag_start}+{len(text)}c")
            self.doc_text.tag_add("bold", tag_start, new_end)
            line = line[:start_idx] + text + line[end_idx:]
        
        # Apply italic tags (avoid bold conflicts)
        for match in list(re.finditer(r'(?<!\*)\*([^*]+)\*(?!\*)|(?<!_)_([^_]+)_(?!_)', line))[::-1]:
            start_idx = match.start()
            end_idx = match.end()
            # Check if already formatted as bold
            tag_start = f"{start_pos}+{start_idx}c"
            tag_end = f"{start_pos}+{end_idx}c"
            existing_tags = self.doc_text.tag_names(tag_start)
            if 'bold' not in existing_tags:
                text = match.group(1) or match.group(2)
                self.doc_text.delete(tag_start, tag_end)
                self.doc_text.insert(tag_start, text)
                new_end = self.doc_text.index(f"{tag_start}+{len(text)}c")
                self.doc_text.tag_add("italic", tag_start, new_end)
    
    def run_full_pipeline(self, resume=True, _skip_confirm=False):
        """Run the full pipeline for selected scraper with resume/checkpoint support.

        Args:
            resume: True to resume from checkpoint, False for fresh run.
            _skip_confirm: If True, skip all confirmation dialogs (used by auto-restart).
        """
        # Check if THIS scraper is already running (not other scrapers)
        scraper_name = self.scraper_var.get()
        if not scraper_name:
            if not _skip_confirm:
                messagebox.showwarning("Warning", "Select a scraper first")
            return

        # Check if this specific scraper is running from GUI
        if scraper_name in self.running_scrapers:
            if not _skip_confirm:
                messagebox.showwarning("Warning", f"{scraper_name} is already running. Wait for completion.")
            return

        # Check external running status through lock+PID validation (auto-cleans stale locks).
        lock_active, _pid, _log_path, _lock_file = self._get_lock_status(scraper_name)
        if lock_active:
            if not _skip_confirm:
                messagebox.showwarning("Warning", f"{scraper_name} is already running.")
            return

        scraper_info = self.scrapers.get(scraper_name)
        if not scraper_info:
            messagebox.showerror("Error", f"Unknown scraper: {scraper_name}")
            return

        # Try resume script first (check both old and new names)
        resume_script = scraper_info["path"] / scraper_info.get("pipeline_script", "run_pipeline_resume.py")
        if not resume_script.exists():
            resume_script = scraper_info["path"] / "run_pipeline_resume.py"

        if resume_script.exists():
            # Use resume script with resume/fresh flag
            mode = "resume" if resume else "fresh"
            if not _skip_confirm:
                if resume:
                    # Get checkpoint info for confirmation
                    try:
                        from core.pipeline_checkpoint import get_checkpoint_manager
                        cp = get_checkpoint_manager(scraper_name)
                        info = cp.get_checkpoint_info()
                        if info["total_completed"] > 0:
                            # Get total steps for this scraper
                            scraper_info = self.scrapers.get(scraper_name)
                            total_steps = len(scraper_info.get("steps", [])) if scraper_info else None

                            msg = f"Resume pipeline for {scraper_name}?\n\n"
                            msg += f"Last completed step: {info['last_completed_step']}"
                            if total_steps is not None:
                                msg += f" (out of {total_steps - 1} total steps)"
                            msg += f"\nWill start from step: {info['next_step']}"
                            all_complete = total_steps is not None and info['next_step'] >= total_steps
                            if all_complete:
                                msg += "\n\nAll steps already completed. Use 'Fresh Run' to start a new pipeline, or clear the checkpoint first."
                                messagebox.showinfo("Pipeline Already Complete", msg)
                                return
                            msg += f"\nCompleted steps: {info['total_completed']}"
                            if total_steps is not None:
                                msg += f" / {total_steps}"
                            if not messagebox.askyesno("Confirm Resume", msg):
                                return
                        else:
                            if not messagebox.askyesno("Confirm", f"Run pipeline for {scraper_name}?\n\nNo checkpoint found - will start from step 0."):
                                return
                    except Exception as e:
                        if not messagebox.askyesno("Confirm", f"Run pipeline for {scraper_name}?"):
                            return
                else:
                    if not messagebox.askyesno("Confirm Fresh Run", f"Run fresh pipeline for {scraper_name}?\n\nThis will:\n- Clear checkpoint\n- Start from step 0\n- Create backup and clean output"):
                        return

            extra_args = [] if resume else ["--fresh"]
            if scraper_name == "India":
                workers = os.getenv("INDIA_WORKERS", "5").strip()
                if workers and "--workers" not in extra_args:
                    extra_args += ["--workers", workers]
            self.run_script_in_thread(
                resume_script,
                scraper_info["path"],
                is_pipeline=True,
                extra_args=extra_args,
                preserve_existing_log=resume,
            )
        else:
            # Fallback to workflow script or batch file
            workflow_script = scraper_info["path"] / "run_workflow.py"

            if workflow_script.exists():
                # Use new unified workflow
                if not _skip_confirm:
                    if not messagebox.askyesno("Confirm", f"Run full pipeline for {scraper_name}?\n\nThis will:\n- Create a backup first\n- Run all steps\n- Organize outputs in run folder"):
                        return

                self.run_script_in_thread(
                    workflow_script,
                    scraper_info["path"],
                    is_pipeline=True,
                    extra_args=[],
                    preserve_existing_log=False,
                )
            else:
                # Fallback to old batch file
                pipeline_bat = scraper_info["path"] / scraper_info["pipeline_bat"]

                if not pipeline_bat.exists():
                    if not _skip_confirm:
                        messagebox.showerror("Error", f"Pipeline script not found:\n{pipeline_bat}")
                    return

                # Confirm
                if not _skip_confirm:
                    if not messagebox.askyesno("Confirm", f"Run full pipeline for {scraper_name}?"):
                        return

                self.run_script_in_thread(
                    pipeline_bat,
                    scraper_info["path"],
                    is_pipeline=True,
                    extra_args=[],
                    preserve_existing_log=False,
                )

    def clear_step_data_action(self):
        """Clear step data for the selected scraper (current run_id) if supported."""
        scraper_name = self._get_selected_scraper_for_data_reset()
        if not scraper_name:
            messagebox.showwarning("Warning", "Select a scraper before clearing step data.")
            return

        # Validate step selection
        step_val = self.clear_step_var.get() if hasattr(self, "clear_step_var") else None
        try:
            step_int = int(step_val)
            if step_int not in (1, 2, 3, 4, 5):
                raise ValueError()
        except Exception:
            messagebox.showerror("Invalid Step", "Select a step number (1-5) to clear.")
            return

        downstream = bool(self.clear_downstream_var.get()) if hasattr(self, "clear_downstream_var") else False

        confirm_msg = (
            f"Clear data for Step {step_int}"
            f"{' and downstream steps' if downstream else ''} for the current run_id?\n\n"
            "This deletes rows in the relevant table(s) for this run only."
        )
        if not messagebox.askyesno("Confirm Clear", confirm_msg):
            return

        scraper_info = self.scrapers.get(scraper_name, {})
        script_path = scraper_info.get("path", self.repo_root / "scripts" / scraper_name) / "clear_step_data.py"
        if not script_path.exists():
            messagebox.showerror("Script Missing", f"clear_step_data.py not found for {scraper_name}:\n{script_path}")
            return

        extra_args = ["--step", str(step_int)]
        if downstream:
            extra_args.append("--downstream")

        # Run as a non-pipeline task
        self.run_script_in_thread(script_path, script_path.parent, is_pipeline=False, extra_args=extra_args)
    
    
    def run_script_in_thread(self, script_path, working_dir, is_pipeline=False, extra_args=None, preserve_existing_log=False):
        """Run script in a separate thread"""
        if extra_args is None:
            extra_args = []

        startup_lock_file = None
        startup_lock_reason = ""

        # Set running state and disable run button for this scraper only
        scraper_name = self.scraper_var.get()

        # Atomic single-instance guard across GUI/API/Telegram
        if is_pipeline:
            try:
                from core.pipeline_start_lock import claim_pipeline_start_lock
                acquired, startup_lock_file, startup_lock_reason = claim_pipeline_start_lock(
                    scraper_name,
                    owner="gui",
                    repo_root=self.repo_root,
                )
            except Exception as exc:
                messagebox.showerror("Start Failed", f"Could not acquire pipeline lock for {scraper_name}:\n{exc}")
                return

            if not acquired:
                messagebox.showwarning(
                    "Already Running",
                    f"{scraper_name} is already running.\n\nDetails: {startup_lock_reason}",
                )
                self.refresh_run_button_state()
                return

            self._pipeline_lock_files[scraper_name] = startup_lock_file

        self.running_scrapers.add(scraper_name)
        self._last_completed_logs.pop(scraper_name, None)
        # Lock input/output controls for the running scraper
        self._refresh_io_lock_states()
        self.refresh_run_button_state()
        # Update kill all Chrome button state (disable it)
        self.update_kill_all_chrome_button_state()
        
        existing_log_text = self.scraper_logs.get(scraper_name, "")

        # Clear old logs when starting pipeline, unless we are resuming and need history preserved.
        if is_pipeline:
            if not preserve_existing_log:
                # Clear console display for this scraper when starting
                if scraper_name == self.scraper_var.get():
                    self.clear_logs(scraper_name, silent=True, clear_storage=False)  # Clear console but keep storage for now
                # Clear log storage for fresh pipeline run
                self.scraper_logs[scraper_name] = ""
            # Refresh network info after a delay so it updates when pipeline (or our auto-start) brings Tor up
            def _refresh_network_after_pipeline_start():
                self.root.after(10000, lambda: self.update_network_info(scraper_name, force_refresh=True))
            self.root.after(0, _refresh_network_after_pipeline_start)
        
        # Initialize log storage for this scraper if not exists
        if scraper_name not in self.scraper_logs:
            self.scraper_logs[scraper_name] = ""
        
        # Disable run button and enable stop button only for the currently selected scraper
        current_scraper = self.scraper_var.get()
        if current_scraper == scraper_name:
            self.run_button.config(state=tk.DISABLED, text="⏸ Running...")
            self.stop_button.config(state=tk.NORMAL)
        self.update_status(f"Running {scraper_name}...")
        
        # Generate run_id for metrics tracking (or use existing if pipeline is running)
        run_id = None
        if is_pipeline:
            # Check if pipeline is already running (started from elsewhere)
            try:
                from core.config_manager import ConfigManager
                # Migrated: pm = get_path_manager()
                output_dir = ConfigManager.get_output_dir(scraper_name)
                run_id_file = output_dir / ".current_run_id"
                if run_id_file.exists():
                    # Pipeline might be running - read the run_id
                    existing_run_id = run_id_file.read_text(encoding='utf-8').strip()
                    if existing_run_id:
                        # Check if lock file exists (confirming it's running)
                        lock_file = pm.get_lock_file(scraper_name)
                        if lock_file.exists():
                            run_id = existing_run_id
                            print(f"[SYNC] Using existing run_id from running pipeline: {run_id}")
            except Exception:
                pass  # Fall through to generate new run_id
            
            if not run_id:
                # Generate new run_id
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                run_id = f"{scraper_name}_{timestamp}"
        
        # Start metrics tracking for pipeline runs
        metrics_tracker = None
        if is_pipeline and run_id:
            try:
                from core.run_metrics_tracker import RunMetricsTracker
                metrics_tracker = RunMetricsTracker()
                metrics_tracker.start_run(run_id, scraper_name)
                print(f"[METRICS] Started tracking for run: {run_id}")
            except Exception as e:
                print(f"[METRICS] Warning: Could not start metrics tracking: {e}")
                metrics_tracker = None
        
        def run():
            try:
                # Initialize log for this scraper
                run_action = "Resuming execution" if preserve_existing_log else "Starting execution"
                log_header = f"{run_action} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                log_header += f"Scraper: {scraper_name}\n"
                log_header += f"Script: {script_path}\n"
                log_header += f"Working Directory: {working_dir}\n"
                if extra_args:
                    log_header += f"Extra Arguments: {' '.join(extra_args)}\n"
                log_header += "=" * 80 + "\n\n"

                if preserve_existing_log and existing_log_text.strip():
                    separator = "\n\n" + "=" * 80 + "\n"
                    separator += f"[GUI] Resume requested at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    separator += "=" * 80 + "\n\n"
                    self.scraper_logs[scraper_name] = existing_log_text + separator + log_header
                else:
                    self.scraper_logs[scraper_name] = log_header
                
                # Initialize progress state
                progress_start = "Resuming" if preserve_existing_log else "Starting"
                self.scraper_progress[scraper_name] = {"percent": 0, "description": f"{progress_start} {scraper_name}..."}
                
                # Update progress bar display only if this scraper is selected
                if scraper_name == self.scraper_var.get():
                    self.progress_label.config(text=f"Starting {scraper_name}...")
                    self.progress_bar['value'] = 0
                    self.progress_percent.config(text="0%")
                
                # Update log display if this scraper is selected
                # Store current scraper before thread starts (GUI thread is safe here)
                initial_selected_scraper = self.scraper_var.get()
                if scraper_name == initial_selected_scraper:
                    self.root.after(0, lambda sn=scraper_name: self.update_log_display(sn))
                
                # Run script with binary pipes so we can decode as UTF-8 in the reader thread.
                # This avoids Windows default (charmap) decoding which fails on Cyrillic etc.
                env = os.environ.copy()
                env["PYTHONUNBUFFERED"] = "1"
                
                # Pass run_id to pipeline so all components (GUI/API/Telegram) are in sync
                if is_pipeline and run_id:
                    env_var_name = f"{scraper_name.upper().replace(' ', '_').replace('-', '_')}_RUN_ID"
                    env[env_var_name] = run_id
                    print(f"[SYNC] Passing run_id to pipeline via {env_var_name}: {run_id}")
                
                subprocess_kw = dict(
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    cwd=str(working_dir),
                    env=env,
                )
                if script_path.suffix == ".bat":
                    # Run batch file
                    cmd = ["cmd", "/c", str(script_path)] + extra_args
                    process = subprocess.Popen(cmd, **subprocess_kw)
                else:
                    # Run Python script
                    # In packaged EXE mode, use sys.executable to prevent recursive relaunch
                    if getattr(sys, 'frozen', False):
                        # Packaged EXE mode - use sys.executable (the EXE itself)
                        # But we're launching a script, so we need Python interpreter
                        # Check if we can find python.exe in the same directory
                        exe_dir = Path(sys.executable).parent
                        python_exe = exe_dir / "python.exe"
                        if not python_exe.exists():
                            # Fallback to sys.executable (might work if it's a Python launcher)
                            python_cmd = sys.executable
                        else:
                            python_cmd = str(python_exe)
                    else:
                        # Development mode - use "python" command
                        python_cmd = "python"
                    
                    cmd = [python_cmd, str(script_path)] + extra_args
                    process = subprocess.Popen(cmd, **subprocess_kw)

                # Finalize lock file with child PID (lock was atomically claimed before start)
                if is_pipeline:
                    try:
                        from core.pipeline_start_lock import update_pipeline_lock
                        lock_target = startup_lock_file
                        if lock_target is None:
                            from core.config_manager import ConfigManager
                            # Migrated: pm = get_path_manager()
                            lock_target = pm.get_lock_file(scraper_name)
                        log_path_value = self._external_log_files.get(scraper_name, "")
                        log_path_obj = Path(log_path_value) if log_path_value else None
                        update_pipeline_lock(lock_target, process.pid, log_path=log_path_obj)
                        self._pipeline_lock_files[scraper_name] = lock_target
                    except Exception as e:
                        # If lock update fails, log but continue (process is already running)
                        print(f"Warning: Could not update lock file with PID: {e}")
                
                self.running_processes[scraper_name] = process
                
                # Read output in real-time: binary pipe, decode as UTF-8 in this process
                # so Windows charmap is never used (avoids decode errors on Cyrillic etc.)
                output_queue = queue.Queue()
                
                def read_output():
                    try:
                        for raw in iter(process.stdout.readline, b''):
                            if raw:
                                try:
                                    line = raw.decode("utf-8", errors="replace")
                                except Exception:
                                    line = raw.decode("latin-1", errors="replace")
                                output_queue.put(('line', line))
                        output_queue.put(('done', None))
                    except Exception as e:
                        output_queue.put(('error', str(e)))
                
                # Start output reader thread
                output_thread = threading.Thread(target=read_output, daemon=True)
                output_thread.start()
                
                # Process output from queue
                while True:
                    try:
                        msg_type, data = output_queue.get(timeout=0.1)
                        if msg_type == 'line':
                            # Append to scraper's log (cap size to avoid memory bloat on long runs)
                            self.scraper_logs[scraper_name] += data
                            if len(self.scraper_logs[scraper_name]) > self.MAX_LOG_CHARS:
                                self._cap_scraper_log(scraper_name)
                            # Update display if this scraper is currently selected
                            # Check in GUI thread via root.after
                            line_data = data
                            self.root.after(0, lambda l=line_data, sn=scraper_name: self.append_to_log_if_selected(l, sn))
                        elif msg_type == 'done':
                            break
                        elif msg_type == 'error':
                            error_data = data
                            self.scraper_logs[scraper_name] += f"\nError reading output: {error_data}\n"
                            self.root.after(0, lambda e=error_data, sn=scraper_name: self.append_to_log_if_selected(f"\nError: {e}\n", sn))
                            break
                    except queue.Empty:
                        # Check if process is still running
                        if process.poll() is not None:
                            # Process finished, check for remaining output (binary → decode UTF-8)
                            try:
                                raw_remaining = process.stdout.read()
                                if raw_remaining:
                                    try:
                                        remaining = raw_remaining.decode("utf-8", errors="replace")
                                    except Exception:
                                        remaining = raw_remaining.decode("latin-1", errors="replace")
                                    self.scraper_logs[scraper_name] += remaining
                                    if len(self.scraper_logs[scraper_name]) > self.MAX_LOG_CHARS:
                                        self._cap_scraper_log(scraper_name)
                                    rem_data = remaining
                                    self.root.after(0, lambda r=rem_data, sn=scraper_name: self.append_to_log_if_selected(r, sn))
                            except Exception:
                                pass
                            break
                        continue
                
                process.wait()

                # Explicitly close stdout to ensure process can fully terminate
                try:
                    if process.stdout:
                        process.stdout.close()
                except:
                    pass
                
                # Update progress state to completion
                if process.returncode == 0:
                    self.scraper_progress[scraper_name] = {"percent": 100, "description": "Pipeline completed successfully"}
                else:
                    # Keep current progress but update description
                    current_progress = self.scraper_progress.get(scraper_name, {"percent": 0, "description": ""})
                    self.scraper_progress[scraper_name] = {"percent": current_progress["percent"], "description": "Pipeline completed with errors"}
                
                # Update progress bar display only if this scraper is selected
                if scraper_name == self.scraper_var.get():
                    progress_state = self.scraper_progress[scraper_name]
                    self.progress_label.config(text=progress_state["description"])
                    self.progress_bar['value'] = progress_state["percent"]
                    self.progress_percent.config(text=f"{progress_state['percent']:.1f}%")

                # Wait for all child processes to complete (especially important for batch files)
                # On Windows, batch files spawn child Python processes that may still be running
                if script_path.suffix == ".bat" and sys.platform == "win32":
                    import time
                    # Wait longer for batch files to ensure all child Python processes complete
                    # Batch files spawn Python processes that need time to fully terminate
                    time.sleep(3.0)  # Wait 3 seconds for child processes to complete

                # Get return code BEFORE cleanup
                return_code = process.returncode
                was_stopped = scraper_name in self._stopped_by_user
                
                # Clean up lock files IMMEDIATELY after process completes (before status message)
                if is_pipeline:
                    # Remove from tracking sets immediately
                    self.running_scrapers.discard(scraper_name)
                    if scraper_name in self.running_processes:
                        del self.running_processes[scraper_name]
                    # Update kill all Chrome button state
                    self.update_kill_all_chrome_button_state()
                    
                    # Clean up ALL lock files with retry mechanism
                    def cleanup_locks():
                        """Clean up all lock files with retries"""
                        import time
                        max_retries = 10  # Increased retries
                        lock_cleared = False
                        
                        for attempt in range(max_retries):
                            try:
                                all_cleared = True
                                
                                # Clean up GUI-created lock file
                                if scraper_name in self._pipeline_lock_files:
                                    lock_file = self._pipeline_lock_files[scraper_name]
                                    if lock_file and lock_file.exists():
                                        try:
                                            lock_file.unlink()
                                            # Verify deletion
                                            if lock_file.exists():
                                                all_cleared = False
                                        except Exception as e:
                                            all_cleared = False
                                    del self._pipeline_lock_files[scraper_name]
                                
                                # Clean up WorkflowRunner lock files
                                from core.config_manager import ConfigManager
                                # Migrated: pm = get_path_manager()
                                lock_file = pm.get_lock_file(scraper_name)
                                if lock_file.exists():
                                    try:
                                        lock_file.unlink()
                                        # Verify deletion
                                        if lock_file.exists():
                                            all_cleared = False
                                    except Exception as e:
                                        all_cleared = False
                                
                                # Clean up old lock location
                                old_lock = self.repo_root / f".{scraper_name}_run.lock"
                                if old_lock.exists():
                                    try:
                                        old_lock.unlink()
                                        # Verify deletion
                                        if old_lock.exists():
                                            all_cleared = False
                                    except Exception as e:
                                        all_cleared = False
                                
                                # If all locks are cleared, we're done
                                if all_cleared:
                                    lock_cleared = True
                                    break
                                    
                            except Exception as e:
                                if attempt < max_retries - 1:
                                    time.sleep(0.3 * (attempt + 1))  # Exponential backoff
                                else:
                                    print(f"Warning: Could not remove lock files after {max_retries} attempts: {e}")
                            
                            # Wait before retry
                            if attempt < max_retries - 1:
                                time.sleep(0.2)
                        
                        if not lock_cleared:
                            print(f"Warning: Some lock files may still exist for {scraper_name}")
                    
                    cleanup_locks()
                    
                    # Longer delay to ensure file system updates on Windows
                    # Especially important after batch files where child processes need time to fully terminate
                    import time
                    if script_path.suffix == ".bat":
                        time.sleep(2.0)  # Longer delay for batch files
                    else:
                        time.sleep(0.5)  # Standard delay for Python scripts

                # Final status - check if stopped by user
                status_msg = "\n" + "=" * 80 + "\n"
                if was_stopped:
                    status_msg += f"Execution stopped by user at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    self._stopped_by_user.discard(scraper_name)  # Remove from set after using
                elif return_code == 0:
                    status_msg += f"Execution completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    # Simple lock file deletion after successful run
                    try:
                        from core.config_manager import ConfigManager
                        # Migrated: pm = get_path_manager()
                        lock_file = pm.get_lock_file(scraper_name)
                        if lock_file.exists():
                            lock_file.unlink()
                        # Also clean up old lock location
                        old_lock = self.repo_root / f".{scraper_name}_run.lock"
                        if old_lock.exists():
                            old_lock.unlink()
                        # Clean up GUI-created lock file
                        if scraper_name in self._pipeline_lock_files:
                            gui_lock = self._pipeline_lock_files[scraper_name]
                            if gui_lock and gui_lock.exists():
                                gui_lock.unlink()
                            del self._pipeline_lock_files[scraper_name]
                    except Exception as e:
                        pass  # Ignore errors, will be cleaned up by periodic task
                else:
                    status_msg += f"Execution failed with return code {return_code}\n"

                self.scraper_logs[scraper_name] += status_msg
                if len(self.scraper_logs[scraper_name]) > self.MAX_LOG_CHARS:
                    self._cap_scraper_log(scraper_name)

                # Update display if this scraper is selected
                # Schedule finish_scraper_run on GUI thread
                self.root.after(0, lambda sn=scraper_name, rc=return_code, stopped=was_stopped: self.finish_scraper_run(sn, rc, stopped))

            except Exception as e:
                error_msg = f"\nError: {str(e)}\n"
                self.scraper_logs[scraper_name] += error_msg
                if len(self.scraper_logs[scraper_name]) > self.MAX_LOG_CHARS:
                    self._cap_scraper_log(scraper_name)
                if is_pipeline:
                    try:
                        from core.pipeline_start_lock import release_pipeline_lock
                        release_pipeline_lock(startup_lock_file)
                    except Exception:
                        pass
                    self._pipeline_lock_files.pop(scraper_name, None)
                error_str = str(e)
                self.root.after(0, lambda sn=scraper_name, err=error_str: self.handle_scraper_error(sn, err))
                
                # Complete metrics tracking on exception
                if is_pipeline and metrics_tracker and run_id:
                    try:
                        metrics_tracker.complete_run(run_id, "failed")
                        print(f"[METRICS] Completed tracking for run: {run_id} (exception)")
                    except Exception as metrics_err:
                        print(f"[METRICS] Warning: Could not complete metrics tracking: {metrics_err}")
            finally:
                # Final cleanup - ensure everything is cleaned up even if there was an exception
                # (Most cleanup happens above after process.wait(), but this is a safety net)
                self.running_scrapers.discard(scraper_name)
                if scraper_name in self.running_processes:
                    del self.running_processes[scraper_name]
                
                # Final button state refresh (with delay to ensure lock files are gone)
                if is_pipeline:
                    def delayed_refresh():
                        """Refresh button state after a short delay to ensure lock files are deleted"""
                        import time
                        # Try multiple times to ensure lock is cleared
                        for attempt in range(5):
                            try:
                                from core.config_manager import ConfigManager
                                # Migrated: pm = get_path_manager()
                                lock_file = pm.get_lock_file(scraper_name)
                                if lock_file.exists():
                                    # Lock still exists, try to remove it
                                    try:
                                        lock_file.unlink()
                                        # Wait a bit and check again
                                        time.sleep(0.3)
                                        if not lock_file.exists():
                                            break
                                    except Exception as e:
                                        # If deletion fails, wait longer
                                        time.sleep(0.5)
                                else:
                                    # Lock is gone, we're good
                                    break
                            except:
                                pass
                            time.sleep(0.5)  # Give file system time to update
                        
                        # Final refresh
                        self.refresh_run_button_state()
                        
                        # Schedule another check after a longer delay as a safety net
                        def final_check():
                            try:
                                from core.config_manager import ConfigManager
                                # Migrated: pm = get_path_manager()
                                lock_file = pm.get_lock_file(scraper_name)
                                if lock_file.exists():
                                    try:
                                        lock_file.unlink()
                                    except:
                                        pass
                                self.refresh_run_button_state()
                            except:
                                pass
                        
                        self.root.after(2000, final_check)  # Check again after 2 seconds
                    
                    self.root.after(500, delayed_refresh)  # Schedule after 500ms
                
                # Clean up process resources
                try:
                    if 'process' in locals() and process:
                        # Close all pipes
                        if process.stdout:
                            process.stdout.close()
                        if process.stderr:
                            process.stderr.close()
                        if process.stdin:
                            process.stdin.close()
                        # Give process time to fully terminate
                        import time
                        time.sleep(0.5)
                        # Force terminate if still alive
                        if process.poll() is None:
                            process.terminate()
                            time.sleep(0.3)
                            if process.poll() is None:
                                process.kill()
                except:
                    pass

                # Clean up tracking
                if scraper_name in self.running_processes:
                    del self.running_processes[scraper_name]
                self.running_scrapers.discard(scraper_name)
                
                # Complete metrics tracking for pipeline runs
                if is_pipeline and metrics_tracker and run_id:
                    try:
                        if was_stopped:
                            # Pause metrics (run was stopped, may be resumed later)
                            metrics_tracker.pause_run(run_id)
                            print(f"[METRICS] Paused tracking for run: {run_id} (stopped by user)")
                        elif return_code == 0:
                            # Complete metrics successfully
                            final_metrics = metrics_tracker.complete_run(run_id, "completed")
                            if final_metrics:
                                print(f"[METRICS] Completed tracking for run: {run_id}")
                                print(f"[METRICS] Duration: {final_metrics.active_duration_seconds:.2f}s, "
                                      f"Network: {final_metrics.network_total_gb:.4f} GB")
                        else:
                            # Complete metrics with failure
                            metrics_tracker.complete_run(run_id, "failed")
                            print(f"[METRICS] Completed tracking for run: {run_id} (failed)")
                    except Exception as e:
                        print(f"[METRICS] Warning: Could not complete metrics tracking: {e}")
                
                # Refresh button state after cleanup (lock should be released by workflow runner)
                self.root.after(0, lambda: self.refresh_run_button_state())
        
        thread = threading.Thread(target=run, daemon=True)
        try:
            thread.start()
        except Exception:
            if is_pipeline:
                try:
                    from core.pipeline_start_lock import release_pipeline_lock
                    release_pipeline_lock(startup_lock_file)
                except Exception:
                    pass
                self._pipeline_lock_files.pop(scraper_name, None)
            raise

        # Start auto-restart timer if enabled and this is a pipeline run
        if is_pipeline and self._auto_restart_enabled.get():
            self._start_auto_restart_timer(scraper_name)

    # ------------------------------------------------------------------
    # Auto-restart: stop every 20 min → pause 30s → resume to clear cache/memory
    # ------------------------------------------------------------------

    def _start_auto_restart_timer(self, scraper_name: str):
        """Schedule an auto-restart stop after 20 minutes for the given scraper."""
        # Netherlands URLs are session-bound; stopping mid-run invalidates URL ids.
        if scraper_name == "Netherlands":
            self.append_to_log_display(
                "[AUTO-RESTART] Disabled for Netherlands (session-bound URL ids require uninterrupted scraping).\n"
            )
            return

        # Cancel any existing timer first
        if scraper_name in self._auto_restart_timers:
            try:
                self.root.after_cancel(self._auto_restart_timers[scraper_name])
            except Exception:
                pass

        timer_id = self.root.after(
            20 * 60 * 1000,  # 20 minutes in ms
            lambda sn=scraper_name: self._auto_restart_stop_pipeline(sn))
        self._auto_restart_timers[scraper_name] = timer_id
        cycle = self._auto_restart_cycle_count.get(scraper_name, 0)
        if cycle == 0:
            self.append_to_log_display(
                f"[AUTO-RESTART] Enabled: pipeline will auto-restart every 20 min to clear cache/memory.\n")
        # Update header icon to show active state
        self._update_auto_restart_header_icon()

    def _cancel_auto_restart(self, scraper_name: str):
        """Cancel all auto-restart timers and pending resumes for a scraper."""
        # Cancel the 20-min timer
        if scraper_name in self._auto_restart_timers:
            try:
                self.root.after_cancel(self._auto_restart_timers[scraper_name])
            except Exception:
                pass
            del self._auto_restart_timers[scraper_name]

        # Cancel any pending 30s resume
        if scraper_name in self._auto_restart_resume_ids:
            try:
                self.root.after_cancel(self._auto_restart_resume_ids[scraper_name])
            except Exception:
                pass
            del self._auto_restart_resume_ids[scraper_name]

        # Clear pausing state
        self._auto_restart_pausing.discard(scraper_name)
        self._auto_restart_cycle_count.pop(scraper_name, None)

    def _auto_restart_stop_pipeline(self, scraper_name: str):
        """Called after 20 minutes: silently stop the pipeline for auto-restart."""
        # Remove timer reference (it has already fired)
        self._auto_restart_timers.pop(scraper_name, None)

        # Check if auto-restart is still enabled
        if not self._auto_restart_enabled.get():
            return

        # Check if the scraper is actually running
        if scraper_name not in self.running_processes:
            return
        process = self.running_processes[scraper_name]
        if process is None or process.poll() is not None:
            return

        # Increment cycle counter
        cycle = self._auto_restart_cycle_count.get(scraper_name, 0) + 1
        self._auto_restart_cycle_count[scraper_name] = cycle

        # Mark as auto-restart pausing so finish_scraper_run schedules the resume
        self._auto_restart_pausing.add(scraper_name)
        self._stopped_by_user.add(scraper_name)

        self.append_to_log_display(
            f"\n{'='*80}\n"
            f"[AUTO-RESTART] Cycle #{cycle}: Stopping pipeline after 20 min to clear cache/memory...\n"
            f"{'='*80}\n")

        # Kill process in a background thread to avoid blocking the GUI
        def do_kill():
            try:
                # Kill Chrome instances first (scraper-specific)
                try:
                    from core.chrome_pid_tracker import terminate_scraper_pids
                    terminated = terminate_scraper_pids(scraper_name, self.repo_root, silent=True)
                    if terminated > 0:
                        self.root.after(0, lambda: self.append_to_log_display(
                            f"[AUTO-RESTART] Terminated {terminated} Chrome process(es)\n"))
                except Exception:
                    pass

                # Kill the main process tree
                import time
                if sys.platform == "win32":
                    try:
                        subprocess.run(
                            ["taskkill", "/F", "/T", "/PID", str(process.pid)],
                            capture_output=True, text=True, timeout=10)
                    except Exception:
                        process.terminate()
                else:
                    process.terminate()

                time.sleep(1)
                if process.poll() is None:
                    process.kill()
                    time.sleep(0.5)

                # Final Chrome cleanup
                try:
                    from core.chrome_pid_tracker import terminate_scraper_pids
                    terminate_scraper_pids(scraper_name, self.repo_root, silent=True)
                except Exception:
                    pass
            except Exception as e:
                self.root.after(0, lambda: self.append_to_log_display(
                    f"[AUTO-RESTART] Error during stop: {e}\n"))

        threading.Thread(target=do_kill, daemon=True).start()

    def _update_auto_restart_header_icon(self):
        """Update the auto-restart icon and status text in header based on current state."""
        if not hasattr(self, 'auto_restart_icon_label'):
            return

        enabled = self._auto_restart_enabled.get()
        if enabled:
            self.auto_restart_icon_label.config(
                text="🔄",
                fg=self.colors['console_yellow']
            )
            self.auto_restart_status_label.config(
                text="Auto-restart: ON (20 min)",
                fg=self.colors['white']
            )
        else:
            self.auto_restart_icon_label.config(
                text="⏸",
                fg=self.colors['light_gray']
            )
            self.auto_restart_status_label.config(
                text="Auto-restart: OFF",
                fg=self.colors['light_gray']
            )
    
    def _initialize_auto_restart_icon(self):
        """Initialize auto-restart icon state after UI is fully set up."""
        if hasattr(self, 'auto_restart_icon_label'):
            self._update_auto_restart_header_icon()

    def _open_telegram_bot(self):
        """Open the Telegram bot chat in the default browser."""
        import webbrowser
        webbrowser.open("https://t.me/esreportstatusbot")

    # ── API Server toggle (embedded FastAPI) ──────────────────────────────
    def _write_to_log(self, message):
        """Thread-safe helper to append a line to the GUI execution log."""
        def _insert():
            self.log_text.config(state=tk.NORMAL)
            self.log_text.insert(tk.END, message + "\n")
            self.log_text.see(tk.END)
            self.log_text.config(state=tk.DISABLED)
        self.root.after(0, _insert)

    def _toggle_api_server(self):
        """Start or stop the embedded FastAPI server."""
        if self._api_server_running:
            self._stop_api_server()
        else:
            self._start_api_server()

    def _start_api_server(self):
        """Launch the FastAPI API server in a background thread."""
        if self._api_server_running:
            return
        try:
            from scripts.common.api_server import start_embedded, stop_embedded  # noqa: F401
        except ImportError as exc:
            self._write_to_log(f"[API] Failed to import api_server: {exc}")
            return

        def _run():
            try:
                start_embedded(host="0.0.0.0", port=self._api_server_port)
            except Exception as exc:
                print(f"[API] Server error: {exc}")

        self._api_server_thread = threading.Thread(target=_run, daemon=True, name="api-server")
        self._api_server_thread.start()
        self._api_server_running = True
        self.api_toggle_label.config(text=f"API: ON (:{self._api_server_port})", fg='#10b981')
        self._write_to_log(f"[API] Server started on http://0.0.0.0:{self._api_server_port}  (Swagger: /docs)")

    def _stop_api_server(self):
        """Stop the embedded FastAPI server."""
        if not self._api_server_running:
            return
        try:
            from scripts.common.api_server import stop_embedded
            stop_embedded()
        except Exception as exc:
            self._write_to_log(f"[API] Error stopping server: {exc}")
        self._api_server_running = False
        self.api_toggle_label.config(text="API: OFF", fg=self.colors['light_gray'])
        self._write_to_log("[API] Server stopped")

    def _init_prometheus_server(self):
        """Initialize Prometheus metrics server on GUI startup."""
        try:
            from core.prometheus_exporter import init_prometheus_metrics
            success = init_prometheus_metrics(port=9090)
            if success:
                print("[GUI] Prometheus metrics server started on port 9090")
            else:
                print("[GUI] Prometheus metrics server not available (prometheus_client not installed)")
        except Exception as e:
            print(f"[GUI] Failed to start Prometheus metrics server: {e}")

    def _toggle_auto_restart_from_header(self):
        """Toggle auto-restart from header icon click."""
        current_state = self._auto_restart_enabled.get()
        new_state = not current_state
        self._auto_restart_enabled.set(new_state)
        
        # Checkbox removed - control is via header icon only
        
        # Update header icon
        self._update_auto_restart_header_icon()
        
        # Show feedback
        status = "ENABLED" if new_state else "DISABLED"
        self.append_to_log_display(f"[AUTO-RESTART] Auto-restart {status} via header icon.\n")
        
        # If disabling, cancel all active timers
        if not new_state:
            for scraper_name in list(self._auto_restart_timers.keys()):
                self._cancel_auto_restart(scraper_name)

    def _auto_restart_resume_pipeline(self, scraper_name: str):
        """Called 30 seconds after auto-restart stop: resume the pipeline."""
        # Clean up resume timer reference
        self._auto_restart_resume_ids.pop(scraper_name, None)

        # Safety checks
        if not self._auto_restart_enabled.get():
            self.append_to_log_display(f"[AUTO-RESTART] Auto-restart disabled, skipping resume.\n")
            return
        if scraper_name in self.running_scrapers:
            self.append_to_log_display(f"[AUTO-RESTART] Scraper already running, skipping resume.\n")
            return

        cycle = self._auto_restart_cycle_count.get(scraper_name, 0)
        self.append_to_log_display(
            f"\n{'='*80}\n"
            f"[AUTO-RESTART] Cycle #{cycle}: Resuming pipeline now...\n"
            f"{'='*80}\n")

        # Ensure the correct scraper is selected (run_full_pipeline reads from scraper_var)
        current = self.scraper_var.get()
        if current != scraper_name:
            self.scraper_var.set(scraper_name)

        # Resume pipeline without confirmation dialogs
        self.run_full_pipeline(resume=True, _skip_confirm=True)

    def stop_pipeline(self):
        """Stop the running pipeline for the currently selected scraper"""
        scraper_name = self.scraper_var.get()
        if not scraper_name:
            messagebox.showwarning("Warning", "Select a scraper first", parent=self.root)
            return

        # Prevent multiple simultaneous stop attempts — but with a 30s timeout
        # to avoid getting permanently stuck if a previous stop attempt hung
        if scraper_name in self._stopping_scrapers:
            stop_started = self._stopping_started_at.get(scraper_name)
            if stop_started and (time.time() - stop_started) < 30:
                return  # Still within timeout, ignore duplicate request
            else:
                # Stale stop state — force clear it and proceed
                self._stopping_scrapers.discard(scraper_name)
                self._stopping_started_at.pop(scraper_name, None)
                print(f"[STOP] Cleared stale _stopping_scrapers state for {scraper_name}")

        # Mark as stopping with timestamp
        self._stopping_scrapers.add(scraper_name)
        self._stopping_started_at[scraper_name] = time.time()

        # Cancel auto-restart cycle when user manually stops (don't restart after this)
        self._cancel_auto_restart(scraper_name)

        process = None
        stop_confirmed = False

        # Check if running
        if scraper_name in self.running_processes:
            process = self.running_processes[scraper_name]
            if process and process.poll() is None:  # Process is still running
                # Confirm stop (parent=self.root ensures dialog appears on top)
                if not messagebox.askyesno("Confirm Stop", f"Stop {scraper_name} pipeline?\n\nThis will terminate the running process.", parent=self.root):
                    self._cleanup_stopping_state(scraper_name)
                    return
                stop_confirmed = True

        if not stop_confirmed:
            # Process is dead but UI thinks it's running — clean up stale state
            if scraper_name in self.running_processes:
                del self.running_processes[scraper_name]
            self.finish_scraper_run(scraper_name, -1, stopped=True)
            self._cleanup_stopping_state(scraper_name)
            # Also clean up lock file
            try:
                from core.config_manager import ConfigManager
                # Migrated: pm = get_path_manager()
                lock_file = pm.get_lock_file(scraper_name)
                if lock_file.exists():
                    lock_file.unlink()
            except Exception:
                pass
            messagebox.showinfo("Cleaned Up", f"{scraper_name} was no longer running.\nUI state has been reset.", parent=self.root)
            return

        # Run termination in a background thread to prevent GUI freeze/crash
        def termination_worker():
            try:
                self.update_status(f"Stopping {scraper_name}...")
                
                # Step 1: Clean up Chrome instances (scraper-specific)
                try:
                    from core.chrome_pid_tracker import terminate_scraper_pids
                    terminated_count = terminate_scraper_pids(scraper_name, self.repo_root, silent=True)
                    if terminated_count > 0:
                        self.append_to_log_display(f"[STOP] Terminated {terminated_count} Chrome process(es) for {scraper_name}\n")
                except Exception as e:
                    print(f"Error terminating Chrome PIDs: {e}")

                # Step 2: Terminate main process
                if process and process.poll() is None:
                    import time
                    if sys.platform == "win32":
                        try:
                            # Try taskkill /T to kill tree
                            subprocess.run(
                                ["taskkill", "/F", "/T", "/PID", str(process.pid)],
                                capture_output=True, text=True, timeout=5
                            )
                        except Exception:
                            try:
                                process.terminate()
                            except:
                                pass
                    else:
                        try:
                            process.terminate()
                        except:
                            pass
                    
                    # Wait for shutdown
                    time.sleep(1)
                    if process.poll() is None:
                        try:
                            process.kill()
                        except:
                            pass
                
                # Step 3: Final cleanup
                try:
                    from core.chrome_pid_tracker import terminate_scraper_pids
                    terminate_scraper_pids(scraper_name, self.repo_root, silent=True)
                except:
                    pass
                    
                # Clean up lock file
                try:
                    from core.config_manager import ConfigManager
                    # Migrated: pm = get_path_manager()
                    lock_file = pm.get_lock_file(scraper_name)
                    if lock_file.exists():
                        lock_file.unlink()
                except:
                    pass

                # Mark DATABASE run_ledger status as RESUME (PostgreSQL table)
                try:
                    from core.db.postgres_connection import PostgresDB
                    # Use a new connection for specific operation
                    db = PostgresDB(scraper_name)
                    db.connect()
                    with db.cursor() as cursor:
                        cursor.execute(
                            "SELECT run_id FROM run_ledger WHERE status = 'running' AND scraper_name = %s ORDER BY started_at DESC LIMIT 1",
                            (scraper_name,)
                        )
                        row = cursor.fetchone()
                        if row:
                            cursor.execute("UPDATE run_ledger SET status = 'resume', ended_at = NOW() WHERE run_id = %s", (row[0],))
                            db.commit()
                            self.append_to_log_display(f"[DB] Marked run {row[0]} as 'resume'\n")
                    db.close()
                except Exception as e:
                    print(f"Failed to update DB run status: {e}")

                # Update UI via main thread
                self.root.after(0, lambda: self._finalize_stop(scraper_name))

            except Exception as e:
                print(f"Error during stop_pipeline: {e}")
                self.root.after(0, lambda: messagebox.showerror("Error", f"Error stopping pipeline: {e}", parent=self.root))
                self.root.after(0, lambda: self._cleanup_stopping_state(scraper_name))

        threading.Thread(target=termination_worker, daemon=True).start()

    def _finalize_stop(self, scraper_name):
        """Helper to update UI after stop completes"""
        self.finish_scraper_run(scraper_name, -1, stopped=True)
        self.update_status(f"Stopped {scraper_name}")
        self._cleanup_stopping_state(scraper_name)
        messagebox.showinfo("Stopped", f"Pipeline for {scraper_name} has been stopped.", parent=self.root)

    def _cleanup_stopping_state(self, scraper_name):
        self._stopping_scrapers.discard(scraper_name)
        self._stopping_started_at.pop(scraper_name, None)

    def update_checkpoint_status(self):
        """Update checkpoint status label"""
        scraper_name = self.scraper_var.get()
        if not scraper_name:
            if hasattr(self, 'checkpoint_status_label'):
                self.checkpoint_status_label.config(text="Checkpoint: No scraper selected")
            if hasattr(self, 'timeline_status_label'):
                self.timeline_status_label.config(text="Timeline: No scraper selected")
            return
        
        try:
            from core.pipeline_checkpoint import get_checkpoint_manager
            cp = get_checkpoint_manager(scraper_name)
            info = cp.get_checkpoint_info()
            
            if info["total_completed"] > 0:
                # Get total steps for this scraper
                scraper_info = self.scrapers.get(scraper_name)
                steps_list = scraper_info.get("steps", []) if scraper_info else []

                total_steps = len(steps_list)
                if total_steps is not None and info['next_step'] >= total_steps:
                    status_text = f"Checkpoint: All {total_steps} steps completed"
                    resume_text = "Pipeline finished"
                else:
                    status_text = f"Checkpoint: Step {info['last_completed_step']}/{total_steps - 1 if total_steps else '?'} completed"
                    resume_text = f"Resume from step {info['next_step']}"
            else:
                status_text = "Checkpoint: No checkpoint"
                resume_text = "Will start from step 0"
            
            if hasattr(self, 'checkpoint_status_label'):
                self.checkpoint_status_label.config(text=status_text)
            if hasattr(self, 'checkpoint_resume_label'):
                self.checkpoint_resume_label.config(text=resume_text)
        except Exception as e:
            if hasattr(self, 'checkpoint_status_label'):
                self.checkpoint_status_label.config(text=f"Checkpoint: Error - {str(e)[:50]}")
            if hasattr(self, 'checkpoint_resume_label'):
                self.checkpoint_resume_label.config(text="")
        finally:
            self.update_timeline_status()

    def _api_base_url(self):
        """Return base URL for embedded API server."""
        return f"http://127.0.0.1:{self._api_server_port}"

    def _build_local_step_snapshot(self, scraper_name, cp):
        """Best-effort step snapshot from local checkpoint when API is unavailable."""
        scraper_info = self.scrapers.get(scraper_name, {})
        steps = scraper_info.get("steps", [])
        info = cp.get_checkpoint_info()
        metadata = cp.get_metadata() or {}
        checkpoint_data = cp._load_checkpoint()
        step_outputs = checkpoint_data.get("step_outputs", {})
        completed_steps = set(info.get("completed_steps", []))
        current_step = metadata.get("current_step")
        snapshot = []

        for i, step in enumerate(steps):
            status = "pending"
            if i in completed_steps:
                status = "completed"
            elif current_step == i and metadata.get("status") in ("running", "resume"):
                status = "in_progress"

            row = {
                "step_number": i,
                "name": step.get("name"),
                "script": step.get("script"),
                "status": status,
                "source": "checkpoint",
            }
            step_output = step_outputs.get(f"step_{i}", {})
            if step_output:
                if step_output.get("completed_at"):
                    row["completed_at"] = step_output.get("completed_at")
                if step_output.get("duration_seconds") is not None:
                    row["duration_seconds"] = step_output.get("duration_seconds")
                row["output_files_count"] = len(step_output.get("output_files", []))
            snapshot.append(row)
        return snapshot

    def _fetch_state_timeline_payload(self, scraper_name, limit=200, prefer_api=True, api_timeout=1.0):
        """
        Fetch timeline/state payload from API.
        Falls back to local checkpoint data if API is unavailable.
        """
        api_error = None
        should_try_api = bool(prefer_api and _REQUESTS_AVAILABLE and getattr(self, "_api_server_running", False))
        if should_try_api:
            try:
                resp = requests.get(
                    f"{self._api_base_url()}/api/v1/scrapers/{scraper_name}/timeline",
                    params={"limit": int(limit)},
                    timeout=max(0.1, float(api_timeout)),
                )
                if resp.status_code == 200:
                    data = resp.json()
                    data["source"] = "api"
                    return data
                api_error = f"HTTP {resp.status_code}"
            except Exception as exc:
                api_error = str(exc)
        elif not _REQUESTS_AVAILABLE:
            api_error = "requests not available"
        elif not prefer_api:
            api_error = "api skipped"
        else:
            api_error = "api server off"

        try:
            from core.pipeline_checkpoint import get_checkpoint_manager
            cp = get_checkpoint_manager(scraper_name)
            metadata = cp.get_metadata() or {}
            run_id = metadata.get("run_id")
            events = cp.get_events(limit=max(1, min(int(limit), 2000)), run_id=run_id)
            return {
                "scraper": scraper_name,
                "run_id": run_id,
                "running": scraper_name in self.running_scrapers or scraper_name in self.running_processes,
                "event_count": len(events),
                "events": events,
                "step_snapshot": self._build_local_step_snapshot(scraper_name, cp),
                "source": "checkpoint",
                "api_error": api_error,
            }
        except Exception as exc:
            return {
                "scraper": scraper_name,
                "run_id": None,
                "running": scraper_name in self.running_scrapers or scraper_name in self.running_processes,
                "event_count": 0,
                "events": [],
                "step_snapshot": [],
                "source": "none",
                "api_error": api_error,
                "error": str(exc),
            }

    def update_timeline_status(self):
        """Update short timeline status line under checkpoint status."""
        if not hasattr(self, "timeline_status_label"):
            return
        scraper_name = self.scraper_var.get() if hasattr(self, "scraper_var") else ""
        if not scraper_name:
            self.timeline_status_label.config(text="Timeline: No scraper selected")
            return

        # Keep dropdown/label updates non-blocking: use local checkpoint snapshot only.
        payload = self._fetch_state_timeline_payload(scraper_name, limit=20, prefer_api=False)
        source = payload.get("source", "none")
        events = payload.get("events", []) or []
        run_id = payload.get("run_id") or "N/A"
        running = payload.get("running", False)

        if events:
            last = events[-1]
            ts = (last.get("timestamp") or "")[11:19] if last.get("timestamp") else "--:--:--"
            ev = last.get("event_type", "event")
            st = last.get("status", "")
            step_num = last.get("step_number")
            step_name = last.get("step_name", "")
            step_text = ""
            if step_num is not None:
                step_text = f" step {step_num}"
                if step_name:
                    step_text += f" ({step_name})"
            status_text = f" {st}" if st else ""
            run_flag = "RUNNING" if running else "IDLE"
            self.timeline_status_label.config(
                text=f"Timeline [{source}/{run_flag}] {ts} {ev}{status_text}{step_text} | run: {run_id}"
            )
        else:
            self.timeline_status_label.config(
                text=f"Timeline [{source}] no events yet | run: {run_id}"
            )

    def show_state_timeline(self):
        """Open a detailed state/step timeline viewer for selected scraper."""
        scraper_name = self.scraper_var.get()
        if not scraper_name:
            messagebox.showwarning("Warning", "Select a scraper first", parent=self.root)
            return

        payload = self._fetch_state_timeline_payload(scraper_name, limit=400, prefer_api=True, api_timeout=1.0)

        win = tk.Toplevel(self.root)
        win.title(f"State Timeline - {scraper_name}")
        win.geometry("1200x760")
        win.configure(bg=self.colors['white'])

        top = tk.Frame(win, bg=self.colors['white'])
        top.pack(fill=tk.X, padx=10, pady=(10, 6))

        summary_var = tk.StringVar(value="")
        tk.Label(
            top,
            textvariable=summary_var,
            bg=self.colors['white'],
            fg='#000000',
            font=self.fonts['standard'],
            anchor=tk.W
        ).pack(side=tk.LEFT, fill=tk.X, expand=True)

        auto_refresh_var = tk.BooleanVar(value=bool(payload.get("running", False)))

        def refresh_now():
            data = self._fetch_state_timeline_payload(scraper_name, limit=400, prefer_api=True, api_timeout=1.0)
            source = data.get("source", "none")
            run_id = data.get("run_id") or "N/A"
            running = data.get("running", False)
            events = data.get("events", []) or []
            steps = data.get("step_snapshot", []) or []
            api_error = data.get("api_error")

            source_text = source.upper()
            if api_error and source != "api":
                source_text += f" fallback ({api_error})"
            summary_var.set(
                f"Scraper: {scraper_name} | run_id: {run_id} | running: {running} | "
                f"events: {len(events)} | steps: {len(steps)} | source: {source_text}"
            )

            for item in step_tree.get_children():
                step_tree.delete(item)
            for row in sorted(steps, key=lambda r: r.get("step_number", 10**9)):
                step_tree.insert(
                    "",
                    tk.END,
                    values=(
                        row.get("step_number"),
                        row.get("status", ""),
                        row.get("name", ""),
                        row.get("duration_seconds", ""),
                        row.get("rows_processed", ""),
                        row.get("error_message", "") or "",
                    ),
                )

            for item in event_tree.get_children():
                event_tree.delete(item)
            for ev in events:
                event_tree.insert(
                    "",
                    tk.END,
                    values=(
                        ev.get("sequence", ""),
                        ev.get("timestamp", ""),
                        ev.get("event_type", ""),
                        ev.get("status", ""),
                        ev.get("step_number", ""),
                        ev.get("step_name", ""),
                        ev.get("message", ""),
                    ),
                    tags=("event_row",),
                )

            event_tree._events_payload = events  # attach for selection lookup

        ttk.Button(top, text="Refresh", command=refresh_now, style='Secondary.TButton').pack(side=tk.RIGHT, padx=(6, 0))
        ttk.Checkbutton(top, text="Auto-refresh (2s)", variable=auto_refresh_var, style='Secondary.TCheckbutton').pack(side=tk.RIGHT)

        splitter = tk.PanedWindow(win, orient=tk.VERTICAL, sashrelief=tk.RAISED, bg=self.colors['background_gray'])
        splitter.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        step_frame = tk.Frame(splitter, bg=self.colors['white'])
        splitter.add(step_frame, minsize=180)
        tk.Label(step_frame, text="Step Snapshot", bg=self.colors['white'], fg='#000000', font=self.fonts['bold']).pack(anchor=tk.W, pady=(6, 4))
        step_tree = ttk.Treeview(
            step_frame,
            columns=("step", "status", "name", "duration", "rows", "error"),
            show="headings",
            height=8,
        )
        step_tree.heading("step", text="Step")
        step_tree.heading("status", text="Status")
        step_tree.heading("name", text="Name")
        step_tree.heading("duration", text="Duration(s)")
        step_tree.heading("rows", text="Rows Processed")
        step_tree.heading("error", text="Error")
        step_tree.column("step", width=60, anchor=tk.CENTER)
        step_tree.column("status", width=100, anchor=tk.CENTER)
        step_tree.column("name", width=320, anchor=tk.W)
        step_tree.column("duration", width=100, anchor=tk.E)
        step_tree.column("rows", width=120, anchor=tk.E)
        step_tree.column("error", width=380, anchor=tk.W)
        step_tree.pack(fill=tk.BOTH, expand=True)

        event_frame = tk.Frame(splitter, bg=self.colors['white'])
        splitter.add(event_frame, minsize=260)
        tk.Label(event_frame, text="Event Timeline", bg=self.colors['white'], fg='#000000', font=self.fonts['bold']).pack(anchor=tk.W, pady=(6, 4))
        event_tree = ttk.Treeview(
            event_frame,
            columns=("seq", "time", "event", "status", "step", "step_name", "message"),
            show="headings",
            height=12,
        )
        event_tree.heading("seq", text="#")
        event_tree.heading("time", text="Timestamp")
        event_tree.heading("event", text="Event")
        event_tree.heading("status", text="Status")
        event_tree.heading("step", text="Step")
        event_tree.heading("step_name", text="Step Name")
        event_tree.heading("message", text="Message")
        event_tree.column("seq", width=60, anchor=tk.E)
        event_tree.column("time", width=170, anchor=tk.W)
        event_tree.column("event", width=170, anchor=tk.W)
        event_tree.column("status", width=110, anchor=tk.CENTER)
        event_tree.column("step", width=70, anchor=tk.CENTER)
        event_tree.column("step_name", width=250, anchor=tk.W)
        event_tree.column("message", width=330, anchor=tk.W)
        event_tree.pack(fill=tk.BOTH, expand=True)

        details_frame = tk.Frame(splitter, bg=self.colors['white'])
        splitter.add(details_frame, minsize=140)
        tk.Label(details_frame, text="Selected Event Details", bg=self.colors['white'], fg='#000000', font=self.fonts['bold']).pack(anchor=tk.W, pady=(6, 4))
        details_text = scrolledtext.ScrolledText(
            details_frame,
            wrap=tk.WORD,
            height=8,
            font=self.fonts['monospace'],
            bg='#1e1e1e',
            fg='#00cc66',
            insertbackground='#00cc66',
        )
        details_text.pack(fill=tk.BOTH, expand=True)

        def on_event_select(_event=None):
            sel = event_tree.selection()
            if not sel:
                return
            row_index = event_tree.index(sel[0])
            events = getattr(event_tree, "_events_payload", [])
            if row_index < 0 or row_index >= len(events):
                return
            details_text.config(state=tk.NORMAL)
            details_text.delete("1.0", tk.END)
            details_text.insert("1.0", json.dumps(events[row_index], indent=2, ensure_ascii=False))
            details_text.config(state=tk.DISABLED)

        event_tree.bind("<<TreeviewSelect>>", on_event_select)

        def schedule_auto_refresh():
            if not win.winfo_exists():
                return
            if auto_refresh_var.get():
                refresh_now()
            win.after(2000, schedule_auto_refresh)

        refresh_now()
        schedule_auto_refresh()
    
    def view_checkpoint_file(self):
        """View checkpoint file location and contents"""
        scraper_name = self.scraper_var.get()
        if not scraper_name:
            messagebox.showwarning("Warning", "Select a scraper first")
            return
        
        try:
            from core.pipeline_checkpoint import get_checkpoint_manager
            cp = get_checkpoint_manager(scraper_name)
            checkpoint_file = cp.checkpoint_file
            checkpoint_dir = cp.checkpoint_dir
            
            # Get checkpoint info
            info = cp.get_checkpoint_info()
            
            # Create a detailed message
            msg = f"Checkpoint File Location:\n{checkpoint_file}\n\n"
            msg += f"Checkpoint Directory:\n{checkpoint_dir}\n\n"
            msg += f"Checkpoint Status:\n"
            msg += f"  Scraper: {info['scraper']}\n"
            msg += f"  Last Run: {info['last_run'] or 'Never'}\n"
            msg += f"  Completed Steps: {info['completed_steps']}\n"
            msg += f"  Last Completed Step: {info['last_completed_step'] or 'None'}\n"
            msg += f"  Next Step: {info['next_step']}\n"
            msg += f"  Total Completed: {info['total_completed']}\n\n"
            
            if checkpoint_file.exists():
                # Read and show file contents (first 2000 chars)
                try:
                    with open(checkpoint_file, 'r', encoding='utf-8') as f:
                        contents = f.read()
                    if len(contents) > 2000:
                        contents = contents[:2000] + "\n\n... (truncated, file is too long)"
                    msg += f"File Contents:\n{contents}"
                except Exception as e:
                    msg += f"Could not read file contents: {e}"
            else:
                msg += "Checkpoint file does not exist yet."
            
            # Create a new window to display this
            view_window = tk.Toplevel(self.root)
            view_window.title(f"Checkpoint Details - {scraper_name}")
            view_window.geometry("700x500")
            
            # Text widget with scrollbar
            text_frame = ttk.Frame(view_window)
            text_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
            
            scrollbar = ttk.Scrollbar(text_frame)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            
            text_widget = tk.Text(text_frame, wrap=tk.WORD, yscrollcommand=scrollbar.set, 
                                 font=self.fonts['monospace'])
            text_widget.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            scrollbar.config(command=text_widget.yview)
            
            text_widget.insert("1.0", msg)
            text_widget.config(state=tk.DISABLED)
            
            # Button to open file location
            button_frame = ttk.Frame(view_window)
            button_frame.pack(fill=tk.X, padx=10, pady=5)
            
            def open_file_location():
                import os
                import subprocess
                try:
                    if sys.platform == "win32":
                        os.startfile(checkpoint_dir)
                    elif sys.platform == "darwin":
                        subprocess.run(["open", str(checkpoint_dir)])
                    else:
                        subprocess.run(["xdg-open", str(checkpoint_dir)])
                except Exception as e:
                    messagebox.showerror("Error", f"Could not open file location:\n{e}")
            
            ttk.Button(button_frame, text="Open Folder in Explorer", 
                      command=open_file_location).pack(side=tk.LEFT, padx=5)
            ttk.Button(button_frame, text="Close", 
                      command=view_window.destroy).pack(side=tk.RIGHT, padx=5)
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to view checkpoint:\n{e}")
    
    def clear_checkpoint(self):
        """Clear checkpoint for selected scraper"""
        scraper_name = self.scraper_var.get()
        if not scraper_name:
            messagebox.showwarning("Warning", "Select a scraper first")
            return
        
        if not messagebox.askyesno("Confirm", f"Clear checkpoint for {scraper_name}?\n\nThis will reset the pipeline to start from step 0 on next run."):
            return
        
        try:
            from core.pipeline_checkpoint import get_checkpoint_manager
            cp = get_checkpoint_manager(scraper_name)
            
            # For Argentina: Reset database status before clearing checkpoint
            if scraper_name == "Argentina":
                try:
                    from core.db.connection import CountryDB
                    from scripts.Argentina.db.schema import apply_argentina_schema
                    from scripts.Argentina.config_loader import get_output_dir
                    from pathlib import Path
                    import os
                    
                    # Get run_id - prefer existing run from database
                    output_dir = get_output_dir()
                    run_id_file = output_dir / '.current_run_id'
                    run_id = None
                    
                    # First try to get from checkpoint metadata (before clearing)
                    metadata = cp.get_metadata() or {}
                    run_id = metadata.get('run_id')
                    
                    # If not in metadata, try file
                    if not run_id and run_id_file.exists():
                        run_id = run_id_file.read_text(encoding='utf-8').strip()
                    
                    # If still no run_id, get latest run from database
                    if not run_id:
                        # Last resort: get run_id from ar_step_progress (the table we're updating)
                        # Prioritize run_id with status='running' in run_ledger
                        try:
                            db_temp = CountryDB("Argentina")
                            apply_argentina_schema(db_temp)
                            with db_temp.cursor() as cur:
                                # Prioritize run_id with status='running' in run_ledger
                                cur.execute("""
                                    SELECT sp.run_id, COUNT(*) as step_count
                                    FROM ar_step_progress sp
                                    JOIN run_ledger rl ON sp.run_id = rl.run_id
                                    WHERE sp.progress_key = 'pipeline'
                                      AND rl.scraper_name = 'Argentina'
                                      AND rl.status = 'running'
                                    GROUP BY sp.run_id
                                    ORDER BY step_count DESC, sp.run_id DESC
                                    LIMIT 1
                                """)
                                row = cur.fetchone()
                                if row:
                                    run_id = row[0]
                                
                                # If no match, get any run_id with step_progress (ignore run_ledger status)
                                if not run_id:
                                    cur.execute("""
                                        SELECT run_id, COUNT(*) as step_count
                                        FROM ar_step_progress
                                        WHERE progress_key = 'pipeline'
                                        GROUP BY run_id
                                        ORDER BY step_count DESC, run_id DESC
                                        LIMIT 1
                                    """)
                                    row = cur.fetchone()
                                    if row:
                                        run_id = row[0]
                                
                                # Final fallback: get latest run from run_ledger with status='running' if no step_progress exists
                                if not run_id:
                                    cur.execute("""
                                        SELECT run_id FROM run_ledger 
                                        WHERE scraper_name = 'Argentina' 
                                          AND status = 'running'
                                        ORDER BY started_at DESC 
                                        LIMIT 1
                                    """)
                                    row = cur.fetchone()
                                    if row:
                                        run_id = row[0]
                            db_temp.close()
                        except Exception:
                            pass
                    
                    if run_id:
                        # Ensure run_id is set in environment and file
                        os.environ["ARGENTINA_RUN_ID"] = run_id
                        run_id_file.parent.mkdir(parents=True, exist_ok=True)
                        run_id_file.write_text(run_id, encoding='utf-8')
                        
                        with CountryDB("Argentina") as db:
                            apply_argentina_schema(db)
                            
                            # Ensure run_id exists in run_ledger (required for foreign key constraint)
                            from core.db.models import run_ledger_ensure_exists
                            sql, params = run_ledger_ensure_exists(run_id, "Argentina", mode="resume")
                            with db.cursor() as cur:
                                cur.execute(sql, params)
                            db.commit()
                            
                            # Reset all step progress to 'pending' in ar_step_progress table
                            with db.cursor() as cur:
                                cur.execute("""
                                    UPDATE ar_step_progress
                                    SET status = 'pending',
                                        completed_at = NULL,
                                        error_message = NULL
                                    WHERE run_id = %s
                                      AND progress_key = 'pipeline'
                                      AND status IN ('completed', 'failed', 'in_progress')
                                """, (run_id,))
                                reset_steps = cur.rowcount
                                if reset_steps > 0:
                                    print(f"[CHECKPOINT] Reset {reset_steps} step progress records to 'pending' when clearing checkpoint")
                            
                            # Reset all failed/in_progress products to pending
                            with db.cursor() as cur:
                                cur.execute("""
                                    UPDATE ar_product_index
                                    SET status = 'pending'
                                    WHERE run_id = %s
                                      AND total_records = 0
                                      AND status IN ('failed', 'in_progress')
                                """, (run_id,))
                                reset_count = cur.rowcount
                                if reset_count > 0:
                                    print(f"[CHECKPOINT] Reset {reset_count} products to 'pending' when clearing checkpoint")
                            db.commit()
                except Exception as db_error:
                    print(f"[CHECKPOINT] Warning: Failed to update database: {db_error}")
                    import traceback
                    traceback.print_exc()
                    # Continue anyway - checkpoint clear should still work
            
            cp.clear_checkpoint()
            messagebox.showinfo("Success", f"Checkpoint cleared for {scraper_name}")
            self.update_checkpoint_status()
        except Exception as e:
            messagebox.showerror("Error", f"Failed to clear checkpoint:\n{e}")
    
    def show_validation_table(self):
        """Show validation/progress table for the currently selected scraper"""
        scraper_name = self.scraper_var.get()
        if not scraper_name:
            messagebox.showwarning("Warning", "Please select a scraper first")
            return
        
        # Route to appropriate validation viewer based on scraper
        if scraper_name == "Russia":
            self._show_russia_validation_table()
        elif scraper_name == "Netherlands":
            self._show_netherlands_validation_table()
        elif scraper_name == "NorthMacedonia":
            self._show_north_macedonia_validation_table()
        else:
            messagebox.showinfo("Validation Table",
                f"Validation table for {scraper_name} is not yet implemented.\n\n"
                f"Currently supported:\n - Russia (VED metrics)\n - Netherlands (Pipeline progress)\n - north_macedonia (Pipeline progress)")
    
    def _show_russia_validation_table(self):
        """Show Russia VED scraping progress with detailed metrics"""
        try:
            import sys
            sys.path.insert(0, str(self.repo_root / "scripts" / "Russia"))
            from core.db.connection import CountryDB
            
            db = CountryDB("Russia")
            
            with db.cursor() as cur:
                # Get run with most progress entries (not just latest)
                cur.execute('''
                    SELECT rl.run_id, rl.status, rl.started_at, COUNT(sp.id) as progress_count
                    FROM run_ledger rl
                    LEFT JOIN ru_step_progress sp ON rl.run_id = sp.run_id
                    GROUP BY rl.run_id, rl.status, rl.started_at
                    ORDER BY progress_count DESC, rl.started_at DESC
                    LIMIT 1
                ''')
                run = cur.fetchone()
                
                if not run or run[3] == 0:
                    messagebox.showinfo("Russia VED Progress", "No runs with progress data found in database.")
                    return
                
                run_id, status, started_at, progress_count = run
                
                # Get progress entries with metrics
                cur.execute('''
                    SELECT progress_key, status, rows_found, ean_found, rows_scraped,
                           rows_inserted, ean_missing, db_count_before, db_count_after,
                           started_at, completed_at, error_message
                    FROM ru_step_progress 
                    WHERE run_id = %s
                    ORDER BY progress_key
                ''', (run_id,))
                
                rows = cur.fetchall()
                
                # Create progress window
                progress_window = tk.Toplevel(self.root)
                progress_window.title(f"Russia VED Validation Table - Run {run_id[:20]}...")
                progress_window.geometry("1200x700")
                progress_window.configure(bg='white')
                
                # Header info
                header_frame = tk.Frame(progress_window, bg='white', padx=10, pady=10)
                header_frame.pack(fill=tk.X)
                
                tk.Label(header_frame, text=f"Run ID: {run_id}", 
                        bg='white', font=('Segoe UI', 10, 'bold')).pack(anchor=tk.W)
                tk.Label(header_frame, text=f"Status: {status} | Started: {started_at}", 
                        bg='white', font=('Segoe UI', 9)).pack(anchor=tk.W)
                tk.Label(header_frame, text=f"Total Pages Tracked: {len(rows)}", 
                        bg='white', font=('Segoe UI', 9)).pack(anchor=tk.W)
                
                # Summary statistics
                # Column indices: 0=key, 1=status, 2=rows_found, 3=ean_found, 4=rows_scraped, 
                #                 5=rows_inserted, 6=ean_missing, 7=db_before, 8=db_after
                if rows:
                    total_rows_found = sum(r[2] or 0 for r in rows)
                    total_ean_found = sum(r[3] or 0 for r in rows)
                    total_inserted = sum(r[5] or 0 for r in rows)  # Fixed: was r[6]
                    total_missing = sum(r[6] or 0 for r in rows)   # Fixed: was r[5]
                    
                    summary_text = (f"Summary: {total_rows_found} rows found, {total_ean_found} EANs found, "
                                  f"{total_inserted} inserted, {total_missing} missing EAN")
                    tk.Label(header_frame, text=summary_text, 
                            bg='white', font=('Segoe UI', 9, 'bold'), fg='#0066cc').pack(anchor=tk.W, pady=(5, 0))
                
                # Create treeview for detailed data
                columns = ('Page', 'Status', 'Rows', 'EAN', 'Scraped', 'Inserted', 
                          'Missing', 'DB Before', 'DB After', 'Started', 'Completed')
                tree = ttk.Treeview(progress_window, columns=columns, show='headings', height=25)
                
                # Define column widths and headings
                col_widths = {
                    'Page': 80, 'Status': 100, 'Rows': 60, 'EAN': 60, 
                    'Scraped': 70, 'Inserted': 70, 'Missing': 70,
                    'DB Before': 80, 'DB After': 80, 'Started': 140, 'Completed': 140
                }
                
                for col in columns:
                    tree.heading(col, text=col)
                    tree.column(col, width=col_widths.get(col, 100), anchor='center')
                
                # Add scrollbar
                scrollbar = ttk.Scrollbar(progress_window, orient=tk.VERTICAL, command=tree.yview)
                tree.configure(yscrollcommand=scrollbar.set)
                
                # Pack tree and scrollbar
                tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(10, 0), pady=10)
                scrollbar.pack(side=tk.RIGHT, fill=tk.Y, padx=(0, 10), pady=10)
                
                # Insert data
                for row in rows:
                    (page_key, status, rows_found, ean_found, rows_scraped, rows_inserted,
                     ean_missing, db_before, db_after, started, completed, error) = row
                    
                    # Extract page number from key (e.g., "ved_page:1" -> "1")
                    page_num = page_key.split(':')[-1] if ':' in page_key else page_key
                    
                    # Determine status color
                    status_display = status
                    if status == 'completed':
                        status_display = '✓ Done'
                    elif status == 'ean_missing':
                        status_display = '⚠ EAN Missing'
                    elif status == 'failed':
                        status_display = '✗ Failed'
                    
                    tree.insert('', tk.END, values=(
                        page_num, status_display,
                        rows_found or 0, ean_found or 0, rows_scraped or 0,
                        rows_inserted or 0, ean_missing or 0,
                        db_before or 0, db_after or 0,
                        str(started)[:19] if started else '',
                        str(completed)[:19] if completed else ''
                    ), tags=(status,))
                
                # Configure tag colors
                tree.tag_configure('completed', background='#e6ffe6')
                tree.tag_configure('ean_missing', background='#fff3e6')
                tree.tag_configure('failed', background='#ffe6e6')
                
                # Close button
                ttk.Button(progress_window, text="Close", 
                          command=progress_window.destroy).pack(pady=(0, 10))
                
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load Russia VED progress:\n{e}")
            import traceback
            traceback.print_exc()

    def _show_netherlands_validation_table(self):
        """Show Netherlands pipeline step progress from nl_step_progress"""
        try:
            from core.db.postgres_connection import get_db
            
            db = get_db("Netherlands")
            
            with db.cursor() as cur:
                # Find the run_id from run_ledger that has the most progress entries
                cur.execute('''
                    SELECT rl.run_id, rl.status, rl.started_at, COUNT(sp.id) as progress_count
                    FROM run_ledger rl
                    LEFT JOIN nl_step_progress sp ON rl.run_id = sp.run_id
                    GROUP BY rl.run_id, rl.status, rl.started_at
                    ORDER BY progress_count DESC, rl.started_at DESC
                    LIMIT 1
                ''')
                run = cur.fetchone()
                
                if not run or run[3] == 0:
                    messagebox.showinfo("Netherlands Progress", "No runs with progress data found in database.")
                    return
                
                run_id, status, started_at, progress_count = run
                
                # Get all progress entries for this run
                cur.execute('''
                    SELECT step_number, step_name, status, error_message, started_at, completed_at
                    FROM nl_step_progress
                    WHERE run_id = %s
                    ORDER BY step_number
                ''', (run_id,))
                
                rows = cur.fetchall()
                
                # Create progress window
                progress_window = tk.Toplevel(self.root)
                progress_window.title(f"Netherlands Pipeline Progress - Run {run_id[:20]}...")
                progress_window.geometry("1000x600")
                progress_window.configure(bg='white')
                
                # Header info
                header_frame = tk.Frame(progress_window, bg='white', padx=20, pady=20)
                header_frame.pack(fill=tk.X)
                
                tk.Label(header_frame, text=f"Netherlands Pipeline Run Details", 
                        bg='white', font=('Segoe UI', 14, 'bold')).pack(anchor=tk.W)
                
                info_frame = tk.Frame(header_frame, bg='white', pady=10)
                info_frame.pack(fill=tk.X)
                
                tk.Label(info_frame, text=f"Run ID: {run_id}", 
                        bg='white', font=('Segoe UI', 10)).pack(anchor=tk.W)
                tk.Label(info_frame, text=f"Pipeline Status: {status} | Started: {started_at}", 
                        bg='white', font=('Segoe UI', 10)).pack(anchor=tk.W)
                
                # Progress Treeview
                columns = ('Step #', 'Step Name', 'Status', 'Started At', 'Completed At', 'Error')
                tree = ttk.Treeview(progress_window, columns=columns, show='headings', height=15)
                
                col_widths = {
                    'Step #': 70, 'Step Name': 250, 'Status': 120, 
                    'Started At': 160, 'Completed At': 160, 'Error': 200
                }
                
                for col in columns:
                    tree.heading(col, text=col)
                    tree.column(col, width=col_widths.get(col, 150), anchor=tk.W if col in ('Step Name', 'Error') else tk.CENTER)
                
                # Add scrollbar
                scrollbar = ttk.Scrollbar(progress_window, orient=tk.VERTICAL, command=tree.yview)
                tree.configure(yscrollcommand=scrollbar.set)
                
                tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(20, 0), pady=(0, 20))
                scrollbar.pack(side=tk.RIGHT, fill=tk.Y, padx=(0, 20), pady=(0, 20))
                
                # Insert data
                status_icons = {
                    'completed': '✓ Completed',
                    'in_progress': '↻ In Progress',
                    'failed': '✗ Failed',
                    'pending': '○ Pending',
                    'skipped': '→ Skipped'
                }
                
                for row in rows:
                    step_num, step_name, status, error, started, completed = row
                    
                    status_display = status_icons.get(status, status)
                    
                    tree.insert('', tk.END, values=(
                        step_num, step_name, status_display,
                        str(started)[:19] if started else '-',
                        str(completed)[:19] if completed else '-',
                        error or ''
                    ), tags=(status,))
                
                # Configure tag colors
                tree.tag_configure('completed', background='#e6ffe6')   # Light green
                tree.tag_configure('in_progress', background='#fff9e6') # Light yellow
                tree.tag_configure('failed', background='#ffe6e6')      # Light red
                tree.tag_configure('skipped', background='#f0f0f0')     # Light gray
                
                # Footer actions
                footer = tk.Frame(progress_window, bg='white', pady=15)
                footer.pack(fill=tk.X)
                
                ttk.Button(footer, text="Close", command=progress_window.destroy, 
                          style='Secondary.TButton').pack()

        except Exception as e:
            messagebox.showerror("Error", f"Failed to load Netherlands progress:\n{e}")
            import traceback
            traceback.print_exc()

    def _show_north_macedonia_validation_table(self):
        """Show north_macedonia pipeline step progress from nm_step_progress"""
        try:
            from core.db.postgres_connection import get_db

            db = get_db("NorthMacedonia")

            with db.cursor() as cur:
                # Find the run_id from run_ledger that has the most progress entries
                cur.execute('''
                    SELECT rl.run_id, rl.status, rl.started_at, COUNT(sp.id) as progress_count
                    FROM run_ledger rl
                    LEFT JOIN nm_step_progress sp ON rl.run_id = sp.run_id
                    WHERE rl.scraper_name = 'NorthMacedonia'
                    GROUP BY rl.run_id, rl.status, rl.started_at
                    ORDER BY progress_count DESC, rl.started_at DESC
                    LIMIT 1
                ''')
                run = cur.fetchone()

                if not run or run[3] == 0:
                    messagebox.showinfo("north_macedonia Progress", "No runs with progress data found in database.")
                    return

                run_id, status, started_at, progress_count = run

                # Get all progress entries for this run
                cur.execute('''
                    SELECT step_number, step_name, status, started_at, completed_at, error_message
                    FROM nm_step_progress
                    WHERE run_id = %s
                    ORDER BY step_number, started_at
                ''', (run_id,))

                rows = cur.fetchall()

                # Create progress window
                progress_window = tk.Toplevel(self.root)
                progress_window.title(f"north_macedonia Pipeline Progress - Run {run_id[:20]}...")
                progress_window.geometry("1000x600")
                progress_window.configure(bg='white')

                # Header info
                header_frame = tk.Frame(progress_window, bg='white', padx=20, pady=20)
                header_frame.pack(fill=tk.X)

                tk.Label(header_frame, text=f"north_macedonia Pipeline Run Details",
                        bg='white', font=('Segoe UI', 14, 'bold')).pack(anchor=tk.W)

                info_frame = tk.Frame(header_frame, bg='white', pady=10)
                info_frame.pack(fill=tk.X)

                tk.Label(info_frame, text=f"Run ID: {run_id}", bg='white', font=('Courier', 10)).pack(anchor=tk.W)
                tk.Label(info_frame, text=f"Status: {status}", bg='white', font=('Courier', 10)).pack(anchor=tk.W)
                tk.Label(info_frame, text=f"Started: {started_at}", bg='white', font=('Courier', 10)).pack(anchor=tk.W)
                tk.Label(info_frame, text=f"Progress Entries: {progress_count}", bg='white', font=('Courier', 10)).pack(anchor=tk.W)

                # Table frame
                table_frame = tk.Frame(progress_window, bg='white', padx=20)
                table_frame.pack(fill=tk.BOTH, expand=True)

                # Scrollbar
                scrollbar = ttk.Scrollbar(table_frame)
                scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

                # Treeview
                columns = ("Step", "Name", "Status", "Started", "Completed", "Error")
                tree = ttk.Treeview(table_frame, columns=columns, show='headings', yscrollcommand=scrollbar.set)
                scrollbar.config(command=tree.yview)

                # Column headers
                tree.heading("Step", text="Step #")
                tree.heading("Name", text="Step Name")
                tree.heading("Status", text="Status")
                tree.heading("Started", text="Started At")
                tree.heading("Completed", text="Completed At")
                tree.heading("Error", text="Error Message")

                # Column widths
                tree.column("Step", width=60)
                tree.column("Name", width=200)
                tree.column("Status", width=100)
                tree.column("Started", width=150)
                tree.column("Completed", width=150)
                tree.column("Error", width=300)

                # Insert data
                for row in rows:
                    step_num, step_name, step_status, start_time, complete_time, error = row
                    tree.insert('', tk.END, values=(
                        step_num,
                        step_name,
                        step_status,
                        str(start_time) if start_time else "",
                        str(complete_time) if complete_time else "",
                        error or ""
                    ))

                tree.pack(fill=tk.BOTH, expand=True)

                # Footer with close button
                footer = tk.Frame(progress_window, bg='white', pady=20)
                footer.pack()

                ttk.Button(footer, text="Close", command=progress_window.destroy,
                          style='Secondary.TButton').pack()

        except Exception as e:
            messagebox.showerror("Error", f"Failed to load north_macedonia progress:\n{e}")
            import traceback
            traceback.print_exc()

    def update_system_stats(self):
        """Update system statistics (Chrome, Tor, RAM, CPU, GPU, Network)"""
        try:
            import psutil
            
            # Count Chrome instances tracked by scrapers (avoid counting user Chrome)
            chrome_count = 0
            tracked_pids = set()
            try:
                from core.chrome_pid_tracker import load_chrome_pids
                for scraper_name in self.scrapers.keys():
                    pids = load_chrome_pids(scraper_name, self.repo_root)
                    if not pids:
                        pids = self._infer_chrome_pids_from_lock(scraper_name)
                        if pids:
                            try:
                                from core.chrome_pid_tracker import save_chrome_pids
                                save_chrome_pids(scraper_name, self.repo_root, pids)
                            except Exception:
                                pass
                    tracked_pids.update(pids)
            except Exception:
                tracked_pids = set()

            if tracked_pids:
                for pid in tracked_pids:
                    try:
                        proc = psutil.Process(pid)
                        if proc.is_running():
                            chrome_count += 1
                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                        pass
            
            # Count Tor instances - total running
            tor_count = 0
            try:
                for proc in psutil.process_iter(['pid', 'name']):
                    try:
                        name = proc.info['name'] or ''
                        if 'tor' in name.lower():
                            tor_count += 1
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
            except Exception:
                tor_count = 0
            
            # Get RAM usage
            ram_str = "N/A"
            try:
                ram = psutil.virtual_memory()
                ram_percent = ram.percent
                ram_used_gb = ram.used / (1024**3)
                ram_total_gb = ram.total / (1024**3)
                ram_str = f"{ram_percent:.1f}% ({ram_used_gb:.1f}/{ram_total_gb:.1f}GB)"
            except Exception:
                ram_str = "N/A"
            
            # Get CPU usage
            cpu_str = "N/A"
            try:
                cpu_percent = psutil.cpu_percent(interval=0.1)
                cpu_str = f"{cpu_percent:.1f}%"
            except Exception:
                cpu_str = "N/A"
            
            # Get GPU usage (try multiple methods)
            gpu_str = "N/A"
            try:
                # Try nvidia-ml-py for NVIDIA GPUs (most common)
                try:
                    import pynvml
                    pynvml.nvmlInit()
                    handle = pynvml.nvmlDeviceGetHandleByIndex(0)
                    gpu_util = pynvml.nvmlDeviceGetUtilizationRates(handle)
                    gpu_str = f"{gpu_util.gpu:.1f}%"
                    pynvml.nvmlShutdown()
                except (ImportError, AttributeError, Exception):
                    # Try GPUtil as alternative (simpler API)
                    try:
                        import GPUtil
                        gpus = GPUtil.getGPUs()
                        if gpus and len(gpus) > 0:
                            gpu_str = f"{gpus[0].load * 100:.1f}%"
                    except (ImportError, Exception):
                        # GPU libraries not available - show N/A
                        gpu_str = "N/A"
            except Exception:
                gpu_str = "N/A"
            
            # Get network usage (bytes sent/received per second)
            network_str = "N/A"
            try:
                import time
                # Get network I/O stats
                net_io = psutil.net_io_counters()
                if net_io:
                    current_time = time.time()
                    current_sent = net_io.bytes_sent
                    current_recv = net_io.bytes_recv
                    
                    if self._prev_net_time is not None:
                        # Calculate rate (bytes per second)
                        time_diff = current_time - self._prev_net_time
                        if time_diff > 0:
                            sent_rate = (current_sent - self._prev_net_sent) / time_diff
                            recv_rate = (current_recv - self._prev_net_recv) / time_diff
                            
                            # Format to show KB/s or MB/s
                            if sent_rate < 1024 and recv_rate < 1024:
                                network_str = f"↑{sent_rate:.1f}KB/s ↓{recv_rate:.1f}KB/s"
                            elif sent_rate < 1024**2 and recv_rate < 1024**2:
                                sent_mb = sent_rate / 1024
                                recv_mb = recv_rate / 1024
                                network_str = f"↑{sent_mb:.2f}MB/s ↓{recv_mb:.2f}MB/s"
                            else:
                                sent_gb = sent_rate / (1024**2)
                                recv_gb = recv_rate / (1024**2)
                                network_str = f"↑{sent_gb:.2f}GB/s ↓{recv_gb:.2f}GB/s"
                        else:
                            network_str = "↑0KB/s ↓0KB/s"
                    else:
                        network_str = "Calculating..."
                    
                    # Store current values for next calculation
                    self._prev_net_sent = current_sent
                    self._prev_net_recv = current_recv
                    self._prev_net_time = current_time
            except Exception:
                network_str = "N/A"
            
            # Update labels - show metrics in 2 lines with human-readable formatting
            if hasattr(self, 'system_stats_label_line1'):
                # Line 1: Chrome, Tor, RAM, CPU
                line1_text = f"Chrome Instances (tracked): {chrome_count}  |  Tor Instances: {tor_count}  |  RAM Usage: {ram_str}  |  CPU Usage: {cpu_str}"
                self.system_stats_label_line1.config(text=line1_text)
            
            if hasattr(self, 'system_stats_label_line2'):
                # Line 2: GPU, Network
                line2_text = f"GPU Usage: {gpu_str}  |  Network: {network_str}"
                self.system_stats_label_line2.config(text=line2_text)
        except ImportError:
            # psutil not available
            if hasattr(self, 'system_stats_label_line1'):
                self.system_stats_label_line1.config(
                    text="Chrome Instances (tracked): --  |  Tor Instances: --  |  RAM Usage: --  |  CPU Usage: -- (psutil not available)")
            if hasattr(self, 'system_stats_label_line2'):
                self.system_stats_label_line2.config(text="GPU Usage: --  |  Network: --")
        except Exception:
            if hasattr(self, 'system_stats_label_line1'):
                self.system_stats_label_line1.config(text="Chrome Instances (tracked): --  |  Tor Instances: --  |  RAM Usage: --  |  CPU Usage: --")
            if hasattr(self, 'system_stats_label_line2'):
                self.system_stats_label_line2.config(text="GPU Usage: --  |  Network: --")
        
        # Schedule next update (every 2 seconds)
        self.root.after(2000, self.update_system_stats)
    
    def update_chrome_count(self):
        """Update Chrome instance count for selected scraper"""
        scraper_name = self.scraper_var.get()
        if not scraper_name:
            if hasattr(self, 'chrome_count_label'):
                self.chrome_count_label.config(text="Chrome Instances: 0")
            return
        
        active_count = 0
        try:
            from core.chrome_pid_tracker import load_chrome_pids
            import psutil
            
            # Load tracked PIDs for this scraper
            pids = load_chrome_pids(scraper_name, self.repo_root)
            if not pids:
                pids = self._infer_chrome_pids_from_lock(scraper_name)
                if pids:
                    try:
                        from core.chrome_pid_tracker import save_chrome_pids
                        save_chrome_pids(scraper_name, self.repo_root, pids)
                    except Exception:
                        pass
            
            # Count active PIDs (processes that still exist)
            if pids:
                for pid in pids:
                    try:
                        proc = psutil.Process(pid)
                        if proc.is_running():
                            active_count += 1
                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                        pass
            
            # Also check ChromeManager as fallback (for in-process drivers)
            try:
                from core.chrome_manager import get_chrome_driver_count
                manager_count = get_chrome_driver_count()
                # Use the higher count (PID tracking is more accurate for multi-process scenarios)
                if manager_count > active_count:
                    active_count = manager_count
            except Exception:
                pass  # ChromeManager check failed, use PID count
            
            # Update label
            if hasattr(self, 'chrome_count_label'):
                self.chrome_count_label.config(text=f"Chrome Instances: {active_count}")
        except ImportError:
            # psutil not available, try alternative method
            try:
                from core.chrome_pid_tracker import load_chrome_pids
                pids = load_chrome_pids(scraper_name, self.repo_root)
                count = len(pids) if pids else 0
                
                # Also check ChromeManager
                try:
                    from core.chrome_manager import get_chrome_driver_count
                    manager_count = get_chrome_driver_count()
                    if manager_count > count:
                        count = manager_count
                except Exception:
                    pass
                
                if hasattr(self, 'chrome_count_label'):
                    self.chrome_count_label.config(text=f"Chrome Instances: {count} (estimated)")
            except Exception:
                # Last resort: check ChromeManager only
                try:
                    from core.chrome_manager import get_chrome_driver_count
                    manager_count = get_chrome_driver_count()
                    if hasattr(self, 'chrome_count_label'):
                        self.chrome_count_label.config(text=f"Chrome Instances: {manager_count} (manager)")
                except Exception:
                    if hasattr(self, 'chrome_count_label'):
                        self.chrome_count_label.config(text="Chrome Instances: Unknown")
        except Exception as e:
            # Try ChromeManager as fallback even on error
            try:
                from core.chrome_manager import get_chrome_driver_count
                manager_count = get_chrome_driver_count()
                if hasattr(self, 'chrome_count_label'):
                    self.chrome_count_label.config(text=f"Chrome Instances: {manager_count} (manager)")
            except Exception:
                if hasattr(self, 'chrome_count_label'):
                    self.chrome_count_label.config(text="Chrome Instances: Error")

    def update_network_info(self, scraper_name: str = None, force_refresh: bool = False):
        """Update network info display for selected scraper. Use force_refresh=True to re-detect (e.g. after pipeline may have started Tor)."""
        if scraper_name is None:
            scraper_name = self.scraper_var.get()
        
        if not scraper_name or not hasattr(self, 'network_info_label'):
            return
        
        # Run in background thread to avoid blocking UI
        def _fetch_network_info():
            try:
                from core.network_info import get_network_info_for_scraper, format_network_status
                info = get_network_info_for_scraper(scraper_name, force_refresh=force_refresh)
                status_text = format_network_status(info)
                
                # Update UI in main thread
                self.root.after(0, lambda: self.network_info_label.config(
                    text=f"Network: {status_text}",
                    fg='#006600' if info.network_type == 'Tor' else 
                       '#0066cc' if info.network_type == 'VPN' else
                       '#666666'
                ))
            except Exception as e:
                # Update UI with error
                self.root.after(0, lambda: self.network_info_label.config(
                    text=f"Network: Error detecting",
                    fg='#cc0000'
                ))
        
        # Start background thread
        thread = threading.Thread(target=_fetch_network_info, daemon=True)
        thread.start()

    def _infer_chrome_pids_from_lock(self, scraper_name: str):
        """Infer Chrome/ChromeDriver PIDs from the scraper lock process tree."""
        try:
            import psutil
        except Exception:
            return set()

        lock_active, pid, _log_path, _lock_file = self._get_lock_status(scraper_name)
        if not lock_active or not pid:
            return set()

        inferred = set()
        try:
            parent = psutil.Process(pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            return set()

        try:
            children = parent.children(recursive=True)
        except Exception:
            children = []

        for proc in children:
            try:
                name = (proc.name() or "").lower()
                # Playwright/Selenium browsers may show up as chrome/chromium/msedge/brave, etc.
                if any(token in name for token in ("chrome", "chromium", "msedge", "brave", "opera")):
                    inferred.add(proc.pid)
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue

        return inferred
    
    def manage_checkpoint(self):
        """Open dialog to manage checkpoint steps (roll back or add steps)"""
        scraper_name = self.scraper_var.get()
        if not scraper_name:
            messagebox.showwarning("Warning", "Select a scraper first")
            return
        
        try:
            from core.pipeline_checkpoint import get_checkpoint_manager
            cp = get_checkpoint_manager(scraper_name)
            info = cp.get_checkpoint_info()
            
            # Get scraper steps
            scraper_info = self.scrapers.get(scraper_name)
            if not scraper_info:
                messagebox.showerror("Error", f"Scraper {scraper_name} not found")
                return
            
            steps = scraper_info.get("steps", [])
            if not steps:
                messagebox.showwarning("Warning", f"No steps defined for {scraper_name}")
                return
            
            # Create dialog window
            dialog = tk.Toplevel(self.root)
            dialog.title(f"Manage Checkpoint - {scraper_name}")
            dialog.geometry("600x500")
            dialog.transient(self.root)
            dialog.grab_set()
            
            # Main frame
            main_frame = ttk.Frame(dialog, padding=10)
            main_frame.pack(fill=tk.BOTH, expand=True)
            
            # Instructions
            instructions = ttk.Label(main_frame, 
                text="Select steps to mark as complete. Uncheck to roll back.\nClick 'Apply' to save changes.",
                font=self.fonts['standard'])
            instructions.pack(pady=(0, 10))
            
            # Scrollable frame for checkboxes
            canvas_frame = ttk.Frame(main_frame)
            canvas_frame.pack(fill=tk.BOTH, expand=True)
            
            canvas = tk.Canvas(canvas_frame, height=300)
            scrollbar = ttk.Scrollbar(canvas_frame, orient="vertical", command=canvas.yview)
            scrollable_frame = ttk.Frame(canvas)
            
            scrollable_frame.bind(
                "<Configure>",
                lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
            )
            
            canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
            canvas.configure(yscrollcommand=scrollbar.set)
            
            # Store checkboxes and their step numbers
            step_vars = {}
            completed_steps = set(info.get("completed_steps", []))
            
            # Flag to disable validation during programmatic updates
            _updating_programmatically = False
            
            def on_checkbox_change(idx, var):
                """Handle checkbox change with sequential validation"""
                # Skip validation during programmatic updates
                if _updating_programmatically:
                    return
                    
                if var.get():
                    # Checking a step - ensure all previous steps are checked
                    for prev_idx in range(idx):
                        if not step_vars[prev_idx].get():
                            # Uncheck this step and show warning
                            var.set(False)
                            messagebox.showwarning("Invalid Selection", 
                                f"Cannot skip steps!\n\n"
                                f"You must complete steps sequentially.\n"
                                f"Step {idx} cannot be marked complete until step {prev_idx} is complete.")
                            return
                else:
                    # Unchecking a step - uncheck all subsequent steps
                    for next_idx in range(idx + 1, len(steps)):
                        if step_vars[next_idx].get():
                            step_vars[next_idx].set(False)
            
            # Create checkbox for each step
            step_index_map = {}  # Maps GUI index to actual step number
            actual_step_nums = []

            # Use sequential step numbering based on current scraper step list.
            actual_step_nums = list(range(len(steps)))
            for gui_idx in range(len(steps)):
                step_index_map[gui_idx] = gui_idx
            
            for gui_idx, step in enumerate(steps):
                actual_step = step_index_map.get(gui_idx, gui_idx)
                is_completed = actual_step in completed_steps
                
                # Check if step should be skipped by default
                skip_by_default = step.get("skip_by_default", False)
                
                # Completed steps should always render checked, even for skip_by_default steps.
                var = tk.BooleanVar(value=is_completed)
                step_vars[gui_idx] = var
                
                step_frame = ttk.Frame(scrollable_frame)
                step_frame.pack(fill=tk.X, padx=5, pady=2)
                
                step_display = f"Step {actual_step}"
                checkbox_text = f"{step_display}: {step['name']}"
                if skip_by_default:
                    checkbox_text += " (SKIPPED BY DEFAULT)"
                
                checkbox = ttk.Checkbutton(step_frame, text=checkbox_text, 
                                           variable=var,
                                           command=lambda i=gui_idx, v=var: on_checkbox_change(i, v))
                checkbox.pack(side=tk.LEFT, padx=5)
                
                # Show status
                if skip_by_default and not is_completed:
                    status_text = "⊘ Skipped"
                    status_color = "#999999"
                elif is_completed:
                    status_text = "✓ Complete"
                    status_color = "#00AA00"
                else:
                    status_text = "○ Pending"
                    status_color = "#666666"
                    
                status_label = ttk.Label(step_frame, text=status_text, 
                                         font=("Segoe UI", 8), foreground=status_color)
                status_label.pack(side=tk.LEFT, padx=10)
            
            canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            
            # Button frame
            button_frame = ttk.Frame(main_frame)
            button_frame.pack(fill=tk.X, pady=(10, 0))
            
            def apply_changes():
                """Apply checkpoint changes"""
                try:
                    # Get selected steps (map GUI indices to actual step numbers)
                    selected_gui_indices = [gui_idx for gui_idx, var in step_vars.items() if var.get()]
                    
                    # Map to actual step numbers
                    selected_steps = [step_index_map[gui_idx] for gui_idx in selected_gui_indices]
                    
                    # Validate sequential selection (0..N without gaps)
                    if selected_steps:
                        expected_steps = list(range(len(selected_steps)))
                        if selected_steps != expected_steps:
                            messagebox.showerror("Invalid Selection", 
                                f"Cannot skip steps!\n\n"
                                f"Steps must be marked sequentially from the beginning.\n"
                                f"Selected steps: {selected_steps}\n"
                                f"Expected: {expected_steps}\n\n"
                                f"Please uncheck steps from the end to roll back, or check steps sequentially from the beginning.")
                            return
                    
                    # Get previous completed steps to detect which steps were unmarked
                    previous_completed = set(info.get("completed_steps", []))
                    new_completed = set(selected_steps)
                    unmarked_steps = previous_completed - new_completed
                    
                    # Get run_id BEFORE clearing checkpoint (to preserve it)
                    # Priority: checkpoint metadata > .current_run_id file > run_id from completed steps in checkpoint
                    run_id_before_clear = None
                    if scraper_name == "Argentina":
                        try:
                            from scripts.Argentina.config_loader import get_output_dir
                            metadata_before = cp.get_metadata() or {}
                            run_id_before_clear = metadata_before.get('run_id')
                            
                            # If not in metadata, try .current_run_id file
                            if not run_id_before_clear:
                                output_dir = get_output_dir()
                                run_id_file = output_dir / '.current_run_id'
                                if run_id_file.exists():
                                    run_id_before_clear = run_id_file.read_text(encoding='utf-8').strip()
                            
                            # If still no run_id, try to get from completed steps in checkpoint
                            # This helps when checkpoint metadata was lost but steps are still marked
                            if not run_id_before_clear:
                                completed_steps = info.get("completed_steps", [])
                                if completed_steps:
                                    try:
                                        from core.db.connection import CountryDB
                                        from scripts.Argentina.db.schema import apply_argentina_schema
                                        db_temp = CountryDB("Argentina")
                                        apply_argentina_schema(db_temp)
                                        with db_temp.cursor() as cur:
                                            # Get run_id that has step_progress matching the completed steps
                                            # This ensures we use the run_id associated with the checkpoint
                                            cur.execute("""
                                                SELECT run_id, COUNT(*) as step_count
                                                FROM ar_step_progress
                                                WHERE progress_key = 'pipeline'
                                                  AND step_number = ANY(%s)
                                                  AND status = 'completed'
                                                GROUP BY run_id
                                                ORDER BY step_count DESC, run_id DESC
                                                LIMIT 1
                                            """, ([s for s in completed_steps],))
                                            row = cur.fetchone()
                                            if row:
                                                run_id_before_clear = row[0]
                                        db_temp.close()
                                    except Exception:
                                        pass
                        except Exception:
                            # If import fails, just use metadata
                            metadata_before = cp.get_metadata() or {}
                            run_id_before_clear = metadata_before.get('run_id')
                    
                    # Clear checkpoint first
                    cp.clear_checkpoint()
                    
                    # Preserve run_id in checkpoint metadata if we had one
                    if run_id_before_clear:
                        cp.update_metadata({"run_id": run_id_before_clear})
                    
                    # For Argentina: Reset database status for unmarked steps
                    if scraper_name == "Argentina" and (unmarked_steps or not selected_steps):
                        try:
                            from core.db.connection import CountryDB
                            from scripts.Argentina.db.schema import apply_argentina_schema
                            from scripts.Argentina.config_loader import get_output_dir
                            from pathlib import Path
                            import os
                            
                            # Use the preserved run_id
                            run_id = run_id_before_clear
                            
                            # If still no run_id, try to get from file or metadata
                            if not run_id:
                                output_dir = get_output_dir()
                                run_id_file = output_dir / '.current_run_id'
                                if run_id_file.exists():
                                    run_id = run_id_file.read_text(encoding='utf-8').strip()
                                else:
                                    # Try to get from checkpoint metadata (after clear, but we restored it)
                                    metadata = cp.get_metadata() or {}
                                    run_id = metadata.get('run_id')
                            
                            if not run_id:
                                # Last resort: try to match run_id with checkpoint's completed steps
                                # This ensures we use the run_id associated with the checkpoint, even if records were deleted
                                try:
                                    completed_steps = selected_steps  # Steps that will be marked complete
                                    if completed_steps:
                                        db_temp = CountryDB("Argentina")
                                        apply_argentina_schema(db_temp)
                                        with db_temp.cursor() as cur:
                                            # Get run_id that has step_progress matching the steps we're about to mark complete
                                            cur.execute("""
                                                SELECT run_id, COUNT(*) as step_count
                                                FROM ar_step_progress
                                                WHERE progress_key = 'pipeline'
                                                  AND step_number = ANY(%s)
                                                GROUP BY run_id
                                                ORDER BY step_count DESC, run_id DESC
                                                LIMIT 1
                                            """, ([s for s in completed_steps],))
                                            row = cur.fetchone()
                                            if row:
                                                run_id = row[0]
                                            
                                            # If no match, get run_id with most step_progress entries (most likely the active run)
                                            if not run_id:
                                                cur.execute("""
                                                    SELECT run_id, COUNT(*) as step_count
                                                    FROM ar_step_progress
                                                    WHERE progress_key = 'pipeline'
                                                    GROUP BY run_id
                                                    ORDER BY step_count DESC, run_id DESC
                                                    LIMIT 1
                                                """)
                                                row = cur.fetchone()
                                                if row:
                                                    run_id = row[0]
                                            
                                            # Final fallback: get latest run from run_ledger if no step_progress exists
                                            if not run_id:
                                                cur.execute("""
                                                    SELECT run_id FROM run_ledger 
                                                    WHERE scraper_name = 'Argentina' 
                                                    ORDER BY started_at DESC 
                                                    LIMIT 1
                                                """)
                                                row = cur.fetchone()
                                                if row:
                                                    run_id = row[0]
                                        db_temp.close()
                                except Exception:
                                    pass
                            
                            if run_id:
                                # Ensure run_id is set in environment and file
                                os.environ["ARGENTINA_RUN_ID"] = run_id
                                output_dir = get_output_dir()
                                run_id_file = output_dir / '.current_run_id'
                                run_id_file.parent.mkdir(parents=True, exist_ok=True)
                                run_id_file.write_text(run_id, encoding='utf-8')
                                
                                with CountryDB("Argentina") as db:
                                    apply_argentina_schema(db)
                                    
                                    # Ensure run_id exists in run_ledger (required for foreign key constraint)
                                    from core.db.models import run_ledger_ensure_exists
                                    sql, params = run_ledger_ensure_exists(run_id, "Argentina", mode="resume")
                                    with db.cursor() as cur:
                                        cur.execute(sql, params)
                                    db.commit()
                                    
                                    # Update ar_step_progress table for unmarked steps
                                    reset_count = 0
                                    for step_num in unmarked_steps:
                                        # Find step name
                                        step_name = None
                                        for gui_idx, step in enumerate(steps):
                                            if step_index_map.get(gui_idx, gui_idx) == step_num:
                                                step_name = step['name']
                                                break
                                        
                                        if step_name:
                                            with db.cursor() as cur:
                                                # Reset step status to 'pending' in ar_step_progress
                                                cur.execute("""
                                                    UPDATE ar_step_progress
                                                    SET status = 'pending',
                                                        completed_at = NULL,
                                                        error_message = NULL
                                                    WHERE run_id = %s
                                                      AND step_number = %s
                                                      AND progress_key = 'pipeline'
                                                """, (run_id, step_num))
                                                if cur.rowcount > 0:
                                                    reset_count += 1
                                    
                                    # Reset step 3 (Selenium): Reset failed/in_progress products to pending
                                    if 3 in unmarked_steps:
                                        with db.cursor() as cur:
                                            cur.execute("""
                                                UPDATE ar_product_index
                                                SET status = 'pending'
                                                WHERE run_id = %s
                                                  AND total_records = 0
                                                  AND status IN ('failed', 'in_progress')
                                            """, (run_id,))
                                            reset_count_products = cur.rowcount
                                            if reset_count_products > 0:
                                                print(f"[CHECKPOINT] Reset {reset_count_products} products to 'pending' for step 3 retry")
                                    
                                    # Reset step 2 (Prepare URLs): Reset products missing URLs
                                    if 2 in unmarked_steps:
                                        with db.cursor() as cur:
                                            cur.execute("""
                                                UPDATE ar_product_index
                                                SET url = NULL,
                                                    status = 'pending'
                                                WHERE run_id = %s
                                                  AND (url IS NULL OR url = '')
                                            """, (run_id,))
                                            reset_count_products = cur.rowcount
                                            if reset_count_products > 0:
                                                print(f"[CHECKPOINT] Reset {reset_count_products} products for step 2 retry")
                                    
                                    db.commit()
                                    if reset_count > 0:
                                        print(f"[CHECKPOINT] Reset {reset_count} step(s) in ar_step_progress table to 'pending' for run_id={run_id}")
                            else:
                                print(f"[CHECKPOINT] Warning: No run_id found, cannot reset unmarked steps in database")
                        except Exception as db_error:
                            error_msg = f"Failed to reset database for unmarked steps: {db_error}"
                            print(f"[CHECKPOINT] ERROR: {error_msg}")
                            import traceback
                            traceback.print_exc()
                            # Show error to user
                            messagebox.showerror("Database Update Error", 
                                f"Failed to update database:\n{error_msg}\n\n"
                                f"Checkpoint file was updated, but database may be out of sync.\n"
                                f"Please check the console for details.")
                    
                    # Mark selected steps as complete (use actual step numbers)
                    for actual_step in selected_steps:
                        # Find step name by actual step number
                        step_name = None
                        for gui_idx, step in enumerate(steps):
                            if step_index_map.get(gui_idx, gui_idx) == actual_step:
                                step_name = step['name']
                                break
                        
                        if step_name:
                            cp.mark_step_complete(actual_step, step_name)
                    
                    # For Argentina: Update ar_step_progress table for marked steps
                    if scraper_name == "Argentina":
                        db_updated = False
                        try:
                            from core.db.connection import CountryDB
                            from scripts.Argentina.db.schema import apply_argentina_schema
                            from scripts.Argentina.config_loader import get_output_dir
                            import os
                            
                            # Use the preserved run_id
                            run_id = run_id_before_clear
                            
                            # If still no run_id, try to get from file or metadata
                            if not run_id:
                                output_dir = get_output_dir()
                                run_id_file = output_dir / '.current_run_id'
                                if run_id_file.exists():
                                    run_id = run_id_file.read_text(encoding='utf-8').strip()
                                else:
                                    # Try to get from checkpoint metadata (after clear, but we restored it)
                                    metadata = cp.get_metadata() or {}
                                    run_id = metadata.get('run_id')
                            
                            if not run_id:
                                # Last resort: try to match run_id with checkpoint's completed steps
                                # This ensures we use the run_id associated with the checkpoint, even if records were deleted
                                try:
                                    completed_steps = selected_steps  # Steps that will be marked complete
                                    if completed_steps:
                                        db_temp = CountryDB("Argentina")
                                        apply_argentina_schema(db_temp)
                                        with db_temp.cursor() as cur:
                                            # Get run_id that has step_progress matching the steps we're about to mark complete
                                            cur.execute("""
                                                SELECT run_id, COUNT(*) as step_count
                                                FROM ar_step_progress
                                                WHERE progress_key = 'pipeline'
                                                  AND step_number = ANY(%s)
                                                GROUP BY run_id
                                                ORDER BY step_count DESC, run_id DESC
                                                LIMIT 1
                                            """, ([s for s in completed_steps],))
                                            row = cur.fetchone()
                                            if row:
                                                run_id = row[0]
                                            
                                            # If no match, get run_id with most step_progress entries (most likely the active run)
                                            # BUT prioritize run_id with status='running' in run_ledger
                                            if not run_id:
                                                cur.execute("""
                                                    SELECT sp.run_id, COUNT(*) as step_count
                                                    FROM ar_step_progress sp
                                                    JOIN run_ledger rl ON sp.run_id = rl.run_id
                                                    WHERE sp.progress_key = 'pipeline'
                                                      AND rl.scraper_name = 'Argentina'
                                                      AND rl.status = 'running'
                                                    GROUP BY sp.run_id
                                                    ORDER BY step_count DESC, sp.run_id DESC
                                                    LIMIT 1
                                                """)
                                                row = cur.fetchone()
                                                if row:
                                                    run_id = row[0]
                                            
                                            # If still no match, get any run_id with step_progress (ignore run_ledger status)
                                            if not run_id:
                                                cur.execute("""
                                                    SELECT run_id, COUNT(*) as step_count
                                                    FROM ar_step_progress
                                                    WHERE progress_key = 'pipeline'
                                                    GROUP BY run_id
                                                    ORDER BY step_count DESC, run_id DESC
                                                    LIMIT 1
                                                """)
                                                row = cur.fetchone()
                                                if row:
                                                    run_id = row[0]
                                            
                                            # Final fallback: get latest run from run_ledger with status='running' if no step_progress exists
                                            if not run_id:
                                                cur.execute("""
                                                    SELECT run_id FROM run_ledger 
                                                    WHERE scraper_name = 'Argentina' 
                                                      AND status = 'running'
                                                    ORDER BY started_at DESC 
                                                    LIMIT 1
                                                """)
                                                row = cur.fetchone()
                                                if row:
                                                    run_id = row[0]
                                        db_temp.close()
                                except Exception:
                                    pass
                            
                            if run_id:
                                # Ensure run_id is set in environment and file
                                os.environ["ARGENTINA_RUN_ID"] = run_id
                                output_dir = get_output_dir()
                                run_id_file = output_dir / '.current_run_id'
                                run_id_file.parent.mkdir(parents=True, exist_ok=True)
                                run_id_file.write_text(run_id, encoding='utf-8')
                                
                                with CountryDB("Argentina") as db:
                                    apply_argentina_schema(db)
                                    
                                    # Ensure run_id exists in run_ledger (required for foreign key constraint)
                                    from core.db.models import run_ledger_ensure_exists
                                    sql, params = run_ledger_ensure_exists(run_id, "Argentina", mode="resume")
                                    with db.cursor() as cur:
                                        cur.execute(sql, params)
                                    db.commit()
                                    
                                    # Update ar_step_progress for marked steps
                                    updated_count = 0
                                    for actual_step in selected_steps:
                                        # Find step name
                                        step_name = None
                                        for gui_idx, step in enumerate(steps):
                                            if step_index_map.get(gui_idx, gui_idx) == actual_step:
                                                step_name = step['name']
                                                break
                                        
                                        if step_name:
                                            with db.cursor() as cur:
                                                # Mark step as completed in ar_step_progress
                                                cur.execute("""
                                                    INSERT INTO ar_step_progress
                                                        (run_id, step_number, step_name, progress_key, status, completed_at)
                                                    VALUES
                                                        (%s, %s, %s, 'pipeline', 'completed', CURRENT_TIMESTAMP)
                                                    ON CONFLICT (run_id, step_number, progress_key) DO UPDATE SET
                                                        step_name = EXCLUDED.step_name,
                                                        status = 'completed',
                                                        completed_at = CURRENT_TIMESTAMP,
                                                        error_message = NULL
                                                """, (run_id, actual_step, step_name))
                                                if cur.rowcount > 0:
                                                    updated_count += 1
                                    
                                    db.commit()
                                    db_updated = True
                                success_msg = f"[CHECKPOINT] Successfully updated {updated_count} step(s) in ar_step_progress table for run_id={run_id}"
                                print(success_msg)
                                # Show success message to user
                                if updated_count > 0:
                                    messagebox.showinfo("Checkpoint Updated", 
                                        f"Checkpoint and database updated successfully!\n\n"
                                        f"Updated {updated_count} step(s) in database.\n"
                                        f"Run ID: {run_id}")
                            else:
                                warning_msg = "[CHECKPOINT] Warning: No run_id found, cannot update ar_step_progress table"
                                print(warning_msg)
                                messagebox.showwarning("Database Update Skipped", 
                                    f"Checkpoint file was updated, but database was not updated.\n\n"
                                    f"Reason: No run_id found.\n\n"
                                    f"Database may be out of sync with checkpoint.")
                        except Exception as db_error:
                            error_msg = f"Failed to update ar_step_progress: {db_error}"
                            print(f"[CHECKPOINT] ERROR: {error_msg}")
                            import traceback
                            traceback.print_exc()
                            # Show error to user
                            messagebox.showerror("Database Update Error", 
                                f"Failed to update database:\n{error_msg}\n\n"
                                f"Checkpoint file was updated, but database may be out of sync.\n"
                                f"Please check the console for details.")
                        finally:
                            if not db_updated and scraper_name == "Argentina":
                                print(f"[CHECKPOINT] Warning: Database update was skipped or failed for Argentina")
                    
                    # Update checkpoint status in main window
                    self.update_checkpoint_status()
                    
                    # Close dialog - success message already shown above if database was updated
                    dialog.destroy()
                except Exception as e:
                    messagebox.showerror("Error", f"Failed to update checkpoint:\n{e}")
            
            def mark_all_up_to():
                """Mark all steps up to a selected step as complete (sequential only)"""
                nonlocal _updating_programmatically
                try:
                    step_str = simpledialog.askstring("Mark Steps", 
                        f"Enter step number (0-{len(steps)-1}) to mark all steps up to and including it as complete:\n\n"
                        f"Note: Steps must be marked sequentially (cannot skip steps).",
                        parent=dialog)
                    if step_str is None:
                        return
                    
                    last_step = int(step_str)
                    if last_step < 0 or last_step >= len(steps):
                        messagebox.showerror("Error", f"Step number must be between 0 and {len(steps)-1}")
                        return
                    
                    # Disable validation during programmatic update
                    _updating_programmatically = True
                    try:
                        # First, uncheck all steps to reset state
                        for step_num in step_vars:
                            step_vars[step_num].set(False)
                        
                        # Then check all steps up to last_step sequentially
                        for step_num in range(last_step + 1):
                            if step_num in step_vars:
                                step_vars[step_num].set(True)
                    finally:
                        # Re-enable validation
                        _updating_programmatically = False
                except ValueError:
                    messagebox.showerror("Error", "Invalid step number")
                except Exception as e:
                    messagebox.showerror("Error", f"Error: {e}")
                finally:
                    # Ensure flag is reset even on error
                    _updating_programmatically = False
            
            ttk.Button(button_frame, text="Mark All Up To...", 
                     command=mark_all_up_to).pack(side=tk.LEFT, padx=5)
            ttk.Button(button_frame, text="Apply", 
                     command=apply_changes).pack(side=tk.RIGHT, padx=5)
            ttk.Button(button_frame, text="Cancel", 
                     command=dialog.destroy).pack(side=tk.RIGHT, padx=5)
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to open checkpoint manager:\n{e}")
    
    def clear_run_lock(self):
        """Clear run lock file for the currently selected scraper only"""
        scraper_name = self.scraper_var.get()
        if not scraper_name:
            messagebox.showwarning("Warning", "Select a scraper first")
            return

        # Find lock file for the selected scraper only - check both new and old locations
        lock_file = None
        old_lock_file = None
        try:
            from core.config_manager import ConfigManager
            # Migrated: pm = get_path_manager()
            lock_file = pm.get_lock_file(scraper_name)
            # Also check old location as fallback
            old_lock_file = self.repo_root / f".{scraper_name}_run.lock"
        except Exception:
            # Fallback to old location
            old_lock_file = self.repo_root / f".{scraper_name}_run.lock"
        
        # Use the lock file that exists, or prefer new location
        if lock_file and not lock_file.exists() and old_lock_file and old_lock_file.exists():
            lock_file = old_lock_file
        elif not lock_file:
            lock_file = old_lock_file

        if not lock_file or not lock_file.exists():
            messagebox.showinfo("Information", f"No lock file found for {scraper_name}. Scraper is unlocked.")
            self.update_status(f"No lock file to clear for {scraper_name}")
            return

        # Show confirmation for this scraper only
        confirm_msg = f"Clear run lock for {scraper_name}?\n\nLock file: {lock_file.name}\n\nThis will allow a new run to start immediately."

        if not messagebox.askyesno("Confirm Clear Run Lock", confirm_msg):
            return
        
        # Delete lock file for selected scraper
        deleted = []
        failed = []
        
        try:
            # On Windows, check if the process owning the lock is still running
            if sys.platform == "win32":
                # Read lock file to get PID
                process_still_running = False
                try:
                    with open(lock_file, 'r', encoding='utf-8') as f:
                        lock_content = f.read().strip().split('\n')
                        if lock_content and lock_content[0].isdigit():
                            lock_pid = int(lock_content[0])
                            
                            # Check if process is still running
                            result = subprocess.run(
                                ['tasklist', '/FI', f'PID eq {lock_pid}', '/FO', 'CSV'],
                                capture_output=True,
                                text=True,
                                timeout=2
                            )
                            
                            # Check if PID was found in tasklist output
                            # If PID is found, verify it's actually a Python/workflow process
                            if str(lock_pid) in result.stdout:
                                # Check if it's a Python process (workflow runner)
                                # tasklist CSV format: "Image Name","PID","Session Name","Session#","Mem Usage"
                                lines = result.stdout.strip().split('\n')
                                for line in lines:
                                    if f'"{lock_pid}"' in line or f',{lock_pid},' in line:
                                        # Check if it's python.exe or pythonw.exe
                                        if 'python' in line.lower() or 'pythonw' in line.lower():
                                            process_still_running = True
                                            break
                                        # If it's not a Python process, might be stale lock
                                        break
                            
                            # If PID not found, process has completed - allow clearing
                            if not process_still_running:
                                # Process has completed, safe to clear lock
                                pass
                            else:
                                # Process is still running, warn user but allow force clear
                                response = messagebox.askyesno(
                                    "Process Still Running",
                                    f"Process {lock_pid} appears to be still running for {scraper_name}.\n\n"
                                    f"Do you want to force clear the lock anyway?\n\n"
                                    f"Warning: This may cause issues if the process is actually running."
                                )
                                if not response:
                                    self.update_status(f"Lock clear cancelled for {scraper_name}")
                                    return
                except (ValueError, IndexError, IOError, subprocess.TimeoutExpired) as e:
                    # Lock file format is invalid or can't read, allow clearing
                    # This might be a stale lock file
                    pass
                
                # Try to delete with retry (file handle might still be closing)
                max_retries = 5
                deleted_successfully = False
                
                for attempt in range(max_retries):
                    try:
                        # On Windows, try to close any open handles first
                        if sys.platform == "win32" and lock_file.exists():
                            # Try to open the file in exclusive mode to release any handles
                            try:
                                with open(lock_file, 'r+', encoding='utf-8') as f:
                                    f.close()
                            except:
                                pass  # Ignore if we can't open it
                        
                        lock_file.unlink()
                        deleted.append(scraper_name)
                        deleted_successfully = True
                        break
                    except (PermissionError, OSError, FileNotFoundError) as e:
                        # File might still be locked by a closing process, or already deleted
                        if isinstance(e, FileNotFoundError):
                            # File was already deleted, consider it successful
                            deleted.append(scraper_name)
                            deleted_successfully = True
                            break
                        
                        if attempt < max_retries - 1:
                            # Increasing wait time with each retry: 0.5s, 1s, 1.5s, 2s, 2.5s
                            wait_time = 0.5 * (attempt + 1)
                            time.sleep(wait_time)
                        else:
                            # Final attempt failed, try renaming as fallback
                            try:
                                # Try renaming the file (sometimes works when delete doesn't)
                                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                                backup_name = lock_file.with_name(f"{lock_file.stem}_{timestamp}.lock.old")
                                if backup_name.exists():
                                    backup_name.unlink()
                                lock_file.rename(backup_name)
                                # Rename succeeded - file is effectively cleared
                                deleted.append(scraper_name)
                                deleted_successfully = True
                                # Try to delete the renamed file in background (best effort)
                                try:
                                    time.sleep(1)
                                    backup_name.unlink()
                                except:
                                    pass  # Ignore if we can't delete the backup
                                break
                            except Exception as rename_error:
                                # Rename also failed, report original error
                                failed.append((scraper_name, f"{str(e)} (tried {max_retries} times, rename also failed: {rename_error})"))
                    except Exception as e:
                        failed.append((scraper_name, str(e)))
                        break
            else:
                # Unix-like: straightforward deletion
                try:
                    lock_file.unlink()
                    deleted.append(scraper_name)
                except FileNotFoundError:
                    # File was already deleted, consider it successful
                    deleted.append(scraper_name)
                except Exception as e:
                    failed.append((scraper_name, str(e)))
        except Exception as e:
            failed.append((scraper_name, str(e)))
        
        # Show results
        if failed:
            error_msg = f"Failed to delete lock for {scraper_name}:\n"
            error_msg += "\n".join([f"  - {error}" for _, error in failed])
            messagebox.showerror("Error", error_msg)
        else:
            messagebox.showinfo("Information", f"Run lock cleared for {scraper_name}")
            self.update_status(f"Cleared lock file for {scraper_name}")
            
            # Always refresh button state after clearing lock
            self.refresh_run_button_state()
    
    
    def open_docs_folder(self):
        """Open documentation folder"""
        scraper_name = self.scraper_var.get()
        if not scraper_name:
            # Open repo root
            os.startfile(str(self.repo_root))
            return
        
        scraper_info = self.scrapers[scraper_name]
        # All docs are now in root doc/ folder, not scraper-specific folders
        docs_path = self.repo_root / "doc"
        
        if docs_path.exists():
            # Open the doc folder in file explorer
            if sys.platform == "win32":
                os.startfile(str(docs_path))
            else:
                # Unix-like systems
                import subprocess
                subprocess.run(["xdg-open", str(docs_path)])
        else:
            messagebox.showwarning("Warning", f"Documentation folder not found:\n{docs_path}")
    
    def refresh_logs(self):
        """Refresh execution logs"""
        # Logs are updated in real-time during execution
        self.update_status("Logs refreshed")
    
    def clear_logs(self, scraper_name=None, silent=False, clear_storage=True):
        """Clear log viewer"""
        target_scraper = scraper_name or self.scraper_var.get()
        if clear_storage and target_scraper:
            if target_scraper in self.scraper_logs:
                self.scraper_logs[target_scraper] = ""
            self._last_completed_logs.pop(target_scraper, None)
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state=tk.DISABLED)
        if not silent:
            self.update_status("Logs cleared")

    def _resolve_logs_dir(self) -> Path:
        """Return the root logs directory (Documents/ScraperPlatform/logs or repo/logs)."""
        try:
            from core.config_manager import ConfigManager
            # Migrated: pm = get_path_manager()
            logs_dir = Path(pm.get_logs_dir())
        except Exception:
            logs_dir = self.repo_root / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        return logs_dir

    def _get_scraper_logs_dir(self, scraper_name: Optional[str]) -> Path:
        """Return the per-scraper log directory inside the root logs folder."""
        base_dir = self._resolve_logs_dir()
        target = base_dir / (scraper_name or "general")
        target.mkdir(parents=True, exist_ok=True)
        return target

    def _get_scraper_archive_dir(self, scraper_name: Optional[str]) -> Path:
        """Return the archive subfolder for a scraper's logs."""
        archive_dir = self._get_scraper_logs_dir(scraper_name) / "archive"
        archive_dir.mkdir(parents=True, exist_ok=True)
        return archive_dir

    def _get_scraper_auto_dir(self, scraper_name: Optional[str]) -> Path:
        """Return the automatic save subfolder for a scraper's logs."""
        auto_dir = self._get_scraper_logs_dir(scraper_name) / "auto"
        auto_dir.mkdir(parents=True, exist_ok=True)
        return auto_dir

    def _ensure_unique_path(self, candidate: Path) -> Path:
        """Ensure the destination path does not already exist by appending a counter."""
        counter = 1
        target = candidate
        while target.exists():
            target = candidate.with_name(f"{candidate.stem}_{counter}{candidate.suffix}")
            counter += 1
        return target

    def _move_logs_to_archive_dir(self, logs_dir: Path, archive_dir: Path) -> list[Path]:
        """Move any log files outside the archive folder into archive."""
        moved_paths = []
        for log_path in sorted(logs_dir.rglob("*.log")):
            if archive_dir in log_path.parents:
                continue
            rel = log_path.relative_to(logs_dir)
            dest = archive_dir / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest = self._ensure_unique_path(dest)
            try:
                log_path.replace(dest)
            except Exception:
                continue
            moved_paths.append(dest)
        return moved_paths

    def archive_log_without_clearing(self, scraper_name: Optional[str] = None, footer: Optional[str] = None):
        """Save the current log to disk without clearing the viewer (used when stopping pipeline)."""
        target_scraper = scraper_name or self.scraper_var.get()
        if not target_scraper:
            return
        content = self.scraper_logs.get(target_scraper, "")
        if not content.strip():
            # Get from display if storage is empty
            if target_scraper == self.scraper_var.get():
                content = self.log_text.get(1.0, tk.END)
            if not content.strip():
                content = self._last_completed_logs.get(target_scraper, "")
        
        logs_dir = self._get_scraper_logs_dir(target_scraper)
        archive_dir = self._get_scraper_archive_dir(target_scraper)
        moved_paths = self._move_logs_to_archive_dir(logs_dir, archive_dir)

        if footer:
            content += footer

        if moved_paths:
            desc = ", ".join(p.name for p in moved_paths)
            self.update_status(f"Archived log file(s): {desc}")
        else:
            if content.strip():
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                archive_path = archive_dir / f"{target_scraper}_stopped_{timestamp}.log"
                archive_path = self._ensure_unique_path(archive_path)
                try:
                    with open(archive_path, "w", encoding="utf-8") as f:
                        f.write(content)
                    self.update_status(f"Archived stopped log: {archive_path.name}")
                except Exception as exc:
                    self.update_status(f"Failed to archive log: {exc}")

        # Save to last_completed_logs for potential viewing later
        if content.strip():
            self._last_completed_logs[target_scraper] = content
        # DON'T clear logs - user wants to see them after stop
    
    def archive_and_clear_log(self, scraper_name: Optional[str] = None, footer: Optional[str] = None):
        """Save the current log to disk (stopped run) and clear the viewer."""
        target_scraper = scraper_name or self.scraper_var.get()
        if not target_scraper:
            return
        content = self.scraper_logs.get(target_scraper, "")
        if not content.strip():
            content = self._last_completed_logs.get(target_scraper, "")
        logs_dir = self._get_scraper_logs_dir(target_scraper)
        archive_dir = self._get_scraper_archive_dir(target_scraper)
        moved_paths = self._move_logs_to_archive_dir(logs_dir, archive_dir)

        if footer:
            content += footer

        if moved_paths:
            desc = ", ".join(p.name for p in moved_paths)
            self.update_status(f"Archived log file(s): {desc}")
        else:
            if not content.strip():
                self.clear_logs(target_scraper, silent=True)
                return
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_path = archive_dir / f"{target_scraper}_archive_{timestamp}.log"
            archive_path = self._ensure_unique_path(archive_path)
            try:
                with open(archive_path, "w", encoding="utf-8") as f:
                    f.write(content)
                self.update_status(f"Archived log: {archive_path.name}")
                moved_paths.append(archive_path)
            except Exception as exc:
                self.update_status(f"Failed to archive log: {exc}")

        self._last_completed_logs.pop(target_scraper, None)
        self.clear_logs(target_scraper, silent=True)

    def archive_current_log(self):
        """Archive the current scraper's log when toolbar button is clicked."""
        scraper_name = self.scraper_var.get()
        if not scraper_name:
            messagebox.showwarning("Warning", "Select a scraper first to archive its log.")
            return
        self.archive_and_clear_log(scraper_name)

    def copy_logs_to_clipboard(self):
        """Copy current log content to clipboard"""
        raw_content = self.log_text.get(1.0, tk.END)
        scraper_name = self.scraper_var.get()
        if raw_content.strip():
            content = raw_content
        else:
            content = ""
            if scraper_name and scraper_name in self.scraper_logs:
                content = self.scraper_logs.get(scraper_name, "")
            if not content.strip() and scraper_name in self._last_completed_logs:
                content = self._last_completed_logs.get(scraper_name, "")
        if not content.strip():
            messagebox.showwarning("Warning", "No log content to copy")
            return
        
        try:
            self.root.clipboard_clear()
            self.root.clipboard_append(content)
            self.update_status("Log content copied to clipboard")
            messagebox.showinfo("Information", "Log content copied to clipboard")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to copy to clipboard:\n{str(e)}")
    
    def save_log(self):
        """Save current log to file in logs directory"""
        raw_content = self.log_text.get(1.0, tk.END)
        scraper_name = self.scraper_var.get()
        if raw_content.strip():
            content = raw_content
        else:
            content = ""
            if scraper_name and scraper_name in self.scraper_logs:
                content = self.scraper_logs.get(scraper_name, "")
            if not content.strip() and scraper_name in self._last_completed_logs:
                content = self._last_completed_logs.get(scraper_name, "")

        if not content or not content.strip():
            messagebox.showwarning("Warning", "No log content to save")
            return
        
        try:
            log_dir = self._get_scraper_logs_dir(scraper_name)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_filename = log_dir / f"manual_{timestamp}.log"
            
            # Save log file
            with open(log_filename, "w", encoding="utf-8") as f:
                f.write(content)
            
            messagebox.showinfo("Information", f"Log saved to:\n{log_filename}")
            self.update_status(f"Log saved to: {log_filename.name}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save log:\n{str(e)}")

    def open_console_in_cursor(self):
        """Open console output in Cursor; open last error location when available."""
        content = self.log_text.get(1.0, tk.END)
        if not content.strip():
            messagebox.showwarning("Warning", "No log content to open")
            return

        cursor_cmd = self._get_cursor_command()
        if not cursor_cmd:
            messagebox.showwarning(
                "Warning",
                "Cursor CLI not found. Install Cursor or add `cursor` to PATH."
            )
            return

        error_location = self._extract_error_location(content)
        if error_location:
            file_path, line_no, col_no = error_location
            if not file_path.is_absolute():
                candidate = (self.repo_root / file_path).resolve()
                if candidate.exists():
                    file_path = candidate
            if file_path.exists():
                self._open_cursor_at(cursor_cmd, file_path, line_no, col_no)
                return

        log_file = self._write_console_log_file(content)
        if log_file:
            self._open_cursor_at(cursor_cmd, log_file, 1, 1)

    def _get_cursor_command(self):
        env_path = os.environ.get("CURSOR_PATH", "").strip()
        if env_path:
            candidate = Path(env_path)
            if candidate.exists():
                return str(candidate)

        for cmd in ("cursor", "Cursor"):
            found = shutil.which(cmd)
            if found:
                return found
        return None

    def _open_cursor_at(self, cursor_cmd, file_path, line_no=1, col_no=1):
        try:
            target = f"{file_path}:{line_no}:{col_no}"
            subprocess.Popen([cursor_cmd, "--new-window", "--goto", target])
        except Exception as e:
            messagebox.showerror("Error", f"Failed to open Cursor:\n{str(e)}")

    def _extract_error_location(self, log_content):
        import re

        patterns = [
            re.compile(r'File "([^"]+)", line (\d+)'),
            re.compile(r"File '([^']+)', line (\d+)"),
            re.compile(r'([A-Za-z]:\\[^:\n]+?\.[a-zA-Z0-9]+):(\d+)(?::(\d+))?'),
            re.compile(r'(/[^:\n]+?\.[a-zA-Z0-9]+):(\d+)(?::(\d+))?'),
            re.compile(r'\bat\s+(.*?\.[a-zA-Z0-9]+):(\d+):(\d+)')
        ]

        for line in reversed(log_content.splitlines()):
            for pattern in patterns:
                match = pattern.search(line)
                if match:
                    file_path = Path(match.group(1))
                    line_no = int(match.group(2))
                    col_no = 1
                    if match.lastindex and match.lastindex >= 3 and match.group(3):
                        col_no = int(match.group(3))
                    return file_path, line_no, col_no
        return None

    def _write_console_log_file(self, content):
        try:
            try:
                from core.config_manager import ConfigManager
                # Migrated: pm = get_path_manager()
                logs_dir = pm.get_logs_dir()
            except Exception:
                logs_dir = self.repo_root / "logs"

            logs_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_filename = logs_dir / f"console_log_{timestamp}.txt"

            with open(log_filename, "w", encoding="utf-8") as f:
                f.write(content)
            return log_filename
        except Exception as e:
            messagebox.showerror("Error", f"Failed to write console log:\n{str(e)}")
            return None
    
    def save_log_automatically(self, scraper_name: str, log_content: Optional[str] = None):
        """Automatically save log to logs/<scraper>/auto after successful run"""
        try:
            content = log_content if log_content is not None else self.scraper_logs.get(scraper_name, "")
            if not content or not content.strip():
                return  # No log content to save

            auto_dir = self._get_scraper_auto_dir(scraper_name)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_filename = auto_dir / f"{scraper_name}_run_{timestamp}.log"
            with open(log_filename, "w", encoding="utf-8") as f:
                f.write(content)

            self.update_status(f"{scraper_name} execution completed - Log saved to: {log_filename.name}")
        except Exception as e:
            print(f"Warning: Failed to automatically save log for {scraper_name}: {e}")
    
    def refresh_output_files(self):
        """Refresh output files list for the currently selected scraper"""
        if not hasattr(self, 'output_listbox') or not self.output_listbox:
            return
        
        self.output_listbox.delete(0, tk.END)
        
        # Get the selected scraper to ensure we're showing files for the right scraper
        scraper_name = self.scraper_var.get()
        if not scraper_name:
            return
        
        output_path = self.output_path_var.get()
        if not output_path:
            return
        
        output_dir = Path(output_path)
        if not output_dir.exists():
            return
        
        # Verify the output directory is correct for the selected scraper
        # If path doesn't match, update to correct output directory
        try:
            from core.config_manager import ConfigManager
            # Migrated: pm = get_path_manager()
            correct_output_dir = ConfigManager.get_output_dir(scraper_name)
            if output_dir != correct_output_dir:
                output_dir = correct_output_dir
                self.output_path_var.set(str(correct_output_dir))
        except Exception:
            # Fallback: check if it's the scraper's output directory
            scraper_info = self.scrapers.get(scraper_name, {})
            if scraper_info:
                scraper_output = scraper_info["path"] / "output"
                if output_dir != scraper_output:
                    output_dir = scraper_output
                    self.output_path_var.set(str(scraper_output))
        
        # List all files recursively from output directory
        for file_path in sorted(output_dir.rglob("*")):
            if file_path.is_file():
                rel_path = file_path.relative_to(output_dir)
                self.output_listbox.insert(tk.END, str(rel_path))
    
    def open_output_folder(self):
        """Open output folder"""
        output_path = self.output_path_var.get()
        if output_path:
            output_dir = Path(output_path)
            if output_dir.exists():
                os.startfile(str(output_dir))
            else:
                messagebox.showwarning("Warning", f"Output folder not found:\n{output_dir}")
        else:
            messagebox.showwarning("Warning", "No output path specified")
    
    def open_output_file(self, event=None):
        """Open selected output file"""
        if not hasattr(self, 'output_listbox') or not self.output_listbox:
            return
        
        selection = self.output_listbox.curselection()
        if not selection:
            return
        
        output_path = self.output_path_var.get()
        if not output_path:
            return
        
        output_dir = Path(output_path)
        file_name = self.output_listbox.get(selection[0])
        file_path = output_dir / file_name
        
        if file_path.exists():
            # Update file info
            try:
                size = file_path.stat().st_size
                modified = datetime.fromtimestamp(file_path.stat().st_mtime)
                info = f"File: {file_path.name}\n"
                info += f"Size: {size:,} bytes\n"
                info += f"Modified: {modified.strftime('%Y-%m-%d %H:%M:%S')}"
                
                self.file_info_text.config(state=tk.NORMAL)
                self.file_info_text.delete(1.0, tk.END)
                self.file_info_text.insert(1.0, info)
                self.file_info_text.config(state=tk.DISABLED)
                
                # Open file
                os.startfile(str(file_path))
            except Exception as e:
                messagebox.showerror("Error", f"Failed to open file:\n{str(e)}")
        else:
            messagebox.showwarning("Warning", f"File not found:\n{file_path}")
    
    def refresh_final_output_files(self):
        """Refresh final output files list for the currently selected scraper"""
        self.final_output_listbox.delete(0, tk.END)
        
        # Get the selected scraper to ensure we're showing files for the right scraper
        scraper_name = self.scraper_var.get()
        if not scraper_name:
            return
        
        final_output_path = self.final_output_path_var.get()
        if not final_output_path:
            return
        
        output_dir = Path(final_output_path)
        if not output_dir.exists():
            self.final_output_info_text.config(state=tk.NORMAL)
            self.final_output_info_text.delete(1.0, tk.END)
            self.final_output_info_text.insert(1.0, f"Directory does not exist: {output_dir}")
            self.final_output_info_text.config(state=tk.DISABLED)
            return
        
        # List only final report files (CSV and XLSX) for the selected scraper
        # Filter by scraper-specific naming patterns
        scraper_patterns = {
            "canada_quebec": ["canadaquebecreport"],
            "CanadaOntario": ["canadaontarioreport"],
            "Malaysia": ["malaysia"],
            "Argentina": ["alfabeta_report"],
            "NorthMacedonia": ["north_macedonia_drug_register"],
            "Russia": ["russia_ved_report", "russia_excluded_report"],
            "tender_chile": ["final_tender_data"],
            "India": ["details_combined"],
            "Taiwan": ["taiwan_drug_code_details"]
        }

        patterns = scraper_patterns.get(scraper_name, [])
        files = []
        for file_path in sorted(output_dir.iterdir()):
            if file_path.is_file() and file_path.suffix.lower() in ['.csv', '.xlsx']:
                # Show all files when no pattern is defined for the scraper.
                file_lower = file_path.name.lower()
                if not patterns or any(pattern in file_lower for pattern in patterns):
                    files.append((file_path.name, file_path))
                    self.final_output_listbox.insert(tk.END, file_path.name)
        
        # Update info with general information
        self.final_output_info_text.config(state=tk.NORMAL)
        self.final_output_info_text.delete(1.0, tk.END)
        info = "Final Output Information\n"
        info += "=" * 50 + "\n\n"
        info += f"Directory: {output_dir}\n"
        info += f"Files Found: {len(files)}\n\n"
        info += "Select a file from the list above to view its data summary.\n"
        info += "The summary will show:\n"
        info += "  • Total rows and columns\n"
        info += "  • Column names and data types\n"
        info += "  • Basic statistics for numeric columns\n"
        self.final_output_info_text.insert(1.0, info)
        self.final_output_info_text.config(state=tk.DISABLED)
    
    def open_final_output_folder(self):
        """Open final output folder"""
        final_output_path = self.final_output_path_var.get()
        if final_output_path:
            output_dir = Path(final_output_path)
            if output_dir.exists():
                os.startfile(str(output_dir))
            else:
                messagebox.showwarning("Warning", f"Output folder not found:\n{output_dir}")
        else:
            messagebox.showwarning("Warning", "No output path specified")
    
    def open_final_output_file(self, event=None):
        """Open selected final output file"""
        selection = self.final_output_listbox.curselection()
        if not selection:
            return
        
        final_output_path = self.final_output_path_var.get()
        if not final_output_path:
            return
        
        output_dir = Path(final_output_path)
        file_name = self.final_output_listbox.get(selection[0])
        # Final output files are directly in output directory (not in subdirectories)
        file_path = output_dir / file_name
        
        if file_path.exists():
            try:
                # Show summary first
                self.show_file_data_summary(file_path)
                
                # Open file
                os.startfile(str(file_path))
            except Exception as e:
                messagebox.showerror("Error", f"Failed to open file:\n{str(e)}")
        else:
            messagebox.showwarning("Warning", f"File not found:\n{file_path}")
    
    def on_final_output_file_selected(self, event=None):
        """Show data summary when a file is selected (single click)"""
        selection = self.final_output_listbox.curselection()
        if not selection:
            return
        
        final_output_path = self.final_output_path_var.get()
        if not final_output_path:
            return
        
        output_dir = Path(final_output_path)
        file_name = self.final_output_listbox.get(selection[0])
        file_path = output_dir / file_name
        
        if file_path.exists():
            self.show_file_data_summary(file_path)
    
    def show_file_data_summary(self, file_path: Path):
        """Show data summary for a file"""
        try:
            modified = datetime.fromtimestamp(file_path.stat().st_mtime)
            info = f"File: {file_path.name}\n"
            info += f"Path: {file_path}\n"
            info += f"Last Modified: {modified.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            
            # Read file and show data summary
            try:
                import pandas as pd
                
                # Read file based on extension
                if file_path.suffix.lower() == '.csv':
                    df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip', low_memory=False)
                elif file_path.suffix.lower() in ['.xlsx', '.xls']:
                    df = pd.read_excel(file_path, engine='openpyxl')
                else:
                    df = None
                
                if df is not None and not df.empty:
                    info += "Data Summary\n"
                    info += "=" * 50 + "\n\n"
                    info += f"Total Rows: {len(df):,}\n"
                    info += f"Total Columns: {len(df.columns)}\n\n"
                    info += "Columns:\n"
                    for i, col in enumerate(df.columns, 1):
                        # Show column name and data type
                        dtype = str(df[col].dtype)
                        non_null = df[col].notna().sum()
                        info += f"  {i}. {col} ({dtype}) - {non_null:,} non-null values\n"
                    
                    # Show some basic statistics for numeric columns
                    numeric_cols = df.select_dtypes(include=['number']).columns
                    if len(numeric_cols) > 0:
                        info += "\nNumeric Columns Summary:\n"
                        for col in numeric_cols[:5]:  # Limit to first 5 numeric columns
                            info += f"  • {col}: min={df[col].min():.2f}, max={df[col].max():.2f}, mean={df[col].mean():.2f}\n"
                else:
                    info += "Unable to read file or file is empty.\n"
                    
            except ImportError:
                info += "Note: Install pandas and openpyxl to view data summary.\n"
            except Exception as e:
                info += f"Error reading file: {str(e)}\n"
            
            info += "\nDouble-click to open the file in your default application."
            
            self.final_output_info_text.config(state=tk.NORMAL)
            self.final_output_info_text.delete(1.0, tk.END)
            self.final_output_info_text.insert(1.0, info)
            self.final_output_info_text.config(state=tk.DISABLED)
        except Exception as e:
            self.final_output_info_text.config(state=tk.NORMAL)
            self.final_output_info_text.delete(1.0, tk.END)
            self.final_output_info_text.insert(1.0, f"Error reading file: {str(e)}")
            self.final_output_info_text.config(state=tk.DISABLED)
    
    def search_final_output(self):
        """Search for files in final output directory"""
        search_term = simpledialog.askstring("Search", "Enter search term:")
        if not search_term:
            return
        
        self.final_output_listbox.delete(0, tk.END)
        
        final_output_path = self.final_output_path_var.get()
        if not final_output_path:
            return
        
        output_dir = Path(final_output_path)
        if not output_dir.exists():
            return
        
        # Search only final report files (CSV/XLSX) for the selected scraper
        scraper_name = self.scraper_var.get()
        scraper_patterns = {
            "canada_quebec": ["canadaquebecreport"],
            "CanadaOntario": ["canadaontarioreport"],
            "Malaysia": ["malaysia"],
            "Argentina": ["alfabeta_report"],
            "NorthMacedonia": ["north_macedonia_drug_register"],
            "Russia": ["russia_ved_report", "russia_excluded_report"],
            "tender_chile": ["final_tender_data"],
            "India": ["details_combined"],
            "Taiwan": ["taiwan_drug_code_details"]
        }
        patterns = scraper_patterns.get(scraper_name, [])

        matches = []
        for file_path in sorted(output_dir.iterdir()):
            if file_path.is_file() and file_path.suffix.lower() in ['.csv', '.xlsx']:
                file_lower = file_path.name.lower()
                # Show all files when no pattern is defined for the scraper.
                if not patterns or any(pattern in file_lower for pattern in patterns):
                    if search_term.lower() in file_path.name.lower():
                        matches.append(file_path.name)
                        self.final_output_listbox.insert(tk.END, file_path.name)
        
        self.update_status(f"Found {len(matches)} files matching '{search_term}'")
    
    def push_to_database(self):
        """Push selected final output file to database"""
        # Get selected file
        selection = self.final_output_listbox.curselection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a file from the list to push to database.")
            return
        
        # Get file path
        final_output_path = self.final_output_path_var.get()
        if not final_output_path:
            messagebox.showwarning("Warning", "No output path specified.")
            return
        
        output_dir = Path(final_output_path)
        file_name = self.final_output_listbox.get(selection[0])
        file_path = output_dir / file_name
        
        if not file_path.exists():
            messagebox.showerror("Error", f"File not found:\n{file_path}")
            return
        
        # Get scraper name
        scraper_name = self.scraper_var.get()
        if not scraper_name:
            messagebox.showwarning("Warning", "No scraper selected.")
            return
        
        # Confirm action
        if not messagebox.askyesno("Confirm", f"Push '{file_name}' to database?\n\nThis will insert all rows from the file into the database table."):
            return
        
        # Push to database in a separate thread to avoid blocking UI
        def push_thread():
            try:
                self.update_status(f"Pushing {file_name} to database...")
                
                # Load database config from scraper's config_loader
                scraper_scripts_dir = self.repo_root / "scripts" / scraper_name
                config_loader_path = scraper_scripts_dir / "config_loader.py"
                
                if not config_loader_path.exists():
                    raise ImportError(f"config_loader.py not found for {scraper_name}")
                
                # Add scraper scripts directory to path
                import sys
                import importlib.util
                if str(scraper_scripts_dir) not in sys.path:
                    sys.path.insert(0, str(scraper_scripts_dir))
                
                # Import config_loader dynamically
                spec = importlib.util.spec_from_file_location("config_loader", config_loader_path)
                config_loader = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(config_loader)
                
                # Get database config
                DB_ENABLED = getattr(config_loader, 'DB_ENABLED', False)
                if not DB_ENABLED:
                    raise ValueError(f"Database is not enabled for {scraper_name}. Set DB_ENABLED=true in config.")
                
                DB_HOST = getattr(config_loader, 'DB_HOST', 'localhost')
                DB_PORT = getattr(config_loader, 'DB_PORT', 5432)
                DB_NAME = getattr(config_loader, 'DB_NAME', 'scraper_db')
                DB_USER = getattr(config_loader, 'DB_USER', 'postgres')
                DB_PASSWORD = getattr(config_loader, 'DB_PASSWORD', '')
                SCRAPER_ID_DB = getattr(config_loader, 'SCRAPER_ID_DB', scraper_name.lower().replace(' ', '_'))
                
                # Determine table name based on scraper
                table_name_map = {
                    "canada_quebec": "canada_quebec_reports",
                    "Malaysia": "malaysia_reports",
                    "Argentina": "argentina_reports",
                    "Taiwan": "taiwan_reports"
                }
                table_name = table_name_map.get(scraper_name, f"{scraper_name.lower().replace(' ', '_')}_reports")
                
                # Read CSV file
                import pandas as pd
                if file_path.suffix.lower() == '.csv':
                    df = pd.read_csv(file_path, encoding='utf-8-sig', on_bad_lines='skip', low_memory=False)
                elif file_path.suffix.lower() in ['.xlsx', '.xls']:
                    df = pd.read_excel(file_path, engine='openpyxl')
                else:
                    raise ValueError(f"Unsupported file format: {file_path.suffix}")
                
                if df.empty:
                    raise ValueError("File is empty or could not be read.")
                
                # Connect to database
                try:
                    import psycopg2
                    from psycopg2.extras import execute_values
                    from psycopg2 import sql
                except ImportError:
                    raise ImportError("psycopg2 is required for database operations. Install with: pip install psycopg2-binary")
                
                # First, try to connect to PostgreSQL server to check if database exists
                try:
                    # Connect to postgres database to check if target database exists
                    admin_conn = psycopg2.connect(
                        host=DB_HOST,
                        port=DB_PORT,
                        database='postgres',  # Connect to default postgres database
                        user=DB_USER,
                        password=DB_PASSWORD
                    )
                    admin_cur = admin_conn.cursor()
                    admin_cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
                    db_exists = admin_cur.fetchone() is not None
                    admin_cur.close()
                    admin_conn.close()
                    
                    if not db_exists:
                        raise ValueError(f"Database '{DB_NAME}' does not exist. Please create it first.")
                except psycopg2.OperationalError as e:
                    # If we can't connect to postgres database, try connecting to target database directly
                    # This handles cases where user doesn't have access to postgres database
                    pass
                except Exception as e:
                    # If checking fails, proceed with connection attempt - will fail with better error
                    pass
                
                # Connect to target database
                try:
                    conn = psycopg2.connect(
                        host=DB_HOST,
                        port=DB_PORT,
                        database=DB_NAME,
                        user=DB_USER,
                        password=DB_PASSWORD
                    )
                except psycopg2.OperationalError as e:
                    if "database" in str(e).lower() and "does not exist" in str(e).lower():
                        raise ValueError(f"Database '{DB_NAME}' does not exist. Please create it first using:\nCREATE DATABASE {DB_NAME};")
                    elif "password" in str(e).lower() or "authentication" in str(e).lower():
                        raise ValueError(f"Database authentication failed. Please check DB_USER and DB_PASSWORD in config.")
                    elif "could not connect" in str(e).lower():
                        raise ValueError(f"Could not connect to database server at {DB_HOST}:{DB_PORT}. Please check DB_HOST and DB_PORT in config.")
                    else:
                        raise ValueError(f"Database connection error: {str(e)}")
                
                try:
                    cur = conn.cursor()
                    
                    # Add scraper_id and import_timestamp columns if they don't exist
                    df['scraper_id'] = SCRAPER_ID_DB
                    df['import_timestamp'] = datetime.now()
                    
                    # Prepare data for insertion
                    # Replace NaN with None for proper NULL handling
                    df = df.where(pd.notnull(df), None)
                    
                    # Get column names (sanitize for SQL)
                    columns = [str(col).strip() for col in df.columns]
                    
                    # Check if table exists
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = %s
                        )
                    """, (table_name,))
                    table_exists = cur.fetchone()[0]
                    
                    if not table_exists:
                        # Create new table with all columns
                        # Start with required columns
                        column_defs = [
                            "id SERIAL PRIMARY KEY",
                            "scraper_id VARCHAR(100)",
                            "import_timestamp TIMESTAMP"
                        ]
                        # Add data columns
                        for col in columns:
                            if col not in ['scraper_id', 'import_timestamp', 'id']:
                                # Sanitize column name and add as TEXT
                                safe_col = col.replace('"', '""')  # Escape quotes
                                column_defs.append(f'"{safe_col}" TEXT')
                        
                        create_table_sql = f"""
                        CREATE TABLE {table_name} (
                            {', '.join(column_defs)}
                        );
                        """
                        cur.execute(create_table_sql)
                        conn.commit()
                    else:
                        # Table exists - check and add missing columns
                        cur.execute("""
                            SELECT column_name 
                            FROM information_schema.columns 
                            WHERE table_schema = 'public' 
                            AND table_name = %s
                        """, (table_name,))
                        existing_columns = [row[0] for row in cur.fetchall()]
                        
                        for col in columns:
                            if col not in existing_columns and col not in ['id']:
                                try:
                                    safe_col = col.replace('"', '""')  # Escape quotes
                                    cur.execute(f'ALTER TABLE {table_name} ADD COLUMN "{safe_col}" TEXT')
                                    conn.commit()
                                except Exception as e:
                                    # Column might already exist or other error
                                    conn.rollback()
                                    # If it's not a duplicate column error, re-raise
                                    if "already exists" not in str(e).lower() and "duplicate" not in str(e).lower():
                                        raise
                    
                    # Insert data
                    # Prepare column list with proper escaping
                    safe_columns = []
                    for col in columns:
                        escaped_col = col.replace('"', '""')  # Escape quotes
                        safe_columns.append(f'"{escaped_col}"')
                    values = [tuple(row) for row in df.values]
                    
                    insert_sql = f"""
                        INSERT INTO {table_name} ({', '.join(safe_columns)})
                        VALUES %s
                    """
                    execute_values(cur, insert_sql, values)
                    
                    conn.commit()
                    rows_inserted = len(df)
                    
                    # Update UI in main thread
                    self.root.after(0, lambda: messagebox.showinfo(
                        "Success", 
                        f"Successfully pushed {rows_inserted:,} rows to database table '{table_name}'.\n\n"
                        f"Table '{table_name}' was created if it didn't exist."
                    ))
                    self.root.after(0, lambda: self.update_status(
                        f"Pushed {rows_inserted:,} rows from {file_name} to {table_name}"
                    ))
                    
                except Exception as e:
                    conn.rollback()
                    raise e
                finally:
                    cur.close()
                    conn.close()
                    
            except Exception as e:
                error_msg = f"Error pushing to database: {str(e)}"
                self.root.after(0, lambda: messagebox.showerror("Error", error_msg))
                self.root.after(0, lambda: self.update_status(f"Database push failed: {str(e)}"))
        
        # Start push in background thread
        thread = threading.Thread(target=push_thread, daemon=True)
        thread.start()
    
    def refresh_all_outputs(self):
        """Refresh all output tabs"""
        if hasattr(self, 'output_listbox') and self.output_listbox:
            self.refresh_output_files()
        if hasattr(self, 'final_output_listbox'):
            self.refresh_final_output_files()
        if hasattr(self, 'output_table_combo'):
            self._refresh_output_tables()
        self.update_status("All outputs refreshed")
    
    def show_statistics(self):
        """Show statistics about outputs"""
        stats = []
        
        # Root output folder
        root_output = self.repo_root / "output"
        if root_output.exists():
            files = list(root_output.rglob("*"))
            file_count = sum(1 for f in files if f.is_file())
            total_size = sum(f.stat().st_size for f in files if f.is_file())
            stats.append(f"Root Output Folder:")
            stats.append(f"  Files: {file_count}")
            stats.append(f"  Size: {total_size:,} bytes ({total_size / 1024 / 1024:.2f} MB)")
        
        # Scraper outputs
        for scraper_name, scraper_info in self.scrapers.items():
            output_dir = scraper_info["path"] / "output"
            if output_dir.exists():
                files = list(output_dir.rglob("*"))
                file_count = sum(1 for f in files if f.is_file())
                total_size = sum(f.stat().st_size for f in files if f.is_file())
                stats.append(f"\n{scraper_name} Output:")
                stats.append(f"  Files: {file_count}")
                stats.append(f"  Size: {total_size:,} bytes ({total_size / 1024 / 1024:.2f} MB)")
        
        self.rest_info_text.config(state=tk.NORMAL)
        self.rest_info_text.delete(1.0, tk.END)
        self.rest_info_text.insert(1.0, "\n".join(stats) if stats else "No output files found")
        self.rest_info_text.config(state=tk.DISABLED)
        self.update_status("Statistics updated")
    
    def clean_temp_files(self):
        """Clean temporary files"""
        if not messagebox.askyesno("Confirm", "Clean temporary files? This will remove .tmp, .log, and cache files."):
            return
        
        cleaned = 0
        for scraper_name, scraper_info in self.scrapers.items():
            output_dir = scraper_info["path"] / "output"
            if output_dir.exists():
                for file_path in output_dir.rglob("*"):
                    if file_path.is_file():
                        if file_path.suffix in [".tmp", ".log", ".cache"] or "~$" in file_path.name:
                            try:
                                file_path.unlink()
                                cleaned += 1
                            except Exception:
                                pass
        
        self.update_status(f"Cleaned {cleaned} temporary files")
        messagebox.showinfo("Information", f"Cleaned {cleaned} temporary files")
    
    def view_all_logs(self):
        """View all log files"""
        log_files = []
        try:
            root_logs = self._resolve_logs_dir()
            log_files.extend(str(p) for p in root_logs.rglob("*.log"))
        except Exception:
            pass
        for scraper_name, scraper_info in self.scrapers.items():
            logs_dir = scraper_info["path"] / "logs"
            if logs_dir.exists():
                log_files.extend(str(log_file) for log_file in logs_dir.rglob("*.log"))
        
        log_files = sorted(set(log_files))
        
        if log_files:
            info = f"Found {len(log_files)} log files:\n\n"
            info += "\n".join(log_files)
        else:
            info = "No log files found"
        
        self.rest_info_text.config(state=tk.NORMAL)
        self.rest_info_text.delete(1.0, tk.END)
        self.rest_info_text.insert(1.0, info)
        self.rest_info_text.config(state=tk.DISABLED)
    
    def organize_outputs(self):
        """Organize output files"""
        messagebox.showinfo("Information", "Output organization feature - Not yet implemented")
        self.update_status("Organize outputs - Feature in development")
    
    def export_summary(self):
        """Export summary of all outputs"""
        filename = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
        )
        
        if filename:
            try:
                summary = []
                summary.append("Scraper Output Summary")
                summary.append("=" * 50)
                summary.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                
                for scraper_name, scraper_info in self.scrapers.items():
                    output_dir = scraper_info["path"] / "output"
                    summary.append(f"\n{scraper_name}:")
                    if output_dir.exists():
                        files = list(output_dir.rglob("*"))
                        file_count = sum(1 for f in files if f.is_file())
                        summary.append(f"  Files: {file_count}")
                        for f in sorted(files):
                            if f.is_file():
                                rel_path = f.relative_to(output_dir)
                                summary.append(f"    - {rel_path}")
                    else:
                        summary.append("  No output directory")
                
                with open(filename, "w", encoding="utf-8") as f:
                    f.write("\n".join(summary))
                messagebox.showinfo("Information", f"Summary exported to:\n{filename}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to export summary:\n{str(e)}")
    
    def update_rest_info(self):
        """Update rest tab information"""
        info = []
        info.append("System Information")
        info.append("=" * 50)
        info.append(f"Repository Root: {self.repo_root}")
        info.append(f"Scrapers Available: {len(self.scrapers)}")
        info.append("\nScrapers:")
        for scraper_name, scraper_info in self.scrapers.items():
            info.append(f"  - {scraper_name}")
            info.append(f"    Path: {scraper_info['path']}")
            info.append(f"    Steps: {len(scraper_info['steps'])}")
        
        info.append(f"\nRoot Output Folder: {self.repo_root / 'output'}")
        
        self.rest_info_text.config(state=tk.NORMAL)
        self.rest_info_text.delete(1.0, tk.END)
        self.rest_info_text.insert(1.0, "\n".join(info))
        self.rest_info_text.config(state=tk.DISABLED)
    
    def update_status(self, message):
        """Update status bar (removed - no-op)"""
        # Status bar removed per user request
        pass
    
    def install_dependencies_in_gui(self):
        """Install dependencies and show progress in GUI console"""
        def write_to_console(message, end="\n"):
            """Write message to GUI console"""
            self.log_text.config(state=tk.NORMAL)
            self.log_text.insert(tk.END, message + end)
            self.log_text.see(tk.END)
            self.log_text.config(state=tk.DISABLED)
            self.root.update_idletasks()
        
        def update_progress(description, percent=None):
            """Update progress description and bar"""
            if description:
                self.progress_label.config(text=description)
            if percent is not None:
                self.progress_bar['value'] = percent
                self.progress_percent.config(text=f"{int(percent)}%")
            self.root.update_idletasks()
        
        # Run dependency installation in a separate thread to avoid blocking UI
        def run_installation():
            try:
                update_progress("Installing dependencies...", 0)
                write_to_console("=" * 70)
                write_to_console("DEPENDENCY INSTALLATION")
                write_to_console("=" * 70)
                write_to_console("")
                
                result = install_dependencies(
                    write_callback=write_to_console,
                    progress_callback=update_progress
                )
                
                if result:
                    update_progress("Dependencies installed successfully", 100)
                    write_to_console("")
                    write_to_console("=" * 70)
                    write_to_console("Dependency installation complete. Ready to use.")
                    write_to_console("=" * 70)
                    write_to_console("")
                else:
                    update_progress("Dependency installation failed", 0)
                    write_to_console("")
                    write_to_console("⚠ Warning: Some dependencies may have failed to install.")
                    write_to_console("You can manually install them with: pip install -r requirements.txt")
                    write_to_console("")
            except Exception as e:
                update_progress(f"Error: {str(e)}", 0)
                write_to_console(f"\n[ERROR] Dependency installation failed: {e}\n")
        
        # Start installation in background thread
        thread = threading.Thread(target=run_installation, daemon=True)
        thread.start()


def main():
    try:
        root = tk.Tk()
        app = ScraperGUI(root)
        root.mainloop()
    except Exception as e:
        import traceback
        with open("gui_crash.log", "w", encoding='utf-8') as f:
            f.write(f"GUI Startup Crash: {e}\n")
            traceback.print_exc(file=f)
        try:
            messagebox.showerror("Critical Error", f"Failed to start GUI:\n{e}\n\nSee gui_crash.log for details.")
        except:
            pass
        raise e


def install_dependencies(write_callback=None, progress_callback=None):
    """
    Install dependencies from requirements.txt if it exists.
    
    Args:
        write_callback: Function to write messages (takes message, end="\n")
        progress_callback: Function to update progress (takes description, percent=None)
    
    Returns:
        bool: True if successful, False otherwise
    """
    # Default to print if no callbacks provided
    if write_callback is None:
        write_callback = lambda msg, end="\n": print(msg, end=end, flush=True)
    if progress_callback is None:
        progress_callback = lambda desc, pct=None: None
    
    requirements_file = Path(__file__).parent / "requirements.txt"
    
    if not requirements_file.exists():
        write_callback("=" * 70)
        write_callback("No requirements.txt file found. Skipping dependency installation.")
        write_callback("=" * 70)
        write_callback("")
        return True
    
    try:
        import subprocess
        import sys
        import re
        
        # Step 1: Read requirements.txt
        progress_callback("Reading requirements.txt...", 10)
        write_callback("[Step 1/3] Reading requirements.txt...")
        with open(requirements_file, 'r', encoding='utf-8') as f:
            requirements = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
        
        if not requirements:
            write_callback("  ⚠ No packages found in requirements.txt")
            write_callback("")
            return True
        
        write_callback(f"  Found {len(requirements)} package(s):")
        for req in requirements[:10]:  # Show first 10
            if req and not req.startswith('#'):
                write_callback(f"    • {req}")
        if len(requirements) > 10:
            write_callback(f"    ... and {len(requirements) - 10} more")
        write_callback("")
        
        # Step 2: Check pip
        progress_callback("Checking pip...", 20)
        write_callback("[Step 2/3] Checking pip...")
        try:
            result = subprocess.run(
                [sys.executable, "-m", "pip", "--version"],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                pip_version = result.stdout.strip().split()[1] if len(result.stdout.split()) > 1 else "unknown"
                write_callback(f"  ✓ pip is available (version: {pip_version})")
            else:
                write_callback("  ⚠ pip check failed")
                return False
        except Exception as e:
            write_callback(f"  ⚠ pip check failed: {e}")
            return False
        write_callback("")
        
        # Step 3: Install dependencies
        progress_callback("Installing dependencies...", 30)
        write_callback("[Step 3/3] Installing dependencies...")
        write_callback("-" * 70)
        
        # Use unbuffered output for real-time progress
        process = subprocess.Popen(
            [sys.executable, "-m", "pip", "install", "-r", str(requirements_file), "--disable-pip-version-check"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=0
        )
        
        # Track packages
        packages_installing = []
        packages_installed = []
        packages_existing = []
        current_package = None
        total_packages = len(requirements)
        packages_processed = 0
        
        # Stream output in real-time
        for line in iter(process.stdout.readline, ''):
            if not line:
                break
            
            line = line.strip()
            if not line:
                continue
            
            # Show progress for collecting packages
            if line.startswith("Collecting "):
                package_match = re.search(r'Collecting ([^\s]+)', line)
                if package_match:
                    current_package = package_match.group(1).split('==')[0].split('>=')[0].split('<=')[0]
                    write_callback(f"  → Installing {current_package}...")
                    if current_package not in packages_installing:
                        packages_installing.append(current_package)
                        packages_processed += 1
                        # Update progress: 30% + (packages_processed/total_packages * 60%)
                        progress = 30 + int((packages_processed / total_packages) * 60)
                        progress_callback(f"Installing {current_package}... ({packages_processed}/{total_packages})", min(progress, 90))
            
            # Show already installed packages
            elif "Requirement already satisfied:" in line:
                package_match = re.search(r'Requirement already satisfied: ([^\s]+)', line)
                if package_match:
                    package = package_match.group(1).split('==')[0].split('>=')[0].split('<=')[0]
                    if package not in packages_existing:
                        packages_existing.append(package)
                        write_callback(f"  ✓ {package} (already installed)")
            
            # Show successfully installed packages
            elif "Successfully installed" in line:
                installed_match = re.findall(r'([a-zA-Z0-9_-]+)-([0-9.]+)', line)
                for package_name, version in installed_match:
                    if package_name not in packages_installed:
                        packages_installed.append(package_name)
                        write_callback(f"  ✓ {package_name} (v{version}) installed")
            
            # Show errors
            elif "ERROR" in line or ("error" in line.lower() and "warning" not in line.lower()):
                write_callback(f"  ⚠ {line}")
        
        process.wait()
        write_callback("-" * 70)
        
        # Summary
        progress_callback("Finishing installation...", 95)
        if process.returncode == 0:
            write_callback("")
            if packages_installed:
                write_callback(f"✓ Successfully installed {len(packages_installed)} new package(s)")
            if packages_existing:
                write_callback(f"✓ {len(packages_existing)} package(s) were already installed")
            if not packages_installed and not packages_existing:
                write_callback("✓ All packages were already installed")
            progress_callback("Dependencies installed successfully", 100)
            return True
        else:
            write_callback("⚠ Warning: Some dependencies may have failed to install.")
            write_callback("  You can manually install them with:")
            write_callback(f"  pip install -r {requirements_file}")
            write_callback("  Continuing with application startup...")
            progress_callback("Dependency installation completed with warnings", 100)
            return True
        
    except FileNotFoundError:
        write_callback("  ⚠ Error: pip not found. Please install Python and pip first.")
        write_callback("  Skipping dependency installation.")
        write_callback("")
        progress_callback("Dependency installation failed - pip not found", 0)
        return False
    except Exception as e:
        write_callback(f"  ⚠ Error: Failed to install dependencies: {e}")
        write_callback("  You can manually install them with:")
        write_callback(f"  pip install -r {requirements_file}")
        write_callback("  Continuing with application startup...")
        write_callback("")
        progress_callback("Dependency installation failed", 0)
        return False


if __name__ == "__main__":
    # Initialize ConfigManager if available
    if ConfigManager:
        try:
            ConfigManager.ensure_dirs()
        except:
            pass
    
    def cleanup_on_exit():
        """Cleanup function to release lock on exit"""
        # Only release lock if we actually acquired it
        if ConfigManager and _app_lock_acquired:
            try:
                ConfigManager.release_lock()
            except:
                pass
    
    import atexit
    atexit.register(cleanup_on_exit)
    main()

