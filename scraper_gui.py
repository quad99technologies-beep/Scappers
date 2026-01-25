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
from typing import Optional

# CRITICAL: Initialize ConfigManager FIRST before any other imports
# Add repo root to path for core.config_manager
repo_root = Path(__file__).resolve().parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

try:
    from core.config_manager import ConfigManager
    
    # Ensure directories exist
    ConfigManager.ensure_dirs()
    
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
        
        # Current scraper and step
        self.current_scraper = None
        self.current_step = None
        self.running_processes = {}  # Track processes per scraper: {scraper_name: process}
        self.running_scrapers = set()  # Track which scrapers are running from GUI
        self.scraper_logs = {}  # Store logs per scraper: {scraper_name: log_text}
        self._pipeline_lock_files = {}  # Track lock files created for pipeline runs: {scraper_name: lock_file_path}
        self._stopped_by_user = set()  # Track scrapers that were stopped by user: {scraper_name}
        self._stopping_scrapers = set()  # Track scrapers currently being stopped to prevent multiple simultaneous stop attempts
        self.scraper_progress = {}  # Store progress state per scraper: {scraper_name: {"percent": float, "description": str}}
        self._last_completed_logs = {}  # Store last run log content per scraper for archive/save
        self._log_stream_state = {}  # Track external log stream offsets per scraper
        self._scraper_active_state = {}  # Track lock-based run activity per scraper
        self.telegram_process = None
        self.telegram_log_path = None
        self._external_log_files = {}  # Track external log files for pipelines started outside GUI
        
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
        self.start_periodic_telegram_status()
        
        # Step explanations cache (key: script_path, value: explanation text)
        self.step_explanations = {}
        # Explanation cache file
        self.explanation_cache_file = self.repo_root / ".step_explanations_cache.json"
        self.load_explanation_cache()
        
        # Define scrapers and their steps
        self.scrapers = {
            "CanadaQuebec": {
                "path": self.repo_root / "scripts" / "CanadaQuebec",
                "scripts_dir": "",
                "docs_dir": None,  # All docs now in root doc/ folder
                "steps": [
                    {"name": "00 - Backup and Clean", "script": "00_backup_and_clean.py", "desc": "Backup output folder and clean for fresh run"},
                    {"name": "01 - Split PDF into Annexes", "script": "01_split_pdf_into_annexes.py", "desc": "Split PDF into annexes (IV.1, IV.2, V)"},
                    {"name": "02 - Validate PDF Structure", "script": "02_validate_pdf_structure.py", "desc": "Validate PDF structure (optional)"},
                    {"name": "03 - Extract Annexe IV.1", "script": "03_extract_annexe_iv1.py", "desc": "Extract Annexe IV.1 data"},
                    {"name": "04 - Extract Annexe IV.2", "script": "04_extract_annexe_iv2.py", "desc": "Extract Annexe IV.2 data"},
                    {"name": "05 - Extract Annexe V", "script": "05_extract_annexe_v.py", "desc": "Extract Annexe V data"},
                    {"name": "06 - Merge All Annexes", "script": "06_merge_all_annexes.py", "desc": "Merge all annexe outputs into final CSV"},
                ],
                "pipeline_bat": "run_pipeline.bat"
            },
            "Malaysia": {
                "path": self.repo_root / "scripts" / "Malaysia",
                "scripts_dir": "",
                "docs_dir": None,  # All docs now in root doc/ folder
                "steps": [
                    {"name": "00 - Backup and Clean", "script": "00_backup_and_clean.py", "desc": "Backup output folder and clean for fresh run"},
                    {"name": "01 - Product Registration Number", "script": "01_Product_Registration_Number.py", "desc": "Get drug prices from MyPriMe"},
                    {"name": "02 - Product Details", "script": "02_Product_Details.py", "desc": "Get company/holder info from QUEST3+"},
                    {"name": "03 - Consolidate Results", "script": "03_Consolidate_Results.py", "desc": "Standardize and clean product details"},
                    {"name": "04 - Get Fully Reimbursable", "script": "04_Get_Fully_Reimbursable.py", "desc": "Scrape fully reimbursable drugs list"},
                    {"name": "05 - Generate PCID Mapped", "script": "05_Generate_PCID_Mapped.py", "desc": "Generate final PCID-mapped report"},
                ],
                "pipeline_bat": "run_pipeline.bat"
            },
            "Argentina": {
                "path": self.repo_root / "scripts" / "Argentina",
                "scripts_dir": "",
                "docs_dir": None,  # All docs now in root doc/ folder
                "steps": [
                    {"name": "00 - Backup and Clean", "script": "00_backup_and_clean.py", "desc": "Backup output folder and clean for fresh run"},
                    {"name": "01 - Get Product List", "script": "01_getProdList.py", "desc": "Extract product list for each company"},
                    {"name": "02 - Prepare URLs", "script": "02_prepare_urls.py", "desc": "Prepare URLs and initialize scrape state"},
                    {"name": "03 - Scrape Products (Selenium)", "script": "03_alfabeta_selenium_scraper.py", "desc": "Scrape products using Selenium"},
                    {"name": "04 - Scrape Products (API)", "script": "04_alfabeta_api_scraper.py", "desc": "Scrape products using API to fill gaps"},
                    {"name": "05 - Translate Using Dictionary", "script": "05_TranslateUsingDictionary.py", "desc": "Translate Spanish to English"},
                    {"name": "06 - Generate Output", "script": "06_GenerateOutput.py", "desc": "Generate final output report"},
                ],
                "pipeline_bat": "run_pipeline.bat"
            },
            "CanadaOntario": {
                "path": self.repo_root / "scripts" / "Canada Ontario",
                "scripts_dir": "",
                "docs_dir": None,  # All docs now in root doc/ folder
                "steps": [
                    {"name": "00 - Backup and Clean", "script": "00_backup_and_clean.py", "desc": "Backup output folder and clean for fresh run"},
                    {"name": "01 - Extract Product Details", "script": "01_extract_product_details.py", "desc": "Extract product details from Ontario Formulary"},
                    {"name": "02 - Extract EAP Prices", "script": "02_ontario_eap_prices.py", "desc": "Extract Exceptional Access Program product prices"},
                    {"name": "03 - Generate Final Output", "script": "03_GenerateOutput.py", "desc": "Generate final output report with standardized columns"},
                ],
                "pipeline_bat": "run_pipeline.bat"
            },
            "Netherlands": {
                "path": self.repo_root / "scripts" / "Netherlands",
                "scripts_dir": "",
                "docs_dir": None,  # All docs now in root doc/ folder
                "steps": [
                    {"name": "00 - Backup and Clean", "script": "00_backup_and_clean.py", "desc": "Backup output folder and clean for fresh run"},
                    {"name": "01 - Collect URLs", "script": "01_collect_urls.py", "desc": "Collect product URLs from search terms"},
                    {"name": "02 - Reimbursement Extraction", "script": "02_reimbursement_extraction.py", "desc": "Extract reimbursement data from collected URLs"},
                ],
                "pipeline_bat": "run_pipeline.bat"
            },
            "Belarus": {
                "path": self.repo_root / "scripts" / "Belarus",
                "scripts_dir": "",
                "docs_dir": None,  # All docs now in root doc/ folder
                "steps": [
                    {"name": "00 - Backup and Clean", "script": "00_backup_and_clean.py", "desc": "Backup output folder and clean for fresh run"},
                    {"name": "01 - Extract RCETH Data", "script": "01_belarus_rceth_extract.py", "desc": "Extract drug registration and pricing data from rceth.by"},
                ],
                "pipeline_bat": "run_pipeline.bat"
            },
            "Russia": {
                "path": self.repo_root / "scripts" / "Russia",
                "scripts_dir": "",
                "docs_dir": None,  # All docs now in root doc/ folder
                "steps": [
                    {"name": "00 - Backup and Clean", "script": "00_backup_and_clean.py", "desc": "Backup output folder and clean for fresh run"},
                    {"name": "01 - Extract VED Registry", "script": "01_russia_farmcom_scraper.py", "desc": "Extract VED drug pricing from farmcom.info/site/reestr (with page-level resume)"},
                    {"name": "02 - Extract Excluded List", "script": "02_russia_farmcom_excluded_scraper.py", "desc": "Extract excluded drugs from farmcom.info/site/reestr?vw=excl (with page-level resume)"},
                    {"name": "03 - Retry Failed Pages", "script": "03_retry_failed_pages.py", "desc": "Retry pages with missing EAN or extraction failures (MANDATORY before translation)"},
                    {"name": "04 - Process and Translate", "script": "04_process_and_translate.py", "desc": "Process raw data, translate Russian text to English using dictionary and AI"},
                    {"name": "05 - Format for Export", "script": "05_format_for_export.py", "desc": "Format translated data into final export template"},
                ],
                "pipeline_bat": "run_pipeline.bat"
            },
            "Taiwan": {
                "path": self.repo_root / "scripts" / "Taiwan",
                "scripts_dir": "",
                "docs_dir": None,  # All docs now in root doc/ folder
                "steps": [
                    {"name": "00 - Backup and Clean", "script": "00_backup_and_clean.py", "desc": "Backup output folder and clean for fresh run"},
                    {"name": "01 - Collect Drug Code URLs", "script": "01_taiwan_collect_drug_code_urls.py.py", "desc": "Collect drug code URLs from NHI site"},
                    {"name": "02 - Extract Drug Code Details", "script": "02_taiwan_extract_drug_code_details.py", "desc": "Extract license details for each drug code"},
                ],
                "pipeline_bat": "run_pipeline.bat"
            },
            "NorthMacedonia": {
                "path": self.repo_root / "scripts" / "North Macedonia",
                "scripts_dir": "",
                "docs_dir": None,  # All docs now in root doc/ folder
                "steps": [
                    {"name": "00 - Backup and Clean", "script": "00_backup_and_clean.py", "desc": "Backup output folder and clean for fresh run"},
                    {"name": "01 - Collect Detail URLs", "script": "01_collect_urls.py", "desc": "Collect detail URLs across overview pages"},
                    {"name": "02 - Scrape Detail Pages", "script": "02_scrape_details.py", "desc": "Scrape drug register detail data from collected URLs"},
                    {"name": "03 - Scrape Max Prices", "script": "03_scrape_zdravstvo.py", "desc": "Scrape max prices and effective dates from Zdravstvo"},
                ],
                "pipeline_bat": "run_pipeline.bat"
            },
            "Tender_Chile": {
                "path": self.repo_root / "scripts" / "Tender- Chile",
                "scripts_dir": "",
                "docs_dir": None,  # All docs now in root doc/ folder
                "steps": [
                    {"name": "00 - Backup and Clean", "script": "00_backup_and_clean.py", "desc": "Backup output folder and clean for fresh run"},
                    {"name": "01 - Get Redirect URLs", "script": "01_get_redirect_urls.py", "desc": "Get redirect URLs with qs parameters from tender list"},
                    {"name": "02 - Extract Tender Details", "script": "02_extract_tender_details.py", "desc": "Extract tender and lot details from MercadoPublico"},
                    {"name": "03 - Extract Tender Awards", "script": "03_extract_tender_awards.py", "desc": "Extract bidder-level award data from award pages"},
                    {"name": "04 - Merge Final CSV", "script": "04_merge_final_csv.py", "desc": "Merge all outputs into final EVERSANA-format CSV"},
                ],
                "pipeline_bat": "run_pipeline.bat"
            },
            "India": {
                "path": self.repo_root / "scripts" / "India",
                "scripts_dir": "",
                "docs_dir": None,  # All docs now in root doc/ folder
                "steps": [
                    {"name": "00 - Backup and Clean", "script": "00_backup_and_clean.py", "desc": "Backup output folder and clean for fresh run"},
                    {"name": "01 - Download Ceiling Prices", "script": "01_Ceiling Prices of Essential Medicines downlaod.py", "desc": "Download ceiling prices Excel from NPPA (provides formulations for Step 02)"},
                    {"name": "02 - Get Medicine Details", "script": "02 get details.py", "desc": "Extract medicine details and substitutes (auto-loads formulations from ceiling prices, supports resume)"},
                ],
                "pipeline_bat": "run_pipeline.bat",
                "resume_options": {
                    "supports_formulation_resume": True,
                    "checkpoint_dir": ".checkpoints",
                    "resume_script_args": ["--resume-details"]
                }
            }
        }

        self.health_check_scripts = self._discover_health_check_scripts()
        self.health_check_json_path = None
        self.health_check_running = False
        
        self.setup_ui()
        self.load_documentation()
        # Load first documentation if available (after UI is set up)
        self.root.after(100, self.load_first_documentation)
        # Install dependencies and show progress in GUI console
        self.root.after(200, self.install_dependencies_in_gui)
    
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

        # Dashboard page (execution + logs + outputs/config)
        dashboard_frame = ttk.Frame(self.main_notebook)
        self.main_notebook.add(dashboard_frame, text="Dashboard")
        self.setup_dashboard_page(dashboard_frame)

        # Pipeline Steps page
        pipeline_steps_frame = ttk.Frame(self.main_notebook)
        self.main_notebook.add(pipeline_steps_frame, text="Pipeline Steps")
        self.setup_pipeline_steps_tab(pipeline_steps_frame)

        # Documentation page
        documentation_frame = ttk.Frame(self.main_notebook)
        self.main_notebook.add(documentation_frame, text="Documentation")
        self.setup_documentation_tab(documentation_frame)

        # Health Check page (manual diagnostics)
        health_check_frame = ttk.Frame(self.main_notebook)
        self.main_notebook.add(health_check_frame, text="Health Check")
        self.setup_health_check_tab(health_check_frame)

    def _discover_health_check_scripts(self) -> dict[str, Path]:
        """Locate health_check scripts for enabled scrapers."""
        result = {}
        for scraper_name, scraper_info in self.scrapers.items():
            script_path = scraper_info["path"] / "health_check.py"
            if script_path.exists():
                result[scraper_name] = script_path
        return result

    def setup_dashboard_page(self, parent):
        """Setup dashboard page with execution controls, logs, and outputs/config tabs"""
        # Create main container with fixed widths (no resizing)
        # Fixed widths: 17% (exec controls) + 43% (logs) = 60% left, 40% right
        screen_width = self.root.winfo_screenwidth()
        dashboard_container = tk.Frame(parent, bg=self.colors['white'])
        dashboard_container.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        # Left panel - Execution (scraper selection, run controls, logs) - 60% total (17% + 43%)
        left_panel = ttk.Frame(dashboard_container)
        left_panel.configure(style='TFrame')
        left_panel.pack(side=tk.LEFT, fill=tk.BOTH, expand=False)
        left_panel.config(width=int(screen_width * 0.60))
        left_panel.pack_propagate(False)

        # Right panel - Outputs/config tabs - 40%
        right_panel = ttk.Frame(dashboard_container)
        right_panel.configure(style='TFrame')
        right_panel.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Setup left panel (execution)
        self.setup_left_panel(left_panel)

        # Setup right panel (outputs/config only)
        self.setup_right_panel(
            right_panel,
            include_pipeline_steps=False,
            include_docs=False
        )
        
    def setup_left_panel(self, parent):
        """Setup left panel with execution controls and logs side by side"""
        # Create fixed-width split for execution and logs (no resizing)
        # Fixed widths: 17% (exec controls) + 43% (logs) = 60% total of window
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
        
        # Right side - Execution logs - 43% of total window (fixed width)
        logs_frame = ttk.Frame(exec_split)
        logs_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=False)
        logs_frame.config(width=int(screen_width * 0.43))
        logs_frame.pack_propagate(False)
        self.setup_logs_tab(logs_frame)
    
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
        scraper_combo.pack(fill=tk.X, expand=True, padx=8, pady=(0, 6))
        scraper_combo.bind("<<ComboboxSelected>>", self.on_scraper_selected)

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

        self.open_tor_browser_button = ttk.Button(actions_section, text="Open Tor Browser",
                  command=self.open_tor_browser, width=23, state=tk.NORMAL, style='Secondary.TButton')
        self.open_tor_browser_button.pack(pady=(0, 6), padx=8, fill=tk.X, expand=True)
        
        # Checkpoint management - light gray border
        checkpoint_section = tk.Frame(parent, bg=self.colors['white'],
                                      highlightbackground=self.colors['border_gray'],
                                      highlightthickness=1)
        checkpoint_section.pack(fill=tk.X, padx=8, pady=(0, 6))
        
        tk.Label(checkpoint_section, text="Checkpoint Management", bg=self.colors['white'], fg='#000000', 
                font=self.fonts['bold']).pack(anchor=tk.W, padx=8, pady=(6, 3))

        self.view_checkpoint_button = ttk.Button(checkpoint_section, text="View Checkpoint",
                  command=self.view_checkpoint_file, width=23, style='Secondary.TButton')
        self.view_checkpoint_button.pack(pady=(0, 3), padx=8, fill=tk.X, expand=True)

        self.manage_checkpoint_button = ttk.Button(checkpoint_section, text="Manage Checkpoint",
                  command=self.manage_checkpoint, width=23, style='Secondary.TButton')
        self.manage_checkpoint_button.pack(pady=(0, 3), padx=8, fill=tk.X, expand=True)

        self.clear_checkpoint_button = ttk.Button(checkpoint_section, text="Clear Checkpoint",
                  command=self.clear_checkpoint, width=23, style='Secondary.TButton')
        self.clear_checkpoint_button.pack(pady=(0, 6), padx=8, fill=tk.X, expand=True)

        # Telegram bot control - light gray border
        telegram_section = tk.Frame(parent, bg=self.colors['white'],
                                    highlightbackground=self.colors['border_gray'],
                                    highlightthickness=1)
        telegram_section.pack(fill=tk.X, padx=8, pady=(0, 6))

        # Header frame with title and status icon
        telegram_header_frame = tk.Frame(telegram_section, bg=self.colors['white'])
        telegram_header_frame.pack(fill=tk.X, padx=8, pady=(6, 3))

        tk.Label(telegram_header_frame, text="Telegram Bot", bg=self.colors['white'], fg='#000000',
                font=self.fonts['bold']).pack(side=tk.LEFT)

        # Status icon (green for running, red for stopped)
        self.telegram_status_icon = tk.Label(telegram_header_frame, text="●", bg=self.colors['white'],
                                            fg='#dc3545', font=('Arial', 16))
        self.telegram_status_icon.pack(side=tk.RIGHT)

        telegram_status_frame = tk.Frame(telegram_section, bg=self.colors['white'])
        telegram_status_frame.pack(fill=tk.X, padx=8, pady=(0, 6))

        self.telegram_status_label = tk.Label(telegram_status_frame,
                                              text="Status: Stopped",
                                              bg=self.colors['white'],
                                              fg='#000000',
                                              font=self.fonts['standard'],
                                              anchor=tk.W)
        self.telegram_status_label.pack(fill=tk.X, pady=(0, 3), padx=0)

        self.telegram_log_label = tk.Label(telegram_status_frame,
                                           text="Log: (none)",
                                           bg=self.colors['white'],
                                           fg='#000000',
                                           font=self.fonts['standard'],
                                           anchor=tk.W)
        self.telegram_log_label.pack(fill=tk.X, pady=(0, 3), padx=0)

        self.start_telegram_button = ttk.Button(telegram_section, text="Start Bot",
                                                command=self.start_telegram_bot, width=23,
                                                style='Secondary.TButton')
        self.start_telegram_button.pack(pady=(0, 3), padx=8, fill=tk.X, expand=True)

        self.stop_telegram_button = ttk.Button(telegram_section, text="Stop Bot",
                                               command=self.stop_telegram_bot, width=23,
                                               style='Secondary.TButton')
        self.stop_telegram_button.pack(pady=(0, 6), padx=8, fill=tk.X, expand=True)
        self.refresh_telegram_status()
    
    def setup_pipeline_steps_tab(self, parent):
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
            width=26
        )
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
        self.explain_button.pack(side=tk.LEFT, padx=0, pady=0)

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
        for step in scraper_info["steps"]:
            self.steps_listbox.insert(tk.END, step["name"])
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
            style='Modern.TCombobox'
        )
        self.health_check_combo.grid(row=0, column=1, sticky=tk.EW, pady=4)
        controls_frame.columnconfigure(1, weight=1)

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
            path = message.split(":", 1)[1].strip()
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
        
        
    def setup_logs_tab(self, parent):
        """Setup logs viewer panel"""
        # System Status frame (FIRST - at the top) - white background with light gray border
        stats_frame = tk.Frame(parent, bg=self.colors['white'],
                               highlightthickness=0,
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
        
        # Execution Log section (BELOW System Status) - white background, no border
        execution_log_frame = tk.Frame(parent, bg=self.colors['white'],
                                       highlightthickness=0,
                                       bd=0)
        execution_log_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))
        
        # Label for the section
        tk.Label(execution_log_frame, text="Execution Log", 
                font=self.fonts['bold'],
                bg=self.colors['white'],
                fg='#000000').pack(anchor=tk.W, padx=16, pady=(16, 4))
        
        # Subtle horizontal separator below label
        separator = tk.Frame(execution_log_frame, height=1, bg=self.colors['border_light'])
        separator.pack(fill=tk.X, padx=16, pady=(0, 8))
        
        # Toolbar - white background
        toolbar = tk.Frame(execution_log_frame, bg=self.colors['white'])
        toolbar.pack(fill=tk.X, padx=5, pady=8)

        ttk.Button(toolbar, text="Clear", command=self.clear_logs, style='Secondary.TButton').pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(toolbar, text="Copy to Clipboard", command=self.copy_logs_to_clipboard, style='Secondary.TButton').pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(toolbar, text="Save Log", command=self.save_log, style='Secondary.TButton').pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(toolbar, text="Archive Log", command=self.archive_current_log, style='Secondary.TButton').pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(toolbar, text="Open in Cursor", command=self.open_console_in_cursor, style='Secondary.TButton').pack(side=tk.LEFT, padx=(0, 8))
        
        # Progress frame - white background (2 lines)
        progress_frame = tk.Frame(execution_log_frame, bg=self.colors['white'])
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

        # Log viewer - CRITICAL: Black background with yellow text
        log_viewer_frame = tk.Frame(
            execution_log_frame,
            bg=self.colors['dark_gray'],
            highlightthickness=0,
            bd=0,
            relief='flat'
        )
        log_viewer_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

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
            from platform_config import get_path_manager
            pm = get_path_manager()
            default_exports = pm.get_exports_dir()
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
        # Toolbar - white background
        toolbar = tk.Frame(parent, bg=self.colors['white'])
        toolbar.pack(fill=tk.X, padx=8, pady=8)
        
        tk.Label(toolbar, text="Scraper Configuration:", 
                font=self.fonts['standard'],
                bg=self.colors['white'],
                fg='#000000').pack(side=tk.LEFT, padx=(0, 8))
        
        ttk.Button(toolbar, text="Load", command=self.load_config_file, 
                  style='Secondary.TButton').pack(side=tk.LEFT, padx=3)
        ttk.Button(toolbar, text="Save", command=self.save_config_file, 
                  style='Primary.TButton').pack(side=tk.LEFT, padx=3)
        ttk.Button(toolbar, text="Open File", command=self.open_config_file, 
                  style='Secondary.TButton').pack(side=tk.LEFT, padx=3)
        ttk.Button(toolbar, text="Create from Template", command=self.create_config_from_template, 
                  style='Secondary.TButton').pack(side=tk.LEFT, padx=3)
        
        # Config editor - dark theme with border
        editor_frame = ttk.LabelFrame(parent, text="Configuration Editor", padding=12, style='Title.TLabelframe')
        editor_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        
        self.config_text = scrolledtext.ScrolledText(editor_frame, wrap=tk.WORD,
                                                     font=self.fonts['monospace'],  # Monospace
                                                     bg=self.colors['white'],  # White background
                                                     fg='#000000',  # Black text
                                                     borderwidth=0,
                                                     relief='flat',
                                                     highlightthickness=0,
                                                     padx=16,
                                                     pady=16,
                                                     insertbackground='#000000',
                                                     selectbackground=self.colors['background_gray'],
                                                     selectforeground='#000000')
        self.config_text.pack(fill=tk.BOTH, expand=True)
        
        # Configure JSON syntax highlighting tags
        self.config_text.tag_configure("json_key", foreground='#000000')
        self.config_text.tag_configure("json_string", foreground="#10b981")  # Green for strings
        self.config_text.tag_configure("json_number", foreground="#f59e0b")  # Amber for numbers
        self.config_text.tag_configure("json_boolean", foreground='#000000')
        
        # Status - white background, no border
        self.config_status = tk.Label(parent, text="Scraper-specific configuration file", 
                                       relief=tk.FLAT, anchor=tk.W,
                                       bg=self.colors['white'],
                                       fg='#000000',
                                       font=self.fonts['standard'],
                                       padx=10,
                                       borderwidth=0,
                                       highlightthickness=0)
        self.config_status.pack(fill=tk.X, padx=8, pady=8)
        
        # Store current config file path (will be set when scraper is selected)
        self.current_config_file = None
        
        # Don't auto-load - wait for scraper selection
    
    
    def load_config_file(self):
        """Load scraper-specific config file (config/{scraper_id}.env.json)"""
        scraper_name = self.scraper_var.get() if hasattr(self, 'scraper_var') else None
        if not scraper_name:
            messagebox.showwarning("Warning", "Please select a scraper first to load its configuration.")
            return
        
        # Use scraper-specific config file from config directory
        try:
            from platform_config import get_path_manager
            pm = get_path_manager()
            config_dir = pm.get_config_dir()
            config_file = config_dir / f"{scraper_name}.env.json"
        except Exception:
            # Fallback to repo root config directory
            config_dir = self.repo_root / "config"
            config_file = config_dir / f"{scraper_name}.env.json"
        
        # Update current config file path
        self.current_config_file = config_file
        
        if config_file.exists():
            try:
                with open(config_file, "r", encoding="utf-8") as f:
                    content = f.read()
                self.config_text.delete(1.0, tk.END)
                self.config_text.insert(1.0, content)
                self.config_status.config(text=f"Loaded: {config_file.name} ({scraper_name} configuration)")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to load config file:\n{str(e)}")
                self.config_status.config(text=f"Error loading config: {str(e)}")
        else:
            # File doesn't exist, show empty editor with template
            self.config_text.delete(1.0, tk.END)
            template = "{\n"
            template += f'  "{scraper_name}": {{\n'
            template += '    "OPENAI_API_KEY": "",\n'
            template += '    "OPENAI_MODEL": "gpt-4o-mini"\n'
            template += "  }\n"
            template += "}\n"
            self.config_text.insert(1.0, template)
            self.config_status.config(text=f"Config file not found. Will create: {config_file.name}")
    
    def save_config_file(self):
        """Save .env file for selected scraper"""
        if not self.current_config_file:
            messagebox.showwarning("Warning", "No configuration file loaded. Select a scraper first.")
            return
        
        try:
            content = self.config_text.get(1.0, tk.END)
            # Remove trailing newline from tkinter
            if content.endswith("\n"):
                content = content[:-1]
            
            # Create directory if needed
            self.current_config_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.current_config_file, "w", encoding="utf-8") as f:
                f.write(content)
            
            self.config_status.config(text=f"Saved: {self.current_config_file}")
            messagebox.showinfo("Information", f"Configuration saved to:\n{self.current_config_file}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save config file:\n{str(e)}")
            self.config_status.config(text=f"Error saving config: {str(e)}")
    
    def open_config_file(self):
        """Open config file in system default editor"""
        if not self.current_config_file:
            messagebox.showwarning("Warning", "No configuration file loaded. Select a scraper first.")
            return
        
        if self.current_config_file.exists():
            os.startfile(str(self.current_config_file))
        else:
            messagebox.showinfo("Information", f"Configuration file does not exist:\n{self.current_config_file}\n\nUse 'Save' to create the file.")
    
    def create_config_from_template(self):
        """Create scraper config file from template"""
        scraper_name = self.scraper_var.get() if hasattr(self, 'scraper_var') else None
        if not scraper_name:
            messagebox.showwarning("Warning", "Please select a scraper first.")
            return
        
        # Try to find template in config directory
        try:
            from platform_config import get_path_manager
            pm = get_path_manager()
            config_dir = pm.get_config_dir()
            template_file = config_dir / f"{scraper_name}.env.json.example"
        except Exception:
            config_dir = self.repo_root / "config"
            template_file = config_dir / f"{scraper_name}.env.json.example"
        
        if not template_file.exists():
            # Try scraper directory
            scraper_info = self.scrapers.get(scraper_name, {})
            if scraper_info:
                scraper_template = scraper_info["path"] / ".env.example"
                if scraper_template.exists():
                    template_file = scraper_template
        
        if not template_file.exists():
            messagebox.showwarning("Warning", f"Template file not found:\n{template_file}\n\nCreating empty config file instead.")
            self.config_text.delete(1.0, tk.END)
            template = "{\n"
            template += f'  "{scraper_name}": {{\n'
            template += '    "OPENAI_API_KEY": "",\n'
            template += '    "OPENAI_MODEL": "gpt-4o-mini"\n'
            template += "  }\n"
            template += "}\n"
            self.config_text.insert(1.0, template)
            return
        
        try:
            with open(template_file, "r", encoding="utf-8") as f:
                template_content = f.read()
            
            # Load into editor
            self.config_text.delete(1.0, tk.END)
            self.config_text.insert(1.0, template_content)
            self.config_status.config(text=f"Loaded template from: {template_file.name}")
            messagebox.showinfo("Information", f"Template loaded from:\n{template_file}\n\nUse 'Save' to create config file.")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load template:\n{str(e)}")
    
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
                            scraper_name = "CanadaQuebec"
                        elif "MALAYSIA" in scraper_name:
                            scraper_name = "Malaysia"
                        elif "ARGENTINA" in scraper_name or "PIPELINE" in scraper_name:
                            scraper_name = "Argentina"
                        elif "TAIWAN" in scraper_name:
                            scraper_name = "Taiwan"
                        key = f"{scraper_name} - {doc_file.name}"
                        self.docs[key] = doc_file
            
            # Also check for scraper-specific doc directories (doc/CanadaQuebec/, doc/Malaysia/, etc.)
            for scraper_name in ["CanadaQuebec", "Malaysia", "Argentina", "Taiwan"]:
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
        
        # Update output path to scraper output directory (not runs directory)
        if hasattr(self, 'output_path_var'):
            # Use scraper-specific output directory
            try:
                from platform_config import get_path_manager
                pm = get_path_manager()
                output_dir = pm.get_output_dir(scraper_name)
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
                from platform_config import get_path_manager
                pm = get_path_manager()
                exports_dir = pm.get_exports_dir(scraper_name)
                self.final_output_path_var.set(str(exports_dir))
            except Exception:
                # Fallback to repo root/exports/{scraper_name}
                exports_dir = self.repo_root / "exports" / scraper_name
                self.final_output_path_var.set(str(exports_dir))
            
            # Refresh final output files (filtered by scraper)
            if hasattr(self, 'refresh_final_output_files'):
                self.refresh_final_output_files()
        
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
        
        # Refresh button state
        self.refresh_run_button_state()
        
        # Update checkpoint status
        self.update_checkpoint_status()
        
        # Update Chrome instance count
        self.update_chrome_count()
        
        # Update kill all Chrome button state
        self.update_kill_all_chrome_button_state()
    
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
    
    def update_log_display(self, scraper_name: str):
        """Update log display with the selected scraper's log"""
        if self._is_scraper_active(scraper_name):
            self._sync_external_log_if_running(scraper_name)
        log_content = self.scraper_logs.get(scraper_name, "")
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.insert(1.0, log_content)
        self.log_text.see(tk.END)
        self.log_text.config(state=tk.DISABLED)
        
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
            current_content = self.log_text.get(1.0, tk.END)
            if log_content != current_content.rstrip('\n'):
                # Log has been updated, refresh display
                self.log_text.config(state=tk.NORMAL)
                self.log_text.delete(1.0, tk.END)
                self.log_text.insert(1.0, log_content)
                self.log_text.see(tk.END)
                self.log_text.config(state=tk.DISABLED)
            
            # Update display with stored progress state
            progress_state = self.scraper_progress.get(scraper_name, {"percent": 0, "description": f"Running {scraper_name}..."})
            self.progress_label.config(text=progress_state["description"])
            self.progress_bar['value'] = progress_state["percent"]
            self.progress_percent.config(text=f"{progress_state['percent']:.1f}%")
        
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
        
        # Pattern 6: Look for current step name in log (e.g., "Running step: X")
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
        
        # Store progress state for this scraper
        final_percent = progress_percent if progress_percent is not None else 0
        progress_state = {"percent": final_percent, "description": progress_desc}
        self.scraper_progress[scraper_name] = progress_state
        
        # Update display only if this scraper is selected
        if should_update_display:
            self.progress_bar['value'] = final_percent
            self.progress_percent.config(text=f"{final_percent:.1f}%")
            self.progress_label.config(text=progress_desc)
    
    def append_to_log_display(self, line: str):
        """Append a line to the log display (if scraper is selected)"""
        self.log_text.config(state=tk.NORMAL)
        self.log_text.insert(tk.END, line)
        self.log_text.see(tk.END)
        self.log_text.config(state=tk.DISABLED)
    
    def append_to_log_if_selected(self, line: str, scraper_name: str):
        """Append a line to the log display only if this scraper is currently selected"""
        if scraper_name == self.scraper_var.get():
            self.append_to_log_display(line)

    def _get_lock_paths(self, scraper_name: str):
        try:
            from platform_config import get_path_manager
            pm = get_path_manager()
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

        return True, pid, log_path, lock_file

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

    def _find_latest_external_log(self, scraper_name: str) -> Optional[Path]:
        candidates = []
        # Telegram bot logs
        try:
            from platform_config import get_path_manager
            pm = get_path_manager()
            logs_dir = pm.get_logs_dir()
        except Exception:
            logs_dir = self.repo_root / "logs"
        telegram_dir = logs_dir / "telegram"
        if telegram_dir.exists():
            candidates.extend(list(telegram_dir.glob(f"{scraper_name}_pipeline_*.log")))

        # Output logs
        try:
            from platform_config import get_path_manager
            pm = get_path_manager()
            output_dir = pm.get_output_dir(scraper_name)
        except Exception:
            output_dir = self.repo_root / "output" / scraper_name
        if output_dir.exists():
            candidates.extend(list(output_dir.glob("*.log")))

        # Scraper-specific logs (live or automatically saved)
        scraper_logs_dir = self._get_scraper_logs_dir(scraper_name)
        archive_dir = self._get_scraper_archive_dir(scraper_name)
        if scraper_logs_dir.exists():
            for log_path in scraper_logs_dir.rglob("*.log"):
                if archive_dir in log_path.parents:
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
                from platform_config import get_path_manager
                pm = get_path_manager()
                for scraper_name in self.scrapers.keys():
                    lock_file = pm.get_lock_file(scraper_name)
                    if lock_file.exists():
                        # Check if lock is stale
                        try:
                            with open(lock_file, 'r') as f:
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
                # Check all scrapers for stale locks
                for scraper_name in self.scrapers.keys():
                    # Skip if scraper is actually running from GUI
                    if scraper_name in self.running_scrapers:
                        continue
                    
                    try:
                        from platform_config import get_path_manager
                        pm = get_path_manager()
                        lock_file = pm.get_lock_file(scraper_name)
                        
                        if lock_file.exists():
                            # Check if lock is stale
                            try:
                                with open(lock_file, 'r') as f:
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
                                                    # Refresh button state if this is the selected scraper
                                                    if scraper_name == self.scraper_var.get():
                                                        self.root.after(0, self.refresh_run_button_state)
                                                except:
                                                    pass
                            except:
                                # If we can't read the lock file, it might be corrupted - try to remove it
                                try:
                                    lock_file.unlink()
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

    def start_periodic_telegram_status(self):
        """Start a periodic task to refresh Telegram bot status"""
        def periodic_check():
            try:
                self.root.after(0, self.refresh_telegram_status)
            except:
                pass
            self.root.after(5000, periodic_check)
        self.root.after(5000, periodic_check)

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

    def refresh_telegram_status(self):
        """Refresh Telegram bot status label and button states"""
        if not hasattr(self, "telegram_status_label"):
            return

        running = self.telegram_process is not None and self.telegram_process.poll() is None
        if running:
            pid = self.telegram_process.pid
            self.telegram_status_label.config(text=f"Status: Running (PID {pid})")
            self.start_telegram_button.config(state=tk.DISABLED)
            self.stop_telegram_button.config(state=tk.NORMAL)
            # Update status icon to green
            if hasattr(self, "telegram_status_icon"):
                self.telegram_status_icon.config(fg='#28a745')
        else:
            if self.telegram_process is not None and self.telegram_process.poll() is not None:
                self.telegram_process = None
            self.telegram_status_label.config(text="Status: Stopped")
            self.start_telegram_button.config(state=tk.NORMAL)
            self.stop_telegram_button.config(state=tk.DISABLED)
            # Update status icon to red
            if hasattr(self, "telegram_status_icon"):
                self.telegram_status_icon.config(fg='#dc3545')

        if self.telegram_log_path:
            log_name = self.telegram_log_path.name if hasattr(self.telegram_log_path, "name") else str(self.telegram_log_path)
            self.telegram_log_label.config(text=f"Log: {log_name}")
        else:
            self.telegram_log_label.config(text="Log: (none)")

    def start_telegram_bot(self):
        """Start Telegram bot process"""
        if self.telegram_process is not None and self.telegram_process.poll() is None:
            messagebox.showinfo("Information", "Telegram bot is already running.")
            return

        script_path = self.repo_root / "telegram_bot.py"
        if not script_path.exists():
            messagebox.showerror("Error", f"Telegram bot script not found:\n{script_path}")
            return

        token = os.getenv("TELEGRAM_BOT_TOKEN") or self._get_env_value_from_dotenv("TELEGRAM_BOT_TOKEN")
        if not token:
            messagebox.showerror("Error", "Missing TELEGRAM_BOT_TOKEN in .env or environment.")
            return

        try:
            from platform_config import get_path_manager
            pm = get_path_manager()
            logs_dir = pm.get_logs_dir()
        except Exception:
            logs_dir = self.repo_root / "logs"

        telegram_dir = logs_dir / "telegram"
        telegram_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = telegram_dir / f"telegram_bot_{timestamp}.log"

        try:
            log_handle = open(log_path, "a", encoding="utf-8", errors="replace")
        except Exception as e:
            messagebox.showerror("Error", f"Could not open log file:\n{e}")
            return

        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        if "TELEGRAM_BOT_TOKEN" not in env:
            env["TELEGRAM_BOT_TOKEN"] = token

        creation_flags = 0
        if hasattr(subprocess, "CREATE_NO_WINDOW"):
            creation_flags = subprocess.CREATE_NO_WINDOW

        try:
            self.telegram_process = subprocess.Popen(
                [sys.executable, "-u", str(script_path)],
                cwd=str(self.repo_root),
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                env=env,
                creationflags=creation_flags
            )
            self.telegram_log_path = log_path
        except Exception as e:
            messagebox.showerror("Error", f"Failed to start Telegram bot:\n{e}")
            try:
                log_handle.close()
            except Exception:
                pass
            self.telegram_process = None
            self.telegram_log_path = None
            return

        self.update_status("Telegram bot started")
        self.refresh_telegram_status()

    def stop_telegram_bot(self):
        """Stop Telegram bot process"""
        if self.telegram_process is None or self.telegram_process.poll() is not None:
            messagebox.showinfo("Information", "Telegram bot is not running.")
            self.telegram_process = None
            self.refresh_telegram_status()
            return

        try:
            self.telegram_process.terminate()
            self.telegram_process.wait(timeout=5)
        except Exception:
            try:
                self.telegram_process.kill()
            except Exception:
                pass
        finally:
            self.telegram_process = None

        self.update_status("Telegram bot stopped")
        self.refresh_telegram_status()
    
    def finish_scraper_run(self, scraper_name: str, return_code: int, stopped: bool = False):
        """Finish scraper run and update display if selected"""
        # Ensure scraper is removed from running sets
        self.running_scrapers.discard(scraper_name)
        # Update kill all Chrome button state
        self.update_kill_all_chrome_button_state()
        
        # Final cleanup of any remaining lock files (safety net with retries)
        import time
        max_retries = 5
        for attempt in range(max_retries):
            try:
                from platform_config import get_path_manager
                pm = get_path_manager()
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
                from platform_config import get_path_manager
                pm = get_path_manager()
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
    
    def run_full_pipeline(self, resume=True):
        """Run the full pipeline for selected scraper with resume/checkpoint support"""
        # Check if THIS scraper is already running (not other scrapers)
        scraper_name = self.scraper_var.get()
        if not scraper_name:
            messagebox.showwarning("Warning", "Select a scraper first")
            return

        # Check if this specific scraper is running from GUI
        if scraper_name in self.running_scrapers:
            messagebox.showwarning("Warning", f"{scraper_name} is already running. Wait for completion.")
            return
        
        # Check if lock file exists (scraper might be running from outside GUI)
        try:
            from platform_config import get_path_manager
            pm = get_path_manager()
            lock_file = pm.get_lock_file(scraper_name)
            if lock_file.exists():
                messagebox.showwarning("Warning", f"{scraper_name} is already running (lock file exists).\n\nUse 'Clear Run Lock' if you're sure it's not running.")
                return
        except Exception:
            pass  # Continue if check fails
        
        scraper_info = self.scrapers[scraper_name]
        
        # Try resume script first
        resume_script = scraper_info["path"] / "run_pipeline_resume.py"
        
        if resume_script.exists():
            # Use resume script with resume/fresh flag
            mode = "resume" if resume else "fresh"
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
                        if info['next_step'] >= total_steps if total_steps else False:
                            msg += " (pipeline completed)"
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
            self.run_script_in_thread(resume_script, scraper_info["path"], is_pipeline=True, extra_args=extra_args)
        else:
            # Fallback to workflow script or batch file
            workflow_script = scraper_info["path"] / "run_workflow.py"
            
            if workflow_script.exists():
                # Use new unified workflow
                if not messagebox.askyesno("Confirm", f"Run full pipeline for {scraper_name}?\n\nThis will:\n- Create a backup first\n- Run all steps\n- Organize outputs in run folder"):
                    return
                
                self.run_script_in_thread(workflow_script, scraper_info["path"], is_pipeline=True, extra_args=[])
            else:
                # Fallback to old batch file
                pipeline_bat = scraper_info["path"] / scraper_info["pipeline_bat"]
                
                if not pipeline_bat.exists():
                    messagebox.showerror("Error", f"Pipeline script not found:\n{pipeline_bat}")
                    return
                
                # Confirm
                if not messagebox.askyesno("Confirm", f"Run full pipeline for {scraper_name}?"):
                    return
                
                self.run_script_in_thread(pipeline_bat, scraper_info["path"], is_pipeline=True, extra_args=[])
    
    
    def run_script_in_thread(self, script_path, working_dir, is_pipeline=False, extra_args=None):
        """Run script in a separate thread"""
        if extra_args is None:
            extra_args = []
        
        # Set running state and disable run button for this scraper only
        scraper_name = self.scraper_var.get()
        self.running_scrapers.add(scraper_name)
        self._last_completed_logs.pop(scraper_name, None)
        # Update kill all Chrome button state (disable it)
        self.update_kill_all_chrome_button_state()
        
        # Clear old logs when starting pipeline (only clear console, keep storage for archive)
        if is_pipeline:
            # Clear console display for this scraper when starting
            if scraper_name == self.scraper_var.get():
                self.clear_logs(scraper_name, silent=True, clear_storage=False)  # Clear console but keep storage for now
            # Clear log storage for fresh pipeline run
            self.scraper_logs[scraper_name] = ""
        
        # Initialize log storage for this scraper if not exists
        if scraper_name not in self.scraper_logs:
            self.scraper_logs[scraper_name] = ""
        
        # Disable run button and enable stop button only for the currently selected scraper
        current_scraper = self.scraper_var.get()
        if current_scraper == scraper_name:
            self.run_button.config(state=tk.DISABLED, text="⏸ Running...")
            self.stop_button.config(state=tk.NORMAL)
        self.update_status(f"Running {scraper_name}...")
        
        def run():
            try:
                # Initialize log for this scraper
                log_header = f"Starting execution at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                log_header += f"Scraper: {scraper_name}\n"
                log_header += f"Script: {script_path}\n"
                log_header += f"Working Directory: {working_dir}\n"
                if extra_args:
                    log_header += f"Extra Arguments: {' '.join(extra_args)}\n"
                log_header += "=" * 80 + "\n\n"
                
                self.scraper_logs[scraper_name] = log_header
                
                # Initialize progress state
                self.scraper_progress[scraper_name] = {"percent": 0, "description": f"Starting {scraper_name}..."}
                
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
                
                # Run script
                if script_path.suffix == ".bat":
                    # Run batch file
                    cmd = ["cmd", "/c", str(script_path)] + extra_args
                    process = subprocess.Popen(
                        cmd,
                        cwd=str(working_dir),
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        text=True,
                        bufsize=1,
                        universal_newlines=True
                    )
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
                    process = subprocess.Popen(
                        cmd,
                        cwd=str(working_dir),
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        text=True,
                        bufsize=1,
                        universal_newlines=True
                    )

                # Create lock file for pipeline runs using the child process PID
                if is_pipeline:
                    try:
                        from platform_config import get_path_manager
                        pm = get_path_manager()
                        lock_file = pm.get_lock_file(scraper_name)
                        # Ensure lock file directory exists
                        lock_file.parent.mkdir(parents=True, exist_ok=True)
                        with open(lock_file, 'w') as f:
                            f.write(f"{process.pid}\n{datetime.now().isoformat()}\n")
                        # Store lock file path for cleanup
                        self._pipeline_lock_files[scraper_name] = lock_file
                    except Exception as e:
                        # If lock file creation fails, log but continue
                        print(f"Warning: Could not create lock file: {e}")
                
                self.running_processes[scraper_name] = process
                
                # Read output in real-time using non-blocking approach
                output_queue = queue.Queue()
                
                def read_output():
                    try:
                        for line in iter(process.stdout.readline, ''):
                            if line:
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
                            # Append to scraper's log
                            self.scraper_logs[scraper_name] += data
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
                            # Process finished, check for remaining output
                            try:
                                remaining = process.stdout.read()
                                if remaining:
                                    self.scraper_logs[scraper_name] += remaining
                                    rem_data = remaining
                                    self.root.after(0, lambda r=rem_data, sn=scraper_name: self.append_to_log_if_selected(r, sn))
                            except:
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
                                from platform_config import get_path_manager
                                pm = get_path_manager()
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
                        from platform_config import get_path_manager
                        pm = get_path_manager()
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

                # Update display if this scraper is selected
                # Schedule finish_scraper_run on GUI thread
                self.root.after(0, lambda sn=scraper_name, rc=return_code, stopped=was_stopped: self.finish_scraper_run(sn, rc, stopped))

            except Exception as e:
                error_msg = f"\nError: {str(e)}\n"
                self.scraper_logs[scraper_name] += error_msg
                error_str = str(e)
                self.root.after(0, lambda sn=scraper_name, err=error_str: self.handle_scraper_error(sn, err))
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
                                from platform_config import get_path_manager
                                pm = get_path_manager()
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
                                from platform_config import get_path_manager
                                pm = get_path_manager()
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
                
                # Refresh button state after cleanup (lock should be released by workflow runner)
                self.root.after(0, lambda: self.refresh_run_button_state())
        
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
    
    def stop_pipeline(self):
        """Stop the running pipeline for the currently selected scraper"""
        scraper_name = self.scraper_var.get()
        if not scraper_name:
            messagebox.showwarning("Warning", "Select a scraper first")
            return
        
        stop_confirmed = False

        # Prevent multiple simultaneous stop attempts for the same scraper
        if scraper_name in self._stopping_scrapers:
            return  # Already stopping this scraper, ignore duplicate request
        
        # Mark as stopping
        self._stopping_scrapers.add(scraper_name)
        
        try:
            # First, try to stop the process tracked by GUI (if running from GUI)
            if scraper_name in self.running_processes:
                process = self.running_processes[scraper_name]
                if process and process.poll() is None:  # Process is still running
                    # Confirm stop
                    if not messagebox.askyesno("Confirm Stop", f"Stop {scraper_name} pipeline?\n\nThis will terminate the running process."):
                        return
                    stop_confirmed = True
                
                self.update_status(f"Stopping {scraper_name}...")

                try:
                    # IMPORTANT: Clean up Chrome instances FIRST (scraper-specific) BEFORE killing main process
                    # This prevents killing Chrome instances that belong to other scrapers
                    try:
                        from core.chrome_pid_tracker import terminate_scraper_pids
                        terminated_count = terminate_scraper_pids(scraper_name, self.repo_root, silent=True)
                        if terminated_count > 0:
                            self.append_to_log_display(f"[STOP] Terminated {terminated_count} Chrome process(es) for {scraper_name} before process kill\n")
                    except Exception:
                        pass
                    
                    # Step 2: Terminate the process tree (pipeline + child Python processes)
                    # Note: /T kills all child processes, so we clean up Chrome instances first above
                    import time
                    if sys.platform == "win32":
                        try:
                            subprocess.run(
                                ["taskkill", "/F", "/T", "/PID", str(process.pid)],
                                capture_output=True,
                                text=True,
                                timeout=10
                            )
                        except Exception:
                            # Fallback to direct terminate if taskkill fails
                            process.terminate()
                    else:
                        process.terminate()

                    # Wait a bit for shutdown
                    time.sleep(1)
                    if process.poll() is None:
                        # Force kill if still running
                        process.kill()
                        time.sleep(0.5)  # Give it time to die
                    
                    # Step 3: Final cleanup - check for any remaining Chrome instances (in case some were missed)
                    try:
                        from core.chrome_pid_tracker import terminate_scraper_pids
                        terminated_count = terminate_scraper_pids(scraper_name, self.repo_root, silent=True)
                        if terminated_count > 0:
                            self.append_to_log_display(f"[STOP] Terminated {terminated_count} remaining Chrome process(es) for {scraper_name} after process kill\n")
                    except Exception:
                        pass
                    
                    # Clean up lock file if it exists
                    try:
                        from platform_config import get_path_manager
                        pm = get_path_manager()
                        lock_file = pm.get_lock_file(scraper_name)
                        if lock_file.exists():
                            lock_file.unlink()
                    except:
                        pass
                    
                    # Also check old lock location
                    try:
                        old_lock = self.repo_root / f".{scraper_name}_run.lock"
                        if old_lock.exists():
                            old_lock.unlink()
                    except:
                        pass
                    
                    # Mark as stopped by user
                    self._stopped_by_user.add(scraper_name)
                    
                    # Remove from tracking
                    del self.running_processes[scraper_name]
                    self.running_scrapers.discard(scraper_name)
                    # Update kill all Chrome button state
                    self.update_kill_all_chrome_button_state()
                    
                    # Clean up pipeline lock file if created by GUI
                    if scraper_name in self._pipeline_lock_files:
                        try:
                            lock_file = self._pipeline_lock_files[scraper_name]
                            if lock_file and lock_file.exists():
                                lock_file.unlink()
                            del self._pipeline_lock_files[scraper_name]
                        except:
                            pass
                    
                    # Archive log but DON'T clear console (user wants to see logs after stop)
                    stop_msg = f"\n{'='*80}\n[STOPPED] Pipeline stopped by user at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'='*80}\n"
                    # Save log to archive but don't clear console display
                    self.archive_log_without_clearing(scraper_name, footer=stop_msg)
                    # Update progress state
                    self.scraper_progress[scraper_name] = {"percent": 0, "description": "Pipeline stopped"}
                    
                    if scraper_name == self.scraper_var.get():
                        self.append_to_log_display(stop_msg)
                        # Reset progress bar display
                        self.progress_label.config(text="Pipeline stopped")
                        self.progress_bar['value'] = 0
                        self.progress_percent.config(text="0%")
                    
                    # Refresh button state
                    self.refresh_run_button_state()
                    self.update_status(f"Stopped {scraper_name}")
                    messagebox.showinfo("Success", f"Stopped {scraper_name} pipeline")
                    return
                except Exception as e:
                    messagebox.showerror("Error", f"Failed to stop {scraper_name}:\n{str(e)}")
                    self.update_status(f"Error stopping {scraper_name}: {str(e)}")
                    return

            # If not tracked by GUI, try to stop via lock file (external process)
            lock_file = None
            old_lock_file = None
            try:
                from platform_config import get_path_manager
                pm = get_path_manager()
                lock_file = pm.get_lock_file(scraper_name)
                # Also check old location as fallback
                old_lock_file = self.repo_root / f".{scraper_name}_run.lock"
            except Exception:
                old_lock_file = self.repo_root / f".{scraper_name}_run.lock"
            
            # Use the lock file that exists, or prefer new location
            if lock_file and not lock_file.exists() and old_lock_file and old_lock_file.exists():
                lock_file = old_lock_file
            elif not lock_file:
                lock_file = old_lock_file

            if (not lock_file or not lock_file.exists()) and scraper_name not in self.running_scrapers:
                messagebox.showinfo("Information", f"{scraper_name} is not currently running.")
                return

            # Confirm stop
            if not messagebox.askyesno("Confirm Stop", f"Stop {scraper_name} pipeline?\n\nThis will terminate the running process."):
                return
            stop_confirmed = True

            self.update_status(f"Stopping {scraper_name}...")

            # Try to stop via shared workflow runner
            try:
                from shared_workflow_runner import WorkflowRunner
                result = WorkflowRunner.stop_pipeline(scraper_name, self.repo_root)

                # Note: shared_workflow_runner.stop_pipeline already cleans up Chrome instances before killing process
                # This is just a final check for any remaining instances
                import time
                time.sleep(0.5)
                try:
                    from core.chrome_pid_tracker import terminate_scraper_pids
                    terminated_count = terminate_scraper_pids(scraper_name, self.repo_root, silent=True)
                    if terminated_count > 0:
                        self.append_to_log_display(f"[STOP] Terminated {terminated_count} remaining Chrome process(es) for {scraper_name} after pipeline stop\n")
                except Exception:
                    pass

                if result["status"] == "ok":
                    messagebox.showinfo("Success", result["message"])
                    self.update_status(f"Stopped {scraper_name}")

                    # Remove from tracking
                    if scraper_name in self.running_processes:
                        del self.running_processes[scraper_name]
                    self.running_scrapers.discard(scraper_name)
                    # Update kill all Chrome button state
                    self.update_kill_all_chrome_button_state()

                    # Update log
                    stop_msg = f"\n{'='*80}\n[STOPPED] Pipeline stopped by user at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'='*80}\n"
                    if scraper_name in self.scraper_logs:
                        self.scraper_logs[scraper_name] += stop_msg
                    # Update progress state
                    self.scraper_progress[scraper_name] = {"percent": 0, "description": "Pipeline stopped"}
                    
                    if scraper_name == self.scraper_var.get():
                        self.append_to_log_display(stop_msg)
                        # Reset progress bar display
                        self.progress_label.config(text="Pipeline stopped")
                        self.progress_bar['value'] = 0
                        self.progress_percent.config(text="0%")

                        # Refresh button state
                        self.refresh_run_button_state()
                        self.update_checkpoint_status()
                        self.update_kill_all_chrome_button_state()
                else:
                    messagebox.showerror("Error", f"Failed to stop {scraper_name}:\n{result['message']}")
                    self.update_status(f"Failed to stop {scraper_name}")
            except Exception as e:
                # Ensure Chrome cleanup happens even if there's an error (scraper-specific only)
                try:
                    from core.chrome_pid_tracker import terminate_scraper_pids
                    terminate_scraper_pids(scraper_name, self.repo_root, silent=True)
                except Exception:
                    # Don't use general cleanup - it would kill all scrapers' Chrome instances
                    pass
                messagebox.showerror("Error", f"Failed to stop {scraper_name}:\n{str(e)}")
                self.update_status(f"Error stopping {scraper_name}: {str(e)}")
        finally:
            # Final cleanup: Ensure all Chrome instances are killed (one last attempt)
            if stop_confirmed:
                try:
                    import time
                    time.sleep(0.5)  # Give processes time to die
                    from core.chrome_pid_tracker import terminate_scraper_pids
                    terminated_count = terminate_scraper_pids(scraper_name, self.repo_root, silent=True)
                    if terminated_count > 0:
                        if scraper_name == self.scraper_var.get():
                            self.append_to_log_display(f"[STOP] Final cleanup: Terminated {terminated_count} remaining Chrome process(es)\n")
                except Exception:
                    pass
            
            # Always remove from stopping set, even if there was an error or early return
            self._stopping_scrapers.discard(scraper_name)

    def update_checkpoint_status(self):
        """Update checkpoint status label"""
        scraper_name = self.scraper_var.get()
        if not scraper_name:
            if hasattr(self, 'checkpoint_status_label'):
                self.checkpoint_status_label.config(text="Checkpoint: No scraper selected")
            return
        
        try:
            from core.pipeline_checkpoint import get_checkpoint_manager
            cp = get_checkpoint_manager(scraper_name)
            info = cp.get_checkpoint_info()
            
            if info["total_completed"] > 0:
                # Get total steps for this scraper
                scraper_info = self.scrapers.get(scraper_name)
                total_steps = len(scraper_info.get("steps", [])) if scraper_info else None
                
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
            cp.clear_checkpoint()
            messagebox.showinfo("Success", f"Checkpoint cleared for {scraper_name}")
            self.update_checkpoint_status()
        except Exception as e:
            messagebox.showerror("Error", f"Failed to clear checkpoint:\n{e}")
    
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
                    tracked_pids.update(load_chrome_pids(scraper_name, self.repo_root))
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
            for idx, step in enumerate(steps):
                var = tk.BooleanVar(value=(idx in completed_steps))
                step_vars[idx] = var
                
                step_frame = ttk.Frame(scrollable_frame)
                step_frame.pack(fill=tk.X, padx=5, pady=2)
                
                checkbox = ttk.Checkbutton(step_frame, text=f"Step {idx}: {step['name']}", 
                                           variable=var,
                                           command=lambda i=idx, v=var: on_checkbox_change(i, v))
                checkbox.pack(side=tk.LEFT, padx=5)
                
                # Show status
                status_text = "✓ Complete" if idx in completed_steps else "○ Pending"
                status_label = ttk.Label(step_frame, text=status_text, 
                                         font=("Segoe UI", 8), foreground="#666666")
                status_label.pack(side=tk.LEFT, padx=10)
            
            canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            
            # Button frame
            button_frame = ttk.Frame(main_frame)
            button_frame.pack(fill=tk.X, pady=(10, 0))
            
            def apply_changes():
                """Apply checkpoint changes"""
                try:
                    # Get selected steps
                    selected_steps = [step_num for step_num, var in step_vars.items() if var.get()]
                    
                    # Validate sequential selection (no gaps)
                    if selected_steps:
                        # Check if steps are sequential (0, 1, 2, ... with no gaps)
                        expected_steps = list(range(len(selected_steps)))
                        if selected_steps != expected_steps:
                            messagebox.showerror("Invalid Selection", 
                                f"Cannot skip steps!\n\n"
                                f"Steps must be marked sequentially from the beginning.\n"
                                f"Selected steps: {selected_steps}\n"
                                f"Expected: {expected_steps}\n\n"
                                f"Please uncheck steps from the end to roll back, or check steps sequentially from the beginning.")
                            return
                    
                    # Clear checkpoint first
                    cp.clear_checkpoint()
                    
                    # Mark selected steps as complete
                    for step_num in selected_steps:
                        if step_num < len(steps):
                            step = steps[step_num]
                            cp.mark_step_complete(step_num, step['name'])
                    
                    # Update checkpoint status in main window
                    self.update_checkpoint_status()
                    
                    # Close dialog without showing success messagebox
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
            from platform_config import get_path_manager
            pm = get_path_manager()
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
                    with open(lock_file, 'r') as f:
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
                                with open(lock_file, 'r+') as f:
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
            from platform_config import get_path_manager
            pm = get_path_manager()
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
                from platform_config import get_path_manager
                pm = get_path_manager()
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
            from platform_config import get_path_manager
            pm = get_path_manager()
            correct_output_dir = pm.get_output_dir(scraper_name)
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
            "CanadaQuebec": ["canadaquebecreport"],
            "CanadaOntario": ["canadaontarioreport"],
            "Malaysia": ["malaysia"],
            "Argentina": ["alfabeta_report"],
            "NorthMacedonia": ["north_macedonia_drug_register", "maxprices_output"],
            "Russia": ["russia_ved_report", "russia_excluded_report"],
            "Tender_Chile": ["final_tender_data"],
            "India": ["medicine_details", "details", "search_results", "ceiling_prices", "scraping_report"],
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
            "CanadaQuebec": ["canadaquebecreport"],
            "CanadaOntario": ["canadaontarioreport"],
            "Malaysia": ["malaysia"],
            "Argentina": ["alfabeta_report"],
            "NorthMacedonia": ["north_macedonia_drug_register", "maxprices_output"],
            "Russia": ["russia_ved_report", "russia_excluded_report"],
            "Tender_Chile": ["final_tender_data"],
            "India": ["medicine_details", "details", "search_results", "ceiling_prices", "scraping_report"],
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
                    "CanadaQuebec": "canada_quebec_reports",
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
        self.refresh_output_files()
        self.refresh_final_output_files()
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
    root = tk.Tk()
    app = ScraperGUI(root)
    root.mainloop()


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
