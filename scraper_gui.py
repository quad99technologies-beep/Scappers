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
from pathlib import Path
import webbrowser
from datetime import datetime
import json
import time

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
        
        # Open in fullscreen/maximized window
        if sys.platform == "win32":
            self.root.state('zoomed')  # Maximized on Windows
        else:
            # On Unix-like systems, use screen dimensions
            self.root.attributes('-zoomed', True)  # Fullscreen on Linux
        
        self.root.minsize(1000, 700)
        
        # Get repository root
        self.repo_root = Path(__file__).resolve().parent
        
        # Current scraper and step
        self.current_scraper = None
        self.current_step = None
        self.running_processes = {}  # Track processes per scraper: {scraper_name: process}
        self.running_scrapers = set()  # Track which scrapers are running from GUI
        self.scraper_logs = {}  # Store logs per scraper: {scraper_name: log_text}
        
        # Step explanations cache (key: script_path, value: explanation text)
        self.step_explanations = {}
        # Explanation cache file
        self.explanation_cache_file = self.repo_root / ".step_explanations_cache.json"
        self.load_explanation_cache()
        
        # Define scrapers and their steps
        self.scrapers = {
            "CanadaQuebec": {
                "path": self.repo_root / "1. CanadaQuebec",
                "scripts_dir": "Script",
                "docs_dir": "doc",
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
                "path": self.repo_root / "2. Malaysia",
                "scripts_dir": "scripts",
                "docs_dir": "docs",
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
                "path": self.repo_root / "3. Argentina",
                "scripts_dir": "script",
                "docs_dir": "doc",
                "steps": [
                    {"name": "01 - Get Company List", "script": "01_getCompanyList.py", "desc": "Extract company list from AlfaBeta"},
                    {"name": "02 - Get Product List", "script": "02_getProdList.py", "desc": "Extract product list for each company"},
                    {"name": "03 - Scrape Products", "script": "03_alfabeta_scraper_labs.py", "desc": "Scrape product details (supports --max-rows)"},
                    {"name": "04 - Translate Using Dictionary", "script": "04_TranslateUsingDictionary.py", "desc": "Translate Spanish to English"},
                    {"name": "05 - Generate Output", "script": "05_GenerateOutput.py", "desc": "Generate final output report"},
                    {"name": "06 - PCID Missing", "script": "06_PCIDmissing.py", "desc": "Process missing PCIDs"},
                ],
                "pipeline_bat": "run_pipeline.bat"
            }
        }
        
        self.setup_ui()
        self.load_documentation()
        # Load first documentation if available (after UI is set up)
        self.root.after(100, self.load_first_documentation)
    
    def load_first_documentation(self):
        """Load first available documentation file"""
        if hasattr(self, 'docs') and self.docs and hasattr(self, 'doc_var'):
            first_doc = sorted(self.docs.keys())[0]
            self.doc_var.set(first_doc)
            self.on_doc_selected()
        
    def setup_ui(self):
        """Setup the user interface"""
        # Create main container with horizontal split
        main_container = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        main_container.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Left panel - Execution (scraper selection, run controls, logs)
        left_panel = ttk.Frame(main_container)
        main_container.add(left_panel, weight=2)
        
        # Right panel - Documentation (read-only, formatted)
        right_panel = ttk.Frame(main_container)
        main_container.add(right_panel, weight=1)
        
        # Setup left panel (execution)
        self.setup_left_panel(left_panel)
        
        # Setup right panel (documentation)
        self.setup_right_panel(right_panel)
        
    def setup_left_panel(self, parent):
        """Setup left panel with execution controls and logs side by side"""
        # Create horizontal split for execution and logs
        exec_split = ttk.PanedWindow(parent, orient=tk.HORIZONTAL)
        exec_split.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Left side - Execution controls
        exec_controls_frame = ttk.Frame(exec_split)
        exec_split.add(exec_controls_frame, weight=1)
        self.setup_execution_tab(exec_controls_frame)
        
        # Right side - Execution logs
        logs_frame = ttk.Frame(exec_split)
        exec_split.add(logs_frame, weight=1)
        self.setup_logs_tab(logs_frame)
    
    def setup_execution_tab(self, parent):
        """Setup execution control panel"""
        # Scraper selection frame
        scraper_frame = ttk.LabelFrame(parent, text="Select Scraper", padding=10)
        scraper_frame.pack(fill=tk.X, padx=5, pady=5)
        
        self.scraper_var = tk.StringVar()
        scraper_combo = ttk.Combobox(scraper_frame, textvariable=self.scraper_var, 
                                     values=list(self.scrapers.keys()), state="readonly", width=25)
        scraper_combo.pack(fill=tk.X, pady=5)
        scraper_combo.bind("<<ComboboxSelected>>", self.on_scraper_selected)
        
        # Pipeline control frame
        pipeline_frame = ttk.LabelFrame(parent, text="Pipeline Control", padding=10)
        pipeline_frame.pack(fill=tk.X, padx=5, pady=5)

        self.run_button = ttk.Button(pipeline_frame, text="Run Full Pipeline",
                  command=self.run_full_pipeline, width=25, state=tk.NORMAL)
        self.run_button.pack(pady=5)

        self.stop_button = ttk.Button(pipeline_frame, text="Stop Pipeline",
                  command=self.stop_pipeline, width=25, state=tk.DISABLED)
        self.stop_button.pack(pady=5)

        ttk.Button(pipeline_frame, text="Clear Run Lock",
                  command=self.clear_run_lock, width=25).pack(pady=5)
        
        # Steps frame (read-only view)
        steps_frame = ttk.LabelFrame(parent, text="Pipeline Steps", padding=10)
        steps_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Steps listbox with scrollbar (read-only)
        steps_container = ttk.Frame(steps_frame)
        steps_container.pack(fill=tk.BOTH, expand=True)

        scrollbar = ttk.Scrollbar(steps_container)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.steps_listbox = tk.Listbox(steps_container, yscrollcommand=scrollbar.set,
                                        height=15, font=("Segoe UI", 9))
        self.steps_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.steps_listbox.yview)

        self.steps_listbox.bind("<<ListboxSelect>>", self.on_step_selected)

        # Step info frame
        self.step_info_frame = ttk.LabelFrame(parent, text="Step Information", padding=10)
        self.step_info_frame.pack(fill=tk.X, padx=5, pady=5)

        # Step info text (basic info)
        self.step_info_text = tk.Text(self.step_info_frame, height=4, wrap=tk.WORD,
                                     font=("Segoe UI", 9), state=tk.DISABLED)
        self.step_info_text.pack(fill=tk.BOTH, expand=True)
        
        # Explain button frame
        self.explain_button_frame = ttk.Frame(self.step_info_frame)
        self.explain_button_frame.pack(fill=tk.X, pady=(5, 0))
        
        self.explain_button = ttk.Button(self.explain_button_frame, text="Explain This Step",
                                         command=self.explain_step, state=tk.DISABLED)
        self.explain_button.pack(side=tk.LEFT, padx=5)

        # Explanation panel (initially hidden, expandable)
        # Store parent for later packing
        self.exec_controls_parent = parent
        self.explanation_frame = ttk.LabelFrame(parent, text="Step Explanation", padding=10)
        # Don't pack initially - will pack when explanation is shown

        self.explanation_text = scrolledtext.ScrolledText(
            self.explanation_frame,
            wrap=tk.WORD,
            font=("Segoe UI", 9),
            state=tk.DISABLED,
            height=8  # Initial height, will expand
        )
        self.explanation_text.pack(fill=tk.BOTH, expand=True)
        
        self.explanation_visible = False
        
    def setup_right_panel(self, parent):
        """Setup right panel with tabs for documentation and output files"""
        # Create notebook for right panel tabs
        notebook = ttk.Notebook(parent)
        notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Final Output tab (first)
        final_output_frame = ttk.Frame(notebook)
        notebook.add(final_output_frame, text="Final Output")
        self.setup_final_output_tab(final_output_frame)

        # Configuration tab (second)
        config_frame = ttk.Frame(notebook)
        notebook.add(config_frame, text="Configuration")
        self.setup_config_tab(config_frame)

        # Output Files tab (third)
        output_frame = ttk.Frame(notebook)
        notebook.add(output_frame, text="Output Files")
        self.setup_output_tab(output_frame)

        # Documentation tab (fourth)
        doc_frame = ttk.Frame(notebook)
        notebook.add(doc_frame, text="Documentation")
        self.setup_documentation_tab(doc_frame)
    
    def setup_documentation_tab(self, parent):
        """Setup documentation viewer tab (read-only, formatted)"""
        # Documentation header
        doc_header = ttk.LabelFrame(parent, text="Documentation", padding=10)
        doc_header.pack(fill=tk.X, padx=5, pady=5)

        # Documentation selector
        doc_selector_frame = ttk.Frame(doc_header)
        doc_selector_frame.pack(fill=tk.X, pady=5)

        ttk.Label(doc_selector_frame, text="Select Document:").pack(side=tk.LEFT, padx=5)

        self.doc_var = tk.StringVar()
        self.doc_combo = ttk.Combobox(doc_selector_frame, textvariable=self.doc_var,
                                      state="readonly", width=30)
        self.doc_combo.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        self.doc_combo.bind("<<ComboboxSelected>>", self.on_doc_selected)

        ttk.Button(doc_selector_frame, text="Refresh", command=self.load_documentation).pack(side=tk.LEFT, padx=5)
        
        # Documentation viewer (read-only, formatted)
        doc_viewer_frame = ttk.Frame(parent)
        doc_viewer_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Use Text widget with better formatting for markdown/readable docs
        self.doc_text = scrolledtext.ScrolledText(
            doc_viewer_frame,
            wrap=tk.WORD,
            font=("Segoe UI", 10),  # Increased from 9
            state=tk.DISABLED,  # Read-only
            bg="#FFFFFF",
            fg="#2C3E50",
            padx=20,  # Increased from 10
            pady=15,  # Increased from 10
            spacing1=3,  # Space above paragraphs
            spacing3=8   # Space below paragraphs
        )
        self.doc_text.pack(fill=tk.BOTH, expand=True)

        # Configure text tags for professional formatting (larger, more readable)
        self.doc_text.tag_configure("heading1", font=("Segoe UI", 20, "bold"), foreground="#1A1A1A", spacing1=20, spacing3=12)
        self.doc_text.tag_configure("heading2", font=("Segoe UI", 16, "bold"), foreground="#2C3E50", spacing1=16, spacing3=10)
        self.doc_text.tag_configure("heading3", font=("Segoe UI", 14, "bold"), foreground="#34495E", spacing1=14, spacing3=8)
        self.doc_text.tag_configure("heading4", font=("Segoe UI", 12, "bold"), foreground="#34495E", spacing1=12, spacing3=6)
        self.doc_text.tag_configure("heading5", font=("Segoe UI", 11, "bold"), foreground="#5D6D7E", spacing1=10, spacing3=5)
        self.doc_text.tag_configure("heading6", font=("Segoe UI", 10, "bold"), foreground="#5D6D7E", spacing1=8, spacing3=4)
        self.doc_text.tag_configure("code", font=("Consolas", 10), background="#F8F9FA", foreground="#2C3E50",
                                    relief=tk.SOLID, borderwidth=1, lmargin1=20, lmargin2=20, rmargin=20,
                                    spacing1=8, spacing3=8)
        self.doc_text.tag_configure("code_inline", font=("Consolas", 10), background="#ECF0F1", foreground="#C7254E",
                                    relief=tk.FLAT)
        self.doc_text.tag_configure("bold", font=("Segoe UI", 10, "bold"), foreground="#2C3E50")
        self.doc_text.tag_configure("italic", font=("Segoe UI", 10, "italic"), foreground="#2C3E50")
        self.doc_text.tag_configure("link", foreground="#2874A6", underline=True, font=("Segoe UI", 10))
        self.doc_text.tag_configure("blockquote", foreground="#5D6D7E", lmargin1=20, lmargin2=20,
                                    background="#F4F6F7", font=("Segoe UI", 10, "italic"),
                                    spacing1=4, spacing3=4)
        self.doc_text.tag_configure("hr", background="#BDC3C7", lmargin1=0, lmargin2=0, rmargin=0)
        self.doc_text.tag_configure("list", lmargin1=20, lmargin2=40, font=("Segoe UI", 10))
        self.doc_text.tag_configure("list_item", lmargin1=20, lmargin2=40, font=("Segoe UI", 10),
                                    spacing1=2, spacing3=2)
        self.doc_text.tag_configure("list_item", lmargin1=15, lmargin2=30)
        
        
    def setup_logs_tab(self, parent):
        """Setup logs viewer panel"""
        # Toolbar
        toolbar = ttk.Frame(parent)
        toolbar.pack(fill=tk.X, padx=5, pady=5)

        ttk.Label(toolbar, text="Execution Log:").pack(side=tk.LEFT, padx=5)

        ttk.Button(toolbar, text="Clear", command=self.clear_logs).pack(side=tk.LEFT, padx=5)
        ttk.Button(toolbar, text="Copy to Clipboard", command=self.copy_logs_to_clipboard).pack(side=tk.LEFT, padx=5)
        ttk.Button(toolbar, text="Save Log", command=self.save_log).pack(side=tk.LEFT, padx=5)

        # Log viewer with terminal color scheme
        log_viewer_frame = ttk.Frame(parent)
        log_viewer_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        self.log_text = scrolledtext.ScrolledText(
            log_viewer_frame,
            wrap=tk.WORD,
            font=("Consolas", 9),
            state=tk.DISABLED,
            bg="#000000",  # Black background (terminal style)
            fg="#FFFF00",  # Yellow text (terminal style)
            insertbackground="#FFFF00",  # Yellow cursor
            selectbackground="#333333",  # Dark gray selection
            selectforeground="#FFFF00"  # Yellow selected text
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
        # Status bar (shared across all tabs)
        if not hasattr(self, 'status_bar'):
            self.status_bar = ttk.Label(self.root, text="Ready", relief=tk.SUNKEN, anchor=tk.W)
            self.status_bar.pack(side=tk.BOTTOM, fill=tk.X, padx=5, pady=2)
        
    def setup_output_tab(self, parent):
        """Setup output files viewer tab"""
        # Toolbar
        toolbar = ttk.Frame(parent)
        toolbar.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(toolbar, text="Output Directory:").pack(side=tk.LEFT, padx=5)
        
        self.output_path_var = tk.StringVar()
        output_path_entry = ttk.Entry(toolbar, textvariable=self.output_path_var, width=50)
        output_path_entry.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        
        ttk.Button(toolbar, text="Refresh", command=self.refresh_output_files).pack(side=tk.LEFT, padx=5)
        ttk.Button(toolbar, text="Open Folder", command=self.open_output_folder).pack(side=tk.LEFT, padx=5)
        
        # File list
        file_list_frame = ttk.Frame(parent)
        file_list_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Listbox with scrollbar
        list_container = ttk.Frame(file_list_frame)
        list_container.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        scrollbar = ttk.Scrollbar(list_container)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.output_listbox = tk.Listbox(list_container, yscrollcommand=scrollbar.set,
                                        font=("Segoe UI", 9))
        self.output_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.output_listbox.yview)

        self.output_listbox.bind("<Double-Button-1>", self.open_output_file)

        # File info
        file_info_frame = ttk.LabelFrame(parent, text="File Information", padding=10)
        file_info_frame.pack(fill=tk.X, padx=5, pady=5)

        self.file_info_text = tk.Text(file_info_frame, height=3, wrap=tk.WORD,
                                      font=("Segoe UI", 9), state=tk.DISABLED)
        self.file_info_text.pack(fill=tk.BOTH, expand=True)
    
    def setup_final_output_tab(self, parent):
        """Setup final output viewer tab"""
        # Toolbar
        toolbar = ttk.Frame(parent)
        toolbar.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(toolbar, text="Final Output Directory:").pack(side=tk.LEFT, padx=5)
        
        self.final_output_path_var = tk.StringVar()
        final_output_path_entry = ttk.Entry(toolbar, textvariable=self.final_output_path_var, width=50)
        final_output_path_entry.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        
        # Set default to root output folder (final reports only)
        root_output = self.repo_root / "output"
        self.final_output_path_var.set(str(root_output))
        
        ttk.Button(toolbar, text="Refresh", command=self.refresh_final_output_files).pack(side=tk.LEFT, padx=5)
        ttk.Button(toolbar, text="Open Folder", command=self.open_final_output_folder).pack(side=tk.LEFT, padx=5)
        ttk.Button(toolbar, text="Search", command=self.search_final_output).pack(side=tk.LEFT, padx=5)
        
        # File list
        file_list_frame = ttk.Frame(parent)
        file_list_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Listbox with scrollbar
        list_container = ttk.Frame(file_list_frame)
        list_container.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        scrollbar = ttk.Scrollbar(list_container)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.final_output_listbox = tk.Listbox(list_container, yscrollcommand=scrollbar.set,
                                               font=("Segoe UI", 9))
        self.final_output_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.final_output_listbox.yview)

        self.final_output_listbox.bind("<Double-Button-1>", self.open_final_output_file)

        # File preview/info
        file_info_frame = ttk.LabelFrame(parent, text="Final Output Information", padding=10)
        file_info_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        self.final_output_info_text = tk.Text(file_info_frame, wrap=tk.WORD,
                                              font=("Segoe UI", 9), state=tk.DISABLED)
        self.final_output_info_text.pack(fill=tk.BOTH, expand=True)
    
    def setup_config_tab(self, parent):
        """Setup configuration/environment editing tab"""
        # Toolbar
        toolbar = ttk.Frame(parent)
        toolbar.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(toolbar, text="Platform Configuration:").pack(side=tk.LEFT, padx=5)
        
        ttk.Button(toolbar, text="Load", command=self.load_config_file).pack(side=tk.LEFT, padx=5)
        ttk.Button(toolbar, text="Save", command=self.save_config_file).pack(side=tk.LEFT, padx=5)
        ttk.Button(toolbar, text="Open File", command=self.open_config_file).pack(side=tk.LEFT, padx=5)
        ttk.Button(toolbar, text="Create from Template", command=self.create_config_from_template).pack(side=tk.LEFT, padx=5)
        
        # Config editor
        editor_frame = ttk.Frame(parent)
        editor_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        self.config_text = scrolledtext.ScrolledText(editor_frame, wrap=tk.WORD,
                                                     font=("Consolas", 9))
        self.config_text.pack(fill=tk.BOTH, expand=True)
        
        # Status
        self.config_status = ttk.Label(parent, text="Platform .env file (shared across all scrapers)", 
                                       relief=tk.SUNKEN, anchor=tk.W)
        self.config_status.pack(fill=tk.X, padx=5, pady=5)
        
        # Store current config file path (always platform root)
        self.current_config_file = self.repo_root / ".env"
        
        # Auto-load on tab creation
        self.load_config_file()
    
    
    def load_config_file(self):
        """Load platform root .env file (shared across all scrapers)"""
        # Always use platform root .env file (where scraper_gui.py is located)
        env_file = self.repo_root / ".env"
        
        if env_file.exists():
            try:
                with open(env_file, "r", encoding="utf-8") as f:
                    content = f.read()
                self.config_text.delete(1.0, tk.END)
                self.config_text.insert(1.0, content)
                self.current_config_file = env_file
                self.config_status.config(text=f"Loaded platform .env: {env_file}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to load config file:\n{str(e)}")
                self.config_status.config(text=f"Error loading config: {str(e)}")
        else:
            # File doesn't exist, show empty editor with template
            self.config_text.delete(1.0, tk.END)
            template = "# Platform Configuration File (.env)\n"
            template += "# This file is shared across all scrapers (CanadaQuebec, Malaysia, Argentina)\n"
            template += "# Add configuration variables here\n\n"
            template += "# Example:\n"
            template += "# OPENAI_API_KEY=your_key_here\n"
            template += "# ALFABETA_USER=your_email@example.com\n"
            template += "# ALFABETA_PASS=your_password\n\n"
            self.config_text.insert(1.0, template)
            self.current_config_file = env_file
            self.config_status.config(text=f"Platform .env not found. Will create: {env_file}")
    
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
        """Create platform .env file from .env.example template"""
        # Look for .env.example in platform root
        template_file = self.repo_root / ".env.example"
        env_file = self.repo_root / ".env"
        
        if not template_file.exists():
            # Try to find any .env.example in scraper directories
            for scraper_name, scraper_info in self.scrapers.items():
                scraper_template = scraper_info["path"] / ".env.example"
                if scraper_template.exists():
                    template_file = scraper_template
                    break
        
        if not template_file.exists():
            messagebox.showwarning("Warning", f"Template file not found:\n{template_file}\n\nCreating empty config file instead.")
            self.config_text.delete(1.0, tk.END)
            template = "# Platform Configuration File (.env)\n"
            template += "# This file is shared across all scrapers\n"
            template += "# Add your configuration variables here\n\n"
            self.config_text.insert(1.0, template)
            self.current_config_file = env_file
            return
        
        try:
            with open(template_file, "r", encoding="utf-8") as f:
                template_content = f.read()
            
            # Load into editor
            self.config_text.delete(1.0, tk.END)
            self.config_text.insert(1.0, template_content)
            self.current_config_file = env_file
            self.config_status.config(text=f"Loaded template from: {template_file}")
            messagebox.showinfo("Information", f"Template loaded from:\n{template_file}\n\nUse 'Save' to create platform .env file.")
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
                                                       font=("Segoe UI", 9), state=tk.DISABLED)
        self.rest_info_text.pack(fill=tk.BOTH, expand=True)
        
        # Initialize with system info
        self.update_rest_info()
        
    def load_documentation(self):
        """Load all documentation files"""
        self.docs = {}
        
        for scraper_name, scraper_info in self.scrapers.items():
            docs_path = scraper_info["path"] / scraper_info["docs_dir"]
            if docs_path.exists():
                for doc_file in docs_path.rglob("*"):
                    if doc_file.is_file() and doc_file.suffix in [".md", ".txt", ".py"]:
                        rel_path = doc_file.relative_to(docs_path)
                        key = f"{scraper_name} - {rel_path}"
                        self.docs[key] = doc_file
        
        # Update combo box
        self.doc_combo["values"] = sorted(self.docs.keys())
        
        # Also add root-level docs if any
        for doc_file in self.repo_root.rglob("*.md"):
            if "doc" in str(doc_file) or "docs" in str(doc_file):
                key = f"Root - {doc_file.name}"
                self.docs[key] = doc_file
                if key not in self.doc_combo["values"]:
                    current_values = list(self.doc_combo["values"])
                    current_values.append(key)
                    self.doc_combo["values"] = sorted(current_values)
    
    def on_scraper_selected(self, event=None):
        """Handle scraper selection"""
        scraper_name = self.scraper_var.get()
        if not scraper_name:
            return
            
        self.current_scraper = scraper_name
        scraper_info = self.scrapers[scraper_name]
        
        # Clear explanation when scraper changes
        self.clear_explanation()
        
        # Update steps listbox
        self.steps_listbox.delete(0, tk.END)
        for step in scraper_info["steps"]:
            self.steps_listbox.insert(tk.END, step["name"])
        
        # Update output path to latest run directory
        if hasattr(self, 'output_path_var'):
            # Get latest run directory for this scraper
            try:
                from platform_config import get_path_manager
                pm = get_path_manager()
                runs_dir = pm.get_runs_dir()
                # Find latest run for this scraper
                latest_run = None
                latest_time = None
                for run_dir in runs_dir.iterdir():
                    if run_dir.is_dir() and run_dir.name.startswith(scraper_name):
                        mtime = run_dir.stat().st_mtime
                        if latest_time is None or mtime > latest_time:
                            latest_time = mtime
                            latest_run = run_dir
                if latest_run:
                    self.output_path_var.set(str(latest_run))
                else:
                    # No runs yet, show runs directory
                    self.output_path_var.set(str(runs_dir))
            except Exception:
                # Fallback to scraper output directory
                output_dir = scraper_info["path"] / "output"
                self.output_path_var.set(str(output_dir))
            
            # Refresh output files
            self.refresh_output_files()
            
            # Refresh final output files (filtered by scraper)
            if hasattr(self, 'refresh_final_output_files'):
                self.refresh_final_output_files()
        
        # Always update log display when scraper selection changes
        # This ensures the user sees the selected scraper's progress
        self.update_log_display(scraper_name)
        
        # Refresh button state
        self.refresh_run_button_state()
    
    def on_step_selected(self, event=None):
        """Handle step selection"""
        selection = self.steps_listbox.curselection()
        if not selection:
            return
            
        scraper_name = self.scraper_var.get()
        if not scraper_name:
            return
            
        step_index = selection[0]
        scraper_info = self.scrapers[scraper_name]
        step = scraper_info["steps"][step_index]
        
        self.current_step = step
        
        # Clear explanation when step changes (so user sees explanation for new step)
        self.clear_explanation()
        
        # Update step info
        self.step_info_text.config(state=tk.NORMAL)
        self.step_info_text.delete(1.0, tk.END)
        info = f"Script: {step['script']}\n"
        info += f"Description: {step['desc']}\n"
        script_path = scraper_info["path"] / scraper_info["scripts_dir"] / step["script"]
        info += f"Path: {script_path}"
        self.step_info_text.insert(1.0, info)
        self.step_info_text.config(state=tk.DISABLED)
        
        # Enable explain button if script exists
        if script_path.exists():
            self.explain_button.config(state=tk.NORMAL)
        else:
            self.explain_button.config(state=tk.DISABLED)
    
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
            messagebox.showwarning("Warning", "Select a step first")
            return
        
        scraper_name = self.scraper_var.get()
        if not scraper_name:
            return
        
        scraper_info = self.scrapers[scraper_name]
        script_path = scraper_info["path"] / scraper_info["scripts_dir"] / self.current_step["script"]
        
        if not script_path.exists():
            messagebox.showerror("Error", f"Script file not found:\n{script_path}")
            return
        
        # Disable button temporarily while fetching (will re-enable after)
        self.explain_button.config(state=tk.DISABLED, text="Generating explanation...")
        
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
            self.explain_button.config(state=tk.NORMAL, text="Explain This Step")
            return
        
        # Need to get explanation from OpenAI
        if not _OPENAI_AVAILABLE:
            messagebox.showerror("Error", "OpenAI library not available. Install using: pip install openai")
            self.explain_button.config(state=tk.NORMAL, text="Explain This Step")
            return
        
        # Get OpenAI API key
        api_key = os.getenv("OPENAI_API_KEY", "")
        if not api_key:
            # Try to load from .env file
            try:
                from dotenv import load_dotenv
                env_file = self.repo_root / ".env"
                if env_file.exists():
                    load_dotenv(env_file)
                api_key = os.getenv("OPENAI_API_KEY", "")
            except ImportError:
                pass
        
        if not api_key:
            messagebox.showerror("Error", "OPENAI_API_KEY not found. Configure it in environment or .env file.")
            return
        
        # Read script content
        try:
            with open(script_path, 'r', encoding='utf-8', errors='ignore') as f:
                script_content = f.read()
        except Exception as e:
            messagebox.showerror("Error", f"Failed to read script file:\n{str(e)}")
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
                
                explanation = response.choices[0].message.content
                
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
                self.root.after(0, lambda: self.explain_button.config(state=tk.NORMAL, text="💡 Explain This Step"))
                
            except Exception as e:
                error_msg = f"Failed to get explanation from OpenAI:\n{str(e)}"
                self.root.after(0, lambda: messagebox.showerror("Error", error_msg))
                self.root.after(0, lambda: self.hide_explanation())
                # Re-enable button on error
                self.root.after(0, lambda: self.explain_button.config(state=tk.NORMAL, text="💡 Explain This Step"))
        
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
        
        # Show explanation panel and hide step information
        if not self.explanation_visible:
            # Hide step information when showing explanation
            self.step_info_frame.pack_forget()
            # Pack explanation frame in the same parent as step_info_frame (before where step_info_frame was)
            self.explanation_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
            self.explanation_visible = True
        
        # Increase height when showing
        self.explanation_text.config(height=12)
    
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
        self.explanation_text.tag_config("tldr_header", font=("Segoe UI", 10, "bold"), foreground="#2C3E50")
        self.explanation_text.tag_config("full_header", font=("Segoe UI", 10, "bold"), foreground="#34495E")

        content = self.explanation_text.get(1.0, tk.END)
        lines = content.split('\n')

        for i, line in enumerate(lines, 1):
            if line.startswith("SUMMARY:"):
                self.explanation_text.tag_add("tldr_header", f"{i}.0", f"{i}.end")
            elif line.startswith("DETAILED EXPLANATION:"):
                self.explanation_text.tag_add("full_header", f"{i}.0", f"{i}.end")
    
    def hide_explanation(self):
        """Hide explanation panel and show step information"""
        if self.explanation_visible:
            self.explanation_frame.pack_forget()
            self.explanation_visible = False
            # Show step information again when hiding explanation
            self.step_info_frame.pack(fill=tk.X, padx=5, pady=5)
    
    def clear_explanation(self):
        """Clear explanation text and hide explanation panel, show step information"""
        if self.explanation_visible:
            self.explanation_frame.pack_forget()
            self.explanation_visible = False
            # Show step information again
            self.step_info_frame.pack(fill=tk.X, padx=5, pady=5)
        
        self.explanation_text.config(state=tk.NORMAL)
        self.explanation_text.delete(1.0, tk.END)
        self.explanation_text.config(state=tk.DISABLED)
    
    def update_log_display(self, scraper_name: str):
        """Update log display with the selected scraper's log"""
        log_content = self.scraper_logs.get(scraper_name, "")
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.insert(1.0, log_content)
        self.log_text.see(tk.END)
        self.log_text.config(state=tk.DISABLED)
        
        # Schedule periodic refresh if this scraper is running
        # This ensures we see updates even if they come in while viewing another scraper
        if scraper_name in self.running_scrapers or scraper_name in self.running_processes:
            self.schedule_log_refresh(scraper_name)
    
    def schedule_log_refresh(self, scraper_name: str):
        """Schedule periodic refresh of log display for running scraper"""
        # Only refresh if this scraper is still selected and still running
        if scraper_name == self.scraper_var.get() and (scraper_name in self.running_scrapers or scraper_name in self.running_processes):
            # Update display with latest log content
            log_content = self.scraper_logs.get(scraper_name, "")
            current_content = self.log_text.get(1.0, tk.END)
            if log_content != current_content.rstrip('\n'):
                # Log has been updated, refresh display
                self.log_text.config(state=tk.NORMAL)
                self.log_text.delete(1.0, tk.END)
                self.log_text.insert(1.0, log_content)
                self.log_text.see(tk.END)
                self.log_text.config(state=tk.DISABLED)
            
            # Schedule next refresh in 500ms
            self.root.after(500, lambda sn=scraper_name: self.schedule_log_refresh(sn))
    
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
    
    def refresh_run_button_state(self):
        """Refresh run button and stop button state based on current scraper selection and lock status"""
        scraper_name = self.scraper_var.get()
        if not scraper_name or not hasattr(self, 'run_button'):
            return

        try:
            from platform_config import get_path_manager
            pm = get_path_manager()
            lock_file = pm.get_lock_file(scraper_name)

            if lock_file.exists():
                self.update_status(f"Selected scraper: {scraper_name} (RUNNING - lock file exists)")
                # Disable run button, enable stop button if lock exists
                self.run_button.config(state=tk.DISABLED, text="⏸ Running...")
                self.stop_button.config(state=tk.NORMAL)
            elif scraper_name in self.running_scrapers:
                # This scraper is running from GUI
                self.update_status(f"Selected scraper: {scraper_name} (RUNNING)")
                self.run_button.config(state=tk.DISABLED, text="⏸ Running...")
                self.stop_button.config(state=tk.NORMAL)
            else:
                # No lock and not running - enable run button, disable stop button
                self.update_status(f"Selected scraper: {scraper_name}")
                self.run_button.config(state=tk.NORMAL, text="Run Full Pipeline")
                self.stop_button.config(state=tk.DISABLED)
        except Exception:
            # On error, enable run button and disable stop button if not running from GUI
            if scraper_name not in self.running_scrapers:
                self.run_button.config(state=tk.NORMAL, text="Run Full Pipeline")
                self.stop_button.config(state=tk.DISABLED)
            self.update_status(f"Selected scraper: {scraper_name}")
    
    def finish_scraper_run(self, scraper_name: str, return_code: int):
        """Finish scraper run and update display if selected"""
        if scraper_name == self.scraper_var.get():
            self.update_log_display(scraper_name)
            if return_code == 0:
                self.update_status(f"{scraper_name} execution completed")
            else:
                self.update_status(f"{scraper_name} execution failed")
            self.refresh_output_files()
        
        # Refresh button state after run completes (lock should be released by workflow runner)
        self.refresh_run_button_state()
    
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
    
    def run_full_pipeline(self):
        """Run the full pipeline for selected scraper using shared workflow runner"""
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
        
        # Use new workflow runner
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
        # Initialize log storage for this scraper
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
                    cmd = ["python", str(script_path)] + extra_args
                    process = subprocess.Popen(
                        cmd,
                        cwd=str(working_dir),
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        text=True,
                        bufsize=1,
                        universal_newlines=True
                    )
                
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

                # Final status
                status_msg = "\n" + "=" * 80 + "\n"
                if process.returncode == 0:
                    status_msg += f"Execution completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                else:
                    status_msg += f"Execution failed with return code {process.returncode}\n"

                self.scraper_logs[scraper_name] += status_msg

                # Update display if this scraper is selected
                return_code = process.returncode
                self.root.after(0, lambda sn=scraper_name, rc=return_code: self.finish_scraper_run(sn, rc))

            except Exception as e:
                error_msg = f"\nError: {str(e)}\n"
                self.scraper_logs[scraper_name] += error_msg
                error_str = str(e)
                self.root.after(0, lambda sn=scraper_name, err=error_str: self.handle_scraper_error(sn, err))
            finally:
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

        # Check if this scraper is actually running
        try:
            from platform_config import get_path_manager
            pm = get_path_manager()
            lock_file = pm.get_lock_file(scraper_name)
        except Exception:
            lock_file = self.repo_root / f".{scraper_name}_run.lock"

        if not lock_file.exists() and scraper_name not in self.running_scrapers:
            messagebox.showinfo("Information", f"{scraper_name} is not currently running.")
            return

        # Confirm stop
        if not messagebox.askyesno("Confirm Stop", f"Stop {scraper_name} pipeline?\n\nThis will terminate the running process."):
            return

        self.update_status(f"Stopping {scraper_name}...")

        # Try to stop via shared workflow runner
        try:
            from shared_workflow_runner import WorkflowRunner
            result = WorkflowRunner.stop_pipeline(scraper_name, self.repo_root)

            if result["status"] == "ok":
                messagebox.showinfo("Success", result["message"])
                self.update_status(f"Stopped {scraper_name}")

                # Remove from tracking
                if scraper_name in self.running_processes:
                    del self.running_processes[scraper_name]
                self.running_scrapers.discard(scraper_name)

                # Update log
                stop_msg = f"\n{'='*80}\n[STOPPED] Pipeline stopped by user at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'='*80}\n"
                if scraper_name in self.scraper_logs:
                    self.scraper_logs[scraper_name] += stop_msg
                if scraper_name == self.scraper_var.get():
                    self.append_to_log_display(stop_msg)

                # Refresh button state
                self.refresh_run_button_state()
            else:
                messagebox.showerror("Error", f"Failed to stop {scraper_name}:\n{result['message']}")
                self.update_status(f"Failed to stop {scraper_name}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to stop {scraper_name}:\n{str(e)}")
            self.update_status(f"Error stopping {scraper_name}: {str(e)}")

    def clear_run_lock(self):
        """Clear run lock file for the currently selected scraper only"""
        scraper_name = self.scraper_var.get()
        if not scraper_name:
            messagebox.showwarning("Warning", "Select a scraper first")
            return

        # Find lock file for the selected scraper only
        try:
            from platform_config import get_path_manager
            pm = get_path_manager()
            lock_file = pm.get_lock_file(scraper_name)
        except Exception:
            # Fallback to old location
            lock_file = self.repo_root / f".{scraper_name}_run.lock"

        if not lock_file.exists():
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
                        lock_file.unlink()
                        deleted.append(scraper_name)
                        deleted_successfully = True
                        break
                    except (PermissionError, OSError) as e:
                        # File might still be locked by a closing process
                        if attempt < max_retries - 1:
                            # Increasing wait time with each retry: 0.5s, 1s, 1.5s, 2s
                            wait_time = 0.5 * (attempt + 1)
                            time.sleep(wait_time)
                        else:
                            # Final attempt failed, try renaming as fallback
                            try:
                                # Try renaming the file (sometimes works when delete doesn't)
                                backup_name = lock_file.with_suffix('.lock.old')
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
                                failed.append((scraper_name, f"{str(e)} (tried {max_retries} times, rename also failed)"))
                    except Exception as e:
                        failed.append((scraper_name, str(e)))
                        break
            else:
                # Unix-like: straightforward deletion
                lock_file.unlink()
                deleted.append(scraper_name)
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
        docs_path = scraper_info["path"] / scraper_info["docs_dir"]
        
        if docs_path.exists():
            os.startfile(str(docs_path))
        else:
            messagebox.showwarning("Warning", f"Documentation folder not found:\n{docs_path}")
    
    def refresh_logs(self):
        """Refresh execution logs"""
        # Logs are updated in real-time during execution
        self.update_status("Logs refreshed")
    
    def clear_logs(self):
        """Clear log viewer"""
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state=tk.DISABLED)
        self.update_status("Logs cleared")
    
    def copy_logs_to_clipboard(self):
        """Copy current log content to clipboard"""
        content = self.log_text.get(1.0, tk.END)
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
        """Save current log to file"""
        content = self.log_text.get(1.0, tk.END)
        if not content.strip():
            messagebox.showwarning("Warning", "No log content to save")
            return
        
        filename = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
        )
        
        if filename:
            try:
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(content)
                messagebox.showinfo("Information", f"Log saved to:\n{filename}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to save log:\n{str(e)}")
    
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
        
        # Verify the output directory belongs to the selected scraper
        # (for run directories, they should start with scraper name)
        if scraper_name and not output_dir.name.startswith(scraper_name):
            # Try to find the correct run directory for this scraper
            try:
                from platform_config import get_path_manager
                pm = get_path_manager()
                runs_dir = pm.get_runs_dir()
                # Find latest run for this scraper
                latest_run = None
                latest_time = None
                for run_dir in runs_dir.iterdir():
                    if run_dir.is_dir() and run_dir.name.startswith(scraper_name):
                        mtime = run_dir.stat().st_mtime
                        if latest_time is None or mtime > latest_time:
                            latest_time = mtime
                            latest_run = run_dir
                if latest_run:
                    output_dir = latest_run
                    self.output_path_var.set(str(latest_run))
            except Exception:
                pass  # Use the original output_dir if we can't find a better one
        
        # List all files recursively from run directory
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
            "Malaysia": ["malaysia"],
            "Argentina": ["alfabeta_report"]
        }
        
        patterns = scraper_patterns.get(scraper_name, [])
        files = []
        for file_path in sorted(output_dir.iterdir()):
            if file_path.is_file() and file_path.suffix.lower() in ['.csv', '.xlsx']:
                # Only show files that match this scraper's pattern
                file_lower = file_path.name.lower()
                if patterns and any(pattern in file_lower for pattern in patterns):
                    files.append((file_path.name, file_path))
                    self.final_output_listbox.insert(tk.END, file_path.name)
        
        # Update info
        self.final_output_info_text.config(state=tk.NORMAL)
        self.final_output_info_text.delete(1.0, tk.END)
        info = f"Total files: {len(files)}\n"
        info += f"Directory: {output_dir}\n"
        if files:
            total_size = sum(f[1].stat().st_size for f in files)
            info += f"Total size: {total_size:,} bytes ({total_size / 1024 / 1024:.2f} MB)\n"
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
                size = file_path.stat().st_size
                modified = datetime.fromtimestamp(file_path.stat().st_mtime)
                info = f"File: {file_path.name}\n"
                info += f"Path: {file_path}\n"
                info += f"Size: {size:,} bytes ({size / 1024:.2f} KB)\n"
                info += f"Modified: {modified.strftime('%Y-%m-%d %H:%M:%S')}"
                
                self.final_output_info_text.config(state=tk.NORMAL)
                self.final_output_info_text.delete(1.0, tk.END)
                self.final_output_info_text.insert(1.0, info)
                self.final_output_info_text.config(state=tk.DISABLED)
                
                # Open file
                os.startfile(str(file_path))
            except Exception as e:
                messagebox.showerror("Error", f"Failed to open file:\n{str(e)}")
        else:
            messagebox.showwarning("Warning", f"File not found:\n{file_path}")
    
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
            "Malaysia": ["malaysia"],
            "Argentina": ["alfabeta_report"]
        }
        patterns = scraper_patterns.get(scraper_name, [])
        
        matches = []
        for file_path in sorted(output_dir.iterdir()):
            if file_path.is_file() and file_path.suffix.lower() in ['.csv', '.xlsx']:
                file_lower = file_path.name.lower()
                # Only show files that match this scraper's pattern
                if patterns and any(pattern in file_lower for pattern in patterns):
                    if search_term.lower() in file_path.name.lower():
                        matches.append(file_path.name)
                        self.final_output_listbox.insert(tk.END, file_path.name)
        
        self.update_status(f"Found {len(matches)} files matching '{search_term}'")
    
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
        for scraper_name, scraper_info in self.scrapers.items():
            logs_dir = scraper_info["path"] / "logs"
            if logs_dir.exists():
                for log_file in logs_dir.rglob("*.log"):
                    log_files.append(str(log_file))
            output_dir = scraper_info["path"] / "output"
            if output_dir.exists():
                for log_file in output_dir.rglob("*.log"):
                    log_files.append(str(log_file))
        
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
        """Update status bar"""
        self.status_bar.config(text=f"Status: {message}")
        self.root.update_idletasks()


def main():
    root = tk.Tk()
    app = ScraperGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()

