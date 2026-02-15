"""
Configuration Tab for Scraper GUI

Handles scraper-specific configuration file editing (config/{scraper_id}.env.json)
"""

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import json
import os
from pathlib import Path


class ConfigTab:
    """Manages the Configuration tab for editing scraper config files"""
    
    def __init__(self, parent, gui_instance):
        """
        Initialize the configuration tab.
        
        Args:
            parent: Parent tkinter frame
            gui_instance: Reference to main ScraperGUI instance for accessing shared state
        """
        self.parent = parent
        self.gui = gui_instance
        self.current_config_file = None
        self.setup_ui()
    
    def setup_ui(self):
        """Setup the configuration tab UI"""
        # Toolbar - white background
        toolbar = tk.Frame(self.parent, bg=self.gui.colors['white'])
        toolbar.pack(fill=tk.X, padx=8, pady=8)
        
        tk.Label(toolbar, text="Scraper Configuration:", 
                font=self.gui.fonts['standard'],
                bg=self.gui.colors['white'],
                fg='#000000').pack(side=tk.LEFT, padx=(0, 8))
        
        ttk.Button(toolbar, text="Load", command=self.load_config_file, 
                  style='Secondary.TButton').pack(side=tk.LEFT, padx=3)
        ttk.Button(toolbar, text="Save", command=self.save_config_file, 
                  style='Primary.TButton').pack(side=tk.LEFT, padx=3)
        ttk.Button(toolbar, text="Format JSON", command=self.format_config_json,
                  style='Secondary.TButton').pack(side=tk.LEFT, padx=3)
        ttk.Button(toolbar, text="Open File", command=self.open_config_file, 
                  style='Secondary.TButton').pack(side=tk.LEFT, padx=3)
        ttk.Button(toolbar, text="Create from Template", command=self.create_config_from_template, 
                  style='Secondary.TButton').pack(side=tk.LEFT, padx=3)
        
        # Config editor - dark theme with border
        editor_frame = ttk.LabelFrame(self.parent, text="Configuration Editor", padding=12, style='Title.TLabelframe')
        editor_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        
        self.config_text = scrolledtext.ScrolledText(editor_frame, wrap=tk.WORD,
                                                     font=self.gui.fonts['monospace'],
                                                     bg=self.gui.colors['white'],
                                                     fg='#000000',
                                                     borderwidth=0,
                                                     relief='flat',
                                                     highlightthickness=0,
                                                     padx=16,
                                                     pady=16,
                                                     insertbackground='#000000',
                                                     selectbackground=self.gui.colors['background_gray'],
                                                     selectforeground='#000000')
        self.config_text.pack(fill=tk.BOTH, expand=True)
        
        # Configure JSON syntax highlighting tags
        self.config_text.tag_configure("json_key", foreground='#000000')
        self.config_text.tag_configure("json_string", foreground="#10b981")  # Green for strings
        self.config_text.tag_configure("json_number", foreground="#f59e0b")  # Amber for numbers
        self.config_text.tag_configure("json_boolean", foreground='#000000')
        
        # Status - white background, no border
        self.config_status = tk.Label(self.parent, text="Scraper-specific configuration file", 
                                       relief=tk.FLAT, anchor=tk.W,
                                       bg=self.gui.colors['white'],
                                       fg='#000000',
                                       font=self.gui.fonts['standard'],
                                       padx=10,
                                       borderwidth=0,
                                       highlightthickness=0)
        self.config_status.pack(fill=tk.X, padx=8, pady=8)
    
    def load_config_file(self):
        """Load scraper-specific config file (config/{scraper_id}.env.json)"""
        scraper_name = self.gui.scraper_var.get() if hasattr(self.gui, 'scraper_var') else None
        if not scraper_name:
            messagebox.showwarning("Warning", "Please select a scraper first to load its configuration.")
            return
        
        # Use scraper-specific config file from config directory
        try:
            from core.config.config_manager import ConfigManager
            config_dir = ConfigManager.get_config_dir()
            config_file = config_dir / f"{scraper_name}.env.json"
        except Exception:
            # Fallback to repo root config directory
            config_dir = self.gui.repo_root / "config"
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

    def format_config_json(self):
        """Format the config editor content as pretty JSON for readability."""
        try:
            content = self.config_text.get(1.0, tk.END).strip()
            if not content:
                messagebox.showwarning("Warning", "Configuration editor is empty.")
                return
            data = json.loads(content)
            formatted = json.dumps(data, indent=2, ensure_ascii=False)
            self.config_text.delete(1.0, tk.END)
            self.config_text.insert(1.0, formatted + "\n")
            self.config_status.config(text="Formatted JSON for readability.")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to format JSON:\n{e}")
    
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
        scraper_name = self.gui.scraper_var.get() if hasattr(self.gui, 'scraper_var') else None
        if not scraper_name:
            messagebox.showwarning("Warning", "Please select a scraper first.")
            return
        
        # Try to find template in config directory
        try:
            from core.config.config_manager import ConfigManager
            config_dir = ConfigManager.get_config_dir()
            template_file = config_dir / f"{scraper_name}.env.json.example"
        except Exception:
            config_dir = self.gui.repo_root / "config"
            template_file = config_dir / f"{scraper_name}.env.json.example"
        
        if not template_file.exists():
            # Try scraper directory
            scraper_info = self.gui.scrapers.get(scraper_name, {})
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
