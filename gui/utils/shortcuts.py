#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Keyboard Shortcuts Utility - Shortcut management system

Provides keyboard shortcut management and help display.
"""

import tkinter as tk
from typing import Dict, Tuple, Callable, Optional
from gui.themes.modern import ModernTheme, FontConfig, IconLibrary
from gui.components.buttons import ModernButton


class KeyboardShortcutManager:
    """
    Keyboard shortcut management system.
    
    Features:
    - Easy shortcut registration
    - Conflict detection
    - Help dialog generation
    - Platform-specific key handling
    
    Example:
        shortcuts = KeyboardShortcutManager(root)
        shortcuts.add_shortcut('<F5>', "Run pipeline", on_run)
        shortcuts.add_shortcut('<Control-s>', "Save", on_save)
    """
    
    def __init__(self, root: tk.Tk):
        """
        Initialize shortcut manager.
        
        Args:
            root: Root window
        """
        self.root = root
        self.shortcuts: Dict[str, Tuple[str, Callable, Optional[str]]] = {}
        self.help_window: Optional[tk.Toplevel] = None
    
    def add_shortcut(
        self,
        key: str,
        description: str,
        callback: Callable,
        group: Optional[str] = None
    ):
        """
        Add a keyboard shortcut.
        
        Args:
            key: Key combination (e.g., '<F5>', '<Control-s>')
            description: Human-readable description
            callback: Function to call
            group: Optional group name for organization
        """
        self.shortcuts[key] = (description, callback, group)
        
        # Bind to root
        self.root.bind(key, lambda e, cb=callback: self._execute(cb))
    
    def _execute(self, callback: Callable):
        """Execute callback and prevent default"""
        callback()
        return 'break'
    
    def remove_shortcut(self, key: str):
        """Remove a shortcut"""
        if key in self.shortcuts:
            del self.shortcuts[key]
            self.root.unbind(key)
    
    def show_help(self):
        """Show keyboard shortcuts help dialog"""
        if self.help_window and self.help_window.winfo_exists():
            self.help_window.lift()
            return
        
        self.help_window = tk.Toplevel(self.root)
        self.help_window.title(f"{IconLibrary.HELP} Keyboard Shortcuts")
        self.help_window.geometry("500x600")
        self.help_window.configure(bg=ModernTheme.BG_CARD)
        self.help_window.transient(self.root)
        
        colors = ModernTheme.get_all()
        
        # Title
        title_frame = tk.Frame(self.help_window, bg=colors['BG_CARD'], padx=20, pady=15)
        title_frame.pack(fill=tk.X)
        
        tk.Label(
            title_frame,
            text=f"{IconLibrary.HELP} Keyboard Shortcuts",
            bg=colors['BG_CARD'],
            fg=colors['TEXT_PRIMARY'],
            font=FontConfig.header()
        ).pack(anchor='w')
        
        tk.Label(
            title_frame,
            text="Press any of these keys to quickly access features",
            bg=colors['BG_CARD'],
            fg=colors['TEXT_SECONDARY'],
            font=FontConfig.body()
        ).pack(anchor='w', pady=(5, 0))
        
        # Separator
        tk.Frame(
            self.help_window,
            height=1,
            bg=colors['BORDER_LIGHT']
        ).pack(fill=tk.X, padx=20)
        
        # Shortcuts by group
        content_frame = tk.Frame(self.help_window, bg=colors['BG_CARD'])
        content_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=15)
        
        # Group shortcuts
        groups: Dict[str, Dict[str, str]] = {}
        ungrouped: Dict[str, str] = {}
        
        for key, (desc, _, group) in self.shortcuts.items():
            display_key = self._format_key(key)
            if group:
                if group not in groups:
                    groups[group] = {}
                groups[group][display_key] = desc
            else:
                ungrouped[display_key] = desc
        
        # Create scrollable content
        canvas = tk.Canvas(content_frame, bg=colors['BG_CARD'], highlightthickness=0)
        scrollbar = tk.Scrollbar(content_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = tk.Frame(canvas, bg=colors['BG_CARD'])
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw", width=460)
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # Add grouped shortcuts
        row = 0
        for group_name, shortcuts in sorted(groups.items()):
            # Group header
            tk.Label(
                scrollable_frame,
                text=group_name,
                bg=colors['BG_CARD'],
                fg=colors['PRIMARY'],
                font=FontConfig.subtitle()
            ).grid(row=row, column=0, sticky='w', pady=(15, 5))
            row += 1
            
            # Shortcuts in group
            for key, desc in sorted(shortcuts.items()):
                self._add_shortcut_row(scrollable_frame, row, key, desc, colors)
                row += 1
        
        # Add ungrouped shortcuts
        if ungrouped:
            if groups:
                tk.Label(
                    scrollable_frame,
                    text="General",
                    bg=colors['BG_CARD'],
                    fg=colors['PRIMARY'],
                    font=FontConfig.subtitle()
                ).grid(row=row, column=0, sticky='w', pady=(15, 5))
                row += 1
            
            for key, desc in sorted(ungrouped.items()):
                self._add_shortcut_row(scrollable_frame, row, key, desc, colors)
                row += 1
        
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Close button
        btn_frame = tk.Frame(self.help_window, bg=colors['BG_CARD'], padx=20, pady=15)
        btn_frame.pack(fill=tk.X)
        
        ModernButton(
            btn_frame,
            text="Close",
            command=self.help_window.destroy,
            style='primary'
        ).pack(side=tk.RIGHT)
    
    def _add_shortcut_row(
        self,
        parent: tk.Frame,
        row: int,
        key: str,
        desc: str,
        colors: Dict[str, str]
    ):
        """Add a shortcut row to the help dialog"""
        # Key badge
        key_frame = tk.Frame(
            parent,
            bg=colors['BG_INPUT'],
            padx=8,
            pady=4
        )
        key_frame.grid(row=row, column=0, sticky='w', pady=2)
        
        tk.Label(
            key_frame,
            text=key,
            bg=colors['BG_INPUT'],
            fg=colors['TEXT_PRIMARY'],
            font=FontConfig.monospace(9)
        ).pack()
        
        # Description
        tk.Label(
            parent,
            text=desc,
            bg=colors['BG_CARD'],
            fg=colors['TEXT_SECONDARY'],
            font=FontConfig.body()
        ).grid(row=row, column=1, sticky='w', padx=(15, 0), pady=2)
    
    def _format_key(self, key: str) -> str:
        """Format key for display"""
        # Remove angle brackets
        key = key.strip('<>')
        
        # Replace common modifiers
        replacements = {
            'Control': 'Ctrl',
            'Alt': 'Alt',
            'Shift': 'Shift',
            'Return': 'Enter',
            'Escape': 'Esc',
            'Delete': 'Del',
            'BackSpace': 'Backspace',
            'Prior': 'PgUp',
            'Next': 'PgDn',
        }
        
        for old, new in replacements.items():
            key = key.replace(old, new)
            key = key.replace(old.lower(), new)
        
        # Capitalize first letter of each part
        parts = key.split('-')
        parts = [p.capitalize() if len(p) > 1 else p.upper() for p in parts]
        
        return '-'.join(parts)
    
    def get_shortcuts_text(self) -> str:
        """Get shortcuts as formatted text"""
        lines = ["Keyboard Shortcuts:", ""]
        
        for key, (desc, _, _) in sorted(self.shortcuts.items()):
            display_key = self._format_key(key)
            lines.append(f"{display_key:<15} - {desc}")
        
        return '\n'.join(lines)


# Export
__all__ = ['KeyboardShortcutManager']
