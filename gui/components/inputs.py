#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Input Components - Enhanced input widgets

Provides input components with search, validation, and modern styling.
"""

import tkinter as tk
from tkinter import ttk
from typing import List, Optional, Callable
from gui.themes.modern import ModernTheme, FontConfig


class SearchableCombobox:
    """
    Combobox with real-time search/filter functionality.
    
    Features:
    - Real-time filtering as user types
    - Case-insensitive search
    - Maintains original values
    - Keyboard navigation
    
    Example:
        combo = SearchableCombobox(
            parent,
            values=['Apple', 'Banana', 'Cherry', 'Date'],
            width=30
        )
        combo.pack()
        
        # Get selected value
        value = combo.get()
    """
    
    def __init__(
        self,
        parent: tk.Widget,
        values: List[str],
        width: int = 30,
        placeholder: str = "Search...",
        on_select: Optional[Callable] = None,
        **kwargs
    ):
        """
        Initialize searchable combobox.
        
        Args:
            parent: Parent widget
            values: List of available values
            width: Widget width
            placeholder: Placeholder text for search
            on_select: Callback when selection changes
        """
        self.parent = parent
        self.all_values = values.copy()
        self.filtered_values = values.copy()
        self.on_select = on_select
        self.placeholder = placeholder
        
        # Container frame
        self.frame = tk.Frame(parent, bg=ModernTheme.BG_MAIN)
        
        # Search entry
        self.search_var = tk.StringVar()
        self.search_var.trace('w', self._on_search)
        
        self.search_entry = tk.Entry(
            self.frame,
            textvariable=self.search_var,
            font=FontConfig.body(),
            bg=ModernTheme.BG_INPUT,
            fg=ModernTheme.TEXT_MUTED,
            relief=tk.FLAT,
            highlightthickness=1,
            highlightbackground=ModernTheme.BORDER,
            highlightcolor=ModernTheme.PRIMARY,
            width=width
        )
        self.search_entry.pack(fill=tk.X, pady=(0, 5))
        self.search_entry.insert(0, placeholder)
        
        self.search_entry.bind('<FocusIn>', self._on_search_focus_in)
        self.search_entry.bind('<FocusOut>', self._on_search_focus_out)
        self.search_entry.bind('<Return>', self._on_search_enter)
        
        # Combobox
        self.var = tk.StringVar()
        self.combobox = ttk.Combobox(
            self.frame,
            textvariable=self.var,
            values=self.filtered_values,
            state="readonly",
            width=width - 2,
            **kwargs
        )
        self.combobox.pack(fill=tk.X)
        
        if on_select:
            self.combobox.bind("<<ComboboxSelected>>", lambda e: on_select(self.var.get()))
    
    def _on_search_focus_in(self, event):
        """Handle search entry focus in"""
        if self.search_entry.get() == self.placeholder:
            self.search_entry.delete(0, tk.END)
            self.search_entry.config(fg=ModernTheme.TEXT_PRIMARY)
    
    def _on_search_focus_out(self, event):
        """Handle search entry focus out"""
        if not self.search_entry.get():
            self.search_entry.insert(0, self.placeholder)
            self.search_entry.config(fg=ModernTheme.TEXT_MUTED)
    
    def _on_search(self, *args):
        """Filter values based on search"""
        query = self.search_var.get().lower()
        
        if query == self.placeholder.lower():
            self.filtered_values = self.all_values.copy()
        else:
            self.filtered_values = [
                v for v in self.all_values
                if query in v.lower()
            ]
        
        self.combobox['values'] = self.filtered_values
        
        # Select first match if current selection not in filtered
        current = self.var.get()
        if current not in self.filtered_values and self.filtered_values:
            self.var.set(self.filtered_values[0])
    
    def _on_search_enter(self, event):
        """Handle Enter key in search"""
        if self.filtered_values and self.on_select:
            self.on_select(self.var.get())
    
    def get(self) -> str:
        """Get current value"""
        return self.var.get()
    
    def set(self, value: str):
        """Set current value"""
        self.var.set(value)
    
    def set_values(self, values: List[str]):
        """Update available values"""
        self.all_values = values.copy()
        self.filtered_values = values.copy()
        self.combobox['values'] = values
    
    def bind(self, *args, **kwargs):
        """Bind to combobox"""
        self.combobox.bind(*args, **kwargs)
    
    def pack(self, **kwargs):
        """Pack the frame"""
        self.frame.pack(**kwargs)
    
    def grid(self, **kwargs):
        """Grid the frame"""
        self.frame.grid(**kwargs)


class ValidatedEntry:
    """
    Entry widget with validation.
    
    Features:
    - Real-time validation
    - Visual feedback (border color)
    - Custom validation functions
    - Error messages
    
    Example:
        def validate_email(value):
            return '@' in value
        
        entry = ValidatedEntry(
            parent,
            validator=validate_email,
            error_msg="Please enter a valid email"
        )
        entry.pack()
    """
    
    def __init__(
        self,
        parent: tk.Widget,
        validator: Optional[Callable[[str], bool]] = None,
        error_msg: str = "Invalid input",
        width: int = 30,
        show: str = "",
        **kwargs
    ):
        """
        Initialize validated entry.
        
        Args:
            parent: Parent widget
            validator: Validation function (returns True/False)
            error_msg: Error message to display
            width: Entry width
            show: Character to show instead of actual text (for passwords)
        """
        self.parent = parent
        self.validator = validator
        self.error_msg = error_msg
        self.is_valid = True
        
        self.colors = ModernTheme.get_all()
        
        # Container
        self.frame = tk.Frame(parent, bg=self.colors['BG_MAIN'])
        
        # Entry
        self.var = tk.StringVar()
        self.var.trace('w', self._on_change)
        
        self.entry = tk.Entry(
            self.frame,
            textvariable=self.var,
            font=FontConfig.body(),
            bg=self.colors['BG_INPUT'],
            fg=self.colors['TEXT_PRIMARY'],
            relief=tk.FLAT,
            highlightthickness=2,
            highlightbackground=self.colors['BORDER'],
            highlightcolor=self.colors['PRIMARY'],
            width=width,
            show=show,
            **kwargs
        )
        self.entry.pack(fill=tk.X)
        
        # Error label (hidden by default)
        self.error_label = tk.Label(
            self.frame,
            text=error_msg,
            bg=self.colors['BG_MAIN'],
            fg=self.colors['ERROR'],
            font=FontConfig.small()
        )
    
    def _on_change(self, *args):
        """Handle value change"""
        if self.validator:
            value = self.var.get()
            self.is_valid = self.validator(value)
            
            if self.is_valid:
                self.entry.config(highlightbackground=self.colors['BORDER'])
                self.error_label.pack_forget()
            else:
                self.entry.config(highlightbackground=self.colors['ERROR'])
                self.error_label.pack(fill=tk.X, pady=(2, 0))
    
    def get(self) -> str:
        """Get current value"""
        return self.var.get()
    
    def set(self, value: str):
        """Set current value"""
        self.var.set(value)
    
    def validate(self) -> bool:
        """Explicitly validate current value"""
        if self.validator:
            self.is_valid = self.validator(self.var.get())
        return self.is_valid
    
    def pack(self, **kwargs):
        """Pack the frame"""
        self.frame.pack(**kwargs)
    
    def grid(self, **kwargs):
        """Grid the frame"""
        self.frame.grid(**kwargs)


class NumberEntry:
    """
    Entry widget for numeric input only.
    
    Example:
        entry = NumberEntry(parent, min_value=0, max_value=100)
        entry.pack()
    """
    
    def __init__(
        self,
        parent: tk.Widget,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        integer_only: bool = False,
        width: int = 10,
        **kwargs
    ):
        """
        Initialize number entry.
        
        Args:
            parent: Parent widget
            min_value: Minimum allowed value
            max_value: Maximum allowed value
            integer_only: Whether to allow only integers
            width: Entry width
        """
        self.min_value = min_value
        self.max_value = max_value
        self.integer_only = integer_only
        
        self.colors = ModernTheme.get_all()
        
        self.frame = tk.Frame(parent, bg=self.colors['BG_MAIN'])
        
        # Entry with validation
        vcmd = (parent.register(self._validate), '%P')
        
        self.entry = tk.Entry(
            self.frame,
            font=FontConfig.body(),
            bg=self.colors['BG_INPUT'],
            fg=self.colors['TEXT_PRIMARY'],
            relief=tk.FLAT,
            highlightthickness=1,
            highlightbackground=self.colors['BORDER'],
            highlightcolor=self.colors['PRIMARY'],
            width=width,
            validate='key',
            validatecommand=vcmd,
            **kwargs
        )
        self.entry.pack(fill=tk.X)
    
    def _validate(self, value: str) -> bool:
        """Validate numeric input"""
        if not value:
            return True
        
        try:
            if self.integer_only:
                num = int(value)
            else:
                num = float(value)
            
            if self.min_value is not None and num < self.min_value:
                return False
            if self.max_value is not None and num > self.max_value:
                return False
            
            return True
        except ValueError:
            return False
    
    def get(self) -> Optional[float]:
        """Get numeric value"""
        try:
            value = self.entry.get()
            if not value:
                return None
            if self.integer_only:
                return int(value)
            return float(value)
        except ValueError:
            return None
    
    def set(self, value: float):
        """Set numeric value"""
        if self.integer_only:
            self.entry.delete(0, tk.END)
            self.entry.insert(0, str(int(value)))
        else:
            self.entry.delete(0, tk.END)
            self.entry.insert(0, str(value))
    
    def pack(self, **kwargs):
        """Pack the frame"""
        self.frame.pack(**kwargs)
    
    def grid(self, **kwargs):
        """Grid the frame"""
        self.frame.grid(**kwargs)


# Export
__all__ = ['SearchableCombobox', 'ValidatedEntry', 'NumberEntry']
