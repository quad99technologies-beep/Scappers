#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Badge Components - Status indicators and labels

Provides visual status badges and label components.
"""

import tkinter as tk
from typing import Optional, Dict, Any
from gui.themes.modern import ModernTheme, FontConfig, IconLibrary


class StatusBadge:
    """
    Visual status indicator badge.
    
    Features:
    - Color-coded by status
    - Icon support
    - Multiple status types
    - Rounded corners simulation
    
    Example:
        badge = StatusBadge(parent, status='running')
        badge.pack(side=tk.LEFT)
        
        # Update status
        badge.set_status('stopped')
    """
    
    # Status configurations
    STATUSES: Dict[str, Dict[str, Any]] = {
        'running': {
            'bg': ModernTheme.SUCCESS_LIGHT,
            'fg': ModernTheme.SUCCESS,
            'icon': IconLibrary.RUNNING,
            'text': 'Running'
        },
        'active': {
            'bg': ModernTheme.SUCCESS_LIGHT,
            'fg': ModernTheme.SUCCESS,
            'icon': IconLibrary.RUNNING,
            'text': 'Active'
        },
        'stopped': {
            'bg': ModernTheme.ERROR_LIGHT,
            'fg': ModernTheme.ERROR,
            'icon': IconLibrary.STOPPED,
            'text': 'Stopped'
        },
        'paused': {
            'bg': ModernTheme.WARNING_LIGHT,
            'fg': ModernTheme.WARNING,
            'icon': IconLibrary.PAUSED,
            'text': 'Paused'
        },
        'idle': {
            'bg': ModernTheme.BG_INPUT,
            'fg': ModernTheme.TEXT_MUTED,
            'icon': IconLibrary.IDLE,
            'text': 'Idle'
        },
        'loading': {
            'bg': ModernTheme.INFO_LIGHT,
            'fg': ModernTheme.INFO,
            'icon': IconLibrary.LOADING,
            'text': 'Loading'
        },
        'pending': {
            'bg': ModernTheme.INFO_LIGHT,
            'fg': ModernTheme.INFO,
            'icon': IconLibrary.LOADING,
            'text': 'Pending'
        },
        'error': {
            'bg': ModernTheme.ERROR_LIGHT,
            'fg': ModernTheme.ERROR,
            'icon': IconLibrary.ERROR,
            'text': 'Error'
        },
        'failed': {
            'bg': ModernTheme.ERROR_LIGHT,
            'fg': ModernTheme.ERROR,
            'icon': IconLibrary.ERROR,
            'text': 'Failed'
        },
        'success': {
            'bg': ModernTheme.SUCCESS_LIGHT,
            'fg': ModernTheme.SUCCESS,
            'icon': IconLibrary.SUCCESS,
            'text': 'Success'
        },
        'completed': {
            'bg': ModernTheme.SUCCESS_LIGHT,
            'fg': ModernTheme.SUCCESS,
            'icon': IconLibrary.SUCCESS,
            'text': 'Completed'
        },
        'warning': {
            'bg': ModernTheme.WARNING_LIGHT,
            'fg': ModernTheme.WARNING,
            'icon': IconLibrary.WARNING,
            'text': 'Warning'
        },
        'info': {
            'bg': ModernTheme.INFO_LIGHT,
            'fg': ModernTheme.INFO,
            'icon': IconLibrary.INFO,
            'text': 'Info'
        }
    }
    
    def __init__(
        self,
        parent: tk.Widget,
        status: str = 'idle',
        text: Optional[str] = None,
        show_icon: bool = True,
        padx: int = 10,
        pady: int = 4,
        font = None
    ):
        """
        Initialize status badge.
        
        Args:
            parent: Parent widget
            status: Status type
            text: Custom text (overrides default)
            show_icon: Whether to show icon
            padx: Horizontal padding
            pady: Vertical padding
            font: Custom font
        """
        self.parent = parent
        self.status = status
        self.show_icon = show_icon
        self.config = self.STATUSES.get(status, self.STATUSES['idle'])
        
        if font is None:
            font = FontConfig.body_bold()
        
        # Create frame
        self.frame = tk.Frame(
            parent,
            bg=self.config['bg'],
            padx=padx,
            pady=pady
        )
        
        # Icon
        if show_icon and self.config['icon']:
            self.icon_label = tk.Label(
                self.frame,
                text=self.config['icon'],
                bg=self.config['bg'],
                fg=self.config['fg'],
                font=FontConfig.body()
            )
            self.icon_label.pack(side=tk.LEFT, padx=(0, 5))
        
        # Text
        display_text = text or self.config['text']
        self.text_label = tk.Label(
            self.frame,
            text=display_text,
            bg=self.config['bg'],
            fg=self.config['fg'],
            font=font
        )
        self.text_label.pack(side=tk.LEFT)
    
    def set_status(self, status: str, text: Optional[str] = None):
        """
        Update badge status.
        
        Args:
            status: New status type
            text: Optional custom text
        """
        self.status = status
        self.config = self.STATUSES.get(status, self.STATUSES['idle'])
        
        # Update colors
        self.frame.config(bg=self.config['bg'])
        
        if hasattr(self, 'icon_label'):
            self.icon_label.config(
                bg=self.config['bg'],
                fg=self.config['fg'],
                text=self.config['icon']
            )
        
        display_text = text or self.config['text']
        self.text_label.config(
            bg=self.config['bg'],
            fg=self.config['fg'],
            text=display_text
        )
    
    def set_text(self, text: str):
        """Update badge text"""
        self.text_label.config(text=text)
    
    def pack(self, **kwargs):
        """Pack the badge"""
        self.frame.pack(**kwargs)
    
    def grid(self, **kwargs):
        """Grid the badge"""
        self.frame.grid(**kwargs)


class LabelBadge:
    """
    Simple label badge for tags, categories, etc.
    
    Example:
        badge = LabelBadge(parent, text="v2.0", style='primary')
        badge.pack(side=tk.LEFT)
    """
    
    STYLES: Dict[str, Dict[str, str]] = {
        'primary': {'bg': ModernTheme.PRIMARY_GLOW, 'fg': ModernTheme.PRIMARY},
        'secondary': {'bg': ModernTheme.BG_INPUT, 'fg': ModernTheme.TEXT_SECONDARY},
        'success': {'bg': ModernTheme.SUCCESS_LIGHT, 'fg': ModernTheme.SUCCESS},
        'warning': {'bg': ModernTheme.WARNING_LIGHT, 'fg': ModernTheme.WARNING},
        'error': {'bg': ModernTheme.ERROR_LIGHT, 'fg': ModernTheme.ERROR},
        'info': {'bg': ModernTheme.INFO_LIGHT, 'fg': ModernTheme.INFO},
    }
    
    def __init__(
        self,
        parent: tk.Widget,
        text: str,
        style: str = 'secondary',
        padx: int = 8,
        pady: int = 2,
        font = None
    ):
        """
        Initialize label badge.
        
        Args:
            parent: Parent widget
            text: Badge text
            style: Badge style
            padx: Horizontal padding
            pady: Vertical padding
            font: Custom font
        """
        self.style_config = self.STYLES.get(style, self.STYLES['secondary'])
        
        if font is None:
            font = FontConfig.small()
        
        self.frame = tk.Frame(
            parent,
            bg=self.style_config['bg'],
            padx=padx,
            pady=pady
        )
        
        self.label = tk.Label(
            self.frame,
            text=text,
            bg=self.style_config['bg'],
            fg=self.style_config['fg'],
            font=font
        )
        self.label.pack()
    
    def set_text(self, text: str):
        """Update badge text"""
        self.label.config(text=text)
    
    def pack(self, **kwargs):
        """Pack the badge"""
        self.frame.pack(**kwargs)
    
    def grid(self, **kwargs):
        """Grid the badge"""
        self.frame.grid(**kwargs)


class CounterBadge:
    """
    Badge with a count number.
    
    Example:
        badge = CounterBadge(parent, count=5, label="items")
        badge.pack()
        
        # Update count
        badge.set_count(10)
    """
    
    def __init__(
        self,
        parent: tk.Widget,
        count: int = 0,
        label: Optional[str] = None,
        bg: Optional[str] = None,
        fg: Optional[str] = None
    ):
        colors = ModernTheme.get_all()
        bg = bg or colors['PRIMARY']
        fg = fg or 'white'
        
        self.frame = tk.Frame(parent, bg=bg, padx=8, pady=2)
        
        # Count
        self.count_label = tk.Label(
            self.frame,
            text=str(count),
            bg=bg,
            fg=fg,
            font=FontConfig.body_bold()
        )
        self.count_label.pack(side=tk.LEFT)
        
        # Label
        if label:
            self.text_label = tk.Label(
                self.frame,
                text=f" {label}",
                bg=bg,
                fg=fg,
                font=FontConfig.small()
            )
            self.text_label.pack(side=tk.LEFT)
    
    def set_count(self, count: int):
        """Update count"""
        self.count_label.config(text=str(count))
    
    def pack(self, **kwargs):
        """Pack the badge"""
        self.frame.pack(**kwargs)
    
    def grid(self, **kwargs):
        """Grid the badge"""
        self.frame.grid(**kwargs)


# Export
__all__ = ['StatusBadge', 'LabelBadge', 'CounterBadge']
