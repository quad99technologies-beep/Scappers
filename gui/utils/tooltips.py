#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tooltip Utility - Modern tooltip system

Provides enhanced tooltips with modern styling and smart positioning.
"""

import tkinter as tk
from typing import Optional
from gui.themes.modern import ModernTheme, FontConfig


class TooltipManager:
    """
    Modern tooltip manager with enhanced styling.
    
    Features:
    - Configurable delay before showing
    - Smart positioning (stays on screen)
    - Modern dark styling
    - Auto-hide after timeout
    - Support for rich text
    
    Example:
        tooltips = TooltipManager(root)
        tooltips.add_tooltip(button, "Click to save changes")
    """
    
    def __init__(
        self,
        root: tk.Tk,
        delay: int = 400,
        hide_delay: int = 8000,
        bg: Optional[str] = None,
        fg: Optional[str] = None
    ):
        """
        Initialize tooltip manager.
        
        Args:
            root: Root window
            delay: Delay before showing tooltip (ms)
            hide_delay: Auto-hide delay (ms)
            bg: Background color
            fg: Foreground color
        """
        self.root = root
        self._tooltip: Optional[tk.Toplevel] = None
        self._delay = delay
        self._hide_delay = hide_delay
        self._scheduled_id: Optional[str] = None
        
        self.colors = ModernTheme.get_all()
        self.bg = bg or self.colors['BG_DARK']
        self.fg = fg or ModernTheme.TEXT_ON_DARK
    
    def add_tooltip(
        self,
        widget: tk.Widget,
        text: str,
        position: str = 'bottom',
        wraplength: int = 350
    ):
        """
        Add a tooltip to a widget.
        
        Args:
            widget: Widget to attach tooltip to
            text: Tooltip text
            position: Position ('top', 'bottom', 'left', 'right')
            wraplength: Maximum line length
        """
        def on_enter(event):
            self._schedule(widget, text, position, wraplength)
        
        def on_leave(event):
            self._cancel_and_hide()
        
        widget.bind('<Enter>', on_enter)
        widget.bind('<Leave>', on_leave)
        widget.bind('<Button-1>', lambda e: self._cancel_and_hide())
    
    def _schedule(
        self,
        widget: tk.Widget,
        text: str,
        position: str,
        wraplength: int
    ):
        """Schedule tooltip display"""
        self._cancel_and_hide()
        self._scheduled_id = self.root.after(
            self._delay,
            lambda: self._show(widget, text, position, wraplength)
        )
    
    def _cancel_and_hide(self):
        """Cancel scheduled tooltip and hide current"""
        if self._scheduled_id:
            self.root.after_cancel(self._scheduled_id)
            self._scheduled_id = None
        self._hide()
    
    def _show(
        self,
        widget: tk.Widget,
        text: str,
        position: str,
        wraplength: int
    ):
        """Display the tooltip"""
        if self._tooltip:
            self._tooltip.destroy()
        
        self._tooltip = tk.Toplevel(self.root)
        self._tooltip.wm_overrideredirect(True)
        self._tooltip.wm_attributes('-topmost', True)
        self._tooltip.wm_attributes('-alpha', 0.98)
        
        # Frame
        frame = tk.Frame(
            self._tooltip,
            bg=self.bg,
            padx=12,
            pady=8,
            highlightbackground=self.colors['BORDER'],
            highlightthickness=1
        )
        frame.pack()
        
        # Label
        label = tk.Label(
            frame,
            text=text,
            bg=self.bg,
            fg=self.fg,
            font=FontConfig.body(),
            justify=tk.LEFT,
            wraplength=wraplength
        )
        label.pack()
        
        # Calculate position
        x, y = widget.winfo_rootx(), widget.winfo_rooty()
        widget_width = widget.winfo_width()
        widget_height = widget.winfo_height()
        
        tooltip_width = min(wraplength + 24, 400)
        tooltip_height = 60  # approximate
        
        if position == 'bottom':
            x = x + (widget_width - tooltip_width) // 2
            y = y + widget_height + 8
        elif position == 'top':
            x = x + (widget_width - tooltip_width) // 2
            y = y - tooltip_height - 8
        elif position == 'right':
            x = x + widget_width + 8
            y = y + (widget_height - tooltip_height) // 2
        elif position == 'left':
            x = x - tooltip_width - 8
            y = y + (widget_height - tooltip_height) // 2
        
        # Ensure tooltip stays on screen
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        x = max(10, min(x, screen_width - tooltip_width - 10))
        y = max(10, min(y, screen_height - tooltip_height - 10))
        
        self._tooltip.wm_geometry(f"+{x}+{y}")
        
        # Auto-hide
        self._scheduled_id = self.root.after(self._hide_delay, self._hide)
    
    def _hide(self):
        """Hide the tooltip"""
        if self._tooltip:
            self._tooltip.destroy()
            self._tooltip = None


# Export
__all__ = ['TooltipManager']
