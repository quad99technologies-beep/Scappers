#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Button Components - Modern styled buttons

Provides button components with hover effects and multiple styles.
"""

import tkinter as tk
from typing import Optional, Callable, Dict, Any
from gui.themes.modern import ModernTheme, FontConfig, IconLibrary


class ModernButton:
    """
    Modern styled button with hover effects.
    
    Features:
    - Multiple style variants (primary, secondary, success, danger, ghost)
    - Hover and active states
    - Icon support
    - Disabled state handling
    
    Example:
        btn = ModernButton(
            parent,
            text="Save",
            icon=IconLibrary.SAVE,
            style='primary',
            command=on_save
        )
        btn.pack()
    """
    
    # Style configurations
    STYLES: Dict[str, Dict[str, Any]] = {
        'primary': {
            'bg': ModernTheme.PRIMARY,
            'fg': 'white',
            'hover_bg': ModernTheme.PRIMARY_DARK,
            'active_bg': ModernTheme.PRIMARY_DARK,
            'disabled_bg': ModernTheme.BORDER,
            'disabled_fg': ModernTheme.TEXT_MUTED,
            'font': FontConfig.body_bold()
        },
        'secondary': {
            'bg': ModernTheme.BG_CARD,
            'fg': ModernTheme.TEXT_PRIMARY,
            'hover_bg': ModernTheme.BORDER_LIGHT,
            'active_bg': ModernTheme.BORDER,
            'disabled_bg': ModernTheme.BORDER_LIGHT,
            'disabled_fg': ModernTheme.TEXT_MUTED,
            'font': FontConfig.body()
        },
        'success': {
            'bg': ModernTheme.SUCCESS,
            'fg': 'white',
            'hover_bg': ModernTheme.SUCCESS_DARK,
            'active_bg': ModernTheme.SUCCESS_DARK,
            'disabled_bg': ModernTheme.BORDER,
            'disabled_fg': ModernTheme.TEXT_MUTED,
            'font': FontConfig.body_bold()
        },
        'danger': {
            'bg': ModernTheme.ERROR,
            'fg': 'white',
            'hover_bg': ModernTheme.ERROR_DARK,
            'active_bg': ModernTheme.ERROR_DARK,
            'disabled_bg': ModernTheme.BORDER,
            'disabled_fg': ModernTheme.TEXT_MUTED,
            'font': FontConfig.body_bold()
        },
        'warning': {
            'bg': ModernTheme.WARNING,
            'fg': ModernTheme.TEXT_PRIMARY,
            'hover_bg': ModernTheme.WARNING_DARK,
            'active_bg': ModernTheme.WARNING_DARK,
            'disabled_bg': ModernTheme.BORDER,
            'disabled_fg': ModernTheme.TEXT_MUTED,
            'font': FontConfig.body_bold()
        },
        'ghost': {
            'bg': 'transparent',
            'fg': ModernTheme.TEXT_SECONDARY,
            'hover_bg': ModernTheme.BORDER_LIGHT,
            'active_bg': ModernTheme.BORDER,
            'disabled_bg': 'transparent',
            'disabled_fg': ModernTheme.TEXT_MUTED,
            'font': FontConfig.body()
        },
        'link': {
            'bg': 'transparent',
            'fg': ModernTheme.PRIMARY,
            'hover_bg': 'transparent',
            'active_bg': 'transparent',
            'disabled_bg': 'transparent',
            'disabled_fg': ModernTheme.TEXT_MUTED,
            'font': FontConfig.body(),
            'underline': True
        }
    }
    
    def __init__(
        self,
        parent: tk.Widget,
        text: str,
        command: Optional[Callable] = None,
        style: str = 'primary',
        icon: Optional[str] = None,
        width: Optional[int] = None,
        height: Optional[int] = None,
        padx: int = 16,
        pady: int = 8,
        **kwargs
    ):
        """
        Initialize modern button.
        
        Args:
            parent: Parent widget
            text: Button text
            command: Click callback
            style: Button style ('primary', 'secondary', 'success', 'danger', 'warning', 'ghost', 'link')
            icon: Optional icon (from IconLibrary)
            width: Button width
            height: Button height
            padx: Horizontal padding
            pady: Vertical padding
        """
        self.parent = parent
        self.text = text
        self.command = command
        self.style_name = style
        self.style_config = self.STYLES.get(style, self.STYLES['primary'])
        self.icon = icon
        self.is_disabled = False
        
        # Create button frame
        self.frame = tk.Frame(
            parent,
            bg=self.style_config['bg'],
            padx=padx,
            pady=pady,
            cursor='hand2',
            **kwargs
        )
        
        # Content
        content_text = f"{icon} {text}" if icon else text
        
        self.label = tk.Label(
            self.frame,
            text=content_text,
            bg=self.style_config['bg'],
            fg=self.style_config['fg'],
            font=self.style_config['font']
        )
        
        if width:
            self.label.config(width=width)
        self.label.pack()
        
        # Underline for link style
        if style == 'link' and self.style_config.get('underline'):
            self.label.config(font=(FontConfig.FAMILY, FontConfig.BODY, 'underline'))
        
        # Bind events
        self._bind_events()
    
    def _bind_events(self):
        """Bind mouse events"""
        self.frame.bind('<Enter>', self._on_enter)
        self.frame.bind('<Leave>', self._on_leave)
        self.label.bind('<Enter>', self._on_enter)
        self.label.bind('<Leave>', self._on_leave)
        
        if self.command:
            self.frame.bind('<Button-1>', self._on_click)
            self.label.bind('<Button-1>', self._on_click)
    
    def _on_enter(self, event):
        """Handle mouse enter"""
        if not self.is_disabled:
            self.frame.config(bg=self.style_config['hover_bg'])
            self.label.config(bg=self.style_config['hover_bg'])
    
    def _on_leave(self, event):
        """Handle mouse leave"""
        if not self.is_disabled:
            self.frame.config(bg=self.style_config['bg'])
            self.label.config(bg=self.style_config['bg'])
    
    def _on_click(self, event):
        """Handle click"""
        if not self.is_disabled and self.command:
            # Visual feedback
            self.frame.config(bg=self.style_config['active_bg'])
            self.label.config(bg=self.style_config['active_bg'])
            self.frame.after(100, lambda: self._on_leave(None))
            
            # Execute command
            self.command()
    
    def pack(self, **kwargs):
        """Pack the button"""
        self.frame.pack(**kwargs)
    
    def grid(self, **kwargs):
        """Grid the button"""
        self.frame.grid(**kwargs)
    
    def place(self, **kwargs):
        """Place the button"""
        self.frame.place(**kwargs)
    
    def config(
        self,
        text: Optional[str] = None,
        command: Optional[Callable] = None,
        state: Optional[str] = None,
        icon: Optional[str] = None
    ):
        """Configure button properties"""
        if text is not None:
            self.text = text
            content_text = f"{self.icon} {text}" if self.icon else text
            self.label.config(text=content_text)
        
        if icon is not None:
            self.icon = icon
            content_text = f"{icon} {self.text}"
            self.label.config(text=content_text)
        
        if command is not None:
            self.command = command
        
        if state is not None:
            self.set_state(state)
    
    def set_state(self, state: str):
        """
        Set button state.
        
        Args:
            state: 'normal' or 'disabled'
        """
        if state == tk.DISABLED or state == 'disabled':
            self.is_disabled = True
            self.label.config(
                fg=self.style_config['disabled_fg']
            )
            self.frame.config(
                bg=self.style_config['disabled_bg'],
                cursor=''
            )
        else:
            self.is_disabled = False
            self.label.config(fg=self.style_config['fg'])
            self.frame.config(
                bg=self.style_config['bg'],
                cursor='hand2'
            )
    
    def set_style(self, style: str):
        """Change button style"""
        self.style_name = style
        self.style_config = self.STYLES.get(style, self.STYLES['primary'])
        
        if not self.is_disabled:
            self.frame.config(bg=self.style_config['bg'])
            self.label.config(
                bg=self.style_config['bg'],
                fg=self.style_config['fg'],
                font=self.style_config['font']
            )


class ButtonGroup:
    """
    Group of related buttons.
    
    Manages spacing and alignment for multiple buttons.
    """
    
    def __init__(
        self,
        parent: tk.Widget,
        orientation: str = 'horizontal',
        spacing: int = 8,
        align: str = 'right'
    ):
        """
        Initialize button group.
        
        Args:
            parent: Parent widget
            orientation: 'horizontal' or 'vertical'
            spacing: Spacing between buttons
            align: Alignment ('left', 'center', 'right', 'fill')
        """
        self.parent = parent
        self.orientation = orientation
        self.spacing = spacing
        self.align = align
        self.buttons: list = []
        
        self.frame = tk.Frame(parent)
        
        if orientation == 'horizontal':
            if align == 'right':
                self.frame.columnconfigure(0, weight=1)
        else:
            if align == 'fill':
                for i in range(10):  # Assume max 10 buttons
                    self.frame.rowconfigure(i, weight=1)
    
    def add_button(
        self,
        text: str,
        command: Optional[Callable] = None,
        style: str = 'primary',
        icon: Optional[str] = None
    ) -> ModernButton:
        """Add a button to the group"""
        btn = ModernButton(
            self.frame,
            text=text,
            command=command,
            style=style,
            icon=icon
        )
        
        if self.orientation == 'horizontal':
            side = tk.RIGHT if self.align == 'right' else tk.LEFT
            btn.pack(side=side, padx=(0, self.spacing))
        else:
            fill = tk.X if self.align == 'fill' else tk.NONE
            btn.pack(fill=fill, pady=(0, self.spacing))
        
        self.buttons.append(btn)
        return btn
    
    def pack(self, **kwargs):
        """Pack the button group"""
        self.frame.pack(**kwargs)
    
    def grid(self, **kwargs):
        """Grid the button group"""
        self.frame.grid(**kwargs)


# Export
__all__ = ['ModernButton', 'ButtonGroup']
