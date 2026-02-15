#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Card Component - Modern card-style container

Provides a card-based layout component with title, separator, and content area.
"""

import tkinter as tk
from typing import Optional
from gui.themes.modern import ModernTheme, FontConfig


class CardFrame:
    """
    Modern card-style container with title and content area.
    
    Features:
    - Clean white background with subtle border
    - Optional title with separator
    - Consistent padding
    - Hover effects support
    
    Example:
        card = CardFrame(parent, title="Settings", padding=16)
        card.pack(fill=tk.BOTH, expand=True)
        
        # Add content to card.content
        label = tk.Label(card.content, text="Content here")
        label.pack()
    """
    
    def __init__(
        self,
        parent: tk.Widget,
        title: Optional[str] = None,
        icon: Optional[str] = None,
        padding: int = 16,
        bg: Optional[str] = None,
        border: bool = True,
        shadow: bool = False
    ):
        """
        Initialize card frame.
        
        Args:
            parent: Parent widget
            title: Optional card title
            icon: Optional icon (from IconLibrary or unicode)
            padding: Internal padding
            bg: Background color (defaults to BG_CARD)
            border: Whether to show border
            shadow: Whether to simulate shadow (not implemented in tkinter)
        """
        self.parent = parent
        self.title = title
        self.padding = padding
        self.colors = ModernTheme.get_all()
        self.bg = bg or self.colors['BG_CARD']
        
        # Main frame
        self.frame = tk.Frame(
            parent,
            bg=self.bg,
            padx=0,
            pady=0
        )
        
        # Add border if requested
        if border:
            self.frame.config(
                highlightbackground=self.colors['BORDER_LIGHT'],
                highlightthickness=1
            )
        
        # Title section
        if title:
            self._create_title_section(title, icon)
        
        # Content frame
        self.content = tk.Frame(self.frame, bg=self.bg)
        self.content.pack(
            fill=tk.BOTH,
            expand=True,
            padx=padding,
            pady=padding if not title else (0, padding)
        )
    
    def _create_title_section(self, title: str, icon: Optional[str] = None):
        """Create title section with separator"""
        # Title frame
        self.title_frame = tk.Frame(self.frame, bg=self.bg)
        self.title_frame.pack(
            fill=tk.X,
            padx=self.padding,
            pady=(self.padding, 0)
        )
        
        # Icon and title
        display_text = title
        if icon:
            display_text = f"{icon} {title}"
        
        self.title_label = tk.Label(
            self.title_frame,
            text=display_text,
            bg=self.bg,
            fg=self.colors['TEXT_PRIMARY'],
            font=FontConfig.title()
        )
        self.title_label.pack(anchor='w')
        
        # Separator
        self.separator = tk.Frame(
            self.frame,
            height=1,
            bg=self.colors['BORDER_LIGHT']
        )
        self.separator.pack(
            fill=tk.X,
            padx=self.padding,
            pady=(8, 0)
        )
    
    def pack(self, **kwargs):
        """Pack the card frame"""
        self.frame.pack(**kwargs)
    
    def grid(self, **kwargs):
        """Grid the card frame"""
        self.frame.grid(**kwargs)
    
    def place(self, **kwargs):
        """Place the card frame"""
        self.frame.place(**kwargs)
    
    def set_title(self, title: str, icon: Optional[str] = None):
        """Update card title"""
        if hasattr(self, 'title_label'):
            display_text = title
            if icon:
                display_text = f"{icon} {title}"
            self.title_label.config(text=display_text)
    
    def set_bg(self, color: str):
        """Update background color"""
        self.bg = color
        self.frame.config(bg=color)
        self.content.config(bg=color)
        if hasattr(self, 'title_frame'):
            self.title_frame.config(bg=color)
            self.title_label.config(bg=color)
            self.separator.config(bg=color)


class CardGrid:
    """
    Grid layout for multiple cards.
    
    Manages a grid of cards with consistent spacing.
    """
    
    def __init__(
        self,
        parent: tk.Widget,
        columns: int = 2,
        spacing: int = 16,
        bg: Optional[str] = None
    ):
        """
        Initialize card grid.
        
        Args:
            parent: Parent widget
            columns: Number of columns
            spacing: Spacing between cards
            bg: Background color
        """
        self.parent = parent
        self.columns = columns
        self.spacing = spacing
        self.colors = ModernTheme.get_all()
        self.bg = bg or self.colors['BG_MAIN']
        
        self.frame = tk.Frame(parent, bg=self.bg)
        self.cards: list = []
        self.current_row = 0
        self.current_col = 0
    
    def add_card(
        self,
        title: Optional[str] = None,
        icon: Optional[str] = None,
        padding: int = 16,
        rowspan: int = 1,
        colspan: int = 1
    ) -> CardFrame:
        """
        Add a card to the grid.
        
        Args:
            title: Card title
            icon: Card icon
            padding: Card padding
            rowspan: Row span
            colspan: Column span
            
        Returns:
            Created CardFrame
        """
        card = CardFrame(
            self.frame,
            title=title,
            icon=icon,
            padding=padding
        )
        
        card.frame.grid(
            row=self.current_row,
            column=self.current_col,
            rowspan=rowspan,
            columnspan=colspan,
            padx=(0 if self.current_col == 0 else self.spacing // 2,
                  self.spacing // 2 if self.current_col < self.columns - 1 else 0),
            pady=(0, self.spacing),
            sticky='nsew'
        )
        
        self.cards.append(card)
        
        # Update position
        self.current_col += colspan
        if self.current_col >= self.columns:
            self.current_col = 0
            self.current_row += 1
        
        return card
    
    def configure_weights(self):
        """Configure grid weights for equal sizing"""
        for i in range(self.columns):
            self.frame.columnconfigure(i, weight=1)
    
    def pack(self, **kwargs):
        """Pack the grid frame"""
        self.frame.pack(**kwargs)
    
    def grid(self, **kwargs):
        """Grid the grid frame"""
        self.frame.grid(**kwargs)


# Export
__all__ = ['CardFrame', 'CardGrid']
