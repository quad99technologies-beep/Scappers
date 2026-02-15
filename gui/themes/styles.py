#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Theme Styles - ttk Style Configuration

Applies modern styling to tkinter ttk widgets.
"""

from tkinter import ttk
from gui.themes.modern import ModernTheme, FontConfig


def apply_modern_styles(style: ttk.Style) -> None:
    """
    Apply modern styles to ttk widgets.
    
    Args:
        style: ttk.Style instance to configure
    """
    # Use clam theme as base for best customization
    try:
        style.theme_use('clam')
    except tk.TclError:
        pass  # Use default if clam not available
    
    colors = ModernTheme.get_all()
    
    # Frame styles
    style.configure('TFrame', background=colors['BG_MAIN'])
    style.configure('Card.TFrame', background=colors['BG_CARD'])
    
    # Label styles
    style.configure('TLabel',
                   background=colors['BG_MAIN'],
                   foreground=colors['TEXT_PRIMARY'],
                   font=FontConfig.body())
    
    style.configure('Header.TLabel',
                   background=colors['BG_MAIN'],
                   foreground=colors['TEXT_PRIMARY'],
                   font=FontConfig.header())
    
    style.configure('Title.TLabel',
                   background=colors['BG_MAIN'],
                   foreground=colors['TEXT_PRIMARY'],
                   font=FontConfig.title())
    
    style.configure('Subtitle.TLabel',
                   background=colors['BG_MAIN'],
                   foreground=colors['TEXT_SECONDARY'],
                   font=FontConfig.subtitle())
    
    style.configure('Muted.TLabel',
                   background=colors['BG_MAIN'],
                   foreground=colors['TEXT_MUTED'],
                   font=FontConfig.small())
    
    # Notebook styles
    style.configure('TNotebook',
                   background=colors['BG_MAIN'],
                   tabmargins=[2, 5, 2, 0])
    
    style.configure('TNotebook.Tab',
                   background=colors['BG_MAIN'],
                   foreground=colors['TEXT_SECONDARY'],
                   padding=[15, 8],
                   font=FontConfig.body())
    
    style.map('TNotebook.Tab',
             background=[('selected', colors['BG_CARD']),
                        ('active', colors['BORDER_LIGHT'])],
             foreground=[('selected', colors['PRIMARY']),
                        ('active', colors['TEXT_PRIMARY'])],
             expand=[('selected', [1, 1, 1, 0])])
    
    # Button styles
    style.configure('Primary.TButton',
                   background=colors['PRIMARY'],
                   foreground='white',
                   padding=[12, 8],
                   font=FontConfig.body_bold())
    
    style.map('Primary.TButton',
             background=[('active', colors['PRIMARY_DARK']),
                        ('pressed', colors['PRIMARY_DARK']),
                        ('disabled', colors['BORDER'])],
             foreground=[('disabled', colors['TEXT_MUTED'])])
    
    style.configure('Secondary.TButton',
                   background=colors['BG_CARD'],
                   foreground=colors['TEXT_PRIMARY'],
                   padding=[10, 6],
                   font=FontConfig.body())
    
    style.map('Secondary.TButton',
             background=[('active', colors['BORDER_LIGHT']),
                        ('pressed', colors['BORDER'])])
    
    style.configure('Success.TButton',
                   background=colors['SUCCESS'],
                   foreground='white',
                   padding=[10, 6],
                   font=FontConfig.body())
    
    style.map('Success.TButton',
             background=[('active', colors['SUCCESS_DARK']),
                        ('pressed', colors['SUCCESS_DARK'])])
    
    style.configure('Danger.TButton',
                   background=colors['ERROR'],
                   foreground='white',
                   padding=[10, 6],
                   font=FontConfig.body())
    
    style.map('Danger.TButton',
             background=[('active', colors['ERROR_DARK']),
                        ('pressed', colors['ERROR_DARK'])])
    
    style.configure('Ghost.TButton',
                   background='transparent',
                   foreground=colors['TEXT_SECONDARY'],
                   padding=[8, 4],
                   font=FontConfig.body())
    
    style.map('Ghost.TButton',
             background=[('active', colors['BORDER_LIGHT']),
                        ('pressed', colors['BORDER'])])
    
    # Entry styles
    style.configure('TEntry',
                   fieldbackground=colors['BG_INPUT'],
                   foreground=colors['TEXT_PRIMARY'],
                   padding=[8, 6],
                   font=FontConfig.body())
    
    style.map('TEntry',
             fieldbackground=[('focus', colors['BG_CARD'])],
             bordercolor=[('focus', colors['PRIMARY']),
                         ('!focus', colors['BORDER'])])
    
    # Combobox styles
    style.configure('TCombobox',
                   fieldbackground=colors['BG_INPUT'],
                   foreground=colors['TEXT_PRIMARY'],
                   padding=[6, 4],
                   font=FontConfig.body())
    
    style.map('TCombobox',
             fieldbackground=[('readonly', colors['BG_INPUT']),
                            ('!readonly', colors['BG_INPUT'])],
             selectbackground=[('readonly', colors['PRIMARY']),
                             ('!readonly', colors['PRIMARY'])])
    
    # Progressbar styles
    style.configure('Horizontal.TProgressbar',
                   background=colors['SUCCESS'],
                   troughcolor=colors['BORDER_LIGHT'],
                   thickness=8)
    
    style.configure('Vertical.TProgressbar',
                   background=colors['PRIMARY'],
                   troughcolor=colors['BORDER_LIGHT'],
                   thickness=8)
    
    # Treeview styles
    style.configure('Treeview',
                   background=colors['BG_CARD'],
                   foreground=colors['TEXT_PRIMARY'],
                   fieldbackground=colors['BG_CARD'],
                   font=FontConfig.body(),
                   rowheight=28)
    
    style.configure('Treeview.Heading',
                   background=colors['BORDER_LIGHT'],
                   foreground=colors['TEXT_PRIMARY'],
                   font=FontConfig.body_bold(),
                   padding=[8, 4])
    
    style.map('Treeview',
             background=[('selected', colors['PRIMARY']),
                        ('!selected', colors['BG_CARD'])],
             foreground=[('selected', 'white'),
                        ('!selected', colors['TEXT_PRIMARY'])])
    
    # Scrollbar styles
    style.configure('TScrollbar',
                   background=colors['BORDER_LIGHT'],
                   troughcolor=colors['BG_MAIN'],
                   borderwidth=0,
                   arrowsize=12)
    
    style.map('TScrollbar',
             background=[('active', colors['BORDER']),
                        ('!active', colors['BORDER_LIGHT'])])
    
    # Scale styles
    style.configure('TScale',
                   background=colors['BG_MAIN'],
                   troughcolor=colors['BORDER_LIGHT'])
    
    # Separator styles
    style.configure('TSeparator',
                   background=colors['BORDER_LIGHT'])
    
    # Labelframe styles
    style.configure('TLabelframe',
                   background=colors['BG_CARD'],
                   borderwidth=1,
                   relief='solid')
    
    style.configure('TLabelframe.Label',
                   background=colors['BG_CARD'],
                   foreground=colors['TEXT_SECONDARY'],
                   font=FontConfig.small())
    
    # Checkbutton styles
    style.configure('TCheckbutton',
                   background=colors['BG_MAIN'],
                   foreground=colors['TEXT_PRIMARY'],
                   font=FontConfig.body())
    
    # Radiobutton styles
    style.configure('TRadiobutton',
                   background=colors['BG_MAIN'],
                   foreground=colors['TEXT_PRIMARY'],
                   font=FontConfig.body())
    
    # Menubutton styles
    style.configure('TMenubutton',
                   background=colors['BG_INPUT'],
                   foreground=colors['TEXT_PRIMARY'],
                   padding=[8, 4],
                   font=FontConfig.body())


# Export
__all__ = ['apply_modern_styles']
