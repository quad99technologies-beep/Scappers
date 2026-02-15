#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Modern Theme - Professional Color Scheme and Icons

Provides a modern, professional color palette and Unicode icon library
for the Scraper Management System GUI.
"""

from typing import Dict, Tuple


class ModernTheme:
    """
    Modern professional color scheme with high contrast and accessibility.
    
    Designed for professional data processing applications with:
    - High contrast for readability
    - Consistent color semantics
    - Accessibility compliance (WCAG AA)
    - Dark mode ready structure
    """
    
    # Primary Colors - Professional Blue
    PRIMARY: str = "#2563eb"
    PRIMARY_DARK: str = "#1d4ed8"
    PRIMARY_LIGHT: str = "#3b82f6"
    PRIMARY_GLOW: str = "#dbeafe"
    
    # Secondary Colors
    SECONDARY: str = "#64748b"
    SECONDARY_DARK: str = "#475569"
    SECONDARY_LIGHT: str = "#94a3b8"
    
    # Background Colors
    BG_MAIN: str = "#f8fafc"           # Main window background
    BG_CARD: str = "#ffffff"           # Card/panel backgrounds
    BG_DARK: str = "#0f172a"           # Header, dark sections
    BG_CONSOLE: str = "#0d1117"        # Console/log background
    BG_INPUT: str = "#f1f5f9"          # Input field backgrounds
    BG_HOVER: str = "#f1f5f9"          # Hover states
    
    # Text Colors
    TEXT_PRIMARY: str = "#1e293b"      # Main text
    TEXT_SECONDARY: str = "#64748b"    # Secondary text
    TEXT_MUTED: str = "#94a3b8"        # Placeholder, disabled
    TEXT_CONSOLE: str = "#e6edf3"      # Console text
    TEXT_ON_DARK: str = "#f8fafc"      # Text on dark backgrounds
    TEXT_ON_PRIMARY: str = "#ffffff"   # Text on primary color
    
    # Accent Colors
    SUCCESS: str = "#10b981"
    SUCCESS_LIGHT: str = "#d1fae5"
    SUCCESS_DARK: str = "#059669"
    
    WARNING: str = "#f59e0b"
    WARNING_LIGHT: str = "#fef3c7"
    WARNING_DARK: str = "#d97706"
    
    ERROR: str = "#ef4444"
    ERROR_LIGHT: str = "#fee2e2"
    ERROR_DARK: str = "#dc2626"
    
    INFO: str = "#06b6d4"
    INFO_LIGHT: str = "#cffafe"
    INFO_DARK: str = "#0891b2"
    
    # Border Colors
    BORDER_LIGHT: str = "#e2e8f0"
    BORDER: str = "#cbd5e1"
    BORDER_FOCUS: str = "#2563eb"
    BORDER_ERROR: str = "#ef4444"
    BORDER_SUCCESS: str = "#10b981"
    
    # Status Colors
    STATUS_RUNNING: str = "#10b981"
    STATUS_STOPPED: str = "#ef4444"
    STATUS_PAUSED: str = "#f59e0b"
    STATUS_IDLE: str = "#94a3b8"
    STATUS_LOADING: str = "#06b6d4"
    
    # Gradients (simulated with tuples)
    GRADIENT_PRIMARY: Tuple[str, str] = ("#2563eb", "#1d4ed8")
    GRADIENT_SUCCESS: Tuple[str, str] = ("#10b981", "#059669")
    GRADIENT_WARNING: Tuple[str, str] = ("#f59e0b", "#d97706")
    GRADIENT_ERROR: Tuple[str, str] = ("#ef4444", "#dc2626")
    
    @classmethod
    def get_all(cls) -> Dict[str, str]:
        """
        Get all color values as a dictionary.
        
        Returns:
            Dictionary mapping color names to hex values
        """
        return {
            k: v for k, v in cls.__dict__.items()
            if not k.startswith('_') and isinstance(v, str) and v.startswith('#')
        }
    
    @classmethod
    def get_status_color(cls, status: str) -> str:
        """
        Get color for a status string.
        
        Args:
            status: Status name ('running', 'stopped', 'paused', 'idle', etc.)
            
        Returns:
            Hex color code for the status
        """
        status_map = {
            'running': cls.STATUS_RUNNING,
            'active': cls.STATUS_RUNNING,
            'success': cls.SUCCESS,
            'completed': cls.SUCCESS,
            'stopped': cls.STATUS_STOPPED,
            'error': cls.ERROR,
            'failed': cls.ERROR,
            'paused': cls.STATUS_PAUSED,
            'warning': cls.WARNING,
            'idle': cls.STATUS_IDLE,
            'loading': cls.STATUS_LOADING,
            'pending': cls.STATUS_LOADING,
        }
        return status_map.get(status.lower(), cls.STATUS_IDLE)


class IconLibrary:
    """
    Unicode icon library for the application.
    
    Uses Unicode characters that are widely supported across platforms.
    Falls back gracefully if a character is not supported.
    """
    
    # Navigation & Main
    DASHBOARD: str = "📊"
    HOME: str = "🏠"
    INPUT: str = "📥"
    OUTPUT: str = "📤"
    CONFIG: str = "⚙"
    SETTINGS: str = "🔧"
    HEALTH: str = "🏥"
    PIPELINE: str = "🔄"
    DOCS: str = "📚"
    HELP: str = "❓"
    INFO: str = "ℹ"
    
    # Actions
    PLAY: str = "▶"
    PAUSE: str = "⏸"
    STOP: str = "⏹"
    REFRESH: str = "🔄"
    RELOAD: str = "⟳"
    SAVE: str = "💾"
    COPY: str = "📋"
    CLEAR: str = "🗑"
    DELETE: str = "🗑"
    EDIT: str = "✏"
    ADD: str = "➕"
    REMOVE: str = "➖"
    SEARCH: str = "🔍"
    FILTER: str = "🔽"
    SORT: str = "📶"
    
    # Status
    RUNNING: str = "🟢"
    STOPPED: str = "🔴"
    PAUSED: str = "🟡"
    IDLE: str = "⚪"
    LOADING: str = "⏳"
    SUCCESS: str = "✓"
    ERROR: str = "✗"
    WARNING: str = "⚠"
    CHECK: str = "✓"
    CROSS: str = "✕"
    
    # File Operations
    FOLDER: str = "📁"
    FILE: str = "📄"
    DOWNLOAD: str = "⬇"
    UPLOAD: str = "⬆"
    EXPORT: str = "📤"
    IMPORT: str = "📥"
    OPEN: str = "📂"
    CLOSE: str = "🚪"
    
    # Data & Database
    DATABASE: str = "🗄"
    TABLE: str = "📋"
    CHART: str = "📈"
    GRAPH: str = "📉"
    LIST: str = "📃"
    GRID: str = "⊞"
    
    # System
    CPU: str = "🖥"
    MEMORY: str = "💾"
    NETWORK: str = "🌐"
    CHROME: str = "🌐"
    TOR: str = "🔒"
    VPN: str = "🔐"
    GPU: str = "🎮"
    SERVER: str = "🖧"
    
    # Misc
    CHECKPOINT: str = "🚩"
    LOCK: str = "🔒"
    UNLOCK: str = "🔓"
    STAR: str = "⭐"
    FLAG: str = "🚩"
    TAG: str = "🏷"
    LINK: str = "🔗"
    TIME: str = "⏱"
    CALENDAR: str = "📅"
    CLOCK: str = "🕐"
    USER: str = "👤"
    USERS: str = "👥"
    BOT: str = "🤖"
    ROBOT: str = "🤖"
    GEAR: str = "⚙"
    TOOLS: str = "🛠"
    MAGIC: str = "✨"
    LIGHTBULB: str = "💡"
    BELL: str = "🔔"
    MAIL: str = "📧"
    
    @classmethod
    def get(cls, name: str, default: str = "") -> str:
        """
        Get icon by name.
        
        Args:
            name: Icon name (case-insensitive)
            default: Default value if icon not found
            
        Returns:
            Unicode icon character
        """
        return getattr(cls, name.upper(), default)
    
    @classmethod
    def with_text(cls, icon_name: str, text: str, separator: str = " ") -> str:
        """
        Get icon with text combined.
        
        Args:
            icon_name: Name of the icon
            text: Text to display after icon
            separator: Separator between icon and text
            
        Returns:
            Combined icon and text string
        """
        icon = cls.get(icon_name, "")
        return f"{icon}{separator}{text}" if icon else text


# Font configurations
class FontConfig:
    """Font configuration for the application"""
    
    FAMILY: str = "Segoe UI"  # Primary font family
    FALLBACK: str = "Arial"    # Fallback font
    
    # Font sizes
    HEADER: int = 12
    TITLE: int = 11
    SUBTITLE: int = 10
    BODY: int = 9
    SMALL: int = 8
    
    # Font styles
    @classmethod
    def header(cls) -> tuple:
        return (cls.FAMILY, cls.HEADER, "bold")
    
    @classmethod
    def title(cls) -> tuple:
        return (cls.FAMILY, cls.TITLE, "bold")
    
    @classmethod
    def subtitle(cls) -> tuple:
        return (cls.FAMILY, cls.SUBTITLE, "bold")
    
    @classmethod
    def body(cls) -> tuple:
        return (cls.FAMILY, cls.BODY)
    
    @classmethod
    def body_bold(cls) -> tuple:
        return (cls.FAMILY, cls.BODY, "bold")
    
    @classmethod
    def small(cls) -> tuple:
        return (cls.FAMILY, cls.SMALL)
    
    @classmethod
    def monospace(cls, size: int = 9) -> tuple:
        return ("Consolas", size)


# Export all
__all__ = ['ModernTheme', 'IconLibrary', 'FontConfig']
