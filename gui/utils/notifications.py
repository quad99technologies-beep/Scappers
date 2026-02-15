#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Notification Utility - Toast notification system

Provides modern toast notifications with animations.
"""

import tkinter as tk
from typing import Optional, List, Dict, Any, Callable
from gui.themes.modern import ModernTheme, FontConfig, IconLibrary


class NotificationManager:
    """
    Modern toast notification system with animations.
    
    Features:
    - Multiple notification levels (info, success, warning, error)
    - Smooth animations
    - Auto-dismiss
    - Click to dismiss
    - Stacking support
    
    Example:
        notifications = NotificationManager(root)
        notifications.show("Operation completed", level='success')
    """
    
    def __init__(
        self,
        root: tk.Tk,
        max_notifications: int = 5,
        position: str = 'bottom-right',
        offset_x: int = 20,
        offset_y: int = 60
    ):
        """
        Initialize notification manager.
        
        Args:
            root: Root window
            max_notifications: Maximum notifications to show
            position: Position on screen
            offset_x: Horizontal offset from edge
            offset_y: Vertical offset from edge
        """
        self.root = root
        self._notifications: List[Dict[str, Any]] = []
        self._max_notifications = max_notifications
        self._position = position
        self._offset_x = offset_x
        self._offset_y = offset_y
        self._padding = 12
        self._notification_height = 70
        
        self.colors = ModernTheme.get_all()
    
    def show(
        self,
        message: str,
        level: str = 'info',
        duration: int = 4000,
        title: Optional[str] = None
    ):
        """
        Show a notification.
        
        Args:
            message: Notification message
            level: Notification level ('info', 'success', 'warning', 'error')
            duration: Display duration in ms
            title: Optional title
        """
        # Level configuration
        level_config = {
            'info': {
                'bg': ModernTheme.INFO,
                'fg': 'white',
                'icon': IconLibrary.INFO
            },
            'success': {
                'bg': ModernTheme.SUCCESS,
                'fg': 'white',
                'icon': IconLibrary.SUCCESS
            },
            'warning': {
                'bg': ModernTheme.WARNING,
                'fg': ModernTheme.TEXT_PRIMARY,
                'icon': IconLibrary.WARNING
            },
            'error': {
                'bg': ModernTheme.ERROR,
                'fg': 'white',
                'icon': IconLibrary.ERROR
            }
        }
        
        config = level_config.get(level, level_config['info'])
        
        # Create notification window
        notification = tk.Toplevel(self.root)
        notification.wm_overrideredirect(True)
        notification.wm_attributes('-topmost', True)
        notification.wm_attributes('-alpha', 0)
        
        # Main frame
        frame = tk.Frame(
            notification,
            bg=config['bg'],
            padx=0,
            pady=0
        )
        frame.pack(fill=tk.BOTH, expand=True)
        
        # Inner frame
        inner = tk.Frame(
            frame,
            bg=config['bg'],
            padx=16,
            pady=12
        )
        inner.pack(fill=tk.BOTH, expand=True)
        
        # Icon
        icon_label = tk.Label(
            inner,
            text=config['icon'],
            bg=config['bg'],
            fg=config['fg'],
            font=FontConfig.title()
        )
        icon_label.pack(side=tk.LEFT, padx=(0, 12))
        
        # Content frame
        content_frame = tk.Frame(inner, bg=config['bg'])
        content_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Title (if provided)
        if title:
            title_label = tk.Label(
                content_frame,
                text=title,
                bg=config['bg'],
                fg=config['fg'],
                font=FontConfig.body_bold()
            )
            title_label.pack(anchor='w')
        
        # Message
        msg_label = tk.Label(
            content_frame,
            text=message,
            bg=config['bg'],
            fg=config['fg'],
            font=FontConfig.body(),
            wraplength=320,
            justify=tk.LEFT
        )
        msg_label.pack(anchor='w')
        
        # Close button
        close_btn = tk.Label(
            inner,
            text="✕",
            bg=config['bg'],
            fg=config['fg'],
            font=FontConfig.body(),
            cursor='hand2',
            padx=4
        )
        close_btn.pack(side=tk.RIGHT)
        
        # Calculate position
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        
        notif_width = 380
        notif_height = self._notification_height
        
        if self._position == 'bottom-right':
            x = screen_width - notif_width - self._offset_x
            start_y = screen_height - self._offset_y
        elif self._position == 'top-right':
            x = screen_width - notif_width - self._offset_x
            start_y = self._offset_y
        else:
            x = screen_width - notif_width - self._offset_x
            start_y = screen_height - self._offset_y
        
        # Stack notifications
        index = len(self._notifications)
        if self._position.startswith('bottom'):
            y = start_y - (index * (notif_height + self._padding))
        else:
            y = start_y + (index * (notif_height + self._padding))
        
        notification.wm_geometry(f"{notif_width}x{notif_height}+{x}+{y}")
        
        # Store notification
        notif_data = {
            'window': notification,
            'y': y,
            'frame': frame
        }
        self._notifications.append(notif_data)
        
        # Animate in
        self._animate_in(notification, 0)
        
        # Click to dismiss
        for widget in [frame, inner, icon_label, msg_label]:
            if title:
                widget.bind('<Button-1>', lambda e, n=notif_data: self._dismiss(n))
        close_btn.bind('<Button-1>', lambda e, n=notif_data: self._dismiss(n))
        
        # Auto-dismiss
        def auto_dismiss():
            if notif_data in self._notifications:
                self._dismiss(notif_data)
        
        notification.after(duration, auto_dismiss)
        
        # Limit max notifications
        if len(self._notifications) > self._max_notifications:
            self._dismiss(self._notifications[0])
    
    def _animate_in(self, window: tk.Toplevel, step: int):
        """Animate notification appearing"""
        if step <= 10:
            alpha = step / 10
            window.wm_attributes('-alpha', alpha)
            self.root.after(20, lambda: self._animate_in(window, step + 1))
    
    def _dismiss(self, notif_data: Dict[str, Any]):
        """Dismiss a notification"""
        if notif_data not in self._notifications:
            return
        
        index = self._notifications.index(notif_data)
        self._notifications.remove(notif_data)
        
        # Animate out
        self._animate_out(notif_data['window'], 10, lambda: self._cleanup(notif_data, index))
    
    def _animate_out(
        self,
        window: tk.Toplevel,
        step: int,
        callback: Callable
    ):
        """Animate notification disappearing"""
        if step >= 0:
            alpha = step / 10
            window.wm_attributes('-alpha', alpha)
            self.root.after(15, lambda: self._animate_out(window, step - 1, callback))
        else:
            callback()
    
    def _cleanup(self, notif_data: Dict[str, Any], removed_index: int):
        """Clean up and reposition notifications"""
        notif_data['window'].destroy()
        
        # Reposition remaining
        screen_height = self.root.winfo_screenheight()
        
        if self._position.startswith('bottom'):
            start_y = screen_height - self._offset_y
        else:
            start_y = self._offset_y
        
        for i, notif in enumerate(self._notifications):
            if self._position.startswith('bottom'):
                new_y = start_y - (i * (self._notification_height + self._padding))
            else:
                new_y = start_y + (i * (self._notification_height + self._padding))
            
            notif['y'] = new_y
            self._animate_position(notif['window'], new_y)
    
    def _animate_position(self, window: tk.Toplevel, target_y: int):
        """Animate to new position"""
        current_geom = window.wm_geometry()
        parts = current_geom.split('+')
        if len(parts) >= 3:
            size = parts[0]
            x = parts[1]
            window.wm_geometry(f"{size}+{x}+{target_y}")


# Export
__all__ = ['NotificationManager']
