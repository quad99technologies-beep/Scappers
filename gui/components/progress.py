#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Progress Components - Progress indicators and bars

Provides modern progress indicators with status text and animations.
"""

import tkinter as tk
from typing import Optional
from gui.themes.modern import ModernTheme, FontConfig


class ProgressIndicator:
    """
    Modern progress indicator with percentage and status.
    
    Features:
    - Custom progress bar with smooth rendering
    - Percentage display
    - Status text
    - Color coding
    
    Example:
        progress = ProgressIndicator(parent)
        progress.pack(fill=tk.X)
        
        # Update progress
        progress.set_progress(50, "Processing...")
    """
    
    def __init__(
        self,
        parent: tk.Widget,
        show_percentage: bool = True,
        height: int = 8,
        width: int = 200
    ):
        """
        Initialize progress indicator.
        
        Args:
            parent: Parent widget
            show_percentage: Whether to show percentage
            height: Bar height
            width: Bar width
        """
        self.parent = parent
        self.show_percentage = show_percentage
        self.height = height
        self.colors = ModernTheme.get_all()
        
        # Container
        self.frame = tk.Frame(parent, bg=self.colors['BG_CARD'])
        
        # Status label
        self.status_label = tk.Label(
            self.frame,
            text="Ready",
            bg=self.colors['BG_CARD'],
            fg=self.colors['TEXT_SECONDARY'],
            font=FontConfig.body(),
            anchor='w'
        )
        self.status_label.pack(fill=tk.X, pady=(0, 4))
        
        # Progress frame
        self.progress_frame = tk.Frame(self.frame, bg=self.colors['BG_CARD'])
        self.progress_frame.pack(fill=tk.X)
        
        # Canvas for custom progress bar
        self.canvas = tk.Canvas(
            self.progress_frame,
            height=height,
            bg=self.colors['BORDER_LIGHT'],
            highlightthickness=0
        )
        self.canvas.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        # Percentage label
        if show_percentage:
            self.percent_label = tk.Label(
                self.progress_frame,
                text="0%",
                bg=self.colors['BG_CARD'],
                fg=self.colors['TEXT_PRIMARY'],
                font=FontConfig.body_bold(),
                width=5
            )
            self.percent_label.pack(side=tk.RIGHT, padx=(12, 0))
        
        self._progress = 0
        self._status = "Ready"
        
        # Bind to resize
        self.canvas.bind('<Configure>', lambda e: self._draw())
    
    def _draw(self):
        """Draw the progress bar"""
        self.canvas.delete('all')
        
        width = self.canvas.winfo_width()
        if width < 10:
            return
        
        height = self.height
        
        # Background
        self.canvas.create_rectangle(
            0, 0, width, height,
            fill=self.colors['BORDER_LIGHT'],
            outline=''
        )
        
        # Progress fill
        if self._progress > 0:
            fill_width = (self._progress / 100) * width
            
            # Determine color based on progress
            if self._progress >= 100:
                color = self.colors['SUCCESS']
            elif self._progress >= 75:
                color = self.colors['PRIMARY']
            elif self._progress >= 50:
                color = self.colors['INFO']
            elif self._progress >= 25:
                color = self.colors['WARNING']
            else:
                color = self.colors['ERROR']
            
            self.canvas.create_rectangle(
                0, 0, fill_width, height,
                fill=color,
                outline=''
            )
    
    def set_progress(self, value: float, status: Optional[str] = None):
        """
        Update progress.
        
        Args:
            value: Progress percentage (0-100)
            status: Optional status text
        """
        self._progress = max(0, min(100, value))
        
        self._draw()
        
        if self.show_percentage:
            self.percent_label.config(text=f"{self._progress:.1f}%")
        
        if status:
            self._status = status
            self.status_label.config(text=status)
    
    def set_status(self, status: str):
        """Update status text only"""
        self._status = status
        self.status_label.config(text=status)
    
    def increment(self, amount: float = 1):
        """Increment progress by amount"""
        self.set_progress(self._progress + amount)
    
    def complete(self, status: str = "Completed"):
        """Mark as complete"""
        self.set_progress(100, status)
    
    def reset(self, status: str = "Ready"):
        """Reset progress"""
        self.set_progress(0, status)
    
    def pack(self, **kwargs):
        """Pack the widget"""
        self.frame.pack(**kwargs)
    
    def grid(self, **kwargs):
        """Grid the widget"""
        self.frame.grid(**kwargs)


class CircularProgress:
    """
    Circular progress indicator (simulated with canvas).
    
    Example:
        progress = CircularProgress(parent, size=50)
        progress.pack()
        progress.set_progress(75)
    """
    
    def __init__(
        self,
        parent: tk.Widget,
        size: int = 50,
        thickness: int = 4,
        show_percentage: bool = True
    ):
        """
        Initialize circular progress.
        
        Args:
            parent: Parent widget
            size: Diameter of the circle
            thickness: Line thickness
            show_percentage: Whether to show percentage in center
        """
        self.size = size
        self.thickness = thickness
        self.colors = ModernTheme.get_all()
        
        self.frame = tk.Frame(parent, bg=self.colors['BG_CARD'])
        
        self.canvas = tk.Canvas(
            self.frame,
            width=size,
            height=size,
            bg=self.colors['BG_CARD'],
            highlightthickness=0
        )
        self.canvas.pack()
        
        self._progress = 0
        self._draw()
    
    def _draw(self):
        """Draw the circular progress"""
        self.canvas.delete('all')
        
        center = self.size // 2
        radius = (self.size - self.thickness) // 2
        
        # Background circle
        self.canvas.create_oval(
            center - radius, center - radius,
            center + radius, center + radius,
            outline=self.colors['BORDER_LIGHT'],
            width=self.thickness
        )
        
        # Progress arc
        if self._progress > 0:
            extent = (self._progress / 100) * 360
            
            if self._progress >= 100:
                color = self.colors['SUCCESS']
            else:
                color = self.colors['PRIMARY']
            
            self.canvas.create_arc(
                center - radius, center - radius,
                center + radius, center + radius,
                start=90,
                extent=-extent,
                outline=color,
                width=self.thickness,
                style='arc'
            )
        
        # Percentage text
        if self._progress < 100:
            self.canvas.create_text(
                center, center,
                text=f"{int(self._progress)}%",
                fill=self.colors['TEXT_PRIMARY'],
                font=FontConfig.small()
            )
    
    def set_progress(self, value: float):
        """Update progress"""
        self._progress = max(0, min(100, value))
        self._draw()
    
    def pack(self, **kwargs):
        """Pack the widget"""
        self.frame.pack(**kwargs)
    
    def grid(self, **kwargs):
        """Grid the widget"""
        self.frame.grid(**kwargs)


class StepProgress:
    """
    Multi-step progress indicator.
    
    Shows progress through multiple discrete steps.
    
    Example:
        steps = ["Step 1", "Step 2", "Step 3", "Step 4"]
        progress = StepProgress(parent, steps=steps)
        progress.pack(fill=tk.X)
        progress.set_step(2)  # Set to step 2
    """
    
    def __init__(
        self,
        parent: tk.Widget,
        steps: list,
        current_step: int = 0
    ):
        """
        Initialize step progress.
        
        Args:
            parent: Parent widget
            steps: List of step names
            current_step: Current step index (0-based)
        """
        self.steps = steps
        self.current_step = current_step
        self.colors = ModernTheme.get_all()
        
        self.frame = tk.Frame(parent, bg=self.colors['BG_CARD'])
        
        # Create step indicators
        self.step_frames = []
        self.step_labels = []
        
        for i, step in enumerate(steps):
            step_frame = tk.Frame(self.frame, bg=self.colors['BG_CARD'])
            step_frame.pack(side=tk.LEFT, expand=True)
            
            # Step number/indicator
            if i < current_step:
                # Completed
                indicator = tk.Label(
                    step_frame,
                    text="✓",
                    bg=self.colors['SUCCESS'],
                    fg='white',
                    font=FontConfig.body_bold(),
                    width=2
                )
            elif i == current_step:
                # Current
                indicator = tk.Label(
                    step_frame,
                    text=str(i + 1),
                    bg=self.colors['PRIMARY'],
                    fg='white',
                    font=FontConfig.body_bold(),
                    width=2
                )
            else:
                # Pending
                indicator = tk.Label(
                    step_frame,
                    text=str(i + 1),
                    bg=self.colors['BORDER_LIGHT'],
                    fg=self.colors['TEXT_MUTED'],
                    font=FontConfig.body(),
                    width=2
                )
            
            indicator.pack()
            
            # Step label
            label = tk.Label(
                step_frame,
                text=step,
                bg=self.colors['BG_CARD'],
                fg=self.colors['TEXT_SECONDARY'] if i > current_step else self.colors['TEXT_PRIMARY'],
                font=FontConfig.small()
            )
            label.pack()
            
            self.step_frames.append(step_frame)
            self.step_labels.append(label)
            
            # Connector line (except for last step)
            if i < len(steps) - 1:
                connector = tk.Frame(
                    self.frame,
                    height=2,
                    width=40,
                    bg=self.colors['SUCCESS'] if i < current_step else self.colors['BORDER_LIGHT']
                )
                connector.pack(side=tk.LEFT, padx=5)
    
    def set_step(self, step: int):
        """Set current step"""
        self.current_step = step
        # Rebuild the widget
        for widget in self.frame.winfo_children():
            widget.destroy()
        self.__init__(self.frame.master, self.steps, step)
    
    def pack(self, **kwargs):
        """Pack the widget"""
        self.frame.pack(**kwargs)
    
    def grid(self, **kwargs):
        """Grid the widget"""
        self.frame.grid(**kwargs)


# Export
__all__ = ['ProgressIndicator', 'CircularProgress', 'StepProgress']
