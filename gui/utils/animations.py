#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Animation Utility - UI animations and transitions

Provides smooth animations for UI elements.
"""

import tkinter as tk
from typing import Optional, Callable, Dict, Any


class AnimationManager:
    """
    UI animation manager.
    
    Features:
    - Fade in/out animations
    - Slide animations
    - Pulse effects
    - Smooth transitions
    
    Example:
        animations = AnimationManager(root)
        animations.fade_in(widget, duration=300)
    """
    
    def __init__(self, root: tk.Tk):
        """
        Initialize animation manager.
        
        Args:
            root: Root window
        """
        self.root = root
        self._animations: Dict[str, Any] = {}
        self._running = True
        self._counter = 0
    
    def fade_in(
        self,
        widget: tk.Widget,
        duration: int = 300,
        callback: Optional[Callable] = None
    ):
        """
        Fade in a widget.
        
        Args:
            widget: Widget to animate
            duration: Animation duration in ms
            callback: Optional callback after animation
        """
        steps = 20
        delay = duration // steps
        anim_id = self._get_id()
        
        def animate(step=0):
            if not self._running or anim_id not in self._animations:
                return
            
            if step > steps:
                if callback:
                    callback()
                self._cleanup(anim_id)
                return
            
            alpha = step / steps
            # For tkinter, we can't easily set alpha on most widgets
            # This is a placeholder for future implementation
            
            self._animations[anim_id]['after_id'] = self.root.after(
                delay, lambda: animate(step + 1)
            )
        
        self._animations[anim_id] = {'widget': widget}
        animate()
    
    def fade_out(
        self,
        widget: tk.Widget,
        duration: int = 300,
        callback: Optional[Callable] = None
    ):
        """
        Fade out a widget.
        
        Args:
            widget: Widget to animate
            duration: Animation duration in ms
            callback: Optional callback after animation
        """
        steps = 20
        delay = duration // steps
        anim_id = self._get_id()
        
        def animate(step=0):
            if not self._running or anim_id not in self._animations:
                return
            
            if step > steps:
                if callback:
                    callback()
                self._cleanup(anim_id)
                return
            
            alpha = 1 - (step / steps)
            # Placeholder for fade implementation
            
            self._animations[anim_id]['after_id'] = self.root.after(
                delay, lambda: animate(step + 1)
            )
        
        self._animations[anim_id] = {'widget': widget}
        animate()
    
    def slide_in(
        self,
        widget: tk.Widget,
        direction: str = 'left',
        distance: int = 50,
        duration: int = 300,
        callback: Optional[Callable] = None
    ):
        """
        Slide in a widget.
        
        Args:
            widget: Widget to animate
            direction: Direction ('left', 'right', 'top', 'bottom')
            distance: Slide distance in pixels
            duration: Animation duration in ms
            callback: Optional callback after animation
        """
        steps = 20
        delay = duration // steps
        anim_id = self._get_id()
        
        # Store original position
        original_x = widget.winfo_x()
        original_y = widget.winfo_y()
        
        # Calculate start position
        if direction == 'left':
            start_x = original_x - distance
            start_y = original_y
        elif direction == 'right':
            start_x = original_x + distance
            start_y = original_y
        elif direction == 'top':
            start_x = original_x
            start_y = original_y - distance
        else:  # bottom
            start_x = original_x
            start_y = original_y + distance
        
        # Set initial position
        widget.place(x=start_x, y=start_y)
        
        def animate(step=0):
            if not self._running or anim_id not in self._animations:
                return
            
            if step > steps:
                widget.place(x=original_x, y=original_y)
                if callback:
                    callback()
                self._cleanup(anim_id)
                return
            
            # Easing function: ease-out
            progress = step / steps
            eased = 1 - (1 - progress) ** 3
            
            current_x = start_x + (original_x - start_x) * eased
            current_y = start_y + (original_y - start_y) * eased
            
            widget.place(x=int(current_x), y=int(current_y))
            
            self._animations[anim_id]['after_id'] = self.root.after(
                delay, lambda: animate(step + 1)
            )
        
        self._animations[anim_id] = {
            'widget': widget,
            'original_x': original_x,
            'original_y': original_y
        }
        animate()
    
    def pulse(
        self,
        widget: tk.Widget,
        duration: int = 1000,
        min_scale: float = 0.95,
        max_scale: float = 1.05
    ):
        """
        Create a pulsing effect on a widget.
        
        Args:
            widget: Widget to animate
            duration: Pulse duration in ms
            min_scale: Minimum scale
            max_scale: Maximum scale
        """
        steps = 30
        delay = duration // steps
        anim_id = self._get_id()
        
        def animate(step=0):
            if not self._running or anim_id not in self._animations:
                return
            
            # Sine wave for smooth pulsing
            import math
            progress = (step / steps) * 2 * math.pi
            scale = 1 + (max_scale - 1) * math.sin(progress) * 0.5
            
            # Placeholder for scale implementation
            # In practice, this would require canvas or custom drawing
            
            if step < steps * 3:  # Pulse 3 times
                self._animations[anim_id]['after_id'] = self.root.after(
                    delay, lambda: animate(step + 1)
                )
            else:
                self._cleanup(anim_id)
        
        self._animations[anim_id] = {'widget': widget}
        animate()
    
    def stop_all(self):
        """Stop all animations"""
        self._running = False
        
        for anim_id, data in list(self._animations.items()):
            if 'after_id' in data:
                self.root.after_cancel(data['after_id'])
        
        self._animations.clear()
    
    def _get_id(self) -> str:
        """Get unique animation ID"""
        self._counter += 1
        return f"anim_{self._counter}"
    
    def _cleanup(self, anim_id: str):
        """Clean up animation"""
        if anim_id in self._animations:
            del self._animations[anim_id]


class TransitionManager:
    """
    Manage transitions between views or states.
    
    Example:
        transitions = TransitionManager(root)
        transitions.crossfade(frame1, frame2, duration=500)
    """
    
    def __init__(self, root: tk.Tk):
        """
        Initialize transition manager.
        
        Args:
            root: Root window
        """
        self.root = root
    
    def crossfade(
        self,
        from_widget: tk.Widget,
        to_widget: tk.Widget,
        duration: int = 500
    ):
        """
        Crossfade between two widgets.
        
        Args:
            from_widget: Widget to fade out
            to_widget: Widget to fade in
            duration: Transition duration in ms
        """
        steps = 20
        delay = duration // steps
        
        # Show both widgets
        from_widget.lift()
        to_widget.lift()
        
        def animate(step=0):
            if step > steps:
                from_widget.lower()
                return
            
            # Calculate opacity
            from_opacity = 1 - (step / steps)
            to_opacity = step / steps
            
            # Placeholder for opacity implementation
            
            self.root.after(delay, lambda: animate(step + 1))
        
        animate()


# Export
__all__ = ['AnimationManager', 'TransitionManager']
