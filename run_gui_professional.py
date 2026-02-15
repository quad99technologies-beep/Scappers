#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper Management System - Professional GUI Entry Point

This is the main entry point for the professional edition of the
Scraper Management System with enhanced UI/UX.

Usage:
    python run_gui_professional.py

Features:
    - Modern professional interface
    - Keyboard shortcuts (F1 for help)
    - Toast notifications
    - Enhanced visual hierarchy
    - Smooth animations
"""

import sys
import tkinter as tk
from pathlib import Path

# Add repository root to path
repo_root = Path(__file__).resolve().parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

# Import the professional GUI
from gui import ProfessionalScraperGUI


def main():
    """Main entry point"""
    # Create root window
    root = tk.Tk()
    
    # Initialize professional GUI
    app = ProfessionalScraperGUI(root)
    
    # Start main loop
    root.mainloop()


if __name__ == "__main__":
    main()
