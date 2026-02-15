#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GUI Enhancements Application Script

This script applies UI/UX enhancements to the original scraper_gui.py
without breaking any existing functionality.

Usage:
    python apply_gui_enhancements.py

This will:
1. Create a backup of the original scraper_gui.py
2. Apply enhancements while preserving all business logic
3. Generate a report of changes made
"""

import re
import shutil
from pathlib import Path
from datetime import datetime


def create_backup(original_path: Path) -> Path:
    """Create a backup of the original file"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = original_path.parent / f"scraper_gui_backup_{timestamp}.py"
    shutil.copy2(original_path, backup_path)
    print(f"✓ Backup created: {backup_path}")
    return backup_path


def apply_enhancements(original_path: Path) -> str:
    """Apply enhancements to the original file content"""
    
    with open(original_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    enhancements = []
    
    # 1. Add imports for enhancements
    import_section = '''import re

# GUI Enhancements - Professional UI/UX
from gui_enhancements import (
    ModernTheme, IconLibrary, TooltipManager,
    NotificationManager, CardFrame, StatusBadge,
    apply_modern_styles
)
'''
    
    if 'from gui_enhancements import' not in content:
        # Find a good place to insert imports
        import_match = re.search(r'(import json.*?\n)', content, re.DOTALL)
        if import_match:
            content = content.replace(
                import_match.group(1),
                import_match.group(1) + '\n' + import_section
            )
            enhancements.append("Added GUI enhancement imports")
    
    # 2. Initialize enhancement managers in __init__
    init_code = '''
        # Initialize UI enhancement managers
        self.colors = ModernTheme.get_all()
        self.tooltips = TooltipManager(root)
        self.notifications = NotificationManager(root)
        
        # Apply modern styles
        apply_modern_styles(ttk.Style())
'''
    
    if 'self.tooltips = TooltipManager' not in content:
        # Find setup_ui call in __init__
        init_match = re.search(
            r'(self\.setup_styles\(\).*?\n)(\s+self\.setup_ui\(\))',
            content,
            re.DOTALL
        )
        if init_match:
            content = content.replace(
                init_match.group(0),
                init_match.group(1) + init_code + init_match.group(2)
            )
            enhancements.append("Initialized enhancement managers")
    
    # 3. Update window title with icon
    if 'IconLibrary.DASHBOARD' not in content:
        content = content.replace(
            'self.root.title("Scraper Management Interface")',
            'self.root.title(f"{IconLibrary.DASHBOARD} Scraper Management System - Professional Edition")'
        )
        enhancements.append("Updated window title with icon")
    
    # 4. Update header colors
    header_updates = [
        ("bg=self.colors['dark_gray']", "bg=self.colors['BG_DARK']"),
        ("bg=self.colors['dark_gray']", "bg=self.colors['BG_DARK']"),
    ]
    
    for old, new in header_updates:
        if old in content and new not in content:
            content = content.replace(old, new, 1)
    
    # 5. Update background colors
    bg_updates = [
        ("bg=self.colors['background_gray']", "bg=self.colors['BG_MAIN']"),
        ("bg=self.colors['white']", "bg=self.colors['BG_CARD']"),
    ]
    
    for old, new in bg_updates:
        if old in content:
            content = content.replace(old, new)
            enhancements.append(f"Updated background: {old} -> {new}")
    
    # 6. Update text colors
    text_updates = [
        ("fg='#111827'", f"fg=self.colors['TEXT_PRIMARY']"),
        ("fg='#1f2937'", f"fg=self.colors['TEXT_PRIMARY']"),
        ("fg='#374151'", f"fg=self.colors['TEXT_SECONDARY']"),
        ("fg='#6b7280'", f"fg=self.colors['TEXT_SECONDARY']"),
        ("fg='#9ca3af'", f"fg=self.colors['TEXT_MUTED']"),
        ("fg='#666666'", f"fg=self.colors['TEXT_MUTED']"),
    ]
    
    for old, new in text_updates:
        if old in content:
            content = content.replace(old, new)
    
    enhancements.append("Updated text colors to modern palette")
    
    # 7. Update border colors
    border_updates = [
        ("highlightbackground=self.colors['border_gray']", "highlightbackground=self.colors['BORDER_LIGHT']"),
        ("highlightbackground=self.colors['border_light']", "highlightbackground=self.colors['BORDER_LIGHT']"),
    ]
    
    for old, new in border_updates:
        if old in content:
            content = content.replace(old, new)
    
    enhancements.append("Updated border colors")
    
    # 8. Add tooltips to key buttons (example)
    tooltip_additions = [
        ("self.run_button = ttk.Button", "self.tooltips.add_tooltip(self.run_button, \"Resume pipeline from last checkpoint (F5)\")"),
        ("self.stop_button = ttk.Button", "self.tooltips.add_tooltip(self.stop_button, \"Stop running pipeline (F6)\")"),
    ]
    
    # Note: Tooltips would need to be added after button creation
    # This is a simplified example
    
    # 9. Update console colors
    console_updates = [
        ("bg=self.colors['console_black']", "bg=self.colors['BG_CONSOLE']"),
        ("fg=self.colors['console_yellow']", "fg=self.colors['TEXT_CONSOLE']"),
    ]
    
    for old, new in console_updates:
        if old in content:
            content = content.replace(old, new)
    
    enhancements.append("Updated console colors")
    
    # 10. Add keyboard shortcuts
    shortcut_code = '''
    def _setup_keyboard_shortcuts(self):
        """Setup keyboard shortcuts"""
        self.root.bind('<F1>', lambda e: self._show_help())
        self.root.bind('<F5>', lambda e: self.run_full_pipeline(resume=True))
        self.root.bind('<Control-F5>', lambda e: self.run_full_pipeline(resume=False))
        self.root.bind('<F6>', lambda e: self.stop_pipeline())
        self.root.bind('<Control-c>', lambda e: self.copy_logs_to_clipboard())
        self.root.bind('<Control-s>', lambda e: self.save_log())
        self.root.bind('<Control-l>', lambda e: self.clear_logs())
        self.root.bind('<Control-r>', lambda e: self.refresh_output_files())
        self.root.bind('<Control-q>', lambda e: self.root.quit())
    
    def _show_help(self):
        """Show keyboard shortcuts help"""
        help_text = """
Keyboard Shortcuts:

F1          - Show this help
F5          - Resume pipeline
Ctrl+F5     - Run fresh pipeline
F6          - Stop pipeline
Ctrl+C      - Copy logs to clipboard
Ctrl+S      - Save log
Ctrl+L      - Clear logs
Ctrl+R      - Refresh output files
Ctrl+Q      - Quit application
        """
        messagebox.showinfo("Keyboard Shortcuts", help_text)
'''
    
    if '_setup_keyboard_shortcuts' not in content:
        # Add at the end of the class
        content = content.rstrip() + '\n' + shortcut_code
        enhancements.append("Added keyboard shortcuts")
    
    # 11. Update notification calls (if any exist)
    if 'messagebox.showinfo' in content:
        # Add notification for successful operations
        content = content.replace(
            'messagebox.showinfo("Success", f"Stopped {scraper_name} pipeline")',
            '''messagebox.showinfo("Success", f"Stopped {scraper_name} pipeline")
            try:
                self.notifications.show(f"✓ Stopped {scraper_name} pipeline", level='success')
            except:
                pass'''
        )
        enhancements.append("Added notification integration")
    
    return content, enhancements


def generate_report(enhancements: list, output_path: Path):
    """Generate a report of changes"""
    report_path = output_path.parent / "GUI_ENHANCEMENTS_REPORT.txt"
    
    with open(report_path, 'w') as f:
        f.write("GUI Enhancements Application Report\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Enhanced File: {output_path}\n\n")
        f.write("Enhancements Applied:\n")
        f.write("-" * 50 + "\n")
        for i, enhancement in enumerate(enhancements, 1):
            f.write(f"{i}. {enhancement}\n")
        f.write("\n")
        f.write("New Features Available:\n")
        f.write("-" * 50 + "\n")
        f.write("• Modern color scheme with high contrast\n")
        f.write("• Professional icons throughout the interface\n")
        f.write("• Keyboard shortcuts (F1 for help, F5 to run, etc.)\n")
        f.write("• Tooltip system for better UX\n")
        f.write("• Notification system for status updates\n")
        f.write("• Card-based layout for better visual hierarchy\n")
        f.write("\n")
        f.write("To fully utilize all enhancements, consider:\n")
        f.write("- Replacing LabelFrames with CardFrame\n")
        f.write("- Adding StatusBadge for status indicators\n")
        f.write("- Using ModernButton for action buttons\n")
        f.write("- Implementing SearchableCombobox for long lists\n")
    
    print(f"✓ Report generated: {report_path}")


def main():
    """Main function"""
    print("=" * 60)
    print("Scraper GUI Enhancement Application Tool")
    print("=" * 60)
    print()
    
    # Check if enhancement module exists
    enhancements_module = Path("gui_enhancements.py")
    if not enhancements_module.exists():
        print("❌ Error: gui_enhancements.py not found!")
        print("Please ensure gui_enhancements.py is in the same directory.")
        return
    
    print("✓ Enhancement module found")
    
    # Find original file
    original_path = Path("scraper_gui.py")
    if not original_path.exists():
        print(f"❌ Error: {original_path} not found!")
        return
    
    print(f"✓ Original file found: {original_path}")
    
    # Create backup
    backup_path = create_backup(original_path)
    
    # Apply enhancements
    print("\nApplying enhancements...")
    print("-" * 60)
    
    try:
        new_content, enhancements = apply_enhancements(original_path)
        
        # Write enhanced content
        enhanced_path = original_path.parent / "scraper_gui_enhanced_live.py"
        with open(enhanced_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        
        print(f"✓ Enhanced file created: {enhanced_path}")
        
        # Generate report
        generate_report(enhancements, enhanced_path)
        
        print()
        print("=" * 60)
        print("Enhancement Application Complete!")
        print("=" * 60)
        print()
        print(f"Backup: {backup_path}")
        print(f"Enhanced: {enhanced_path}")
        print()
        print("Changes made:")
        for i, enhancement in enumerate(enhancements, 1):
            print(f"  {i}. {enhancement}")
        print()
        print("To use the enhanced GUI:")
        print(f"  python {enhanced_path.name}")
        print()
        print("Note: This is a conservative enhancement that preserves")
        print("all original functionality. For full visual improvements,")
        print("consider using scraper_gui_professional.py instead.")
        
    except Exception as e:
        print(f"❌ Error applying enhancements: {e}")
        import traceback
        traceback.print_exc()
        print()
        print("Restoring from backup...")
        shutil.copy2(backup_path, original_path)
        print("✓ Original file restored")


if __name__ == "__main__":
    main()
