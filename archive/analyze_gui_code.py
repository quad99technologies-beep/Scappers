#!/usr/bin/env python3
"""
Comprehensive GUI Code Analysis Script
Checks for:
1. Unused imports
2. Unused functions/methods
3. Undefined variables
4. GUI-specific errors (missing widgets, broken bindings)
5. Ghost code (unreachable code)
"""

import ast
import os
import sys
from pathlib import Path
from collections import defaultdict
import re

class CodeAnalyzer(ast.NodeVisitor):
    def __init__(self, filename):
        self.filename = filename
        self.imports = set()
        self.used_names = set()
        self.defined_functions = {}
        self.called_functions = set()
        self.defined_classes = {}
        self.class_methods = defaultdict(set)
        self.called_methods = defaultdict(set)
        self.gui_widgets = set()
        self.gui_bindings = []
        self.errors = []
        self.warnings = []
        self.current_class = None
        
    def visit_Import(self, node):
        for alias in node.names:
            self.imports.add(alias.name if alias.asname is None else alias.asname)
        self.generic_visit(node)
    
    def visit_ImportFrom(self, node):
        for alias in node.names:
            self.imports.add(alias.name if alias.asname is None else alias.asname)
        self.generic_visit(node)
    
    def visit_FunctionDef(self, node):
        if self.current_class:
            self.class_methods[self.current_class].add(node.name)
        else:
            self.defined_functions[node.name] = node.lineno
        self.generic_visit(node)
    
    def visit_ClassDef(self, node):
        self.defined_classes[node.name] = node.lineno
        old_class = self.current_class
        self.current_class = node.name
        self.generic_visit(node)
        self.current_class = old_class
    
    def visit_Call(self, node):
        # Track function calls
        if isinstance(node.func, ast.Name):
            self.called_functions.add(node.func.id)
            self.used_names.add(node.func.id)
        elif isinstance(node.func, ast.Attribute):
            if isinstance(node.func.value, ast.Name):
                self.used_names.add(node.func.value.id)
                # Track method calls
                if node.func.value.id == 'self':
                    self.called_methods[self.current_class].add(node.func.attr)
        self.generic_visit(node)
    
    def visit_Name(self, node):
        self.used_names.add(node.id)
        self.generic_visit(node)
    
    def visit_Attribute(self, node):
        if isinstance(node.value, ast.Name):
            self.used_names.add(node.value.id)
        self.generic_visit(node)

def analyze_file(filepath):
    """Analyze a Python file for issues"""
    print(f"\n{'='*80}")
    print(f"Analyzing: {filepath}")
    print(f"{'='*80}\n")
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    try:
        tree = ast.parse(content, filename=str(filepath))
    except SyntaxError as e:
        print(f"[ERROR] SYNTAX ERROR: {e}")
        return
    
    analyzer = CodeAnalyzer(filepath)
    analyzer.visit(tree)
    
    # Check for unused imports
    unused_imports = analyzer.imports - analyzer.used_names
    if unused_imports:
        print("[!] UNUSED IMPORTS:")
        for imp in sorted(unused_imports):
            print(f"   - {imp}")
    
    # Check for unused functions (not in class)
    unused_functions = set(analyzer.defined_functions.keys()) - analyzer.called_functions
    if unused_functions:
        print("\n[!] POTENTIALLY UNUSED FUNCTIONS:")
        for func in sorted(unused_functions):
            line = analyzer.defined_functions[func]
            print(f"   - {func} (line {line})")
    
    # Check for unused methods in classes
    for class_name, methods in analyzer.class_methods.items():
        called = analyzer.called_methods.get(class_name, set())
        unused_methods = methods - called - {'__init__', '__str__', '__repr__', '__del__'}
        if unused_methods:
            print(f"\n[!] POTENTIALLY UNUSED METHODS IN CLASS '{class_name}':")
            for method in sorted(unused_methods):
                print(f"   - {method}")
    
    # Check for GUI-specific issues
    print("\n[GUI] GUI-SPECIFIC CHECKS:")
    
    # Check for missing widget references
    widget_pattern = re.compile(r'self\.(\w+)\s*=\s*(?:tk\.|ttk\.)')
    widget_usage_pattern = re.compile(r'self\.(\w+)\.')
    
    defined_widgets = set(widget_pattern.findall(content))
    used_widgets = set(widget_usage_pattern.findall(content))
    
    unused_widgets = defined_widgets - used_widgets
    if unused_widgets:
        print("   [!] Potentially unused GUI widgets:")
        for widget in sorted(unused_widgets):
            print(f"      - self.{widget}")
    
    # Check for broken bindings (common GUI error)
    binding_pattern = re.compile(r'\.bind\(["\']<([^>]+)>["\'],\s*(?:self\.)?(\w+)')
    bindings = binding_pattern.findall(content)
    
    if bindings:
        print("\n   [OK] Event bindings found:")
        for event, handler in bindings[:10]:  # Show first 10
            print(f"      - <{event}> -> {handler}")
        if len(bindings) > 10:
            print(f"      ... and {len(bindings) - 10} more")
    
    # Check for undefined callback references
    callback_pattern = re.compile(r'command\s*=\s*(?:self\.)?(\w+)')
    callbacks = set(callback_pattern.findall(content))
    
    undefined_callbacks = callbacks - analyzer.called_functions - set(analyzer.class_methods.get('ScraperGUI', []))
    if undefined_callbacks:
        print("\n   [!] Potentially undefined callbacks:")
        for cb in sorted(undefined_callbacks):
            print(f"      - {cb}")
    
    # Check for ghost code patterns
    print("\n[GHOST] GHOST CODE CHECKS:")
    
    # Check for unreachable code after return
    unreachable_pattern = re.compile(r'return\s+.*\n\s+\w+', re.MULTILINE)
    unreachable_matches = unreachable_pattern.findall(content)
    if unreachable_matches:
        print("   [!] Potential unreachable code after return statements")
    
    # Check for commented-out code blocks
    commented_code_pattern = re.compile(r'^\s*#\s+(def |class |import |from )', re.MULTILINE)
    commented_code = commented_code_pattern.findall(content)
    if commented_code:
        print(f"   [!] Found {len(commented_code)} commented-out code blocks")
    
    # Check for duplicate function definitions
    func_counts = defaultdict(int)
    for func in analyzer.defined_functions:
        func_counts[func] += 1
    
    duplicates = {k: v for k, v in func_counts.items() if v > 1}
    if duplicates:
        print("\n   [ERROR] DUPLICATE FUNCTION DEFINITIONS:")
        for func, count in duplicates.items():
            print(f"      - {func} (defined {count} times)")
    
    print("\n" + "="*80)
    print("[OK] Analysis complete!")
    print("="*80)

def main():
    repo_root = Path(__file__).parent
    
    # Analyze main GUI file
    main_gui = repo_root / "scraper_gui.py"
    if main_gui.exists():
        analyze_file(main_gui)
    
    # Analyze GUI module files
    gui_dir = repo_root / "gui"
    if gui_dir.exists():
        for py_file in gui_dir.rglob("*.py"):
            if py_file.name != "__init__.py":
                analyze_file(py_file)
    
    # Check for backup files (potential ghost code)
    print("\n" + "="*80)
    print("[BACKUP] BACKUP/GHOST FILES:")
    print("="*80)
    
    backup_patterns = ["*backup*.py", "*old*.py", "*_bak.py", "*_copy.py"]
    found_backups = []
    
    for pattern in backup_patterns:
        found_backups.extend(repo_root.rglob(pattern))
    
    if found_backups:
        print("\n[!] Found backup/old files (consider removing):")
        for backup in sorted(found_backups):
            rel_path = backup.relative_to(repo_root)
            size_kb = backup.stat().st_size / 1024
            print(f"   - {rel_path} ({size_kb:.1f} KB)")
    else:
        print("\n[OK] No backup files found")

if __name__ == "__main__":
    main()
