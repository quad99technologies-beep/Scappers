#!/usr/bin/env python3
"""
Automated GUI Issue Fixer
Fixes the issues identified in the code analysis report
"""

import re
from pathlib import Path

def remove_unused_imports(filepath, unused_imports):
    """Remove unused imports from a file"""
    print(f"\nProcessing: {filepath}")
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original_content = content
    changes_made = []
    
    for imp in unused_imports:
        # Try to remove from "from X import Y, Z" style imports
        pattern1 = rf',\s*{re.escape(imp)}\b'
        pattern2 = rf'\b{re.escape(imp)}\s*,\s*'
        pattern3 = rf'from\s+[\w.]+\s+import\s+{re.escape(imp)}\s*\n'
        pattern4 = rf'import\s+{re.escape(imp)}\s*\n'
        
        if re.search(pattern1, content):
            content = re.sub(pattern1, '', content)
            changes_made.append(f"  - Removed '{imp}' from import list")
        elif re.search(pattern2, content):
            content = re.sub(pattern2, '', content)
            changes_made.append(f"  - Removed '{imp}' from import list")
        elif re.search(pattern3, content):
            content = re.sub(pattern3, '', content)
            changes_made.append(f"  - Removed import line for '{imp}'")
        elif re.search(pattern4, content):
            content = re.sub(pattern4, '', content)
            changes_made.append(f"  - Removed import line for '{imp}'")
    
    if content != original_content:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"  [OK] Fixed {len(changes_made)} unused imports:")
        for change in changes_made:
            print(change)
        return True
    else:
        print(f"  [SKIP] No changes needed")
        return False

def main():
    repo_root = Path(__file__).parent
    
    print("="*80)
    print("GUI ISSUE FIXER")
    print("="*80)
    
    # Define files and their unused imports
    files_to_fix = {
        repo_root / "gui" / "components" / "cards.py": ["IconLibrary", "Union"],
        repo_root / "gui" / "components" / "inputs.py": ["Any"],
        repo_root / "gui" / "utils" / "animations.py": ["List"],
        repo_root / "gui" / "utils" / "shortcuts.py": ["scrolledtext"],
        repo_root / "gui" / "utils" / "tooltips.py": ["Any", "Dict"],
    }
    
    fixed_count = 0
    for filepath, unused_imports in files_to_fix.items():
        if filepath.exists():
            if remove_unused_imports(filepath, unused_imports):
                fixed_count += 1
        else:
            print(f"\n[SKIP] File not found: {filepath}")
    
    print("\n" + "="*80)
    print(f"SUMMARY: Fixed {fixed_count} files")
    print("="*80)
    
    # List backup files to remove
    print("\n" + "="*80)
    print("BACKUP FILES TO REMOVE MANUALLY:")
    print("="*80)
    
    backup_files = [
        repo_root / "backups" / "gui_enhancements_backup.py",
        repo_root / "backups" / "scraper_gui_enhanced_backup.py",
        repo_root / "backups" / "scraper_gui_professional_backup.py",
    ]
    
    total_size = 0
    for backup in backup_files:
        if backup.exists():
            size_kb = backup.stat().st_size / 1024
            total_size += size_kb
            print(f"  - {backup.name} ({size_kb:.1f} KB)")
    
    if total_size > 0:
        print(f"\nTotal space to reclaim: {total_size:.1f} KB")
        print("\nTo remove these files, run:")
        for backup in backup_files:
            if backup.exists():
                print(f"  del \"{backup}\"")
    
    print("\n" + "="*80)
    print("DONE!")
    print("="*80)

if __name__ == "__main__":
    main()
