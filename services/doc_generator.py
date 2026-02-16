#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline Documentation Generator

Auto-generate pipeline documentation from code.

Usage:
    python services/doc_generator.py Malaysia
"""

import sys
import ast
import inspect
from pathlib import Path
from typing import Dict, List, Any

# Add repo root to path
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))


def extract_step_info(script_path: Path) -> Dict[str, Any]:
    """Extract step information from script."""
    try:
        with open(script_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        tree = ast.parse(content)
        
        info = {
            "file": script_path.name,
            "description": "",
            "inputs": [],
            "outputs": [],
            "dependencies": []
        }
        
        # Extract docstring
        if tree.body and isinstance(tree.body[0], ast.Expr) and isinstance(tree.body[0].value, ast.Str):
            info["description"] = tree.body[0].value.s
        
        # Look for input/output patterns in code
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    if node.func.id in ["get_input", "read_csv", "load_input"]:
                        # Extract input references
                        pass
                    elif node.func.id in ["write_csv", "export", "save_output"]:
                        # Extract output references
                        pass
        
        return info
    except Exception as e:
        return {"file": script_path.name, "error": str(e)}


def generate_documentation(scraper_name: str) -> str:
    """Generate documentation for a scraper."""
    scripts_dir = REPO_ROOT / "scripts" / scraper_name
    
    # Find step scripts
    steps_dir = scripts_dir / "steps"
    if not steps_dir.exists():
        steps_dir = scripts_dir
    
    step_files = sorted(steps_dir.glob("step_*.py")) + sorted(steps_dir.glob("*_step_*.py"))
    
    doc_lines = [
        f"# {scraper_name} Pipeline Documentation",
        "",
        "**Auto-generated from code**",
        "",
        "## Pipeline Steps",
        ""
    ]
    
    for step_file in step_files:
        info = extract_step_info(step_file)
        
        doc_lines.append(f"### {info['file']}")
        if info.get("description"):
            doc_lines.append(f"\n{info['description']}\n")
        else:
            doc_lines.append("\n*No description available*\n")
        
        if info.get("inputs"):
            doc_lines.append("**Inputs:**")
            for inp in info["inputs"]:
                doc_lines.append(f"- {inp}")
            doc_lines.append("")
        
        if info.get("outputs"):
            doc_lines.append("**Outputs:**")
            for out in info["outputs"]:
                doc_lines.append(f"- {out}")
            doc_lines.append("")
    
    return "\n".join(doc_lines)


def main():
    parser = argparse.ArgumentParser(description="Generate pipeline documentation")
    parser.add_argument("scraper_name", help="Scraper name")
    parser.add_argument("--output", help="Output file path")
    
    args = parser.parse_args()
    
    doc = generate_documentation(args.scraper_name)
    
    if args.output:
        output_path = Path(args.output)
        output_path.write_text(doc, encoding="utf-8")
        print(f"Documentation written to: {output_path}")
    else:
        print(doc)


if __name__ == "__main__":
    import argparse
    main()
