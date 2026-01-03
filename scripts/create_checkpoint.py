#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Checkpoint Creation Utility

Allows manual creation and editing of checkpoint files for any scraper.
Useful for resuming from a specific step after a crash or manual intervention.

Usage:
    python create_checkpoint.py Argentina --step 3  # Mark steps 0-3 as complete
    python create_checkpoint.py Argentina --steps 0,1,2,3  # Mark specific steps as complete
    python create_checkpoint.py Argentina --clear  # Clear checkpoint
    python create_checkpoint.py Argentina --view  # View current checkpoint
    python create_checkpoint.py Argentina --list  # List all scrapers and their checkpoints
"""

import sys
import argparse
import json
from pathlib import Path
from datetime import datetime

# Add repo root to path
_repo_root = Path(__file__).resolve().parent.parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.pipeline_checkpoint import get_checkpoint_manager

# Define pipeline steps for each scraper
PIPELINE_STEPS = {
    "Argentina": [
        (0, "Backup and Clean"),
        (1, "Get Product List"),
        (2, "Prepare URLs"),
        (3, "Scrape Products"),
        (4, "Translate Using Dictionary"),
        (5, "Generate Output"),
        (6, "PCID Missing"),
    ],
    "Malaysia": [
        (0, "Backup and Clean"),
        (1, "Product Registration Number"),
        (2, "Product Details"),
        (3, "Consolidate Results"),
        (4, "Get Fully Reimbursable"),
        (5, "Generate PCID Mapped"),
    ],
    "CanadaQuebec": [
        (0, "Backup and Clean"),
        (1, "Split PDF into Annexes"),
        (2, "Validate PDF Structure"),
        (3, "Extract Annexe IV.1"),
        (4, "Extract Annexe IV.2"),
        (5, "Extract Annexe V"),
        (6, "Merge All Annexes"),
    ],
}


def create_checkpoint_up_to_step(scraper_name: str, last_completed_step: int):
    """Mark all steps up to and including last_completed_step as complete."""
    if scraper_name not in PIPELINE_STEPS:
        print(f"ERROR: Unknown scraper '{scraper_name}'")
        print(f"Available scrapers: {', '.join(PIPELINE_STEPS.keys())}")
        return False
    
    steps = PIPELINE_STEPS[scraper_name]
    max_step = max(step_num for step_num, _ in steps)
    
    if last_completed_step < 0 or last_completed_step > max_step:
        print(f"ERROR: Step {last_completed_step} is out of range (0-{max_step})")
        return False
    
    cp = get_checkpoint_manager(scraper_name)
    
    # Mark all steps up to last_completed_step as complete
    completed_steps = []
    for step_num in range(last_completed_step + 1):
        step_name = dict(steps)[step_num]
        cp.mark_step_complete(step_num, step_name)
        completed_steps.append(step_num)
        print(f"✓ Marked step {step_num}: {step_name} as complete")
    
    print(f"\n✓ Checkpoint created for {scraper_name}")
    print(f"  Completed steps: {completed_steps}")
    print(f"  Next step to run: {last_completed_step + 1}")
    
    return True


def create_checkpoint_for_steps(scraper_name: str, step_numbers: list):
    """Mark specific steps as complete."""
    if scraper_name not in PIPELINE_STEPS:
        print(f"ERROR: Unknown scraper '{scraper_name}'")
        print(f"Available scrapers: {', '.join(PIPELINE_STEPS.keys())}")
        return False
    
    steps = dict(PIPELINE_STEPS[scraper_name])
    max_step = max(steps.keys())
    
    cp = get_checkpoint_manager(scraper_name)
    
    # Mark specified steps as complete
    completed_steps = []
    for step_num in step_numbers:
        if step_num < 0 or step_num > max_step:
            print(f"WARNING: Step {step_num} is out of range (0-{max_step}), skipping")
            continue
        
        step_name = steps[step_num]
        cp.mark_step_complete(step_num, step_name)
        completed_steps.append(step_num)
        print(f"✓ Marked step {step_num}: {step_name} as complete")
    
    print(f"\n✓ Checkpoint updated for {scraper_name}")
    print(f"  Completed steps: {sorted(completed_steps)}")
    
    return True


def view_checkpoint(scraper_name: str):
    """View current checkpoint status."""
    if scraper_name not in PIPELINE_STEPS:
        print(f"ERROR: Unknown scraper '{scraper_name}'")
        print(f"Available scrapers: {', '.join(PIPELINE_STEPS.keys())}")
        return False
    
    cp = get_checkpoint_manager(scraper_name)
    info = cp.get_checkpoint_info()
    checkpoint_file = cp.checkpoint_file
    
    print(f"\n{'='*60}")
    print(f"Checkpoint Status for {scraper_name}")
    print(f"{'='*60}")
    print(f"Checkpoint File: {checkpoint_file}")
    print(f"  Exists: {'Yes' if checkpoint_file.exists() else 'No'}")
    print(f"\nStatus:")
    print(f"  Last Run: {info['last_run'] or 'Never'}")
    print(f"  Completed Steps: {info['completed_steps']}")
    print(f"  Last Completed Step: {info['last_completed_step'] or 'None'}")
    print(f"  Next Step: {info['next_step']}")
    print(f"  Total Completed: {info['total_completed']}")
    
    if checkpoint_file.exists():
        print(f"\nFile Contents:")
        try:
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                contents = json.load(f)
            print(json.dumps(contents, indent=2, ensure_ascii=False))
        except Exception as e:
            print(f"  Error reading file: {e}")
    
    return True


def list_all_checkpoints():
    """List all scrapers and their checkpoint status."""
    print(f"\n{'='*60}")
    print("Checkpoint Status for All Scrapers")
    print(f"{'='*60}\n")
    
    for scraper_name in PIPELINE_STEPS.keys():
        cp = get_checkpoint_manager(scraper_name)
        info = cp.get_checkpoint_info()
        checkpoint_file = cp.checkpoint_file
        
        exists = "✓" if checkpoint_file.exists() else "✗"
        status = f"Step {info['last_completed_step'] or 'N/A'} completed" if info['last_completed_step'] is not None else "No checkpoint"
        
        print(f"{exists} {scraper_name:15} - {status:30} (Next: Step {info['next_step']})")
    
    print()


def clear_checkpoint(scraper_name: str):
    """Clear checkpoint for scraper."""
    if scraper_name not in PIPELINE_STEPS:
        print(f"ERROR: Unknown scraper '{scraper_name}'")
        print(f"Available scrapers: {', '.join(PIPELINE_STEPS.keys())}")
        return False
    
    cp = get_checkpoint_manager(scraper_name)
    cp.clear_checkpoint()
    print(f"✓ Checkpoint cleared for {scraper_name}")
    return True


def interactive_mode(scraper_name: str):
    """Interactive mode for creating checkpoints."""
    if scraper_name not in PIPELINE_STEPS:
        print(f"ERROR: Unknown scraper '{scraper_name}'")
        print(f"Available scrapers: {', '.join(PIPELINE_STEPS.keys())}")
        return False
    
    steps = PIPELINE_STEPS[scraper_name]
    
    print(f"\n{'='*60}")
    print(f"Interactive Checkpoint Creator for {scraper_name}")
    print(f"{'='*60}\n")
    
    print("Available steps:")
    for step_num, step_name in steps:
        print(f"  {step_num}: {step_name}")
    
    print("\nOptions:")
    print("  1. Mark steps 0-N as complete (for resuming from step N+1)")
    print("  2. Mark specific steps as complete")
    print("  3. View current checkpoint")
    print("  4. Clear checkpoint")
    print("  5. Exit")
    
    while True:
        try:
            choice = input("\nEnter option (1-5): ").strip()
            
            if choice == "1":
                last_step = int(input(f"Enter last completed step number (0-{len(steps)-1}): "))
                create_checkpoint_up_to_step(scraper_name, last_step)
            elif choice == "2":
                step_str = input("Enter step numbers separated by commas (e.g., 0,1,2): ")
                step_nums = [int(s.strip()) for s in step_str.split(",")]
                create_checkpoint_for_steps(scraper_name, step_nums)
            elif choice == "3":
                view_checkpoint(scraper_name)
            elif choice == "4":
                confirm = input(f"Clear checkpoint for {scraper_name}? (yes/no): ")
                if confirm.lower() == "yes":
                    clear_checkpoint(scraper_name)
            elif choice == "5":
                break
            else:
                print("Invalid option. Please enter 1-5.")
        except (ValueError, KeyboardInterrupt) as e:
            if isinstance(e, KeyboardInterrupt):
                print("\nExiting...")
                break
            print(f"Invalid input: {e}")
    
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Create or manage checkpoint files for scrapers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Mark steps 0-3 as complete (resume from step 4)
  python create_checkpoint.py Argentina --step 3
  
  # Mark specific steps as complete
  python create_checkpoint.py Argentina --steps 0,1,2,3
  
  # View current checkpoint
  python create_checkpoint.py Argentina --view
  
  # Clear checkpoint
  python create_checkpoint.py Argentina --clear
  
  # List all checkpoints
  python create_checkpoint.py --list
  
  # Interactive mode
  python create_checkpoint.py Argentina --interactive
        """
    )
    
    parser.add_argument("scraper", nargs="?", help="Scraper name (Argentina, Malaysia, CanadaQuebec)")
    parser.add_argument("--step", type=int, help="Mark all steps up to and including this step as complete")
    parser.add_argument("--steps", type=str, help="Comma-separated list of step numbers to mark as complete (e.g., 0,1,2,3)")
    parser.add_argument("--view", action="store_true", help="View current checkpoint status")
    parser.add_argument("--clear", action="store_true", help="Clear checkpoint")
    parser.add_argument("--list", action="store_true", help="List all scrapers and their checkpoint status")
    parser.add_argument("--interactive", "-i", action="store_true", help="Interactive mode")
    
    args = parser.parse_args()
    
    if args.list:
        list_all_checkpoints()
        return
    
    if not args.scraper:
        parser.print_help()
        return
    
    scraper_name = args.scraper
    
    if args.interactive:
        interactive_mode(scraper_name)
    elif args.clear:
        clear_checkpoint(scraper_name)
    elif args.view:
        view_checkpoint(scraper_name)
    elif args.step is not None:
        create_checkpoint_up_to_step(scraper_name, args.step)
    elif args.steps:
        step_nums = [int(s.strip()) for s in args.steps.split(",")]
        create_checkpoint_for_steps(scraper_name, step_nums)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

