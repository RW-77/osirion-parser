#!/usr/bin/env python3
"""
Script to normalize movement_events.json files.

Converts files from:
    {"events": [...]}
to:
    [...]

This ensures all movement_events.json files have a consistent structure.
"""

import json
import sys
from pathlib import Path


def normalize_movement_events_file(file_path: Path) -> bool:
    """
    Normalize a single movement_events.json file.
    
    Returns:
        True if the file was modified, False otherwise
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Check if it needs normalization (is a dict with "events" key)
        if isinstance(data, dict) and "events" in data:
            # Extract the list
            normalized_data = data["events"]
            
            # Write back the normalized data
            with open(file_path, 'w') as f:
                json.dump(normalized_data, f, indent=2)
            
            print(f"✅ Normalized {file_path} ({len(normalized_data)} events)")
            return True
        elif isinstance(data, list):
            # Already normalized
            print(f"✓ Already normalized {file_path} ({len(data)} events)")
            return False
        else:
            print(f"⚠️  Unexpected structure in {file_path}: {type(data)}")
            return False
            
    except Exception as e:
        print(f"❌ Error processing {file_path}: {e}")
        return False


def main():
    """Find and normalize all movement_events.json files."""
    data_dir = Path("data/raw")
    
    if not data_dir.exists():
        print(f"❌ Data directory not found: {data_dir}")
        sys.exit(1)
    
    # Find all movement_events.json files
    movement_files = list(data_dir.glob("match_*/movement_events.json"))
    
    if not movement_files:
        print(f"❌ No movement_events.json files found in {data_dir}")
        sys.exit(1)
    
    print(f"Found {len(movement_files)} movement_events.json files")
    print("=" * 60)
    
    modified_count = 0
    for file_path in sorted(movement_files):
        if normalize_movement_events_file(file_path):
            modified_count += 1
    
    print("=" * 60)
    print(f"✅ Normalized {modified_count} out of {len(movement_files)} files")


if __name__ == "__main__":
    main()
