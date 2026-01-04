#!/usr/bin/env python3
"""
Script to read and display effects_summary.json files from the out/ folder.
Uses Polars for data manipulation.
"""

import json
from pathlib import Path
from typing import List, Dict, Any
import polars as pl


def find_effects_summary_files(root_dir: str = "out") -> List[Path]:
    """Find all effects_summary.json files in the out/ directory."""
    root_path = Path(root_dir)
    if not root_path.exists():
        print(f"Error: Directory '{root_dir}' does not exist.")
        return []
    
    files = list(root_path.rglob("effects_summary.json"))
    return sorted(files)


def read_json_file(file_path: Path) -> List[Dict[str, Any]]:
    """Read and parse a JSON file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return []


def format_ci(ci_list: List[float]) -> str:
    """Format confidence interval as a string."""
    if len(ci_list) == 2:
        return f"[{ci_list[0]:.4f}, {ci_list[1]:.4f}]"
    return str(ci_list)


def format_iqr(iqr_list: List[float]) -> str:
    """Format IQR as a string."""
    if len(iqr_list) == 2:
        return f"[{iqr_list[0]:.4f}, {iqr_list[1]:.4f}]"
    return str(iqr_list)


def display_table(data: List[Dict[str, Any]], file_path: Path):
    """Display the data in a nicely formatted table."""
    if not data:
        print(f"\nNo data found in {file_path}\n")
        return
    
    # Format the data first, then create Polars DataFrame
    formatted_data = []
    for row in data:
        formatted_data.append({
            "Věk": row["věk"],
            "Medián": f"{row['Med']:.4f}",
            "IQR": format_iqr(row["IQR"]),
            "95% CI": format_ci(row["95% CI"]),
            "Počet očko": row["počet očko"]
        })
    
    display_df = pl.DataFrame(formatted_data)
    
    # Print header with file path
    relative_path = file_path.relative_to(Path("out"))
    print("\n" + "="*80)
    print(f"File: {relative_path}")
    print("="*80)
    
    # Print table using Polars formatting
    print(display_df)
    print()


def main():
    """Main function to find and display all effects_summary.json files."""
    print("Searching for effects_summary.json files in out/ folder...")
    files = find_effects_summary_files()
    
    if not files:
        print("No effects_summary.json files found.")
        return
    
    print(f"Found {len(files)} file(s):\n")
    
    for file_path in files:
        data = read_json_file(file_path)
        if data:
            display_table(data, file_path)


if __name__ == "__main__":
    main()

