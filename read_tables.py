#!/usr/bin/env python3
"""
Script to read and display effects_summary.json files from the out/ folder.
Uses Polars for data manipulation and creates a combined CSV file.
"""

import json
from pathlib import Path
from typing import List, Dict, Any, Optional
import polars as pl


def find_effects_summary_files(
    root_dir: str = "out/cpzp/matching_analysis",
) -> List[Path]:
    """Find all effects_summary.json files in the historical_matching_analysis directory."""
    root_path = Path(root_dir)
    if not root_path.exists():
        print(f"Error: Directory '{root_dir}' does not exist.")
        return []

    files = list(root_path.rglob("effects_summary.json"))
    return sorted(files)


def extract_metadata_from_path(file_path: Path) -> Dict[str, str]:
    """Extract metadata (cohort, time_period, PE_count) from file path."""
    parts = file_path.parts

    # Find anchor: historical_matching_analysis or matching_analysis
    for anchor in ("matching_analysis", "matching_analysis"):
        try:
            anchor_idx = parts.index(anchor)
            cohort = parts[anchor_idx - 1] if anchor_idx > 0 else "unknown"
            time_period = (
                parts[anchor_idx + 1] if anchor_idx + 1 < len(parts) else "unknown"
            )
            pe_count = (
                parts[anchor_idx + 2] if anchor_idx + 2 < len(parts) else "unknown"
            )
            return {"cohort": cohort, "time_period": time_period, "PE_count": pe_count}
        except ValueError:
            continue

    # Fallback if structure is different
    cohort = parts[1] if len(parts) > 1 else "unknown"
    return {"cohort": cohort, "time_period": "unknown", "PE_count": "unknown"}


def read_json_file(file_path: Path) -> List[Dict[str, Any]]:
    """Read and parse a JSON file."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return []


def format_ci(ci_list: Optional[List[float]]) -> Optional[str]:
    """Format confidence interval as a string."""
    if ci_list is None:
        return None
    if len(ci_list) == 2:
        return f"[{ci_list[0]:.4f}, {ci_list[1]:.4f}]"
    return str(ci_list)


def format_iqr(iqr_list: Optional[List[float]]) -> Optional[str]:
    """Format IQR as a string."""
    if iqr_list is None:
        return None
    if len(iqr_list) == 2:
        return f"[{iqr_list[0]:.4f}, {iqr_list[1]:.4f}]"
    return str(iqr_list)


def format_med(med_value: Optional[float]) -> Optional[str]:
    """Format median value."""
    if med_value is None:
        return None
    return f"{med_value:.4f}"


def display_table(data: List[Dict[str, Any]], file_path: Path):
    """Display the data in a nicely formatted table."""
    if not data:
        print(f"\nNo data found in {file_path}\n")
        return

    # Format the data first, then create Polars DataFrame
    formatted_data = []
    for row in data:
        formatted_data.append(
            {
                "Věk": row["věk"],
                "Medián": format_med(row.get("Med")),
                "IQR": format_iqr(row.get("IQR")),
                "95% CI": format_ci(row.get("95% CI")),
                "Počet očko": row.get("počet očko", 0),
            }
        )

    display_df = pl.DataFrame(formatted_data)

    # Print header with file path
    relative_path = file_path.relative_to(Path("out"))
    print("\n" + "=" * 80)
    print(f"File: {relative_path}")
    print("=" * 80)

    # Print table using Polars formatting
    print(display_df)
    print()


def create_combined_dataframe(files: List[Path]) -> pl.DataFrame:
    """Create a combined DataFrame with all data and metadata."""
    all_rows = []

    for file_path in files:
        data = read_json_file(file_path)
        if not data:
            continue

        metadata = extract_metadata_from_path(file_path)

        for row in data:
            # Handle null values properly
            med_value = row.get("Med")
            iqr_value = row.get("IQR")
            ci_value = row.get("95% CI")

            all_rows.append(
                {
                    "cohort": metadata["cohort"],
                    "time_period": metadata["time_period"],
                    "PE_count": metadata["PE_count"],
                    "věk": row.get("věk", ""),
                    "Med": med_value if med_value is not None else None,
                    "Med_formatted": format_med(med_value),
                    "IQR_lower": iqr_value[0]
                    if iqr_value and len(iqr_value) >= 1
                    else None,
                    "IQR_upper": iqr_value[1]
                    if iqr_value and len(iqr_value) >= 2
                    else None,
                    "IQR_formatted": format_iqr(iqr_value),
                    "CI_lower": ci_value[0]
                    if ci_value and len(ci_value) >= 1
                    else None,
                    "CI_upper": ci_value[1]
                    if ci_value and len(ci_value) >= 2
                    else None,
                    "CI_formatted": format_ci(ci_value),
                    "počet_očko": row.get("počet očko", 0),
                    "file_path": str(file_path.relative_to(Path("out"))),
                }
            )

    return pl.DataFrame(all_rows)


def main():
    """Main function to find and display all effects_summary.json files."""
    print(
        "Searching for effects_summary.json files in out/cpzp/historical_matching_analysis..."
    )
    files = find_effects_summary_files()

    if not files:
        print("No effects_summary.json files found.")
        return

    print(f"Found {len(files)} file(s)\n")

    # Create combined DataFrame
    print("Creating combined dataset...")
    combined_df = create_combined_dataframe(files)

    # Save to CSV
    output_csv = "./effects_summary_combined.csv"
    combined_df.write_csv(output_csv)
    print(f"\n✓ Combined data saved to: {output_csv}")
    print(f"  Total rows: {len(combined_df)}")
    print(f"  Columns: {', '.join(combined_df.columns)}\n")

    # Display summary
    print("=" * 80)
    print("Summary by cohort and time_period:")
    print("=" * 80)
    summary = (
        combined_df.group_by(["cohort", "time_period", "PE_count"])
        .agg([pl.len().alias("num_records")])
        .sort(["cohort", "time_period", "PE_count"])
    )
    print(summary)
    print()

    # Optionally display tables (commented out to reduce output, uncomment if needed)
    # print("\n" + "="*80)
    # print("Displaying individual tables:")
    # print("="*80)
    # for file_path in files:
    #     data = read_json_file(file_path)
    #     if data:
    #         display_table(data, file_path)


if __name__ == "__main__":
    main()
