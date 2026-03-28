"""
Thin plotting script that reads JSON output from the Rust matching-analysis
binary and generates matplotlib charts identical to the original pipeline.

Usage:
    python plot_results.py [--root out/cpzp/matching_analysis]
"""

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt


def find_summaries(root: Path) -> list[Path]:
    return sorted(root.rglob("effects_summary.json"))


def plot_summary(json_path: Path):
    with open(json_path) as f:
        rows = json.load(f)

    folder = json_path.parent

    for row in rows:
        cohort = row["věk"]
        median = row.get("Med")
        iqr = row.get("IQR")
        ci = row.get("95% CI")
        vax_count = row.get("počet očko", 0)

        if median is None:
            continue

        fig, ax = plt.subplots(figsize=(6, 3))

        ax.barh(cohort, median, color="steelblue", height=0.5)

        if ci:
            ax.errorbar(
                median,
                cohort,
                xerr=[[median - ci[0]], [ci[1] - median]],
                fmt="none",
                ecolor="black",
                capsize=4,
            )

        ax.set_xlabel("Effect")
        ax.set_title(f"{cohort}  (n={vax_count})")
        fig.tight_layout()

        out_path = folder / f"{cohort}.png"
        fig.savefig(out_path, dpi=150)
        plt.close(fig)

    print(f"  plotted {json_path.relative_to(json_path.parents[5])}")


def main():
    parser = argparse.ArgumentParser(description="Plot matching analysis results")
    parser.add_argument(
        "--root",
        default="out",
        help="Root directory containing effects_summary.json files",
    )
    args = parser.parse_args()

    root = Path(args.root)
    summaries = find_summaries(root)
    print(f"Found {len(summaries)} effects_summary.json file(s)")

    for s in summaries:
        plot_summary(s)


if __name__ == "__main__":
    main()
