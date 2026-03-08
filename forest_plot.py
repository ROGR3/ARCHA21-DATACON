"""
Forest plot visualization of matching analysis effects.

Compares vaccination period vs 3-years-back period for each PE bucket,
broken down by age cohort.
"""

import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

BASE = Path("out/cpzp")
VACCINATION = BASE / "matching_analysis" / "whole_period"
THREE_YEARS = BASE / "3_years_back_matching_analysis" / "whole_period"

BUCKETS = [
    "0_PE",
    "1_to_500_PE",
    "500_to_5000_PE",
    "ZERO_PE_SUSPECTIBLE",
    "NEVER_PRESCRIBED",
]

BUCKET_LABELS = {
    "0_PE": "0 PE",
    "1_to_500_PE": "1–500 PE",
    "500_to_5000_PE": "500–5 000 PE",
    "ZERO_PE_SUSPECTIBLE": "0 PE (susceptible)",
    "NEVER_PRESCRIBED": "Nikdy předepsáno",
}

AGE_ORDER = ["16-29", "30-49", "50-59"]
AGE_LABELS = {
    "16-29": "16–29 let",
    "30-49": "30–49 let",
    "50-59": "50–59 let",
}

# Buckets where the effect is a ratio (reference line at 1);
# others are differences (reference line at 0).
RATIO_BUCKETS = {"ZERO_PE_SUSPECTIBLE", "NEVER_PRESCRIBED", "0_PE"}


def load_effects(directory: Path, bucket: str) -> dict:
    path = directory / bucket / "effects_summary.json"
    with open(path) as f:
        data = json.load(f)
    return {entry["věk"]: entry for entry in data}


def _fmt_n(n: int) -> str:
    return f"n={n:,}".replace(",", "\u2009")


def make_forest_plot(bucket: str, ax: plt.Axes):
    vax_data = load_effects(VACCINATION, bucket)
    back_data = load_effects(THREE_YEARS, bucket)

    is_ratio = bucket in RATIO_BUCKETS
    ref_line = 1.0 if is_ratio else 0.0

    y_positions = []
    group_seps = []
    for i, age in enumerate(AGE_ORDER):
        y_vax = i * 3
        y_back = i * 3 + 1
        y_positions.append((age, y_vax, y_back))
        if i < len(AGE_ORDER) - 1:
            group_seps.append(i * 3 + 2)

    colors = {"vax": "#2171b5", "back": "#cb181d"}

    all_right_edges = []

    for age, y_vax, y_back in y_positions:
        if age in vax_data:
            v = vax_data[age]
            med = v["Med"]
            ci_lo, ci_hi = v["95% CI"]
            ax.errorbar(
                med, y_vax,
                xerr=[[med - ci_lo], [ci_hi - med]],
                fmt="o", color=colors["vax"], markersize=6, capsize=3,
                linewidth=1.5, markeredgewidth=1.5,
            )
            all_right_edges.append(ci_hi)

        if age in back_data:
            b = back_data[age]
            med = b["Med"]
            ci_lo, ci_hi = b["95% CI"]
            ax.errorbar(
                med, y_back,
                xerr=[[med - ci_lo], [ci_hi - med]],
                fmt="s", color=colors["back"], markersize=5, capsize=3,
                linewidth=1.5, markeredgewidth=1.5,
            )
            all_right_edges.append(ci_hi)

    # Add n= annotations after computing x-limits for consistent placement
    for age, y_vax, y_back in y_positions:
        if age in vax_data:
            v = vax_data[age]
            ci_hi = v["95% CI"][1]
            ax.annotate(
                _fmt_n(v["počet očko"]),
                (ci_hi, y_vax), textcoords="offset points",
                xytext=(5, 0), fontsize=6.5, color=colors["vax"], va="center",
            )
        if age in back_data:
            b = back_data[age]
            ci_hi = b["95% CI"][1]
            ax.annotate(
                _fmt_n(b["počet očko"]),
                (ci_hi, y_back), textcoords="offset points",
                xytext=(5, 0), fontsize=6.5, color=colors["back"], va="center",
            )

    yticks = []
    ylabels = []
    for age, y_vax, y_back in y_positions:
        mid = (y_vax + y_back) / 2
        yticks.append(mid)
        ylabels.append(AGE_LABELS[age])

    ax.set_yticks(yticks)
    ax.set_yticklabels(ylabels, fontsize=9)
    ax.invert_yaxis()

    ax.axvline(ref_line, color="grey", linestyle="--", linewidth=0.8, zorder=0)

    for sep in group_seps:
        ax.axhline(sep, color="#dddddd", linestyle="-", linewidth=0.5, zorder=0)

    ax.set_title(BUCKET_LABELS[bucket], fontsize=12, fontweight="bold", pad=10)
    ax.set_xlabel("Medián efektu (95% CI)", fontsize=9)
    ax.grid(axis="x", alpha=0.2, linewidth=0.5)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    ax.margins(x=0.15)


def main():
    legend_elements = [
        plt.Line2D([0], [0], marker="o", color="#2171b5", linestyle="None",
                   markersize=7, label="Očkovací období"),
        plt.Line2D([0], [0], marker="s", color="#cb181d", linestyle="None",
                   markersize=6, label="3 roky zpět"),
    ]

    out_dir = Path("out/cpzp/forest_plots")
    out_dir.mkdir(parents=True, exist_ok=True)

    for bucket in BUCKETS:
        fig, ax = plt.subplots(figsize=(9, 3.2))
        make_forest_plot(bucket, ax)
        fig.legend(
            handles=legend_elements, loc="upper right",
            fontsize=9, frameon=True, fancybox=True, edgecolor="#cccccc",
        )
        fig.tight_layout()
        out_path = out_dir / f"forest_{bucket.lower()}.png"
        fig.savefig(out_path, dpi=200, bbox_inches="tight", facecolor="white")
        plt.close(fig)
        print(f"Saved → {out_path}")


if __name__ == "__main__":
    main()
