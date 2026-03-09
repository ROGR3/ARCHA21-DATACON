"""
Forest plot visualization of matching analysis effects.

Shows 5 periods (3y back → 1y forward) for each PE bucket,
broken down by age cohort.
"""

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

BASE = Path("out/cpzp")

PERIODS = [
    ("3_years_back_matching_analysis", "3 roky zpět"),
    ("2_years_back_matching_analysis", "2 roky zpět"),
    ("1_years_back_matching_analysis", "1 rok zpět"),
    ("matching_analysis", "Očkovací období"),
    ("-1_years_back_matching_analysis", "1 rok dopředu"),
]

PERIOD_STYLES = [
    {"color": "#888888", "marker": "v", "ms": 5},  # 3y back
    {"color": "#888888", "marker": "D", "ms": 4.5},  # 2y back
    {"color": "#888888", "marker": "s", "ms": 4.5},  # 1y back
    {"color": "#2171b5", "marker": "o", "ms": 6},  # vaccination
    {"color": "#888888", "marker": "^", "ms": 5},  # 1y forward
]

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

RATIO_BUCKETS = {"ZERO_PE_SUSPECTIBLE", "NEVER_PRESCRIBED", "0_PE"}

N_PERIODS = len(PERIODS)
ROW_HEIGHT = N_PERIODS + 1.5  # vertical space per age group


def load_effects(directory: Path, bucket: str) -> dict | None:
    path = directory / bucket / "effects_summary.json"
    if not path.exists():
        return None
    with open(path) as f:
        data = json.load(f)
    return {entry["věk"]: entry for entry in data}


def _fmt_n(n: int) -> str:
    return f"n={n:,}".replace(",", "\u2009")


def make_forest_plot(bucket: str, ax: plt.Axes):
    is_ratio = bucket in RATIO_BUCKETS
    ref_line = 1.0 if is_ratio else 0.0

    all_data = []
    for dir_name, label in PERIODS:
        d = load_effects(BASE / dir_name / "whole_period", bucket)
        all_data.append(d)

    for age_idx, age in enumerate(AGE_ORDER):
        group_base = age_idx * ROW_HEIGHT

        for p_idx, (data, (_, plabel), style) in enumerate(
            zip(all_data, PERIODS, PERIOD_STYLES)
        ):
            if data is None or age not in data:
                continue
            entry = data[age]
            med = entry["Med"]
            ci_lo, ci_hi = entry["95% CI"]
            y = group_base + p_idx

            ax.errorbar(
                med,
                y,
                xerr=[[med - ci_lo], [ci_hi - med]],
                fmt=style["marker"],
                color=style["color"],
                markersize=style["ms"],
                capsize=3,
                linewidth=1.5,
                markeredgewidth=1.5,
            )
            ax.annotate(
                _fmt_n(entry["počet očko"]),
                (ci_hi, y),
                textcoords="offset points",
                xytext=(5, 0),
                fontsize=6,
                color=style["color"],
                va="center",
            )

    yticks = []
    ylabels = []
    for age_idx, age in enumerate(AGE_ORDER):
        mid = age_idx * ROW_HEIGHT + (N_PERIODS - 1) / 2
        yticks.append(mid)
        ylabels.append(AGE_LABELS[age])

    ax.set_yticks(yticks)
    ax.set_yticklabels(ylabels, fontsize=9)
    ax.invert_yaxis()

    ax.axvline(ref_line, color="grey", linestyle="--", linewidth=0.8, zorder=0)

    for age_idx in range(len(AGE_ORDER) - 1):
        sep_y = (age_idx + 1) * ROW_HEIGHT - 1
        ax.axhline(sep_y, color="#dddddd", linestyle="-", linewidth=0.5, zorder=0)

    ax.set_title(BUCKET_LABELS[bucket], fontsize=12, fontweight="bold", pad=10)
    ax.set_xlabel("Medián efektu (95% CI)", fontsize=9)
    ax.grid(axis="x", alpha=0.2, linewidth=0.5)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.margins(x=0.15)


def main():
    legend_elements = [
        plt.Line2D(
            [0],
            [0],
            marker=s["marker"],
            color=s["color"],
            linestyle="None",
            markersize=s["ms"],
            label=label,
        )
        for (_, label), s in zip(PERIODS, PERIOD_STYLES)
    ]

    out_dir = Path("out/cpzp/forest_plots/more_years_included")
    out_dir.mkdir(parents=True, exist_ok=True)

    for bucket in BUCKETS:
        fig, ax = plt.subplots(figsize=(10, 4.5))
        make_forest_plot(bucket, ax)
        fig.legend(
            handles=legend_elements,
            loc="upper right",
            fontsize=8,
            frameon=True,
            fancybox=True,
            edgecolor="#cccccc",
        )
        fig.tight_layout()
        out_path = out_dir / f"forest_{bucket.lower()}.png"
        fig.savefig(out_path, dpi=200, bbox_inches="tight", facecolor="white")
        plt.close(fig)
        print(f"Saved → {out_path}")


if __name__ == "__main__":
    main()
