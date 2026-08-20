"""
Forest plot visualization of the booster (3rd dose) matching analysis.

Compares boosted persons (effective ≥3 doses) against two matched pools:
  - "2-dose, no booster" persons (Pool B)
  - never-vaccinated persons (Pool C)

Shows 5 periods (3y back → 1y forward) for the 0 PE group, broken down by
age cohort. Three plot types:
  1. Effect vs. 2-dose pool (ratio, Med + 95% CI)
  2. Effect vs. never-vaccinated pool (ratio, Med + 95% CI)
  3. Raw before/after PE per person for all three arms (boosted / 2-dose / novax)
"""

import argparse
import json
import multiprocessing as mp
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

DEFAULT_OUT_ROOT = Path("out")
DEFAULT_COMPANIES = ["cpzp"]

COMPANY_LABELS = {
    "cpzp": "Data z ČPZP",
    "ozp": "Data z OZP",
    "both_companies": "Souhrnná data",
}

ANALYSIS_MODES = [
    "non_inj_analysis",
    "inj_analysis",
    "every_prescription_analysis",
]

EFFECT_BASELINES = [
    "different_effect_baseline",
    "unified_effect_baseline",
]

PERIODS = [
    ("3_years_back_matching_analysis", "3 roky zpět"),
    ("2_years_back_matching_analysis", "2 roky zpět"),
    ("1_years_back_matching_analysis", "1 rok zpět"),
    ("0_years_back_matching_analysis", "Boostrovací období"),
    ("-1_years_back_matching_analysis", "1 rok dopředu"),
]

PERIOD_STYLES = [
    {"color": "#888888", "marker": "v", "ms": 5},  # 3y back
    {"color": "#888888", "marker": "D", "ms": 4.5},  # 2y back
    {"color": "#888888", "marker": "s", "ms": 4.5},  # 1y back
    {"color": "#2171b5", "marker": "o", "ms": 6},  # booster period
    {"color": "#888888", "marker": "^", "ms": 5},  # 1y forward
]

AGE_ORDER = ["12-15", "16-22", "23-29", "30-39", "40-49", "50-59"]
AGE_LABELS = {
    "12-15": "12–15 let",
    "16-22": "16–22 let",
    "23-29": "23–29 let",
    "30-39": "30–39 let",
    "40-49": "40–49 let",
    "50-59": "50–59 let",
}

N_PERIODS = len(PERIODS)
ROW_HEIGHT = N_PERIODS + 1.5

BOOSTED_COLOR = "#2171b5"
TWO_DOSE_COLOR = "#238b45"
NOVAX_COLOR = "#d94801"

ARM_MARKERS = ["v", "D", "s", "o", "^"]


def load_effects(base: Path) -> dict | None:
    """Load `.../whole_period/0_PE/effects_summary.json`, keyed by age."""
    path = base / "whole_period" / "0_PE" / "effects_summary.json"
    if not path.exists():
        return None
    with open(path) as f:
        data = json.load(f)
    return {entry["věk"]: entry for entry in data}


def make_ratio_forest(
    ax: plt.Axes,
    *,
    base: Path,
    med_key: str,
    ci_key: str,
    title: str,
):
    """Forest plot of a ratio-style effect (boosted/2-dose or boosted/novax)."""
    all_data = [load_effects(base / dir_name) for dir_name, _ in PERIODS]

    for age_idx, age in enumerate(AGE_ORDER):
        group_base = age_idx * ROW_HEIGHT

        for p_idx, (data, style) in enumerate(zip(all_data, PERIOD_STYLES)):
            if data is None or age not in data:
                continue
            entry = data[age]
            med = entry.get(med_key)
            ci = entry.get(ci_key)
            if med is None or ci is None:
                continue
            ci_lo, ci_hi = ci
            y = group_base + p_idx

            ax.errorbar(
                med,
                y,
                xerr=[[max(0, med - ci_lo)], [max(0, ci_hi - med)]],
                fmt=style["marker"],
                color=style["color"],
                markersize=style["ms"],
                capsize=3,
                linewidth=1.5,
                markeredgewidth=1.5,
            )
            n_boosted = entry.get("počet boosterů", 0)
            label = f"n={n_boosted:,}".replace(",", "\u2009")
            ax.annotate(
                label,
                (ci_hi, y),
                textcoords="offset points",
                xytext=(5, 0),
                fontsize=5.5,
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

    ax.axvline(1.0, color="grey", linestyle="--", linewidth=0.8, zorder=0)

    for age_idx in range(len(AGE_ORDER) - 1):
        sep_y = (age_idx + 1) * ROW_HEIGHT - 1
        ax.axhline(sep_y, color="#dddddd", linestyle="-", linewidth=0.5, zorder=0)

    ax.set_title(title, fontsize=12, fontweight="bold", pad=10)
    ax.set_xlabel("Medián efektu (95% CI)", fontsize=9)
    ax.grid(axis="x", alpha=0.2, linewidth=0.5)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.margins(x=0.15)


def make_raw_effects_plot(ax: plt.Axes, *, base: Path):
    """Plot per-person po-před PE for boosted / 2-dose / novax in one graph."""
    all_data = [load_effects(base / dir_name) for dir_name, _ in PERIODS]

    arms = [
        ("boosteři po-před", "boosteři 95% CI", BOOSTED_COLOR, "full"),
        ("2-davkoví po-před", "2-davkoví 95% CI", TWO_DOSE_COLOR, "none"),
        ("neočko po-před", "neočko 95% CI", NOVAX_COLOR, "none"),
    ]

    for age_idx, age in enumerate(AGE_ORDER):
        group_base = age_idx * ROW_HEIGHT

        for p_idx, data in enumerate(all_data):
            if data is None or age not in data:
                continue
            entry = data[age]
            y = group_base + p_idx
            marker = ARM_MARKERS[p_idx]

            rightmost = float("-inf")
            has_point = False

            for val_key, ci_key, color, fillstyle in arms:
                val = entry.get(val_key)
                ci = entry.get(ci_key)
                if val is None:
                    continue
                if ci is not None:
                    ci_lo, ci_hi = ci
                    ax.errorbar(
                        val,
                        y,
                        xerr=[[max(0, val - ci_lo)], [max(0, ci_hi - val)]],
                        fmt=marker,
                        color=color,
                        markersize=5,
                        capsize=2.5,
                        linewidth=1,
                        markeredgewidth=1.2,
                        fillstyle=fillstyle,
                    )
                    rightmost = max(rightmost, ci_hi)
                else:
                    ax.plot(
                        val,
                        y,
                        marker=marker,
                        color=color,
                        markersize=5,
                        linestyle="None",
                        markeredgewidth=1.2,
                        fillstyle=fillstyle,
                    )
                    rightmost = max(rightmost, val)
                has_point = True

            if has_point:
                n_boosted = entry.get("počet boosterů", 0)
                if n_boosted:
                    label = f"n={n_boosted:,}".replace(",", "\u2009")
                    ax.annotate(
                        label,
                        (rightmost, y),
                        textcoords="offset points",
                        xytext=(5, 0),
                        fontsize=5.5,
                        color="#555555",
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

    ax.axvline(0.0, color="grey", linestyle="--", linewidth=0.8, zorder=0)

    for age_idx in range(len(AGE_ORDER) - 1):
        sep_y = (age_idx + 1) * ROW_HEIGHT - 1
        ax.axhline(sep_y, color="#dddddd", linestyle="-", linewidth=0.5, zorder=0)

    ax.set_title(
        "Booster (0 PE) — průměrné PE na osobu (po – před)",
        fontsize=12,
        fontweight="bold",
        pad=10,
    )
    ax.set_xlabel("Průměrné PE na osobu (po – před)", fontsize=9)
    ax.grid(axis="x", alpha=0.2, linewidth=0.5)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.margins(x=0.15)


def _period_legend():
    return [
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


def _raw_legend() -> list:
    return [
        plt.Line2D(
            [0], [0], marker="o", color=BOOSTED_COLOR, linestyle="None",
            markersize=5, label="Boosteři (po–před)",
        ),
        plt.Line2D(
            [0], [0], marker="o", color=TWO_DOSE_COLOR, linestyle="None",
            markersize=5, fillstyle="none", label="2-dávkoví, nenaboostrovaní (po–před)",
        ),
        plt.Line2D(
            [0], [0], marker="o", color=NOVAX_COLOR, linestyle="None",
            markersize=5, fillstyle="none", label="Neočkovaní (po–před)",
        ),
    ]


def _worker(job: tuple) -> str:
    kind, base_str, out_str = job
    base = Path(base_str)
    out_path = Path(out_str)
    fig_height = max(5, len(AGE_ORDER) * 1.8)

    if kind == "vs_two_dose":
        fig, ax = plt.subplots(figsize=(10, fig_height))
        make_ratio_forest(
            ax,
            base=base,
            med_key="efekt vs. 2 davky",
            ci_key="efekt vs. 2 davky 95% CI",
            title="Booster vs. 2-dávkoví (0 PE)",
        )
        fig.legend(
            handles=_period_legend(),
            loc="upper right",
            fontsize=8,
            frameon=True,
            fancybox=True,
            edgecolor="#cccccc",
        )
    elif kind == "vs_novax":
        fig, ax = plt.subplots(figsize=(10, fig_height))
        make_ratio_forest(
            ax,
            base=base,
            med_key="efekt vs. neočko",
            ci_key="efekt vs. neočko 95% CI",
            title="Booster vs. neočkovaní (0 PE)",
        )
        fig.legend(
            handles=_period_legend(),
            loc="upper right",
            fontsize=8,
            frameon=True,
            fancybox=True,
            edgecolor="#cccccc",
        )
    elif kind == "raw":
        fig, ax = plt.subplots(figsize=(10, fig_height))
        make_raw_effects_plot(ax, base=base)
        period_raw_legend = [
            plt.Line2D(
                [0],
                [0],
                marker=ARM_MARKERS[i],
                color="#555555",
                linestyle="None",
                markersize=5,
                label=label,
            )
            for i, (_, label) in enumerate(PERIODS)
        ]
        fig.legend(
            handles=_raw_legend() + period_raw_legend,
            loc="upper right",
            fontsize=8,
            frameon=True,
            fancybox=True,
            edgecolor="#cccccc",
        )
    else:
        raise ValueError(f"unknown plot kind: {kind}")

    fig.tight_layout()
    fig.savefig(out_path, dpi=200, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return str(out_path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--companies",
        nargs="+",
        default=DEFAULT_COMPANIES,
        help="Insurance companies to plot for (e.g. cpzp ozp both_companies).",
    )
    parser.add_argument(
        "--out-root",
        default=str(DEFAULT_OUT_ROOT),
        help="Root output directory containing <company>/booster_analysis/... (default: out).",
    )
    args = parser.parse_args()

    out_root = Path(args.out_root)
    jobs = []

    for company in args.companies:
        root = out_root / company / "booster_analysis"
        if not root.exists():
            print(f"Skipping {company}: {root} does not exist")
            continue

        for mode in ANALYSIS_MODES:
            for eb in EFFECT_BASELINES:
                base = root / mode / eb
                if not base.exists():
                    continue

                plots_root = base / "forest_plots"
                plots_root.mkdir(parents=True, exist_ok=True)

                jobs.append(("vs_two_dose", str(base), str(plots_root / "booster_vs_two_dose.png")))
                jobs.append(("vs_novax", str(base), str(plots_root / "booster_vs_novax.png")))
                jobs.append(("raw", str(base), str(plots_root / "raw_effects_0_pe.png")))

    print(f"Generating {len(jobs)} booster plots across {mp.cpu_count()} cores...")
    with mp.Pool() as pool:
        for path in pool.imap_unordered(_worker, jobs):
            print(f"  Saved → {path}")


if __name__ == "__main__":
    main()
