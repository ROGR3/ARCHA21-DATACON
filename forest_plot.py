"""
Forest plot visualization of matching analysis effects.

Shows 5 periods (3y back → 1y forward) for each PE bucket,
broken down by age cohort.

Two plot types per bucket:
  1. Treatment effect (Med + 95 % CI) — same as before
  2. Raw before/after PE sums for vax vs novax in a single graph
"""

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path("out/cpzp/matching_analysis")

ANALYSIS_MODES = [
    "non_inj_analysis",
    "inj_analysis",
    "every_prescription_analysis",
]

EFFECT_BASELINES = [
    "different_effect_baseline",
    "unified_effect_baseline",
]

MODE_LABELS = {
    "non_inj_analysis": "non-inj",
    "inj_analysis": "inj",
    "every_prescription_analysis": "every-rx",
}

BASELINE_LABELS = {
    "different_effect_baseline": "diff-baseline",
    "unified_effect_baseline": "uni-baseline",
}

PERIODS = [
    ("3_years_back_matching_analysis", "3 roky zpět"),
    ("2_years_back_matching_analysis", "2 roky zpět"),
    ("1_years_back_matching_analysis", "1 rok zpět"),
    ("0_years_back_matching_analysis", "Očkovací období"),
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

AGE_ORDER = ["12-15", "16-22", "23-29", "30-39", "40-49", "50-59"]
AGE_LABELS = {
    "12-15": "12–15 let",
    "16-22": "16–22 let",
    "23-29": "23–29 let",
    "30-39": "30–39 let",
    "40-49": "40–49 let",
    "50-59": "50–59 let",
}

RATIO_BUCKETS = {"ZERO_PE_SUSPECTIBLE", "NEVER_PRESCRIBED", "0_PE"}
DIFF_BASELINE_RATIO_BUCKETS = {"1_to_500_PE", "500_to_5000_PE"}

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


def make_forest_plot(bucket: str, ax: plt.Axes, *, base: Path, effect_baseline: str):
    if (
        effect_baseline == "unified_effect_baseline"
        or bucket in DIFF_BASELINE_RATIO_BUCKETS
    ):
        ref_line = 0.0
    else:
        ref_line = 1.0

    all_data = []
    for dir_name, label in PERIODS:
        d = load_effects(base / dir_name / "whole_period", bucket)
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
            if med is None or entry["95% CI"] is None:
                continue
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


VAX_COLOR = "#2171b5"
NOVAX_COLOR = "#d94801"

VAX_MARKERS = ["v", "D", "s", "o", "^"]
NOVAX_MARKERS = ["v", "D", "s", "o", "^"]


def make_raw_effects_plot(bucket: str, ax: plt.Axes, *, base: Path, effect_baseline: str):
    """Plot očko po-před and neočko po-před for each period in one graph."""
    all_data = []
    for dir_name, _ in PERIODS:
        d = load_effects(base / dir_name / "whole_period", bucket)
        all_data.append(d)

    for age_idx, age in enumerate(AGE_ORDER):
        group_base = age_idx * ROW_HEIGHT

        for p_idx, (data, (_, plabel)) in enumerate(zip(all_data, PERIODS)):
            if data is None or age not in data:
                continue
            entry = data[age]
            vax_val = entry.get("očko po-před")
            novax_val = entry.get("neočko po-před")
            y = group_base + p_idx
            marker = VAX_MARKERS[p_idx]

            vax_ci = entry.get("očko 95% CI")
            if vax_val is not None:
                if vax_ci is not None:
                    ci_lo, ci_hi = vax_ci
                    ax.errorbar(
                        vax_val,
                        y,
                        xerr=[[max(0, vax_val - ci_lo)], [max(0, ci_hi - vax_val)]],
                        fmt=marker,
                        color=VAX_COLOR,
                        markersize=5,
                        capsize=2.5,
                        linewidth=1,
                        markeredgewidth=1.2,
                    )
                else:
                    ax.plot(
                        vax_val,
                        y,
                        marker=marker,
                        color=VAX_COLOR,
                        markersize=5,
                        linestyle="None",
                        markeredgewidth=1.2,
                    )
            novax_ci = entry.get("neočko 95% CI")
            if novax_val is not None:
                if novax_ci is not None:
                    ci_lo, ci_hi = novax_ci
                    ax.errorbar(
                        novax_val,
                        y,
                        xerr=[[max(0, novax_val - ci_lo)], [max(0, ci_hi - novax_val)]],
                        fmt=marker,
                        color=NOVAX_COLOR,
                        markersize=5,
                        capsize=2.5,
                        linewidth=1,
                        markeredgewidth=1.2,
                        fillstyle="none",
                    )
                else:
                    ax.plot(
                        novax_val,
                        y,
                        marker=marker,
                        color=NOVAX_COLOR,
                        markersize=5,
                        linestyle="None",
                        markeredgewidth=1.2,
                        fillstyle="none",
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

    is_ratio = (
        effect_baseline == "different_effect_baseline"
        and bucket in DIFF_BASELINE_RATIO_BUCKETS
    )
    ref_line = 1.0 if is_ratio else 0.0
    op_label = "po / před" if is_ratio else "po – před"

    ax.axvline(ref_line, color="grey", linestyle="--", linewidth=0.8, zorder=0)

    for age_idx in range(len(AGE_ORDER) - 1):
        sep_y = (age_idx + 1) * ROW_HEIGHT - 1
        ax.axhline(sep_y, color="#dddddd", linestyle="-", linewidth=0.5, zorder=0)

    ax.set_title(
        f"{BUCKET_LABELS[bucket]} — průměrné PE na osobu ({op_label})",
        fontsize=12,
        fontweight="bold",
        pad=10,
    )
    ax.set_xlabel(f"Průměrné PE na osobu ({op_label})", fontsize=9)
    ax.grid(axis="x", alpha=0.2, linewidth=0.5)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.margins(x=0.15)


VAX_PERIOD_IDX = 3  # "0_years_back_matching_analysis" = vaccination period


def make_single_period_forest(bucket: str, ax: plt.Axes, *, base: Path, effect_baseline: str):
    """Forest plot showing only the vaccination period (blue markers)."""
    if (
        effect_baseline == "unified_effect_baseline"
        or bucket in DIFF_BASELINE_RATIO_BUCKETS
    ):
        ref_line = 0.0
    else:
        ref_line = 1.0

    dir_name, _ = PERIODS[VAX_PERIOD_IDX]
    style = PERIOD_STYLES[VAX_PERIOD_IDX]
    data = load_effects(base / dir_name / "whole_period", bucket)

    for age_idx, age in enumerate(AGE_ORDER):
        if data is None or age not in data:
            continue
        entry = data[age]
        med = entry["Med"]
        if med is None or entry["95% CI"] is None:
            continue
        ci_lo, ci_hi = entry["95% CI"]

        ax.errorbar(
            med,
            age_idx,
            xerr=[[max(0, med - ci_lo)], [max(0, ci_hi - med)]],
            fmt=style["marker"],
            color=style["color"],
            markersize=style["ms"],
            capsize=3,
            linewidth=1.5,
            markeredgewidth=1.5,
        )
        ax.annotate(
            _fmt_n(entry["počet očko"]),
            (ci_hi, age_idx),
            textcoords="offset points",
            xytext=(5, 0),
            fontsize=7,
            color=style["color"],
            va="center",
        )

    ax.set_yticks(range(len(AGE_ORDER)))
    ax.set_yticklabels([AGE_LABELS[a] for a in AGE_ORDER], fontsize=9)
    ax.invert_yaxis()

    ax.axvline(ref_line, color="grey", linestyle="--", linewidth=0.8, zorder=0)
    ax.set_title(
        f"{BUCKET_LABELS[bucket]} — očkovací období",
        fontsize=12,
        fontweight="bold",
        pad=10,
    )
    ax.set_xlabel("Medián efektu (95% CI)", fontsize=9)
    ax.grid(axis="x", alpha=0.2, linewidth=0.5)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.margins(x=0.15, y=0.1)


def main():
    period_legend = [
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

    def _raw_legend(bucket: str, effect_baseline: str) -> list:
        is_ratio = (
            effect_baseline == "different_effect_baseline"
            and bucket in DIFF_BASELINE_RATIO_BUCKETS
        )
        op = "po/před" if is_ratio else "po–před"
        return [
            plt.Line2D(
                [0],
                [0],
                marker="o",
                color=VAX_COLOR,
                linestyle="None",
                markersize=5,
                label=f"Očkovaní ({op})",
            ),
            plt.Line2D(
                [0],
                [0],
                marker="o",
                color=NOVAX_COLOR,
                linestyle="None",
                markersize=5,
                fillstyle="none",
                label=f"Neočkovaní ({op})",
            ),
        ]

    for mode in ANALYSIS_MODES:
        for eb in EFFECT_BASELINES:
            base = ROOT / mode / eb
            if not base.exists():
                continue
            plots_root = base / "forest_plots"
            te_dir = plots_root / "treatment_effect"
            raw_dir = plots_root / "raw_effects"
            nh_dir = plots_root / "no_history"
            for d in (te_dir, raw_dir, nh_dir):
                d.mkdir(parents=True, exist_ok=True)
            tag = f"{MODE_LABELS[mode]}/{BASELINE_LABELS[eb]}"

            for bucket in BUCKETS:
                fig_height = max(5, len(AGE_ORDER) * 1.8)
                kw = dict(base=base, effect_baseline=eb)

                # — treatment effect forest plot —
                fig, ax = plt.subplots(figsize=(10, fig_height))
                make_forest_plot(bucket, ax, **kw)
                fig.legend(
                    handles=period_legend,
                    loc="upper right",
                    fontsize=8,
                    frameon=True,
                    fancybox=True,
                    edgecolor="#cccccc",
                )
                fig.tight_layout()
                out_path = te_dir / f"forest_{bucket.lower()}.png"
                fig.savefig(out_path, dpi=200, bbox_inches="tight", facecolor="white")
                plt.close(fig)
                print(f"[{tag}] Saved → {out_path}")

                # — raw vax/novax before→after plot —
                fig2, ax2 = plt.subplots(figsize=(10, fig_height))
                make_raw_effects_plot(bucket, ax2, **kw)

                period_raw_legend = [
                    plt.Line2D(
                        [0],
                        [0],
                        marker=VAX_MARKERS[i],
                        color="#555555",
                        linestyle="None",
                        markersize=5,
                        label=label,
                    )
                    for i, (_, label) in enumerate(PERIODS)
                ]
                fig2.legend(
                    handles=_raw_legend(bucket, eb) + period_raw_legend,
                    loc="upper right",
                    fontsize=8,
                    frameon=True,
                    fancybox=True,
                    edgecolor="#cccccc",
                )
                fig2.tight_layout()
                out_path2 = raw_dir / f"raw_effects_{bucket.lower()}.png"
                fig2.savefig(out_path2, dpi=200, bbox_inches="tight", facecolor="white")
                plt.close(fig2)
                print(f"[{tag}] Saved → {out_path2}")

                # — vaccination-only forest plot —
                fig3, ax3 = plt.subplots(figsize=(8, max(3, len(AGE_ORDER) * 0.7)))
                make_single_period_forest(bucket, ax3, **kw)
                fig3.tight_layout()
                out_path3 = nh_dir / f"no_history_forest_{bucket.lower()}.png"
                fig3.savefig(out_path3, dpi=200, bbox_inches="tight", facecolor="white")
                plt.close(fig3)
                print(f"[{tag}] Saved → {out_path3}")


if __name__ == "__main__":
    main()
