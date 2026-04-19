"""
Per-specialty forest plots in the same style as forest_plot.py:
5 periods (3y back → 1y forward) per age cohort, one file per
(specialty × PE bucket).

Two plot types per (specialty × bucket):
  1. Treatment effect (Med + 95% CI) — vax−novax PE difference per person
  2. Raw vax (filled) and novax (open) PE change with 95% CI whiskers
"""

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

EFFECT_BASELINE = "different_effect_baseline"
BASE = Path(f"out/cpzp/matching_analysis/non_inj_analysis/{EFFECT_BASELINE}")

PERIODS = [
    ("3_years_back_matching_analysis", "3 roky zpět"),
    ("2_years_back_matching_analysis", "2 roky zpět"),
    ("1_years_back_matching_analysis", "1 rok zpět"),
    ("0_years_back_matching_analysis", "Očkovací období"),
    ("-1_years_back_matching_analysis", "1 rok dopředu"),
]

PERIOD_STYLES = [
    {"color": "#888888", "marker": "v", "ms": 5},
    {"color": "#888888", "marker": "D", "ms": 4.5},
    {"color": "#888888", "marker": "s", "ms": 4.5},
    {"color": "#2171b5", "marker": "o", "ms": 6},
    {"color": "#888888", "marker": "^", "ms": 5},
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

SPECIALTIES = [
    "ortopedie_chirurgie",
    "revmatologie",
    "neurologie",
    "praktik",
    "gastroenterologie",
    "pneumo_ftizeo",
    "interna",
    "other",
]

SPEC_LABELS = {
    "ortopedie_chirurgie": "Ortopedie + chirurgie",
    "revmatologie": "Revmatologie",
    "neurologie": "Neurologie",
    "praktik": "Praktický lékař",
    "gastroenterologie": "Gastroenterologie",
    "pneumo_ftizeo": "Pneumologie + ftizeo",
    "interna": "Interna",
    "other": "Ostatní",
}

VAX_COLOR = "#2171b5"
NOVAX_COLOR = "#d94801"

N_PERIODS = len(PERIODS)
ROW_HEIGHT = N_PERIODS + 1.5


def _fmt_n(n: int) -> str:
    return f"n={n:,}".replace(",", "\u2009")


def load_specialty_data(directory: Path, bucket: str) -> dict | None:
    path = directory / bucket / "specialty_effects.json"
    if not path.exists():
        return None
    with open(path) as f:
        rows = json.load(f)
    result = {}
    for row in rows:
        spec = row["specializace"]
        age = row["věk"]
        result.setdefault(spec, {})[age] = row
    return result


def make_spec_treatment_effect_plot(bucket: str, spec: str, ax: plt.Axes):
    """Treatment-effect forest plot for one specialty: Med + 95% CI across periods."""
    all_data = []
    for dir_name, _ in PERIODS:
        d = load_specialty_data(BASE / dir_name / "whole_period", bucket)
        all_data.append(d)

    for age_idx, age in enumerate(AGE_ORDER):
        group_base = age_idx * ROW_HEIGHT

        for p_idx, (data, (_, plabel), style) in enumerate(
            zip(all_data, PERIODS, PERIOD_STYLES)
        ):
            if data is None or spec not in data or age not in data[spec]:
                continue
            entry = data[spec][age]
            med = entry.get("Med")
            ci = entry.get("95% CI")
            if med is None or ci is None:
                continue
            ci_lo, ci_hi = ci
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

            vax_count = entry.get("počet očko", 0)
            spec_count = entry.get("počet u spec.", 0)
            if vax_count:
                label = f"n={vax_count:,} ({spec_count:,} u spec.)".replace(
                    ",", "\u2009"
                )
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
    ax.axvline(0, color="grey", linestyle="--", linewidth=0.8, zorder=0)

    for age_idx in range(len(AGE_ORDER) - 1):
        sep_y = (age_idx + 1) * ROW_HEIGHT - 1
        ax.axhline(sep_y, color="#dddddd", linestyle="-", linewidth=0.5, zorder=0)

    ax.set_title(
        f"{BUCKET_LABELS[bucket]} — {SPEC_LABELS[spec]} (treatment effect)",
        fontsize=12,
        fontweight="bold",
        pad=10,
    )
    ax.set_xlabel("Medián efektu (95% CI) — PE na osobu (vax−novax)", fontsize=9)
    ax.grid(axis="x", alpha=0.2, linewidth=0.5)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.margins(x=0.15)


def make_spec_period_plot(bucket: str, spec: str, ax: plt.Axes):
    """Forest plot for one specialty: 5 periods × age cohorts, vax + novax."""
    all_data = []
    for dir_name, _ in PERIODS:
        d = load_specialty_data(BASE / dir_name / "whole_period", bucket)
        all_data.append(d)

    for age_idx, age in enumerate(AGE_ORDER):
        group_base = age_idx * ROW_HEIGHT

        for p_idx, (data, (_, plabel), style) in enumerate(
            zip(all_data, PERIODS, PERIOD_STYLES)
        ):
            if data is None or spec not in data or age not in data[spec]:
                continue
            entry = data[spec][age]
            vax_val = entry.get("očko PE po-před")
            novax_val = entry.get("neočko PE po-před")
            y = group_base + p_idx
            marker = style["marker"]

            rightmost = float("-inf")
            has_point = False

            if vax_val is not None:
                vax_ci = entry.get("očko 95% CI")
                if vax_ci is not None:
                    lo, hi = vax_ci
                    ax.errorbar(
                        vax_val,
                        y,
                        xerr=[[max(0, vax_val - lo)], [max(0, hi - vax_val)]],
                        fmt=marker,
                        color=VAX_COLOR,
                        markersize=style["ms"],
                        capsize=2.5,
                        linewidth=1,
                        markeredgewidth=1.2,
                    )
                    rightmost = max(rightmost, hi)
                    has_point = True
                else:
                    ax.plot(
                        vax_val,
                        y,
                        marker=marker,
                        color=VAX_COLOR,
                        markersize=style["ms"],
                        linestyle="None",
                        markeredgewidth=1.2,
                    )
                    rightmost = max(rightmost, vax_val)
                    has_point = True

            if novax_val is not None:
                novax_ci = entry.get("neočko 95% CI")
                if novax_ci is not None:
                    lo, hi = novax_ci
                    ax.errorbar(
                        novax_val,
                        y,
                        xerr=[[max(0, novax_val - lo)], [max(0, hi - novax_val)]],
                        fmt=marker,
                        color=NOVAX_COLOR,
                        markersize=style["ms"],
                        capsize=2.5,
                        linewidth=1,
                        markeredgewidth=1.2,
                        fillstyle="none",
                    )
                    rightmost = max(rightmost, hi)
                    has_point = True
                else:
                    ax.plot(
                        novax_val,
                        y,
                        marker=marker,
                        color=NOVAX_COLOR,
                        markersize=style["ms"],
                        linestyle="None",
                        markeredgewidth=1.2,
                        fillstyle="none",
                    )
                    rightmost = max(rightmost, novax_val)
                    has_point = True

            vax_count = entry.get("počet očko", 0)
            spec_count = entry.get("počet u spec.", 0)
            if vax_count and has_point:
                label = f"n={vax_count:,} ({spec_count:,} u spec.)".replace(
                    ",", "\u2009"
                )
                ax.annotate(
                    label,
                    (rightmost, y),
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
    ax.axvline(0, color="grey", linestyle="--", linewidth=0.8, zorder=0)

    for age_idx in range(len(AGE_ORDER) - 1):
        sep_y = (age_idx + 1) * ROW_HEIGHT - 1
        ax.axhline(sep_y, color="#dddddd", linestyle="-", linewidth=0.5, zorder=0)

    ax.set_title(
        f"{BUCKET_LABELS[bucket]} — {SPEC_LABELS[spec]}",
        fontsize=12,
        fontweight="bold",
        pad=10,
    )
    ax.set_xlabel("Průměrné PE na osobu (po – před)", fontsize=9)
    ax.grid(axis="x", alpha=0.2, linewidth=0.5)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.margins(x=0.15)


def main():
    out_dir = BASE / "forest_plots" / "per_spec"
    out_dir.mkdir(parents=True, exist_ok=True)

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
    type_legend = [
        plt.Line2D(
            [0],
            [0],
            marker="o",
            color=VAX_COLOR,
            linestyle="None",
            markersize=5,
            label="Očkovaní (po–před)",
        ),
        plt.Line2D(
            [0],
            [0],
            marker="o",
            color=NOVAX_COLOR,
            linestyle="None",
            markersize=4,
            fillstyle="none",
            label="Neočkovaní (po–před)",
        ),
    ]

    for bucket in BUCKETS:
        for spec in SPECIALTIES:
            fig_height = max(5, len(AGE_ORDER) * 1.8)

            # — treatment effect forest plot —
            fig_te, ax_te = plt.subplots(figsize=(10, fig_height))
            make_spec_treatment_effect_plot(bucket, spec, ax_te)
            fig_te.legend(
                handles=period_legend,
                loc="upper right",
                fontsize=7,
                frameon=True,
                fancybox=True,
                edgecolor="#cccccc",
            )
            fig_te.tight_layout()
            out_te = out_dir / f"spec_te_{spec}_{bucket.lower()}.png"
            fig_te.savefig(out_te, dpi=200, bbox_inches="tight", facecolor="white")
            plt.close(fig_te)
            print(f"Saved → {out_te}")

            # — raw effects forest plot —
            fig, ax = plt.subplots(figsize=(10, fig_height))
            make_spec_period_plot(bucket, spec, ax)
            fig.legend(
                handles=period_legend + type_legend,
                loc="upper right",
                fontsize=7,
                frameon=True,
                fancybox=True,
                edgecolor="#cccccc",
            )
            fig.tight_layout()
            out_path = out_dir / f"spec_{spec}_{bucket.lower()}.png"
            fig.savefig(out_path, dpi=200, bbox_inches="tight", facecolor="white")
            plt.close(fig)
            print(f"Saved → {out_path}")


if __name__ == "__main__":
    main()
