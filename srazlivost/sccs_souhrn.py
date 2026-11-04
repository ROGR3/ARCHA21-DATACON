#!/usr/bin/env python3
"""
Souhrnné grafy SCCS analýzy: skutečné očkování proti virtuálnímu.

Čte výstupy sccs_analysis.py (musí doběhnout s --year-offsets 0 1 2 3 4)
a kreslí:

  charts/<company>/sccs/souhrn/forest_<kohorta>_1_davka.png
      Forest plot pro jednu kohortu a 1. dávku. Bloky = okna ±30…365 dní,
      v bloku řádek na rok: 2021 skutečné očkování (modře), virtuální
      2020 (prázdný kroužek, covidový rok) a 2019–2017 (šedě). Osa x =
      poměr poměrů (po/před očkovaní ÷ po/před neočkovaní, logaritmicky).
      Poměr poměrů je odds ratio tabulky 2×2, interval je Woolfův.

  charts/<company>/sccs/souhrn/prubeh_<kohorta>_1_davka.png
      Průběh po 14denních periodách po 1. dávce, v % průměru období před
      očkováním. Modrá = skutečně očkovaní, šedý pás = rozsah virtuálních
      let, oranžová čárkovaná = neočkovaní.
"""

from __future__ import annotations

import argparse

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import polars as pl

from sccs_analysis import AGE_COHORTS, BIN_DAYS, METRICS, out_dir_for

YEAR_OFFSETS = [1, 2, 3, 4]
FOREST_PERIODS = [30, 60, 90, 180, 365]
COHORTS = [name for _, name in AGE_COHORTS if name != "vek_do_12"]
COHORT_LABELS = {
    "vek_12-29": "12–29 let",
    "vek_30-49": "30–49 let",
    "vek_50-59": "50–59 let",
    "vek_60_plus": "60+ let",
}

REAL_COLOR = "#2a6fdb"
NOVAX_COLOR = "#e07b00"
VIRTUAL_COLOR = "#9a9a9a"
INK = "#333333"


def load_results(company: str) -> pl.DataFrame:
    frames = []
    for off in [0, *YEAR_OFFSETS]:
        frames.append(
            pl.read_csv(out_dir_for(company, off) / "vysledky.csv").with_columns(
                pl.lit(off).alias("year_offset")
            )
        )
    return pl.concat(frames, how="diagonal_relaxed")


def load_bins(company: str) -> pl.DataFrame:
    frames = []
    for off in [0, *YEAR_OFFSETS]:
        frames.append(
            pl.read_csv(out_dir_for(company, off) / "14denni_periody" / "periody.csv").with_columns(
                pl.lit(off).alias("year_offset")
            )
        )
    return pl.concat(frames)


def format_p(p: float) -> str:
    return "p<0.001" if p < 0.001 else f"p={p:.3f}"


def woolf_ci(vb: float, va: float, nb: float, na: float) -> tuple[float, float]:
    if min(vb, va, nb, na) <= 0:
        return np.nan, np.nan
    se = np.sqrt(1 / vb + 1 / va + 1 / nb + 1 / na)
    log_or = np.log((va / vb) / (na / nb))
    return float(np.exp(log_or - 1.96 * se)), float(np.exp(log_or + 1.96 * se))


def draw_forest(company: str, results: pl.DataFrame, cohort: str, out_path) -> None:
    """Jedna kohorta, 1. dávka. Bloky = okna, v bloku řádek na každý rok
    (2021 skutečné, pak virtuální 2020 … 2017), každý se svým 95% intervalem."""
    offsets = [0, *YEAR_OFFSETS]
    rows = [(p, off) for p in FOREST_PERIODS for off in offsets]
    fig, axes = plt.subplots(1, len(METRICS), figsize=(16, 0.42 * len(rows) + 2.8), sharey=True)
    data = results.filter((pl.col("age_cohort") == cohort) & (pl.col("vax_dose") == 1))

    for ax, (metric, label) in zip(axes, METRICS):
        m = data.filter(pl.col("metric") == metric)
        for i, (period, off) in enumerate(rows):
            y = len(rows) - 1 - i
            sel = m.filter((pl.col("period_days") == period) & (pl.col("year_offset") == off))
            if not sel.height:
                continue
            r = sel.row(0, named=True)
            lo, hi = woolf_ci(r["vax_before"], r["vax_after"], r["novax_before"], r["novax_after"])
            ratio = r["vax_vs_novax_ratio"]
            color = REAL_COLOR if off == 0 else VIRTUAL_COLOR
            ax.plot([lo, hi], [y, y], color=color, lw=2, solid_capstyle="round")
            ax.plot(
                ratio, y, "o", ms=9 if off == 0 else 7, zorder=3, mew=1.5,
                color=color, mfc="white" if off == 1 else color, mec=color if off == 1 else "white",
            )
            ax.annotate(
                f"{ratio:.2f}  ({format_p(r['p_value'])})", xy=(1.0, y), xycoords=("axes fraction", "data"),
                xytext=(6, 0), textcoords="offset points", va="center", fontsize=9,
                color=INK if off == 0 else "#777777", fontweight="bold" if off == 0 else "normal",
            )
        ax.axvline(1, color=INK, lw=1)
        ax.set_xscale("log")
        ticks = [0.25, 0.5, 0.75, 1, 1.5, 2, 3, 4]
        ax.set_xticks(ticks, [f"{t:g}" for t in ticks])
        ax.minorticks_off()
        ax.set_xlim(0.25, 4)
        ax.set_title(label, fontsize=13, color=INK)
        ax.set_xlabel("poměr poměrů: (po/před očkovaní) ÷ (po/před neočkovaní)", color=INK)
        ax.grid(axis="x", color="#e5e5e5")
        for s in ["top", "right"]:
            ax.spines[s].set_visible(False)
        for k in range(1, len(FOREST_PERIODS)):
            ax.axhline(k * len(offsets) - 0.5, color="#bbbbbb", lw=0.8)

    labels = []
    for period, off in rows:
        year = "2021 skutečné" if off == 0 else f"{2021 - off} virtuální" + (" (covid)" if off == 1 else "")
        labels.append(f"±{period} dní   {year}" if off == 0 else year)
    axes[0].set_yticks(range(len(rows)), list(reversed(labels)))
    for tick, (_, off) in zip(axes[0].get_yticklabels(), reversed(rows)):
        if off == 0:
            tick.set_fontweight("bold")
    fig.suptitle(
        f"{company} – {COHORT_LABELS[cohort]}, 1. dávka: skutečné očkování vs stejná analýza v minulých letech\n"
        "> 1 = očkovaným přibylo víc než neočkovaným, úsečka = 95% interval",
        fontsize=14, color=INK,
    )
    fig.tight_layout(rect=(0, 0, 0.95, 1))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


def as_pct_of_before(df: pl.DataFrame) -> pl.DataFrame:
    """count → % průměru period před očkováním (bin_start_day < 0)."""
    keys = ["year_offset", "age_cohort", "vax_dose", "metric", "group"]
    return df.with_columns(
        (
            pl.col("count")
            / pl.col("count").filter(pl.col("bin_start_day") < 0).mean().over(keys)
            * 100
        ).alias("pct")
    )


def draw_timeline(company: str, bins: pl.DataFrame, cohort: str, out_path) -> None:
    fig, axes = plt.subplots(1, len(METRICS), figsize=(16, 6), sharey=False)
    d = bins.filter((pl.col("age_cohort") == cohort) & (pl.col("vax_dose") == 1))
    for ax, (metric, label) in zip(axes, METRICS):
        m = d.filter(pl.col("metric") == metric)
        x_of = lambda f: (f["bin_start_day"] + BIN_DAYS / 2).to_numpy()  # noqa: E731

        virt = (
            m.filter((pl.col("group") == "ockovani") & (pl.col("year_offset") > 0))
            .group_by("bin_start_day")
            .agg(pl.col("pct").min().alias("lo"), pl.col("pct").max().alias("hi"))
            .sort("bin_start_day")
        )
        ax.fill_between(x_of(virt), virt["lo"], virt["hi"], color=VIRTUAL_COLOR, alpha=0.25, lw=0,
                        label="virtuální očkování 2017–2020 (rozsah)")
        novax = m.filter((pl.col("group") == "neockovani") & (pl.col("year_offset") == 0)).sort("bin_start_day")
        ax.plot(x_of(novax), novax["pct"], color=NOVAX_COLOR, lw=1.5, ls="--", label="neočkovaní (2021)")
        real = m.filter((pl.col("group") == "ockovani") & (pl.col("year_offset") == 0)).sort("bin_start_day")
        ax.plot(x_of(real), real["pct"], color=REAL_COLOR, lw=2, marker="o", ms=5, label="očkovaní (2021)")

        ax.axhline(100, color=INK, lw=0.8)
        ax.axvline(0, color=INK, lw=1, ls=":")
        ax.set_title(label, fontsize=13, color=INK)
        ax.set_xlabel("dny od očkování (14denní periody)", color=INK)
        ax.set_ylabel("% průměru před očkováním", color=INK)
        ax.set_xticks(range(-168, 169, 28))
        ax.grid(color="#eeeeee")
        for s in ["top", "right"]:
            ax.spines[s].set_visible(False)
        n_real = int(real.filter(pl.col("bin_start_day") >= 0)["count"].sum())
        n_before = int(real.filter(pl.col("bin_start_day") < 0)["count"].sum())
        ax.text(0.01, 0.98, f"očkovaní: před {n_before:,}, po {n_real:,}", transform=ax.transAxes,
                va="top", fontsize=9, color=INK)
    axes[0].legend(loc="lower left", frameon=False, fontsize=9)
    fig.suptitle(f"{company} – {COHORT_LABELS[cohort]}, 1. dávka: průběh kolem očkování", fontsize=14, color=INK)
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


def run(company: str) -> None:
    out = out_dir_for(company, 0) / "souhrn"
    results = load_results(company)
    bins = as_pct_of_before(load_bins(company))
    for cohort in COHORTS:
        draw_forest(company, results, cohort, out / f"forest_{cohort}_1_davka.png")
        draw_timeline(company, bins, cohort, out / f"prubeh_{cohort}_1_davka.png")
    print(f"  hotovo: {out}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    parser.add_argument("--company", choices=["cpzp", "ozp", "both_companies"], default="both_companies")
    run(parser.parse_args().company)


if __name__ == "__main__":
    main()
