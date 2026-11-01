#!/usr/bin/env python3
"""
SCCS analýza předpisů B01 (srážlivost) kolem očkování — port notebooku
kortikoidy/max_vax_analysis.ipynb.

Postup (stejný jako u kortikoidů):
  1. Začátek očkování pro kohortu × dávku = první den, kdy denní počet
     očkování dosáhl 50 % maxima.
  2. Očkovaní = dostali danou dávku 0–60 dní od začátku očkování kohorty.
  3. Předpisy každého očkovaného přepočtené na dny od jeho očkování (den 0).
  4. Součty před (den < 0) a po (den ≥ 0) v oknech ±30/60/90/180/365 dní.
  5. Rozhodné datum = začátek očkování kohorty + 30 dní.
  6. Neočkovaní: jejich předpisy podle kalendáře vůči rozhodnému datu.
  7. Srovnání po/před očko vs neočko, Fisherův test.

Rozdíly proti kortikoidům:
  - metriky: "predpisy" = jen terapeutické dávky (jen_vyssi_davky
    z generate_charts.py), "prvopredpisy" = první B01 předpis bez
    preventivního heparinu (stejně jako charts/.../prvopredpisy/).
  - do analýzy jdou jen lidi s ≥1 terapeutickým předpisem.
  - pojištění celé období 2015-01-01 až 2023-12-31 (konec dat).
  - okno je u obou skupin [-P, P), u kortikoidů měli neočkovaní o den víc.
  - injekce se neslučují (to bylo kvůli kortikoidovým injekcím).
  - navíc graf po 14denních periodách v okně ±182 dní (perioda 0 = den 0–13).

Věková kohorta: očkovaní podle věku při očkování, neočkovaní podle věku
při předpisu (jako u kortikoidů).

Výstupy:
  out/<company>/sccs/results_all_periods.csv
  out/<company>/sccs/sums/<P>/<kohorta>-dose_<N>.png
  out/<company>/sccs/periody_14d/<kohorta>-dose_<N>.png
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
from scipy.stats import fisher_exact

from generate_charts import _higher_dose_expr, first_prescriptions, load_company

OUT_DIR = Path("out")

START_DATE = date(2015, 1, 1)
END_DATE = date(2023, 12, 31)
VAX_PERIOD_IN_DAYS = 30
PEAK_THRESHOLD = 0.5
PERIODS = [30, 60, 90, 180, 365]
DOSES = [1, 2, 3]
BIN_DAYS = 14
N_BINS = 13  # 13 × 14 = 182 dní na každou stranu

# (spodní věk včetně, název) — stejné hranice jako AgeCohort u kortikoidů
AGE_COHORTS = [
    (0, "less_than_12"),
    (12, "between_12_and_30"),
    (30, "between_30_and_50"),
    (50, "between_50_and_60"),
    (60, "more_than_60"),
]
METRICS = [("predpisy", "Předpisy (terapeutické dávky)"), ("prvopredpisy", "Prvopředpisy")]

EXTRA_COLS = [
    "Mesic_narozeni",
    "Posledni_zahajeni_pojisteni",
    "Posledni_ukonceni_pojisteni",
    "Datum_umrti",
]


def _to_date(col: str) -> pl.Expr:
    return pl.col(col).cast(pl.Utf8).str.to_date("%Y-%m-%d", strict=False)


def age_cohort_expr(event_col: str) -> pl.Expr:
    """Věk k datu události (rok − rok narození, −1 když ještě neměl narozeniny
    v měsíci; OZP měsíc nemá) → název kohorty."""
    month = pl.col("Mesic_narozeni").cast(pl.Int64, strict=False)
    age = (pl.col(event_col).dt.year() - pl.col("Rok_narozeni")) - (
        month.is_not_null() & (pl.col(event_col).dt.month() < month)
    ).cast(pl.Int64)
    expr = pl.lit(None, dtype=pl.Utf8)
    for lo, name in AGE_COHORTS:
        expr = pl.when(age >= lo).then(pl.lit(name)).otherwise(expr)
    return expr.alias("cohort")


def load(company: str) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Vrátí (people, therapeutic prescriptions, vaccinations)."""
    # OZP nemá měsíc narození (sloupec je celý prázdný → String), proto se
    # pojišťovny načtou zvlášť a Mesic_narozeni se sjednotí před spojením.
    parts = ["cpzp", "ozp"] if company == "both_companies" else [company]
    df = pl.concat([
        load_company(c, EXTRA_COLS)
        .with_columns(
            (pl.lit(f"{c}:") + pl.col("Id_pojistence")).alias("Id_pojistence"),
            pl.col("Mesic_narozeni").cast(pl.Int64, strict=False),
        )
        for c in parts
    ])
    people = (
        df.group_by("Id_pojistence")
        .agg(
            pl.col("Rok_narozeni").drop_nulls().first(),
            pl.col("Mesic_narozeni").drop_nulls().first(),
            _to_date("Posledni_zahajeni_pojisteni").drop_nulls().min().alias("zahajeni"),
            _to_date("Posledni_ukonceni_pojisteni").drop_nulls().max().alias("ukonceni"),
            _to_date("Datum_umrti").drop_nulls().first().alias("died_at"),
            (pl.col("Typ_udalosti") == "vakcinace").any().alias("is_vax"),
        )
        .collect()
    )
    presc = (
        df.filter(
            (pl.col("Typ_udalosti") == "předpis") & (pl.col("lekova_skupina") == "srazlivost")
        )
        .drop_nulls("Datum_udalosti")
        .with_columns(
            pl.col("ATC_skupina").str.slice(0, 5).alias("atc5"),
            _higher_dose_expr().alias("is_higher_dose"),
        )
        .select("Id_pojistence", "Datum_udalosti", "atc5", "is_higher_dose")
        .collect()
    )
    vax = (
        df.filter(pl.col("Typ_udalosti") == "vakcinace")
        .drop_nulls("Datum_udalosti")
        .select("Id_pojistence", "Datum_udalosti")
        .sort("Id_pojistence", "Datum_udalosti")
        .with_columns(
            pl.col("Datum_udalosti").rank("ordinal").over("Id_pojistence").alias("dose")
        )
        .collect()
    )
    return people, presc, vax


def start_vax_dates(vax: pl.DataFrame) -> pl.DataFrame:
    """Krok 1: (cohort, dose) → první den s počtem očkování ≥ 50 % maxima."""
    daily = vax.group_by("cohort", "dose", "Datum_udalosti").len()
    return (
        daily.with_columns(pl.col("len").max().over("cohort", "dose").alias("peak"))
        .filter(pl.col("len") >= PEAK_THRESHOLD * pl.col("peak"))
        .group_by("cohort", "dose")
        .agg(pl.col("Datum_udalosti").min().alias("start"))
    )


def daily_counts(rows: pl.DataFrame, keys: list[str], day_col: str) -> dict:
    """{(*keys): {day: count}}"""
    out: dict = {}
    for row in rows.group_by(*keys, day_col).len().iter_rows(named=True):
        out.setdefault(tuple(row[k] for k in keys), {})[row[day_col]] = row["len"]
    return out


def window(counts: dict, offset: int, period: int) -> np.ndarray:
    """Pole délky 2P pro dny -P..P-1, kde den = klíč - offset."""
    arr = np.zeros(2 * period)
    for d, n in counts.items():
        i = d - offset + period
        if 0 <= i < 2 * period:
            arr[i] = n
    return arr


def before_after(arr: np.ndarray) -> tuple[float, float]:
    half = len(arr) // 2
    return float(arr[:half].sum()), float(arr[half:].sum())


def safe_div(a: float, b: float) -> float:
    return np.nan if b == 0 else a / b


def draw_sums(ax, title: str, vb: float, va: float, nb: float, na: float) -> None:
    labels = ["Očkovaná: před", "Očkovaná: po", "Neočkovaná: před", "Neočkovaná: po"]
    values = [vb, va, nb, na]
    bars = ax.bar(labels, values, color=["skyblue", "skyblue", "seagreen", "seagreen"],
                  edgecolor="black", width=1)
    pct = [100, safe_div(va, vb) * 100, 100, safe_div(na, nb) * 100]
    for bar, value, p in zip(bars, values, pct):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                f"{value:,.0f} / {p:.2f}%", ha="center", va="bottom", fontsize=12)
    ax.set_title(title)
    ax.set_ylabel("Celkový počet")
    ax.grid(True, alpha=0.3)


def draw_bins(ax, arr: np.ndarray, title: str) -> None:
    bins = arr.reshape(2 * N_BINS, BIN_DAYS).sum(axis=1)
    x = np.arange(-N_BINS, N_BINS)
    colors = ["skyblue" if i < 0 else "salmon" for i in x]
    ax.bar(x, bins, color=colors, edgecolor="black", width=0.9)
    ax.axvline(-0.5, color="green", linestyle="--", linewidth=2)
    vb, va = before_after(arr)
    ax.axhline(vb / N_BINS, color="blue", linestyle="--", label="Průměr před")
    ax.axhline(va / N_BINS, color="purple", linestyle="--", label="Průměr po")
    ax.set_xticks(x[1::2], [f"{i * BIN_DAYS}" for i in x[1::2]])
    ax.set_xlabel("Začátek 14denní periody (dny od očkování / rozhodného data)")
    ax.set_ylabel("Počet")
    ax.set_title(f"{title} — před {vb:,.0f}, po {va:,.0f} ({safe_div(va, vb) * 100:.1f} %)")
    ax.legend()
    ax.grid(True, alpha=0.3)


def run(company: str) -> None:
    print(f"== {company}")
    people, presc, vax = load(company)
    out_dir = OUT_DIR / company / "sccs"

    alive = people.filter(pl.col("died_at").is_null())
    eligible = alive.filter(
        (pl.col("zahajeni") <= START_DATE)
        & (pl.col("ukonceni").is_null() | (pl.col("ukonceni") >= END_DATE))
    )
    ther = presc.filter(pl.col("is_higher_dose"))
    with_ther = ther.select("Id_pojistence").unique()
    eligible = eligible.join(with_ther, on="Id_pojistence", how="semi")
    first = first_prescriptions(presc).select("Id_pojistence", "Datum_udalosti")
    print(f"  {eligible.height:,} osob v analýze ({eligible['is_vax'].sum():,} očkovaných)")

    birth = people.select("Id_pojistence", "Rok_narozeni", "Mesic_narozeni")
    vax = vax.join(birth, on="Id_pojistence").with_columns(age_cohort_expr("Datum_udalosti"))

    # Krok 1: ze všech očkování živých lidí
    starts = start_vax_dates(vax.join(alive, on="Id_pojistence", how="semi"))
    start_map = {(r["cohort"], r["dose"]): r["start"] for r in starts.iter_rows(named=True)}

    # Krok 2: očkovaní v analýze, dávka 0–60 dní od začátku kohorty
    vax_sel = (
        vax.filter(pl.col("dose").is_in(DOSES))
        .join(eligible.filter(pl.col("is_vax")), on="Id_pojistence", how="semi")
        .join(starts, on=["cohort", "dose"])
        .filter(
            ((pl.col("Datum_udalosti") - pl.col("start")).dt.total_days())
            .is_between(0, 2 * VAX_PERIOD_IN_DAYS)
        )
        .select("Id_pojistence", pl.col("Datum_udalosti").alias("vax_date"), "cohort", "dose")
    )

    # Krok 3: dny předpisu od očkování
    def rel_days(events: pl.DataFrame) -> pl.DataFrame:
        return events.join(vax_sel, on="Id_pojistence").with_columns(
            (pl.col("Datum_udalosti") - pl.col("vax_date")).dt.total_days().alias("day")
        )

    vax_counts = {
        "predpisy": daily_counts(rel_days(ther), ["cohort", "dose"], "day"),
        "prvopredpisy": daily_counts(rel_days(first), ["cohort", "dose"], "day"),
    }

    # Krok 6: neočkovaní, kohorta podle věku při předpisu, kalendářní dny
    novax_ids = eligible.filter(~pl.col("is_vax")).select("Id_pojistence")

    def novax_events(events: pl.DataFrame) -> pl.DataFrame:
        return (
            events.join(novax_ids, on="Id_pojistence", how="semi")
            .join(birth, on="Id_pojistence")
            .with_columns(age_cohort_expr("Datum_udalosti"))
        )

    novax_counts = {
        "predpisy": daily_counts(novax_events(ther), ["cohort"], "Datum_udalosti"),
        "prvopredpisy": daily_counts(novax_events(first), ["cohort"], "Datum_udalosti"),
    }
    n_vax = {(c, d): n for c, d, n in vax_sel.group_by("cohort", "dose").len().iter_rows()}

    rows = []
    for (cohort, dose), start in sorted(start_map.items()):
        if dose not in DOSES or (cohort, dose) not in n_vax:
            continue
        rozhodne = start + timedelta(days=VAX_PERIOD_IN_DAYS)

        def arrays(metric: str, period: int) -> tuple[np.ndarray, np.ndarray]:
            v = window(vax_counts[metric].get((cohort, dose), {}), 0, period)
            n_map = {(d - rozhodne).days: c for d, c in novax_counts[metric].get((cohort,), {}).items()}
            return v, window(n_map, 0, period)

        for period in PERIODS:
            fig, axes = plt.subplots(nrows=1, ncols=len(METRICS), figsize=(20, 10))
            fig.suptitle(
                f"{company} - {period} dnů od {rozhodne.strftime('%d.%m.%Y')} - {cohort} - Dose {dose}",
                fontsize=24,
            )
            for ax, (metric, label) in zip(axes, METRICS):
                v, n = arrays(metric, period)
                vb, va = before_after(v)
                nb, na = before_after(n)
                draw_sums(ax, label, vb, va, nb, na)
                vax_inc = safe_div(va, vb) * 100
                novax_inc = safe_div(na, nb) * 100
                rows.append({
                    "period_days": period,
                    "age_cohort": cohort,
                    "vax_dose": dose,
                    "metric": metric,
                    "start_vax_date": start,
                    "rozhodne_datum": rozhodne,
                    "n_vax_persons": n_vax[(cohort, dose)],
                    "vax_increase": vax_inc,
                    "novax_increase": novax_inc,
                    "diff": vax_inc - novax_inc,
                    "p_value": fisher_exact([[int(vb), int(va)], [int(nb), int(na)]])[1],
                    "vax_before": vb,
                    "vax_after": va,
                    "novax_before": nb,
                    "novax_after": na,
                    "vax_vs_novax_ratio": safe_div(safe_div(va, vb), safe_div(na, nb)),
                })
            path = out_dir / "sums" / str(period) / f"{cohort}-dose_{dose}.png"
            path.parent.mkdir(parents=True, exist_ok=True)
            fig.tight_layout()
            fig.savefig(path)
            plt.close(fig)

        # 14denní periody ±182 dní
        fig, axes = plt.subplots(nrows=2, ncols=len(METRICS), figsize=(24, 12))
        fig.suptitle(
            f"{company} - 14denní periody - {cohort} - Dose {dose} "
            f"(rozhodné datum neočko {rozhodne.strftime('%d.%m.%Y')})",
            fontsize=20,
        )
        for col, (metric, label) in enumerate(METRICS):
            v, n = arrays(metric, N_BINS * BIN_DAYS)
            draw_bins(axes[0][col], v, f"Očkovaní - {label}")
            draw_bins(axes[1][col], n, f"Neočkovaní - {label}")
        path = out_dir / "periody_14d" / f"{cohort}-dose_{dose}.png"
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.tight_layout()
        fig.savefig(path)
        plt.close(fig)

    results = pl.DataFrame(rows).sort("age_cohort", "vax_dose", "period_days", "metric")
    results.write_csv(out_dir / "results_all_periods.csv")
    print(f"  hotovo: {out_dir}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    parser.add_argument("--company", choices=["cpzp", "ozp", "both_companies"], default="cpzp")
    run(parser.parse_args().company)


if __name__ == "__main__":
    main()
