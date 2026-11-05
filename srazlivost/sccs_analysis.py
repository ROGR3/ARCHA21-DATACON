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
    z generate_charts.py), "prvopredpisy" = první terapeutický předpis
    B01 u člověka v datech (stejný filtr jako "predpisy").
  - do analýzy jdou jen lidi s ≥1 terapeutickým předpisem.
  - pojištění celé období 2015-01-01 až 2023-12-31 (konec dat).
  - okno je u obou skupin [-P, P), u kortikoidů měli neočkovaní o den víc.
  - injekce se neslučují (to bylo kvůli kortikoidovým injekcím).
  - navíc graf po 14denních periodách v okně ±182 dní (perioda 0 = den 0–13).

Věková kohorta: očkovaní podle věku při očkování, neočkovaní podle věku
při předpisu (jako u kortikoidů).

Výstupy:
  charts/<company>/sccs/vysledky.csv
  charts/<company>/sccs/soucty_pred_po/okno_<P>_dni/<kohorta>_<N>_davka.png
  charts/<company>/sccs/14denni_periody/<kohorta>_<N>_davka.png
  charts/<company>/sccs/14denni_periody/periody.csv  (počty po periodách)
  charts/<company>/sccs/virtualni_ockovani/<N>_roky_drive/...  (stejný strom)
  charts/<company>/sccs/virtualni_ockovani/srovnani.csv

Virtuální očkování (--year-offsets 1 2 3 4): stejní lidé, každé očkování
posunuté o 365 × N dní dřív. Kontrola, jestli stejný „efekt“ nevyjde i bez
očkování (sezónnost, artefakt metody).

HVE (--hve-days 28): posledních 28 dní před očkováním (u neočkovaných před
rozhodným datem) se do „před“ nepočítá, okno „před“ je [-P-28, -28).
Výstupy jdou do charts/<company>/sccs_bez_28_dni_pred/ (stejný strom).

Normalizace na celkovou spotřebu (vždy): pro každého očkovaného se z celé
populace v analýze stejné věkové kohorty (věk při předpisu, očkovaní
i neočkovaní) spočítá, kolik předpisů padlo do jeho okna před a po.
Očekávaný poměr po/před = tyto součty přes všechny očkované. Sloupec
vax_vs_expected_ratio = (po/před očkovaní) ÷ (po/před očekávané),
p_value_expected = binomický test počtu „po“ při očekávaném podílu.
Popis v charts/README.md (sekce SCCS).
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
from scipy.stats import binomtest, fisher_exact

from generate_charts import _higher_dose_expr, load_company

CHARTS_DIR = Path("charts")

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
    (0, "vek_do_12"),
    (12, "vek_12-29"),
    (30, "vek_30-49"),
    (50, "vek_50-59"),
    (60, "vek_60_plus"),
]
METRICS = [("predpisy", "Předpisy (terapeutické dávky)"), ("prvopredpisy", "Prvopředpisy (terapeutické dávky)")]

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


def window(counts: dict, lo: int, hi: int) -> np.ndarray:
    """Pole pro dny lo..hi-1 (klíč = den)."""
    arr = np.zeros(hi - lo)
    for d, n in counts.items():
        if lo <= d < hi:
            arr[d - lo] = n
    return arr


def before_after(arr: np.ndarray) -> tuple[float, float]:
    half = len(arr) // 2
    return float(arr[:half].sum()), float(arr[half:].sum())


def sums_before_after(counts: dict, period: int, hve_days: int) -> tuple[float, float]:
    """Před = dny [-P-hve, -hve), po = dny [0, P)."""
    return (
        float(window(counts, -period - hve_days, -hve_days).sum()),
        float(window(counts, 0, period).sum()),
    )


class PopulationRate:
    """Denní počty událostí v celé populaci jedné kohorty, kumulativně,
    aby šlo rychle sečíst libovolné kalendářní okno."""

    def __init__(self, counts: dict):
        n = (END_DATE - START_DATE).days + 1
        daily = np.zeros(n)
        for d, c in counts.items():
            i = (d - START_DATE).days
            if 0 <= i < n:
                daily[i] = c
        self.cum = np.concatenate([[0.0], np.cumsum(daily)])
        self.n = n

    def total(self, start_idx: np.ndarray, end_idx: np.ndarray) -> np.ndarray:
        """Součet dnů [start, end) pro pole indexů (dny od START_DATE)."""
        a = np.clip(start_idx, 0, self.n)
        b = np.clip(end_idx, 0, self.n)
        return self.cum[b] - self.cum[a]


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


def offset_label(years: int) -> str:
    """1 → "1_rok_drive", 2–4 → "2_roky_drive", 5+ → "5_let_drive"."""
    unit = "rok" if years == 1 else "roky" if years < 5 else "let"
    return f"{years}_{unit}_drive"


def sccs_root(company: str, hve_days: int = 0) -> Path:
    name = "sccs" if hve_days == 0 else f"sccs_bez_{hve_days}_dni_pred"
    return CHARTS_DIR / company / name


def out_dir_for(company: str, year_offset: int, hve_days: int = 0) -> Path:
    base = sccs_root(company, hve_days)
    return base if year_offset == 0 else base / "virtualni_ockovani" / offset_label(year_offset)


def run(
    company: str,
    data: tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame],
    year_offset: int = 0,
    hve_days: int = 0,
) -> None:
    """year_offset > 0 = virtuální očkování: každé očkování se posune o
    365 × year_offset dní dřív. Kohorta zůstává podle věku při skutečném
    očkování, takže jde o stejné lidi jako ve skutečné analýze.
    hve_days > 0 = posledních hve_days dní před dnem 0 se do „před“ nepočítá."""
    print(
        f"== {company}"
        + (f", virtuální očkování o {year_offset} r. dřív" if year_offset else "")
        + (f", bez {hve_days} dní před" if hve_days else "")
    )
    people, presc, vax = data
    out_dir = out_dir_for(company, year_offset, hve_days)
    title_prefix = (
        company
        + (f" - VIRTUÁLNÍ očkování o {year_offset} r. dřív" if year_offset else "")
        + (f" - bez {hve_days} dní před" if hve_days else "")
    )

    alive = people.filter(pl.col("died_at").is_null())
    eligible = alive.filter(
        (pl.col("zahajeni") <= START_DATE)
        & (pl.col("ukonceni").is_null() | (pl.col("ukonceni") >= END_DATE))
    )
    ther = presc.filter(pl.col("is_higher_dose"))
    with_ther = ther.select("Id_pojistence").unique()
    eligible = eligible.join(with_ther, on="Id_pojistence", how="semi")
    # první terapeutický předpis; kdo měl dřív jen nižší dávku nebo
    # preventivní heparin, má prvopředpis až v den první terapeutické dávky
    first = (
        ther.sort("Datum_udalosti")
        .unique("Id_pojistence", keep="first")
        .select("Id_pojistence", "Datum_udalosti")
    )
    print(f"  {eligible.height:,} osob v analýze ({eligible['is_vax'].sum():,} očkovaných)")

    birth = people.select("Id_pojistence", "Rok_narozeni", "Mesic_narozeni")
    vax = (
        vax.join(birth, on="Id_pojistence")
        .with_columns(age_cohort_expr("Datum_udalosti"))
        .with_columns(pl.col("Datum_udalosti") - pl.duration(days=365 * year_offset))
    )

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

    # Normalizace: celá populace v analýze, kohorta podle věku při předpisu
    def population_events(events: pl.DataFrame) -> pl.DataFrame:
        return (
            events.join(eligible, on="Id_pojistence", how="semi")
            .join(birth, on="Id_pojistence")
            .with_columns(age_cohort_expr("Datum_udalosti"))
        )

    population = {
        metric: {
            key[0]: PopulationRate(c)
            for key, c in daily_counts(population_events(ev), ["cohort"], "Datum_udalosti").items()
        }
        for metric, ev in [("predpisy", ther), ("prvopredpisy", first)]
    }
    vax_days = {}
    for (c, d), grp in vax_sel.group_by("cohort", "dose"):
        per_day = grp.group_by("vax_date").len()
        idx = np.array([(v - START_DATE).days for v in per_day["vax_date"]])
        vax_days[(c, d)] = (idx, per_day["len"].to_numpy().astype(float))

    def expected(metric: str, cohort: str, dose: int, lo: int, hi: int) -> float:
        """Kolik událostí by očkovaní měli ve dnech [lo, hi) od svého očkování,
        kdyby se chovali jako celá populace (sečteno přes kalendář)."""
        rate = population[metric].get(cohort)
        if rate is None:
            return 0.0
        idx, n = vax_days[(cohort, dose)]
        return float((rate.total(idx + lo, idx + hi) * n).sum())

    rows = []
    bin_rows = []
    for (cohort, dose), start in sorted(start_map.items()):
        if dose not in DOSES or (cohort, dose) not in n_vax:
            continue
        rozhodne = start + timedelta(days=VAX_PERIOD_IN_DAYS)

        def counts(metric: str) -> tuple[dict, dict]:
            v = vax_counts[metric].get((cohort, dose), {})
            n_map = {(d - rozhodne).days: c for d, c in novax_counts[metric].get((cohort,), {}).items()}
            return v, n_map

        for period in PERIODS:
            fig, axes = plt.subplots(nrows=1, ncols=len(METRICS), figsize=(20, 10))
            fig.suptitle(
                f"{title_prefix} - {period} dnů od {rozhodne.strftime('%d.%m.%Y')} - {cohort} - {dose}. dávka",
                fontsize=24,
            )
            for ax, (metric, label) in zip(axes, METRICS):
                v, n = counts(metric)
                vb, va = sums_before_after(v, period, hve_days)
                nb, na = sums_before_after(n, period, hve_days)
                eb = expected(metric, cohort, dose, -period - hve_days, -hve_days)
                ea = expected(metric, cohort, dose, 0, period)
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
                    "expected_before": eb,
                    "expected_after": ea,
                    "vax_vs_expected_ratio": safe_div(safe_div(va, vb), safe_div(ea, eb)),
                    "p_value_expected": (
                        binomtest(int(va), int(va + vb), ea / (ea + eb)).pvalue
                        if va + vb > 0 and ea + eb > 0 else np.nan
                    ),
                    "hve_days": hve_days,
                })
            path = out_dir / "soucty_pred_po" / f"okno_{period}_dni" / f"{cohort}_{dose}_davka.png"
            path.parent.mkdir(parents=True, exist_ok=True)
            fig.tight_layout()
            fig.savefig(path)
            plt.close(fig)

        # 14denní periody ±182 dní
        fig, axes = plt.subplots(nrows=2, ncols=len(METRICS), figsize=(24, 12))
        fig.suptitle(
            f"{title_prefix} - 14denní periody - {cohort} - {dose}. dávka "
            f"(rozhodné datum neočko {rozhodne.strftime('%d.%m.%Y')})",
            fontsize=20,
        )
        half = N_BINS * BIN_DAYS
        for col, (metric, label) in enumerate(METRICS):
            v_map, n_map = counts(metric)
            v = window(v_map, -half, half)
            n = window(n_map, -half, half)
            e = np.concatenate([
                [expected(metric, cohort, dose, b * BIN_DAYS, (b + 1) * BIN_DAYS)] + [0.0] * (BIN_DAYS - 1)
                for b in range(-N_BINS, N_BINS)
            ])
            draw_bins(axes[0][col], v, f"Očkovaní - {label}")
            draw_bins(axes[1][col], n, f"Neočkovaní - {label}")
            for group, arr in [("ockovani", v), ("neockovani", n), ("ocekavani", e)]:
                for b, count in zip(range(-N_BINS, N_BINS), arr.reshape(2 * N_BINS, BIN_DAYS).sum(axis=1)):
                    bin_rows.append({
                        "age_cohort": cohort, "vax_dose": dose, "metric": metric,
                        "group": group, "bin_start_day": b * BIN_DAYS, "count": float(count),
                    })
        path = out_dir / "14denni_periody" / f"{cohort}_{dose}_davka.png"
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.tight_layout()
        fig.savefig(path)
        plt.close(fig)

    results = pl.DataFrame(rows).sort("age_cohort", "vax_dose", "period_days", "metric")
    results.write_csv(out_dir / "vysledky.csv")
    pl.DataFrame(bin_rows).write_csv(out_dir / "14denni_periody" / "periody.csv")
    print(f"  hotovo: {out_dir}")


def write_comparison(company: str, offsets: list[int], hve_days: int = 0) -> None:
    """virtualni_ockovani/srovnani.csv: vax_vs_novax_ratio a vax_increase
    skutečného očkování vedle všech virtuálních, jeden řádek na okno ×
    kohortu × dávku × metriku."""
    keys = ["age_cohort", "vax_dose", "metric", "period_days"]
    table = None
    for off in [0, *offsets]:
        name = "skutecne" if off == 0 else offset_label(off)
        df = pl.read_csv(out_dir_for(company, off, hve_days) / "vysledky.csv").select(
            *keys,
            pl.col("vax_vs_novax_ratio").alias(f"pomer_{name}"),
            pl.col("vax_vs_expected_ratio").alias(f"pomer_norm_{name}"),
            pl.col("vax_increase").alias(f"ocko_po_pred_{name}"),
            pl.col("p_value").alias(f"p_{name}"),
        )
        table = df if table is None else table.join(df, on=keys, how="left")
    assert table is not None
    path = out_dir_for(company, 0, hve_days) / "virtualni_ockovani" / "srovnani.csv"
    table.sort(keys).write_csv(path)
    print(f"  srovnání: {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    parser.add_argument("--company", choices=["cpzp", "ozp", "both_companies"], default="cpzp")
    parser.add_argument(
        "--year-offsets", type=int, nargs="+", default=[0],
        help="0 = skutečné očkování, N > 0 = virtuální očkování o N let dřív (např. 0 1 2 3 4)",
    )
    parser.add_argument(
        "--hve-days", type=int, nargs="+", default=[0],
        help="kolik dní před očkováním vynechat z „před“ (0 = nic, 28 = HVE varianta)",
    )
    args = parser.parse_args()
    data = load(args.company)
    virtual = [o for o in args.year_offsets if o > 0]
    for hve in args.hve_days:
        for off in args.year_offsets:
            run(args.company, data, off, hve)
        if virtual:
            write_comparison(args.company, virtual, hve)


if __name__ == "__main__":
    main()
