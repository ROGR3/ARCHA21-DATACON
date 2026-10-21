#!/usr/bin/env python3
"""
Měsíční křivky počtu předpisů B01 (srážlivost) — krok 1 z e-mailu PaJi (20.8.2026).

Pro každou pojišťovnu (cpzp, ozp, both_companies) generuje strukturu:

  charts/<company>/<období>/<pohlaví>/absolute/<dávková varianta>/
      atc_predpisy_mesicne_<scope>.png            (age-only, počty)
      crosstab/vedle_sebe/<scope>.png              (očko|neočko, počty)
      crosstab/nad_sebou/<scope>.png
  charts/<company>/<období>/<pohlaví>/absolute/specializace_predpisy_mesicne.png
  charts/<company>/<období>/<pohlaví>/relative/... (stejný strom, na 1 000 osob)
  charts/<company>/vax_period/<pohlaví>/<scope>.png
      denní počty očkování (7denní průměr), jedna čára na každou dávku
      v pořadí u daného člověka (1. až poslední, i 10.). Bez zvýraznění
      období vakcinace.
  charts/<company>/histogram_vyssi_davky/<rok>.png
      pro roky 2019–2022: osa x = kolik předpisů dané ATC skupiny měl
      jeden člověk v tom roce, osa y = kolik takových lidí. Sloupec =
      ATC skupina. Jen varianta vyšších dávek (Fraxiparin ≥0,6 ml a
      terapeutické síly NOAC, viz příloha v charts/README.md). Černý
      sloupec "B01 celkem" = předpisy všech skupin u člověka sečtené.
  charts/<company>/prvopredpisy/<období>/<scope>.png
      první předpis B01 u člověka v datech, po měsících, očkovaní nad
      neočkovanými. Preventivní heparin (B01AB mimo jen_vyssi_davky) se
      nepočítá.

<období>:
  - "2015-2024" — celý rozsah dat, roční popisky osy x
  - "2019-2024" — jen od 2019, měsíční popisky osy x (vidět jednotlivé
    měsíce 2021/2022 — kdy přesně se co zvedá)

<pohlaví>: "vsichni", "muzi", "zeny". Pohlaví je jedno na člověka (častější
hodnota z jeho řádků). V relative/ má každé pohlaví vlastní jmenovatel.

<dávková varianta>:
  - "vse"             — beze změny, všechny síly tak, jak jsou v datech
  - "jen_vyssi_davky" — jen terapeutické dávky: nadroparin (Fraxiparin,
    B01AB06) se stříkačkou ≥0,6 ml, dabigatran 150 mg, rivaroxaban 15 a
    20 mg, apixaban 5 a 10 mg, edoxaban 30 a 60 mg. Warfarin (B01AA),
    altepláza (B01AD) a B01AX bez filtru síly. Ostatní hepariny (enoxaparin, bemiparin, …)
    v téhle variantě nejsou. Objem stříkačky je z "velikost_baleni"
    (např. 10X0,6ML). U OZP je látka doplněná ze SÚKL, filtr jede podle ATC a objemu.
  - "bez_heparinu"    — B01AB úplně vynechané (jen AA/AD/AE/AF/AX + jejich
    součet, bez filtru síly NOAC)

<scope> = "vsechny_veky" nebo "vek_<bucket>" (9 věkových dekád od 0–9).

V absolute/ je N počet distinct osob s ≥1 vykresleným předpisem.
V relative/ jsou křivky vydělené velikostí populace (předpisů na 1 000 osob).
Jmenovatel je každý člověk v datech, včetně těch bez antikoagulancia a včetně
řádků bez jakéhokoli záznamu. U crosstabu má očkovaný a neočkovaný panel
každý svůj jmenovatel. U grafu bez rozpadu na očkování je jmenovatel celá
věková skupina. Do dekády člověk patří, když v některém roce období měl věk
(rok − rok narození) v té dekádě.

Věk = rok předpisu − rok narození (hrubý odhad, u OZP chybí měsíc narození).
Očkovaný/neočkovaný = osoba má/nemá kdykoliv v datech alespoň jeden
vakcinační řádek (celoobdobní ever-vaccinated, ne zarovnané k datu vakcinace).
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import polars as pl

OUT_DIR = Path("data")
CHARTS_DIR = Path("charts")

COMPANIES = ["cpzp", "ozp", "both_companies"]
COMPANY_LABELS = {
    "cpzp": "Data z ČPZP",
    "ozp": "Data z OZP",
    "both_companies": "Souhrnná data (ČPZP + OZP)",
}

NEEDED_COLS = [
    "Id_pojistence",
    "Rok_narozeni",
    "Typ_udalosti",
    "ATC_skupina",
    "lekova_skupina",
    "Specializace",
    "Datum_udalosti",
    "síla",
    "léčivé_látky",
    "velikost_baleni",
    "Pohlavi",
]

ATC_SUBGROUPS = ["B01AA", "B01AB", "B01AD", "B01AE", "B01AF", "B01AX"]
ATC_SUBGROUPS_NO_HEPARIN = [c for c in ATC_SUBGROUPS if c != "B01AB"]
ATC_COLORS = {
    "B01AA": "#1f77b4",
    "B01AB": "#ff7f0e",
    "B01AD": "#17becf",
    "B01AE": "#2ca02c",
    "B01AF": "#d62728",
    "B01AX": "#9467bd",
}
TOTAL_LABEL = "B01 celkem"

# Terapeutické síly NOAC. Klíč je sedmimístné ATC. Apixaban 10 mg v datech
# není, v pravidle je. Látka u OZP je doplněná ze SÚKL, filtr ji nepotřebuje.
NOAC_HIGHER_STRENGTHS: dict[str, set[str]] = {
    "B01AE07": {"150MG"},
    "B01AF01": {"15MG", "20MG"},
    "B01AF02": {"5MG", "10MG"},
    "B01AF03": {"30MG", "60MG"},
}
NADROPARIN_ATC = "B01AB06"
NADROPARIN_MIN_ML = 0.6
# Relativní grafy: počet předpisů na tisíc osob dané populace.
RELATIVE_PER = 1_000

# (zobrazovaný label, bezpečný slug pro název souboru)
AGE_BUCKETS = [
    ("0-9", "0-9"),
    ("10-19", "10-19"),
    ("20-29", "20-29"),
    ("30-39", "30-39"),
    ("40-49", "40-49"),
    ("50-59", "50-59"),
    ("60-69", "60-69"),
    ("70-79", "70-79"),
    ("80+", "80_plus"),
]
AGE_BREAKS = [10, 20, 30, 40, 50, 60, 70, 80]
AGE_LABELS = [label for label, _ in AGE_BUCKETS]
# (spodní věk včetně, horní věk vyjma). 80+ má horní mez mimo reálný věk.
AGE_BOUNDS = {label: (lo, lo + 10) for lo, (label, _) in zip(AGE_BREAKS, AGE_BUCKETS[1:])}
AGE_BOUNDS["0-9"] = (0, 10)
AGE_BOUNDS["80+"] = (80, 10_000)

TOP_N_SPECIALTIES = 10
UNKNOWN_SPECIALTY = "neznámo"
OTHER_SPECIALTY = "ostatní"

# (slug složky, popisek do titulku, kód v datech; None = bez filtru)
SEXES: list[tuple[str, str, str | None]] = [
    ("vsichni", "", None),
    ("muzi", "muži, ", "M"),
    ("zeny", "ženy, ", "F"),
]

MONTH_START = date(2015, 1, 1)
MONTH_END = date(2023, 12, 1)
RECENT_START = date(2019, 1, 1)


def _read_one(path: Path, id_prefix: str | None, cols: list[str]) -> pl.LazyFrame:
    df = (
        pl.scan_csv(path, null_values=["", "NA"], infer_schema_length=20_000)
        .select(cols)
        .with_columns(
            pl.col("Id_pojistence").cast(pl.Utf8),
            pl.col("Rok_narozeni").cast(pl.Int64, strict=False),
        )
    )
    if id_prefix:
        df = df.with_columns(
            (pl.lit(id_prefix) + pl.col("Id_pojistence")).alias("Id_pojistence")
        )
    return df.with_columns(
        pl.col("Datum_udalosti").str.to_date("%Y-%m-%d", strict=False)
    )


def load_company(company: str, extra_cols: list[str] | None = None) -> pl.LazyFrame:
    cols = NEEDED_COLS + (extra_cols or [])
    if company == "both_companies":
        cpzp = _read_one(OUT_DIR / "CPZP_preskladane.csv", "cpzp:", cols)
        ozp = _read_one(OUT_DIR / "OZP_preskladane.csv", "ozp:", cols)
        return pl.concat([cpzp, ozp])
    fname = "CPZP_preskladane.csv" if company == "cpzp" else "OZP_preskladane.csv"
    return _read_one(OUT_DIR / fname, None, cols)


def _sex_agg() -> pl.Expr:
    """Jedno pohlaví na člověka. Pár lidí v ČPZP má v různých řádcích M i F,
    bere se častější hodnota (při shodě F)."""
    return pl.col("Pohlavi").drop_nulls().mode().sort().first().alias("Pohlavi")


def _sex_lookup(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.group_by("Id_pojistence").agg(_sex_agg())


def _and(a: pl.Expr | None, b: pl.Expr | None) -> pl.Expr | None:
    if a is None:
        return b
    if b is None:
        return a
    return a & b


def _higher_dose_expr() -> pl.Expr:
    """True = řádek zůstane ve variantě jen_vyssi_davky.

    Warfarin, altepláza (B01AD) a B01AX nemají filtr síly. Nadroparin jen
    při objemu stříkačky ≥0,6 ml. NOAC jen síly z NOAC_HIGHER_STRENGTHS.
    Všechno ostatní (jiné hepariny, nízké NOAC, B01AC) je False."""
    sila = pl.col("síla").str.to_uppercase().str.replace_all(r"\s+", "").str.replace_all(",", ".")
    syringe_ml = (
        pl.col("velikost_baleni")
        .str.replace_all(",", ".")
        .str.extract(r"(?i)[xX×]\s*(\d+(?:\.\d+)?)\s*ML", 1)
        .cast(pl.Float64, strict=False)
    )
    atc7 = pl.col("ATC_skupina").str.to_uppercase().str.strip_chars().str.slice(0, 7)
    noac = pl.lit(False)
    for atc, strengths in NOAC_HIGHER_STRENGTHS.items():
        noac = noac | ((atc7 == atc) & sila.is_in(sorted(strengths)))
    return (
        pl.when(atc7.str.slice(0, 5).is_in(["B01AA", "B01AD", "B01AX"]))
        .then(True)
        .when((atc7 == NADROPARIN_ATC) & (syringe_ml >= NADROPARIN_MIN_ML))
        .then(True)
        .when(noac)
        .then(True)
        .otherwise(False)
        .fill_null(False)
    )


def build_prescriptions(df: pl.LazyFrame) -> pl.DataFrame:
    """Vrátí (malý, materializovaný) eager frame: jeden řádek na B01 předpis,
    doplněný o month/atc5/age_bucket/is_vax/Specializace/is_higher_dose
    (bez NULL) a Pohlavi. Řádky NEJSOU filtrované podle dávky — to dělá až
    `apply_dose_variant()` podle zvolené varianty (vse/jen_vyssi_davky/
    bez_heparinu)."""
    vax_ids = (
        df.filter(pl.col("Typ_udalosti") == "vakcinace")
        .select("Id_pojistence")
        .unique()
        .with_columns(pl.lit(True).alias("is_vax"))
    )
    presc = (
        df.filter(
            (pl.col("Typ_udalosti") == "předpis")
            & (pl.col("lekova_skupina") == "srazlivost")
        )
        .with_columns(
            pl.col("Datum_udalosti").dt.truncate("1mo").alias("month"),
            pl.col("ATC_skupina").str.slice(0, 5).alias("atc5"),
            (pl.col("Datum_udalosti").dt.year() - pl.col("Rok_narozeni")).alias("age"),
            pl.col("Specializace").fill_null(UNKNOWN_SPECIALTY),
        )
        .drop("Pohlavi")
        .join(vax_ids, on="Id_pojistence", how="left")
        .join(_sex_lookup(df), on="Id_pojistence", how="left")
        .with_columns(pl.col("is_vax").fill_null(False))
        .with_columns(
            pl.col("age")
            .cut(AGE_BREAKS, labels=AGE_LABELS, left_closed=True)
            .cast(pl.Utf8)
            .alias("age_bucket")
        )
        .with_columns(_higher_dose_expr().alias("is_higher_dose"))
        .select(
            "Id_pojistence", "Datum_udalosti", "month", "atc5", "age_bucket", "is_vax",
            "Specializace", "is_higher_dose", "Pohlavi",
        )
    )
    return presc.collect()


DOSE_VARIANTS: list[tuple[str, str, str]] = [
    ("vse", "vše", ""),
    ("jen_vyssi_davky", "jen vyšší dávky", " — jen vyšší dávky"),
    ("bez_heparinu", "bez heparinu", " — bez B01AB"),
]


def apply_dose_variant(presc: pl.DataFrame, variant: str) -> tuple[pl.DataFrame, list[str]]:
    """Vrátí (přefiltrovaný presc, seznam ATC podskupin k vykreslení) pro
    danou dávkovou variantu — viz docstring modulu."""
    if variant == "vse":
        return presc, ATC_SUBGROUPS
    if variant == "jen_vyssi_davky":
        return presc.filter(pl.col("is_higher_dose")), ATC_SUBGROUPS
    if variant == "bez_heparinu":
        return presc, ATC_SUBGROUPS_NO_HEPARIN
    raise ValueError(f"neznámá dávková varianta: {variant}")


def full_month_index(start: date = MONTH_START, end: date = MONTH_END) -> pl.DataFrame:
    return pl.DataFrame({"month": pl.date_range(start, end, interval="1mo", eager=True)})


def monthly_atc_counts(
    presc: pl.DataFrame,
    scope_filter: pl.Expr | None,
    subgroups: list[str] = ATC_SUBGROUPS,
    start: date = MONTH_START,
    end: date = MONTH_END,
) -> pl.DataFrame:
    """Wide tabulka: month, <subgroups>, B01 celkem (počty), pro dané scope.

    "B01 celkem" je součet jen zobrazených `subgroups` — cokoliv mimo ně
    (B01AC, případně B01AB při volání s ATC_SUBGROUPS_NO_HEPARIN) se
    nevykreslí ani nezapočítá do celkem."""
    scoped = presc.filter(pl.col("atc5").is_in(subgroups))
    if scope_filter is not None:
        scoped = scoped.filter(scope_filter)

    present_subgroups = [c for c in subgroups if scoped.filter(pl.col("atc5") == c).height > 0]
    sub = (
        scoped.group_by("month", "atc5")
        .len()
        .pivot(on="atc5", index="month", values="len")
        if present_subgroups
        else pl.DataFrame({"month": []}, schema={"month": pl.Date})
    )

    wide = full_month_index(start, end).join(sub, on="month", how="left")
    value_cols = [c for c in subgroups if c in wide.columns]
    wide = wide.with_columns([pl.col(c).fill_null(0) for c in value_cols])
    wide = wide.with_columns(pl.sum_horizontal(value_cols).alias(TOTAL_LABEL))
    return wide.sort("month")


def population_size(
    presc: pl.DataFrame,
    scope_filter: pl.Expr | None,
    subgroups: list[str] = ATC_SUBGROUPS,
) -> int:
    """Počet distinct osob s ≥1 předpisem z `subgroups` v daném scope
    (napříč celým obdobím) — "N" v titulcích absolute grafů."""
    scoped = presc.filter(pl.col("atc5").is_in(subgroups))
    if scope_filter is not None:
        scoped = scoped.filter(scope_filter)
    return scoped.select(pl.col("Id_pojistence").n_unique()).item()


def build_people(df: pl.LazyFrame) -> pl.DataFrame:
    """Jedna řádka na člověka: rok narození, pohlaví a jestli má kdykoli vakcinaci.

    Zahrnuje i lidi bez jakéhokoli záznamu (jen identifikační řádek)."""
    return (
        df.group_by("Id_pojistence")
        .agg(
            pl.col("Rok_narozeni").drop_nulls().first().alias("Rok_narozeni"),
            (pl.col("Typ_udalosti") == "vakcinace").any().alias("is_vax"),
            _sex_agg(),
        )
        .collect()
    )


def age_label_of(scope_slug: str) -> str | None:
    if scope_slug == "vsechny_veky":
        return None
    for label, file_slug in AGE_BUCKETS:
        if scope_slug == f"vek_{file_slug}":
            return label
    raise ValueError(f"neznámý scope: {scope_slug}")


def cohort_size(
    people: pl.DataFrame,
    start_year: int,
    end_year: int,
    age_label: str | None,
    vax: bool | None,
) -> int:
    """Velikost jmenovatele. vax=True/False řeže ever-vaccinated, None nechá obě.

    Věková dekáda: člověk do ní patří, když se jeho věk (rok − rok narození)
    v některém roce <start_year, end_year> s dekádou překryje."""
    scoped = people
    if vax is True:
        scoped = scoped.filter(pl.col("is_vax"))
    elif vax is False:
        scoped = scoped.filter(~pl.col("is_vax"))
    if age_label is not None:
        lo, hi = AGE_BOUNDS[age_label]
        scoped = scoped.filter(
            pl.col("Rok_narozeni").is_not_null()
            & (pl.col("Rok_narozeni") > start_year - hi)
            & (pl.col("Rok_narozeni") <= end_year - lo)
        )
    return scoped.height


def per_capita(wide: pl.DataFrame, n: int, per: int = RELATIVE_PER) -> pl.DataFrame:
    """Vydělí všechny sloupečky kromě month velikostí populace a vynásobí `per`."""
    if n <= 0:
        return wide
    factor = per / n
    cols = [c for c in wide.columns if c != "month"]
    return wide.with_columns([(pl.col(c) * factor).alias(c) for c in cols])


def specialty_chart_data(
    presc: pl.DataFrame, start: date = MONTH_START, end: date = MONTH_END
) -> tuple[pl.DataFrame, list[str]]:
    top_codes = (
        presc.group_by("Specializace")
        .len()
        .sort("len", descending=True)
        .head(TOP_N_SPECIALTIES)["Specializace"]
        .to_list()
    )
    bucketed = presc.with_columns(
        pl.when(pl.col("Specializace").is_in(top_codes))
        .then(pl.col("Specializace"))
        .otherwise(pl.lit(OTHER_SPECIALTY))
        .alias("spec_bucket")
    )
    counts = bucketed.group_by("month", "spec_bucket").len().pivot(
        on="spec_bucket", index="month", values="len"
    )
    wide = full_month_index(start, end).join(counts, on="month", how="left")
    value_cols = [c for c in wide.columns if c != "month"]
    wide = wide.with_columns([pl.col(c).fill_null(0) for c in value_cols])
    return wide.sort("month"), top_codes


def _fmt_n(n: int) -> str:
    return f"{n:,}".replace(",", " ")


def plot_lines(
    wide: pl.DataFrame,
    series: list[tuple[str, dict]],
    title: str,
    output_path: Path,
    n_population: int | None = None,
    monthly_ticks: bool = False,
    ylabel: str = "Počet předpisů",
    n_caption: str = "N",
) -> None:
    if n_population is not None:
        title = f"{title}\n({n_caption} = {_fmt_n(n_population)} osob)"
    months = wide["month"].to_list()
    fig, ax = plt.subplots(figsize=(12, 5))
    for col, style in series:
        if col not in wide.columns:
            continue
        ax.plot(months, wide[col].to_list(), label=col, **style)
    ax.set_title(title)
    ax.set_xlabel("Měsíc")
    ax.set_ylabel(ylabel)
    ax.legend(loc="upper left", fontsize=8, ncol=2)
    ax.grid(True, alpha=0.3)
    if monthly_ticks:
        _apply_monthly_ticks(ax)
    else:
        fig.autofmt_xdate()
    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  → uloženo: {output_path}")


def _apply_monthly_ticks(ax: plt.Axes) -> None:
    """Popisek pro každý druhý měsíc (YYYY-MM), svisle natočený — pro
    grafy s krátkým časovým rozsahem, kde chceme vidět jednotlivé měsíce."""
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    for label in ax.get_xticklabels():
        label.set_rotation(90)
        label.set_ha("center")


def plot_crosstab(
    wide_left: pl.DataFrame,
    wide_right: pl.DataFrame,
    series: list[tuple[str, dict]],
    suptitle: str,
    left_title: str,
    right_title: str,
    output_path: Path,
    n_left: int | None = None,
    n_right: int | None = None,
    monthly_ticks: bool = False,
    ylabel: str = "Počet předpisů",
    n_caption: str = "N",
) -> None:
    """Dva panely vedle sebe se sdílenou osou y — pro přímé srovnání tvaru
    křivek (např. AA vs. AF) mezi dvěma scope (typicky očko/neočko)."""
    if n_left is not None:
        left_title = f"{left_title} ({n_caption} = {_fmt_n(n_left)})"
    if n_right is not None:
        right_title = f"{right_title} ({n_caption} = {_fmt_n(n_right)})"
    fig, axes = plt.subplots(1, 2, figsize=(16, 5), sharey=True)
    for ax, wide, subtitle in ((axes[0], wide_left, left_title), (axes[1], wide_right, right_title)):
        months = wide["month"].to_list()
        for col, style in series:
            if col not in wide.columns:
                continue
            ax.plot(months, wide[col].to_list(), label=col, **style)
        ax.set_title(subtitle)
        ax.set_xlabel("Měsíc")
        ax.grid(True, alpha=0.3)
        if monthly_ticks:
            _apply_monthly_ticks(ax)
    axes[0].set_ylabel(ylabel)
    axes[0].legend(loc="upper left", fontsize=8, ncol=2)
    fig.suptitle(suptitle)
    if not monthly_ticks:
        fig.autofmt_xdate()
    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  → uloženo: {output_path}")


def plot_crosstab_stacked(
    wide_top: pl.DataFrame,
    wide_bottom: pl.DataFrame,
    series: list[tuple[str, dict]],
    suptitle: str,
    top_title: str,
    bottom_title: str,
    output_path: Path,
    n_top: int | None = None,
    n_bottom: int | None = None,
    monthly_ticks: bool = False,
    ylabel: str = "Počet předpisů",
    n_caption: str = "N",
) -> None:
    """Dva panely nad sebou se sdílenou osou x — na rozdíl od plot_crosstab
    je tu lépe vidět zarovnání v čase (stejný měsíc = stejné x u obou
    panelů), na úkor přímého srovnání výšky křivek."""
    if n_top is not None:
        top_title = f"{top_title} ({n_caption} = {_fmt_n(n_top)})"
    if n_bottom is not None:
        bottom_title = f"{bottom_title} ({n_caption} = {_fmt_n(n_bottom)})"
    fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True, sharey=True)
    for ax, wide, subtitle in ((axes[0], wide_top, top_title), (axes[1], wide_bottom, bottom_title)):
        months = wide["month"].to_list()
        for col, style in series:
            if col not in wide.columns:
                continue
            ax.plot(months, wide[col].to_list(), label=col, **style)
        ax.set_title(subtitle)
        ax.set_ylabel(ylabel)
        ax.grid(True, alpha=0.3)
    axes[1].set_xlabel("Měsíc")
    axes[0].legend(loc="upper left", fontsize=8, ncol=2)
    fig.suptitle(suptitle)
    if monthly_ticks:
        _apply_monthly_ticks(axes[1])
    else:
        fig.autofmt_xdate()
    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  → uloženo: {output_path}")


def atc_series(subgroups: list[str] = ATC_SUBGROUPS) -> list[tuple[str, dict]]:
    series = [
        (c, {"color": ATC_COLORS[c], "linewidth": 1.2, "marker": "o", "markersize": 2})
        for c in subgroups
    ]
    series.append(
        (TOTAL_LABEL, {"color": "black", "linewidth": 2, "linestyle": "--"})
    )
    return series


def specialty_series(top_codes: list[str]) -> list[tuple[str, dict]]:
    cmap = plt.get_cmap("tab20")
    codes = top_codes + [OTHER_SPECIALTY]
    return [
        (
            code,
            {
                "color": "gray" if code == OTHER_SPECIALTY else cmap(i / max(len(codes) - 1, 1)),
                "linewidth": 1.6 if code == OTHER_SPECIALTY else 1.2,
                "linestyle": "--" if code == OTHER_SPECIALTY else "-",
                "marker": "o",
                "markersize": 2,
            },
        )
        for i, code in enumerate(codes)
    ]


def build_vaccinations(df: pl.LazyFrame) -> pl.DataFrame:
    """Jeden řádek na vakcinaci. `dose` = pořadí záznamu u člověka podle data
    (1 = první očkování, 10 = desáté), ne efektivní dávka (Janssen ≠ 2)."""
    return (
        df.filter(pl.col("Typ_udalosti") == "vakcinace")
        .select("Id_pojistence", "Rok_narozeni", "Datum_udalosti")
        .drop_nulls("Datum_udalosti")
        .join(_sex_lookup(df), on="Id_pojistence", how="left")
        .with_columns(
            (pl.col("Datum_udalosti").dt.year() - pl.col("Rok_narozeni")).alias("age")
        )
        .sort("Id_pojistence", "Datum_udalosti")
        .with_columns(
            pl.col("Datum_udalosti")
            .rank("ordinal")
            .over("Id_pojistence")
            .cast(pl.Int32)
            .alias("dose")
        )
        .with_columns(
            pl.col("age")
            .cut(AGE_BREAKS, labels=AGE_LABELS, left_closed=True)
            .cast(pl.Utf8)
            .alias("age_bucket")
        )
        .select("Id_pojistence", "Datum_udalosti", "dose", "age_bucket", "Pohlavi")
        .collect()
    )


def vaccination_ma(
    vax: pl.DataFrame, scope: pl.Expr | None
) -> tuple[pl.DataFrame, list[int], int]:
    """Denní počty očkování v scope, doplněné nulami na celé období a
    vyhlazené 7denním klouzavým průměrem. Osa x je společná pro celou
    pojišťovnu, ať jdou dekády srovnat."""
    scoped = vax if scope is None else vax.filter(scope)
    n = scoped.select(pl.col("Id_pojistence").n_unique()).item() if scoped.height else 0
    if scoped.height == 0:
        return pl.DataFrame(), [], n

    start, end = vax["Datum_udalosti"].min(), vax["Datum_udalosti"].max()
    counts = scoped.group_by("Datum_udalosti", "dose").len().pivot(
        on="dose", index="Datum_udalosti", values="len"
    )
    days = pl.DataFrame(
        {"Datum_udalosti": pl.date_range(start, end, interval="1d", eager=True)}
    )
    wide = days.join(counts, on="Datum_udalosti", how="left")
    dose_cols = sorted(int(c) for c in wide.columns if c != "Datum_udalosti")
    wide = wide.with_columns(
        [
            pl.col(str(c))
            .fill_null(0)
            .rolling_mean(window_size=7, min_samples=1, center=True)
            for c in dose_cols
        ]
    )
    present = [c for c in dose_cols if wide[str(c)].sum() > 0]
    return wide.sort("Datum_udalosti"), present, n


def plot_vax_doses(
    wide: pl.DataFrame,
    doses: list[int],
    title: str,
    output_path: Path,
    n_population: int,
) -> None:
    dates = wide["Datum_udalosti"].to_list()
    cmap = plt.get_cmap("tab10")
    fig, ax = plt.subplots(figsize=(12, 5))
    for i, dose in enumerate(doses):
        ax.plot(
            dates,
            wide[str(dose)].to_list(),
            label=f"{dose}. dávka",
            color=cmap(i % 10),
            linewidth=1.4,
        )
    ax.set_title(f"{title}\n(N = {_fmt_n(n_population)} osob, 7denní průměr)")
    ax.set_xlabel("Datum")
    ax.set_ylabel("Počet očkování")
    ax.legend(loc="upper right", fontsize=8, ncol=2)
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()
    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  → uloženo: {output_path}")


def process_vax_timelines(company: str) -> None:
    label = COMPANY_LABELS[company]
    print(f"=== {company}: očkování po dávkách ===")
    vax = build_vaccinations(load_company(company))
    print(f"  {vax.height:,} vakcinací, max dávka {vax['dose'].max()}")
    for sex_slug, sex_desc, sex_code in SEXES:
        out_dir = CHARTS_DIR / company / "vax_period" / sex_slug
        sex_scope = None if sex_code is None else pl.col("Pohlavi") == sex_code
        for slug, desc, age_scope in AGE_SCOPES:
            wide, doses, n = vaccination_ma(vax, _and(sex_scope, age_scope))
            if not doses:
                print(f"  {sex_slug}/{slug}: žádná očkování, přeskakuji")
                continue
            plot_vax_doses(
                wide,
                doses,
                f"{label} — očkování ({sex_desc}{desc})",
                out_dir / f"{slug}.png",
                n_population=n,
            )


# (slug složky, popisek, počátek rozsahu, měsíční popisky osy x)
PERIODS: list[tuple[str, str, date, bool]] = [
    ("2015-2024", "2015–2023", MONTH_START, False),
    ("2019-2024", "2019–2023", RECENT_START, True),
]

AGE_SCOPES: list[tuple[str, str, pl.Expr | None]] = [("vsechny_veky", "všechny věky", None)] + [
    (f"vek_{slug}", f"věk {age_label}", pl.col("age_bucket") == age_label)
    for age_label, slug in AGE_BUCKETS
]


def process_company(
    company: str,
    variants: list[str] | None = None,
    extras: bool = True,
) -> None:
    label = COMPANY_LABELS[company]
    print(f"=== {company} ===")
    print("  načítám data…")
    lazy = load_company(company)
    people = build_people(lazy)
    presc_raw = build_prescriptions(lazy)
    print(f"  {presc_raw.height:,} B01 předpisů, {people.height:,} osob")
    out_dir = CHARTS_DIR / company
    # (složka, osa y, N v titulku, None = počty / číslo = na tolik osob)
    scales: list[tuple[str, str, str, int | None]] = [
        ("absolute", "Počet předpisů", "N", None),
        ("relative", f"Předpisů na {_fmt_n(RELATIVE_PER)} osob", "populace", RELATIVE_PER),
    ]

    selected = DOSE_VARIANTS
    if variants is not None:
        selected = [v for v in DOSE_VARIANTS if v[0] in variants]
    end_year = MONTH_END.year

    for period_slug, period_label, start, monthly_ticks in PERIODS:
        print(f"  === období {period_slug} ===")
        period_dir = out_dir / period_slug

        for sex_slug, sex_desc, sex_code in SEXES:
            sex_dir = period_dir / sex_slug
            if sex_code is None:
                presc_sex, people_sex = presc_raw, people
            else:
                presc_sex = presc_raw.filter(pl.col("Pohlavi") == sex_code)
                people_sex = people.filter(pl.col("Pohlavi") == sex_code)
            for scale_slug, ylabel, n_caption, per in scales:
                print(f"  --- {sex_slug}/{scale_slug} ---")
                scale_dir = sex_dir / scale_slug
                for variant_slug, variant_label, subtitle_extra in selected:
                    presc, subgroups = apply_dose_variant(presc_sex, variant_slug)
                    variant_dir = scale_dir / variant_slug
                    series = atc_series(subgroups)
                    title_suffix = subtitle_extra
                    if per is not None:
                        title_suffix += f" — na {_fmt_n(per)} osob"

                    print(f"    {variant_slug}: age sada…")
                    for slug, desc, scope in AGE_SCOPES:
                        wide = monthly_atc_counts(presc, scope, subgroups=subgroups, start=start)
                        if per is None:
                            n = population_size(presc, scope, subgroups=subgroups)
                        else:
                            n = cohort_size(people_sex, start.year, end_year, age_label_of(slug), None)
                            wide = per_capita(wide, n, per)
                        plot_lines(
                            wide,
                            series,
                            f"{label} — B01 předpisy měsíčně ({sex_desc}{desc}, {period_label}){title_suffix}",
                            variant_dir / f"atc_predpisy_mesicne_{slug}.png",
                            n_population=n,
                            monthly_ticks=monthly_ticks,
                            ylabel=ylabel,
                            n_caption=n_caption,
                        )

                    print(f"    {variant_slug}: crosstab (vedle_sebe + nad_sebou)…")
                    crosstab_dir = variant_dir / "crosstab"
                    for slug, desc, age_scope in AGE_SCOPES:
                        vax_scope = pl.col("is_vax") if age_scope is None else (age_scope & pl.col("is_vax"))
                        novax_scope = (
                            ~pl.col("is_vax") if age_scope is None else (age_scope & ~pl.col("is_vax"))
                        )
                        wide_vax = monthly_atc_counts(presc, vax_scope, subgroups=subgroups, start=start)
                        wide_novax = monthly_atc_counts(
                            presc, novax_scope, subgroups=subgroups, start=start
                        )
                        age_label = age_label_of(slug)
                        if per is None:
                            n_vax = population_size(presc, vax_scope, subgroups=subgroups)
                            n_novax = population_size(presc, novax_scope, subgroups=subgroups)
                        else:
                            n_vax = cohort_size(people_sex, start.year, end_year, age_label, True)
                            n_novax = cohort_size(people_sex, start.year, end_year, age_label, False)
                            wide_vax = per_capita(wide_vax, n_vax, per)
                            wide_novax = per_capita(wide_novax, n_novax, per)
                        suptitle = (
                            f"{label} — B01 předpisy měsíčně: očkovaní vs. neočkovaní "
                            f"({sex_desc}{desc}, {period_label}){title_suffix}"
                        )
                        plot_crosstab(
                            wide_vax,
                            wide_novax,
                            series,
                            suptitle,
                            "Očkovaní",
                            "Neočkovaní",
                            crosstab_dir / "vedle_sebe" / f"{slug}.png",
                            n_left=n_vax,
                            n_right=n_novax,
                            monthly_ticks=monthly_ticks,
                            ylabel=ylabel,
                            n_caption=n_caption,
                        )
                        plot_crosstab_stacked(
                            wide_vax,
                            wide_novax,
                            series,
                            suptitle,
                            "Očkovaní",
                            "Neočkovaní",
                            crosstab_dir / "nad_sebou" / f"{slug}.png",
                            n_top=n_vax,
                            n_bottom=n_novax,
                            monthly_ticks=monthly_ticks,
                            ylabel=ylabel,
                            n_caption=n_caption,
                        )

                if variants is None:
                    print(f"    specializace ({period_slug}, {sex_slug}, {scale_slug})…")
                    wide_spec, top_codes = specialty_chart_data(presc_sex, start=start, end=MONTH_END)
                    if per is None:
                        n_spec = population_size(presc_sex, None)
                        spec_suffix = ""
                    else:
                        n_spec = cohort_size(people_sex, start.year, end_year, None, None)
                        wide_spec = per_capita(wide_spec, n_spec, per)
                        spec_suffix = f" — na {_fmt_n(per)} osob"
                    plot_lines(
                        wide_spec,
                        specialty_series(top_codes),
                        f"{label} — B01 předpisy měsíčně dle odbornosti lékaře (top {TOP_N_SPECIALTIES}, {sex_desc}{period_label}){spec_suffix}",
                        scale_dir / "specializace_predpisy_mesicne.png",
                        n_population=n_spec,
                        monthly_ticks=monthly_ticks,
                        ylabel=ylabel,
                        n_caption=n_caption,
                    )

    if extras:
        process_vax_timelines(company)
        process_histograms(company, presc_raw)
        process_first_prescriptions(company, presc_raw)


def first_prescriptions(presc: pl.DataFrame) -> pl.DataFrame:
    """Jeden řádek na člověka: jeho první předpis z ATC_SUBGROUPS v datech.

    Preventivní heparin se nepočítá (B01AB mimo jen_vyssi_davky, např.
    Fraxiparin pod 0,6 ml). Kdo má jen ten, prvopředpis nemá. Věk a ATC
    jsou z toho prvního předpisu."""
    return (
        presc.filter(
            pl.col("atc5").is_in(ATC_SUBGROUPS)
            & ~((pl.col("atc5") == "B01AB") & ~pl.col("is_higher_dose"))
        )
        .sort("Datum_udalosti", "atc5")
        .unique("Id_pojistence", keep="first", maintain_order=True)
    )


def process_first_prescriptions(company: str, presc_raw: pl.DataFrame | None = None) -> None:
    label = COMPANY_LABELS[company]
    print(f"=== {company}: prvopředpisy ===")
    if presc_raw is None:
        presc_raw = build_prescriptions(load_company(company))
    first = first_prescriptions(presc_raw)
    print(f"  {first.height:,} osob s prvopředpisem")
    series = atc_series(ATC_SUBGROUPS)
    for period_slug, period_label, start, monthly_ticks in PERIODS:
        out_dir = CHARTS_DIR / company / "prvopredpisy" / period_slug
        for slug, desc, age_scope in AGE_SCOPES:
            vax_scope = _and(age_scope, pl.col("is_vax"))
            novax_scope = _and(age_scope, ~pl.col("is_vax"))
            plot_crosstab_stacked(
                monthly_atc_counts(first, vax_scope, start=start),
                monthly_atc_counts(first, novax_scope, start=start),
                series,
                f"{label} — první B01 předpis měsíčně: očkovaní vs. neočkovaní "
                f"({desc}, {period_label}) — bez preventivního heparinu",
                "Očkovaní",
                "Neočkovaní",
                out_dir / f"{slug}.png",
                n_top=population_size(first, vax_scope),
                n_bottom=population_size(first, novax_scope),
                monthly_ticks=monthly_ticks,
                ylabel="Počet prvopředpisů",
            )


HIST_YEARS = (2019, 2020, 2021, 2022)
# 95. percentil je kolem 6 předpisů za rok; nad 12 už jsou jen desítky lidí,
# proto poslední sloupec sčítá 13 a víc, ať osa x zůstane čitelná.
HIST_BIN_CAP = 12


def prescription_count_bins(
    presc: pl.DataFrame, year: int, subgroups: list[str]
) -> dict[str, list[int]]:
    """Pro každou ATC skupinu a pro TOTAL_LABEL počty lidí v koších 1, 2, …, CAP, CAP+.

    Člověk s nulou předpisů v dané skupině v tom roce tu není. Kdo má
    předpisy ve dvou skupinách, je v každé skupině zvlášť. V TOTAL_LABEL
    se jeho předpisy ze všech `subgroups` sečtou."""
    n_bins = HIST_BIN_CAP + 1
    year_presc = presc.filter(
        (pl.col("month").dt.year() == year) & pl.col("atc5").is_in(subgroups)
    )
    per_group = year_presc.group_by("Id_pojistence", "atc5").len()
    per_person = (
        year_presc.group_by("Id_pojistence").len().with_columns(pl.lit(TOTAL_LABEL).alias("atc5"))
    )
    counts = (
        pl.concat([per_group, per_person.select(per_group.columns)])
        .with_columns(
            pl.when(pl.col("len") > HIST_BIN_CAP)
            .then(pl.lit(HIST_BIN_CAP + 1))
            .otherwise(pl.col("len"))
            .alias("bin")
        )
        .group_by("atc5", "bin")
        .len()
        .rename({"len": "people"})
    )
    out = {atc: [0] * n_bins for atc in subgroups + [TOTAL_LABEL]}
    for atc, bin_id, people in zip(
        counts["atc5"].to_list(), counts["bin"].to_list(), counts["people"].to_list()
    ):
        out[atc][bin_id - 1] = people
    return out


def plot_prescription_count_hist(
    bins: dict[str, list[int]],
    subgroups: list[str],
    title: str,
    output_path: Path,
) -> None:
    labels = [str(i) for i in range(1, HIST_BIN_CAP + 1)] + [f"{HIST_BIN_CAP + 1}+"]
    n_bins = len(labels)
    bars = subgroups + [TOTAL_LABEL]
    width = 0.8 / len(bars)
    fig, ax = plt.subplots(figsize=(12, 5))
    for i, atc in enumerate(bars):
        offset = (i - (len(bars) - 1) / 2) * width
        ax.bar(
            [x + offset for x in range(n_bins)],
            bins[atc],
            width=width,
            label=atc,
            color=ATC_COLORS.get(atc, "black"),
        )
    ax.set_xticks(range(n_bins), labels)
    ax.set_title(title)
    ax.set_xlabel("Počet předpisů u jednoho člověka v daném roce")
    ax.set_ylabel("Počet lidí")
    ax.legend(loc="upper right", fontsize=8)
    ax.grid(True, axis="y", alpha=0.3)
    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  → uloženo: {output_path}")


def process_histograms(company: str, presc_raw: pl.DataFrame | None = None) -> None:
    label = COMPANY_LABELS[company]
    print(f"=== {company}: histogram počtu předpisů ===")
    if presc_raw is None:
        presc_raw = build_prescriptions(load_company(company))
    presc, subgroups = apply_dose_variant(presc_raw, "jen_vyssi_davky")
    out_dir = CHARTS_DIR / company / "histogram_vyssi_davky"
    for year in HIST_YEARS:
        bins = prescription_count_bins(presc, year, subgroups)
        plot_prescription_count_hist(
            bins,
            subgroups,
            f"{label} — kolik předpisů měl jeden člověk v roce {year}\n(jen vyšší dávky)",
            out_dir / f"{year}.png",
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--companies",
        nargs="+",
        choices=COMPANIES,
        default=COMPANIES,
        help="Které pojišťovny zpracovat (default: všechny)",
    )
    parser.add_argument(
        "--only-vax",
        action="store_true",
        help="Jen grafy očkování po dávkách (charts/<company>/vax_period/)",
    )
    parser.add_argument(
        "--only-histogram",
        action="store_true",
        help="Jen histogramy počtu předpisů na osobu (charts/<company>/histogram_vyssi_davky/)",
    )
    parser.add_argument(
        "--only-first",
        action="store_true",
        help="Jen prvopředpisy (charts/<company>/prvopredpisy/)",
    )
    parser.add_argument(
        "--variants",
        nargs="+",
        choices=[slug for slug, _, _ in DOSE_VARIANTS],
        default=None,
        help="Které dávkové složky překreslit (default: všechny)",
    )
    parser.add_argument(
        "--no-extras",
        action="store_true",
        help="Nepřekreslovat vax_period, histogramy ani prvopředpisy",
    )
    args = parser.parse_args()
    for company in args.companies:
        if args.only_histogram:
            process_histograms(company)
        elif args.only_first:
            process_first_prescriptions(company)
        elif args.only_vax:
            process_vax_timelines(company)
        else:
            process_company(company, variants=args.variants, extras=not args.no_extras)
    print("\nHotovo!")


if __name__ == "__main__":
    main()
