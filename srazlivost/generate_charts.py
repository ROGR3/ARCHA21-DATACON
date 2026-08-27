#!/usr/bin/env python3
"""
Měsíční křivky počtu předpisů B01 (srážlivost) — krok 1 z e-mailu PaJi (20.8.2026).

Pro každou pojišťovnu (cpzp, ozp, both_companies) generuje strukturu:

  charts/<company>/<období>/<dávková varianta>/
      atc_predpisy_mesicne_<scope>.png            (age-only, bez vax rozpadu)
      crosstab/vedle_sebe/<scope>.png              (očko|neočko, 2 panely vedle sebe)
      crosstab/nad_sebou/<scope>.png               (očko nad neočko, sdílená osa x)
  charts/<company>/<období>/specializace_predpisy_mesicne.png

<období>:
  - "2015-2024" — celý rozsah dat, roční popisky osy x
  - "2019-2024" — jen od 2019, měsíční popisky osy x (vidět jednotlivé
    měsíce 2021/2022 — kdy přesně se co zvedá)

<dávková varianta> (B01AB = hepariny a příbuzné látky):
  - "vse"             — beze změny, všechny síly/látky B01AB tak, jak jsou v datech
  - "jen_vyssi_davky" — B01AB filtrované jen na terapeutické dávky (viz
    HEPARIN_STRENGTH_IS_THERAPEUTIC níže) — vyřazuje profylaktické dávky
    (Heparin 5000 IU, Bemiparin 2500/3500 IU apod.) a látky, co se k léčbě
    trombózy nepoužívají (Sulodexid, Antitrombin III). Klasifikace je
    heuristika podle textového pole "síla"; u nadroparinu 9500 IU/ml (bez
    Forte) je konzervativně vyřazena jako neprůkazná (ze "síly" nepoznáme
    podaný objem).
  - "bez_heparinu"    — B01AB úplně vynechané (jen AA/AE/AF/AX + jejich
    součet) — když i "jen_vyssi_davky" pořád dominuje měřítko kvůli AA/AF

<scope> = "vsechny_veky" nebo "vek_<bucket>" (7 věkových dekád).

Velikost populace N (= počet distinct osob s ≥1 předpisem v dané dávkové
variantě a scope) je vypsaná v titulku každého grafu.

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
]

ATC_SUBGROUPS = ["B01AA", "B01AB", "B01AE", "B01AF", "B01AX"]
ATC_SUBGROUPS_NO_HEPARIN = [c for c in ATC_SUBGROUPS if c != "B01AB"]
ATC_COLORS = {
    "B01AA": "#1f77b4",
    "B01AB": "#ff7f0e",
    "B01AE": "#2ca02c",
    "B01AF": "#d62728",
    "B01AX": "#9467bd",
}
TOTAL_LABEL = "B01 celkem"

# Klasifikace B01AB (heparinů a příbuzných látek) na "vyšší dávky" (léčba
# trombózy/embolie) vs. profylaktické/nízké dávky nebo jinou indikaci
# (Sulodexid, Antitrombin III). Heuristika podle (léčivá látka, síla) —
# viz docstring modulu. Kombinace, co tu chybí, jsou brány jako
# neprůkazné → vyřazené (konzervativní default, viz get() níže).
HEPARIN_STRENGTH_IS_THERAPEUTIC: dict[tuple[str, str], bool] = {
    # enoxaparin: síla už obsahuje mg i objem stříkačky — ≥60 mg/0,6 ml je
    # léčebná dávka, 20/40 mg je profylaxe
    ("ENOXAPARIN", "2000IU(20MG)/0,2ML"): False,
    ("ENOXAPARIN", "4000IU(40MG)/0,4ML"): False,
    ("ENOXAPARIN", "6000IU(60MG)/0,6ML"): True,
    ("ENOXAPARIN", "8000IU(80MG)/0,8ML"): True,
    ("ENOXAPARIN", "10000IU(100MG)/1ML"): True,
    ("ENOXAPARIN", "12000IU(120MG)/0,8ML"): True,
    ("ENOXAPARIN", "15000IU(150MG)/1ML"): True,
    ("ENOXAPARIN", "100MG/ML"): True,
    ("ENOXAPARIN", "150MG/ML"): True,
    # nadroparin: "Forte" (19000 IU/ml) je vyšší koncentrace pro léčbu;
    # standardní 9500 IU/ml se používá napříč profylaxí i léčbou podle
    # podaného objemu, který ze "síly" neznáme → konzervativně vyřazeno
    ("NADROPARIN", "9500IU/ML"): False,
    ("NADROPARIN", "9.5KU/ML"): False,
    ("NADROPARIN", "19000IU/ML"): True,
    ("NADROPARIN", "19KU/ML"): True,
    ("NADROPARIN", "19000IU"): True,
    # nefrakcionovaný heparin: 5000 IU je klasická profylaktická SC dávka
    ("HEPARIN", "5000IU/ML"): False,
    ("HEPARIN", "5KU/ML"): False,
    # bemiparin: 2500/3500 IU jsou standardní fixní profylaktické dávky,
    # vyšší koncentrace (12500+) odpovídají váhově dávkované léčbě
    ("BEMIPARIN", "2500IU"): False,
    ("BEMIPARIN", "2500IU/0,2ML"): False,
    ("BEMIPARIN", "3500IU"): False,
    ("BEMIPARIN", "3500IU/0,2ML"): False,
    ("BEMIPARIN", "12500IU/ML"): True,
    ("BEMIPARIN", "12500UT/ML"): True,
    ("BEMIPARIN", "17500IU/ML"): True,
    ("BEMIPARIN", "17500UT/ML"): True,
    ("BEMIPARIN", "25000IU"): True,
    ("BEMIPARIN", "25000IU/ML"): True,
    ("BEMIPARIN", "25000UT/1ML"): True,
    # dalteparin: vysoké koncentrace, používané pro váhově dávkovanou léčbu
    ("DALTEPARIN", "25KU/ML"): True,
    ("DALTEPARIN", "12.5KU/ML"): True,
    # jiná indikace, ne léčba akutní trombózy/embolie
    ("SULODEXID", "250SU"): False,
    ("SULODEXID", "250UT"): False,
    ("SULODEXID", "600SU"): False,
    ("SULODEXID", "300UT/ML"): False,
    ("SULODEXID", "300SU/ML"): False,
    ("ANTITROMBIN III", "50IU/ML"): False,
    ("ANTITROMBIN III", "500IU"): False,
    ("ANTITROMBIN III", "1000IU"): False,
    ("ANTITROMBIN III", "50UT/ML"): False,
}

# (zobrazovaný label, bezpečný slug pro název souboru)
AGE_BUCKETS = [
    ("<30", "pod_30"),
    ("30-39", "30-39"),
    ("40-49", "40-49"),
    ("50-59", "50-59"),
    ("60-69", "60-69"),
    ("70-79", "70-79"),
    ("80+", "80_plus"),
]
AGE_BREAKS = [30, 40, 50, 60, 70, 80]
AGE_LABELS = [label for label, _ in AGE_BUCKETS]

TOP_N_SPECIALTIES = 10
UNKNOWN_SPECIALTY = "neznámo"
OTHER_SPECIALTY = "ostatní"

MONTH_START = date(2015, 1, 1)
MONTH_END = date(2023, 12, 1)
RECENT_START = date(2019, 1, 1)


def _read_one(path: Path, id_prefix: str | None) -> pl.LazyFrame:
    df = (
        pl.scan_csv(path, null_values=["", "NA"], infer_schema_length=20_000)
        .select(NEEDED_COLS)
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


def load_company(company: str) -> pl.LazyFrame:
    if company == "both_companies":
        cpzp = _read_one(OUT_DIR / "CPZP_preskladane.csv", "cpzp:")
        ozp = _read_one(OUT_DIR / "OZP_preskladane.csv", "ozp:")
        return pl.concat([cpzp, ozp])
    fname = "CPZP_preskladane.csv" if company == "cpzp" else "OZP_preskladane.csv"
    return _read_one(OUT_DIR / fname, None)


def _heparin_dose_map() -> pl.LazyFrame:
    rows = [
        {"léčivé_látky": subst, "síla": strength, "b01ab_therapeutic": therapeutic}
        for (subst, strength), therapeutic in HEPARIN_STRENGTH_IS_THERAPEUTIC.items()
    ]
    return pl.DataFrame(rows).lazy()


def build_prescriptions(df: pl.LazyFrame) -> pl.DataFrame:
    """Vrátí (malý, materializovaný) eager frame: jeden řádek na B01 předpis,
    doplněný o month/atc5/age_bucket/is_vax/Specializace/is_b01ab_therapeutic
    (bez NULL). Řádky NEJSOU filtrované podle dávky — to dělá až
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
            pl.col("léčivé_látky").str.to_uppercase(),
        )
        .join(vax_ids, on="Id_pojistence", how="left")
        .with_columns(pl.col("is_vax").fill_null(False))
        .with_columns(
            pl.col("age")
            .cut(AGE_BREAKS, labels=AGE_LABELS, left_closed=True)
            .cast(pl.Utf8)
            .alias("age_bucket")
        )
        .join(_heparin_dose_map(), on=["léčivé_látky", "síla"], how="left")
        .with_columns(pl.col("b01ab_therapeutic").fill_null(False).alias("is_b01ab_therapeutic"))
        .select(
            "Id_pojistence", "month", "atc5", "age_bucket", "is_vax", "Specializace",
            "is_b01ab_therapeutic",
        )
    )
    return presc.collect()


DOSE_VARIANTS: list[tuple[str, str, str]] = [
    ("vse", "vše", ""),
    ("jen_vyssi_davky", "jen vyšší dávky", " — B01AB jen vyšší dávky"),
    ("bez_heparinu", "bez heparinu", " — bez B01AB"),
]


def apply_dose_variant(presc: pl.DataFrame, variant: str) -> tuple[pl.DataFrame, list[str]]:
    """Vrátí (přefiltrovaný presc, seznam ATC podskupin k vykreslení) pro
    danou dávkovou variantu — viz docstring modulu."""
    if variant == "vse":
        return presc, ATC_SUBGROUPS
    if variant == "jen_vyssi_davky":
        filtered = presc.filter((pl.col("atc5") != "B01AB") | pl.col("is_b01ab_therapeutic"))
        return filtered, ATC_SUBGROUPS
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
    (B01AC/AD, případně B01AB při volání s ATC_SUBGROUPS_NO_HEPARIN) se
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
    (napříč celým obdobím) — používá se jako "N" v titulcích grafů."""
    scoped = presc.filter(pl.col("atc5").is_in(subgroups))
    if scope_filter is not None:
        scoped = scoped.filter(scope_filter)
    return scoped.select(pl.col("Id_pojistence").n_unique()).item()


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
) -> None:
    if n_population is not None:
        title = f"{title}\n(N = {_fmt_n(n_population)} osob)"
    months = wide["month"].to_list()
    fig, ax = plt.subplots(figsize=(12, 5))
    for col, style in series:
        if col not in wide.columns:
            continue
        ax.plot(months, wide[col].to_list(), label=col, **style)
    ax.set_title(title)
    ax.set_xlabel("Měsíc")
    ax.set_ylabel("Počet předpisů")
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
) -> None:
    """Dva panely vedle sebe se sdílenou osou y — pro přímé srovnání tvaru
    křivek (např. AA vs. AF) mezi dvěma scope (typicky očko/neočko)."""
    if n_left is not None:
        left_title = f"{left_title} (N = {_fmt_n(n_left)})"
    if n_right is not None:
        right_title = f"{right_title} (N = {_fmt_n(n_right)})"
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
    axes[0].set_ylabel("Počet předpisů")
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
) -> None:
    """Dva panely nad sebou se sdílenou osou x — na rozdíl od plot_crosstab
    je tu lépe vidět zarovnání v čase (stejný měsíc = stejné x u obou
    panelů), na úkor přímého srovnání výšky křivek."""
    if n_top is not None:
        top_title = f"{top_title} (N = {_fmt_n(n_top)})"
    if n_bottom is not None:
        bottom_title = f"{bottom_title} (N = {_fmt_n(n_bottom)})"
    fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True, sharey=True)
    for ax, wide, subtitle in ((axes[0], wide_top, top_title), (axes[1], wide_bottom, bottom_title)):
        months = wide["month"].to_list()
        for col, style in series:
            if col not in wide.columns:
                continue
            ax.plot(months, wide[col].to_list(), label=col, **style)
        ax.set_title(subtitle)
        ax.set_ylabel("Počet předpisů")
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


# (slug složky, popisek, počátek rozsahu, měsíční popisky osy x)
PERIODS: list[tuple[str, str, date, bool]] = [
    ("2015-2024", "2015–2023", MONTH_START, False),
    ("2019-2024", "2019–2023", RECENT_START, True),
]

AGE_SCOPES: list[tuple[str, str, pl.Expr | None]] = [("vsechny_veky", "všechny věky", None)] + [
    (f"vek_{slug}", f"věk {age_label}", pl.col("age_bucket") == age_label)
    for age_label, slug in AGE_BUCKETS
]


def process_company(company: str) -> None:
    label = COMPANY_LABELS[company]
    print(f"=== {company} ===")
    print("  načítám data…")
    presc_raw = build_prescriptions(load_company(company))
    print(f"  {presc_raw.height:,} B01 předpisů")
    out_dir = CHARTS_DIR / company

    for period_slug, period_label, start, monthly_ticks in PERIODS:
        print(f"  === období {period_slug} ===")
        period_dir = out_dir / period_slug

        for variant_slug, variant_label, subtitle_extra in DOSE_VARIANTS:
            presc, subgroups = apply_dose_variant(presc_raw, variant_slug)
            variant_dir = period_dir / variant_slug
            series = atc_series(subgroups)

            print(f"    {variant_slug}: age sada…")
            for slug, desc, scope in AGE_SCOPES:
                wide = monthly_atc_counts(presc, scope, subgroups=subgroups, start=start)
                n = population_size(presc, scope, subgroups=subgroups)
                plot_lines(
                    wide,
                    series,
                    f"{label} — B01 předpisy měsíčně ({desc}, {period_label}){subtitle_extra}",
                    variant_dir / f"atc_predpisy_mesicne_{slug}.png",
                    n_population=n,
                    monthly_ticks=monthly_ticks,
                )

            print(f"    {variant_slug}: crosstab (vedle_sebe + nad_sebou)…")
            crosstab_dir = variant_dir / "crosstab"
            for slug, desc, age_scope in AGE_SCOPES:
                vax_scope = pl.col("is_vax") if age_scope is None else (age_scope & pl.col("is_vax"))
                novax_scope = (
                    ~pl.col("is_vax") if age_scope is None else (age_scope & ~pl.col("is_vax"))
                )
                wide_vax = monthly_atc_counts(presc, vax_scope, subgroups=subgroups, start=start)
                wide_novax = monthly_atc_counts(presc, novax_scope, subgroups=subgroups, start=start)
                n_vax = population_size(presc, vax_scope, subgroups=subgroups)
                n_novax = population_size(presc, novax_scope, subgroups=subgroups)
                suptitle = (
                    f"{label} — B01 předpisy měsíčně: očkovaní vs. neočkovaní "
                    f"({desc}, {period_label}){subtitle_extra}"
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
                )

        print(f"    specializace ({period_slug})…")
        wide_spec, top_codes = specialty_chart_data(presc_raw, start=start, end=MONTH_END)
        plot_lines(
            wide_spec,
            specialty_series(top_codes),
            f"{label} — B01 předpisy měsíčně dle odbornosti lékaře (top {TOP_N_SPECIALTIES}, {period_label})",
            period_dir / "specializace_predpisy_mesicne.png",
            n_population=population_size(presc_raw, None),
            monthly_ticks=monthly_ticks,
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
    args = parser.parse_args()
    for company in args.companies:
        process_company(company)
    print("\nHotovo!")


if __name__ == "__main__":
    main()
