#!/usr/bin/env python3
"""
Měsíční křivky počtu předpisů B01 (srážlivost) — krok 1 z e-mailu PaJi (20.8.2026).

Pro každou pojišťovnu (cpzp, ozp, both_companies) generuje:
  - "age" sadu:  celek + 7 věkových dekád, čáry B01AA/AB/AE/AF/AX + B01 celkem
                 ("B01 celkem" = součet jen těchto pěti podskupin, BEZ B01AC/AD)
  - "vax" sadu:  očkovaní + neočkovaní (stejné čáry jako výše)
  - specializaci: top 10 kódů odbornosti dle objemu + koš "ostatní",
    počty za celé B01 (bez ATC rozpadu, tj. včetně AC/AD)

Věk = rok předpisu − rok narození (hrubý odhad, u OZP chybí měsíc narození).
Očkovaný/neočkovaný = osoba má/nemá kdykoliv v datech alespoň jeden
vakcinační řádek (celoobdobní ever-vaccinated, ne zarovnané k datu vakcinace).

Výstupy: charts/<company>/*.png
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
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
]

ATC_SUBGROUPS = ["B01AA", "B01AB", "B01AE", "B01AF", "B01AX"]
ATC_COLORS = {
    "B01AA": "#1f77b4",
    "B01AB": "#ff7f0e",
    "B01AE": "#2ca02c",
    "B01AF": "#d62728",
    "B01AX": "#9467bd",
}
TOTAL_LABEL = "B01 celkem"

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


def build_prescriptions(df: pl.LazyFrame) -> pl.DataFrame:
    """Vrátí (malý, materializovaný) eager frame: jeden řádek na B01 předpis,
    doplněný o month/atc5/age_bucket/is_vax/Specializace (bez NULL)."""
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
        .join(vax_ids, on="Id_pojistence", how="left")
        .with_columns(pl.col("is_vax").fill_null(False))
        .with_columns(
            pl.col("age")
            .cut(AGE_BREAKS, labels=AGE_LABELS, left_closed=True)
            .cast(pl.Utf8)
            .alias("age_bucket")
        )
        .select("month", "atc5", "age_bucket", "is_vax", "Specializace")
    )
    return presc.collect()


def full_month_index() -> pl.DataFrame:
    return pl.DataFrame(
        {"month": pl.date_range(MONTH_START, MONTH_END, interval="1mo", eager=True)}
    )


def monthly_atc_counts(presc: pl.DataFrame, scope_filter: pl.Expr | None) -> pl.DataFrame:
    """Wide tabulka: month, B01AA..B01AX, B01 celkem (počty), pro dané scope.

    "B01 celkem" je součet jen zobrazených podskupin (AA/AB/AE/AF/AX) —
    B01AC a B01AD se nevykreslují ani nezapočítávají do celkem."""
    scoped = presc.filter(pl.col("atc5").is_in(ATC_SUBGROUPS))
    if scope_filter is not None:
        scoped = scoped.filter(scope_filter)

    present_subgroups = [c for c in ATC_SUBGROUPS if scoped.filter(pl.col("atc5") == c).height > 0]
    sub = (
        scoped.group_by("month", "atc5")
        .len()
        .pivot(on="atc5", index="month", values="len")
        if present_subgroups
        else pl.DataFrame({"month": []}, schema={"month": pl.Date})
    )

    wide = full_month_index().join(sub, on="month", how="left")
    value_cols = [c for c in ATC_SUBGROUPS if c in wide.columns]
    wide = wide.with_columns([pl.col(c).fill_null(0) for c in value_cols])
    wide = wide.with_columns(pl.sum_horizontal(value_cols).alias(TOTAL_LABEL))
    return wide.sort("month")


def specialty_chart_data(presc: pl.DataFrame) -> tuple[pl.DataFrame, list[str]]:
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
    wide = full_month_index().join(counts, on="month", how="left")
    value_cols = [c for c in wide.columns if c != "month"]
    wide = wide.with_columns([pl.col(c).fill_null(0) for c in value_cols])
    return wide.sort("month"), top_codes


def plot_lines(
    wide: pl.DataFrame,
    series: list[tuple[str, dict]],
    title: str,
    output_path: Path,
) -> None:
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
    fig.autofmt_xdate()
    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  → uloženo: {output_path}")


def atc_series() -> list[tuple[str, dict]]:
    series = [
        (c, {"color": ATC_COLORS[c], "linewidth": 1.2, "marker": "o", "markersize": 2})
        for c in ATC_SUBGROUPS
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


def process_company(company: str) -> None:
    label = COMPANY_LABELS[company]
    print(f"=== {company} ===")
    print("  načítám data…")
    presc = build_prescriptions(load_company(company))
    print(f"  {presc.height:,} B01 předpisů")
    out_dir = CHARTS_DIR / company

    print("  age sada…")
    wide_all = monthly_atc_counts(presc, None)
    plot_lines(
        wide_all,
        atc_series(),
        f"{label} — B01 předpisy měsíčně (všechny věky)",
        out_dir / "atc_predpisy_mesicne_vsechny_veky.png",
    )
    for age_label, slug in AGE_BUCKETS:
        wide = monthly_atc_counts(presc, pl.col("age_bucket") == age_label)
        plot_lines(
            wide,
            atc_series(),
            f"{label} — B01 předpisy měsíčně (věk {age_label})",
            out_dir / f"atc_predpisy_mesicne_vek_{slug}.png",
        )

    print("  vax sada…")
    wide_vax = monthly_atc_counts(presc, pl.col("is_vax"))
    plot_lines(
        wide_vax,
        atc_series(),
        f"{label} — B01 předpisy měsíčně (očkovaní)",
        out_dir / "atc_predpisy_mesicne_ockovani.png",
    )
    wide_novax = monthly_atc_counts(presc, ~pl.col("is_vax"))
    plot_lines(
        wide_novax,
        atc_series(),
        f"{label} — B01 předpisy měsíčně (neočkovaní)",
        out_dir / "atc_predpisy_mesicne_neockovani.png",
    )

    print("  specializace…")
    wide_spec, top_codes = specialty_chart_data(presc)
    plot_lines(
        wide_spec,
        specialty_series(top_codes),
        f"{label} — B01 předpisy měsíčně dle odbornosti lékaře (top {TOP_N_SPECIALTIES})",
        out_dir / "specializace_predpisy_mesicne.png",
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
