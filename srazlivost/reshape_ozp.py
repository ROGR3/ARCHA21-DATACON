#!/usr/bin/env python3
"""Reshape OZP B01/N02BF/N06AA dump into unified preskladane CSV (no PE columns)."""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

from reshape_common import OUTPUT_COLUMNS, OZP_VACCINE_NAZEV

DEFAULT_INPUT = Path("data/OZP-Janosek_2025_06_B01_N02BF_N06AA.txt")
DEFAULT_OUTPUT = Path("out/OZP_preskladane.csv")


def reshape_ozp(input_path: Path, output_path: Path) -> dict:
    df = pl.read_csv(
        input_path,
        separator="|",
        encoding="cp1250",
        infer_schema_length=50_000,
        ignore_errors=True,
        truncate_ragged_lines=True,
        null_values=["", "NA"],
    )

    vax_map = pl.DataFrame(
        {
            "Detail_udalosti": list(OZP_VACCINE_NAZEV.keys()),
            "Nazev": list(OZP_VACCINE_NAZEV.values()),
        }
    )

    # Normalize detail to string codes without trailing .0
    detail_str = (
        pl.col("Detail_udalosti")
        .cast(pl.Utf8, strict=False)
        .str.replace(r"\.0$", "")
        .str.strip_chars()
    )

    pocet_baleni = (
        pl.col("Pocet_baleni")
        .cast(pl.Utf8, strict=False)
        .str.replace_all(r"\s+", "")
        .str.replace_all(",", ".")
        .str.replace(r"^\.", "0.")
        .cast(pl.Float64, strict=False)
    )

    # "1. pololetí, 1993" / "2. pololetí, 1993" → YYYY-01-01 / YYYY-07-01
    zahajeni = (
        pl.when(
            pl.col("Posledni_zahajeni_pojisteni")
            .cast(pl.Utf8)
            .str.contains(r"^1\.\s*pololet")
        )
        .then(
            pl.col("Posledni_zahajeni_pojisteni")
            .cast(pl.Utf8)
            .str.extract(r"(\d{4})", 1)
            .add("-01-01")
        )
        .when(
            pl.col("Posledni_zahajeni_pojisteni")
            .cast(pl.Utf8)
            .str.contains(r"^2\.\s*pololet")
        )
        .then(
            pl.col("Posledni_zahajeni_pojisteni")
            .cast(pl.Utf8)
            .str.extract(r"(\d{4})", 1)
            .add("-07-01")
        )
        .otherwise(None)
    )

    typ = (
        pl.when(
            pl.col("Typ_udalosti")
            .cast(pl.Utf8)
            .str.to_lowercase()
            .str.contains("vakcin")
        )
        .then(pl.lit("vakcinace"))
        .when(
            pl.col("Typ_udalosti")
            .cast(pl.Utf8)
            .str.to_lowercase()
            .str.contains("předpis|predpis")
        )
        .then(pl.lit("předpis"))
        .otherwise(None)
    )

    atc = pl.col("ATC").cast(pl.Utf8, strict=False).str.strip_chars()

    velikost = pl.col("Velikost_baleni").cast(pl.Utf8, strict=False).str.strip_chars()
    # Pack count: product for "3X20"/"60X1"; leading count for volumes "10X0,8ML"
    second = velikost.str.extract(r"[xX×]\s*(\d+)", 1).cast(pl.Float64)
    first = velikost.str.extract(r"^\s*(\d+)", 1).cast(pl.Float64)
    is_volume = velikost.str.contains(r"(?i)ML|IU") | velikost.str.contains(
        r"[xX×]\s*\d+[,.]"
    )
    pocet_v_baleni = (
        pl.when(typ != "předpis")
        .then(None)
        .when(velikost.str.contains(r"^\s*\d+\s*[xX×]\s*\d+") & ~is_volume)
        .then(
            pl.when((second == 0) | second.is_null())
            .then(first)
            .otherwise(first * second)
        )
        .otherwise(first)
    )

    lekova = (
        pl.when(atc.str.to_uppercase().str.starts_with("B01"))
        .then(pl.lit("srazlivost"))
        .when(
            atc.str.to_uppercase().str.starts_with("N02BF")
            | atc.str.to_uppercase().str.starts_with("N06AA")
        )
        .then(pl.lit("neuropatie"))
        .otherwise(None)
    )

    out = (
        df.with_columns(
            [
                typ.alias("Typ_udalosti"),
                detail_str.alias("Detail_udalosti"),
                atc.alias("ATC_skupina"),
                pocet_baleni.alias("Pocet_baleni"),
                zahajeni.alias("Posledni_zahajeni_pojisteni"),
                pl.col("Posledni_ukonceni_pojisteni")
                .cast(pl.Utf8, strict=False)
                .str.strip_chars()
                .alias("Posledni_ukonceni_pojisteni"),
                pl.col("Datum_umrti")
                .cast(pl.Utf8, strict=False)
                .str.strip_chars()
                .alias("Datum_umrti"),
                pl.col("Datum_udalosti")
                .cast(pl.Utf8, strict=False)
                .str.strip_chars()
                .alias("Datum_udalosti"),
                pl.col("Sila")
                .cast(pl.Utf8, strict=False)
                .str.strip_chars()
                .alias("síla"),
                pocet_v_baleni.alias("Pocet_v_baleni"),
                lekova.alias("lekova_skupina"),
                pl.when(pl.col("Pohlavi").cast(pl.Utf8).str.to_uppercase().is_in(["M"]))
                .then(pl.lit("M"))
                .when(pl.col("Pohlavi").cast(pl.Utf8).str.to_uppercase().is_in(["F", "Z"]))
                .then(pl.lit("F"))
                .otherwise(None)
                .alias("Pohlavi"),
                pl.col("Specializace")
                .cast(pl.Utf8, strict=False)
                .str.strip_chars()
                .alias("Specializace"),
                pl.lit(None).cast(pl.Int64).alias("Mesic_narozeni"),
                pl.lit(None).cast(pl.Utf8).alias("léková_forma_zkr"),
                pl.lit(None).cast(pl.Utf8).alias("léčivé_látky"),
            ]
        )
        .join(vax_map, on="Detail_udalosti", how="left")
        .with_columns(
            pl.when(pl.col("Typ_udalosti") == "vakcinace")
            .then(
                pl.coalesce(
                    pl.col("Nazev"),
                    pl.concat_str(
                        [
                            pl.lit("(VZP) COVID-19 - OČKOVÁNÍ - KOD "),
                            pl.col("Detail_udalosti"),
                        ]
                    ),
                )
            )
            .otherwise(None)
            .alias("Nazev")
        )
        .select(OUTPUT_COLUMNS)
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.write_csv(output_path, null_value="")

    n06 = out.filter(
        pl.col("ATC_skupina").fill_null("").str.to_uppercase().str.starts_with("N06")
    ).height

    return {
        "rows": out.height,
        "persons": out["Id_pojistence"].n_unique(),
        "typ": out["Typ_udalosti"].value_counts().to_dicts(),
        "lekova_skupina": out["lekova_skupina"].value_counts().to_dicts(),
        "n06_rows": n06,
        "output": str(output_path),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    print(f"Reading {args.input} ...")
    stats = reshape_ozp(args.input, args.output)
    print(
        f"Wrote {stats['rows']:,} rows / {stats['persons']:,} persons → {stats['output']}"
    )
    print("Typ_udalosti:", stats["typ"])
    print("lekova_skupina:", stats["lekova_skupina"])
    if stats["n06_rows"] == 0:
        print("WARNING: OZP has 0 N06AA rows (neuropatie = N02BF only for OZP).")


if __name__ == "__main__":
    main()
