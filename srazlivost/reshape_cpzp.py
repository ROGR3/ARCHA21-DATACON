#!/usr/bin/env python3
"""Reshape CPZP B01/N02BF/N06AA multi-sheet xlsx into unified preskladane CSV.

Streams sheet XML via zipfile (file is multi-GB uncompressed) — no full workbook load.
"""

from __future__ import annotations

import argparse
import csv
import zipfile
from pathlib import Path
from xml.etree.ElementTree import iterparse

from reshape_common import (
    OUTPUT_COLUMNS,
    date_to_str,
    death_from_year_month,
    empty_to_none,
    excel_serial_to_date,
    lekova_skupina_from_atc,
    normalize_pohlavi,
    normalize_typ_udalosti,
    parse_cz_float,
    parse_pocet_v_baleni,
)

DEFAULT_INPUT = Path("data/CPZP-POJ205_zadost25037.xlsx")
DEFAULT_OUTPUT = Path("out/CPZP_preskladane.csv")

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

# workbook.xml order → sheetN.xml
SHEETS = [
    ("xl/worksheets/sheet1.xml", "Roky 2015, 2016"),
    ("xl/worksheets/sheet2.xml", "Roky 2017, 2018"),
    ("xl/worksheets/sheet3.xml", "Roky 2019, 2020"),
    ("xl/worksheets/sheet4.xml", "Rok 2021_1"),
    ("xl/worksheets/sheet5.xml", "Rok 2021_2"),
    ("xl/worksheets/sheet6.xml", "Rok 2021_3"),
    ("xl/worksheets/sheet7.xml", "Rok 2022"),
    ("xl/worksheets/sheet8.xml", "Rok 2023"),
    ("xl/worksheets/sheet9.xml", "Ostatní POJ"),
]

# Expected logical columns on event sheets (Czech headers → index)
EVENT_WIDTH = 18
EMPTY_WIDTH = 8


def col_index_from_ref(cell_ref: str) -> int:
    col = ""
    for ch in cell_ref:
        if ch.isalpha():
            col += ch
        else:
            break
    n = 0
    for ch in col:
        n = n * 26 + (ord(ch.upper()) - ord("A") + 1)
    return n - 1


def load_shared_strings(z: zipfile.ZipFile) -> list[str]:
    ss: list[str] = []
    with z.open("xl/sharedStrings.xml") as f:
        for _event, elem in iterparse(f, events=("end",)):
            if elem.tag == f"{NS}si":
                texts = [
                    t.text or ""
                    for t in elem.iter()
                    if t.tag == f"{NS}t"
                ]
                ss.append("".join(texts))
                elem.clear()
    return ss


def cell_value(cell, shared_strings: list[str]) -> str | None:
    t = cell.attrib.get("t")
    v_elem = cell.find(f"{NS}v")
    if v_elem is None or v_elem.text is None:
        # inline string
        is_elem = cell.find(f"{NS}is")
        if is_elem is not None:
            texts = [t.text or "" for t in is_elem.iter() if t.tag == f"{NS}t"]
            return "".join(texts) or None
        return None
    raw = v_elem.text
    if t == "s":
        return shared_strings[int(raw)]
    return raw


def iter_sheet_rows(z: zipfile.ZipFile, sheet_path: str, shared_strings: list[str]):
    with z.open(sheet_path) as f:
        for _event, elem in iterparse(f, events=("end",)):
            if elem.tag != f"{NS}row":
                continue
            vals: dict[int, str | None] = {}
            max_idx = -1
            for c in elem:
                if c.tag != f"{NS}c":
                    continue
                ref = c.attrib.get("r", "A1")
                idx = col_index_from_ref(ref)
                vals[idx] = cell_value(c, shared_strings)
                if idx > max_idx:
                    max_idx = idx
            width = max(max_idx + 1, 1)
            row = [vals.get(i) for i in range(width)]
            elem.clear()
            yield row


def is_header_row(row: list[str | None]) -> bool:
    if not row or row[0] is None:
        return False
    first = str(row[0]).strip().lower()
    return first.startswith("identifika") or first == "id"


def row_to_output(row: list[str | None], sheet_name: str) -> dict | None:
    """Map a raw sheet row to OUTPUT_COLUMNS dict, or None to skip."""
    # Pad
    if sheet_name == "Ostatní POJ":
        while len(row) < EMPTY_WIDTH:
            row.append(None)
        person_id = empty_to_none(row[0])
        if person_id is None:
            return None
        return {
            "Id_pojistence": person_id,
            "Pohlavi": normalize_pohlavi(row[1]),
            "Rok_narozeni": empty_to_none(row[2]),
            "Mesic_narozeni": empty_to_none(row[3]),
            "Posledni_zahajeni_pojisteni": date_to_str(excel_serial_to_date(row[4])),
            "Posledni_ukonceni_pojisteni": date_to_str(excel_serial_to_date(row[5])),
            "Datum_umrti": date_to_str(death_from_year_month(row[6], row[7])),
            "Typ_udalosti": None,
            "Detail_udalosti": None,
            "Nazev": None,
            "ATC_skupina": None,
            "Pocet_baleni": None,
            "síla": None,
            "Pocet_v_baleni": None,
            "léková_forma_zkr": None,
            "léčivé_látky": None,
            "Specializace": None,
            "Datum_udalosti": None,
            "lekova_skupina": None,
        }

    while len(row) < EVENT_WIDTH:
        row.append(None)

    person_id = empty_to_none(row[0])
    if person_id is None:
        return None

    typ = normalize_typ_udalosti(row[8])
    detail = empty_to_none(row[9])
    atc = empty_to_none(row[10])
    nazev = detail if typ == "vakcinace" else None
    sila = empty_to_none(row[12])
    velikost = empty_to_none(row[13])
    forma = empty_to_none(row[14])
    latka = empty_to_none(row[15])
    spec = empty_to_none(row[17])

    return {
        "Id_pojistence": person_id,
        "Pohlavi": normalize_pohlavi(row[1]),
        "Rok_narozeni": empty_to_none(row[2]),
        "Mesic_narozeni": empty_to_none(row[3]),
        "Posledni_zahajeni_pojisteni": date_to_str(excel_serial_to_date(row[4])),
        "Posledni_ukonceni_pojisteni": date_to_str(excel_serial_to_date(row[5])),
        "Datum_umrti": date_to_str(death_from_year_month(row[6], row[7])),
        "Typ_udalosti": typ,
        "Detail_udalosti": detail,
        "Nazev": nazev,
        "ATC_skupina": atc,
        "Pocet_baleni": parse_cz_float(row[11]),
        "síla": sila,
        "Pocet_v_baleni": parse_pocet_v_baleni(velikost) if typ == "předpis" else None,
        "léková_forma_zkr": forma,
        "léčivé_látky": latka,
        "Specializace": spec if typ == "předpis" else None,
        "Datum_udalosti": date_to_str(excel_serial_to_date(row[16])),
        "lekova_skupina": lekova_skupina_from_atc(atc),
    }


def reshape_cpzp(input_path: Path, output_path: Path) -> dict:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    n_rows = 0
    n_by_typ: dict[str, int] = {}
    n_by_skupina: dict[str, int] = {}
    persons: set[str] = set()
    n06 = 0

    with zipfile.ZipFile(input_path) as z, output_path.open(
        "w", encoding="utf-8", newline=""
    ) as out_f:
        def log(msg: str) -> None:
            print(msg, flush=True)

        log("Loading shared strings...")
        shared = load_shared_strings(z)
        log(f"  {len(shared)} shared strings")

        writer = csv.DictWriter(out_f, fieldnames=OUTPUT_COLUMNS, lineterminator="\n")
        writer.writeheader()

        for sheet_path, sheet_name in SHEETS:
            log(f"Streaming {sheet_name} ({sheet_path})...")
            sheet_rows = 0
            for row in iter_sheet_rows(z, sheet_path, shared):
                if is_header_row(row):
                    continue
                mapped = row_to_output(row, sheet_name)
                if mapped is None:
                    continue
                writer.writerow(
                    {k: ("" if mapped[k] is None else mapped[k]) for k in OUTPUT_COLUMNS}
                )
                n_rows += 1
                sheet_rows += 1
                persons.add(mapped["Id_pojistence"])
                typ_key = mapped["Typ_udalosti"] or "null"
                n_by_typ[typ_key] = n_by_typ.get(typ_key, 0) + 1
                sk = mapped["lekova_skupina"] or "null"
                n_by_skupina[sk] = n_by_skupina.get(sk, 0) + 1
                atc = mapped["ATC_skupina"] or ""
                if atc.upper().startswith("N06"):
                    n06 += 1
                if sheet_rows % 500_000 == 0:
                    log(f"  ... {sheet_rows:,} rows")
            log(f"  done: {sheet_rows:,} data rows")

    return {
        "rows": n_rows,
        "persons": len(persons),
        "typ": n_by_typ,
        "lekova_skupina": n_by_skupina,
        "n06_rows": n06,
        "output": str(output_path),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    print(f"Reading {args.input} ...")
    stats = reshape_cpzp(args.input, args.output)
    print(
        f"Wrote {stats['rows']:,} rows / {stats['persons']:,} persons → {stats['output']}"
    )
    print("Typ_udalosti:", stats["typ"])
    print("lekova_skupina:", stats["lekova_skupina"])
    print("N06 rows:", stats["n06_rows"])


if __name__ == "__main__":
    main()
