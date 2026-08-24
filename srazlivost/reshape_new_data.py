#!/usr/bin/env python3
"""Reshape new B01/N02BF/N06AA data for both insurers into unified preskladane CSVs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--company",
        choices=["ozp", "cpzp", "all"],
        default="all",
        help="Which insurer dump to reshape (default: all)",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data"),
        help="Output directory for *_preskladane.csv",
    )
    args = parser.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    if args.company in ("ozp", "all"):
        from reshape_ozp import reshape_ozp

        out = args.out_dir / "OZP_preskladane.csv"
        print("=== OZP ===")
        stats = reshape_ozp(
            Path("data/OZP-Janosek_2025_06_B01_N02BF_N06AA.txt"), out
        )
        print(
            f"Wrote {stats['rows']:,} rows / {stats['persons']:,} persons → {stats['output']}"
        )
        print("Typ_udalosti:", stats["typ"])
        print("lekova_skupina:", stats["lekova_skupina"])
        if stats["n06_rows"] == 0:
            print("WARNING: OZP has 0 N06AA rows.")

    if args.company in ("cpzp", "all"):
        from reshape_cpzp import reshape_cpzp

        out = args.out_dir / "CPZP_preskladane.csv"
        print("=== CPZP ===")
        stats = reshape_cpzp(Path("data/CPZP-POJ205_zadost25037.xlsx"), out)
        print(
            f"Wrote {stats['rows']:,} rows / {stats['persons']:,} persons → {stats['output']}"
        )
        print("Typ_udalosti:", stats["typ"])
        print("lekova_skupina:", stats["lekova_skupina"])
        print("N06 rows:", stats["n06_rows"])


if __name__ == "__main__":
    main()
    sys.exit(0)
