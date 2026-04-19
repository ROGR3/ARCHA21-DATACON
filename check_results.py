"""
Integration check for matching analysis results.

Usage:
    python check_results.py snapshot          # save current values as baseline
    python check_results.py check             # compare current vs baseline (1% default)
    python check_results.py check --tol 0.02  # custom tolerance (2%)
"""

import argparse
import json
import sys
from pathlib import Path

ROOT = Path("out/cpzp/matching_analysis")
BASELINE_FILE = Path("results_baseline.json")

NUMERIC_FIELDS_SUMMARY = {
    "Med": "scalar",
    "IQR": "pair",
    "95% CI": "pair",
    "očko po-před": "scalar",
    "očko 95% CI": "pair",
    "neočko po-před": "scalar",
    "neočko 95% CI": "pair",
}

NUMERIC_FIELDS_SPECIALTY = {
    "Med": "scalar",
    "95% CI": "pair",
    "očko PE po-před": "scalar",
    "očko 95% CI": "pair",
    "neočko PE po-před": "scalar",
    "neočko 95% CI": "pair",
}


def _rel(path: Path) -> str:
    return str(path.relative_to(ROOT))


def _extract(row: dict, fields: dict) -> dict[str, float]:
    out = {}
    for field, kind in fields.items():
        val = row.get(field)
        if val is None:
            continue
        if kind == "scalar":
            out[field] = float(val)
        elif kind == "pair":
            out[f"{field}[0]"] = float(val[0])
            out[f"{field}[1]"] = float(val[1])
    return out


def collect_all() -> dict[str, float]:
    """Walk all result JSONs and return a flat {key: value} dict."""
    flat: dict[str, float] = {}

    for jpath in sorted(ROOT.rglob("effects_summary.json")):
        prefix = _rel(jpath)
        with open(jpath) as f:
            rows = json.load(f)
        for row in rows:
            age = row["věk"]
            for k, v in _extract(row, NUMERIC_FIELDS_SUMMARY).items():
                flat[f"{prefix}::{age}::{k}"] = v

    for jpath in sorted(ROOT.rglob("specialty_effects.json")):
        prefix = _rel(jpath)
        with open(jpath) as f:
            rows = json.load(f)
        for row in rows:
            spec = row["specializace"]
            age = row["věk"]
            for k, v in _extract(row, NUMERIC_FIELDS_SPECIALTY).items():
                flat[f"{prefix}::{spec}::{age}::{k}"] = v

    return flat


def rel_diff(a: float, b: float) -> float:
    denom = max(abs(a), abs(b))
    if denom == 0:
        return 0.0
    return abs(a - b) / denom


def snapshot():
    flat = collect_all()
    with open(BASELINE_FILE, "w") as f:
        json.dump(flat, f, indent=2, ensure_ascii=False)
    print(f"Baseline saved → {BASELINE_FILE}  ({len(flat):,} values)")


def check(tol: float):
    if not BASELINE_FILE.exists():
        print(f"ERROR: {BASELINE_FILE} not found. Run 'snapshot' first.", file=sys.stderr)
        sys.exit(1)

    with open(BASELINE_FILE) as f:
        baseline = json.load(f)

    current = collect_all()

    diffs = []
    missing_in_current = []
    new_in_current = []

    for key, old_val in baseline.items():
        if key not in current:
            missing_in_current.append(key)
            continue
        new_val = current[key]
        rd = rel_diff(old_val, new_val)
        if rd > tol:
            diffs.append((key, old_val, new_val, rd))

    for key in current:
        if key not in baseline:
            new_in_current.append(key)

    ok = True

    if diffs:
        ok = False
        diffs.sort(key=lambda x: -x[3])
        print(f"\n{'='*80}")
        print(f"  FAIL: {len(diffs)} values differ by more than {tol:.0%}")
        print(f"{'='*80}\n")
        for key, old, new, rd in diffs[:50]:
            print(f"  {rd:>7.2%}  {old:>14.6f} → {new:>14.6f}  {key}")
        if len(diffs) > 50:
            print(f"  ... and {len(diffs) - 50} more")

    if missing_in_current:
        ok = False
        print(f"\n  WARNING: {len(missing_in_current)} baseline keys missing in current results")
        for k in missing_in_current[:10]:
            print(f"    - {k}")
        if len(missing_in_current) > 10:
            print(f"    ... and {len(missing_in_current) - 10} more")

    if new_in_current:
        print(f"\n  INFO: {len(new_in_current)} new keys not in baseline (new fields/groups)")
        for k in new_in_current[:10]:
            print(f"    + {k}")
        if len(new_in_current) > 10:
            print(f"    ... and {len(new_in_current) - 10} more")

    if ok:
        total = len(baseline)
        print(f"\n  OK: all {total:,} values within {tol:.0%} tolerance ✓\n")
    else:
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Check matching results stability")
    sub = parser.add_subparsers(dest="cmd")
    sub.add_parser("snapshot", help="Save current results as baseline")
    chk = sub.add_parser("check", help="Compare current results against baseline")
    chk.add_argument("--tol", type=float, default=0.01, help="Relative tolerance (default 0.01 = 1%%)")

    args = parser.parse_args()
    if args.cmd == "snapshot":
        snapshot()
    elif args.cmd == "check":
        check(args.tol)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
