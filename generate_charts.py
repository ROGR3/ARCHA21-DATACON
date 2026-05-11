#!/usr/bin/env python3
"""
Generuje týdně agregované grafy pro:
  1. Prvopředpisy kortikoidů (první předpis kortikoidu na osobu) rok před očkováním
  2. Celkovou spotřebu kortikoidů (prednison equiv.) na osobu rok před očkováním

Stratifikace: všechny věky, 16-29, 12-29
"""

import pickle
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from common.constants.objects import Person, PrescriptionType

POJISTOVNA = "cpzp"
HIGHLIGHT_DATE = date(2021, 6, 9)
YEAR_FOR_AGE = 2021
OUTPUT_DIR = Path("charts")
MIN_PERSONS_PER_WEEK = 5


def load_persons(pojistovna: str) -> list[Person]:
    if pojistovna == "both":
        with open("./DATACON_data/cpzp_persons.pkl", "rb") as f:
            cpzp: list[Person] = pickle.load(f)
        with open("./DATACON_data/ozp_persons.pkl", "rb") as f:
            ozp: list[Person] = pickle.load(f)
        return cpzp + ozp
    with open(f"./DATACON_data/{pojistovna}_persons.pkl", "rb") as f:
        return pickle.load(f)


def start_of_week(d: date) -> date:
    return d - timedelta(days=d.weekday())


def person_age(person: Person) -> int:
    return YEAR_FOR_AGE - person.born_at.year


def in_age_range(person: Person, min_age: int | None, max_age: int | None) -> bool:
    age = person_age(person)
    if min_age is not None and age < min_age:
        return False
    if max_age is not None and age > max_age:
        return False
    return True


def first_corticoid_date(person: Person) -> date | None:
    corts = [
        p
        for p in person.prescriptions
        if p.prescription_type == PrescriptionType.KORTIKOID
    ]
    if not corts:
        return None
    return min(corts, key=lambda p: p.date).date


def compute_weekly_data(
    persons: list[Person],
    value_fn,
    min_age: int | None = None,
    max_age: int | None = None,
    min_persons: int = MIN_PERSONS_PER_WEEK,
) -> tuple[dict[date, float], dict[date, int]]:
    weekly_sums: dict[date, float] = defaultdict(float)
    weekly_counts: dict[date, int] = defaultdict(int)

    for person in persons:
        if person.died_at or not person.vaccines:
            continue
        if not in_age_range(person, min_age, max_age):
            continue

        first_vax_date = person.vaccines[0].date
        week = start_of_week(first_vax_date)
        weekly_counts[week] += 1
        weekly_sums[week] += value_fn(person, first_vax_date)

    averages: dict[date, float] = {}
    filtered_counts: dict[date, int] = {}
    for week in weekly_sums:
        if weekly_counts[week] >= min_persons:
            averages[week] = weekly_sums[week] / weekly_counts[week]
            filtered_counts[week] = weekly_counts[week]
    return averages, filtered_counts


def prescription_count_value(person: Person, first_vax_date: date) -> float:
    """Počet všech předpisů v roce před očkováním."""
    count = 0
    for pres in person.prescriptions:
        rel = (pres.date - first_vax_date).days
        if -365 <= rel < 0:
            count += 1
    return float(count)


def prvopredpis_value(person: Person, first_vax_date: date) -> float:
    """1.0 pokud první kortikoidní předpis osoby padl do roku před očkováním, jinak 0.0."""
    fc = first_corticoid_date(person)
    if fc is None:
        return 0.0
    rel = (fc - first_vax_date).days
    if -365 <= rel < 0:
        return 1.0
    return 0.0


def consumption_value(person: Person, first_vax_date: date) -> float:
    """Celková spotřeba kortikoidů (prednison equiv.) v roce před očkováním."""
    total = 0.0
    for pres in person.prescriptions:
        if pres.prescription_type != PrescriptionType.KORTIKOID:
            continue
        rel = (pres.date - first_vax_date).days
        if -365 <= rel < 0:
            total += pres.prednison_equiv
    return total


def plot_chart(
    weekly_averages: dict[date, float],
    weekly_counts: dict[date, int],
    title: str,
    ylabel: str,
    output_path: Path,
) -> None:
    sorted_weeks = sorted(weekly_averages.keys())
    values = [weekly_averages[w] for w in sorted_weeks]
    counts = [weekly_counts[w] for w in sorted_weeks]

    fig, ax1 = plt.subplots(figsize=(12, 5))

    ax1.plot(
        sorted_weeks,
        values,
        marker="o",
        markersize=3,
        linewidth=0.8,
        color="steelblue",
        label=ylabel,
    )
    ax1.axvline(
        HIGHLIGHT_DATE,
        color="red",
        linestyle="--",
        linewidth=2,
        label="9.6.2021",
    )
    ax1.set_xlabel("Začátek týdne")
    ax1.set_ylabel(ylabel, color="steelblue")
    ax1.tick_params(axis="y", labelcolor="steelblue")
    ax1.grid(True, alpha=0.3)

    ax2 = ax1.twinx()
    ax2.bar(
        sorted_weeks,
        counts,
        width=5,
        alpha=0.15,
        color="gray",
        label="Počet osob v týdnu",
    )
    ax2.set_ylabel("Počet osob v týdnu", color="gray")
    ax2.tick_params(axis="y", labelcolor="gray")

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=8)

    ax1.set_title(title)
    fig.autofmt_xdate()
    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"  → uloženo: {output_path}")
    plt.close(fig)


def main() -> None:
    pojistovna = sys.argv[1] if len(sys.argv) > 1 else POJISTOVNA
    print(f"Načítám data ({pojistovna})…")
    persons = load_persons(pojistovna)
    print(f"Načteno {len(persons)} osob.\n")

    age_configs: list[tuple[int | None, int | None, str]] = [
        (None, None, "všechny_věky"),
        (16, 29, "16-29"),
        (12, 29, "12-29"),
    ]

    print("=== Průměrný počet předpisů na osobu ===")
    for min_age, max_age, label in age_configs:
        title_label = label.replace("_", " ")
        print(f"  {title_label}…")
        avgs, counts = compute_weekly_data(
            persons, prescription_count_value, min_age, max_age
        )
        plot_chart(
            avgs,
            counts,
            title=(
                "Průměrný počet předpisů na osobu rok před očkováním\n"
                f"({title_label}, agregováno týdně)"
            ),
            ylabel="Průměr předpisů / osoba (týdně)",
            output_path=OUTPUT_DIR / f"predpisy_na_osobu_{label}.png",
        )

    print("\n=== Prvopředpisy kortikoidů ===")
    for min_age, max_age, label in age_configs:
        title_label = label.replace("_", " ")
        print(f"  {title_label}…")
        avgs, counts = compute_weekly_data(persons, prvopredpis_value, min_age, max_age)
        plot_chart(
            avgs,
            counts,
            title=(
                "Prvopředpisy kortikoidů na osobu rok před očkováním\n"
                f"({title_label}, agregováno týdně)"
            ),
            ylabel="Prvopředpisy / osoba (týdně)",
            output_path=OUTPUT_DIR / f"prvopredpisy_{label}.png",
        )

    print("\n=== Celková spotřeba kortikoidů ===")
    for min_age, max_age, label in age_configs:
        title_label = label.replace("_", " ")
        print(f"  {title_label}…")
        avgs, counts = compute_weekly_data(persons, consumption_value, min_age, max_age)
        plot_chart(
            avgs,
            counts,
            title=(
                "Průměrná celková spotřeba kortikoidů na osobu rok před očkováním\n"
                f"({title_label}, agregováno týdně)"
            ),
            ylabel="Spotřeba (prednison equiv.) / osoba (týdně)",
            output_path=OUTPUT_DIR / f"spotreba_kortikoidu_{label}.png",
        )

    print("\nHotovo!")


if __name__ == "__main__":
    main()
