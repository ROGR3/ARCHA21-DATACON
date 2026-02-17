from dataclasses import dataclass
from enum import StrEnum
from datetime import datetime, timedelta, date
import polars as pl
from collections import defaultdict, Counter
from typing import Tuple
from common.constants.column_types import (
    CPZP_SCHEMA,
    OZP_SCHEMA,
    POHLAVI_CPZP,
    TYP_UDALOSTI,
)
from common.constants.column_names import SHARED_COLUMNS, OZP_COLUMNS, CPZP_COLUMNS
import pickle
from common.constants.objects import (
    Person,
    Gender,
    AgeCohort,
    Prescription,
    PrescriptionType,
)
import matplotlib.pyplot as plt
import numpy as np
import os
from common.utils import (
    draw_chart,
    filter_by_date_range,
)
from bisect import bisect_left, bisect_right
import random

pl.Config.set_tbl_rows(20)
pl.Config.set_tbl_cols(60)

pl.Config.set_tbl_rows(-1)


POJISTOVNA = "cpzp"
ZACATEK_POJISTENI = date(2015, 1, 1)
KONEC_POJISTENI = date(2023, 12, 31)

YEAR_OFFSET = 0
DATE_OFFSET = timedelta(days=YEAR_OFFSET * 365)

START_DATE = datetime(2021, 1, 1) - DATE_OFFSET
END_DATE = datetime(2022, 2, 28) - DATE_OFFSET
YEAR_FOR_AGE_CALCULATION = 2021 - YEAR_OFFSET


class AgeCohort(StrEnum):
    _12_15 = "12-15"
    _16_29 = "16-29"
    _30_49 = "30-49"
    _50_59 = "50-59"
    IRRELEVANT = "irrelevant"


def calculate_age_cohort(person: Person) -> AgeCohort:
    age = YEAR_FOR_AGE_CALCULATION - person.born_at.year
    if age >= 12 and age <= 15:
        return AgeCohort._12_15
    elif age >= 16 and age <= 29:
        return AgeCohort._16_29
    elif age >= 30 and age <= 49:
        return AgeCohort._30_49
    elif age >= 50 and age <= 59:
        return AgeCohort._50_59
    else:
        return AgeCohort.IRRELEVANT


def is_injection(pr: Prescription) -> bool:
    return pr.lekova_forma_zkr and (pr.lekova_forma_zkr.startswith("INJ"))


def create_prednison_enum(step: int = 25, max_value: int = 5000):
    values = {"ZERO": "zero", f"MORE_THAN_{max_value}": f"more_than_{max_value}"}

    for start in range(0, max_value, step):
        end = start + step
        name = f"BETWEEN_{start}_AND_{end}"
        values[name] = name.lower()

    return StrEnum("PREDNISON_EQUIV_CATEGORY", values)


PREDNISON_EQUIV_CATEGORY = create_prednison_enum()


def from_prednison_equiv(prednison_equiv: float) -> PREDNISON_EQUIV_CATEGORY:
    if prednison_equiv == 0:
        return PREDNISON_EQUIV_CATEGORY.ZERO

    step = 25
    max_value = 5000

    if prednison_equiv >= max_value:
        return PREDNISON_EQUIV_CATEGORY[f"MORE_THAN_{max_value}"]

    start = int((prednison_equiv // step) * step)
    end = start + step
    name = f"BETWEEN_{start}_AND_{end}"
    return PREDNISON_EQUIV_CATEGORY[name]


def find_matching_person(
    vax_person: Person,
    pe_range: PREDNISON_EQUIV_CATEGORY,
    vax_date: datetime,
    novax_map: dict[PREDNISON_EQUIV_CATEGORY, dict[AgeCohort, dict[Gender, set[int]]]],
) -> str:
    ac = calculate_age_cohort(vax_person)
    gender = vax_person.gender
    novax_matched_ids = novax_map[vax_date][pe_range][ac][gender]
    max_int = len(novax_matched_ids)
    random_index = random.randint(0, max_int - 1)
    return novax_matched_ids[random_index]


def sum_after_date_pe_for_person(person: Person, ddate: datetime) -> float:
    return sum(
        pr.prednison_equiv
        for pr in person.prescriptions
        if (not is_injection(pr))
        and (pr.date > ddate and pr.date < ddate + timedelta(days=365))
    )


def sum_before_date_pe_for_person(person: Person, ddate: datetime) -> float:
    return sum(
        pr.prednison_equiv
        for pr in person.prescriptions
        if (not is_injection(pr))
        and (pr.date > ddate - timedelta(days=365) and pr.date < ddate)
    )


class DataLoader:
    def __init__(self, pojistovna: str):
        self.__pojistovna = pojistovna
        print(f"Loading persons from {self.__pojistovna}")
        self.__persons: list[Person] = self.__load_persons()
        print(f"Loaded {len(self.__persons)} persons")
        self.__person_map: dict[int, Person] = {p.id: p for p in self.__persons}
        self.__vax_people: list[Person] = self.__get_vax_people()
        self.__novax_people: list[Person] = self.__get_novax_people()
        self.__never_prescribed_vax_people: list[Person] = (
            self.__get_never_prescribed_vax_people(self.__vax_people)
        )
        self.__zero_pe_vax_people: list[Person] = self.__get_zero_pe_vax_people(
            self.__vax_people
        )
        self.__one_to_five_hundred_pe_vax_people: list[Person] = (
            self.__get_one_to_five_hundred_pe_vax_people(self.__vax_people)
        )
        self.__five_hundred_to_five_thousand_pe_vax_people: list[Person] = (
            self.__get_five_hundred_to_five_thousand_pe_vax_people(self.__vax_people)
        )

    @property
    def person_map(self) -> dict[int, Person]:
        return self.__person_map

    @property
    def vax_people(self) -> list[Person]:
        return self.__vax_people

    @property
    def novax_people(self) -> list[Person]:
        return self.__novax_people

    @property
    def never_prescribed_vax_people(self) -> list[Person]:
        return self.__never_prescribed_vax_people

    @property
    def zero_pe_vax_people(self) -> list[Person]:
        return self.__zero_pe_vax_people

    @property
    def one_to_five_hundred_pe_vax_people(self) -> list[Person]:
        return self.__one_to_five_hundred_pe_vax_people

    @property
    def five_hundred_to_five_thousand_pe_vax_people(self) -> list[Person]:
        return self.__five_hundred_to_five_thousand_pe_vax_people

    def __load_persons(self) -> list[Person]:
        if self.__pojistovna == "both_companies":
            with open("./DATACON_data/cpzp_persons.pkl", "rb") as f:
                cpzp_persons: list[Person] = pickle.load(f)
            with open("./DATACON_data/ozp_persons.pkl", "rb") as f:
                ozp_persons: list[Person] = pickle.load(f)
            persons = cpzp_persons + ozp_persons
        else:
            with open(f"./DATACON_data/{self.__pojistovna}_persons.pkl", "rb") as f:
                persons: list[Person] = pickle.load(f)

        for p in persons:
            if p.ukonceni_pojisteni is None:
                p.ukonceni_pojisteni = date(2050, 12, 31)

        return persons

    def __get_vax_people(self) -> list[Person]:
        return [
            p
            for p in self.__persons
            if p.vaccines
            and p.died_at is None  # klidně do roku 2023
            and (
                p.zahajeni_pojisteni < ZACATEK_POJISTENI
                and p.ukonceni_pojisteni > KONEC_POJISTENI
            )
        ]

    def __get_novax_people(self) -> list[Person]:
        return [
            p
            for p in self.__persons
            if not p.vaccines
            and p.died_at is None
            and (
                p.zahajeni_pojisteni < ZACATEK_POJISTENI
                and p.ukonceni_pojisteni > KONEC_POJISTENI
            )
        ]

    def __get_never_prescribed_vax_people(
        self, vax_cohort_people: list[Person]
    ) -> list[Person]:
        return [
            p
            for p in vax_cohort_people
            if not any(
                prescription.date < p.vaccines[0].date
                for prescription in p.prescriptions
            )
        ]

    def __get_zero_pe_vax_people(self, vax_cohort_people: list[Person]) -> list[Person]:
        return [
            p
            for p in vax_cohort_people
            if sum_before_date_pe_for_person(p, p.vaccines[0].date) == 0
        ]

    def __get_one_to_five_hundred_pe_vax_people(
        self, vax_cohort_people: list[Person]
    ) -> list[Person]:
        return [
            p
            for p in vax_cohort_people
            if sum_before_date_pe_for_person(p, p.vaccines[0].date) >= 1
            and sum_before_date_pe_for_person(p, p.vaccines[0].date) < 500
        ]

    def __get_five_hundred_to_five_thousand_pe_vax_people(
        self, vax_cohort_people: list[Person]
    ) -> list[Person]:
        return [
            p
            for p in vax_cohort_people
            if sum_before_date_pe_for_person(p, p.vaccines[0].date) >= 500
            and sum_before_date_pe_for_person(p, p.vaccines[0].date) < 5000
        ]


class PrednisonWindowsComputer:
    def __init__(self, pojistovna: str, use_local_cache: bool = False):
        self.__pojistovna = pojistovna
        self.__use_local_cache = use_local_cache

    def get_prednison_windows(
        self, people: list[Person], vax_anchor_dates: list[datetime]
    ) -> dict[datetime, dict[str | int, float]]:
        if self.__use_local_cache:
            return self.__get_prednison_windows_from_local_cache()
        else:
            return self.__compute_prednison_windows(people, vax_anchor_dates)

    def __get_prednison_windows_from_local_cache(
        self,
    ) -> dict[datetime, dict[str | int, float]]:
        import pickle

        with open(f"{self.__pojistovna.lower()}_pe_map.pkl", "rb") as f:
            pe_map = pickle.load(f)

        return pe_map

    def __compute_prednison_windows(
        self, people: list[Person], vax_anchor_dates: list[datetime]
    ) -> dict[datetime, dict[str | int, float]]:
        # Prepare output structure
        result: dict[
            datetime,
            dict[PREDNISON_EQUIV_CATEGORY, dict[AgeCohort, dict[Gender, set[int]]]],
        ] = {ad: {} for ad in vax_anchor_dates}

        completed_persons = 0
        len_of_persons_to_analyse = len(people)
        for person in people:
            completed_persons += 1
            print(
                f"Processing person {completed_persons}/{len_of_persons_to_analyse}",
                end="\r",
            )

            person_prescriptions = [
                pr
                for pr in sorted(person.prescriptions, key=lambda pr: pr.date)
                if not (is_injection(pr))
            ]
            if not person_prescriptions:
                # no prescriptions → all anchor dates = 0
                for anchor in vax_anchor_dates:
                    pe_range = from_prednison_equiv(0)
                    if pe_range not in result[anchor]:
                        result[anchor][pe_range] = {}

                    ac = calculate_age_cohort(person)
                    if ac not in result[anchor][pe_range]:
                        result[anchor][pe_range][ac] = {}

                    if person.gender not in result[anchor][pe_range][ac]:
                        result[anchor][pe_range][ac][person.gender] = set()

                    result[anchor][pe_range][ac][person.gender].add(person.id)
                continue

            # For accumulation:
            # Each anchor date holds its sum of prednison for this person
            per_person_map: dict[datetime, float] = {ad: 0.0 for ad in vax_anchor_dates}

            # 3) For each prescription, add its value to all anchor dates
            #    within pr.date → pr.date + 365 days
            for pr in person_prescriptions:
                start = pr.date
                end = pr.date + timedelta(days=365)

                # indices of anchor dates inside [start, end]
                i = bisect_left(vax_anchor_dates, start)
                j = bisect_right(vax_anchor_dates, end)

                # Add prednison_equiv to each matching anchor date
                for idx in range(i, j):
                    anchor = vax_anchor_dates[idx]
                    per_person_map[anchor] += pr.prednison_equiv

            # Store per-person results
            for anchor, value in per_person_map.items():
                pe_range = from_prednison_equiv(value)
                if pe_range not in result[anchor]:
                    result[anchor][pe_range] = {}

                ac = calculate_age_cohort(person)
                if ac not in result[anchor][pe_range]:
                    result[anchor][pe_range][ac] = {}

                if person.gender not in result[anchor][pe_range][ac]:
                    result[anchor][pe_range][ac][person.gender] = set()

                result[anchor][pe_range][ac][person.gender].add(person.id)

        for anchor, ranges in result.items():
            for pe_range, acs in ranges.items():
                for ac, genders in acs.items():
                    for gender, idset in genders.items():
                        genders[gender] = list(idset)

        return result


def aggregate_date(start_date: date, window_days: int) -> datetime:
    start_date = datetime(start_date.year, start_date.month, start_date.day)
    if window_days <= 1:
        return start_date

    epoch = START_DATE
    days_since_epoch = (start_date - epoch).days
    window_start_days = (days_since_epoch // window_days) * window_days
    return epoch + timedelta(days=window_start_days)


def compute_vax_vs_novax_sums(
    people: list[Person],
    person_map: dict[int, Person],
    pe_map: dict[datetime, dict[str | int, float]],
    aggregation_days: int = 1,
):
    vax_before_pe_map: dict[AgeCohort, dict[datetime, float]] = defaultdict(
        lambda: defaultdict(float)
    )
    vax_after_pe_map: dict[AgeCohort, dict[datetime, float]] = defaultdict(
        lambda: defaultdict(float)
    )
    novax_before_pe_map: dict[AgeCohort, dict[datetime, float]] = defaultdict(
        lambda: defaultdict(float)
    )
    novax_after_pe_map: dict[AgeCohort, dict[datetime, float]] = defaultdict(
        lambda: defaultdict(float)
    )

    print("iterating through people ", len(people))

    for person in people:
        first_vax = person.vaccines[0]

        vax_person_before_pe = sum_before_date_pe_for_person(person, first_vax.date)
        if vax_person_before_pe > 5000:
            continue

        pe_range = from_prednison_equiv(vax_person_before_pe)
        try:
            matched_id = find_matching_person(person, pe_range, first_vax.date, pe_map)
        except Exception:
            continue

        matched_person = person_map[matched_id]

        vax_person_after_pe = sum_after_date_pe_for_person(person, first_vax.date)
        novax_person_before_pe = sum_before_date_pe_for_person(
            matched_person, first_vax.date
        )
        novax_person_after_pe = sum_after_date_pe_for_person(
            matched_person, first_vax.date
        )

        ac = calculate_age_cohort(person)

        aggregated_date = aggregate_date(first_vax.date, aggregation_days)

        vax_before_pe_map[ac][aggregated_date] += vax_person_before_pe
        vax_after_pe_map[ac][aggregated_date] += vax_person_after_pe
        novax_before_pe_map[ac][aggregated_date] += novax_person_before_pe
        novax_after_pe_map[ac][aggregated_date] += novax_person_after_pe

    return (
        vax_before_pe_map,
        vax_after_pe_map,
        novax_before_pe_map,
        novax_after_pe_map,
    )


EffectMap = dict[AgeCohort, dict[datetime, float]]


def run_matching_analysis(
    people: list[Person],
    person_map: dict[int, Person],
    pe_map: dict[datetime, dict[str | int, float]],
    aggregation_days: int,
    group_name: str,
    num_runs: int = 100,
) -> tuple[
    EffectMap,
    dict[AgeCohort, dict[datetime, tuple[float, float]]],
    dict[AgeCohort, dict[datetime, tuple[float, float]]],
    dict[AgeCohort, dict[datetime, list[float]]],
    EffectMap,
    EffectMap,
]:
    effects: dict[AgeCohort, dict[datetime, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )
    vax_ratio_effects: dict[AgeCohort, dict[datetime, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )
    novax_ratio_effects: dict[AgeCohort, dict[datetime, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )

    for i in range(num_runs):
        vax_before, vax_after, novax_before, novax_after = compute_vax_vs_novax_sums(
            people, person_map, pe_map, aggregation_days
        )

        result_map = compute_effect_values(
            vax_before, vax_after, novax_before, novax_after, group_name
        )

        # Also accumulate vax_after/vax_before and novax_after/novax_before for each run
        all_cohorts = (
            set(vax_before.keys())
            | set(vax_after.keys())
            | set(novax_before.keys())
            | set(novax_after.keys())
        )
        for cohort in all_cohorts:
            all_dates = (
                set(vax_before[cohort].keys())
                | set(vax_after[cohort].keys())
                | set(novax_before[cohort].keys())
                | set(novax_after[cohort].keys())
            )
            for dt in all_dates:
                vax_before_val = vax_before[cohort].get(dt, 0.0)
                vax_after_val = vax_after[cohort].get(dt, 0.0)
                novax_before_val = novax_before[cohort].get(dt, 0.0)
                novax_after_val = novax_after[cohort].get(dt, 0.0)

                vax_ratio = (
                    vax_after_val / vax_before_val
                    if vax_before_val != 0
                    else float("nan")
                )
                novax_ratio = (
                    novax_after_val / novax_before_val
                    if novax_before_val != 0
                    else float("nan")
                )

                vax_ratio_effects[cohort][dt].append(vax_ratio)
                novax_ratio_effects[cohort][dt].append(novax_ratio)

        for cohort, date_map in result_map.items():
            for dt, value in date_map.items():
                effects[cohort][dt].append(value)

        print(f"Processed {i} runs")

    median_map, iqr_map, ci_map = compute_statistics(effects)
    vax_ratio_median_map, _, _ = compute_statistics(vax_ratio_effects)
    novax_ratio_median_map, _, _ = compute_statistics(novax_ratio_effects)

    return (
        median_map,
        iqr_map,
        ci_map,
    )


def compute_effect_values(
    vax_before_pe_map,
    vax_after_pe_map,
    novax_before_pe_map,
    novax_after_pe_map,
    group_name,
) -> EffectMap:
    result_map: EffectMap = defaultdict(dict)

    cohorts = (
        set(vax_before_pe_map.keys())
        | set(vax_after_pe_map.keys())
        | set(novax_before_pe_map.keys())
        | set(novax_after_pe_map.keys())
    )

    for cohort in cohorts:
        all_dates = (
            set(vax_before_pe_map[cohort].keys())
            | set(vax_after_pe_map[cohort].keys())
            | set(novax_before_pe_map[cohort].keys())
            | set(novax_after_pe_map[cohort].keys())
        )

        for dt in all_dates:
            vax_before = vax_before_pe_map[cohort].get(dt, 0.0)
            vax_after = vax_after_pe_map[cohort].get(dt, 0.0)
            novax_before = novax_before_pe_map[cohort].get(dt, 0.0)
            novax_after = novax_after_pe_map[cohort].get(dt, 0.0)

            if group_name == "0_PE" or group_name == "NEVER_PRESCRIBED":
                if novax_after != 0:
                    result_map[cohort][dt] = vax_after / novax_after
            else:
                if vax_before != 0 and novax_before != 0:
                    result_map[cohort][dt] = (vax_after / vax_before) - (
                        novax_after / novax_before
                    )

    return result_map


def plot_treatment_effect(
    median_map,
    iqr_map,
    vax_dates_distribution,
    group_name,
    aggregation_days,
    folder_path,
):
    for cohort, date_map in median_map.items():
        sorted_dates = sorted(date_map.keys())
        y_mean = [median_map[cohort][d] for d in sorted_dates]
        y_lower = [iqr_map[cohort][d][0] for d in sorted_dates]
        y_upper = [iqr_map[cohort][d][1] for d in sorted_dates]

        vax_counts = [vax_dates_distribution[cohort].get(d, 0) for d in sorted_dates]

        fig, ax_left = plt.subplots(figsize=(10, 5))

        # left axis
        ax_left.plot(sorted_dates, y_mean, label="Median effect", color="blue")
        ax_left.fill_between(
            sorted_dates, y_lower, y_upper, alpha=0.2, label="IQR", color="blue"
        )
        ax_left.axhline(
            1 if group_name == "0_PE" or group_name == "NEVER_PRESCRIBED" else 0,
            color="black",
            linewidth=1,
        )
        ax_left.set_ylabel("Effect value")

        # right axis
        ax_right = ax_left.twinx()
        ax_right.plot(
            sorted_dates, vax_counts, linestyle="--", label="Vaccinated", color="orange"
        )
        ax_right.set_ylabel("Vaccinated count")

        ax_left.set_title(f"Cohort {cohort} - {group_name} - {aggregation_days} days")
        fig.autofmt_xdate()
        fig.tight_layout()

        # shared legend
        lines, labels = ax_left.get_legend_handles_labels()
        lines2, labels2 = ax_right.get_legend_handles_labels()
        ax_left.legend(lines + lines2, labels + labels2)

        os.makedirs(folder_path, exist_ok=True)
        fig.savefig(f"{folder_path}/{cohort}.png")
        plt.close(fig)


def write_table(median_map, iqr_map, ci_map, vax_dates_distribution, folder_path):
    table = []

    def get_median(median_map, cohort):
        values = median_map.get(cohort, {})
        return next(iter(values.values()), None)

    def get_iqr(iqr_map, cohort):
        values = iqr_map.get(cohort, {})
        return next(iter(values.values()), None)  # (q1, q3)

    def get_ci(ci_map, cohort):
        values = ci_map.get(cohort, {})
        return next(iter(values.values()), None)  # (ci_low, ci_high)

    def get_total_vaccinations(vax_dates_distribution, cohort):
        date_map = vax_dates_distribution.get(cohort, {})
        return int(sum(date_map.values()))

    for cohort in AgeCohort:
        median = get_median(median_map, cohort)
        iqr = get_iqr(iqr_map, cohort)
        ci = get_ci(ci_map, cohort)
        total_vax = get_total_vaccinations(vax_dates_distribution, cohort)

        table.append(
            {
                "věk": cohort.value,
                "Med": median,
                "IQR": iqr,
                "95% CI": ci,
                "počet očko": total_vax,
            }
        )

    import polars as pl

    df = pl.DataFrame(table)

    df.write_json(f"{folder_path}/../effects_summary.json")


def compute_statistics(
    effects: dict[AgeCohort, dict[datetime, list[float]]],
) -> tuple[EffectMap, EffectMap, EffectMap]:
    median_map: EffectMap = defaultdict(dict)
    iqr_map: dict[AgeCohort, dict[datetime, tuple[float, float]]] = defaultdict(dict)
    ci_map: EffectMap = defaultdict(dict)

    for cohort, date_map in effects.items():
        for dt, values in date_map.items():
            arr = np.asarray(values)

            median = np.median(arr)
            q1, q3 = np.percentile(arr, [25, 75])
            ci_low, ci_high = np.percentile(arr, [2.5, 97.5])

            median_map[cohort][dt] = median
            iqr_map[cohort][dt] = (q1, q3)
            ci_map[cohort][dt] = (ci_low, ci_high)

    return median_map, iqr_map, ci_map


def get_vax_dates_distribution(
    group, aggregation_days: int
) -> dict[AgeCohort, dict[datetime, float]]:
    vax_dates_distribution: dict[AgeCohort, dict[datetime, float]] = defaultdict(
        lambda: defaultdict(float)
    )

    for vax_person in group:
        ac = calculate_age_cohort(vax_person)
        vax_date = vax_person.vaccines[0].date
        aggregated_date = aggregate_date(vax_date, aggregation_days)

        vax_dates_distribution[ac][aggregated_date] += 1

    return vax_dates_distribution


def main():
    data_loader = DataLoader("cpzp")
    anchor_dates = [
        (START_DATE + timedelta(days=i)).date()
        for i in range((END_DATE - START_DATE).days + 1)
    ]
    prednison_windows_computer = PrednisonWindowsComputer(POJISTOVNA)
    prednison_windows = prednison_windows_computer.get_prednison_windows(
        data_loader.novax_people, anchor_dates
    )

    compute_vax_vs_novax_sums(
        data_loader.vax_people,
        data_loader.person_map,
        prednison_windows,
        1,
    )
    groups = {
        "NEVER_PRESCRIBED": data_loader.never_prescribed_vax_people,
        "0_PE": data_loader.zero_pe_vax_people,
        "1_to_500_PE": data_loader.one_to_five_hundred_pe_vax_people,
        "500_to_5000_PE": data_loader.five_hundred_to_five_thousand_pe_vax_people,
    }

    aggregation_days_list = [1, 14, len(anchor_dates)]

    for group_name, group in groups.items():
        for aggregation_days in aggregation_days_list:
            print(f"Processing {group_name} with {aggregation_days} days aggregation")

            folder_path = f"out/{POJISTOVNA}/matching_analysis/whole_period/{group_name}/{aggregation_days}_days_aggregation"

            (
                median_map,
                iqr_map,
                ci_map,
            ) = run_matching_analysis(
                people=group,
                person_map=data_loader.person_map,
                pe_map=prednison_windows,
                aggregation_days=aggregation_days,
                group_name=group_name,
                num_runs=2,
            )
            vax_dates_distribution = get_vax_dates_distribution(group, aggregation_days)

            plot_treatment_effect(
                median_map,
                iqr_map,
                vax_dates_distribution,
                group_name,
                aggregation_days,
                folder_path,
            )
            if aggregation_days == len(anchor_dates):
                write_table(
                    median_map, iqr_map, ci_map, vax_dates_distribution, folder_path
                )


if __name__ == "__main__":
    main()
