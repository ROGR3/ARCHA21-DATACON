from datetime import datetime, timedelta, date
from collections import defaultdict
import pickle
from common.constants.objects import (
    Person,
)
import matplotlib.pyplot as plt
from bisect import bisect_left, bisect_right
import time

from utils import (
    PREDNISON_WINDOWS_MAP_TYPE,
    AgeCohort,
    calculate_age_cohort,
    find_matching_person,
    from_prednison_equiv,
    is_injection,
    sum_after_date_pe_for_person,
    sum_before_date_pe_for_person,
)

ZACATEK_POJISTENI = date(2015, 1, 1)
KONEC_POJISTENI = date(2023, 12, 31)


POJISTOVNA = "cpzp"
with open(f"./DATACON_data/{POJISTOVNA}_persons.pkl", "rb") as f:
    persons: list[Person] = pickle.load(f)
end_time = time.time()


novax_persons_to_analyse = [
    p
    for p in persons
    if not p.vaccines
    and p.died_at is None
    and (
        p.zahajeni_pojisteni < ZACATEK_POJISTENI
        and p.ukonceni_pojisteni > KONEC_POJISTENI
    )
]

person_map = {p.id: p for p in persons}
vax_people = [
    p
    for p in persons
    if p.vaccines
    and p.died_at is None  # klidně do roku 2023
    and (
        p.zahajeni_pojisteni < ZACATEK_POJISTENI
        and p.ukonceni_pojisteni > KONEC_POJISTENI
    )
]

anchor_dates = [
    (datetime(2021, 1, 1) + timedelta(days=i)).date()
    for i in range((datetime(2022, 2, 28) - datetime(2021, 1, 1)).days + 1)
]

vax_dates_distribution: dict[AgeCohort, dict[datetime, float]] = defaultdict(
    lambda: defaultdict(float)
)

for vax_person in vax_people:
    ac = calculate_age_cohort(vax_person)
    vax_date = vax_person.vaccines[0].date
    vax_dates_distribution[ac][vax_date] += 1


class PrednisonWindowMapComputer:
    def __init__(self, people: list[Person], vax_anchor_dates: list[date]):
        self.people = people
        self.vax_anchor_dates = vax_anchor_dates

    def compute(self) -> PREDNISON_WINDOWS_MAP_TYPE:
        return compute_prednison_windows(self.people, self.vax_anchor_dates)


def compute_prednison_windows(
    people: list[Person], vax_anchor_dates: list[date]
) -> PREDNISON_WINDOWS_MAP_TYPE:
    # Prepare output structure
    result: PREDNISON_WINDOWS_MAP_TYPE = {ad: {} for ad in vax_anchor_dates}

    completed_persons = 0
    len_of_persons_to_analyse = len(people)
    for person in people:
        completed_persons += 1
        print(
            f"Processing person {completed_persons}/{len_of_persons_to_analyse}",
            end="\r",
        )

        # person_prescriptions = sorted(person.prescriptions, key=lambda pr: pr.date)
        person_prescriptions = [
            pr
            for pr in sorted(person.prescriptions, key=lambda pr: pr.date)
            if not (is_injection(pr))
        ]
        if not person_prescriptions:
            # no prescriptions → all anchor dates = 0
            for anchor in vax_anchor_dates:
                # result[ad][person.id] = 0.0
                pe_range = from_prednison_equiv(0)
                if pe_range not in result[anchor]:
                    result[anchor][pe_range] = {}

                ac = calculate_age_cohort(person)
                if ac not in result[anchor][pe_range]:
                    result[anchor][pe_range][ac] = {}

                if person.gender not in result[anchor][pe_range][ac]:
                    result[anchor][pe_range][ac][person.gender] = set()  # type: ignore

                result[anchor][pe_range][ac][person.gender].add(person.id)  # type: ignore
            continue

        # For accumulation:
        # Each anchor date holds its sum of prednison for this person
        per_person_map: dict[date, float] = {ad: 0.0 for ad in vax_anchor_dates}

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
            # result[anchor][person.id] = value
            pe_range = from_prednison_equiv(value)
            if pe_range not in result[anchor]:
                result[anchor][pe_range] = {}

            ac = calculate_age_cohort(person)
            if ac not in result[anchor][pe_range]:
                result[anchor][pe_range][ac] = {}

            if person.gender not in result[anchor][pe_range][ac]:
                result[anchor][pe_range][ac][person.gender] = set()  # type: ignore

            result[anchor][pe_range][ac][person.gender].add(person.id)  # type: ignore

    for anchor, ranges in result.items():
        for pe_range, acs in ranges.items():
            for ac, genders in acs.items():
                for gender, idset in genders.items():
                    genders[gender] = list(idset)

    return result


pe_map = compute_prednison_windows(novax_persons_to_analyse, anchor_dates)


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

total_people = len(vax_people)
completed_people = 0
skipped_people = 0
for person in vax_people:
    first_vax = person.vaccines[0]
    vax_person_before_pe = sum_before_date_pe_for_person(person, first_vax.date)
    if vax_person_before_pe > 5000:
        continue

    pe_range = from_prednison_equiv(vax_person_before_pe)
    try:
        matched_id = find_matching_person(person, pe_range, first_vax.date, pe_map)
    except Exception:
        skipped_people += 1
        continue

    vax_person_after_pe = sum_after_date_pe_for_person(person, first_vax.date)
    novax_person_before_pe = sum_before_date_pe_for_person(
        person_map[matched_id], first_vax.date
    )
    novax_person_after_pe = sum_after_date_pe_for_person(
        person_map[matched_id], first_vax.date
    )

    ac = calculate_age_cohort(person)
    vax_before_pe_map[ac][first_vax.date] += vax_person_before_pe
    vax_after_pe_map[ac][first_vax.date] += vax_person_after_pe
    novax_before_pe_map[ac][first_vax.date] += novax_person_before_pe
    novax_after_pe_map[ac][first_vax.date] += novax_person_after_pe

    completed_people += 1
    print(f"Processing person {completed_people}/{total_people}", end="\r")


print(f"\n Skipped {skipped_people} people")


result_map: dict[AgeCohort, dict[datetime, float]] = defaultdict(dict)

for cohort in (
    set(vax_before_pe_map.keys())
    | set(vax_after_pe_map.keys())
    | set(novax_before_pe_map.keys())
    | set(novax_after_pe_map.keys())
):
    # All dates inside this cohort across all four dicts
    all_dates = (
        set(vax_before_pe_map[cohort].keys())
        | set(vax_after_pe_map[cohort].keys())
        | set(novax_before_pe_map[cohort].keys())
        | set(novax_after_pe_map[cohort].keys())
    )

    result_map[cohort] = {}

    for dt in all_dates:
        vax_before = vax_before_pe_map[cohort].get(dt, 0.0)
        vax_after = vax_after_pe_map[cohort].get(dt, 0.0)
        novax_before = novax_before_pe_map[cohort].get(dt, 0.0)
        novax_after = novax_after_pe_map[cohort].get(dt, 0.0)

        # avoid division by zero
        if vax_after != 0 and novax_after != 0:
            res = (vax_before / vax_after) - (novax_before / novax_after)
            # if abs(res) < 1:
            result_map[cohort][dt] = res

# 0, 1-100, 100-1000, 1000-5000, 5000+
# u 0 mít vax_after/novax_after
# možná udělat (vax_after-vax_before)/(novax_after-novax_before)


for cohort, date_map in result_map.items():
    sorted_dates = sorted(date_map.keys())
    sorted_values = [date_map[d] for d in sorted_dates]

    vax_counts = [vax_dates_distribution[cohort].get(d, 0) for d in sorted_dates]

    fig, ax_left = plt.subplots(figsize=(10, 5))

    ax_left.plot(sorted_dates, sorted_values, label="Effect", color="blue")
    ax_left.axhline(0, color="black", linewidth=1)
    ax_left.set_xlabel("Date")
    ax_left.set_ylabel("Effect Value")
    ax_left.set_title(f"Effect Over Time — Cohort: {cohort}")

    ax_right = ax_left.twinx()
    ax_right.plot(
        sorted_dates,
        vax_counts,
        label="Vaccinated",
        linestyle="--",
        color="orange",
    )
    ax_right.set_ylabel("Vaccinated Count")

    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(f"runs/3/effect_over_time_{cohort}.png")
    plt.show()
    plt.close(fig)
