from dataclasses import dataclass
from enum import StrEnum
from datetime import datetime, timedelta
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
from common.utils import draw_chart

pl.Config.set_tbl_rows(20)
pl.Config.set_tbl_cols(60)


with open("./DATACON_data/cpzp_persons.pkl", "rb") as f:
    cpzp_persons: list[Person] = pickle.load(f)


def to_date(dt):
    if hasattr(dt, "date") and callable(dt.date):
        return dt.date()
    return dt


# Data structures for dose-specific analysis
# Key: (age_cohort, dose_number)
dose_vax_dates: dict[Tuple[AgeCohort, int], list[datetime]] = defaultdict(list)
dose_people: dict[Tuple[AgeCohort, int], list[Person]] = defaultdict(list)
people_dose_info: dict[Tuple[AgeCohort, int], list[Tuple[Person, datetime]]] = (
    defaultdict(list)
)

# Collect vaccination data by age cohort and dose number
for person in cpzp_persons:
    if person.vaccines:  # Only consider people who have been vaccinated
        # Sort vaccines by date to assign dose numbers
        sorted_vaccines = sorted(person.vaccines, key=lambda v: v.date)

        for dose_num, vaccine in enumerate(sorted_vaccines, 1):
            if dose_num <= 5:  # Max 5 doses as requested
                # Use age cohort at time of vaccination, not person's current age cohort
                cohort_dose_key = (vaccine.age_cohort, dose_num)
                dose_vax_dates[cohort_dose_key].append(vaccine.date)
                people_dose_info[cohort_dose_key].append((person, vaccine.date))

print(f"Total people: {len(cpzp_persons)}")
print(f"People with vaccines: {len([p for p in cpzp_persons if p.vaccines])}")

# Find peak vaccination date for each cohort-dose combination
peak_dates_by_cohort_dose: dict[Tuple[AgeCohort, int], datetime] = {}
cohort_dose_stats: dict[Tuple[AgeCohort, int], dict] = {}

print(f"\n" + "=" * 100)
print("PEAK VACCINATION ANALYSIS BY AGE COHORT AND DOSE NUMBER")
print("=" * 100)

for (cohort, dose_num), vax_dates in dose_vax_dates.items():
    if not vax_dates:
        continue

    # Count vaccinations by date
    date_counts = Counter(to_date(vax_date) for vax_date in vax_dates)

    # Find the date with maximum vaccinations
    peak_date, peak_count = date_counts.most_common(1)[0]
    peak_dates_by_cohort_dose[(cohort, dose_num)] = datetime.combine(
        peak_date, datetime.min.time()
    )

    # Calculate date range
    all_dates = [to_date(vax_date) for vax_date in vax_dates]
    min_date, max_date = min(all_dates), max(all_dates)

    # Count unique people for this dose
    unique_people = len(people_dose_info[(cohort, dose_num)])

    cohort_dose_stats[(cohort, dose_num)] = {
        "total_vaccines": len(vax_dates),
        "unique_people": unique_people,
        "peak_date": peak_date,
        "peak_count": peak_count,
        "date_range": (min_date, max_date),
    }

    print(f"\n{cohort} - Dose {dose_num}:")
    print(f"  Total vaccines: {len(vax_dates)}")
    print(f"  Unique people: {unique_people}")
    print(f"  Peak date: {peak_date} ({peak_count} vaccinations)")
    print(f"  Date range: {min_date} to {max_date}")


# Filter people vaccinated within window around peak for each dose
def filter_people_around_peak_by_dose(
    window_days: int = 30,
) -> dict[Tuple[AgeCohort, int], list[Tuple[Person, datetime]]]:
    """
    Filter people vaccinated within window_days of the peak vaccination date for their cohort and dose.

    Args:
        window_days: Number of days before and after peak date to include

    Returns:
        Dictionary mapping (age_cohort, dose_num) to filtered lists of (person, vax_date) tuples
    """
    filtered_people: dict[Tuple[AgeCohort, int], list[Tuple[Person, datetime]]] = (
        defaultdict(list)
    )

    for (cohort, dose_num), person_date_list in people_dose_info.items():
        if (cohort, dose_num) not in peak_dates_by_cohort_dose:
            continue

        peak_date = peak_dates_by_cohort_dose[(cohort, dose_num)]
        window_start = peak_date - timedelta(days=window_days)
        window_end = peak_date + timedelta(days=window_days)

        for person, vax_date in person_date_list:
            # Ensure vax_date is a datetime for comparison
            if hasattr(vax_date, "date") and callable(vax_date.date):
                vax_datetime = vax_date
            else:
                vax_datetime = datetime.combine(vax_date, datetime.min.time())

            if window_start <= vax_datetime <= window_end:
                filtered_people[(cohort, dose_num)].append((person, vax_date))

    return filtered_people


# Apply filtering with different window sizes
print("\n" + "=" * 100)
print("FILTERING RESULTS BY DOSE")
print("=" * 100)

for window_days in [15, 30, 45, 60]:
    filtered_people_dose = filter_people_around_peak_by_dose(window_days)

    print(f"\nWindow: ±{window_days} days around peak")
    print("-" * 50)

    total_original = sum(len(person_list) for person_list in people_dose_info.values())
    total_filtered = sum(
        len(person_list) for person_list in filtered_people_dose.values()
    )

    # Group results by cohort for better readability
    cohort_results: dict[AgeCohort, dict[int, int]] = defaultdict(dict)
    for (cohort, dose_num), person_list in filtered_people_dose.items():
        cohort_results[cohort][dose_num] = len(person_list)

    for cohort in sorted(cohort_results.keys()):
        print(f"\n  {cohort}:")
        for dose_num in sorted(cohort_results[cohort].keys()):
            filtered_count = cohort_results[cohort][dose_num]
            original_count = len(people_dose_info[(cohort, dose_num)])
            retention_pct = (
                (filtered_count / original_count * 100) if original_count > 0 else 0
            )

            print(
                f"    Dose {dose_num}: {filtered_count}/{original_count} ({retention_pct:.1f}%)"
            )

    print(
        f"\nTotal retention: {total_filtered}/{total_original} ({total_filtered/total_original*100:.1f}%)"
    )

# Store the filtered results for further analysis
# Using 30-day window as default
filtered_people_dose_30day = filter_people_around_peak_by_dose(30)

print(f"\n" + "=" * 100)
print("FINAL FILTERED DATASET (30-day window) BY DOSE")
print("=" * 100)

# Group by cohort for display
cohort_final_results: dict[AgeCohort, dict[int, int]] = defaultdict(dict)
for (cohort, dose_num), person_list in filtered_people_dose_30day.items():
    cohort_final_results[cohort][dose_num] = len(person_list)

for cohort in sorted(cohort_final_results.keys()):
    print(f"\n{cohort}:")
    total_cohort = sum(cohort_final_results[cohort].values())
    print(f"  Total people: {total_cohort}")
    for dose_num in sorted(cohort_final_results[cohort].keys()):
        print(f"    Dose {dose_num}: {cohort_final_results[cohort][dose_num]} people")

# Save filtered results to pickle file for further analysis
with open("./DATACON_data/filtered_cpzp_persons_by_dose_30day.pkl", "wb") as f:
    pickle.dump(filtered_people_dose_30day, f)

print(
    f"\nFiltered dataset saved to './DATACON_data/filtered_cpzp_persons_by_dose_30day.pkl'"
)
print(
    "This dataset contains (person, vaccination_date) tuples grouped by (age_cohort, dose_number)"
)
print(
    "for people vaccinated within 30 days of peak vaccination intensity for their specific dose."
)

# Also create a summary of peak dates for reference
peak_summary_by_dose = {
    f"{cohort}_dose_{dose_num}": {
        "cohort": cohort,
        "dose_number": dose_num,
        "peak_date": stats["peak_date"],
        "peak_count": stats["peak_count"],
        "total_people": stats["unique_people"],
        "filtered_people": len(filtered_people_dose_30day.get((cohort, dose_num), [])),
        "retention_rate": (
            len(filtered_people_dose_30day.get((cohort, dose_num), []))
            / stats["unique_people"]
            * 100
            if stats["unique_people"] > 0
            else 0
        ),
    }
    for (cohort, dose_num), stats in cohort_dose_stats.items()
}

with open("./DATACON_data/peak_vaccination_summary_by_dose.pkl", "wb") as f:
    pickle.dump(peak_summary_by_dose, f)

print(
    "Peak vaccination summary by dose saved to './DATACON_data/peak_vaccination_summary_by_dose.pkl'"
)
