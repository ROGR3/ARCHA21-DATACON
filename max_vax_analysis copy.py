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
    persons: list[Person] = pickle.load(f)


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
for person in persons:
    if (
        person.vaccines and not person.died_at
    ):  # Only consider people who have been vaccinated
        # Sort vaccines by date to assign dose numbers
        sorted_vaccines = sorted(person.vaccines, key=lambda v: v.date)

        for dose_num, vaccine in enumerate(sorted_vaccines, 1):
            cohort_dose_key = (vaccine.age_cohort, dose_num)
            dose_vax_dates[cohort_dose_key].append(vaccine.date)
            people_dose_info[cohort_dose_key].append((person, vaccine.date))

print(f"Total people: {len(persons)}")
print(f"People with vaccines: {len([p for p in persons if p.vaccines])}")

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


filtered_people_dose = filter_people_around_peak_by_dose(30)


# Group by cohort for display
cohort_final_results: dict[AgeCohort, dict[int, int]] = defaultdict(dict)
for (cohort, dose_num), person_list in filtered_people_dose.items():
    cohort_final_results[cohort][dose_num] = len(person_list)

for cohort in sorted(cohort_final_results.keys()):
    print(f"\n{cohort}:")
    total_cohort = sum(cohort_final_results[cohort].values())
    print(f"  Total people: {total_cohort}")
    for dose_num in sorted(cohort_final_results[cohort].keys()):
        print(f"    Dose {dose_num}: {cohort_final_results[cohort][dose_num]} people")


# Create DataFrame with peak dates
print(f"\n" + "=" * 100)
print("PEAK DATES DATAFRAME")
print("=" * 100)

# Convert peak dates data to lists for DataFrame creation
vekova_kohorta_list = []
cislo_davky_list = []
datum_rozhodne_list = []

for (cohort, dose_num), peak_datetime in peak_dates_by_cohort_dose.items():
    vekova_kohorta_list.append(str(cohort))
    cislo_davky_list.append(dose_num)
    datum_rozhodne_list.append(peak_datetime.date())

# Create polars DataFrame
peak_dates_df = pl.DataFrame(
    {
        "Vekova_kohorta": vekova_kohorta_list,
        "Cislo_davky": cislo_davky_list,
        "Datum_rozhodne": datum_rozhodne_list,
    }
)

# Sort by age cohort and dose number for better readability
peak_dates_df = peak_dates_df.sort(["Vekova_kohorta", "Cislo_davky"])

with open("./rozhodna_data.pkl", "rb") as f:
    old_data_df = pickle.load(f)

print("CPZP-based results (max_vax_analysis.py):")
print(peak_dates_df)

print(f"\n" + "=" * 100)
print("OZP-based results (test.py):")
print(old_data_df)

# Create a detailed comparison
print(f"\n" + "=" * 100)
print("DETAILED COMPARISON")
print("=" * 100)

# Join the dataframes to compare
comparison_df = (
    peak_dates_df.join(
        old_data_df, on=["Vekova_kohorta", "Cislo_davky"], how="outer", suffix="_ozp"
    )
    .select(
        [
            "Vekova_kohorta",
            "Cislo_davky",
            pl.col("Datum_rozhodne").alias("CPZP_date"),
            pl.col("Datum_rozhodne_ozp").alias("OZP_date"),
            (pl.col("Datum_rozhodne") == pl.col("Datum_rozhodne_ozp")).alias(
                "dates_match"
            ),
        ]
    )
    .sort(["Vekova_kohorta", "Cislo_davky"])
)

print("Side-by-side comparison:")
print(comparison_df)

# Show only the differences
differences_df = comparison_df.filter(pl.col("dates_match") == False)
print(f"\n" + "=" * 60)
print("DIFFERENCES ONLY:")
print("=" * 60)
print(differences_df)

# Summary statistics
total_rows = comparison_df.height
matching_rows = comparison_df.filter(pl.col("dates_match") == True).height
different_rows = comparison_df.filter(pl.col("dates_match") == False).height

print(f"\nSummary:")
print(f"  Total combinations: {total_rows}")
print(f"  Matching dates: {matching_rows} ({100*matching_rows/total_rows:.1f}%)")
print(f"  Different dates: {different_rows} ({100*different_rows/total_rows:.1f}%)")
