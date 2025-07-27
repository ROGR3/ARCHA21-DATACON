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
import matplotlib.pyplot as plt
import numpy as np
import os

pl.Config.set_tbl_rows(20)
pl.Config.set_tbl_cols(60)


def moving_average(data, window_size=7):
    return np.convolve(data, np.ones(window_size) / window_size, mode="valid")


with open("./DATACON_data/cpzp_persons.pkl", "rb") as f:
    persons: list[Person] = pickle.load(f)

vax_dates_map: dict[AgeCohort, dict[int, list[datetime]]] = defaultdict(dict)

for person in persons:
    if person.died_at or not person.vaccines:
        continue
    for vax in person.vaccines:
        if vax.dose_number not in vax_dates_map[vax.age_cohort]:
            vax_dates_map[vax.age_cohort][vax.dose_number] = []
        vax_dates_map[vax.age_cohort][vax.dose_number].append(vax.date)

max_vax_dates_map: dict[AgeCohort, dict[int, datetime]] = defaultdict(dict)

for age_cohort, doses_map in vax_dates_map.items():
    for dose_number, dates in doses_map.items():
        if not dates:
            continue

        max_date, _ = max(Counter(dates).items(), key=lambda x: x[1])
        max_vax_dates_map[age_cohort][dose_number] = max_date

print("Max vaccination dates by cohort:")
print(max_vax_dates_map)

# Create charts for each cohort and dose combination
print("\nGenerating charts for each cohort and dose...")


for age_cohort in vax_dates_map:
    for dose_number, dates in vax_dates_map[age_cohort].items():
        if not dates:
            continue

        days_to_counts = {}
        for date, count in Counter(dates).items():
            days_to_counts[date] = count

        draw_chart(
            mapp=days_to_counts,
            x_label="Days since first vaccination in this dose",
            y_label="Number of vaccinations",
            title=f"Vaccination Timeline - {age_cohort.value} - Dose {dose_number}",
            save_location=f"out/vax_period/{age_cohort.value}_dose_{dose_number}.png",
        )
