import pickle
import polars as pl
from common.constants.column_types import (
    CPZP_SCHEMA,
    OZP_SCHEMA,
    TYP_UDALOSTI,
)
from common.constants.column_names import SHARED_COLUMNS, OZP_COLUMNS, CPZP_COLUMNS
import matplotlib.pyplot as plt
import numpy as np
from common.constants.objects import (
    AgeCohort,
    Gender,
    PrescriptionType,
    Prescription,
    Person,
    Vaccine,
)
from datetime import datetime

pl.Config.set_tbl_rows(20)
pl.Config.set_tbl_cols(60)


def read_preskladane_data(file_path: str, schema: pl.Schema) -> pl.DataFrame:
    return pl.read_csv(
        file_path,
        null_values=["NA", ""],
        schema=schema,
    )


data = read_preskladane_data("./DATACON_data/CPZP_preskladane.csv", CPZP_SCHEMA)

# Filter for predpis rows only
predpis_rows = data.filter(pl.col(SHARED_COLUMNS.TYP_UDALOSTI) == TYP_UDALOSTI.PREDPIS)

# Check for rows where ATC_skupina is null/missing
missing_atc = predpis_rows.filter(pl.col(SHARED_COLUMNS.ATC_SKUPINA).is_null())

print(f"Total predpis rows: {len(predpis_rows)}")
print(f"Predpis rows with missing ATC_skupina: {len(missing_atc)}")
print(f"Percentage missing: {len(missing_atc) / len(predpis_rows) * 100:.2f}%")

print("\n" + "=" * 80)
print("DETAILED ANALYSIS OF MISSING ATC_SKUPINA")
print("=" * 80)

# Check unique Detail_udalosti values in missing rows
print("\nUnique Detail_udalosti values in missing rows:")
print(missing_atc.select(SHARED_COLUMNS.DETAIL_UDALOSTI).unique())

# Check other medication-related fields
print("\nChecking other medication fields in missing rows:")
relevant_cols = [
    SHARED_COLUMNS.DETAIL_UDALOSTI,
    SHARED_COLUMNS.LEKOVA_FORMA_ZKR,
    SHARED_COLUMNS.SILA,
    SHARED_COLUMNS.LEKOVA_FORMA,
    SHARED_COLUMNS.LECIVE_LATKY,
    SHARED_COLUMNS.EQUIV_SLOUCENINA,
    CPZP_COLUMNS.SPECIALIZACE,
]
print(missing_atc.select(relevant_cols).head(10))

# Count by Specializace (medical specialization)
print("\nDistribution by Specializace:")
print(
    missing_atc.group_by(CPZP_COLUMNS.SPECIALIZACE)
    .count()
    .sort("count", descending=True)
)
