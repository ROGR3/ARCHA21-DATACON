from dataclasses import dataclass
from enum import StrEnum
from datetime import datetime
import polars as pl
from collections import defaultdict
from common.constants.column_types import (
    CPZP_SCHEMA,
    OZP_SCHEMA,
    POHLAVI_CPZP,
    TYP_UDALOSTI,
)
from common.constants.column_names import SHARED_COLUMNS, OZP_COLUMNS, CPZP_COLUMNS

pl.Config.set_tbl_rows(20)
pl.Config.set_tbl_cols(60)

FILE_PATH = "./DATACON_data/CPZP_preskladane.csv"


def read_preskladane_data(file_path: str, schema: pl.Schema) -> pl.DataFrame:
    return pl.read_csv(
        file_path,
        null_values=["NA", ""],
        schema=schema,
    )


df = read_preskladane_data(FILE_PATH, CPZP_SCHEMA)


@dataclass
class Prescription:
    date: datetime
    latka: str
    equiv_sloucenina: str | None
    prednison_equiv: float | None


class Gender(StrEnum):
    MALE = "male"
    FEMALE = "female"


@dataclass
class Person:
    id: int
    gender: Gender
    born_at: datetime | None
    vaccines: list[datetime]
    prescriptions: list[Prescription]
    died_at: datetime | None = None


def convert_gender(pohlavi: str) -> Gender:
    """Convert CPZP gender code to Gender enum"""
    if pohlavi == POHLAVI_CPZP.MUZ:
        return Gender.MALE
    elif pohlavi == POHLAVI_CPZP.ZENA:
        return Gender.FEMALE
    else:
        raise ValueError(f"Unknown gender: {pohlavi}")


def create_birth_date(year: int, month: int | None) -> datetime | None:
    """Create birth date from year and month"""
    if year is None:
        return None
    # Use month 1 if month is None/missing
    month = month if month is not None else 1
    try:
        return datetime(year, month, 1)
    except (ValueError, TypeError):
        return datetime(year, 1, 1)  # Fallback to January 1st


def df_to_persons(df: pl.DataFrame) -> list[Person]:
    """Convert dataframe to list of Person objects - optimized version"""
    persons = []

    # Get unique person info efficiently
    person_info = df.group_by(SHARED_COLUMNS.ID_POJISTENCE.value).agg(
        [
            pl.first(SHARED_COLUMNS.POHLAVI.value).alias("gender"),
            pl.first(SHARED_COLUMNS.ROK_NAROZENI.value).alias("birth_year"),
            pl.first(CPZP_COLUMNS.MESIC_NAROZENI.value).alias("birth_month"),
            pl.first(SHARED_COLUMNS.DATUM_UMRTI.value).alias("death_date"),
        ]
    )

    # Get prescriptions efficiently
    prescriptions_df = (
        df.filter(pl.col(SHARED_COLUMNS.TYP_UDALOSTI.value) == TYP_UDALOSTI.PREDPIS)
        .filter(pl.col(SHARED_COLUMNS.DATUM_UDALOSTI.value).is_not_null())
        .group_by(SHARED_COLUMNS.ID_POJISTENCE.value)
        .agg(
            [
                pl.col(SHARED_COLUMNS.DATUM_UDALOSTI.value).alias("prescription_dates"),
                pl.col(SHARED_COLUMNS.LECIVE_LATKY.value).alias("latka"),
                pl.col(SHARED_COLUMNS.EQUIV_SLOUCENINA.value).alias("equiv_sloucenina"),
                pl.col(SHARED_COLUMNS.PREDNISON_EQUIV.value).alias("prednison_equiv"),
            ]
        )
    )

    # Get vaccinations efficiently
    vaccines_df = (
        df.filter(pl.col(SHARED_COLUMNS.TYP_UDALOSTI.value) == TYP_UDALOSTI.VAKCINACE)
        .filter(pl.col(SHARED_COLUMNS.DATUM_UDALOSTI.value).is_not_null())
        .group_by(SHARED_COLUMNS.ID_POJISTENCE.value)
        .agg([pl.col(SHARED_COLUMNS.DATUM_UDALOSTI.value).alias("vaccine_dates")])
    )

    # Join all the data together
    combined = person_info.join(
        prescriptions_df, on=SHARED_COLUMNS.ID_POJISTENCE.value, how="left"
    ).join(vaccines_df, on=SHARED_COLUMNS.ID_POJISTENCE.value, how="left")

    # Convert to Person objects
    for row in combined.iter_rows(named=True):
        person_id = int(row[SHARED_COLUMNS.ID_POJISTENCE.value])

        # Convert gender
        gender_code = row["gender"]
        gender = Gender.MALE if gender_code == POHLAVI_CPZP.MUZ else Gender.FEMALE

        # Create birth date
        birth_year = row["birth_year"]
        birth_month = row["birth_month"]
        born_at = create_birth_date(birth_year, birth_month)

        # Death date
        died_at = row["death_date"]

        # Process prescriptions
        prescriptions = []
        prescription_dates = row["prescription_dates"] or []
        latka = row["latka"] or []
        equiv_sloucenina = row["equiv_sloucenina"] or []
        prednison_equiv = row["prednison_equiv"] or []

        for i, date in enumerate(prescription_dates):
            prescriptions.append(
                Prescription(
                    date=date,
                    latka=latka[i],
                    equiv_sloucenina=equiv_sloucenina[i],
                    prednison_equiv=prednison_equiv[i],
                )
            )

        # Process vaccines
        vaccines = row["vaccine_dates"] or []

        # Create Person object
        person = Person(
            id=person_id,
            gender=gender,
            born_at=born_at,
            vaccines=list(vaccines),
            prescriptions=prescriptions,
            died_at=died_at,
        )

        persons.append(person)

    return persons


# Convert dataframe to list of Person objects
persons = df_to_persons(df)
print(f"Converted {len(persons)} persons from dataframe")
print(f"Sample person: {persons[0] if persons else 'No persons found'}")
