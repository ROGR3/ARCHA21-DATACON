import pickle
from dataclasses import dataclass
from enum import StrEnum
from datetime import datetime


class AgeCohort(StrEnum):
    LESS_THAN_12 = "less_than_12"
    BETWEEN_12_AND_16 = "between_12_and_16"
    BETWEEN_16_AND_30 = "between_16_and_30"
    BETWEEN_30_AND_35 = "between_30_and_35"
    BETWEEN_35_AND_40 = "between_35_and_40"
    BETWEEN_40_AND_45 = "between_40_and_45"
    BETWEEN_45_AND_50 = "between_45_and_50"
    BETWEEN_50_AND_55 = "between_50_and_55"
    BETWEEN_55_AND_60 = "between_55_and_60"
    BETWEEN_60_AND_65 = "between_60_and_65"
    BETWEEN_65_AND_70 = "between_65_and_70"
    BETWEEN_70_AND_75 = "between_70_and_75"
    BETWEEN_75_AND_80 = "between_75_and_80"
    MORE_THAN_80 = "more_than_80"


class PrescriptionType(StrEnum):
    KORTIKOID = "kortikoid"
    IMUNOSUPRESSIVE = "imunosupressive"


@dataclass
class Prescription:
    date: datetime
    latka: str
    age_cohort_at_prescription: AgeCohort
    prescription_type: PrescriptionType
    equiv_sloucenina: str | None
    prednison_equiv: float | None
    specializace_lekare: str | None
    atc_skupina: str | None


class Gender(StrEnum):
    MALE = "male"
    FEMALE = "female"


@dataclass
class Person:
    id: int | str
    gender: Gender
    born_at: datetime
    age_cohort: AgeCohort
    vaccines: list[datetime]
    prescriptions: list[Prescription]
    died_at: datetime | None = None
