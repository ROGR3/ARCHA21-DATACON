from dataclasses import dataclass
from enum import StrEnum
from datetime import datetime


class AgeCohort(StrEnum):
    LESS_THAN_12 = "less_than_12"
    BETWEEN_12_AND_30 = "between_12_and_30"
    BETWEEN_30_AND_50 = "between_30_and_50"
    BETWEEN_50_AND_70 = "between_50_and_70"
    MORE_THAN_70 = "more_than_70"


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
    lekova_forma: str | None


@dataclass
class Vaccine:
    date: datetime
    dose_number: int
    age_cohort: AgeCohort


class Gender(StrEnum):
    MALE = "male"
    FEMALE = "female"


@dataclass
class Person:
    id: int | str
    gender: Gender
    born_at: datetime
    age_cohort: AgeCohort
    vaccines: list[Vaccine]
    prescriptions: list[Prescription]
    died_at: datetime | None = None
