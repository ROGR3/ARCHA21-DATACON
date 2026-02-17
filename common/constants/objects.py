from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum


class AgeCohort(StrEnum):
    LESS_THAN_12 = "less_than_12"
    BETWEEN_12_AND_30 = "between_12_and_30"
    BETWEEN_30_AND_50 = "between_30_and_50"
    BETWEEN_50_AND_60 = "between_50_and_60"
    MORE_THAN_60 = "more_than_60"


class PrescriptionType(StrEnum):
    KORTIKOID = "kortikoid"
    IMUNOSUPRESSIVE = "imunosupressive"
    TEST = "test"


@dataclass
class Prescription:
    date: date
    latka: str
    prescription_type: PrescriptionType
    prednison_equiv: float
    equiv_sloucenina: str | None
    specializace_lekare: str | None
    atc_skupina: str | None
    lekova_forma: str | None
    lekova_forma_zkr: str | None


@dataclass
class Vaccine:
    date: date
    dose_number: int
    nazev: str | None


class Gender(StrEnum):
    MALE = "male"
    FEMALE = "female"


@dataclass
class Person:
    id: int | str
    gender: Gender
    born_at: datetime
    zahajeni_pojisteni: date
    ukonceni_pojisteni: date
    vaccines: list[Vaccine]
    prescriptions: list[Prescription]
    died_at: date | None = None
