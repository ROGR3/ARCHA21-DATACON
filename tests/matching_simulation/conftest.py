"""Shared fixtures and factory helpers for matching_simulation tests."""

from datetime import date, datetime

import pytest

from common.constants.objects import (
    Gender,
    Person,
    Prescription,
    PrescriptionType,
    Vaccine,
)
from common.matching_simulation.utils import AgeCohortCalculator, MatchingAnalysisConfig


# ---------------------------------------------------------------------------
# Factory helpers
# ---------------------------------------------------------------------------


def make_prescription(
    dt: date,
    pe: float = 10.0,
    *,
    injection: bool = False,
    latka: str = "prednison",
) -> Prescription:
    """Create a Prescription with sensible defaults."""
    return Prescription(
        date=dt,
        latka=latka,
        prescription_type=PrescriptionType.KORTIKOID,
        prednison_equiv=pe,
        equiv_sloucenina=None,
        specializace_lekare=None,
        atc_skupina=None,
        lekova_forma="Injekce" if injection else "Tableta",
        lekova_forma_zkr="INJ" if injection else "TBL",
    )


def make_vaccine(dt: date, dose: int = 1) -> Vaccine:
    """Create a Vaccine with sensible defaults."""
    return Vaccine(date=dt, dose_number=dose, nazev="Comirnaty")


def make_person(
    pid: int | str = 1,
    gender: Gender = Gender.MALE,
    born_year: int = 1985,
    vaccines: list[Vaccine] | None = None,
    prescriptions: list[Prescription] | None = None,
    died_at: datetime | None = None,
    zahajeni: datetime | None = None,
    ukonceni: datetime | None = None,
) -> Person:
    """Create a Person with sensible defaults.

    born_year=1985 → age 36 in 2021 → AgeCohort._30_39
    """
    return Person(
        id=pid,
        gender=gender,
        born_at=datetime(born_year, 6, 15),
        zahajeni_pojisteni=zahajeni or datetime(2018, 1, 1),
        ukonceni_pojisteni=ukonceni or datetime(2050, 12, 31),
        vaccines=vaccines or [],
        prescriptions=prescriptions or [],
        died_at=died_at,
    )


# ---------------------------------------------------------------------------
# Reusable config fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def default_config() -> MatchingAnalysisConfig:
    """Config with year_offset=0 covering 2021-01-01 → 2022-02-28."""
    return MatchingAnalysisConfig(
        pojistovna="test",
        zacatek_pojisteni=date(2020, 1, 1),
        konec_pojisteni=date(2023, 1, 1),
        year_offset=0,
        use_local_cache=False,
    )


@pytest.fixture
def age_calculator(default_config) -> AgeCohortCalculator:
    return AgeCohortCalculator(default_config)


@pytest.fixture
def short_config() -> MatchingAnalysisConfig:
    """Config with a very narrow date range for focused tests."""
    return MatchingAnalysisConfig(
        pojistovna="test",
        zacatek_pojisteni=date(2020, 1, 1),
        konec_pojisteni=date(2023, 1, 1),
        year_offset=0,
        use_local_cache=False,
    )
