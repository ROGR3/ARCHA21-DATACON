from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import StrEnum
from common.constants.objects import Person, Prescription


@dataclass(frozen=True)
class MatchingAnalysisConfig:
    pojistovna: str
    zacatek_pojisteni: date
    konec_pojisteni: date
    year_offset: int
    use_local_cache: bool = False

    @property
    def day_offset(self) -> timedelta:
        return timedelta(days=self.year_offset * 365)

    @property
    def start_date(self) -> datetime:
        return datetime(2021, 1, 1) - self.day_offset

    @property
    def end_date(self) -> datetime:
        return datetime(2022, 2, 28) - self.day_offset

    @property
    def year_for_age_calculation(self) -> int:
        return 2021 - self.year_offset

    @property
    def anchor_dates(self) -> list[date]:
        return [
            (self.start_date + timedelta(days=i)).date()
            for i in range((self.end_date - self.start_date).days + 1)
        ]

    @property
    def maximum_aggregation_days(self) -> int:
        return len(self.anchor_dates)


class AgeCohort(StrEnum):
    _12_15 = "12-15"
    _16_29 = "16-29"
    _30_49 = "30-49"
    _50_59 = "50-59"
    IRRELEVANT = "irrelevant"


EffectMap = dict[AgeCohort, dict[datetime, float]]


class AgeCohortCalculator:
    def __init__(self, config: MatchingAnalysisConfig):
        self.__config = config

    def calculate_age_cohort(self, person: Person) -> AgeCohort:
        age = self.__config.year_for_age_calculation - person.born_at.year
        if age >= 12 and age <= 15:
            return AgeCohort._12_15
        elif age >= 16 and age <= 29:
            return AgeCohort._16_29
        elif age >= 30 and age <= 49:
            return AgeCohort._30_49
        elif age >= 50 and age <= 59:
            return AgeCohort._50_59
        else:
            return AgeCohort.IRRELEVANT


def is_injection(pr: Prescription) -> bool:
    return pr.lekova_forma_zkr and (pr.lekova_forma_zkr.startswith("INJ"))


class PE_GROUP_NAMES(StrEnum):
    ZERO_PE = "0_PE"
    NEVER_PRESCRIBED = "NEVER_PRESCRIBED"
    ZERO_PE_SUSPECTIBLE = "ZERO_PE_SUSPECTIBLE"
    ONE_TO_FIVE_HUNDRED_PE = "1_to_500_PE"
    FIVE_HUNDRED_TO_FIVE_THOUSAND_PE = "500_to_5000_PE"


def is_zero_pe_group(group_name: PE_GROUP_NAMES) -> bool:
    return (
        group_name == PE_GROUP_NAMES.ZERO_PE
        or group_name == PE_GROUP_NAMES.NEVER_PRESCRIBED
        or group_name == PE_GROUP_NAMES.ZERO_PE_SUSPECTIBLE
    )


def create_prednison_enum(step: int = 25, max_value: int = 5000):
    values = {
        "ZERO_PE": "zero_pe",
        "ZERO_NO_PRE": "zero_no_pre",
        "ZERO_PE_SUSPECTIBLE": "zero_pe_suspectible",
        f"MORE_THAN_{max_value}": f"more_than_{max_value}",
    }

    for start in range(0, max_value, step):
        end = start + step
        name = f"BETWEEN_{start}_AND_{end}"
        values[name] = name.lower()

    return StrEnum("PREDNISON_EQUIV_CATEGORY", values)


PREDNISON_EQUIV_CATEGORY = create_prednison_enum()


def from_prednison_equiv(prednison_equiv: float) -> PREDNISON_EQUIV_CATEGORY:
    if prednison_equiv == 0:
        return PREDNISON_EQUIV_CATEGORY.ZERO_PE

    step = 25
    max_value = 5000

    if prednison_equiv >= max_value:
        return PREDNISON_EQUIV_CATEGORY[f"MORE_THAN_{max_value}"]

    start = int((prednison_equiv // step) * step)
    end = start + step
    name = f"BETWEEN_{start}_AND_{end}"
    return PREDNISON_EQUIV_CATEGORY[name]


def has_prescriptions_before_date(person: Person, anchor_date: datetime) -> bool:
    return any(pr.date < anchor_date for pr in person.prescriptions)


def sum_after_date_pe_for_person(person: Person, ddate: datetime) -> float:
    return sum(
        pr.prednison_equiv
        for pr in person.prescriptions
        if (not is_injection(pr))
        and (pr.date > ddate and pr.date < ddate + timedelta(days=365))
    )


def sum_before_date_pe_for_person(person: Person, ddate: datetime) -> float:
    return sum(
        pr.prednison_equiv
        for pr in person.prescriptions
        if (not is_injection(pr))
        and (pr.date > ddate - timedelta(days=365) and pr.date < ddate)
    )
