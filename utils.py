from datetime import date, datetime, timedelta
from enum import StrEnum
import random
from common.constants.objects import Gender, Person, Prescription


class AgeCohort(StrEnum):
    _12_15 = "12-15"
    _16_29 = "16-29"
    _30_49 = "30-49"
    _50_59 = "50-59"
    IRRELEVANT = "irrelevant"


def calculate_age_cohort(person: Person) -> AgeCohort:
    YEAR_FOR_AGE_CALCULATION = 2021

    age = YEAR_FOR_AGE_CALCULATION - person.born_at.year
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
    return bool(pr.lekova_forma_zkr and (pr.lekova_forma_zkr.startswith("INJ")))


class PREDNISON_EQUIV_CATEGORY(StrEnum):
    ZERO = "zero"
    BETWEEN_0_AND_25 = "between_0_and_25"
    BETWEEN_25_AND_50 = "between_25_and_50"
    BETWEEN_50_AND_75 = "between_50_and_75"
    BETWEEN_75_AND_100 = "between_75_and_100"
    BETWEEN_100_AND_125 = "between_100_and_125"
    BETWEEN_125_AND_150 = "between_125_and_150"
    BETWEEN_150_AND_175 = "between_150_and_175"
    BETWEEN_175_AND_200 = "between_175_and_200"
    BETWEEN_200_AND_225 = "between_200_and_225"
    BETWEEN_225_AND_250 = "between_225_and_250"
    BETWEEN_250_AND_275 = "between_250_and_275"
    BETWEEN_275_AND_300 = "between_275_and_300"
    BETWEEN_300_AND_325 = "between_300_and_325"
    BETWEEN_325_AND_350 = "between_325_and_350"
    BETWEEN_350_AND_375 = "between_350_and_375"
    BETWEEN_375_AND_400 = "between_375_and_400"
    BETWEEN_400_AND_425 = "between_400_and_425"
    BETWEEN_425_AND_450 = "between_425_and_450"
    BETWEEN_450_AND_475 = "between_450_and_475"
    BETWEEN_475_AND_500 = "between_475_and_500"
    BETWEEN_500_AND_525 = "between_500_and_525"
    BETWEEN_525_AND_550 = "between_525_and_550"
    BETWEEN_550_AND_575 = "between_550_and_575"
    BETWEEN_575_AND_600 = "between_575_and_600"
    BETWEEN_600_AND_625 = "between_600_and_625"
    BETWEEN_625_AND_650 = "between_625_and_650"
    BETWEEN_650_AND_675 = "between_650_and_675"
    BETWEEN_675_AND_700 = "between_675_and_700"
    BETWEEN_700_AND_725 = "between_700_and_725"
    BETWEEN_725_AND_750 = "between_725_and_750"
    BETWEEN_750_AND_775 = "between_750_and_775"
    BETWEEN_775_AND_800 = "between_775_and_800"
    BETWEEN_800_AND_825 = "between_800_and_825"
    BETWEEN_825_AND_850 = "between_825_and_850"
    BETWEEN_850_AND_875 = "between_850_and_875"
    BETWEEN_875_AND_900 = "between_875_and_900"
    BETWEEN_900_AND_925 = "between_900_and_925"
    BETWEEN_925_AND_950 = "between_925_and_950"
    BETWEEN_950_AND_975 = "between_950_and_975"
    BETWEEN_975_AND_1000 = "between_975_and_1000"
    BETWEEN_1000_AND_1025 = "between_1000_and_1025"
    BETWEEN_1025_AND_1050 = "between_1025_and_1050"
    BETWEEN_1050_AND_1075 = "between_1050_and_1075"
    BETWEEN_1075_AND_1100 = "between_1075_and_1100"
    BETWEEN_1100_AND_1125 = "between_1100_and_1125"
    BETWEEN_1125_AND_1150 = "between_1125_and_1150"
    BETWEEN_1150_AND_1175 = "between_1150_and_1175"
    BETWEEN_1175_AND_1200 = "between_1175_and_1200"
    BETWEEN_1200_AND_1225 = "between_1200_and_1225"
    BETWEEN_1225_AND_1250 = "between_1225_and_1250"
    BETWEEN_1250_AND_1275 = "between_1250_and_1275"
    BETWEEN_1275_AND_1300 = "between_1275_and_1300"
    BETWEEN_1300_AND_1325 = "between_1300_and_1325"
    BETWEEN_1325_AND_1350 = "between_1325_and_1350"
    BETWEEN_1350_AND_1375 = "between_1350_and_1375"
    BETWEEN_1375_AND_1400 = "between_1375_and_1400"
    BETWEEN_1400_AND_1425 = "between_1400_and_1425"
    BETWEEN_1425_AND_1450 = "between_1425_and_1450"
    BETWEEN_1450_AND_1475 = "between_1450_and_1475"
    BETWEEN_1475_AND_1500 = "between_1475_and_1500"
    BETWEEN_1500_AND_1525 = "between_1500_and_1525"
    BETWEEN_1525_AND_1550 = "between_1525_and_1550"
    BETWEEN_1550_AND_1575 = "between_1550_and_1575"
    BETWEEN_1575_AND_1600 = "between_1575_and_1600"
    BETWEEN_1600_AND_1625 = "between_1600_and_1625"
    BETWEEN_1625_AND_1650 = "between_1625_and_1650"
    BETWEEN_1650_AND_1675 = "between_1650_and_1675"
    BETWEEN_1675_AND_1700 = "between_1675_and_1700"
    BETWEEN_1700_AND_1725 = "between_1700_and_1725"
    BETWEEN_1725_AND_1750 = "between_1725_and_1750"
    BETWEEN_1750_AND_1775 = "between_1750_and_1775"
    BETWEEN_1775_AND_1800 = "between_1775_and_1800"
    BETWEEN_1800_AND_1825 = "between_1800_and_1825"
    BETWEEN_1825_AND_1850 = "between_1825_and_1850"
    BETWEEN_1850_AND_1875 = "between_1850_and_1875"
    BETWEEN_1875_AND_1900 = "between_1875_and_1900"
    BETWEEN_1900_AND_1925 = "between_1900_and_1925"
    BETWEEN_1925_AND_1950 = "between_1925_and_1950"
    BETWEEN_1950_AND_1975 = "between_1950_and_1975"
    BETWEEN_1975_AND_2000 = "between_1975_and_2000"
    BETWEEN_2000_AND_2025 = "between_2000_and_2025"
    BETWEEN_2025_AND_2050 = "between_2025_and_2050"
    BETWEEN_2050_AND_2075 = "between_2050_and_2075"
    BETWEEN_2075_AND_2100 = "between_2075_and_2100"
    BETWEEN_2100_AND_2125 = "between_2100_and_2125"
    BETWEEN_2125_AND_2150 = "between_2125_and_2150"
    BETWEEN_2150_AND_2175 = "between_2150_and_2175"
    BETWEEN_2175_AND_2200 = "between_2175_and_2200"
    BETWEEN_2200_AND_2225 = "between_2200_and_2225"
    BETWEEN_2225_AND_2250 = "between_2225_and_2250"
    BETWEEN_2250_AND_2275 = "between_2250_and_2275"
    BETWEEN_2275_AND_2300 = "between_2275_and_2300"
    BETWEEN_2300_AND_2325 = "between_2300_and_2325"
    BETWEEN_2325_AND_2350 = "between_2325_and_2350"
    BETWEEN_2350_AND_2375 = "between_2350_and_2375"
    BETWEEN_2375_AND_2400 = "between_2375_and_2400"
    BETWEEN_2400_AND_2425 = "between_2400_and_2425"
    BETWEEN_2425_AND_2450 = "between_2425_and_2450"
    BETWEEN_2450_AND_2475 = "between_2450_and_2475"
    BETWEEN_2475_AND_2500 = "between_2475_and_2500"
    BETWEEN_2500_AND_2525 = "between_2500_and_2525"
    BETWEEN_2525_AND_2550 = "between_2525_and_2550"
    BETWEEN_2550_AND_2575 = "between_2550_and_2575"
    BETWEEN_2575_AND_2600 = "between_2575_and_2600"
    BETWEEN_2600_AND_2625 = "between_2600_and_2625"
    BETWEEN_2625_AND_2650 = "between_2625_and_2650"
    BETWEEN_2650_AND_2675 = "between_2650_and_2675"
    BETWEEN_2675_AND_2700 = "between_2675_and_2700"
    BETWEEN_2700_AND_2725 = "between_2700_and_2725"
    BETWEEN_2725_AND_2750 = "between_2725_and_2750"
    BETWEEN_2750_AND_2775 = "between_2750_and_2775"
    BETWEEN_2775_AND_2800 = "between_2775_and_2800"
    BETWEEN_2800_AND_2825 = "between_2800_and_2825"
    BETWEEN_2825_AND_2850 = "between_2825_and_2850"
    BETWEEN_2850_AND_2875 = "between_2850_and_2875"
    BETWEEN_2875_AND_2900 = "between_2875_and_2900"
    BETWEEN_2900_AND_2925 = "between_2900_and_2925"
    BETWEEN_2925_AND_2950 = "between_2925_and_2950"
    BETWEEN_2950_AND_2975 = "between_2950_and_2975"
    BETWEEN_2975_AND_3000 = "between_2975_and_3000"
    BETWEEN_3000_AND_3025 = "between_3000_and_3025"
    BETWEEN_3025_AND_3050 = "between_3025_and_3050"
    BETWEEN_3050_AND_3075 = "between_3050_and_3075"
    BETWEEN_3075_AND_3100 = "between_3075_and_3100"
    BETWEEN_3100_AND_3125 = "between_3100_and_3125"
    BETWEEN_3125_AND_3150 = "between_3125_and_3150"
    BETWEEN_3150_AND_3175 = "between_3150_and_3175"
    BETWEEN_3175_AND_3200 = "between_3175_and_3200"
    BETWEEN_3200_AND_3225 = "between_3200_and_3225"
    BETWEEN_3225_AND_3250 = "between_3225_and_3250"
    BETWEEN_3250_AND_3275 = "between_3250_and_3275"
    BETWEEN_3275_AND_3300 = "between_3275_and_3300"
    BETWEEN_3300_AND_3325 = "between_3300_and_3325"
    BETWEEN_3325_AND_3350 = "between_3325_and_3350"
    BETWEEN_3350_AND_3375 = "between_3350_and_3375"
    BETWEEN_3375_AND_3400 = "between_3375_and_3400"
    BETWEEN_3400_AND_3425 = "between_3400_and_3425"
    BETWEEN_3425_AND_3450 = "between_3425_and_3450"
    BETWEEN_3450_AND_3475 = "between_3450_and_3475"
    BETWEEN_3475_AND_3500 = "between_3475_and_3500"
    BETWEEN_3500_AND_3525 = "between_3500_and_3525"
    BETWEEN_3525_AND_3550 = "between_3525_and_3550"
    BETWEEN_3550_AND_3575 = "between_3550_and_3575"
    BETWEEN_3575_AND_3600 = "between_3575_and_3600"
    BETWEEN_3600_AND_3625 = "between_3600_and_3625"
    BETWEEN_3625_AND_3650 = "between_3625_and_3650"
    BETWEEN_3650_AND_3675 = "between_3650_and_3675"
    BETWEEN_3675_AND_3700 = "between_3675_and_3700"
    BETWEEN_3700_AND_3725 = "between_3700_and_3725"
    BETWEEN_3725_AND_3750 = "between_3725_and_3750"
    BETWEEN_3750_AND_3775 = "between_3750_and_3775"
    BETWEEN_3775_AND_3800 = "between_3775_and_3800"
    BETWEEN_3800_AND_3825 = "between_3800_and_3825"
    BETWEEN_3825_AND_3850 = "between_3825_and_3850"
    BETWEEN_3850_AND_3875 = "between_3850_and_3875"
    BETWEEN_3875_AND_3900 = "between_3875_and_3900"
    BETWEEN_3900_AND_3925 = "between_3900_and_3925"
    BETWEEN_3925_AND_3950 = "between_3925_and_3950"
    BETWEEN_3950_AND_3975 = "between_3950_and_3975"
    BETWEEN_3975_AND_4000 = "between_3975_and_4000"
    BETWEEN_4000_AND_4025 = "between_4000_and_4025"
    BETWEEN_4025_AND_4050 = "between_4025_and_4050"
    BETWEEN_4050_AND_4075 = "between_4050_and_4075"
    BETWEEN_4075_AND_4100 = "between_4075_and_4100"
    BETWEEN_4100_AND_4125 = "between_4100_and_4125"
    BETWEEN_4125_AND_4150 = "between_4125_and_4150"
    BETWEEN_4150_AND_4175 = "between_4150_and_4175"
    BETWEEN_4175_AND_4200 = "between_4175_and_4200"
    BETWEEN_4200_AND_4225 = "between_4200_and_4225"
    BETWEEN_4225_AND_4250 = "between_4225_and_4250"
    BETWEEN_4250_AND_4275 = "between_4250_and_4275"
    BETWEEN_4275_AND_4300 = "between_4275_and_4300"
    BETWEEN_4300_AND_4325 = "between_4300_and_4325"
    BETWEEN_4325_AND_4350 = "between_4325_and_4350"
    BETWEEN_4350_AND_4375 = "between_4350_and_4375"
    BETWEEN_4375_AND_4400 = "between_4375_and_4400"
    BETWEEN_4400_AND_4425 = "between_4400_and_4425"
    BETWEEN_4425_AND_4450 = "between_4425_and_4450"
    BETWEEN_4450_AND_4475 = "between_4450_and_4475"
    BETWEEN_4475_AND_4500 = "between_4475_and_4500"
    BETWEEN_4500_AND_4525 = "between_4500_and_4525"
    BETWEEN_4525_AND_4550 = "between_4525_and_4550"
    BETWEEN_4550_AND_4575 = "between_4550_and_4575"
    BETWEEN_4575_AND_4600 = "between_4575_and_4600"
    BETWEEN_4600_AND_4625 = "between_4600_and_4625"
    BETWEEN_4625_AND_4650 = "between_4625_and_4650"
    BETWEEN_4650_AND_4675 = "between_4650_and_4675"
    BETWEEN_4675_AND_4700 = "between_4675_and_4700"
    BETWEEN_4700_AND_4725 = "between_4700_and_4725"
    BETWEEN_4725_AND_4750 = "between_4725_and_4750"
    BETWEEN_4750_AND_4775 = "between_4750_and_4775"
    BETWEEN_4775_AND_4800 = "between_4775_and_4800"
    BETWEEN_4800_AND_4825 = "between_4800_and_4825"
    BETWEEN_4825_AND_4850 = "between_4825_and_4850"
    BETWEEN_4850_AND_4875 = "between_4850_and_4875"
    BETWEEN_4875_AND_4900 = "between_4875_and_4900"
    BETWEEN_4900_AND_4925 = "between_4900_and_4925"
    BETWEEN_4925_AND_4950 = "between_4925_and_4950"
    BETWEEN_4950_AND_4975 = "between_4950_and_4975"
    BETWEEN_4975_AND_5000 = "between_4975_and_5000"
    MORE_THAN_5000 = "more_than_5000"


PREDNISON_WINDOWS_MAP_TYPE = dict[
    date,
    dict[PREDNISON_EQUIV_CATEGORY, dict[AgeCohort, dict[Gender, list[str | int]]]],
]


def from_prednison_equiv(prednison_equiv: float) -> PREDNISON_EQUIV_CATEGORY:
    if prednison_equiv == 0:
        return PREDNISON_EQUIV_CATEGORY.ZERO

    step = 25
    max_value = 5000

    if prednison_equiv >= max_value:
        return PREDNISON_EQUIV_CATEGORY[f"MORE_THAN_{max_value}"]

    start = int((prednison_equiv // step) * step)
    end = start + step
    name = f"BETWEEN_{start}_AND_{end}"
    return PREDNISON_EQUIV_CATEGORY[name]


def find_matching_person(
    vax_person: Person,
    pe_range: PREDNISON_EQUIV_CATEGORY,
    vax_date: datetime,
    novax_map: PREDNISON_WINDOWS_MAP_TYPE,
) -> str | int:
    ac = calculate_age_cohort(vax_person)
    gender = vax_person.gender
    novax_matched_ids = novax_map[vax_date][pe_range][ac][gender]
    return random.choice(novax_matched_ids)


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
