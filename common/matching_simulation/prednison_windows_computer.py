from common.constants.objects import Gender, Person
from datetime import datetime, timedelta
from bisect import bisect_left, bisect_right

from common.matching_simulation.utils import (
    PREDNISON_EQUIV_CATEGORY,
    MatchingAnalysisConfig,
    is_injection,
    has_prescriptions_before_date,
)
from common.matching_simulation.utils import (
    from_prednison_equiv,
    AgeCohort,
    AgeCohortCalculator,
)
import pickle


class PrednisonWindowsComputer:
    def __init__(
        self, config: MatchingAnalysisConfig, age_cohort_calculator: AgeCohortCalculator
    ):
        self.__config = config
        self.__age_cohort_calculator = age_cohort_calculator

    def get_prednison_windows(
        self, people: list[Person], vax_anchor_dates: list[datetime]
    ) -> dict[datetime, dict[str | int, float]]:
        if self.__config.use_local_cache:
            return self.__get_prednison_windows_from_local_cache()
        else:
            return self.__compute_prednison_windows(people, vax_anchor_dates)

    def __get_prednison_windows_from_local_cache(
        self,
    ) -> dict[datetime, dict[str | int, float]]:
        with open(f"{self.__config.pojistovna.lower()}_pe_map.pkl", "rb") as f:
            pe_map = pickle.load(f)

        return pe_map

    def __compute_prednison_windows(
        self, people: list[Person], vax_anchor_dates: list[datetime]
    ) -> dict[datetime, dict[str | int, float]]:
        # Prepare output structure
        result: dict[
            datetime,
            dict[PREDNISON_EQUIV_CATEGORY, dict[AgeCohort, dict[Gender, set[int]]]],
        ] = {ad: {} for ad in vax_anchor_dates}

        completed_persons = 0
        len_of_persons_to_analyse = len(people)
        for person in people:
            completed_persons += 1
            print(
                f"Processing person {completed_persons}/{len_of_persons_to_analyse}",
                end="\r",
            )

            person_prescriptions = [
                pr
                for pr in sorted(person.prescriptions, key=lambda pr: pr.date)
                if not (is_injection(pr))
            ]
            if not person_prescriptions:
                for anchor in vax_anchor_dates:
                    self.__add_person_to_result(
                        result, anchor, PREDNISON_EQUIV_CATEGORY.ZERO_PE, person
                    )
                    if has_prescriptions_before_date(person, anchor):
                        self.__add_person_to_result(
                            result,
                            anchor,
                            PREDNISON_EQUIV_CATEGORY.ZERO_PE_SUSPECTIBLE,
                            person,
                        )
                    else:
                        self.__add_person_to_result(
                            result,
                            anchor,
                            PREDNISON_EQUIV_CATEGORY.ZERO_NO_PRE,
                            person,
                        )
                continue

            # For accumulation:
            # Each anchor date holds its sum of prednison for this person
            per_person_map: dict[datetime, float] = {ad: 0.0 for ad in vax_anchor_dates}

            # 3) For each prescription, add its value to all anchor dates
            #    within pr.date → pr.date + 365 days
            for pr in person_prescriptions:
                start = pr.date
                end = pr.date + timedelta(days=365)

                # indices of anchor dates inside [start, end]
                i = bisect_left(vax_anchor_dates, start)
                j = bisect_right(vax_anchor_dates, end)

                # Add prednison_equiv to each matching anchor date
                for idx in range(i, j):
                    anchor = vax_anchor_dates[idx]
                    per_person_map[anchor] += pr.prednison_equiv

            # Store per-person results
            for anchor, value in per_person_map.items():
                if value == 0:
                    self.__add_person_to_result(
                        result, anchor, PREDNISON_EQUIV_CATEGORY.ZERO_PE, person
                    )
                    if has_prescriptions_before_date(person, anchor):
                        self.__add_person_to_result(
                            result,
                            anchor,
                            PREDNISON_EQUIV_CATEGORY.ZERO_PE_SUSPECTIBLE,
                            person,
                        )
                    else:
                        self.__add_person_to_result(
                            result,
                            anchor,
                            PREDNISON_EQUIV_CATEGORY.ZERO_NO_PRE,
                            person,
                        )
                else:
                    pe_range = from_prednison_equiv(value)
                    self.__add_person_to_result(result, anchor, pe_range, person)

        for anchor, ranges in result.items():
            for pe_range, acs in ranges.items():
                for ac, genders in acs.items():
                    for gender, idset in genders.items():
                        genders[gender] = list(idset)

        return result

    def __add_person_to_result(
        self,
        result: dict[
            datetime,
            dict[PREDNISON_EQUIV_CATEGORY, dict[AgeCohort, dict[Gender, set[int]]]],
        ],
        anchor: datetime,
        pe_range: PREDNISON_EQUIV_CATEGORY,
        person: Person,
    ):
        if pe_range not in result[anchor]:
            result[anchor][pe_range] = {}

        ac = self.__age_cohort_calculator.calculate_age_cohort(person)
        if ac not in result[anchor][pe_range]:
            result[anchor][pe_range][ac] = {}

        if person.gender not in result[anchor][pe_range][ac]:
            result[anchor][pe_range][ac][person.gender] = set()

        result[anchor][pe_range][ac][person.gender].add(person.id)
