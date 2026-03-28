from common.constants.objects import Person
from datetime import date, timedelta
from bisect import bisect_left, bisect_right

from common.matching_simulation.utils import (
    PREDNISON_EQUIV_CATEGORY,
    MatchingAnalysisConfig,
    PeMap,
    PeMapInternal,
    is_valid_prescription,
    has_prescriptions_before_date,
    from_prednison_equiv,
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
        self, people: list[Person], vax_anchor_dates: list[date]
    ) -> PeMap:
        if not self.__config.use_local_cache:
            return self.__compute_prednison_windows(people, vax_anchor_dates)

        return self.__get_prednison_windows_from_local_cache()

    def __compute_prednison_windows(
        self, people: list[Person], vax_anchor_dates: list[date]
    ) -> PeMap:
        # Prepare output structure
        result: PeMapInternal = {ad: {} for ad in vax_anchor_dates}

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
                if (is_valid_prescription(pr, self.__config))
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
            per_person_map: dict[date, float] = {ad: 0.0 for ad in vax_anchor_dates}

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

        output: PeMap = {}
        for anchor, ranges in result.items():
            output[anchor] = {}
            for pe_range, acs in ranges.items():
                output[anchor][pe_range] = {}
                for ac, genders in acs.items():
                    output[anchor][pe_range][ac] = {}
                    for gender, idset in genders.items():
                        output[anchor][pe_range][ac][gender] = list(idset)

        self.__save_map_to_local_cache(output)
        return output

    def __add_person_to_result(
        self,
        result: PeMapInternal,
        anchor: date,
        pe_range: PREDNISON_EQUIV_CATEGORY,
        person: Person,
    ) -> None:
        if pe_range not in result[anchor]:
            result[anchor][pe_range] = {}

        ac = self.__age_cohort_calculator.calculate_age_cohort(person)
        if ac not in result[anchor][pe_range]:
            result[anchor][pe_range][ac] = {}

        if person.gender not in result[anchor][pe_range][ac]:
            result[anchor][pe_range][ac][person.gender] = set()

        result[anchor][pe_range][ac][person.gender].add(person.id)

    def __save_map_to_local_cache(self, pe_map: PeMap) -> None:
        with open(
            self.__get_file_cache_file_name(),
            "wb",
        ) as f:
            pickle.dump(pe_map, f)

    def __get_prednison_windows_from_local_cache(
        self,
    ) -> PeMap:
        with open(
            self.__get_file_cache_file_name(),
            "rb",
        ) as f:
            pe_map: PeMap = pickle.load(f)

        return pe_map

    def __get_file_cache_file_name(self) -> str:
        return f"{self.__config.pojistovna.lower()}_{self.__config.year_offset}_years_back_pe_map.pkl"
