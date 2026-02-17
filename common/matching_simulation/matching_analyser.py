import random
from common.constants.objects import Person, Gender
from collections import defaultdict
from datetime import date, datetime, timedelta
from common.matching_simulation.utils import (
    PREDNISON_EQUIV_CATEGORY,
    EffectMap,
    MatchingAnalysisConfig,
    from_prednison_equiv,
    sum_after_date_pe_for_person,
    sum_before_date_pe_for_person,
    AgeCohort,
    AgeCohortCalculator,
)
import numpy as np


class MatchingAnalyser:
    def __init__(
        self, config: MatchingAnalysisConfig, age_cohort_calculator: AgeCohortCalculator
    ):
        self.__config = config
        self.__age_cohort_calculator = age_cohort_calculator

    def get_vax_dates_distribution(
        self, group: list[Person], aggregation_days: int
    ) -> dict[AgeCohort, dict[datetime, float]]:
        vax_dates_distribution: dict[AgeCohort, dict[datetime, float]] = defaultdict(
            lambda: defaultdict(float)
        )

        for vax_person in group:
            ac = self.__age_cohort_calculator.calculate_age_cohort(vax_person)
            vax_date = vax_person.vaccines[0].date
            aggregated_date = self.__aggregate_date(vax_date, aggregation_days)

            vax_dates_distribution[ac][aggregated_date] += 1

        return vax_dates_distribution

    def run_matching_analysis(
        self,
        people: list[Person],
        person_map: dict[int | str, Person],
        pe_map: dict[datetime, dict[str | int, float]],
        aggregation_days: int,
        group_name: str,
        num_runs: int = 100,
    ) -> tuple[EffectMap, EffectMap, EffectMap]:
        effects = defaultdict(lambda: defaultdict(list))
        vax_ratio_effects = defaultdict(lambda: defaultdict(list))
        novax_ratio_effects = defaultdict(lambda: defaultdict(list))

        for i in range(num_runs):
            vax_before, vax_after, novax_before, novax_after = (
                self.__compute_vax_vs_novax_sums(
                    people, person_map, pe_map, aggregation_days
                )
            )

            result_map = self.__compute_effect_values(
                vax_before, vax_after, novax_before, novax_after, group_name
            )

            # Also accumulate vax_after/vax_before and novax_after/novax_before for each run
            all_cohorts = (
                set(vax_before.keys())
                | set(vax_after.keys())
                | set(novax_before.keys())
                | set(novax_after.keys())
            )
            for cohort in all_cohorts:
                all_dates = (
                    set(vax_before[cohort].keys())
                    | set(vax_after[cohort].keys())
                    | set(novax_before[cohort].keys())
                    | set(novax_after[cohort].keys())
                )
                for dt in all_dates:
                    vax_before_val = vax_before[cohort].get(dt, 0.0)
                    vax_after_val = vax_after[cohort].get(dt, 0.0)
                    novax_before_val = novax_before[cohort].get(dt, 0.0)
                    novax_after_val = novax_after[cohort].get(dt, 0.0)

                    vax_ratio = (
                        vax_after_val / vax_before_val
                        if vax_before_val != 0
                        else float("nan")
                    )
                    novax_ratio = (
                        novax_after_val / novax_before_val
                        if novax_before_val != 0
                        else float("nan")
                    )

                    vax_ratio_effects[cohort][dt].append(vax_ratio)
                    novax_ratio_effects[cohort][dt].append(novax_ratio)

            for cohort, date_map in result_map.items():
                for dt, value in date_map.items():
                    effects[cohort][dt].append(value)

            print(f"Processed {i} runs")

        return self.__compute_statistics(effects)

    def __compute_vax_vs_novax_sums(
        self,
        people: list[Person],
        person_map: dict[int | str, Person],
        pe_map: dict[datetime, dict[str | int, float]],
        aggregation_days: int = 1,
    ):
        vax_before_pe_map: dict[AgeCohort, dict[datetime, float]] = defaultdict(
            lambda: defaultdict(float)
        )
        vax_after_pe_map: dict[AgeCohort, dict[datetime, float]] = defaultdict(
            lambda: defaultdict(float)
        )
        novax_before_pe_map: dict[AgeCohort, dict[datetime, float]] = defaultdict(
            lambda: defaultdict(float)
        )
        novax_after_pe_map: dict[AgeCohort, dict[datetime, float]] = defaultdict(
            lambda: defaultdict(float)
        )

        print("iterating through people ", len(people))

        for person in people:
            first_vax = person.vaccines[0]

            vax_person_before_pe = sum_before_date_pe_for_person(person, first_vax.date)
            if vax_person_before_pe > 5000:
                continue

            pe_range = from_prednison_equiv(vax_person_before_pe)
            try:
                matched_id = self.__find_matching_person(
                    vax_person=person,
                    pe_range=pe_range,
                    vax_date=first_vax.date,
                    novax_map=pe_map,
                )
            except Exception:
                continue

            matched_person = person_map[matched_id]

            vax_person_after_pe = sum_after_date_pe_for_person(person, first_vax.date)
            novax_person_before_pe = sum_before_date_pe_for_person(
                matched_person, first_vax.date
            )
            novax_person_after_pe = sum_after_date_pe_for_person(
                matched_person, first_vax.date
            )

            ac = self.__age_cohort_calculator.calculate_age_cohort(person)

            aggregated_date = self.__aggregate_date(first_vax.date, aggregation_days)

            vax_before_pe_map[ac][aggregated_date] += vax_person_before_pe
            vax_after_pe_map[ac][aggregated_date] += vax_person_after_pe
            novax_before_pe_map[ac][aggregated_date] += novax_person_before_pe
            novax_after_pe_map[ac][aggregated_date] += novax_person_after_pe

        return (
            vax_before_pe_map,
            vax_after_pe_map,
            novax_before_pe_map,
            novax_after_pe_map,
        )

    def __compute_effect_values(
        self,
        vax_before_pe_map,
        vax_after_pe_map,
        novax_before_pe_map,
        novax_after_pe_map,
        group_name,
    ) -> EffectMap:
        result_map: EffectMap = defaultdict(dict)

        cohorts = (
            set(vax_before_pe_map.keys())
            | set(vax_after_pe_map.keys())
            | set(novax_before_pe_map.keys())
            | set(novax_after_pe_map.keys())
        )

        for cohort in cohorts:
            all_dates = (
                set(vax_before_pe_map[cohort].keys())
                | set(vax_after_pe_map[cohort].keys())
                | set(novax_before_pe_map[cohort].keys())
                | set(novax_after_pe_map[cohort].keys())
            )

            for dt in all_dates:
                vax_before = vax_before_pe_map[cohort].get(dt, 0.0)
                vax_after = vax_after_pe_map[cohort].get(dt, 0.0)
                novax_before = novax_before_pe_map[cohort].get(dt, 0.0)
                novax_after = novax_after_pe_map[cohort].get(dt, 0.0)

                if group_name == "0_PE" or group_name == "NEVER_PRESCRIBED":
                    if novax_after != 0:
                        result_map[cohort][dt] = vax_after / novax_after
                else:
                    if vax_before != 0 and novax_before != 0:
                        result_map[cohort][dt] = (vax_after / vax_before) - (
                            novax_after / novax_before
                        )

        return result_map

    def __compute_statistics(
        self,
        effects: dict[AgeCohort, dict[datetime, list[float]]],
    ) -> tuple[EffectMap, EffectMap, EffectMap]:
        median_map: EffectMap = defaultdict(dict)
        iqr_map: dict[AgeCohort, dict[datetime, tuple[float, float]]] = defaultdict(
            dict
        )
        ci_map: EffectMap = defaultdict(dict)

        for cohort, date_map in effects.items():
            for dt, values in date_map.items():
                arr = np.asarray(values)

                median = np.median(arr)
                q1, q3 = np.percentile(arr, [25, 75])
                ci_low, ci_high = np.percentile(arr, [2.5, 97.5])

                median_map[cohort][dt] = median
                iqr_map[cohort][dt] = (q1, q3)
                ci_map[cohort][dt] = (ci_low, ci_high)

        return median_map, iqr_map, ci_map

    def __aggregate_date(self, start_date: date, window_days: int) -> datetime:
        start_date = datetime(start_date.year, start_date.month, start_date.day)
        if window_days <= 1:
            return start_date

        epoch = self.__config.start_date
        days_since_epoch = (start_date - epoch).days
        window_start_days = (days_since_epoch // window_days) * window_days
        return epoch + timedelta(days=window_start_days)

    def __find_matching_person(
        self,
        vax_person: Person,
        pe_range: PREDNISON_EQUIV_CATEGORY,
        vax_date: datetime,
        novax_map: dict[
            PREDNISON_EQUIV_CATEGORY, dict[AgeCohort, dict[Gender, set[int]]]
        ],
    ) -> str:
        ac = self.__age_cohort_calculator.calculate_age_cohort(vax_person)
        gender = vax_person.gender
        novax_matched_ids = novax_map[vax_date][pe_range][ac][gender]
        max_int = len(novax_matched_ids)
        random_index = random.randint(0, max_int - 1)
        return novax_matched_ids[random_index]
