from datetime import date
import polars as pl

from common.matching_simulation.data_loader import DataLoader
from common.matching_simulation.matching_analyser import MatchingAnalyser
from common.matching_simulation.prednison_windows_computer import (
    PrednisonWindowsComputer,
)
from common.matching_simulation.result_writer import ResultWriter
from common.matching_simulation.utils import (
    PE_GROUP_NAMES,
    MatchingAnalysisConfig,
    AgeCohortCalculator,
)

pl.Config.set_tbl_rows(20)
pl.Config.set_tbl_cols(60)

pl.Config.set_tbl_rows(-1)


def main():
    config = MatchingAnalysisConfig(
        pojistovna="cpzp",
        zacatek_pojisteni=date(2015, 1, 1),
        konec_pojisteni=date(2023, 12, 31),
        year_offset=3,
        use_local_cache=True,
        use_unified_effect_baseline=False,
    )
    data_loader = DataLoader(config)
    age_cohort_calculator = AgeCohortCalculator(config)
    prednison_windows_computer = PrednisonWindowsComputer(config, age_cohort_calculator)
    result_writer = ResultWriter(config)
    matching_analyser = MatchingAnalyser(config, age_cohort_calculator)

    prednison_windows = prednison_windows_computer.get_prednison_windows(
        people=data_loader.novax_people, vax_anchor_dates=config.anchor_dates
    )

    aggregation_days_list = [config.maximum_aggregation_days]
    groups = {
        PE_GROUP_NAMES.NEVER_PRESCRIBED: data_loader.never_prescribed_vax_people,
        PE_GROUP_NAMES.ZERO_PE_SUSPECTIBLE: data_loader.zero_pe_suspectible,
        PE_GROUP_NAMES.ZERO_PE: data_loader.zero_pe_vax_people,
        PE_GROUP_NAMES.ONE_TO_FIVE_HUNDRED_PE: data_loader.one_to_five_hundred_pe_vax_people,
        PE_GROUP_NAMES.FIVE_HUNDRED_TO_FIVE_THOUSAND_PE: data_loader.five_hundred_to_five_thousand_pe_vax_people,
    }

    for group_name, group in groups.items():
        for aggregation_days in aggregation_days_list:
            print(f"Processing {group_name} with {aggregation_days} days aggregation")

            (
                median_map,
                iqr_map,
                ci_map,
                vax_effects_median,
                novax_effects_median,
            ) = matching_analyser.run_matching_analysis(
                people=group,
                person_map=data_loader.person_map,
                pe_map=prednison_windows,
                aggregation_days=aggregation_days,
                group_name=group_name,
                num_runs=100,
            )

            result_writer.write_result(
                median_map=median_map,
                iqr_map=iqr_map,
                ci_map=ci_map,
                vax_dates_distribution=matching_analyser.get_vax_dates_distribution(
                    group, aggregation_days
                ),
                group_name=group_name,
                aggregation_days=aggregation_days,
                vax_effects_median=vax_effects_median,
                novax_effects_median=novax_effects_median,
            )


if __name__ == "__main__":
    main()
