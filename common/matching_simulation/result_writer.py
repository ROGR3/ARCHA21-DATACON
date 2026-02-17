from common.matching_simulation.utils import MatchingAnalysisConfig
import matplotlib.pyplot as plt
import os
from common.matching_simulation.utils import (
    AgeCohort,
)


class ResultWriter:
    def __init__(self, config: MatchingAnalysisConfig):
        self.__config = config

    def write_result(
        self,
        median_map,
        iqr_map,
        ci_map,
        vax_dates_distribution,
        group_name,
        aggregation_days,
    ):
        folder_path = f"out/{self.__config.pojistovna}/matching_analysis/whole_period/{group_name}/{aggregation_days}_days_aggregation"
        self.__plot_treatment_effect(
            median_map,
            iqr_map,
            vax_dates_distribution,
            group_name,
            aggregation_days,
            folder_path,
        )
        if aggregation_days == self.__config.maximum_aggregation_days:
            self.__write_table(
                median_map=median_map,
                iqr_map=iqr_map,
                ci_map=ci_map,
                vax_dates_distribution=vax_dates_distribution,
                folder_path=folder_path,
            )

    def __plot_treatment_effect(
        self,
        median_map,
        iqr_map,
        vax_dates_distribution,
        group_name,
        aggregation_days,
        folder_path,
    ):
        for cohort, date_map in median_map.items():
            sorted_dates = sorted(date_map.keys())
            y_mean = [median_map[cohort][d] for d in sorted_dates]
            y_lower = [iqr_map[cohort][d][0] for d in sorted_dates]
            y_upper = [iqr_map[cohort][d][1] for d in sorted_dates]

            vax_counts = [
                vax_dates_distribution[cohort].get(d, 0) for d in sorted_dates
            ]

            fig, ax_left = plt.subplots(figsize=(10, 5))

            # left axis
            ax_left.plot(sorted_dates, y_mean, label="Median effect", color="blue")
            ax_left.fill_between(
                sorted_dates, y_lower, y_upper, alpha=0.2, label="IQR", color="blue"
            )
            ax_left.axhline(
                1 if group_name == "0_PE" or group_name == "NEVER_PRESCRIBED" else 0,
                color="black",
                linewidth=1,
            )
            ax_left.set_ylabel("Effect value")

            # right axis
            ax_right = ax_left.twinx()
            ax_right.plot(
                sorted_dates,
                vax_counts,
                linestyle="--",
                label="Vaccinated",
                color="orange",
            )
            ax_right.set_ylabel("Vaccinated count")

            ax_left.set_title(
                f"Cohort {cohort} - {group_name} - {aggregation_days} days"
            )
            fig.autofmt_xdate()
            fig.tight_layout()

            # shared legend
            lines, labels = ax_left.get_legend_handles_labels()
            lines2, labels2 = ax_right.get_legend_handles_labels()
            ax_left.legend(lines + lines2, labels + labels2)

            os.makedirs(folder_path, exist_ok=True)
            fig.savefig(f"{folder_path}/{cohort}.png")
            plt.close(fig)

    def __write_table(
        self, median_map, iqr_map, ci_map, vax_dates_distribution, folder_path
    ):
        table = []

        def get_median(median_map, cohort):
            values = median_map.get(cohort, {})
            return next(iter(values.values()), None)

        def get_iqr(iqr_map, cohort):
            values = iqr_map.get(cohort, {})
            return next(iter(values.values()), None)  # (q1, q3)

        def get_ci(ci_map, cohort):
            values = ci_map.get(cohort, {})
            return next(iter(values.values()), None)  # (ci_low, ci_high)

        def get_total_vaccinations(vax_dates_distribution, cohort):
            date_map = vax_dates_distribution.get(cohort, {})
            return int(sum(date_map.values()))

        for cohort in AgeCohort:
            median = get_median(median_map, cohort)
            iqr = get_iqr(iqr_map, cohort)
            ci = get_ci(ci_map, cohort)
            total_vax = get_total_vaccinations(vax_dates_distribution, cohort)

            table.append(
                {
                    "věk": cohort.value,
                    "Med": median,
                    "IQR": iqr,
                    "95% CI": ci,
                    "počet očko": total_vax,
                }
            )

        import polars as pl

        df = pl.DataFrame(table)

        df.write_json(f"{folder_path}/../effects_summary.json")
