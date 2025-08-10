from typing import Any
import matplotlib.pyplot as plt
import numpy as np
import os


import polars as pl
from datetime import timedelta, datetime

import matplotlib.dates as mdates
from matplotlib.patches import Patch


def draw_bar_chart(
    mapp,
    x_label,
    y_label,
    title,
    save_location: str | None = None,
):
    # Convert enum keys to their value for display
    x_data = [str(k.value) if hasattr(k, "value") else str(k) for k in mapp.keys()]
    y_data = list(mapp.values())

    plt.figure(figsize=(10, 6))
    bars = plt.bar(x_data, y_data, color="skyblue", edgecolor="black")

    plt.xlabel(x_label)
    plt.xticks(rotation=90)
    plt.ylabel(y_label)
    plt.title(title)
    plt.tight_layout()

    # Optionally annotate bars with their values
    for bar, value in zip(bars, y_data):
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            f"{value:,}",
            ha="center",
            va="bottom",
            fontsize=10,
        )

    if save_location:
        plt.savefig(save_location)
    else:
        plt.show()


def draw_chart(
    mapp,
    x_label,
    y_label,
    title,
    average: int | None = None,
    save_location: str | None = None,
    vertical_line: Any = None,
):
    plt.figure(figsize=(12, 6))

    x_data = list(mapp.keys())
    y_data = list(mapp.values())

    sorted_days_after_last_vax, sorted_first_prescription_counts = zip(
        *sorted(zip(x_data, y_data))
    )

    plt.plot(
        sorted_days_after_last_vax,
        sorted_first_prescription_counts,
        label="Original data",
        alpha=0.7,
        marker="o",
        linestyle="None",
    )

    if average:
        window_size = average  # Adjust as needed
        smoothed_counts = moving_average(sorted_first_prescription_counts, window_size)
        smoothed_days = sorted_days_after_last_vax[window_size - 1 :]

        plt.plot(
            smoothed_days,
            smoothed_counts,
            color="red",
            linewidth=2,
            label=f"{window_size}-day Moving Average",
        )

    if vertical_line:
        closest_index = min(
            range(len(sorted_days_after_last_vax)),
            key=lambda i: abs(sorted_days_after_last_vax[i] - vertical_line),
        )
        closest_x = sorted_days_after_last_vax[closest_index]

        plt.axvline(
            x=vertical_line,
            color="green",
            linestyle="--",
            linewidth=2,
            label=f"Day {closest_x}",
        )

    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    if save_location:
        os.makedirs(os.path.dirname(save_location), exist_ok=True)
        plt.savefig(save_location, dpi=300, bbox_inches="tight")
        plt.close()
        print(f"✓ Chart saved: {save_location}")
    else:
        plt.show()
        plt.close()


def moving_average(data, window_size=7):
    return np.convolve(data, np.ones(window_size) / window_size, mode="valid")


class ChartDrawer:
    def draw_2x2_block(
        self,
        vax_dates_map,
        novax_dates_map,
        rozhodne_datum,
        title,
        axes,
        row_offset,
        col_offset,
    ):
        vax_sums = self.__get_before_after_sums(vax_dates_map, rozhodne_datum)
        novax_sums = self.__get_before_after_sums(novax_dates_map, rozhodne_datum)
        total_vax_sum = sum(vax_sums.values())
        total_novax_sum = sum(novax_sums.values())

        # Očkovaná skupina
        self.__draw_scatter_plot(
            axes[row_offset][col_offset],
            list(vax_dates_map.keys()),
            list(vax_dates_map.values()),
            f"{title} - Očkovaná skupina",
            rozhodne_datum,
            total_vax_sum,
        )

        self.__draw_bar_chart(
            axes[row_offset][col_offset + 1],
            list(vax_sums.keys()),
            list(vax_sums.values()),
            f"{title} - Očkovaná skupina",
        )

        # Neočkovaná skupina
        self.__draw_scatter_plot(
            axes[row_offset + 1][col_offset],
            list(novax_dates_map.keys()),
            list(novax_dates_map.values()),
            f"{title} - Neočkovaná skupina",
            rozhodne_datum,
            total_novax_sum,
        )

        self.__draw_bar_chart(
            axes[row_offset + 1][col_offset + 1],
            list(novax_sums.keys()),
            list(novax_sums.values()),
            f"{title} - Neočkovaná skupina",
        )

    def __draw_scatter_plot(self, ax, x_data, y_data, title, rozhodne_datum, total_sum):
        ax.plot(x_data, y_data, label="Data", alpha=0.7, marker="o", linestyle="None")

        ax.axvline(
            x=rozhodne_datum,
            color="green",
            linestyle="--",
            linewidth=2,
            label=f"Rozhodné datum: {rozhodne_datum.strftime('%Y-%m-%d')}",
        )

        # Průměr před rozhodným datem
        before_values = [y for x, y in zip(x_data, y_data) if x < rozhodne_datum]
        if before_values:
            before_avg = sum(before_values) / len(before_values)
            ax.axhline(before_avg, color="blue", linestyle="--", label="Průměr před")

        # Průměr po rozhodném datu (včetně něj)
        after_values = [y for x, y in zip(x_data, y_data) if x >= rozhodne_datum]
        if after_values:
            after_avg = sum(after_values) / len(after_values)
            ax.axhline(after_avg, color="purple", linestyle="--", label="Průměr po")

        # Týdenní průměry
        weekly_buckets = defaultdict(list)
        for x, y in zip(x_data, y_data):
            week_start = x - timedelta(days=x.weekday())  # pondělí daného týdne
            weekly_buckets[week_start].append(y)

        # Spočítat průměr za každý týden
        weekly_avg_points = sorted(
            (week_start, sum(vals) / len(vals))
            for week_start, vals in weekly_buckets.items()
        )

        if weekly_avg_points:
            week_x, week_y = zip(*weekly_avg_points)
            ax.plot(week_x, week_y, color="orange", marker="s", label="Týdenní průměr")

        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        ax.tick_params(axis="x", rotation=45)

        ax.set_xlabel("Dny kolem max intenzity")
        ax.set_ylabel("Počet předpisů za den")
        ax.set_title(title)
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.text(
            0.99,
            0.95,
            f"Celkem: {total_sum:,}",
            transform=ax.transAxes,
            ha="right",
            va="top",
            fontsize=10,
            bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.5),
        )

    def __draw_bar_chart(self, ax, x_data, y_data, title):
        bars = ax.bar(
            x_data,
            y_data,
            color="skyblue",
            edgecolor="black",
            width=0.4,  # 🔹 Make bars narrower
        )
        ax.set_xlabel("Období před a po")
        ax.set_ylabel("Celkový počet předpisů")
        ax.set_title(title)
        ax.grid(True, alpha=0.3)

        percentages = [100, (y_data[1] / y_data[0]) * 100 if y_data[0] != 0 else 0]
        for i, (bar, value) in enumerate(zip(bars, y_data)):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height(),
                f"{value:,} / {percentages[i]:.2f}%",
                ha="center",
                va="bottom",
                fontsize=12,  # 🔹 Bigger text on bars
            )

    def __get_before_after_sums(self, dates_map, rozhodne_datum):
        before_items = [(d, v) for d, v in dates_map.items() if d < rozhodne_datum]
        after_items = [(d, v) for d, v in dates_map.items() if d >= rozhodne_datum]

        before_sum = sum(v for _, v in before_items)
        after_sum = sum(v for _, v in after_items)

        return {"před": before_sum, "po": after_sum}

    def draw_vax_vs_unvax_sums(
        self, ax, vax_dates_map, novax_dates_map, rozhodne_datum, title
    ):
        vax = self.__get_before_after_sums(vax_dates_map, rozhodne_datum)
        novax = self.__get_before_after_sums(novax_dates_map, rozhodne_datum)

        x_labels = [
            "Očkovaná: před",
            "Očkovaná: po",
            "Neočkovaná: před",
            "Neočkovaná: po",
        ]
        y_values = [vax["před"], vax["po"], novax["před"], novax["po"]]

        colors = ["skyblue", "skyblue", "seagreen", "seagreen"]

        bars = ax.bar(
            x_labels,
            y_values,
            color=colors,
            edgecolor="black",
            width=1,  # užší sloupce
        )

        # Procenta: vždy relativně k "před" v rámci dané skupiny
        vax_percentages = [
            100,
            (vax["po"] / vax["před"]) * 100 if vax["před"] != 0 else 0,
        ]
        novax_percentages = [
            100,
            (novax["po"] / novax["před"]) * 100 if novax["před"] != 0 else 0,
        ]
        percentages = [
            vax_percentages[0],
            vax_percentages[1],
            novax_percentages[0],
            novax_percentages[1],
        ]

        # Popisky nad sloupci
        for i, (bar, value) in enumerate(zip(bars, y_values)):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height(),
                f"{value:,} / {percentages[i]:.2f}%",
                ha="center",
                va="bottom",
                fontsize=14,  # larger font
                fontweight="bold",  # bold text
            )

        # Osy, mřížka, titul
        ax.set_title(title)
        ax.grid(True, axis="y", alpha=0.3)
        ax.tick_params(axis="x", rotation=15)

        # Legenda (proxí záznamy)
        legend_handles = [
            Patch(facecolor="skyblue", edgecolor="black", label="Očkovaná skupina"),
            Patch(facecolor="seagreen", edgecolor="black", label="Neočkovaná skupina"),
        ]
        ax.legend(handles=legend_handles, loc="best")


def plot_vax_timeline(vax_dates_map, age_cohort, dose_number):
    df = pl.DataFrame({"date": vax_dates_map[age_cohort][dose_number]})
    counts = df.group_by("date").len().rename({"len": "count"}).sort("date")
    start, end = counts["date"].min(), counts["date"].max()
    counts = (
        pl.DataFrame({"date": pl.date_range(start, end, "1d", eager=True)})
        .join(counts, on="date", how="left")
        .fill_null(0)
        .with_columns(pl.col("count").rolling_mean(7).alias("ma"))
    )

    # peak den
    peak = counts.sort("count", descending=True).head(1)
    peak_date, _ = peak["date"][0], int(peak["count"][0])

    # -30 a +30 dní
    left = peak_date - timedelta(days=30)
    right = peak_date + timedelta(days=30)

    plt.figure(figsize=(14, 6))
    plt.plot(counts["date"], counts["count"], label="Original data", alpha=0.5)
    plt.plot(counts["date"], counts["ma"], label="7-day Moving Average", linewidth=2)

    # peak + popisek
    plt.axvline(
        peak_date,
        color="green",
        linestyle="--",
        linewidth=1.5,
        label=f"Peak: {peak_date}",
    )

    # -30 / +30 dní a vyšrafovaná oblast
    plt.axvline(left, color="green", linestyle="--", linewidth=1)
    plt.axvline(right, color="green", linestyle="--", linewidth=1)
    plt.axvspan(
        left, right, facecolor="green", alpha=0.08, hatch="//", edgecolor="green"
    )

    plt.title(f"Vaccination Timeline - {age_cohort} - Dose {dose_number}")
    plt.xlabel("Date")  # klidně změním na denní offsety
    plt.ylabel("Number of vaccinations")
    plt.legend()
    plt.tight_layout()
    plt.show()


def filter_by_date_range(
    dates: dict[datetime, int], decisive_date: datetime, days: int
) -> dict[datetime, int]:
    start = decisive_date - timedelta(days=days)
    end = decisive_date + timedelta(days=days)
    return {
        day: dates.get(day, 0)
        for day in (start + timedelta(days=i) for i in range((end - start).days + 1))
    }
