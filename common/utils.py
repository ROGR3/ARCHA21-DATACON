from typing import Any
import matplotlib.pyplot as plt
import numpy as np
import os


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
