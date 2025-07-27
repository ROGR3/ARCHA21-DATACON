from typing import Any
import matplotlib.pyplot as plt
import numpy as np
import os


def draw_chart(
    mapp,
    x_label,
    y_label,
    title,
    save_location: str | None = None,
    vertical_line: Any = None,
):
    plt.figure(figsize=(12, 6))

    x_data = list(mapp.keys())
    y_data = list(mapp.values())

    if len(x_data) < 10 or len(y_data) < 10:
        plt.close()
        return
    avg = sum(y_data) / len(y_data)

    if avg < 1.3:
        plt.close()
        return

    sorted_days_after_last_vax, sorted_first_prescription_counts = zip(
        *sorted(zip(x_data, y_data))
    )

    window_size = 7  # Adjust as needed
    smoothed_counts = moving_average(sorted_first_prescription_counts, window_size)
    smoothed_days = sorted_days_after_last_vax[window_size - 1 :]

    plt.plot(
        sorted_days_after_last_vax,
        sorted_first_prescription_counts,
        label="Original data",
        alpha=0.7,
    )
    plt.plot(
        smoothed_days,
        smoothed_counts,
        color="red",
        linewidth=2,
        label=f"{window_size}-day Moving Average",
    )

    if vertical_line:
        plt.axvline(x=vertical_line, color="green", linestyle="--", linewidth=2)

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


def moving_average(data, window_size=7):
    return np.convolve(data, np.ones(window_size) / window_size, mode="valid")
