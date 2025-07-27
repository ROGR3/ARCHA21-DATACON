import matplotlib.pyplot as plt
import numpy as np


def draw_chart(mapp, x_label, y_label, title):
    plt.figure(figsize=(12, 6))

    days_after_last_vax = list(mapp.keys())
    first_prescription_counts = list(mapp.values())

    if len(days_after_last_vax) < 10 or len(first_prescription_counts) < 10:
        return
    avg = sum(first_prescription_counts) / len(first_prescription_counts)
    print(avg)
    if avg < 1.3:
        return

    sorted_days_after_last_vax, sorted_first_prescription_counts = zip(
        *sorted(zip(days_after_last_vax, first_prescription_counts))
    )

    # Calculate and plot moving average
    window_size = 7  # Adjust as needed
    smoothed_counts = moving_average(sorted_first_prescription_counts, window_size)
    smoothed_days = sorted_days_after_last_vax[window_size - 1 :]  # Align x-axis

    plt.plot(
        sorted_days_after_last_vax,
        sorted_first_prescription_counts,
        label="Original data",
    )
    plt.plot(
        smoothed_days,
        smoothed_counts,
        color="red",
        linewidth=2,
        label=f"{window_size}-day Moving Average",
    )

    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    plt.legend()
    plt.show()


def moving_average(data, window_size=7):
    return np.convolve(data, np.ones(window_size) / window_size, mode="valid")
