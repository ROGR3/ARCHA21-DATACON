import matplotlib.pyplot as plt

fig, axes = plt.subplots(2, 2, figsize=(14, 10))  # 2 rows, 2 columns
fig.tight_layout(pad=5.0)
fig.suptitle("Prescriptions before and after max intensity", fontsize=16)


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


plt.xlabel(x_label)
plt.ylabel(y_label)
plt.title(title)
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()


x_data = [str(k.value) if hasattr(k, "value") else str(k) for k in mapp.keys()]
y_data = list(mapp.values())

plt.figure(figsize=(10, 6))
bars = plt.bar(x_data, y_data, color="skyblue", edgecolor="black")

plt.xlabel(x_label)
plt.ylabel(y_label)
plt.title(title)
plt.tight_layout()

# Optionally annotate bars with their values
last_value = 0
for i, (bar, value) in enumerate(zip(bars, y_data)):
    plt.text(
        bar.get_x() + bar.get_width() / 2,
        bar.get_height(),
        f"{value:,} / 100%",
        ha="center",
        va="bottom",
        fontsize=10,
    )
