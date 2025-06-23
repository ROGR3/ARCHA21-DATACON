import pandas as pd
import random

# Načti vzorek z OZP_raw_data.txt
raw_path = "DATACON_data/OZP_raw_data.txt"
raw_df = pd.read_csv(raw_path, sep="|", nrows=100000, encoding="cp1250")
raw_df = raw_df.dropna(subset=["Id_pojistence", "Pocet_baleni", "Datum_udalosti"])
# Načti vzorek z OZP_preskladane.csv
preskladane_path = "DATACON_data/OZP_preskladane.csv"
preskladane_df = pd.read_csv(preskladane_path, nrows=100000)
preskladane_df = preskladane_df.dropna(
    subset=["Id_pojistence", "Pocet_baleni", "Datum_udalosti"]
)

# Porovnání počtu unikátních Id_pojistence
print("Raw unique IDs:", raw_df["Id_pojistence"].nunique())
print("Preskladane unique IDs:", preskladane_df["Id_pojistence"].nunique())

# Náhodný vzorek z obou pro porovnání
sample_raw = (
    raw_df.sample(100, random_state=42)
    .sort_values(by="Id_pojistence")
    .reset_index(drop=True)
)
sample_preskladane = (
    preskladane_df.sample(100, random_state=42)
    .sort_values(by="Id_pojistence")
    .reset_index(drop=True)
)

print(sample_raw.head())
print(sample_preskladane.head())
