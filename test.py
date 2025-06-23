import pandas as pd

df = pd.read_csv("./DATACON_data/CPZP_preskladane.csv", low_memory=False)

# Columns that should be numeric
cols_to_check = ["Pocet_baleni", "Prednison_equiv", "Pocet_v_baleni"]


def is_numeric(val: str) -> bool:
    """Returns True if val looks like a number."""
    try:
        float(val.replace(",", "."))
        return True
    except:
        return False


print(f"Total rows: {len(df)}")


for col in cols_to_check:
    weird_rows = []

    for idx, val in df[col].items():
        if pd.isna(val):
            continue  # Ignore NaNs
        val_str = str(val).strip()
        if not is_numeric(val_str):
            weird_rows.append((idx, val_str))

    print(f"\n--- Column '{col}' ---")
    print(f"NaN count: {df[col].isna().sum()}")
    if weird_rows:
        print(f"Weird values found ({len(weird_rows)}):")
        for idx, val in weird_rows:
            print(f"Row {idx}: '{val}'")
    else:
        print("✅ No weird values.")
