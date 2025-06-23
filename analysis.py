import polars as pl


# Funkce pro načítání a označení období
def load_preskladane_with_period(path: str):
    # Polars umí načítat selektivně sloupce i streamingově
    df = pl.scan_csv(path, null_values=["NA"])
    df = df.with_columns(
        pl.col("Datum_udalosti")
        .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
        .alias("datum")
    )
    df = df.with_columns(
        pl.when(pl.col("datum") <= pl.lit("2020-02-29").cast(pl.Date))
        .then(pl.lit("pred"))
        .otherwise(pl.lit("behem"))
        .alias("obdobi")
    )
    return df


# Příklad použití
df_cpzp = load_preskladane_with_period("./DATACON_data/CPZP_preskladane.csv")
df_ozp = load_preskladane_with_period("./DATACON_data/OZP_preskladane.csv")

# Můžeme hned načíst například počty záznamů podle období
counts = df_cpzp.group_by("obdobi").agg(pl.len().alias("pocet_zaznamu")).collect()
print(counts)

counts = df_ozp.group_by("obdobi").agg(pl.len().alias("pocet_zaznamu")).collect()
print(counts)


# Načti ekvivalenty
ekvivalenty = pl.read_csv("./DATACON_data/kortizonove_ekvivalenty.csv")

# Ujisti se, že názvy jsou ve správném formátu (např. lowercase)
ekvivalenty = ekvivalenty.with_columns(
    pl.col("Substance").str.strip_chars().str.to_lowercase()
)


# Funkce pro načtení dávky v prednisonových ekvivalentech
def load_with_prednison_eq(path: str, ekv_df: pl.DataFrame):
    df = pl.scan_csv(
        path,
        null_values=["NA"],
        schema_overrides={
            "Id_pojistence": pl.Utf8,
            "síla": pl.Utf8,
            "Pocet_v_baleni": pl.Float64,
        },
    )

    # Předzpracování názvů
    df = df.with_columns(
        [
            pl.col("léčivé_látky").fill_null("").str.strip_chars().str.to_lowercase(),
            # Extract numeric value from 'síla', replace comma with dot, and cast to float
            pl.col("síla")
            .cast(pl.Utf8)
            .str.replace_all(",", ".")
            .str.extract(r"(\d+\.?\d*)", 1)
            .cast(pl.Float64)
            .alias("davka_float"),
            pl.col("Pocet_baleni").cast(pl.Float64),
        ]
    )

    # Join na základě účinné látky
    df_joined = df.join(
        ekv_df.lazy(), left_on="léčivé_látky", right_on="Substance", how="left"
    )

    # Spočítej prednisonové ekvivalenty
    df_joined = df_joined.with_columns(
        (pl.col("davka_float") * pl.col("Pocet_baleni") * pl.col("equiv_mg")).alias(
            "prednison_eq"
        )
    )

    return df_joined.collect()


# Příklad
df_cpzp_eq = load_with_prednison_eq("./DATACON_data/CPZP_preskladane.csv", ekvivalenty)
df_ozp_eq = load_with_prednison_eq("./DATACON_data/OZP_preskladane.csv", ekvivalenty)

# Debug: print column names to check join result
print("\nCPZP columns:", df_cpzp_eq.columns)
print("OZP columns:", df_ozp_eq.columns)


# Debug: print a sample of the joined dataframe to check the join and calculation
def debug_print(df, label):
    print(f"\nSample from {label}:")
    print(
        df.select(
            ["léčivé_látky", "equiv_mg", "davka_float", "Pocet_baleni", "prednison_eq"]
        ).head(10)
    )


debug_print(df_cpzp_eq, "CPZP")
debug_print(df_ozp_eq, "OZP")

# Shrnutí: kolik celkem prednison ekvivalentu bylo spotřebováno?
total_cpzp = df_cpzp_eq.select(pl.sum("prednison_eq")).item()
total_ozp = df_ozp_eq.select(pl.sum("prednison_eq")).item()

print("CPZP:", total_cpzp, "mg prednison ekv.")
print("OZP:", total_ozp, "mg prednison ekv.")

print("\nUnique 'síla' values (CPZP):")
print(df_cpzp_eq.select("síla").unique().head(20))
