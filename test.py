import polars as pl
import matplotlib.pyplot as plt

from common.constants.column_types import CPZP_SCHEMA, OZP_SCHEMA
from common.constants.column_names import SHARED_COLUMNS, OZP_COLUMNS
from common.constants.objects import AgeCohort

pl.Config.set_tbl_rows(100)
pl.Config.set_tbl_cols(100)


def read_preskladane_data(file_path: str, schema: pl.Schema) -> pl.DataFrame:
    return pl.read_csv(
        file_path,
        null_values=["NA", ""],
        schema=schema,
    )


def get_vekova_kohorta(age):
    if age < 12:
        return AgeCohort.LESS_THAN_12
    elif age < 16:
        return AgeCohort.BETWEEN_12_AND_16
    elif age < 30:
        return AgeCohort.BETWEEN_16_AND_30
    elif age < 35:
        return AgeCohort.BETWEEN_30_AND_35
    elif age < 40:
        return AgeCohort.BETWEEN_35_AND_40
    elif age < 45:
        return AgeCohort.BETWEEN_40_AND_45
    elif age < 50:
        return AgeCohort.BETWEEN_45_AND_50
    elif age < 55:
        return AgeCohort.BETWEEN_50_AND_55
    elif age < 60:
        return AgeCohort.BETWEEN_55_AND_60
    elif age < 65:
        return AgeCohort.BETWEEN_60_AND_65
    elif age < 70:
        return AgeCohort.BETWEEN_65_AND_70
    elif age < 75:
        return AgeCohort.BETWEEN_70_AND_75
    elif age < 80:
        return AgeCohort.BETWEEN_75_AND_80
    else:
        return AgeCohort.MORE_THAN_80


FILE_PATH = "./DATACON_data/CPZP_preskladane.csv"
ozp_df = read_preskladane_data(FILE_PATH, CPZP_SCHEMA)

print("OZP DATA: ", ozp_df.schema)
ozp_alive_df = ozp_df.filter(pl.col(SHARED_COLUMNS.DATUM_UMRTI).is_null())

print(ozp_alive_df)
ocko_df = (
    ozp_alive_df.filter(pl.col("Typ_udalosti") == "vakcinace")
    .sort(["Id_pojistence", "Datum_udalosti"])
    .with_columns(pl.arange(0, pl.len()).over("Id_pojistence").alias("Cislo_davky") + 1)
    .with_columns(
        (pl.col("Datum_udalosti").dt.year() - pl.col("Rok_narozeni")).alias(
            "Vek_v_dobe_vakcinace"
        )
    )
    .with_columns(
        pl.col("Vek_v_dobe_vakcinace")
        .map_elements(get_vekova_kohorta, return_dtype=pl.Utf8)
        .alias("Vekova_kohorta")
    )
    .select(
        [
            "Id_pojistence",
            pl.col("Datum_udalosti").alias("Datum_vakcinace"),
            "Cislo_davky",
            "Vekova_kohorta",
        ]
    )
)

vakcinace_grouped = (
    ocko_df.group_by(["Vekova_kohorta", "Datum_vakcinace", "Cislo_davky"])
    .agg(pl.len().alias("pocet_vakcinaci"))
    .sort(["Vekova_kohorta", "Datum_vakcinace", "Cislo_davky"])
)

rozhodna_data = (
    vakcinace_grouped.sort(
        ["Vekova_kohorta", "Cislo_davky", "pocet_vakcinaci", "Datum_vakcinace"],
        descending=[False, False, True, False],
    )
    .group_by(["Vekova_kohorta", "Cislo_davky"])
    .agg([pl.col("Datum_vakcinace").first().alias("Datum_rozhodne")])
)

print(
    rozhodna_data.sort(["Vekova_kohorta", "Cislo_davky"]).write_csv(
        "cpzp_rozhodna_data.csv"
    )
)

# import pickle

# with open("rozhodna_data.pkl", "wb") as f:
#     pickle.dump(rozhodna_data.sort(["Vekova_kohorta", "Cislo_davky"]), f)
