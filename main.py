import polars as pl

from common.constants.column_types import CPZP_SCHEMA, OZP_SCHEMA
from common.constants.column_names import SHARED_COLUMNS, OZP_COLUMNS

# Read the CSV file and print its schema
ozp_df = pl.read_csv(
    "./DATACON_data/OZP_preskladane.csv",
    null_values=["NA", ""],
    schema=OZP_SCHEMA,
)

cpzp_df = pl.read_csv(
    "./DATACON_data/CPZP_preskladane.csv",
    null_values=["NA", ""],
    schema=CPZP_SCHEMA,
)
print("OZP DATA: ", ozp_df.schema)
print("CPZP DATA: ", cpzp_df.schema)


print("Before filtering missing DATUM_UDALOSTI: ", ozp_df.height)
ozp_df = ozp_df.filter(pl.col(SHARED_COLUMNS.DATUM_UDALOSTI).is_not_null())
print("After filtering: ", ozp_df.height)

print("Before filtering death: ", ozp_df.height)
ozp_df = ozp_df.filter(
    (pl.col(SHARED_COLUMNS.DATUM_UDALOSTI) > pl.col(SHARED_COLUMNS.DATUM_UMRTI))
    | (pl.col(SHARED_COLUMNS.DATUM_UMRTI).is_null())
)
print("After filtering death: ", ozp_df.height)

print("Before filtering unique: ", ozp_df.height)
print("DUPLICATES: ", ozp_df.filter(pl.lit(ozp_df.is_duplicated())))
ozp_df = ozp_df.unique(keep="first")
print("After filtering unique: ", ozp_df.height)
