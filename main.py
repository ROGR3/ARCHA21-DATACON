import polars as pl

from common.constants.column_types import OZP_SCHEMA

# Read the CSV file and print its schema
cpzp_df = pl.read_csv(
    "./DATACON_data/OZP_preskladane.csv",
    null_values=["NA", ""],
    schema=OZP_SCHEMA,
)
print(cpzp_df.schema)
