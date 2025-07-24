import polars as pl

from common.constants.column_types import CPZP_SCHEMA, OZP_SCHEMA
from common.constants.column_names import SHARED_COLUMNS, OZP_COLUMNS

FILE_PATH = "./DATACON_data/OZP_preskladane.csv"


def read_preskladane_data(file_path: str, schema: pl.Schema) -> pl.DataFrame:
    return pl.read_csv(
        file_path,
        null_values=["NA", ""],
        schema=schema,
    )


ozp_df = read_preskladane_data(FILE_PATH, OZP_SCHEMA)

print("OZP DATA: ", ozp_df.schema)
