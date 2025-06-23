import polars as pl
from polars import Int64, Float64, String

from common.constants.column_names import (
    CPZP_SCHEMA,
    OZP_SCHEMA,
    OZP_COLUMNS,
    SHARED_COLUMNS,
    CPZP_COLUMNS,
)
from common.constants.column_types import TYP_UDALOSTI

schema = pl.Schema(
    {
        "Id_pojistence": Float64,
        "Pohlavi": String,
        "Rok_narozeni": Int64,
        "Mesic_narozeni": Int64,
        "Posledni_zahajeni_pojisteni": String,
        "Posledni_ukonceni_pojisteni": String,
        "Rok_umrti": Int64,
        "Mesic_umrti": Int64,
        "Typ_udalosti": TYP_UDALOSTI,
        "Kod_udalosti": String,
        "Detail_udalosti": String,
        "Pocet_baleni": Float64,
        "Datum_udalosti": String,
        "Specializace": String,
        "Datum_umrti": String,
        "léková_forma_zkr": String,
        "ATC_skupina": String,
        "síla": String,
        "doplněk_názvu": String,
        "léková_forma": String,
        "léčivé_látky": String,
        "Equiv_sloucenina": String,
        "Prednison_equiv": Float64,
        "Pocet_v_baleni": Float64,
        "pololeti": Int64,
        "rok_zahajeni": Int64,
        "poradi": Int64,
        "pocet_vakcinaci": Int64,
        "ockovany": Int64,
        "pocet_predpisu": Int64,
    }
)


schema2 = pl.Schema(
    {
        SHARED_COLUMNS.ID_POJISTENCE.value: pl.Float64,
        SHARED_COLUMNS.POHLAVI.value: pl.String,
        SHARED_COLUMNS.ROK_NAROZENI.value: pl.Int64,
        SHARED_COLUMNS.POSLEDNI_ZAHAJENI_POJISTENI.value: pl.String,
        SHARED_COLUMNS.POSLEDNI_UKONCENI_POJISTENI.value: pl.String,
        SHARED_COLUMNS.DATUM_UMRTI.value: pl.String,
        SHARED_COLUMNS.TYP_UDALOSTI.value: TYP_UDALOSTI,
        SHARED_COLUMNS.DETAIL_UDALOSTI.value: pl.String,
        SHARED_COLUMNS.POCET_BALENI.value: pl.Float64,
        SHARED_COLUMNS.DATUM_UDALOSTI.value: pl.String,
        SHARED_COLUMNS.LEKOVA_FORMA_ZKR.value: pl.String,
        SHARED_COLUMNS.ATC_SKUPINA.value: pl.String,
        SHARED_COLUMNS.SILA.value: pl.String,
        SHARED_COLUMNS.DOPLNEK_NAZVU.value: pl.String,
        SHARED_COLUMNS.LEKOVA_FORMA.value: pl.String,
        SHARED_COLUMNS.LECIVE_LATKY.value: pl.String,
        SHARED_COLUMNS.EQUIV_SLOUCENINA.value: pl.String,
        SHARED_COLUMNS.PREDNISON_EQUIV.value: pl.Float64,
        SHARED_COLUMNS.POCET_V_BALENI.value: pl.Float64,
        SHARED_COLUMNS.POCET_VAKCINACI.value: pl.Int64,
        SHARED_COLUMNS.OCKOVANY.value: pl.Int64,
        SHARED_COLUMNS.POCET_PREDPISU.value: pl.Int64,
        CPZP_COLUMNS.MESIC_NAROZENI.value: pl.Int64,
        CPZP_COLUMNS.ROK_UMRTI.value: pl.Int64,
        CPZP_COLUMNS.MESIC_UMRTI.value: pl.Int64,
        CPZP_COLUMNS.KOD_UDALOSTI.value: pl.String,
        CPZP_COLUMNS.SPECIALIZACE.value: pl.String,
        CPZP_COLUMNS.POLOLETI.value: pl.Int64,
        CPZP_COLUMNS.ROK_ZAHAJENI.value: pl.Int64,
        CPZP_COLUMNS.PORADI.value: pl.Int64,
    }
)

print(schema)
print(schema2)

# Read the CSV file and print its schema
cpzp_df = pl.read_csv(
    "./DATACON_data/CPZP_preskladane.csv",
    null_values=["NA", ""],
    schema=schema2,
)
print(cpzp_df.schema)
