from enum import StrEnum, Enum
import polars as pl


class SHARED_COLUMNS(StrEnum):
    ID_POJISTENCE = "Id_pojistence"
    POHLAVI = "Pohlavi"
    ROK_NAROZENI = "Rok_narozeni"
    POSLEDNI_ZAHAJENI_POJISTENI = "Posledni_zahajeni_pojisteni"
    POSLEDNI_UKONCENI_POJISTENI = "Posledni_ukonceni_pojisteni"
    DATUM_UMRTI = "Datum_umrti"
    TYP_UDALOSTI = "Typ_udalosti"
    DETAIL_UDALOSTI = "Detail_udalosti"
    POCET_BALENI = "Pocet_baleni"
    DATUM_UDALOSTI = "Datum_udalosti"
    LEKOVA_FORMA_ZKR = "léková_forma_zkr"
    ATC_SKUPINA = "ATC_skupina"
    SILA = "síla"
    DOPLNEK_NAZVU = "doplněk_názvu"
    LEKOVA_FORMA = "léková_forma"
    LECIVE_LATKY = "léčivé_látky"
    EQUIV_SLOUCENINA = "Equiv_sloucenina"
    PREDNISON_EQUIV = "Prednison_equiv"
    POCET_V_BALENI = "Pocet_v_baleni"
    POCET_VAKCINACI = "pocet_vakcinaci"
    OCKOVANY = "ockovany"
    POCET_PREDPISU = "pocet_predpisu"


class CPZP_COLUMNS(StrEnum):
    MESIC_NAROZENI = "Mesic_narozeni"
    ROK_UMRTI = "Rok_umrti"
    MESIC_UMRTI = "Mesic_umrti"
    KOD_UDALOSTI = "Kod_udalosti"
    SPECIALIZACE = "Specializace"
    POLOLETI = "pololeti"
    ROK_ZAHAJENI = "rok_zahajeni"
    PORADI = "poradi"


class OZP_COLUMNS(StrEnum):
    NAZEV = "Nazev"


class TYP_UDALOSTI(StrEnum):
    PREDPIS = "předpis"
    VAKCINACE = "vakcinace"


class VACCINE_STATUS(Enum):
    OCKOVANY = 1
    NEOCKOVANY = 0


def get_shared_schema() -> dict:
    """Get the schema for shared columns."""
    return {
        SHARED_COLUMNS.ID_POJISTENCE.value: pl.Int64,
        SHARED_COLUMNS.POHLAVI.value: pl.String,
        SHARED_COLUMNS.ROK_NAROZENI.value: pl.Int64,
        SHARED_COLUMNS.POSLEDNI_ZAHAJENI_POJISTENI.value: pl.String,
        SHARED_COLUMNS.POSLEDNI_UKONCENI_POJISTENI.value: pl.String,
        SHARED_COLUMNS.DATUM_UMRTI.value: pl.String,
        SHARED_COLUMNS.TYP_UDALOSTI.value: pl.String,
        SHARED_COLUMNS.DETAIL_UDALOSTI.value: pl.Int64,
        SHARED_COLUMNS.POCET_BALENI.value: pl.String,
        SHARED_COLUMNS.DATUM_UDALOSTI.value: pl.String,
        SHARED_COLUMNS.LEKOVA_FORMA_ZKR.value: pl.String,
        SHARED_COLUMNS.ATC_SKUPINA.value: pl.String,
        SHARED_COLUMNS.SILA.value: pl.String,
        SHARED_COLUMNS.DOPLNEK_NAZVU.value: pl.String,
        SHARED_COLUMNS.LEKOVA_FORMA.value: pl.String,
        SHARED_COLUMNS.LECIVE_LATKY.value: pl.String,
        SHARED_COLUMNS.EQUIV_SLOUCENINA.value: pl.String,
        SHARED_COLUMNS.PREDNISON_EQUIV.value: pl.String,
        SHARED_COLUMNS.POCET_V_BALENI.value: pl.String,
        SHARED_COLUMNS.POCET_VAKCINACI.value: pl.Int64,
        SHARED_COLUMNS.OCKOVANY.value: pl.Int64,
        SHARED_COLUMNS.POCET_PREDPISU.value: pl.Int64,
    }


def get_cpzp_specific_schema() -> dict:
    """Get the schema for CPZP-specific columns."""
    return {
        CPZP_COLUMNS.MESIC_NAROZENI.value: pl.Int64,
        CPZP_COLUMNS.ROK_UMRTI.value: pl.String,
        CPZP_COLUMNS.MESIC_UMRTI.value: pl.String,
        CPZP_COLUMNS.KOD_UDALOSTI.value: pl.String,
        CPZP_COLUMNS.SPECIALIZACE.value: pl.String,
        CPZP_COLUMNS.POLOLETI.value: pl.Int64,
        CPZP_COLUMNS.ROK_ZAHAJENI.value: pl.Int64,
        CPZP_COLUMNS.PORADI.value: pl.Int64,
    }


def get_ozp_specific_schema() -> dict:
    """Get the schema for OZP-specific columns."""
    return {
        OZP_COLUMNS.NAZEV.value: pl.String,
    }


# Build complete schemas by combining shared and specific columns
CPZP_SCHEMA: pl.Schema = pl.Schema(
    {
        **get_shared_schema(),
        **get_cpzp_specific_schema(),
    }
)

OZP_SCHEMA: pl.Schema = pl.Schema(
    {
        **get_shared_schema(),
        **get_ozp_specific_schema(),
    }
)

# Update OZP schema to use correct data types for specific columns
OZP_SCHEMA = pl.Schema(
    {
        **{
            k: v
            for k, v in get_shared_schema().items()
            if k != SHARED_COLUMNS.ID_POJISTENCE.value
        },
        SHARED_COLUMNS.ID_POJISTENCE.value: pl.String,  # OZP uses String for ID
        SHARED_COLUMNS.TYP_UDALOSTI.value: TYP_UDALOSTI,  # OZP uses enum
        SHARED_COLUMNS.POCET_BALENI.value: pl.Int64,  # OZP uses Int64
        SHARED_COLUMNS.PREDNISON_EQUIV.value: pl.Int64,  # OZP uses Int64
        SHARED_COLUMNS.POCET_V_BALENI.value: pl.Int64,  # OZP uses Int64
        **get_ozp_specific_schema(),
    }
)
