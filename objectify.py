import pickle
import polars as pl
from common.constants.column_types import (
    CPZP_SCHEMA,
    OZP_SCHEMA,
    TYP_UDALOSTI,
)
from common.constants.column_names import SHARED_COLUMNS, CPZP_COLUMNS
from common.constants.objects import (
    Gender,
    PrescriptionType,
    Prescription,
    Person,
    Vaccine,
)
from datetime import datetime

pl.Config.set_tbl_rows(20)
pl.Config.set_tbl_cols(60)


def read_preskladane_data(file_path: str, schema: pl.Schema) -> pl.DataFrame:
    return pl.read_csv(
        file_path,
        null_values=["NA", ""],
        schema=schema,
    )


class DataframeToPersonsClassConverter:
    def __extract_person_info(self, df: pl.DataFrame) -> pl.DataFrame:
        try:
            person_info = df.group_by(SHARED_COLUMNS.ID_POJISTENCE.value).agg(
                [
                    pl.first(SHARED_COLUMNS.POHLAVI.value).alias("gender"),
                    pl.first(SHARED_COLUMNS.ROK_NAROZENI.value).alias("birth_year"),
                    pl.first(CPZP_COLUMNS.MESIC_NAROZENI.value).alias("birth_month"),
                    pl.first(SHARED_COLUMNS.DATUM_UMRTI.value).alias("death_date"),
                    pl.first(SHARED_COLUMNS.POSLEDNI_ZAHAJENI_POJISTENI.value).alias(
                        "Posledni_zahajeni_pojisteni"
                    ),
                    pl.first(SHARED_COLUMNS.POSLEDNI_UKONCENI_POJISTENI.value).alias(
                        "Posledni_ukonceni_pojisteni"
                    ),
                ]
            )
        except Exception as _e:
            person_info = df.group_by(SHARED_COLUMNS.ID_POJISTENCE.value).agg(
                [
                    pl.first(SHARED_COLUMNS.POHLAVI.value).alias("gender"),
                    pl.first(SHARED_COLUMNS.ROK_NAROZENI.value).alias("birth_year"),
                    pl.first(SHARED_COLUMNS.DATUM_UMRTI.value).alias("death_date"),
                    pl.first(SHARED_COLUMNS.POSLEDNI_ZAHAJENI_POJISTENI.value).alias(
                        "Posledni_zahajeni_pojisteni"
                    ),
                    pl.first(SHARED_COLUMNS.POSLEDNI_UKONCENI_POJISTENI.value).alias(
                        "Posledni_ukonceni_pojisteni"
                    ),
                ]
            )

        return person_info

    def __extract_prescriptions(self, df: pl.DataFrame) -> pl.DataFrame:
        try:
            return (
                df.filter(
                    pl.col(SHARED_COLUMNS.TYP_UDALOSTI.value) == TYP_UDALOSTI.PREDPIS
                )
                .filter(pl.col(SHARED_COLUMNS.DATUM_UDALOSTI.value).is_not_null())
                .group_by(SHARED_COLUMNS.ID_POJISTENCE.value)
                .agg(
                    [
                        pl.col(SHARED_COLUMNS.DATUM_UDALOSTI.value).alias(
                            "prescription_dates"
                        ),
                        pl.col(SHARED_COLUMNS.LECIVE_LATKY.value).alias("latka"),
                        pl.col(SHARED_COLUMNS.EQUIV_SLOUCENINA.value).alias(
                            "equiv_sloucenina"
                        ),
                        pl.col(SHARED_COLUMNS.PREDNISON_EQUIV.value).alias(
                            "prednison_equiv"
                        ),
                        pl.col(SHARED_COLUMNS.POCET_BALENI.value).alias("pocet_baleni"),
                        pl.col(SHARED_COLUMNS.POCET_V_BALENI.value).alias(
                            "pocet_v_baleni"
                        ),
                        pl.col(SHARED_COLUMNS.SILA.value).alias("sila"),
                        pl.col(CPZP_COLUMNS.SPECIALIZACE.value).alias("Specializace"),
                        pl.col(SHARED_COLUMNS.ATC_SKUPINA.value).alias("ATC_skupina"),
                        pl.col(SHARED_COLUMNS.LEKOVA_FORMA.value).alias("léková_forma"),
                        pl.col(SHARED_COLUMNS.LEKOVA_FORMA_ZKR.value).alias(
                            "léková_forma_zkr"
                        ),
                    ]
                )
            )
        except Exception as _e:
            return (
                df.filter(
                    pl.col(SHARED_COLUMNS.TYP_UDALOSTI.value) == TYP_UDALOSTI.PREDPIS
                )
                .filter(pl.col(SHARED_COLUMNS.DATUM_UDALOSTI.value).is_not_null())
                .group_by(SHARED_COLUMNS.ID_POJISTENCE.value)
                .agg(
                    [
                        pl.col(SHARED_COLUMNS.DATUM_UDALOSTI.value).alias(
                            "prescription_dates"
                        ),
                        pl.col(SHARED_COLUMNS.LECIVE_LATKY.value).alias("latka"),
                        pl.col(SHARED_COLUMNS.EQUIV_SLOUCENINA.value).alias(
                            "equiv_sloucenina"
                        ),
                        pl.col(SHARED_COLUMNS.PREDNISON_EQUIV.value).alias(
                            "prednison_equiv"
                        ),
                        pl.col(SHARED_COLUMNS.POCET_BALENI.value).alias("pocet_baleni"),
                        pl.col(SHARED_COLUMNS.POCET_V_BALENI.value).alias(
                            "pocet_v_baleni"
                        ),
                        pl.col(SHARED_COLUMNS.SILA.value).alias("sila"),
                        pl.col(SHARED_COLUMNS.ATC_SKUPINA.value).alias("ATC_skupina"),
                        pl.col(SHARED_COLUMNS.LEKOVA_FORMA.value).alias("léková_forma"),
                        pl.col(SHARED_COLUMNS.LEKOVA_FORMA_ZKR.value).alias(
                            "léková_forma_zkr"
                        ),
                    ]
                )
            )

    def __extract_vaccines(self, df: pl.DataFrame) -> pl.DataFrame:
        try:
            return (
                df.filter(
                    pl.col(SHARED_COLUMNS.TYP_UDALOSTI.value) == TYP_UDALOSTI.VAKCINACE
                )
                .filter(pl.col(SHARED_COLUMNS.DATUM_UDALOSTI.value).is_not_null())
                .group_by(SHARED_COLUMNS.ID_POJISTENCE.value)
                .agg(
                    [
                        pl.col(SHARED_COLUMNS.DATUM_UDALOSTI.value).alias(
                            "vaccine_dates"
                        ),
                        pl.col(CPZP_COLUMNS.KOD_UDALOSTI.value).alias("nazev"),
                    ]
                )
            )
        except Exception as _e:
            return (
                df.filter(
                    pl.col(SHARED_COLUMNS.TYP_UDALOSTI.value) == TYP_UDALOSTI.VAKCINACE
                )
                .filter(pl.col(SHARED_COLUMNS.DATUM_UDALOSTI.value).is_not_null())
                .group_by(SHARED_COLUMNS.ID_POJISTENCE.value)
                .agg(
                    [
                        pl.col(SHARED_COLUMNS.DATUM_UDALOSTI.value).alias(
                            "vaccine_dates"
                        ),
                    ]
                )
            )

    def convert(self, df: pl.DataFrame) -> list[Person]:
        persons = []

        # Get unique person info efficiently
        person_info = self.__extract_person_info(df)

        # Get prescriptions efficiently
        prescriptions_df = self.__extract_prescriptions(df)

        # Get vaccinations efficiently
        vaccines_df = self.__extract_vaccines(df)

        # Join all the data together
        combined = person_info.join(
            prescriptions_df, on=SHARED_COLUMNS.ID_POJISTENCE.value, how="left"
        ).join(vaccines_df, on=SHARED_COLUMNS.ID_POJISTENCE.value, how="left")

        for row in combined.iter_rows(named=True):
            person_id = row[SHARED_COLUMNS.ID_POJISTENCE.value]

            # Convert gender
            gender_code = row["gender"]
            gender = Gender.MALE if gender_code == "M" else Gender.FEMALE

            # Create birth date
            birth_year = row["birth_year"]
            birth_month = row.get("birth_month", None)
            born_at = self.__create_birth_date(birth_year, birth_month)

            # Death date
            died_at = row["death_date"]

            # Process prescriptions
            prescriptions = []
            prescription_dates = row["prescription_dates"] or []
            latka = row["latka"] or []
            equiv_sloucenina = row["equiv_sloucenina"] or []
            prednison_equiv = row["prednison_equiv"] or []
            pocet_baleni = row["pocet_baleni"] or []
            pocet_v_baleni = row["pocet_v_baleni"] or []
            sila = row["sila"] or []
            specializace_lekare = row.get("Specializace", None)
            atc_skupina = row["ATC_skupina"] or []
            lekova_forma = row["léková_forma"] or []
            lekova_forma_zkr = row["léková_forma_zkr"] or []
            prescription_types: list[PrescriptionType | None] = []
            for atc_code in atc_skupina:
                if atc_code is None:
                    prescription_types.append(None)
                elif atc_code.startswith("H02"):
                    prescription_types.append(PrescriptionType.KORTIKOID)
                elif atc_code.startswith("L04"):
                    prescription_types.append(PrescriptionType.IMUNOSUPRESSIVE)
                else:
                    prescription_types.append(None)

            for i, date in enumerate(prescription_dates):
                if (
                    sila[i] is not None
                    and pocet_baleni[i] is not None
                    and pocet_v_baleni[i] is not None
                    and prednison_equiv[i] is not None
                    and sila[i] != ""
                ):
                    sila[i] = float(
                        sila[i].replace("MG", "").replace(",", ".").replace("/ML", "")
                    )

                    current_pred_equiv = (
                        prednison_equiv[i]
                        * pocet_v_baleni[i]
                        * pocet_baleni[i]
                        * sila[i]
                    )
                else:
                    current_pred_equiv = 0

                ptype = prescription_types[i]
                if ptype is None:
                    continue
                prescriptions.append(
                    Prescription(
                        date=date,
                        latka=latka[i],
                        equiv_sloucenina=equiv_sloucenina[i],
                        prednison_equiv=current_pred_equiv,
                        specializace_lekare=(
                            specializace_lekare[i]
                            if specializace_lekare is not None
                            else None
                        ),
                        atc_skupina=atc_skupina[i],
                        prescription_type=ptype,
                        lekova_forma=lekova_forma[i],
                        lekova_forma_zkr=lekova_forma_zkr[i],
                    )
                )

            # Process vaccines
            vaccine_dates = row["vaccine_dates"] or []
            nazev = row.get("nazev", None)
            vaccines = []
            for i, vaccine_date in enumerate(vaccine_dates):
                vaccines.append(
                    Vaccine(
                        date=vaccine_date,
                        dose_number=i + 1,  # Dose number starts from 1
                        nazev=nazev[i] if nazev is not None else None,
                    )
                )

            # Create Person object
            person = Person(
                id=person_id,
                gender=gender,
                born_at=born_at,
                zahajeni_pojisteni=row["Posledni_zahajeni_pojisteni"],
                ukonceni_pojisteni=row["Posledni_ukonceni_pojisteni"],
                vaccines=vaccines,
                prescriptions=prescriptions,
                died_at=died_at,
            )

            persons.append(person)
        return persons

    def __create_birth_date(self, year: int, month: int | None) -> datetime:
        month = month if month is not None else 1
        return datetime(year, month, 1)


cpzp_df = read_preskladane_data("./DATACON_data/CPZP_preskladane.csv", CPZP_SCHEMA)
cpzp_persons = DataframeToPersonsClassConverter().convert(cpzp_df)

# save the persons to a pickle file
with open("DATACON_data/cpzp_persons.pkl", "wb") as f:
    pickle.dump(cpzp_persons, f)


ozp_df = read_preskladane_data("./DATACON_data/OZP_preskladane.csv", OZP_SCHEMA)
ozp_persons = DataframeToPersonsClassConverter().convert(ozp_df)

# save the persons to a pickle file
with open("DATACON_data/ozp_persons.pkl", "wb") as f:
    pickle.dump(ozp_persons, f)
