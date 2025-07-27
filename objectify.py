import pickle
from dataclasses import dataclass
from enum import StrEnum
from datetime import datetime, timedelta
import polars as pl
from common.constants.column_types import (
    CPZP_SCHEMA,
    OZP_SCHEMA,
    POHLAVI_CPZP,
    TYP_UDALOSTI,
)
from common.constants.column_names import SHARED_COLUMNS, OZP_COLUMNS, CPZP_COLUMNS
import matplotlib.pyplot as plt
import numpy as np
from common.constants.objects import (
    AgeCohort,
    Gender,
    PrescriptionType,
    Prescription,
    Person,
    Vaccine,
)

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
                ]
            )
        except Exception as _e:
            person_info = df.group_by(SHARED_COLUMNS.ID_POJISTENCE.value).agg(
                [
                    pl.first(SHARED_COLUMNS.POHLAVI.value).alias("gender"),
                    pl.first(SHARED_COLUMNS.ROK_NAROZENI.value).alias("birth_year"),
                    pl.first(SHARED_COLUMNS.DATUM_UMRTI.value).alias("death_date"),
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
                        pl.col(CPZP_COLUMNS.SPECIALIZACE.value).alias("Specializace"),
                        pl.col(SHARED_COLUMNS.ATC_SKUPINA.value).alias("ATC_skupina"),
                        pl.col(SHARED_COLUMNS.LEKOVA_FORMA.value).alias("léková_forma"),
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
                        pl.col(SHARED_COLUMNS.ATC_SKUPINA.value).alias("ATC_skupina"),
                        pl.col(SHARED_COLUMNS.LEKOVA_FORMA.value).alias("léková_forma"),
                    ]
                )
            )

    def __extract_vaccines(self, df: pl.DataFrame) -> pl.DataFrame:
        return (
            df.filter(
                pl.col(SHARED_COLUMNS.TYP_UDALOSTI.value) == TYP_UDALOSTI.VAKCINACE
            )
            .filter(pl.col(SHARED_COLUMNS.DATUM_UDALOSTI.value).is_not_null())
            .group_by(SHARED_COLUMNS.ID_POJISTENCE.value)
            .agg([pl.col(SHARED_COLUMNS.DATUM_UDALOSTI.value).alias("vaccine_dates")])
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

        # Convert to Person objects
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
            specializace_lekare = row.get("Specializace", None)
            atc_skupina = row["ATC_skupina"] or []
            lekova_forma = row["léková_forma"] or []
            prescription_types = []
            for atc_code in atc_skupina:
                if atc_code is None:
                    prescription_types.append(PrescriptionType.KORTIKOID)
                    continue
                if atc_code.startswith("L04"):
                    prescription_types.append(PrescriptionType.IMUNOSUPRESSIVE)
                else:
                    prescription_types.append(PrescriptionType.KORTIKOID)

            for i, date in enumerate(prescription_dates):
                age_cohort = self.__calculate_age_cohort(born_at, date)
                prescriptions.append(
                    Prescription(
                        date=date,
                        latka=latka[i],
                        equiv_sloucenina=equiv_sloucenina[i],
                        prednison_equiv=prednison_equiv[i],
                        specializace_lekare=(
                            specializace_lekare[i]
                            if specializace_lekare is not None
                            else None
                        ),
                        atc_skupina=atc_skupina[i],
                        age_cohort_at_prescription=age_cohort,
                        prescription_type=prescription_types[i],
                        lekova_forma=lekova_forma[i],
                    )
                )

            # Process vaccines
            vaccine_dates = row["vaccine_dates"] or []
            vaccines = []
            for i, vaccine_date in enumerate(vaccine_dates):
                age_cohort_at_vaccination = self.__calculate_age_cohort(
                    born_at, vaccine_date
                )
                vaccines.append(
                    Vaccine(
                        date=vaccine_date,
                        dose_number=i + 1,  # Dose number starts from 1
                        age_cohort=age_cohort_at_vaccination,
                    )
                )

            # Create Person object
            person_age_cohort = (
                self.__calculate_age_cohort(born_at, died_at)
                if died_at is not None
                else self.__calculate_age_cohort(born_at, datetime.now())
            )
            person = Person(
                id=person_id,
                gender=gender,
                born_at=born_at,
                vaccines=vaccines,
                prescriptions=prescriptions,
                died_at=died_at,
                age_cohort=person_age_cohort,
            )

            persons.append(person)

        return persons

    def __create_birth_date(self, year: int, month: int | None) -> datetime:
        month = month if month is not None else 1
        return datetime(year, month, 1)

    def __calculate_age_cohort(
        self, birth_date: datetime, event_date: datetime
    ) -> AgeCohort:
        age = event_date.year - birth_date.year

        if event_date.month < birth_date.month or (
            event_date.month == birth_date.month and event_date.day < birth_date.day
        ):
            age -= 1

        if age < 12:
            return AgeCohort.LESS_THAN_12
        elif age < 30:
            return AgeCohort.BETWEEN_12_AND_30
        elif age < 50:
            return AgeCohort.BETWEEN_30_AND_50
        elif age < 70:
            return AgeCohort.BETWEEN_50_AND_70
        else:
            return AgeCohort.MORE_THAN_70

        # if age < 12:
        #     return AgeCohort.LESS_THAN_12
        # elif age < 16:
        #     return AgeCohort.BETWEEN_12_AND_16
        # elif age < 30:
        #     return AgeCohort.BETWEEN_16_AND_30
        # elif age < 35:
        #     return AgeCohort.BETWEEN_30_AND_35
        # elif age < 40:
        #     return AgeCohort.BETWEEN_35_AND_40
        # elif age < 45:
        #     return AgeCohort.BETWEEN_40_AND_45
        # elif age < 50:
        #     return AgeCohort.BETWEEN_45_AND_50
        # elif age < 55:
        #     return AgeCohort.BETWEEN_50_AND_55
        # elif age < 60:
        #     return AgeCohort.BETWEEN_55_AND_60
        # elif age < 65:
        #     return AgeCohort.BETWEEN_60_AND_65
        # elif age < 70:
        #     return AgeCohort.BETWEEN_65_AND_70
        # elif age < 75:
        #     return AgeCohort.BETWEEN_70_AND_75
        # elif age < 80:
        #     return AgeCohort.BETWEEN_75_AND_80
        # else:
        #     return AgeCohort.MORE_THAN_80


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
