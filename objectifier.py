import pickle
from datetime import datetime
import polars as pl

from common.constants.column_types import CPZP_SCHEMA, OZP_SCHEMA, TYP_UDALOSTI
from common.constants.column_names import SHARED_COLUMNS, CPZP_COLUMNS
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
    """Reads CSV file with the given schema and handles null values."""
    return pl.read_csv(file_path, null_values=["NA", ""], schema=schema)


class DataframeToPersonsClassConverter:
    """Converts a health insurance dataframe into a list of Person objects."""

    def __extract_person_info(self, df: pl.DataFrame) -> pl.DataFrame:
        """Extracts unique person-level info."""
        agg_exprs = [
            pl.first(SHARED_COLUMNS.POHLAVI.value).alias("gender"),
            pl.first(SHARED_COLUMNS.ROK_NAROZENI.value).alias("birth_year"),
            pl.first(CPZP_COLUMNS.MESIC_NAROZENI.value, default=None).alias(
                "birth_month"
            ),
            pl.first(SHARED_COLUMNS.DATUM_UMRTI.value).alias("death_date"),
            pl.first(SHARED_COLUMNS.POSLEDNI_ZAHAJENI_POJISTENI.value).alias(
                "start_insurance"
            ),
            pl.first(SHARED_COLUMNS.POSLEDNI_UKONCENI_POJISTENI.value).alias(
                "end_insurance"
            ),
        ]
        return df.group_by(SHARED_COLUMNS.ID_POJISTENCE.value).agg(agg_exprs)

    def __extract_events(
        self, df: pl.DataFrame, event_type: TYP_UDALOSTI, columns: dict
    ) -> pl.DataFrame:
        """Generic extractor for prescriptions or vaccines."""
        event_df = (
            df.filter(pl.col(SHARED_COLUMNS.TYP_UDALOSTI.value) == event_type)
            .filter(pl.col(SHARED_COLUMNS.DATUM_UDALOSTI.value).is_not_null())
            .group_by(SHARED_COLUMNS.ID_POJISTENCE.value)
            .agg([pl.col(col).alias(alias) for col, alias in columns.items()])
        )
        return event_df

    def __extract_prescriptions(self, df: pl.DataFrame) -> pl.DataFrame:
        """Extracts prescription-level data."""
        columns = {
            SHARED_COLUMNS.DATUM_UDALOSTI.value: "prescription_dates",
            SHARED_COLUMNS.LECIVE_LATKY.value: "latka",
            SHARED_COLUMNS.EQUIV_SLOUCENINA.value: "equiv_sloucenina",
            SHARED_COLUMNS.PREDNISON_EQUIV.value: "prednison_equiv",
            CPZP_COLUMNS.SPECIALIZACE.value: "specializace",
            SHARED_COLUMNS.ATC_SKUPINA.value: "atc_skupina",
            SHARED_COLUMNS.LEKOVA_FORMA.value: "lekova_forma",
        }
        return self.__extract_events(df, TYP_UDALOSTI.PREDPIS, columns)

    def __extract_vaccines(self, df: pl.DataFrame) -> pl.DataFrame:
        """Extracts vaccination-level data."""
        columns = {
            SHARED_COLUMNS.DATUM_UDALOSTI.value: "vaccine_dates",
            CPZP_COLUMNS.KOD_UDALOSTI.value: "nazev",
        }
        return self.__extract_events(df, TYP_UDALOSTI.VAKCINACE, columns)

    def convert(self, df: pl.DataFrame) -> list[Person]:
        """Converts dataframe into Person objects."""
        persons = []

        # Extract and join person-level, prescription, and vaccination data
        combined = (
            self.__extract_person_info(df)
            .join(
                self.__extract_prescriptions(df),
                on=SHARED_COLUMNS.ID_POJISTENCE.value,
                how="left",
            )
            .join(
                self.__extract_vaccines(df),
                on=SHARED_COLUMNS.ID_POJISTENCE.value,
                how="left",
            )
        )

        for row in combined.iter_rows(named=True):
            person_id = row[SHARED_COLUMNS.ID_POJISTENCE.value]
            born_at = self.__create_birth_date(
                row["birth_year"], row.get("birth_month")
            )
            died_at = row["death_date"]

            # Prescriptions
            prescriptions = self.__build_prescriptions(
                born_at,
                row.get("prescription_dates") or [],
                row.get("latka") or [],
                row.get("equiv_sloucenina") or [],
                row.get("prednison_equiv") or [],
                row.get("specializace") or [],
                row.get("atc_skupina") or [],
                row.get("lekova_forma") or [],
            )

            # Vaccines
            vaccines = self.__build_vaccines(
                born_at,
                row.get("vaccine_dates") or [],
                row.get("nazev") or [],
            )

            # Final Person object
            person_age_cohort = (
                self.__calculate_age_cohort(born_at, died_at)
                if died_at
                else self.__calculate_age_cohort(born_at, datetime.now())
            )

            persons.append(
                Person(
                    id=person_id,
                    gender=Gender.MALE if row["gender"] == "M" else Gender.FEMALE,
                    born_at=born_at,
                    zahajeni_pojisteni=row["start_insurance"],
                    ukonceni_pojisteni=row["end_insurance"],
                    vaccines=vaccines,
                    prescriptions=prescriptions,
                    died_at=died_at,
                    age_cohort=person_age_cohort,
                )
            )

        return persons

    def __build_prescriptions(
        self, born_at, dates, latky, equivs, preds, specs, atcs, forms
    ) -> list[Prescription]:
        """Builds a list of Prescription objects."""
        prescription_types = [
            (
                PrescriptionType.IMUNOSUPRESSIVE
                if (atc and atc.startswith("L04"))
                else PrescriptionType.KORTIKOID
            )
            for atc in atcs
        ]
        return [
            Prescription(
                date=date,
                latka=latky[i],
                equiv_sloucenina=equivs[i],
                prednison_equiv=preds[i],
                specializace_lekare=specs[i] if specs else None,
                atc_skupina=atcs[i],
                age_cohort_at_prescription=self.__calculate_age_cohort(born_at, date),
                prescription_type=prescription_types[i],
                lekova_forma=forms[i],
            )
            for i, date in enumerate(dates)
        ]

    def __build_vaccines(self, born_at, dates, names) -> list[Vaccine]:
        """Builds a list of Vaccine objects."""
        return [
            Vaccine(
                date=date,
                dose_number=i + 1,
                age_cohort=self.__calculate_age_cohort(born_at, date),
                nazev=names[i] if names else None,
            )
            for i, date in enumerate(dates)
        ]

    def __create_birth_date(self, year: int, month: int | None) -> datetime:
        return datetime(year, month if month else 1, 1)

    def __calculate_age_cohort(
        self, birth_date: datetime, event_date: datetime
    ) -> AgeCohort:
        age = event_date.year - birth_date.year
        if (event_date.month, event_date.day) < (birth_date.month, birth_date.day):
            age -= 1

        if age < 12:
            return AgeCohort.LESS_THAN_12
        if age < 30:
            return AgeCohort.BETWEEN_12_AND_30
        if age < 50:
            return AgeCohort.BETWEEN_30_AND_50
        if age < 70:
            return AgeCohort.BETWEEN_50_AND_70
        return AgeCohort.MORE_THAN_70


# ---- Run Conversion ----
cpzp_df = read_preskladane_data("./DATACON_data/CPZP_preskladane.csv", CPZP_SCHEMA)
cpzp_persons = DataframeToPersonsClassConverter().convert(cpzp_df)
with open("DATACON_data/cpzp_persons.pkl", "wb") as f:
    pickle.dump(cpzp_persons, f)

ozp_df = read_preskladane_data("./DATACON_data/OZP_preskladane.csv", OZP_SCHEMA)
ozp_persons = DataframeToPersonsClassConverter().convert(ozp_df)
with open("DATACON_data/ozp_persons.pkl", "wb") as f:
    pickle.dump(ozp_persons, f)
