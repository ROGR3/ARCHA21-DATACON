from common.constants.objects import Person
from datetime import date
import pickle

from common.matching_simulation.utils import (
    MatchingAnalysisConfig,
    sum_before_date_pe_for_person,
)


class DataLoader:
    def __init__(self, matching_analysis_config: MatchingAnalysisConfig):
        self.__config = matching_analysis_config
        print(f"Loading persons from {self.__config.pojistovna}")
        self.__persons: list[Person] = self.__load_persons()
        print(f"Loaded {len(self.__persons)} persons")
        self.__person_map: dict[int | str, Person] = {p.id: p for p in self.__persons}
        self.__vax_people: list[Person] = self.__get_vax_people()
        self.__novax_people: list[Person] = self.__get_novax_people()
        self.__never_prescribed_vax_people: list[Person] = (
            self.__get_never_prescribed_vax_people(self.__vax_people)
        )
        self.__zero_pe_suspectible: list[Person] = self.__get_zero_pe_suspects(
            self.__vax_people
        )
        self.__zero_pe_vax_people: list[Person] = self.__get_zero_pe_vax_people(
            self.__vax_people
        )
        self.__one_to_five_hundred_pe_vax_people: list[Person] = (
            self.__get_one_to_five_hundred_pe_vax_people(self.__vax_people)
        )
        self.__five_hundred_to_five_thousand_pe_vax_people: list[Person] = (
            self.__get_five_hundred_to_five_thousand_pe_vax_people(self.__vax_people)
        )

    @property
    def person_map(self) -> dict[int | str, Person]:
        return self.__person_map

    @property
    def vax_people(self) -> list[Person]:
        return self.__vax_people

    @property
    def novax_people(self) -> list[Person]:
        return self.__novax_people

    @property
    def never_prescribed_vax_people(self) -> list[Person]:
        return self.__never_prescribed_vax_people

    @property
    def zero_pe_suspectible(self) -> list[Person]:
        return self.__zero_pe_suspectible

    @property
    def zero_pe_vax_people(self) -> list[Person]:
        return self.__zero_pe_vax_people

    @property
    def one_to_five_hundred_pe_vax_people(self) -> list[Person]:
        return self.__one_to_five_hundred_pe_vax_people

    @property
    def five_hundred_to_five_thousand_pe_vax_people(self) -> list[Person]:
        return self.__five_hundred_to_five_thousand_pe_vax_people

    def __load_persons(self) -> list[Person]:
        if self.__config.pojistovna == "both_companies":
            with open("./DATACON_data/cpzp_persons.pkl", "rb") as f:
                cpzp_persons: list[Person] = pickle.load(f)
            with open("./DATACON_data/ozp_persons.pkl", "rb") as f:
                ozp_persons: list[Person] = pickle.load(f)
            persons = cpzp_persons + ozp_persons
        else:
            with open(
                f"./DATACON_data/{self.__config.pojistovna}_persons.pkl",
                "rb",
            ) as f:
                persons: list[Person] = pickle.load(f)

        for p in persons:
            if p.ukonceni_pojisteni is None:
                p.ukonceni_pojisteni = date(2050, 12, 31)

        for p in persons:
            if p.vaccines:
                p.vaccines[0].date = p.vaccines[0].date - self.__config.day_offset

        return persons

    def __get_vax_people(self) -> list[Person]:
        return [
            p
            for p in self.__persons
            if p.vaccines
            and p.died_at is None  # klidně do roku 2023
            and (
                p.zahajeni_pojisteni < self.__config.zacatek_pojisteni
                and p.ukonceni_pojisteni > self.__config.konec_pojisteni
            )
        ]

    def __get_novax_people(self) -> list[Person]:
        return [
            p
            for p in self.__persons
            if not p.vaccines
            and p.died_at is None
            and (
                p.zahajeni_pojisteni < self.__config.zacatek_pojisteni
                and p.ukonceni_pojisteni > self.__config.konec_pojisteni
            )
        ]

    def __get_never_prescribed_vax_people(
        self, vax_cohort_people: list[Person]
    ) -> list[Person]:
        return [
            p
            for p in vax_cohort_people
            if not any(
                prescription.date < p.vaccines[0].date
                for prescription in p.prescriptions
            )
        ]

    def __get_zero_pe_suspects(self, vax_cohort_people: list[Person]) -> list[Person]:
        return [
            p
            for p in vax_cohort_people
            if sum_before_date_pe_for_person(p, p.vaccines[0].date) == 0
            and any(
                prescription.date < p.vaccines[0].date
                for prescription in p.prescriptions
            )
        ]

    def __get_zero_pe_vax_people(self, vax_cohort_people: list[Person]) -> list[Person]:
        return [
            p
            for p in vax_cohort_people
            if sum_before_date_pe_for_person(p, p.vaccines[0].date) == 0
        ]

    def __get_one_to_five_hundred_pe_vax_people(
        self, vax_cohort_people: list[Person]
    ) -> list[Person]:
        return [
            p
            for p in vax_cohort_people
            if sum_before_date_pe_for_person(p, p.vaccines[0].date) >= 1
            and sum_before_date_pe_for_person(p, p.vaccines[0].date) < 500
        ]

    def __get_five_hundred_to_five_thousand_pe_vax_people(
        self, vax_cohort_people: list[Person]
    ) -> list[Person]:
        return [
            p
            for p in vax_cohort_people
            if sum_before_date_pe_for_person(p, p.vaccines[0].date) >= 500
            and sum_before_date_pe_for_person(p, p.vaccines[0].date) < 5000
        ]
