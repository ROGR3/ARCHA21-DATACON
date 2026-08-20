from enum import StrEnum


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
