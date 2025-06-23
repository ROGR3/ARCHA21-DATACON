from enum import Enum, StrEnum


class TYP_UDALOSTI(StrEnum):
    PREDPIS = "předpis"
    VAKCINACE = "vakcinace"


class VACCINE_STATUS(Enum):
    OCKOVANY = 1
    NEOCKOVANY = 0
