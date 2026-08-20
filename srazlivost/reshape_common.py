"""Shared helpers for reshaping new B01/N02BF/N06AA insurer dumps."""

from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Optional

OUTPUT_COLUMNS = [
    "Id_pojistence",
    "Pohlavi",
    "Rok_narozeni",
    "Mesic_narozeni",
    "Posledni_zahajeni_pojisteni",
    "Posledni_ukonceni_pojisteni",
    "Datum_umrti",
    "Typ_udalosti",
    "Detail_udalosti",
    "Nazev",
    "ATC_skupina",
    "Pocet_baleni",
    "síla",
    "Pocet_v_baleni",
    "léková_forma_zkr",
    "léčivé_látky",
    "Specializace",
    "Datum_udalosti",
    "lekova_skupina",
]

# VZP performance codes → vaccine label (from VZP org. opatření 56/57/2020+)
OZP_VACCINE_NAZEV = {
    "99930": "(VZP) COVID-19 - OČKOVÁNÍ - BIONTECH/PFIZER",
    "99931": "(VZP) COVID-19 - OČKOVÁNÍ - MODERNA",
    "99932": "(VZP) COVID-19 - OČKOVÁNÍ - ASTRAZENECA",
    "99933": "(VZP) COVID-19 - OČKOVÁNÍ - JOHNSON & JOHNSON",
    "99934": "(VZP) COVID-19 - OČKOVÁNÍ - CUREVAC - SPOLEČNÝ DISTRIBUTOR",
    "99935": "(VZP) COVID-19 - OČKOVÁNÍ - NOVAVAX - SPOLEČNÝ DISTRIBUTOR",
    "99936": "(VZP) COVID-19 - OČKOVÁNÍ - BIONTECH/PFIZER - SPOLEČNÝ DISTRIBUTOR",
    "99937": "(VZP) COVID-19 - OČKOVÁNÍ - MODERNA - SPOLEČNÝ DISTRIBUTOR",
    "99938": "(VZP) COVID-19 - OČKOVÁNÍ - ASTRAZENECA - SPOLEČNÝ DISTRIBUTOR",
    "99939": "(VZP) COVID-19 - OČKOVÁNÍ - JOHNSON & JOHNSON - SPOLEČNÝ DISTRIBUTOR",
    "99940": "(VZP) COVID-19 - OČKOVÁNÍ - BIONTECH/PFIZER - DĚTI 5 - 11 LET - SPOLEČNÝ DISTRIBUTOR",
    "99941": "(VZP) COVID-19 - OČKOVÁNÍ - NOVAVAX",
    "99942": "(VZP) COVID-19 - OČKOVÁNÍ - SANOFI - SPOLEČNÝ DISTRIBUTOR",
}

# Excel serial above this treated as "still insured" / missing sentinel (e.g. 2958465)
EXCEL_DATE_SENTINEL_MIN = 100_000

_POLOLETI_RE = re.compile(
    r"([12])\.\s*pololet[ií],\s*(\d{4})",
    re.IGNORECASE,
)
_PACK_PRODUCT_RE = re.compile(
    r"^\s*(\d+)\s*[xX×]\s*(\d+)",
)
_PACK_LEADING_RE = re.compile(r"^\s*(\d+)")


def normalize_typ_udalosti(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s.upper() == "NA":
        return None
    low = s.lower()
    if "vakcin" in low or "covid" in low:
        return "vakcinace"
    if "předpis" in low or "predpis" in low:
        return "předpis"
    return s


def normalize_pohlavi(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    s = str(raw).strip().upper()
    if s in {"M", "MUZ", "MUŽ"}:
        return "M"
    if s in {"F", "Z", "ZENA", "ŽENA"}:
        return "F"
    return None


def lekova_skupina_from_atc(atc: Optional[str]) -> Optional[str]:
    if not atc:
        return None
    a = str(atc).strip().upper()
    if a.startswith("B01"):
        return "srazlivost"
    if a.startswith("N02BF") or a.startswith("N06AA"):
        return "neuropatie"
    return None


def parse_cz_float(raw: Optional[str | float | int]) -> Optional[float]:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    s = str(raw).strip().replace(" ", "").replace("\xa0", "")
    if not s or s.upper() == "NA":
        return None
    s = s.replace(",", ".")
    if s.startswith("."):
        s = "0" + s
    try:
        return float(s)
    except ValueError:
        return None


def parse_pololeti_to_date(raw: Optional[str]) -> Optional[date]:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    m = _POLOLETI_RE.match(s)
    if not m:
        return None
    half = int(m.group(1))
    year = int(m.group(2))
    return date(year, 1 if half == 1 else 7, 1)


def parse_iso_date(raw: Optional[str]) -> Optional[date]:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s.upper() == "NA":
        return None
    # already ISO-ish
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        y, m, d = map(int, s.split("-"))
        return date(y, m, d)
    # D.M.YYYY
    m = re.fullmatch(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", s)
    if m:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    return None


def excel_serial_to_date(raw: Optional[str | int | float]) -> Optional[date]:
    if raw is None or raw == "":
        return None
    try:
        serial = int(float(str(raw).strip().replace(",", ".")))
    except ValueError:
        return None
    if serial <= 0 or serial >= EXCEL_DATE_SENTINEL_MIN:
        return None
    return date(1899, 12, 30) + timedelta(days=serial)


def death_from_year_month(
    year: Optional[str | int], month: Optional[str | int]
) -> Optional[date]:
    if year is None or year == "":
        return None
    try:
        y = int(float(str(year)))
        m = int(float(str(month))) if month not in (None, "") else 1
    except ValueError:
        return None
    if y <= 0:
        return None
    m = min(max(m, 1), 12)
    return date(y, m, 1)


def parse_pocet_v_baleni(velikost: Optional[str]) -> Optional[float]:
    """Best-effort pack-count parse from strings like '60X1 I', '3X20', '100'.

    For volumes like '10X0,8ML' take the leading pack count (10), not a broken
    product against a truncated decimal.
    """
    if velikost is None:
        return None
    s = str(velikost).strip()
    if not s:
        return None
    m = _PACK_PRODUCT_RE.match(s)
    if m:
        a = int(m.group(1))
        b = int(m.group(2))
        # "10X0,8ML" → second \d+ matches only "0"; treat as pack count
        after_x = s[m.start(2) :]
        if b == 0 or re.match(r"\d+[,.]", after_x) or re.search(
            r"ML|IU", s, re.IGNORECASE
        ):
            return float(a)
        return float(a * b)
    m = _PACK_LEADING_RE.match(s)
    if m:
        return float(int(m.group(1)))
    return None


def date_to_str(d: Optional[date]) -> Optional[str]:
    return d.isoformat() if d else None


def empty_to_none(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s.upper() == "NA":
        return None
    return s
