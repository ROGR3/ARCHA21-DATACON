# SCCS analýza — předpisy B01 kolem očkování

Výstupy skriptu [`sccs_analysis.py`](../sccs_analysis.py). Je to převod SCCS analýzy z kortikoidů ([`kortikoidy/max_vax_analysis.ipynb`](../../kortikoidy/max_vax_analysis.ipynb)) na data o srážlivosti.

```bash
cd srazlivost
../.venv/bin/python sccs_analysis.py --company cpzp   # ozp | both_companies
```

## Postup
1. **Začátek očkování** pro věkovou kohortu × dávku (1–3) = první den, kdy denní počet očkování dosáhl 50 % svého maxima.
2. **Očkovaní** = dostali danou dávku 0–60 dní od začátku očkování své kohorty.
3. Předpisy každého očkovaného se přepočtou na **dny od jeho očkování** (den 0 = den očkování).
4. **Součet před** (den < 0) a **po** (den ≥ 0) v oknech ±30, 60, 90, 180 a 365 dní.
5. **Rozhodné datum** = začátek očkování kohorty + 30 dní (střed 60denního okna).
6. **Neočkovaní** = nikdy neočkovaní. Jejich předpisy se počítají podle kalendáře vůči rozhodnému datu.
7. **Srovnání** poměru po/před (v %) mezi očkovanými a neočkovanými, Fisherův exaktní test.

## Kdo je v analýze
- Živí, pojištění celé období 2015-01-01 až 2023-12-31.
- Aspoň jeden terapeutický předpis B01.
- Věkové kohorty: < 12, 12–29, 30–49, 50–59, 60+. Očkovaní se zařazují podle věku při očkování, neočkovaní podle věku při předpisu (stejně jako u kortikoidů).

## Metriky
- `predpisy` — jen terapeutické dávky (varianta `jen_vyssi_davky` z `generate_charts.py`): warfarin, nadroparin ≥ 0,6 ml, terapeutické síly NOAC, B01AD, B01AX.
- `prvopredpisy` — první předpis B01 u člověka v datech. Preventivní heparin se nepočítá, stejně jako u `charts/.../prvopredpisy/`.

## Rozdíly proti kortikoidům
- Jiné metriky: místo prednison-ekvivalentů a imunosupresiv jsou tu terapeutické předpisy a prvopředpisy.
- Okno je u obou skupin `[-P, P)`. U kortikoidů měli neočkovaní o 1 den víc.
- Injekce se neslučují.
- Navíc je graf po 14denních periodách (±182 dní, první perioda „po“ = den 0–13).

## Soubory
- `<company>/sccs/results_all_periods.csv` — jeden řádek na okno × kohortu × dávku × metriku. Sloupce:
  - `vax_increase` / `novax_increase` = po/před v %
  - `vax_vs_novax_ratio` = poměr těchto poměrů
  - `p_value` = Fisherův test
  - `n_vax_persons` = počet očkovaných v kohortě
- `<company>/sccs/sums/<okno>/<kohorta>-dose_<N>.png` — součty před/po, očkovaní vs neočkovaní.
- `<company>/sccs/periody_14d/<kohorta>-dose_<N>.png` — průběh po 14denních periodách.

## Na co pozor při čtení
- Těsně před očkováním bývá méně prvopředpisů (*healthy vaccinee*: kdo má akutní trombózu, očkování odloží). Nárůst v krátkých oknech je pak částečně návrat z tohoto propadu.
- Testů je hodně a p-hodnoty nejsou korigované na mnohonásobné testování.
- Kohorta < 12 let má jen pár očkovaných, výsledky pro ni nemají vypovídací hodnotu.
