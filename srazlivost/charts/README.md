# Antiagregacia (B01) — grafy

Tady jsou grafy počtu předpisů léků na ředění krve (ATC skupina B01) po měsících.

## Co znamenají zkratky léků

- **B01AA** — warfarin
- **B01AB** — hepariny (nízkomolekulární i nefrakcionovaný)
- **B01AD** — altepláza (skoro celá tahle skupina; protein C je v ní jen pár desítek předpisů)
- **B01AE** — dabigatran
- **B01AF** — rivaroxaban, apixaban, edoxaban (tzv. NOAC)
- **B01AX** — ostatní (málo používané)
- **B01 celkem** (černá čárkovaná čára) — součet právě těchhle šesti skupin (ne celé ATC B01, takže tam není B01AC)

## Tři složky nahoře = tři zdroje dat

- `cpzp/` — jen ČPZP
- `ozp/` — jen OZP
- `both_companies/` — obě pojišťovny dohromady

Uvnitř každé z nich je úplně stejná struktura, popsaná níž.

## Dvě časová období

- `2015-2024/` — celý dostupný rozsah dat, popisky osy x po rocích
- `2019-2024/` — jen od roku 2019, popisky osy x po měsících — aby šlo přesně vidět, kdy se co v roce 2021/2022 hýbe (covid vs. konkrétní vlny očkování)

## Muži a ženy

V každém období jsou tři složky a pod každou je stejný strom (popsaný níž):

- `vsichni/` — všichni dohromady
- `muzi/` — jen muži
- `zeny/` — jen ženy

Každý člověk má jedno pohlaví. Pár lidí v ČPZP (asi 1 600 z 1,5 milionu) má v různých řádcích jednou M a jednou F. U nich se bere ta hodnota, která je v jejich řádcích častější.

V `relative/` je jmenovatel jen dané pohlaví. Tedy třeba „předpisů na 1 000 žen ve věku 30–39“.

Příklad cesty: `cpzp/2019-2024/zeny/relative/jen_vyssi_davky/crosstab/nad_sebou/vek_20-29.png`

## Absolutní a relativní počty

V každé složce pohlaví jsou dvě složky a pod nimi je stejný strom jako dřív:

- `absolute/` — počty předpisů
- `relative/` — stejné grafy vydělené velikostí populace, jednotka je **předpisů na 1 000 osob**

Populace jsou všichni lidé v datech pojišťovny. Patří tam i ti, kdo antikoagulancium nikdy nedostali, a i ti, kdo nemají vůbec žádný záznam (ani předpis, ani očkování).

- graf bez rozpadu na očkování: jmenovatel je celá věková skupina
- graf očkovaní vs. neočkovaní: každý panel má svůj jmenovatel, takže jdou srovnat i když je jedna skupina větší
- do věkové dekády člověk patří, když v některém roce daného období měl věk (rok minus rok narození) v té dekádě. Může tak být ve dvou sousedních dekádách

V `absolute/` je **N** = kolik lidí v dané skupině mělo aspoň jeden vykreslený předpis. V `relative/` je **N** = populace ve jmenovateli (v titulku „populace“).

## Tři varianty podle dávky

Hepariny i NOAC se dávají jak preventivně, tak k léčbě. Proto jsou tu tři varianty:

- `vse/` — úplně všechny předpisy, žádné filtrování síly
- `jen_vyssi_davky/` — jen terapeutické dávky. Přesný seznam je v příloze na konci dokumentu
- `bez_heparinu/` — B01AB úplně vyřazené z grafu i ze součtu, ostatní léky bez filtru síly. Heparin má tak vysoká čísla, že ostatní by se v jeho měřítku "plazily po nule"

## Co je v každé z těchto tří variant

- `atc_predpisy_mesicne_<věk>.png` — křivky B01AA/AB/AD/AE/AF/AX + celkem, samostatně pro každou věkovou dekádu (`vek_0-9`, `vek_10-19`, `vek_20-29`, `vek_30-39`, ... `vek_80_plus`) a pro `vsechny_veky`
- `crosstab/vedle_sebe/<věk>.png` — totéž, ale rozdělené na očkované a neočkované, oba panely vedle sebe se stejnou osou y (dobré pro porovnání výšky/velikosti křivek)
- `crosstab/nad_sebou/<věk>.png` — stejná data jako výš, jen panely nad sebou se sdílenou osou x (dobré pro porovnání, KDY přesně se co děje — stejný měsíc je přesně pod sebou)

Očkovaný/neočkovaný = má/nemá kdykoliv v datech aspoň jeden záznam o vakcinaci (tedy "ever vaccinated" za celé sledované období, ne k nějakému konkrétnímu datu).

## Kdy se očkovalo

`vax_period/` (jeden v `cpzp/`, `ozp/` i `both_companies/`) — kolik lidí se v daný den nechalo očkovat. Jedna čára = jedna dávka v pořadí u daného člověka (1. dávka, 2. dávka, … až po poslední, která v datech je, klidně 10.). Všechny dávky jsou v jednom grafu. Číslo je 7denní průměr, ať denní šum neskáče. Zelené pásmo „období vakcinace“ tu není.

Je rozdělený stejně na `vsichni/`, `muzi/` a `zeny/`. Soubory jsou po stejných věkových dekádách jako předpisy (`vek_0-9` … `vek_80_plus`) plus `vsechny_veky`. Věk je rok očkování minus rok narození. N v titulku = kolik lidí v té skupině má aspoň jedno očkování.

## Kolikrát měl kdo předpis

`histogram_vyssi_davky/<rok>.png` — roky 2019, 2020, 2021 a 2022, zvlášť. Tady se muži a ženy nerozlišují. Osa x je počet předpisů **jedné** ATC skupiny u jednoho člověka v tom roce (1, 2, … 12, poslední sloupec je 13 a víc). Osa y je počet lidí s právě tolika předpisy. Barva sloupce je ATC skupina.

Platí stejný filtr jako `jen_vyssi_davky` (příloha na konci). Člověk s nulou v dané skupině v grafu není. Kdo má třeba warfarin i rivaroxaban, je ve dvou skupinách zvlášť, dohromady se to nesčítá.

## Specializace

`specializace_predpisy_mesicne.png` (v každé složce `absolute/` a `relative/`, tedy pro každé období a pohlaví) — počty předpisů podle odbornosti předepisujícího lékaře, top 10 odborností + koš "ostatní". Tohle je za celé B01 (bez rozpadu na jednotlivé léky, tedy včetně AC/AD) a bez dávkového filtrování.

## Příloha: co je "vyšší dávka" ve variantě `jen_vyssi_davky/`

Warfarin (B01AA), altepláza (B01AD) a B01AX se sílou nefiltrují. Všechno, co tu není vypsané jako ponechané, je z téhle varianty pryč.

- **Nadroparin / Fraxiparin** (B01AB06) — ponecháno, když je stříkačka 0,6 ml a větší (0,6 / 0,8 / 1 ml), a to i u Forte. Objem je z velikosti balení, třeba `10X0,6ML`. Stříkačky 0,3 a 0,4 ml jsou profylaxe a jsou pryč. Když objem v záznamu chybí, řádek je pryč.
- **Ostatní hepariny** (enoxaparin, nefrakcionovaný heparin, bemiparin, dalteparin, sulodexid, antitrombin) — v téhle variantě nejsou, i kdyby šlo o vyšší sílu.
- **Dabigatran** (B01AE07) — ponecháno jen 150 mg. 110 mg a 75 mg jsou pryč.
- **Rivaroxaban** (B01AF01) — ponecháno 15 mg a 20 mg. 10 mg, 2,5 mg a sirup 1 mg/ml jsou pryč.
- **Apixaban** (B01AF02) — ponecháno 5 mg a 10 mg. 2,5 mg je pryč. 10 mg v datech zatím není, pravidlo ho ale nechává.
- **Edoxaban** (B01AF03) — ponecháno 30 mg a 60 mg. 15 mg je pryč.

Filtr je podle ATC a síly (u Fraxiparinu podle objemu stříkačky). Léčivá látka, léková forma, obchodní název a registrační číslo jsou u OZP doplněné z číselníku SÚKL (`lookup/ozp_sukl.csv`) a látka je napsaná stejně jako u ČPZP. U 61 starých kódů registr obchodní název už nevede; látka u nich je.
