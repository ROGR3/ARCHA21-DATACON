# Antiagregacia (B01) — grafy

Tady jsou grafy počtu předpisů léků na ředění krve (ATC skupina B01) po měsících.

## Co znamenají zkratky léků

- **B01AA** — warfarin
- **B01AB** — hepariny (nízkomolekulární i nefrakcionovaný)
- **B01AE** — dabigatran
- **B01AF** — rivaroxaban, apixaban, edoxaban (tzv. NOAC)
- **B01AX** — ostatní (málo používané)
- **B01 celkem** (černá čárkovaná čára) — součet právě těchhle pěti skupin (ne celé ATC B01, takže tam není B01AC ani B01AD)

## Tři složky nahoře = tři zdroje dat

- `cpzp/` — jen ČPZP
- `ozp/` — jen OZP
- `both_companies/` — obě pojišťovny dohromady

Uvnitř každé z nich je úplně stejná struktura, popsaná níž.

## Dvě časová období

- `2015-2024/` — celý dostupný rozsah dat, popisky osy x po rocích
- `2019-2024/` — jen od roku 2019, popisky osy x po měsících — aby šlo přesně vidět, kdy se co v roce 2021/2022 hýbe (covid vs. konkrétní vlny očkování)

## Tři varianty podle dávky heparinu (B01AB)

Hepariny se předepisují jak preventivně (po operaci, po zlomenině...), tak k léčbě trombózy — a v datech to jde odlišit jen podle síly/dávky léku. Proto jsou tu tři varianty, které si můžeš mezi sebou porovnat:

- `vse/` — úplně všechny předpisy B01AB, žádné filtrování (nejvíc "syrová" varianta)
- `jen_vyssi_davky/` — B01AB je omezené jen na vyšší (terapeutické) dávky, tedy odfiltrované ty typicky preventivní/profylaktické (např. Heparin 5000 IU, Bemiparin 2500/3500 IU apod.) — je to jen heuristika podle textového pole "síla", takže berte s rezervou. Přesný seznam, co přesně počítáme jako "vyšší dávka", je v příloze na konci dokumentu.
- `bez_heparinu/` — B01AB úplně vyřazené z grafu i ze součtu. Heparin má tak vysoká čísla, že ostatní léky (AA, AE, AF, AX) by se v jeho měřítku "plazily po nule" — tady je jejich vývoj konečně vidět

## Co je v každé z těchto tří variant

- `atc_predpisy_mesicne_<věk>.png` — křivky B01AA/AB/AE/AF/AX + celkem, samostatně pro každou věkovou dekádu (`vek_pod_30`, `vek_30-39`, ... `vek_80_plus`) a pro `vsechny_veky`
- `crosstab/vedle_sebe/<věk>.png` — totéž, ale rozdělené na očkované a neočkované, oba panely vedle sebe se stejnou osou y (dobré pro porovnání výšky/velikosti křivek)
- `crosstab/nad_sebou/<věk>.png` — stejná data jako výš, jen panely nad sebou se sdílenou osou x (dobré pro porovnání, KDY přesně se co děje — stejný měsíc je přesně pod sebou)

Očkovaný/neočkovaný = má/nemá kdykoliv v datech aspoň jeden záznam o vakcinaci (tedy "ever vaccinated" za celé sledované období, ne k nějakému konkrétnímu datu).

V titulku každého grafu je i **N** = kolik lidí v dané skupině mělo aspoň jeden předpis z těchhle pěti kategorií.

## Specializace

`specializace_predpisy_mesicne.png` (jeden v `2015-2024/`, jeden v `2019-2024/`) — počty předpisů podle odbornosti předepisujícího lékaře, top 10 odborností + koš "ostatní". Tohle je za celé B01 (bez rozpadu na jednotlivé léky, tedy včetně AC/AD) a bez dávkového filtrování.

## Příloha: co přesně je "vyšší dávka" u heparinů (B01AB) ve variantě `jen_vyssi_davky/`

Filtrujeme podle dvojice (léčivá látka, síla), jak je zapsaná v datech. Kombinace, které tu nejsou vypsané vůbec (jiná neznámá síla/látka), se konzervativně berou jako **profylaktické** (tedy vyřazené).

- **Enoxaparin** — vyřazeno: 20 mg, 40 mg. Zahrnuto (léčba): 60 mg a víc (60/80/100/120/150 mg)
- **Nadroparin** — vyřazeno: běžný 9500 IU/ml (bez "Forte" — ze samotné síly nejde poznat podaný objem, takže je to nejednoznačné a raději ho vyřazujeme). Zahrnuto: "Forte" 19000 IU/ml
- **Heparin** (nefrakcionovaný) — vyřazeno: 5000 IU/ml (klasická profylaktická podkožní dávka). Žádná síla tu není označená jako léčebná, takže nefrakcionovaný heparin je v `jen_vyssi_davky/` fakticky vždy vyřazený
- **Bemiparin** — vyřazeno: 2500 IU, 3500 IU (standardní fixní profylaktické dávky). Zahrnuto: 12 500 IU a víc (12 500/17 500/25 000 IU — dávkuje se podle váhy, používá se k léčbě)
- **Dalteparin** — zahrnuto: 12 500 IU/ml a 25 000 IU/ml (vysoké koncentrace, používané pro váhově dávkovanou léčbu)
- **Sulodexid** — vyřazeno vždy (jiná indikace, ne léčba akutní trombózy/embolie)
- **Antitrombin III** — vyřazeno vždy (jiná indikace)
