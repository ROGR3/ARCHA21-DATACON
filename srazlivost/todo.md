# Srazlivost (B01) — plán analýzy

## Krok 1 — vizualizace (hotovo)

Měsíční křivky počtu předpisů B01AA/AB/AE/AF/AX + B01 celkem, po věkových
dekádách, po očko/neočko a po specializaci. Viz `generate_charts.py`
(`just charts`), výstupy v `charts/<company>/`.

## Krok 2 — definice "case" (dle konzultace s angiologem, 20.8.2026)

Zaměřujeme se jen na léčbu trombóz a plicních embolií (ne profylaxi po
fibrilaci apod. — ta se předepisuje nekonzistentně, hlavně u praktiků,
kde by se to mohlo splést s pooperační/postkardiální indikací).

U trombóz/embolií se navíc často dává jen jeden předpis na 3 měsíce,
takže případ = **jakýkoliv jednotlivý předpis** v dané síle (ne 2 předpisy
v okně jako u kortikoidů):

- **B01AA (warfarin)** — vyřadit, není úvodní léčba trombóz.
- **B01AB (hepariny)** — jen vyšší dávky, cca 0,6–1 (nebo 6–10, dle škály
  konkrétního léku).
- **B01AE (dabigatran)** — 150 mg.
- **B01AF** — rivaroxaban (Xarelto) 15 nebo 20 mg; apixaban 5 mg a více;
  edoxaban 30 mg a více.

Síla léku je nejlepší parametr pro stratifikaci (léky se dají i preventivně
v nižších dávkách).

### Selekce pacientů

a) rok před sledovaným oknem žádný předpis z B01AA/AB/AE/AF/AX a zároveň
b) nově ve sledovaném období (rok po vs. rok před, stejně u virtuálních
   vakcinací) splní podmínku výše (heparin ve vyšší dávce NEBO dabigatran
   150 NEBO rivaroxaban/apixaban/edoxaban ve výše uvedené síle)
c) spočítat incidenci nových případů rok po mezi očkovanými vs. neočkovanými
   (CI z opakovaných iterací matchingu)

## Nápady do budoucna

- Simulated case-control study.
- Simulated randomized controlled trial (RCT).
- Zvážit zapojení specializace do selekce (např. jen kardiologie/angiologie,
  vyřadit chirurgy) — umožnilo by zkrátit 2měsíční okno a zachytit
  krátkodobější případy. Riziko: pacient dostane první balení už v
  nemocnici a další předpis mu dá praktik — pro nás by to vypadalo jako
  první předpis, i když nemusí být.
- Zvážit získání diagnóz (ICD-10) pro zpřesnění:
  - trombózy: I80.x, I82.x, I26.x
  - CMP: I60–I66
  - infarkty: I21–I22, I24
  - náhlé smrti: I46, R96
  - nespolehlivé/neznámé: R98, R99

## Starší poznámky (kontext)

- Hepariny nasazené do 2 měsíců po zlomenině — riziko kontaminace vzorku
  (jde o profylaxi, ne o léčbu trombózy), řešit zvlášť.
- Antikoagulancia (rozpuštěné proteiny) jsou rizikovější než antiagregancia.
- Očekávaný přechod v čase z B01AA (warfarin) na B01AE/AF (novější
  preparáty) — dobře vidět na křivkách z kroku 1 (AA klesá, AF roste).
