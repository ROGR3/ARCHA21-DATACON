df <- read.csv("./DATACON_data/agregovana_data.csv", na.strings = c("NA", ""))
df$Datum_udalosti <- as.Date(df$Datum_udalosti)
df$Posledni_zahajeni_pojisteni <- as.Date(df$Posledni_zahajeni_pojisteni)
df$Posledni_ukonceni_pojisteni <- as.Date(df$Posledni_ukonceni_pojisteni)

# Filtrace
filtered <- df[
  df$Rok_narozeni >= 1992 & df$Rok_narozeni <= 2005 &
  df$Posledni_zahajeni_pojisteni < as.Date("2015-01-01") &
  (is.na(df$Posledni_ukonceni_pojisteni) | df$Posledni_ukonceni_pojisteni > as.Date("2023-12-31")) &
  is.na(df$Datum_umrti) &
  df$Typ_udalosti == "vakcinace" &
  df$Datum_udalosti >= as.Date("2021-01-01") & df$Datum_udalosti <= as.Date("2022-02-28"), 
]

# První vakcinace
first_vaccines <- aggregate(Datum_udalosti ~ Id_pojistence, filtered, min)
names(first_vaccines) <- c("Id_pojistence", "first_vax_date")

# Denní počty
daily_counts <- aggregate(Id_pojistence ~ first_vax_date, first_vaccines, length)
names(daily_counts) <- c("first_vax_date", "count")
daily_counts <- daily_counts[order(daily_counts$first_vax_date), ]

cat(sprintf("Celkem prvních vakcinací: %s\n", format(nrow(first_vaccines), big.mark = ",")))

# Graf
plot(daily_counts$first_vax_date, daily_counts$count, type = "l", lwd = 1.5,
     xlab = "Datum", ylab = "Počet prvních očkování",
     main = "Počet 1. očkování (1.1.2021 - 28.2.2022)\nNarozeni 1992-2005", yaxt = "n")
axis(2, at = seq(0, max(daily_counts$count) + 250, by = 250))
abline(h = seq(0, max(daily_counts$count) + 250, by = 250), col = "gray", lty = 1)
grid(nx = NULL, ny = NA, col = "gray", lty = 1)

