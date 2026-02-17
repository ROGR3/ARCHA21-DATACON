df <- read.csv("./DATACON_data/CPZP_preskladane.csv", na.strings = c("NA", ""), fileEncoding = "UTF-8")
df$Datum_udalosti <- as.Date(df$Datum_udalosti)
df$Posledni_zahajeni_pojisteni <- as.Date(df$Posledni_zahajeni_pojisteni)
df$Posledni_ukonceni_pojisteni <- as.Date(df$Posledni_ukonceni_pojisteni)

persons <- df[
  df$Rok_narozeni >= 1992 & df$Rok_narozeni <= 2005 &
  df$Posledni_zahajeni_pojisteni < as.Date("2015-01-01") &
  (is.na(df$Posledni_ukonceni_pojisteni) | df$Posledni_ukonceni_pojisteni >= as.Date("2023-12-31")) &
  is.na(df$Datum_umrti), ]

vaccines <- persons[persons$Typ_udalosti == "vakcinace", ]
first_vax <- aggregate(Datum_udalosti ~ Id_pojistence, vaccines, min)
names(first_vax) <- c("Id_pojistence", "vax_date")
first_vax <- first_vax[first_vax$vax_date >= as.Date("2021-01-01") & first_vax$vax_date <= as.Date("2022-02-28"), ]

vax_persons <- merge(persons, first_vax, by = "Id_pojistence")

prescriptions <- vax_persons[
  vax_persons$Typ_udalosti == "předpis" &
  (substr(vax_persons$ATC_skupina, 1, 3) == "H02" | substr(vax_persons$ATC_skupina, 1, 3) == "L04") &
  !is.na(vax_persons$`léková_forma_zkr`) & substr(vax_persons$`léková_forma_zkr`, 1, 3) != "INJ", ]

prescriptions$sila_clean <- gsub("MG|/ML", "", prescriptions$síla)
prescriptions$sila_clean <- gsub(",", ".", prescriptions$sila_clean)
prescriptions$sila_float <- suppressWarnings(as.numeric(prescriptions$sila_clean))

prescriptions$Prednison_equiv <- as.numeric(prescriptions$Prednison_equiv)
prescriptions$Pocet_baleni <- as.numeric(prescriptions$Pocet_baleni)
prescriptions$Pocet_v_baleni <- as.numeric(prescriptions$Pocet_v_baleni)

prescriptions$pe <- ifelse(
  !is.na(prescriptions$sila_float) & !is.na(prescriptions$Pocet_baleni) & 
  !is.na(prescriptions$Pocet_v_baleni) & !is.na(prescriptions$Prednison_equiv),
  prescriptions$Prednison_equiv * prescriptions$Pocet_baleni * prescriptions$Pocet_v_baleni * prescriptions$sila_float,
  0
)

prescriptions$is_before <- prescriptions$Datum_udalosti > (prescriptions$vax_date - 365) & 
                           prescriptions$Datum_udalosti < prescriptions$vax_date
prescriptions$is_after <- prescriptions$Datum_udalosti > prescriptions$vax_date & 
                          prescriptions$Datum_udalosti < (prescriptions$vax_date + 365)

prescriptions$pe_before <- ifelse(prescriptions$is_before, prescriptions$pe, 0)
prescriptions$pe_after <- ifelse(prescriptions$is_after, prescriptions$pe, 0)

pe_by_person <- aggregate(cbind(pe_before, pe_after) ~ Id_pojistence + vax_date, prescriptions, sum)

all_people <- merge(first_vax, pe_by_person, by = c("Id_pojistence", "vax_date"), all.x = TRUE)
all_people$pe_before[is.na(all_people$pe_before)] <- 0
all_people$pe_after[is.na(all_people$pe_after)] <- 0

pe_filtered <- all_people[all_people$pe_before <= 5000, ]

daily_pe <- aggregate(cbind(pe_before, pe_after) ~ vax_date, pe_filtered, sum)
names(daily_pe) <- c("vax_date", "total_before", "total_after")
daily_pe <- daily_pe[order(daily_pe$vax_date), ]
daily_pe$ratio <- ifelse(daily_pe$total_before > 0, daily_pe$total_after / daily_pe$total_before, 0)

plot(daily_pe$vax_date, daily_pe$ratio, type = "l", lwd = 1.5,
     xlab = "Datum první vakcinace", ylab = "Poměr PE (po/před)",
     main = "Poměr spotřeby PE rok po/před první vakcinací\nNarozeni 1992-2005")
grid(col = "gray", lty = 1)
