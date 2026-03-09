library(tidyverse)

df = read.csv("~/Projects/ARCHA21-DATACON/DATACON_data/agregovana_data.csv")

df$Datum_udalosti <- as.Date(df$Datum_udalosti)
df$Posledni_zahajeni_pojisteni <- as.Date(df$Posledni_zahajeni_pojisteni)
df$Posledni_ukonceni_pojisteni <- as.Date(df$Posledni_ukonceni_pojisteni)
df$datum_prvniho_ockovani = as.Date(df$datum_prvniho_ockovani)

#jenom vekova kategorie k roku 2021
# df = df %>% filter(between(Rok_narozeni,2006,2009)) #12-15
df = df %>% filter(between(Rok_narozeni,1992,2005)) #16-29
# df = df %>% filter(between(Rok_narozeni,1972,1991)) #30-49
# df = df %>% filter(between(Rok_narozeni,1952,1971)) #50-69

#jenom nikdy neockovani nebo ockovani ve "spravnou" dobu
df = df %>% filter(is.na(datum_prvniho_ockovani) | between(datum_prvniho_ockovani,as.Date("2021-1-1"),
                                                           as.Date("2022-2-28")))
#vynulovat vsechny INJ predpisy (takze momentalne i infuze -> INJ/INF)
df$Equiv_prepocet[startsWith(df$léková_forma_zkr,"INJ")]=0

#pocet ockovani ke kazdemu dni
ockovani_pocty = df %>% filter(Typ_udalosti=="vakcinace") %>% 
  group_by(Id_pojistence) %>% 
  summarise(datum_prvniho_ockovani = unique(datum_prvniho_ockovani),.groups = "drop") %>%
  group_by(datum_prvniho_ockovani) %>% 
  summarise(n_ockovanych = n()) %>% 
  arrange(desc(n_ockovanych))


ggplot(ockovani_pocty,aes(x=datum_prvniho_ockovani,y=n_ockovanych)) +
  geom_line()

ockovani_map = df %>% filter(ockovany==1) %>% 
  mutate(is_before = between(Datum_udalosti,datum_prvniho_ockovani-365,datum_prvniho_ockovani-1),
         is_after = between(Datum_udalosti,datum_prvniho_ockovani,datum_prvniho_ockovani+364)) %>% 
  group_by(Id_pojistence) %>% 
  summarise(datum_prvniho_ockovani=unique(datum_prvniho_ockovani),
            sum_before = sum(Equiv_prepocet[is_before],na.rm = T),
            sum_after = sum(Equiv_prepocet[is_after],na.rm = T))

neockovani_map = expand_grid(unique(df$Id_pojistence[df$ockovany==0]),ockovani_pocty$datum_prvniho_ockovani)
names(neockovani_map)=c("Id_pojistence","referencni_datum")
neockovani_map[,c("sum_before","sum_after")]=NA_real_
# length(unique(df$Id_pojistence[df$ockovany==0]))*length(unique(ockovani_pocty$datum_prvniho_ockovani))
# nrow(neockovani_map)
df_tmp = df %>% select(Id_pojistence,Datum_udalosti,Equiv_prepocet,ockovany) %>% 
    filter(ockovany==0)

{
  print("Zacatek v:")
  print(Sys.time())
  pb=txtProgressBar(min=1,max=nrow(ockovani_pocty),style = 3)
  i=1
  for(date in ockovani_pocty$datum_prvniho_ockovani){
    setTxtProgressBar(pb,i)
    map_tmp = df_tmp %>% 
      mutate(is_before = between(Datum_udalosti,as.Date(date)-365,as.Date(date)-1),
             is_after = between(Datum_udalosti,as.Date(date),as.Date(date)+364)) %>% 
      group_by(Id_pojistence) %>%
      summarise(referencni_datum = as.Date(date),
              sum_before = sum(Equiv_prepocet[is_before],na.rm=T),
              sum_after = sum(Equiv_prepocet[is_after],na.rm=T))
    neockovani_map = neockovani_map %>% rows_patch(map_tmp,by=c("Id_pojistence","referencni_datum"))
    #progress bar indikator
    i=i+1
  }
  close(pb)
  print("Hotovo v:")
  print(Sys.time())
}
#pomoci data table

#pridelani skupin
ockovani_map = ockovani_map %>% mutate(matching_group = if_else(sum_before==0,"0",
                                    if_else(sum_before>5000,"5001+",
                                            sprintf("%d-%d",
                                                    (( (sum_before-1) %/% 25) * 25) + 1,
                                                    (( (sum_before-1) %/% 25) * 25) + 25
                                            ))),
                                    before_group = if_else(sum_before==0,"0",
                                                           if_else(between(sum_before,1,500),"1-500",
                                                                   if_else(between(sum_before,501,5000),"501-5000","5001+"))))

neockovani_map = neockovani_map %>% mutate(matching_group = if_else(sum_before==0,"0",
                                                                if_else(sum_before>5000,"5001+",
                                                                        sprintf("%d-%d",
                                                                                (( (sum_before-1) %/% 25) * 25) + 1,
                                                                                (( (sum_before-1) %/% 25) * 25) + 25
                                                                        ))),
                                       before_group = if_else(sum_before==0,"0",
                                                              if_else(between(sum_before,1,500),"1-500",
                                                                      if_else(between(sum_before,501,5000),"501-5000","5001+"))))

#zapsani do souboru
# write.csv2(ockovani_map,"ockovani_map_12_15.csv",row.names=FALSE)
# write.csv2(ockovani_pocty,"ockovani_pocty_12_15.csv",row.names=FALSE)
# write.csv2(neockovani_map,"neockovani_map_12_15.csv",row.names=FALSE)
# #
write.csv2(ockovani_map,"ockovani_map_16_29.csv",row.names=FALSE)
write.csv2(ockovani_pocty,"ockovani_pocty_16_29.csv",row.names=FALSE)
write.csv2(neockovani_map,"neockovani_map_16_29.csv",row.names=FALSE)
# #
# write.csv2(ockovani_map,"ockovani_map_30_49.csv",row.names=FALSE)
# write.csv2(ockovani_pocty,"ockovani_pocty_30_49.csv",row.names=FALSE)
# write.csv2(neockovani_map,"neockovani_map_30_49.csv",row.names=FALSE)
# #
# write.csv2(ockovani_map,"ockovani_map_50_69.csv",row.names=FALSE)
# write.csv2(ockovani_pocty,"ockovani_pocty_50_69.csv",row.names=FALSE)
# write.csv2(neockovani_map,"neockovani_map_50_69.csv",row.names=FALSE)
