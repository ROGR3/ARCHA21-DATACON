library(tidyverse)

# je potreba nacist neockovani_map, ockovani_map, ockovani_pocty
# neockovani_map = read.csv2("neockovani_map_12_15.csv")
# ockovani_map = read.csv2("ockovani_map_12_15.csv")
# ockovani_pocty = read.csv2("ockovani_pocty_12_15.csv")
#
neockovani_map <- read.csv2("neockovani_map_16_29.csv")
ockovani_map <- read.csv2("ockovani_map_16_29.csv")
ockovani_pocty <- read.csv2("ockovani_pocty_16_29.csv")
#
# neockovani_map = read.csv2("neockovani_map_30_49.csv")
# ockovani_map = read.csv2("ockovani_map_30_49.csv")
# ockovani_pocty = read.csv2("ockovani_pocty_30_49.csv")
#
# neockovani_map = read.csv2("neockovani_map_50_69.csv")
# ockovani_map = read.csv2("ockovani_map_50_69.csv")
# ockovani_pocty = read.csv2("ockovani_pocty_50_69.csv")
#

neockovani_map$referencni_datum <- as.Date(neockovani_map$referencni_datum)
ockovani_map$datum_prvniho_ockovani <- as.Date(ockovani_map$datum_prvniho_ockovani)
ockovani_pocty$datum_prvniho_ockovani <- as.Date(ockovani_pocty$datum_prvniho_ockovani)

neockovani_map <- neockovani_map %>% filter(before_group != "5001+")
ockovani_map <- ockovani_map %>% filter(before_group != "5001+")

# matchovaci funkce
match_one_date <- function(
  ref_date,
  trt_map,
  nontrt_map,
  N
) {
  # filter dany den
  trt_map <- trt_map %>% filter(datum_prvniho_ockovani == ref_date)
  nontrt_map <- nontrt_map %>% filter(referencni_datum == ref_date)

  # containers
  results_list <- list()
  diagnostics <- tibble(
    date = as.Date(character()),
    matching_group = character(),
    n_treated = integer(),
    n_controls = integer(),
    issue = character()
  )

  # iterate over matching_group present across treated
  for (mg in unique(trt_map$matching_group)) {
    trt_g <- trt_map %>% filter(matching_group == mg)
    ctrl_g <- nontrt_map %>% filter(matching_group == mg)

    n_t <- nrow(trt_g)
    n_c <- nrow(ctrl_g)

    # diagnostika
    if (n_c == 0) {
      diagnostics <- diagnostics %>% add_row(
        date = ref_date,
        matching_group = mg,
        n_treated = n_t,
        n_controls = n_c,
        issue = "no_controls"
      )
      next
    }

    if (n_c < n_t) {
      diagnostics <- diagnostics %>% add_row(
        date = ref_date,
        matching_group = mg,
        n_treated = n_t,
        n_controls = n_c,
        issue = "thin_controls"
      )
    }

    # sammpling with replacement
    sampled_idx <- sample(
      seq_len(n_c),
      size = n_t * N,
      replace = TRUE
    )

    sampled_ctrl <- ctrl_g[sampled_idx, ]
    # iteration index
    sampled_ctrl$iteration <- rep(seq_len(N), each = n_t)

    # repeat treated N times (z 1-to-N udelat N-to-N coz je ekviv s 1-to-1 Nkrat)
    trt_rep <- trt_g[rep(seq_len(n_t), times = N), ]

    # agregace
    agg <- tibble(
      date = ref_date,
      before_group = trt_rep$before_group,
      iteration = sampled_ctrl$iteration,
      sum_before_ctrl = sampled_ctrl$sum_before,
      sum_after_ctrl = sampled_ctrl$sum_after,
      sum_before_trt = trt_rep$sum_before,
      sum_after_trt = trt_rep$sum_after
    ) %>%
      group_by(date, before_group, iteration) %>%
      summarise(
        sum_before_ctrl = sum(sum_before_ctrl, na.rm = TRUE),
        sum_after_ctrl = sum(sum_after_ctrl, na.rm = TRUE),
        sum_before_trt = sum(sum_before_trt, na.rm = TRUE),
        sum_after_trt = sum(sum_after_trt, na.rm = TRUE),
        n_matched = n(),
        .groups = "drop"
      )

    results_list[[length(results_list) + 1]] <- agg
  }

  list(
    results = bind_rows(results_list),
    diagnostics = diagnostics
  )
}

# test run
tmp_date <- ockovani_pocty$datum_prvniho_ockovani[1]
out <- match_one_date(
  ref_date = tmp_date,
  trt_map = ockovani_map,
  nontrt_map = neockovani_map,
  N = 10
)

# diagnoza
count(out$results, iteration) # 3 iterace - jedna pro kazdou before skupinu
count(out$results, before_group, iteration)
out$diagnostics

# finalni vypocty
{
  all_results <- list()
  all_diagnostics <- list()

  pb <- txtProgressBar(min = 1, max = nrow(ockovani_pocty), style = 3)
  print("Zacatek v:")
  print(Sys.time())

  for (i in 1:nrow(ockovani_pocty)) {
    d <- ockovani_pocty$datum_prvniho_ockovani[i]

    # progres bar
    setTxtProgressBar(pb, i)
    out <- match_one_date(
      ref_date = d,
      trt_map = ockovani_map,
      nontrt_map = neockovani_map,
      N = 100
    )

    all_results[[i]] <- out$results
    all_diagnostics[[i]] <- out$diagnostics
  }
  close(pb)
  print("Hotovo v:")
  print(Sys.time())

  # bind at the end
  final_results <- bind_rows(all_results)
  final_diagnostics <- bind_rows(all_diagnostics)

  # write.csv2(final_results,"final_results_12_15.csv",row.names = FALSE)
  write.csv2(final_results, "final_results_16_29.csv", row.names = FALSE)
  # write.csv2(final_results,"final_results_30_49.csv",row.names = FALSE)
  # write.csv2(final_results,"final_results_50_69.csv",row.names = FALSE)
}
