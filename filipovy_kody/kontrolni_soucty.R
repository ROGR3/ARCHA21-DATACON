library(tidyverse)

# results_12_15 <- read.csv2("final_results_12_15.csv")
results_16_29 <- read.csv2("final_results_16_29.csv")
# results_30_49 <- read.csv2("final_results_30_49.csv")
# results_50_69 <- read.csv2("final_results_50_69.csv")

# results_12_15$date <- as.Date(results_12_15$date)
results_16_29$date <- as.Date(results_16_29$date)
# results_30_49$date <- as.Date(results_30_49$date)
# results_50_69$date <- as.Date(results_50_69$date)


filter_by_pe <- function(df, group) {
  df %>%
    {
      if (group != "all") {
        filter(., before_group == group)
      } else {
        .
      }
    } %>%
    group_by(iteration) %>%
    summarise(
      sum_after_ctrl = sum(sum_after_ctrl, na.rm = T),
      sum_before_ctrl = sum(sum_before_ctrl, na.rm = T),
      sum_after_trt = sum(sum_after_trt, na.rm = T),
      sum_before_trt = sum(sum_before_trt, na.rm = T),
      n_matched = sum(n_matched),
      .groups = "drop"
    ) %>%
    mutate(estimand = case_when(
      group == "0" ~ sum_after_trt / sum_after_ctrl,
      TRUE ~
        (sum_after_trt / sum_before_trt) -
        (sum_after_ctrl / sum_before_ctrl)
    )) %>%
    summarise(
      med = median(estimand, na.rm = T),
      prum = mean(estimand, na.rm = T),
      LQR = quantile(estimand, 0.25, na.rm = T),
      UQR = quantile(estimand, 0.75, na.rm = T),
      LCB = quantile(estimand, 0.05, na.rm = T),
      UCB = quantile(estimand, 0.95, na.rm = T),
      n_matched = mean(n_matched),
      .groups = "drop"
    )
}

pe_groups <- c("all", "0", "1-500", "501-5000")
make_pomer_by_age <- function(results_df) {
  purrr::map_dfr(
    pe_groups,
    ~ filter_by_pe(results_df, group = .x) %>%
      mutate(group = .x)
  ) %>%
    mutate(
      across(
        where(is.numeric),
        \(x) round(x, digits = 2)
      )
    )
}


# pomer_12_15 = make_pomer_by_age(results_12_15)
pomer_16_29 <- make_pomer_by_age(results_16_29)
# pomer_30_49 = make_pomer_by_age(results_30_49)
# pomer_50_69 = make_pomer_by_age(results_50_69)

# write.csv2(pomer_12_15,"pomer_12_15.csv",row.names = F)
write.csv2(pomer_16_29, "pomer_16_29.csv", row.names = F)
# write.csv2(pomer_30_49,"pomer_30_49.csv",row.names = F)
# write.csv2(pomer_50_69,"pomer_50_69.csv",row.names = F)
################################################################################
# pomer_12_15_all = filter_by_pe(results_12_15,group = "all") %>% round(digits = 2)
# pomer_16_29_all = filter_by_pe(results_16_29,group = "all") %>% round(digits = 2)
# pomer_30_49_all = filter_by_pe(results_30_49,group = "all") %>% round(digits = 2)
# pomer_50_69_all = filter_by_pe(results_50_69,group = "all") %>% round(digits = 2)
#
# pomer_12_15_pe0 = filter_by_pe(results_12_15,group = "0") %>% round(digits = 2)
# pomer_16_29_pe0 = filter_by_pe(results_16_29,group = "0") %>% round(digits = 2)
# pomer_30_49_pe0 = filter_by_pe(results_30_49,group = "0") %>% round(digits = 2)
# pomer_50_69_pe0 = filter_by_pe(results_50_69,group = "0") %>% round(digits = 2)
#
# pomer_12_15_pe1_500 = filter_by_pe(results_12_15,group = "1-500") %>% round(digits = 2)
# pomer_16_29_pe1_500 = filter_by_pe(results_16_29,group = "1-500") %>% round(digits = 2)
# pomer_30_49_pe1_500 = filter_by_pe(results_30_49,group = "1-500") %>% round(digits = 2)
# pomer_50_69_pe1_500 = filter_by_pe(results_50_69,group = "1-500") %>% round(digits = 2)
#
# pomer_12_15_pe501_5000 = filter_by_pe(results_12_15,group = "501-5000") %>% round(digits = 2)
# pomer_16_29_pe501_5000 = filter_by_pe(results_16_29,group = "501-5000") %>% round(digits = 2)
# pomer_30_49_pe501_5000 = filter_by_pe(results_30_49,group = "501-5000") %>% round(digits = 2)
# pomer_50_69_pe501_5000 = filter_by_pe(results_50_69,group = "501-5000") %>% round(digits = 2)
