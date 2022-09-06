library(tidyverse)
library(lubridate)
library(logging)

f_to_c <- function(T_F) {5*(T_F - 32)/9}

rh_to_gm3 <- function(rh, T_F) {
  # from relative humidity to absolute
  # returns units of g/m3
  # from https://carnotcycle.wordpress.com/2012/08/04/how-to-convert-relative-humidity-to-absolute-humidity/
  T_C <- f_to_c(T_F)
  100 * 6.112 * exp((17.67*T_C) / (T_C + 243.5)) * rh * 2.1674 / (273.15+T_C)
}

gm3_to_rh <- function(gm3, T_F) {
  # the inverse of rh_to_gm3
  T_C <- f_to_c(T_F)
  num <- gm3 * (273.15+T_C)
  denom <- 100 * 6.112 * 2.1674 * exp((17.67*T_C) / (T_C + 243.5))
  num/denom
}

excess_pints <- function(rh, T_F, rh_target=0.55, m3=650) {
    # 650 \approx m3 of house
    g_at_target <- rh_to_gm3(rh_target, T_F) * m3
    loginfo("G at target = %.2f", g_at_target)
    loginfo("G/m3 at target = %.2f", g_at_target/m3)
    g_current <- rh_to_gm3(rh, T_F) * m3
    loginfo("G current = %.2f", g_current)
    loginfo("G/m3 current = %.2f", g_current/m3)
    (g_current - g_at_target) / 473
}

get_weatherfile <- function(location, base='/home/neriksson/base/nke/darksky-api/summaries') {
    #read the latest file, e.g., sharon_through_2020-07-01.csv.gz
    avail <- sort(list.files(path=base, pattern=location, full.name=TRUE), decreasing=TRUE)
    inf <- avail[1]
    loginfo("reading %s", inf)

    read_csv(inf, col_types='innDicnnnnnnnnnc') %>%
        select(-...1) %>%
        mutate(
            year=year(date),
            doy=yday(date),
            month=month(date, label=TRUE),
            week=week(date),
            mday=mday(date),
            abs_humidity = rh_to_gm3(humidity, temperature),
            ts=date + hours(hour),
            decade=10*round(year/10),
            winter=year(date + days(182))
        )
}
