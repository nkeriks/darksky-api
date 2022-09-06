
library(tidyverse)
library(scales)
source('lib.R')

place <- 'givatayim'
df <- get_weatherfile(place)

high_low_plot <-
    df %>%
    group_by(date, doy, year) %>%
    summarise(low=min(temperature), high=max(temperature)) %>%
    ungroup %>%
    mutate(day = as.Date('2022-01-01') + doy - 1,
           #last_year=(today() - date) < 360
           #last_year=year(today()) == year
           last_year= 2022 == year
           )  %>%
    reshape2::melt(id.vars=c('day', 'date', 'doy', 'last_year', 'year')) %>%
    tbl_df %>%
    print %>%
    ggplot(aes(day, value, group=day)) +
        geom_boxplot() +
        geom_point(data=function(x){x[x$last_year,]}, colour='red', alpha=0.5) +
        geom_line(data=function(x){x[x$last_year,]}, colour='red', alpha=0.5, group=1) +
        facet_wrap(~variable) +
        scale_x_date(breaks = pretty_breaks(24), date_labels = "%d-%b") +   theme(axis.text.x = element_text(angle=45, hjust = 1)) +
        scale_y_continuous(breaks=pretty_breaks(50))

#current_year_plot <-  df %>% filter(year==2021) %>%


noon_plot <- df %>%
    filter(hour==12) %>%
    mutate(day = as.Date('2022-01-01') + doy - 1) %>%
    ggplot(aes(day, temperature, group=day)) + geom_boxplot() + scale_x_date(breaks = pretty_breaks(24), date_labels = "%d-%b")
