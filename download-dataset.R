library(readr)
library(dplyr)
library(magrittr)

#range <- 1987:2008
range <- 2008

sapply(range, function(year) {
  gsub('%year', year, 'http://stat-computing.org/dataexpo/2009/%year.csv.bz2') %>%
    download.file(paste0(year, '.csv.bz2'), method = 'libcurl')
})

csvs <- lapply(list.files(pattern = '2008\\.csv\\.bz2'), read_csv) %>% bind_rows()

airports <- read_csv('http://stat-computing.org/dataexpo/2009/airports.csv')
