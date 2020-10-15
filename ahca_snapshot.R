#! /usr/bin/env Rscript
options(error = NULL)
library(git2rdata)

# Functions-----------------------------------------------------------------
`%>%` <- magrittr::`%>%`

logger <- function(..., .timestamp = ts_now()) {
  ts_now <- function() {
    ts_now <- lubridate::now("America/New_York")
    strftime(ts_now, '%FT%H%M%S', tz = "America/New_York")
  }
  
  message("[", .timestamp, "] ", ...)
}

ahca_sources <- c("covid-hospitalizations", "icu-beds-county", "hospital-beds-county", "hospital-beds-hospital", "icu-beds-hospital")

ahca_data_url <- function(which = "covid-hospitalizations") {
  switch(
    match.arg(which, ahca_sources),
    "covid-hospitalizations" = "https://bi.ahca.myflorida.com/t/ABICC/views/Public/COVIDHospitalizationsCounty.csv",
    "hospital-beds-county" = "https://bi.ahca.myflorida.com/t/ABICC/views/Public/HospitalBedsCounty.csv",
    "icu-beds-county" = "https://bi.ahca.myflorida.com/t/ABICC/views/Public/ICUBedsCounty.csv",
    "hospital-beds-hospital" = "https://bi.ahca.myflorida.com/t/ABICC/views/Public/HospitalBedsHospital.csv",
    "icu-beds-hospital" = "https://bi.ahca.myflorida.com/t/ABICC/views/Public/ICUBedsHospital.csv",
    stop("Not available")
  )
}

ahca_get_data <- function(which) {
  x <- readr::read_csv(ahca_data_url(which), col_types = readr::cols(.default = "c"))
  x <- readr::type_convert(x)
  janitor::clean_names(x)
}


# Prepare for update -------------------------------------------------------
ts_now <- lubridate::now("America/New_York")
ts_now_str <- strftime(ts_now, '%F %T', tz = "America/New_York")

logger("ACHA FLorida Hospital Data Update Start", .timestamp = ts_now_str)

msgs <- c()

# Get AHCA Data --------------------------------------------------------------

for (ahca_source in ahca_sources) {
  ahca_data <- purrr::safely(ahca_get_data)(ahca_source)
  if (is.null(ahca_data$error) && nrow(ahca_data$result)) {
    x <- ahca_data$result %>% 
      dplyr::mutate(timestamp = ts_now) %>%
      dplyr::select(timestamp, dplyr::everything())
    write_vc(
      x,
      paste0("data/", ahca_source), 
      root = git2r::repository(), 
      stage = TRUE,
      sorting = intersect(c("county", "file_number", "measure_names"), names(x)),
      optimize = TRUE, 
      strict = FALSE,
      session = FALSE
    )
    logger("Got ", ahca_source)
  } else {
    msg <- glue::glue("Unable to get {ahca_source} from AHCA dashboard")
    logger(msg, ": ", ahca_data$error$message)
    msgs <- c(msgs, msg)
  }
}

if (git2r::in_repository()) {
  has_staged <- rlang::has_length(git2r::status()$staged)
  if (has_staged) {
    git2r::commit(message = glue::glue("[snapshot] {ts_now_str}\n\n{paste(msgs, collapse = '\n')}"))
    git2r::push(credentials = git2r::cred_token())
  }
}

logger("COMPLETE ---------------------------------------")
