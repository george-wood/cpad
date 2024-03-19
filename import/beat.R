'.__module__.'

box::use(polars[pl])

#' Source of data
#' @export
source <- function() {
  box::use(hash[hash])

  hash(
    p621077 = 1
  )
}

#' Path to data
path <- function() {
  box::use(../proc/utility[ls])

  list(
    p621077 = ls("beat/p621077", reg = "_1")
  )
}

#' Define the schema
#' @export
get_schema <- function() {
  list(
    BEAT = "character",
    DESCR = "character",
    CPD_UNIT_NO = "character",
    UNIT_NAME = "character",
    RADIO_ZONE = "character",
    START_DATE = "character",
    END_DATE = "character"
  )
}

#' Alias for column names
alias <- function(reference) {
  list(
    beat = "BEAT",
    description = "DESCR",
    unit = "CPD_UNIT_NO",
    unit_name = "UNIT_NAME",
    radio_zone = "RADIO_ZONE",
    dt_start = "START_DATE",
    dt_end = "END_DATE"
  )
}

#' Read the data, apply schema, and write dataset
#' @export
build <- function() {
  pl$
    scan_csv(
      path()$p621077,
      dtype = get_schema(),
      try_parse_dates = FALSE
    )$
    rename(
      alias()
    )$
    with_columns(
      pl$
        col("dt_start")$
        str$to_datetime(
          format = "%Y-%m-%dT%H:%M:%S",
          strict = FALSE
        ),
      pl$
        col("dt_end")$
        str$to_datetime(
          format = "%Y-%m-%dT%H:%M:%S",
          strict = FALSE
        )
    )$
    sort("beat")
}
