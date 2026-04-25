'.__module__.'

box::use(
  polars[pl],
  proc/utility[scan_aliased, data_files]
)

#' Path to data
path <- function() {
  list(
    p621077 = data_files("beat/p621077", reg = "_1")
  )
}

#' Define the schema
get_schema <- function() {
  list(
    BEAT = pl$String,
    DESCR = pl$String,
    CPD_UNIT_NO = pl$String,
    UNIT_NAME = pl$String,
    RADIO_ZONE = pl$String,
    START_DATE = pl$String,
    END_DATE = pl$String
  )
}

#' Alias for column names
alias <- function() {
  list(
    BEAT = "beat",
    DESCR = "description",
    CPD_UNIT_NO = "unit",
    UNIT_NAME = "unit_name",
    RADIO_ZONE = "radio_zone",
    START_DATE = "dt_start",
    END_DATE = "dt_end"
  )
}

#' Read the data, apply schema, and write dataset
#' @export
build <- function() {
  scan_aliased(path()$p621077, get_schema(), alias())$
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
