'.__module__.'

box::use(
  polars[pl],
  proc/utility[scan_aliased, data_files]
)

#' Path to data
path <- function() {
  list(
    p638148 = data_files("warrant/p638148", reg = "_1")
  )
}

#' Define the schema
get_schema <- function() {
  list(
    WARRANT_NO = pl$String,
    WARRANT_TYPE = pl$String,
    WARRANT_EXECUTED_DATE = pl$String,
    CITY = pl$String,
    STATE = pl$String,
    ZIP = pl$String,
    BEAT = pl$String,
    AREA = pl$String,
    STREET_NO = pl$String,
    STREET_DIR = pl$String,
    STREET_NME = pl$String,
    LAST_NME = pl$String,
    FIRST_NME = pl$String,
    MIDDLE_INITIAL = pl$String,
    YOB = pl$Int32,
    YEAR_APPOINTED = pl$Int32,
    CPD_STAR_NO = pl$String,
    UNIT_NO = pl$String,
    BEAT_NO = pl$String,
    ROLE = pl$String,
    WARRANT_ISSUED_DATE = pl$String,
    ARREST_MADE_I = pl$Boolean,
    PROPERTY_RECOVERED_I = pl$Boolean
  )
}

#' Alias for column names
alias <- function() {
  list(
    WARRANT_NO = "uid_warrant",
    WARRANT_TYPE = "type",
    WARRANT_EXECUTED_DATE = "date",
    CITY = "city",
    STATE = "state",
    ZIP = "zip",
    BEAT = "beat",
    AREA = "area",
    STREET_NO = "street_number",
    STREET_DIR = "street_direction",
    STREET_NME = "street",
    LAST_NME = "last_name",
    FIRST_NME = "first_name",
    MIDDLE_INITIAL = "middle_initial",
    YOB = "yob",
    YEAR_APPOINTED = "appointed_year",
    CPD_STAR_NO = "star",
    UNIT_NO = "unit",
    BEAT_NO = "beat_assignment",
    ROLE = "role",
    WARRANT_ISSUED_DATE = "issued",
    ARREST_MADE_I = "arrest",
    PROPERTY_RECOVERED_I = "property_recovered"
  )
}

#' Read the data, apply schema, and write dataset
#' @export
build <- function() {
  scan_aliased(path()$p638148, get_schema(), alias())$
    with_columns(
      pl$
        col("date")$
        str$to_date(
          format = "%d-%b-%y",
          strict = FALSE
        ),
      pl$
        col("issued")$
        str$to_date(
          format = "%d-%b-%y",
          strict = FALSE
        )
    )$
    unique()
}
