'.__module__.'

box::use(polars[pl])

#' Source of data
#' @export
source <- function() {
  box::use(hash[hash])

  hash(
    p638148 = 1
  )
}

#' Path to data
path <- function() {
  box::use(../proc/utility[ls])

  list(
    p638148 = ls("warrant/p638148", reg = "_1")
  )
}

#' Define the schema
#' @export
get_schema <- function() {
  list(
    WARRANT_NO = "character",
    WARRANT_TYPE = "character",
    WARRANT_EXECUTED_DATE = "character",
    CITY = "character",
    STATE = "character",
    ZIP = "character",
    BEAT = "character",
    AREA = "character",
    STREET_NO = "character",
    STREET_DIR = "character",
    STREET_NME = "character",
    LAST_NME = "character",
    FIRST_NME = "character",
    MIDDLE_INITIAL = "character",
    YOB = "integer",
    YEAR_APPOINTED = "integer",
    CPD_STAR_NO = "character",
    UNIT_NO = "character",
    BEAT_NO = "character",
    ROLE = "character",
    WARRANT_ISSUED_DATE = "character",
    ARREST_MADE_I = "logical",
    PROPERTY_RECOVERED_I = "logical"
  )
}

#' Alias for column names
alias <- function() {
  list(
    uid_warrant = "WARRANT_NO",
    type = "WARRANT_TYPE",
    date = "WARRANT_EXECUTED_DATE",
    city = "CITY",
    state = "STATE",
    zip = "ZIP",
    beat = "BEAT",
    area = "AREA",
    street_number = "STREET_NO",
    street_direction = "STREET_DIR",
    street = "STREET_NME",
    last_name = "LAST_NME",
    first_name = "FIRST_NME",
    middle_initial = "MIDDLE_INITIAL",
    yob = "YOB",
    appointed_year = "YEAR_APPOINTED",
    star = "CPD_STAR_NO",
    unit = "UNIT_NO",
    beat_assignment = "BEAT_NO",
    role = "ROLE",
    issued = "WARRANT_ISSUED_DATE",
    arrest = "ARREST_MADE_I",
    property_recovered = "PROPERTY_RECOVERED_I"
  )
}

#' Read the data, apply schema, and write dataset
#' @export
build <- function() {
  pl$
    scan_csv(
      path()$p638148,
      dtypes = get_schema(),
      try_parse_dates = FALSE
    )$
    rename(
      alias()
    )$
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
    )
}
