'.__module__.'

box::use(polars[pl])

#' Source of data
#' @export
source <- function() {
  box::use(hash[hash])

  hash(
    p606699 = 3
  )
}

#' Define the schema
#' @export
get_schema <- function() {
  list(
    LAST_NME = "character",
    MI = "character",
    FIRST_NME = "character",
    DESCR = "character",
    STAR_NO = "character",
    RACE = "character",
    SEX = "character",
    BIRTH_YEAR = "integer",
    APPOINTED_DATE = "character",
    ACT_BRANCH = "character",
    ACT_RANK = "character",
    ACT_DATE_DISCHARGED = "character",
    ACT_DISCHARGE_TYPE = "character",
    START_DATE = "character",
    RES_BRANCH = "character",
    RES_RANK = "character",
    RES_DATE_DISCHARGED = "character",
    RES_DISCHARGE_TYPE = "character"
  )
}

#' Alias for column names
alias <- function() {
  list(
    last_name = "LAST_NME",
    middle_initial = "MI",
    first_name = "FIRST_NME",
    description = "DESCR",
    star = "STAR_NO",
    race = "RACE",
    gender = "SEX",
    yob = "BIRTH_YEAR",
    appointed = "APPOINTED_DATE",
    active_branch = "ACT_BRANCH",
    active_rank = "ACT_RANK",
    active_discharged = "ACT_DATE_DISCHARGED",
    active_discharge_type = "ACT_DISCHARGE_TYPE",
    reserve_start = "START_DATE",
    reserve_branch = "RES_BRANCH",
    reserve_rank = "RES_RANK",
    reserve_discharged = "RES_DATE_DISCHARGED",
    reserve_discharge_type = "RES_DISCHARGE_TYPE"
  )
}

#' Read the data, apply schema, and write dataset
#' @export
build <- function(p606699) {
  pl$
    scan_csv(
      p606699,
      dtypes = get_schema(),
      try_parse_dates = FALSE
    )$
    rename(
      alias()
    )$
    with_columns(
      pl$col("appointed")$
        str$strptime(pl$Date, format = "%Y-%m-%d"),
      pl$col("active_discharged")$
        str$strptime(pl$Date, format = "%Y-%m-%d"),
      pl$col("reserve_start")$
        str$strptime(pl$Date, format = "%Y-%m-%d"),
      pl$col("reserve_discharged")$
        str$strptime(pl$Date, format = "%Y-%m-%d")
    )$
    sort(
      "appointed"
    )$
    unique()
}
