'.__module__.'

box::use(
  polars[pl],
  hash[hash],
  proc/utility[scan_aliased, ls]
)

#' Source of data
#' @export
source <- function() {
  hash(
    p606699 = 3
  )
}

#' Path to data
path <- function() {
  list(
    p606699 = ls("military", reg = "_3")
  )
}

#' Define the schema
#' @export
get_schema <- function() {
  list(
    LAST_NME = pl$String,
    MI = pl$String,
    FIRST_NME = pl$String,
    DESCR = pl$String,
    STAR_NO = pl$String,
    RACE = pl$String,
    SEX = pl$String,
    BIRTH_YEAR = pl$Int32,
    APPOINTED_DATE = pl$String,
    ACT_BRANCH = pl$String,
    ACT_RANK = pl$String,
    ACT_DATE_DISCHARGED = pl$String,
    ACT_DISCHARGE_TYPE = pl$String,
    START_DATE = pl$String,
    RES_BRANCH = pl$String,
    RES_RANK = pl$String,
    RES_DATE_DISCHARGED = pl$String,
    RES_DISCHARGE_TYPE = pl$String
  )
}

#' Alias for column names
alias <- function() {
  list(
    LAST_NME = "last_name",
    MI = "middle_initial",
    FIRST_NME = "first_name",
    DESCR = "description",
    STAR_NO = "star",
    RACE = "race",
    SEX = "gender",
    BIRTH_YEAR = "yob",
    APPOINTED_DATE = "appointed",
    ACT_BRANCH = "active_branch",
    ACT_RANK = "active_rank",
    ACT_DATE_DISCHARGED = "active_discharged",
    ACT_DISCHARGE_TYPE = "active_discharge_type",
    START_DATE = "reserve_start",
    RES_BRANCH = "reserve_branch",
    RES_RANK = "reserve_rank",
    RES_DATE_DISCHARGED = "reserve_discharged",
    RES_DISCHARGE_TYPE = "reserve_discharge_type"
  )
}

#' Read the data, apply schema, and write dataset
#' @export
build <- function() {
  scan_aliased(path()$p606699, get_schema(), alias())$
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
