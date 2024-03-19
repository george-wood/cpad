'.__module__.'

box::use(polars[pl])

#' Source of data
#' @export
source <- function() {
  box::use(hash[hash])

  hash(
    p058155 = 0,
    p540798 = 0,
    p596580 = 1:4
  )
}

#' Path to data
path <- function() {
  box::use(../proc/utility[ls])

  list(
    p058155 = ls("roster/p058155"),
    p540798 = ls("roster/p540798"),
    p596580 = ls("roster/p596580")
  )
}

#' Define the schema
#' @export
get_schema <- function() {
  list(
    # p058155
    `First Name` = "character",
    `Middle Initital` = "character",
    `Last Name` = "character",
    `Appointed Date` = "character",
    `D.O.B.` = "integer",
    `Race` = "character",
    `Gender` = "character",
    # p540798
    `FIRST NAME` = "character",
    `MIDDLE INITIAL` = "character",
    `LAST NAME` = "character",
    `APPOINTED DATE` = "character",
    `YEAR OF BIRTH` = "integer",
    `RACE` = "character",
    `SEX` = "character",
    # p596580
    `FIRST_NME` = "character",
    `MIDDLE_INITIAL` = "character",
    `LAST_NME` = "character",
    `APPOINTED_DATE` = "character",
    `YOB` = "integer",
    `RACE_DESCR` = "character",
    `SEX_CODE_CD` = "character"
  )
}

#' Alias for column names
alias <- function() {
  list(
    # p058155
    first_name = "First Name",
    middle_initial = "Middle Initital",
    last_name = "Last Name",
    appointed = "Appointed Date",
    yob = "D.O.B.",
    race = "Race",
    gender = "Gender",
    # p540798
    first_name = "FIRST NAME",
    middle_initial = "MIDDLE INITIAL",
    last_name = "LAST NAME",
    appointed = "APPOINTED DATE",
    yob = "YEAR OF BIRTH",
    race = "RACE",
    gender = "SEX",
    # p596580
    first_name = "FIRST_NME",
    middle_initial = "MIDDLE_INITIAL",
    last_name = "LAST_NME",
    appointed = "APPOINTED_DATE",
    yob = "YOB",
    race = "RACE_DESCR",
    gender = "SEX_CODE_CD"
  )
}

#' Key columns
key <- function() {
  c("last_name",
    "first_name",
    "middle_initial",
    "appointed",
    "yob",
    "race",
    "gender")
}

#' Read the data, apply schema, and wrangle
#' @export
build <- function() {

  pl$
    concat(
      lapply(
        list(
          path()$p058155,
          path()$p540798,
          path()$p596580
        ),
        \(x)
        pl$
          scan_csv(
            x,
            dtypes = get_schema(),
            try_parse_dates = FALSE
          )$
          rename(
            intersect(alias(), pl$scan_csv(x)$columns)
          )$
          select(
            key()
          )
      ),
      how = "vertical"
    )$
    with_columns(
      pl$
        coalesce(
          pl$
            col("appointed")$
            str$to_date(format = "%m/%d/%y", strict = FALSE),
          pl$
            col("appointed")$
            str$to_date(format = "%Y/%m/%d", strict = FALSE)
        )
    )$
    # century correction for dates
    with_columns(
      pl$
        when(
          pl$col("appointed")$gt(as.Date("2022-12-31"))
        )$
        then(
          pl$col("appointed")$dt$offset_by("-100y")
        )$
        otherwise(
          pl$col("appointed")
        )
    )$
    unique()
}

