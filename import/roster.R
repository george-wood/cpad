'.__module__.'

box::use(
  polars[pl],
  proc/utility[scan_aliased, data_files]
)

#' Path to data
path <- function() {
  list(
    p058155 = data_files("roster/p058155"),
    p540798 = data_files("roster/p540798"),
    p596580 = data_files("roster/p596580")
  )
}

#' Define the schema
get_schema <- function() {
  list(
    # p058155
    `First Name` = pl$String,
    `Middle Initital` = pl$String,
    `Last Name` = pl$String,
    `Appointed Date` = pl$String,
    `D.O.B.` = pl$Int32,
    `Race` = pl$String,
    `Gender` = pl$String,
    # p540798
    `FIRST NAME` = pl$String,
    `MIDDLE INITIAL` = pl$String,
    `LAST NAME` = pl$String,
    `APPOINTED DATE` = pl$String,
    `YEAR OF BIRTH` = pl$Int32,
    `RACE` = pl$String,
    `SEX` = pl$String,
    # p596580
    `FIRST_NME` = pl$String,
    `MIDDLE_INITIAL` = pl$String,
    `LAST_NME` = pl$String,
    `APPOINTED_DATE` = pl$String,
    `YOB` = pl$Int32,
    `RACE_DESCR` = pl$String,
    `SEX_CODE_CD` = pl$String
  )
}

#' Alias for column names
alias <- function() {
  list(
    # p058155
    `First Name` = "first_name",
    `Middle Initital` = "middle_initial",
    `Last Name` = "last_name",
    `Appointed Date` = "appointed",
    `D.O.B.` = "yob",
    `Race` = "race",
    `Gender` = "gender",
    # p540798
    `FIRST NAME` = "first_name",
    `MIDDLE INITIAL` = "middle_initial",
    `LAST NAME` = "last_name",
    `APPOINTED DATE` = "appointed",
    `YEAR OF BIRTH` = "yob",
    `RACE` = "race",
    `SEX` = "gender",
    # p596580
    FIRST_NME = "first_name",
    MIDDLE_INITIAL = "middle_initial",
    LAST_NME = "last_name",
    APPOINTED_DATE = "appointed",
    YOB = "yob",
    RACE_DESCR = "race",
    SEX_CODE_CD = "gender"
  )
}

#' Key columns
key <- function() {
  c(
    "last_name",
    "first_name",
    "middle_initial",
    "appointed",
    "yob",
    "race",
    "gender"
  )
}

#' Read the data, apply schema, and wrangle
#' @export
build <- function() {
  pl$
    concat(
      !!!lapply(
        list(
          path()$p058155,
          path()$p540798,
          path()$p596580
        ),
        \(x)
        scan_aliased(
          x,
          get_schema(),
          alias(),
          missing_columns = "insert"
        )$
          select(key())
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
    pl$when(
      pl$col("appointed")$gt(as.Date("2022-12-31"))
    )$then(
      pl$col("appointed")$dt$offset_by("-100y")
    )$otherwise(
      pl$col("appointed")
    )
  )$unique()
}
