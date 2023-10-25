'.__module__.'

box::use(polars[pl])

#' Source of data
#' @export
source <- function() {
  box::use(
    hash[hash],
    glue[glue]
  )

  hash(
    p602033 = c(
      glue("2014_{0:3}"),
      glue("2015_{0:3}"),
      glue("2016_{0:3}"),
      glue("2017_{0:4}"),
      glue("2018_{0:4}"),
      glue("2019_{0:4}"),
      glue("2020_{0:4}")
    )
  )
}

#' Define the schema
#' @export
get_schema <- function() {
  list(
    AA_DATE = "character",
    UNIT = "character",
    WATCH = "character",
    BEAT = "character",
    CAR_VEHICLE_NUMER = "character",
    START_TIME = "character",
    END_TIME = "character",
    LAST_NME = "character",
    FIRST_NME = "character",
    MIDDLE_INITIAL = "character",
    RANK = "character",
    STAR_NO = "character",
    GENDER = "character",
    RACE = "character",
    YEAR_OF_BIRTH = "float64",
    APPOINTMENT_DATE = "character",
    PRESENT_FOR_DUTY = "character",
    ABSENCE_CD = "character",
    ABSENCE_DESCR = "character",
    MODIFIED_BY_LAST = "character",
    MODIFIED_BY_FIRST = "character",
    MODIFIED_DATE = "character"
  )
}

#' Alias for column names
alias <-
  list(
    date = "AA_DATE",
    unit = "UNIT",
    watch = "WATCH",
    beat = "BEAT",
    vehicle = "CAR_VEHICLE_NUMER",
    dt_start = "START_TIME",
    dt_end = "END_TIME",
    last_name = "LAST_NME",
    first_name = "FIRST_NME",
    middle_initial = "MIDDLE_INITIAL",
    rank = "RANK",
    star = "STAR_NO",
    gender = "GENDER",
    race = "RACE",
    yob = "YEAR_OF_BIRTH",
    appointed = "APPOINTMENT_DATE",
    present_for_duty = "PRESENT_FOR_DUTY",
    absence_code = "ABSENCE_CD",
    absence_description = "ABSENCE_DESCR",
    modified_by_last = "MODIFIED_BY_LAST",
    modified_by_first = "MODIFIED_BY_FIRST",
    modified_date = "MODIFIED_DATE"
  )

#' Parse datetime columns
parser <- function(col) {
  pl$concat_str(
    pl$col(col)$str$zfill(4),
    pl$col("date")
  )$str$strptime(
    datatype = pl$Datetime("ms"),
    format = "%H%M%d-%b-%Y",
    strict = FALSE
  )
}

#' Scan csv with schema, wrangle, and create identifier
query <- function(x) {
  pl$
    scan_csv(
      x,
      overwrite_dtype = get_schema()
    )$
    rename(alias)
}

#' Execute the query
#' @export
build <- function(p602033) {
  pl$
    concat(lapply(p602033, query))$
    with_columns(
      parser("dt_start"),
      parser("dt_end"),
      pl$col("date")$str$strptime(pl$Date, format = "%d-%b-%Y"),
      pl$col("appointed")$str$strptime(pl$Date, format = "%Y-%m-%d"),
      pl$col("present_for_duty")$str$contains("True"),
      pl$col("beat")$str$replace_all(pattern = " |[[:punct:]]", value = ""),
      pl$col("vehicle")$str$strip_chars()
    )$
    with_columns(
      pl$when(
        pl$col("dt_end")$lt(pl$col("dt_start"))
      )$then(
        pl$col("dt_end")$dt$offset_by("1d")
      )$otherwise(
        pl$col("dt_end")
      )
    )$
    group_by(
      pl$all()$exclude("^modified.*$")
    )$
    agg(
      pl$all()$sort_by("modified_date")$last()
    )$
    with_row_count(
      "aid"
    )$
    with_columns(
      pl$col("aid")$cast(pl$Utf8)$str$zfill(8)
    )$
    sort(
      c("dt_start", "dt_end")
    )$
    collect()
}


