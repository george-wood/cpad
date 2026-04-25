'.__module__.'

box::use(
  polars[pl],
  proc/utility[scan_aliased, data_files]
)

#' Path to data
path <- function() {
  list(
    p602033 = data_files("assignment/p602033")
  )
}

#' Define the schema
get_schema <- function() {
  list(
    AA_DATE = pl$String,
    UNIT = pl$String,
    WATCH = pl$String,
    BEAT = pl$String,
    CAR_VEHICLE_NUMER = pl$String,
    START_TIME = pl$String,
    END_TIME = pl$String,
    LAST_NME = pl$String,
    FIRST_NME = pl$String,
    MIDDLE_INITIAL = pl$String,
    RANK = pl$String,
    STAR_NO = pl$String,
    GENDER = pl$String,
    RACE = pl$String,
    YEAR_OF_BIRTH = pl$Int32,
    APPOINTMENT_DATE = pl$String,
    PRESENT_FOR_DUTY = pl$String,
    ABSENCE_CD = pl$String,
    ABSENCE_DESCR = pl$String,
    MODIFIED_BY_LAST = pl$String,
    MODIFIED_BY_FIRST = pl$String,
    MODIFIED_DATE = pl$String
  )
}

#' Alias for column names
alias <- function() {
  list(
    AA_DATE = "date",
    UNIT = "unit",
    WATCH = "watch",
    BEAT = "beat",
    CAR_VEHICLE_NUMER = "vehicle",
    START_TIME = "dt_start",
    END_TIME = "dt_end",
    LAST_NME = "last_name",
    FIRST_NME = "first_name",
    MIDDLE_INITIAL = "middle_initial",
    RANK = "rank",
    STAR_NO = "star",
    GENDER = "gender",
    RACE = "race",
    YEAR_OF_BIRTH = "yob",
    APPOINTMENT_DATE = "appointed",
    PRESENT_FOR_DUTY = "present_for_duty",
    ABSENCE_CD = "absence_code",
    ABSENCE_DESCR = "absence_description",
    MODIFIED_BY_LAST = "modified_by_last",
    MODIFIED_BY_FIRST = "modified_by_first",
    MODIFIED_DATE = "modified_date"
  )
}

#' Scan csv with schema, wrangle, and create identifier
#' @export
build <- function() {
  scan_aliased(path()$p602033, get_schema(), alias())$
    with_columns(
      pl$
        col("appointed")$
        str$to_date(format = "%Y-%m-%d", strict = FALSE),
      pl$
        concat_str(pl$col("dt_start")$str$zfill(4), pl$col("date"))$
        str$to_datetime(format = "%H%M%d-%b-%Y", strict = FALSE),
      pl$
        concat_str(pl$col("dt_end")$str$zfill(4), pl$col("date"))$
        str$to_datetime(format = "%H%M%d-%b-%Y", strict = FALSE),
      pl$
        col("date")$
        str$to_date(format = "%d-%b-%Y", strict = FALSE),
      pl$
        col("present_for_duty")$
        str$contains("True"),
      pl$
        col("beat")$
        str$replace_all(pattern = " |[[:punct:]]", value = ""),
      pl$
        col("vehicle")$
        str$strip_chars()
    )$
    with_columns(
      pl$
        when(
          pl$col("dt_end")$lt(pl$col("dt_start"))
        )$
        then(
          pl$col("dt_end")$dt$offset_by("1d")
        )$
        otherwise(
          pl$col("dt_end")
        )
    )$
    drop(
      pl$col("^modified_.*$")
    )$
    unique()$
    sort(
      c("dt_start", "dt_end",
        "date", "star", "unit", "watch", "beat",
        "absence_code", "last_name", "first_name", "middle_initial",
        "vehicle", "rank", "gender", "race", "yob", "appointed",
        "present_for_duty", "absence_description"),
      nulls_last = TRUE
    )$
    with_row_index(
      "aid"
    )$
    with_columns(
      pl$col("aid")$cast(pl$String)$str$zfill(8),
      pl$col("date")$sub(pl$col("appointed"))$dt$total_days()$alias("tenure")
    )
}
