'.__module__.'

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

  box::use(
    arrow[schema, string, date32, timestamp]
  )

  schema(
    date = string(),
    unit = string(),
    watch = string(),
    beat = string(),
    vehicle = string(),
    dt_start = string(),
    dt_end = string(),
    last_name = string(),
    first_name = string(),
    middle_initial = string(),
    rank = string(),
    star = string(),
    gender = string(),
    race = string(),
    yob = double(),
    appointed = date32(),
    present_for_duty = string(),
    absence_code = string(),
    absence_description = string(),
    modified_by_last = string(),
    modified_by_first = string(),
    modified_date = timestamp()
  )

}

parser <- function(col, format = "%H%M%d-%b-%Y") {
  box::use(polars[pl])

  pl$concat_str(
    pl$col(col)$str$zfill(4),
    "date"
  )$str$strptime(
    datatype = pl$Datetime("ms"),
    format = format,
    strict = FALSE
  )
}

#' Read the data, apply schema, wrangle, and create identifier
#' @export
build <- function(p602033) {

  box::use(
    arrow[read_csv_arrow],
    polars[pl],
    purrr[map, list_rbind]
  )

  df <-
    map(
      p602033,
      \(x)
      read_csv_arrow(
        file = x,
        schema = get_schema(),
        timestamp_parsers = "%Y-%m-%dT%H:%M:%S",
        skip = 1
      )
    ) |>
    list_rbind() |>
    pl$DataFrame()

  df$with_columns(
    parser("dt_start"),
    parser("dt_end"),
    pl$col("date")$str$strptime(pl$Date, format = "%d-%b-%Y"),
    pl$col("present_for_duty")$str$contains("True"),
    pl$col("beat")$str$replace_all(pattern = " |[[:punct:]]", value = "")
  )$with_columns(
    pl$when(
      pl$col("dt_end") < pl$col("dt_start")
    )$then(
      pl$col("dt_end")$dt$offset_by("1d")
    )$otherwise(
      pl$col("dt_end")
    )
  )$groupby(
    pl$all()$exclude("^modified.*$")
  )$agg(
    pl$all()$sort_by("modified_date")$last()
  )$with_row_count(
    "aid"
  )$with_columns(
    pl$col("aid")$cast(pl$Utf8)$str$zfill(8)
  )$sort(
    c("dt_start", "dt_end")
  )

}


