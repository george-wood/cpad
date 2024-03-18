'.__module__.'

box::use(polars[pl])

#' Source of data
#' @export
source <- function() {
  box::use(hash[hash])

  hash(
    p701162 = 2:6,
    p708085 = 1:5
  )
}

#' Path to data
path <- function() {
  box::use(../proc/utility[ls])

  list(
    p701162 = ls("arrest/p701162"),
    p708085 = ls("arrest/p708085")
  )
}

#' Define the schema
#' @export
get_schema <- function() {
  list(
    # p701162
    `PO FIRST NAME` = "character",
    `PO MIDDLE NAME` = "character",
    `PO LAST NAME` = "character",
    `PO RACE` = "character",
    `PO GENDER` = "character",
    `ROLE` = "character",
    `CB` = "character",
    `RD` = "character",
    `ARREST DATE/TIME` = "character",
    `ARRESTEE RACE` = "character",
    `ARRESTEE AGE` = "float64",
    `ARRESTEE RACE` = "character",
    `STATUTE` = "character",
    `CHARGE TYPE` = "character",
    # p708085
    `CB NO` = "character",
    `DATETIME` = "character",
    `EMPLOYEE ROLE` = "character",
    `FIRST NAME` = "character",
    `LAST NAME` = "character",
    `APPOINTED DATE` = "character",
    `YOB` = "integer"
  )
}

#' Alias for column names
alias <- function() {
  list(
    # p701162
    first_name = "PO FIRST NAME",
    middle_initial = "PO MIDDLE NAME",
    last_name = "PO LAST NAME",
    race = "PO RACE",
    gender = "PO GENDER",
    role = "ROLE",
    uid_arrest = "CB",
    rd = "RD",
    dt = "ARREST DATE/TIME",
    civilian_race = "ARRESTEE RACE",
    civilian_age = "ARRESTEE AGE",
    civilian_gender = "ARRESTEE GENDER",
    statute = "STATUTE",
    charge_type = "CHARGE TYPE",
    # p708085
    uid_arrest = "CB NO",
    dt = "DATETIME",
    role = "EMPLOYEE ROLE",
    first_name = "FIRST NAME",
    last_name = "LAST NAME",
    appointed = "APPOINTED DATE",
    yob = "YOB"
  )
}

#' Scan csv with schema, wrangle, and create identifier
query <- function(x) {
  pl$
    scan_csv(
      x,
      dtypes = get_schema(),
      try_parse_dates = FALSE
    )$
    rename(
      intersect(alias(), pl$scan_csv(x)$columns)
    )$
    filter(
      pl$col("uid_arrest")$neq("J")
    )$
    with_columns(
      pl$coalesce(
        pl$
          col("dt")$
          str$to_datetime(
            format = "%d-%b-%Y %H:%M",
            strict = FALSE
          ),
        pl$
          col("dt")$
          str$to_datetime(
            format = "%Y-%m-%d %H:%M:%S",
            strict = FALSE
          )
      )
    )$
    unique()
}

#' Execute read, bind, filter and write dataset
#' @export
build <- function() {
  query(
    path()$p701162
  )$
    join(
      other = query(
        path()$p708085
      ),
      on = c(
        "uid_arrest",
        "dt",
        "role",
        "last_name",
        "first_name"
      ),
      how = "left"
    )$
    with_columns(
      pl$
        col("appointed")$
        str$strptime(
          pl$Date,
          format = "%Y-%m-%d",
          strict = FALSE
        )
    )
}

