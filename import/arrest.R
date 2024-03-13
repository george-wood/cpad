'.__module__.'

box::use(polars[pl])

#' Source of data
#' @export
source <- function() {
  box::use(hash[hash])

  hash::hash(
    p701162 = 2:6,
    p708085 = 1:5
  )
}

#' Define the schema
#' @export
get_schema <- function(reference) {

  schm <-
    list(
      p701162 = list(
        `PO FIRST NAME` = "character",
        `PO MIDDLE NAME` = "character",
        `PO LAST NAME` = "character",
        `PO RACE` = "character",
        `PO GENDER` = "character",
        ROLE = "character",
        CB = "character",
        RD = "character",
        `ARREST DATE/TIME` = "character", # timestamp
        `ARRESTEE RACE` = "character",
        `ARRESTEE AGE` = "float64",
        `ARRESTEE RACE` = "character",
        STATUTE = "character",
        `CHARGE TYPE` = "character"
      ),
      p708085 = list(
        `CB NO` = "character",
        DATETIME = "character", # timestamp
        `EMPLOYEE ROLE` = "character",
        `FIRST NAME` = "character",
        `LAST NAME` = "character",
        `APPOINTED DATE` = "character", # date32
        `YOB` = "float64"
      )
    )

  if (missing(reference)) {
    schm
  } else {
    schm[[reference]]
  }

}

#' Alias for column names
alias <- function(reference) {

  als <-
    list(
      p701162 = list(
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
        charge_type = "CHARGE TYPE"
      ),
      p708085 = list(
        uid_arrest = "CB NO",
        dt = "DATETIME",
        role = "EMPLOYEE ROLE",
        first_name = "FIRST NAME",
        last_name = "LAST NAME",
        appointed = "APPOINTED DATE",
        yob = "YOB"
      )
    )

  if (missing(reference)) {
    als
  } else {
    als[[reference]]
  }

}

#' Scan csv with schema, wrangle, and create identifier
query <- function(x, reference) {
  pl$
    scan_csv(
      x,
      dtypes = get_schema(reference),
      try_parse_dates = FALSE
    )$
    rename(
      alias(reference)
    )$
    filter(
      pl$col("uid_arrest")$neq("J")
    )$
    with_columns(
      pl$coalesce(
        pl$
          col("dt")$
          str$strptime(
            pl$Datetime(),
            format = "%d-%b-%Y %H:%M",
            strict = FALSE
          ),
        pl$
          col("dt")$
          str$strptime(
            pl$Datetime(),
            format = "%Y-%m-%d %H:%M:%S",
            strict = FALSE
          )
      )
    )$
    unique()
}

#' Execute read, bind, filter and write dataset
#' @export
build <- function(p701162, p708085) {
  query(
    p701162,
    reference = "p701162"
  )$
    join(
      other = query(
        p708085,
        reference = "p708085"
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



