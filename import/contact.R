'.__module__.'

box::use(polars[pl])

#' Source of data
#' @export
source <- function() {
  box::use(hash[hash])

  hash(
    p058306 = 2012:2015
  )
}

#' Define the schema
#' @export
get_schema <- function() {
  list(
    DATE = "character",
    TIME = "character",
    `ST NUM` = "character",
    DIR = "character",
    `STREET NAME` = "character",
    DIST = "character",
    BEAT = "character",
    `CONTACT TYPE DESCRIPTION` = "character",
    `1st P.O. LAST NAME` = "character",
    `1st P.O. FIRST NAME` = "character",
    `1ST P.O. SEX` = "character",
    `1ST P.O. RACE` = "character",
    `1st P.O. AGE` = "integer",
    `2nd P.O. LAST NAME ` = "character",
    `2nd P.O.FIRST NAME ` = "character",
    `2ND P.O. SEX` = "character",
    `2ND P.O. RACE` = "character",
    `2nd P.O. AGE `  = "integer",
    `SUBJECT SEX` = "character",
    `SUBJECT RACE` = "character",
    `SUBJECT AGE` = "character",
    `SUBJECT HEIGHT` = "float64",
    `SUBJECT WEIGHT` = "float64",
    `SUBJECT BUILD `  = "character",
    `SUBJECT HAIRCOLOR` = "character",
    `SUBJECT HAIRSTYLE` = "character",
    `SUBJECT COMPLEXION` = "character",
    `SUBJECT CLOTHING DESCRIPTION` = "character"
  )
}

#' Alias for column names
alias <- function() {
  list(
    date = "DATE",
    dt = "TIME",
    street_number = "ST NUM",
    direction = "DIR",
    street = "STREET NAME",
    district = "DIST",
    beat = "BEAT",
    contact_type = "CONTACT TYPE DESCRIPTION",
    first.last_name = "1st P.O. LAST NAME",
    first.first_name = "1st P.O. FIRST NAME",
    first.gender = "1ST P.O. SEX",
    first.race = "1ST P.O. RACE",
    first.age = "1st P.O. AGE",
    second.last_name = "2nd P.O. LAST NAME ",
    second.first_name = "2nd P.O.FIRST NAME ",
    second.gender = "2ND P.O. SEX",
    second.race = "2ND P.O. RACE",
    second.age = "2nd P.O. AGE ",
    civilian_gender = "SUBJECT SEX",
    civilian_race = "SUBJECT RACE",
    civilian_age = "SUBJECT AGE",
    civilian_height = "SUBJECT HEIGHT",
    civilian_weight = "SUBJECT WEIGHT",
    civilian_build = "SUBJECT BUILD ",
    civilian_hair_color = "SUBJECT HAIRCOLOR",
    civilian_hairstyle = "SUBJECT HAIRSTYLE",
    civilian_complexion = "SUBJECT COMPLEXION",
    civilian_clothing = "SUBJECT CLOTHING DESCRIPTION"
  )
}

#' Read the data, apply schema, and write dataset
#' @export
query <- function(p058306) {
  pl$
    scan_csv(
      p058306,
      dtypes = get_schema(),
      try_parse_dates = FALSE
    )$
    rename(
      alias()
    )$
    with_row_index(
      "uid_contact"
    )$
    with_columns(
      pl$concat_str(pl$lit("c"), "uid_contact")$alias("uid_contact"),
      pl$concat_str("dt", "date")
    )$
    with_columns(
      pl$coalesce(
        pl$
          col("dt")$
          str$to_datetime(
            format = "%H:%M:%S%Y-%m-%d",
            strict = FALSE
          ),
        pl$
          col("dt")$
          str$to_datetime(
            format = "%H:%M%d-%b-%y",
            strict = FALSE
          )
      ),
      pl$coalesce(
        pl$
          col("date")$
          str$to_date(
            format = "%Y-%m-%d",
            strict = FALSE
          ),
        pl$
          col("date")$
          str$to_date(
            format = "%d-%b-%y",
            strict = FALSE
          )
      )
    )
}


#' Melt the officer information, pivot by role, and rejoin
#' @export
melt <- function(q) {
  q$
    select(
      "uid_contact",
      grep("\\.", names(alias()), invert = TRUE, value = TRUE)
    )$
    join(
      other =
        q$
        melt(
          id_vars = "uid_contact",
          value_vars = grep("\\.", names(alias()), value = TRUE)
        )$
        with_columns(
          pl$col("variable")$str$split_exact(by = ".", 1)
        )$
        unnest(
          "variable"
        )$
        collect()$
        pivot(
          index = c("uid_contact", "field_0"),
          columns = "field_1",
          values = "value",
          aggregate_function = pl$element()$first()
        )$
        rename(
          role = "field_0"
        )$
        lazy(),
      how = "left",
      on = "uid_contact"
    )$
    with_columns(
      pl$col("age")$cast(pl$Int32)
    )$
    with_columns(
      pl$col("dt")$dt$year()$
        sub(pl$col("age"))$
        sub(1)$
        alias("yob_lower")
    )
}

#' Wrapper to scan the data, apply schema, and wrangle
#' @export
build <- function(p058306) {
  melt(
    query(
      p058306
    )
  )
}
