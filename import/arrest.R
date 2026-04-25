'.__module__.'

box::use(
  polars[pl],
  proc/utility[scan_aliased, data_files]
)

#' Path to data
path <- function() {
  list(
    p701162 = data_files("arrest/p701162"),
    p708085 = data_files("arrest/p708085")
  )
}

#' Define the schema
get_schema <- function() {
  list(
    # p701162
    `PO FIRST NAME` = pl$String,
    `PO MIDDLE NAME` = pl$String,
    `PO LAST NAME` = pl$String,
    `PO RACE` = pl$String,
    `PO GENDER` = pl$String,
    `ROLE` = pl$String,
    `CB` = pl$String,
    `RD` = pl$String,
    `ARREST DATE/TIME` = pl$String,
    `ARRESTEE RACE` = pl$String,
    `ARRESTEE AGE` = pl$Float64,
    `STATUTE` = pl$String,
    `CHARGE TYPE` = pl$String,
    # p708085
    `CB NO` = pl$String,
    `DATETIME` = pl$String,
    `EMPLOYEE ROLE` = pl$String,
    `FIRST NAME` = pl$String,
    `LAST NAME` = pl$String,
    `APPOINTED DATE` = pl$String,
    `YOB` = pl$Int32
  )
}

#' Alias for column names
alias <- function() {
  list(
    # p701162
    `PO FIRST NAME`    = "first_name",
    `PO MIDDLE NAME`   = "middle_initial",
    `PO LAST NAME`     = "last_name",
    `PO RACE`          = "race",
    `PO GENDER`        = "gender",
    `ROLE`             = "role",
    `CB`               = "uid_arrest",
    `RD`               = "rd",
    `ARREST DATE/TIME` = "dt",
    `ARRESTEE RACE`    = "civilian_race",
    `ARRESTEE AGE`     = "civilian_age",
    `ARRESTEE GENDER`  = "civilian_gender",
    `STATUTE`          = "statute",
    `CHARGE TYPE`      = "charge_type",
    # p708085
    `CB NO`            = "uid_arrest",
    `DATETIME`         = "dt",
    `EMPLOYEE ROLE`    = "role",
    `FIRST NAME`       = "first_name",
    `LAST NAME`        = "last_name",
    `APPOINTED DATE`   = "appointed",
    `YOB`              = "yob"
  )
}

#' Scan csv with schema, wrangle, and create identifier
query <- function(x) {
  scan_aliased(x, get_schema(), alias())$
    filter(
      pl$col("uid_arrest")$ne("J")
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
    )$
    filter(
      pl$col("dt")$is_not_null()
    )
}
