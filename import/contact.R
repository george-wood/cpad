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
    p058306 = 2012:2015
  )
}

#' Path to data
path <- function() {
  list(
    p058306 = ls("contact/p058306")
  )
}

#' Years covered by each CSV header convention. Split by filename year
#' because the schema changes between files but is stable within each.
legacy_years <- c(2012, 2013)
modern_years <- c(2014, 2015)

#' Legacy (2012/2013) schema: UPPERCASE column names with spaces/periods.
schema_legacy <- function() {
  list(
    DATE = pl$String,
    TIME = pl$String,
    `ST NUM` = pl$String,
    DIR = pl$String,
    `STREET NAME` = pl$String,
    DIST = pl$String,
    BEAT = pl$String,
    `CONTACT TYPE DESCRIPTION` = pl$String,
    `1st P.O. LAST NAME` = pl$String,
    `1st P.O. FIRST NAME` = pl$String,
    `1ST P.O. SEX` = pl$String,
    `1ST P.O. RACE` = pl$String,
    `1st P.O. AGE` = pl$Int32,
    `2nd P.O. LAST NAME ` = pl$String,
    `2nd P.O.FIRST NAME ` = pl$String,
    `2ND P.O. SEX` = pl$String,
    `2ND P.O. RACE` = pl$String,
    `2nd P.O. AGE ` = pl$Int32,
    `SUBJECT SEX` = pl$String,
    `SUBJECT RACE` = pl$String,
    `SUBJECT AGE` = pl$String,
    `SUBJECT HEIGHT` = pl$Float64,
    `SUBJECT WEIGHT` = pl$Float64,
    `SUBJECT BUILD ` = pl$String,
    `SUBJECT HAIRCOLOR` = pl$String,
    `SUBJECT HAIRSTYLE` = pl$String,
    `SUBJECT COMPLEXION` = pl$String,
    `SUBJECT CLOTHING DESCRIPTION` = pl$String
  )
}

alias_legacy <- function() {
  list(
    DATE = "date",
    TIME = "dt",
    `ST NUM` = "street_number",
    DIR = "direction",
    `STREET NAME` = "street",
    DIST = "district",
    BEAT = "beat",
    `CONTACT TYPE DESCRIPTION` = "contact_type",
    `1st P.O. LAST NAME` = "first.last_name",
    `1st P.O. FIRST NAME` = "first.first_name",
    `1ST P.O. SEX` = "first.gender",
    `1ST P.O. RACE` = "first.race",
    `1st P.O. AGE` = "first.age",
    `2nd P.O. LAST NAME ` = "second.last_name",
    `2nd P.O.FIRST NAME ` = "second.first_name",
    `2ND P.O. SEX` = "second.gender",
    `2ND P.O. RACE` = "second.race",
    `2nd P.O. AGE ` = "second.age",
    `SUBJECT SEX` = "civilian_gender",
    `SUBJECT RACE` = "civilian_race",
    `SUBJECT AGE` = "civilian_age",
    `SUBJECT HEIGHT` = "civilian_height",
    `SUBJECT WEIGHT` = "civilian_weight",
    `SUBJECT BUILD ` = "civilian_build",
    `SUBJECT HAIRCOLOR` = "civilian_hair_color",
    `SUBJECT HAIRSTYLE` = "civilian_hairstyle",
    `SUBJECT COMPLEXION` = "civilian_complexion",
    `SUBJECT CLOTHING DESCRIPTION` = "civilian_clothing"
  )
}

#' Modern (2014/2015) schema: Title/CamelCase names.
schema_modern <- function() {
  list(
    `Contact Date` = pl$String,
    `Time of Stop` = pl$String,
    `Street Number` = pl$String,
    `Street Direction` = pl$String,
    `Street Name` = pl$String,
    District = pl$String,
    Beat = pl$String,
    `Contact Type` = pl$String,
    FirstPOLN = pl$String,
    FirstPOFN = pl$String,
    FirstPOSex = pl$String,
    FirstPORace = pl$String,
    `FirstPOAge (on date of stop)` = pl$Int32,
    SecPOLN = pl$String,
    SecPOFN = pl$String,
    SecPOSex = pl$String,
    SecPORace = pl$String,
    `SecPOAge (on date of stop)` = pl$Int32,
    SubSex = pl$String,
    SubRace = pl$String,
    SubAge = pl$String,
    SubHeight = pl$Float64,
    SubWeight = pl$Float64,
    SubBuild = pl$String,
    `SubHair Color` = pl$String,
    `SubHair Style` = pl$String,
    SubComplexion = pl$String,
    `SubClothing Description` = pl$String
  )
}

alias_modern <- function() {
  list(
    `Contact Date` = "date",
    `Time of Stop` = "dt",
    `Street Number` = "street_number",
    `Street Direction` = "direction",
    `Street Name` = "street",
    District = "district",
    Beat = "beat",
    `Contact Type` = "contact_type",
    FirstPOLN = "first.last_name",
    FirstPOFN = "first.first_name",
    FirstPOSex = "first.gender",
    FirstPORace = "first.race",
    `FirstPOAge (on date of stop)` = "first.age",
    SecPOLN = "second.last_name",
    SecPOFN = "second.first_name",
    SecPOSex = "second.gender",
    SecPORace = "second.race",
    `SecPOAge (on date of stop)` = "second.age",
    SubSex = "civilian_gender",
    SubRace = "civilian_race",
    SubAge = "civilian_age",
    SubHeight = "civilian_height",
    SubWeight = "civilian_weight",
    SubBuild = "civilian_build",
    `SubHair Color` = "civilian_hair_color",
    `SubHair Style` = "civilian_hairstyle",
    SubComplexion = "civilian_complexion",
    `SubClothing Description` = "civilian_clothing"
  )
}

#' 2015 CSVs have a typo: `FirtsPORace` instead of `FirstPORace`. We
#' handle that by scanning 2015 files with a variant schema/alias that
#' maps the typo'd column to `first.race`. Doing it this way (rather than
#' reading both columns and coalescing post-scan) sidesteps a polars lazy
#' bug where a coalesce over a `missing_columns = "insert"` null column
#' silently fails to materialize, producing ~524k null officer races.
schema_modern_2015 <- function() {
  s <- schema_modern()
  names(s)[names(s) == "FirstPORace"] <- "FirtsPORace"
  s
}
alias_modern_2015 <- function() {
  a <- alias_modern()
  names(a)[names(a) == "FirstPORace"] <- "FirtsPORace"
  a
}

#' Unified schema/alias for back-compat with callers that expected one.
#' @export
get_schema <- function() c(schema_legacy(), schema_modern())
alias <- function() c(alias_legacy(), alias_modern())

#' Split source paths by header convention. Uses the filename year.
#' 2015 is its own group because of the FirstPORace/FirtsPORace typo.
split_sources <- function() {
  files <- path()$p058306
  year <- as.integer(sub(".*_([0-9]{4})\\.csv$", "\\1", files))
  list(
    legacy = files[year %in% legacy_years],
    modern_2014 = files[year == 2014L],
    modern_2015 = files[year == 2015L]
  )
}

#' Read the data, apply schema, and rough-wrangle into canonical columns.
#' Scans each header-convention group independently then diagonal-concats.
#' @export
query <- function() {
  src <- split_sources()

  lfs <- list()
  if (length(src$legacy)) {
    lfs$legacy <- scan_aliased(
      src$legacy,
      schema_legacy(),
      alias_legacy(),
      missing_columns = "insert"
    )
  }
  if (length(src$modern_2014)) {
    lfs$modern_2014 <- scan_aliased(
      src$modern_2014,
      schema_modern(),
      alias_modern(),
      missing_columns = "insert"
    )
  }
  if (length(src$modern_2015)) {
    lfs$modern_2015 <- scan_aliased(
      src$modern_2015,
      schema_modern_2015(),
      alias_modern_2015(),
      missing_columns = "insert"
    )
  }
  stopifnot(length(lfs) > 0L)

  unified <- if (length(lfs) == 1L) {
    lfs[[1]]
  } else {
    pl$concat(!!!lfs, how = "diagonal")
  }

  unified$
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

#' Melt the officer information, pivot by role, and rejoin.
#' @export
melt <- function(q) {
  new_names <- unname(unlist(alias()))
  role_cols <- grep("\\.", new_names, value = TRUE)
  base_cols <- grep("\\.", new_names, invert = TRUE, value = TRUE)

  q$
    select(
      "uid_contact",
      base_cols
    )$
    join(
      other =
        q$
        unpivot(
          index = "uid_contact",
          on = role_cols
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
          on = "field_1",
          values = "value",
          aggregate_function = pl$element()$first()
        )$
        rename(
          field_0 = "role"
        )$
        lazy(),
      how = "left",
      on = "uid_contact"
    )$
    with_columns(
      pl$col("dt")$dt$year()$
        sub(pl$col("age")$cast(pl$Int32))$
        sub(1)$
        cast(pl$Int32)$
        alias("yob_lower")
    )$
    drop("age")
}

#' Wrapper: scan, wrangle, and assert we didn't silently drop a pile of
#' rows to the `dt is_not_null` filter — that's the canary for future
#' schema drift (the kind that produced the 2014/2015 data-loss bug).
#' @export
build <- function() {
  melted <- melt(query())
  result <- melted$filter(pl$col("dt")$is_not_null())

  before <- as.data.frame(melted$select(pl$len())$collect())[[1]]
  after  <- as.data.frame(result$select(pl$len())$collect())[[1]]
  dropped <- before - after
  if (before > 0L && dropped / before > 0.05) {
    warning(sprintf(
      paste0(
        "contact$build(): dt_is_null filter dropped %d of %d rows (%.1f%%). ",
        "Unusually high — possible schema mismatch; check that every ",
        "source CSV's DATE/TIME columns are covered by an alias."
      ),
      dropped, before, 100 * dropped / before
    ))
  }

  result
}
