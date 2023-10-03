'.__module__.'

#' define key
#' @export
key <- function() {
  c("last_name",
    "first_name",
    "middle_initial",
    "appointed",
    "yob",
    "star")
}

#' generate profile
#' @export
build <- function(l) {

  box::use(polars[pl])

  pl$concat(
    lapply(l, function(x) x$select(key())),
    how = "vertical"
  )$drop_nulls(
    subset = setdiff(key(), c("middle_initial", "star"))
  )$unique()$with_row_count(
    "uid"
  )$with_columns(
    pl$col("uid")$first()$over(
      setdiff(key(), "star")
    )$rank(
      method = "dense"
    )$cast(pl$Utf8)$str$zfill(5),
    pl$col("appointed")$dt$year()$cast(pl$dtypes$Float64)$alias(
      "appointed_year"
    )
  )

}
