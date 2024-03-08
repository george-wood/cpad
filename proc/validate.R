'.__module__.'

box::use(polars[pl])

#' List files in a directory with regex option
#' @export
proportion_missing <- function(path) {
  pl$
    scan_parquet(path)$
    group_by("year")$
    agg(
      pl$len()$cast(pl$dtypes$Float32)$alias("n"),
      pl$col("uid")$is_null()$sum()$alias("missing")
    )$
    with_columns(
      pl$col("missing")$div(pl$col("n"))$mul(100)
    )
}

