'.__module__.'

box::use(polars[pl])

#' Compute proportion of column in dataframe with missing data
#' @export
missing <- function(path, col = "uid") {
  pl$
    scan_parquet(path)$
    group_by("year")$
    agg(
      pl$len()$cast(pl$dtypes$Float32)$alias("n"),
      pl$col(col)$is_null()$sum()$alias("missing")
    )$
    with_columns(
      pl$col("missing")$div(pl$col("n"))$mul(100)
    )
}
