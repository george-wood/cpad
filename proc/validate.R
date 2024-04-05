'.__module__.'

box::use(polars[pl])

#' Compute proportion of null values for a column in a dataframe
#' @export
null_count <- function(db, col = "uid") {
  pl$
    scan_parquet(db)$
    select(
      pl$len()$cast(pl$dtypes$Float32)$alias("n"),
      pl$col(col)$is_null()$sum()$alias("missing")
    )$
    with_columns(
      pl$col("missing")$div(pl$col("n"))$mul(100)$alias("percent")
    )$
    collect()
}

