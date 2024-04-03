'.__module__.'

box::use(
  polars[pl],
  fs[dir_ls]
)

#' Key columns
#' @export
key <- function() {
  c("first_name",
    "middle_initial",
    "last_name",
    "appointed",
    "yob")
}

#' Locate data frames with all key columns
locate_key <- function() {
  sapply(
    dir_ls("db"),
    function(x)
      all(key() %in% pl$scan_parquet(x)$columns)
  )
}

#' Concatenate located data frames and return officers with UID
#' @export
generate_key <- function() {
  pl$
    concat(
      lapply(
        dir_ls("db")[locate_key()],
        \(x)
        pl$
          scan_parquet(x)$
          select(key())$
          unique()
      ),
      how = "vertical"
    )$
    drop_nulls(
      subset = setdiff(key(), c("middle_initial"))
    )$
    unique()$
    sort(
      "appointed",
      "yob"
    )$
    with_row_index(
      "uid"
    )$
    with_columns(
      pl$col("uid")$cast(pl$Utf8)$str$zfill(5),
      pl$col("appointed")$dt$year()$cast(pl$Int32)$alias("appointed_year")
    )
}
