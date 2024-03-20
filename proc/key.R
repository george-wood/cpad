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
locate_keys <- function() {
  sapply(
    dir_ls("db"),
    function(x)
      all(key() %in% pl$scan_parquet(x)$columns)
  )
}

#' Concatenate located data frames and return officers with UID
#' @export
officers <- function() {
  pl$
    concat(
      lapply(
        dir_ls("db")[locate_keys()],
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
    sort(
      "appointed",
      "yob"
    )$
    unique()$
    with_row_index(
      "uid"
    )$
    with_columns(
      pl$col("uid")$cast(pl$Utf8)$str$zfill(5)
    )
}
