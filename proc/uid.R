'.__module__.'

box::use(
  polars[pl],
  fs[dir_ls]
)

#' Key columns
#' @export
standard_key <- function() {
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
      all(standard_key() %in% pl$scan_parquet(x)$columns)
  )
}

#' Concatenate located data frames and return officers with UID
#' @export
officers <- function() {
  pl$
    concat(
      lapply(
        dir_ls("db")[locate_key()],
        \(x)
        pl$
          scan_parquet(x)$
          select(standard_key())$
          unique()
      ),
      how = "vertical"
    )$
    drop_nulls(
      subset = setdiff(standard_key(), c("middle_initial"))
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

#' join UID to other data
#' @export
join <- function(db, key) {
  if (missing(key)) {
    key <- standard_key()
  }

  pl$
    scan_parquet(db)$
    join(
      other = officers(),
      on = key,
      how = "left",
      join_nulls = TRUE
    )
}

