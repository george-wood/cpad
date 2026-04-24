'.__module__.'

box::use(
  polars[pl],
  testthat[expect_equal],
  proc/officer
)

#' Join UID to other data
#' @export
join <- function(db, on, validate = TRUE,
                 officer_path = "private/officer.parquet") {
  if (missing(on)) {
    on <- officer$key()
  }

  # take first row when key does not uniquely identify an officer
  keyed <-
    pl$
    read_parquet(officer_path)$
    unique(!!!on, keep = "first", maintain_order = TRUE)$
    lazy()

  drop_cols <- intersect(
    setdiff(unique(c(on, officer$key())), "appointed"),
    unique(c(names(pl$scan_parquet(db)), names(keyed)))
  )

  res <-
    pl$
    scan_parquet(db)$
    join(
      other = keyed,
      on = on,
      how = "left",
      nulls_equal = TRUE
    )$
    drop(!!!drop_cols)

  if (validate) {
    expect_equal(
      as.data.frame(pl$scan_parquet(db)$select(pl$len())$collect())[[1]],
      as.data.frame(res$select(pl$len())$collect())[[1]]
    )
  }

  res
}

#' Join UID to other data using asof join
#' @export
join_asof <- function(db, validate = TRUE, tolerance = NULL,
                      left_on = "yob_lower", right_on = "yob", ..., by,
                      officer_path = "private/officer.parquet") {

  drop_cols <- c(setdiff(by, "appointed"), left_on, right_on)

  #' Both DataFrames must be sorted by the join_asof key
  res <-
    pl$
    scan_parquet(db)$
    sort(left_on)$
    join_asof(
      other = pl$
        scan_parquet(officer_path)$
        select("uid", !!!by, right_on)$
        unique()$
        sort(right_on),
      left_on = left_on,
      right_on = right_on,
      strategy = "forward",
      tolerance = tolerance,
      by = by
    )$
    drop(!!!drop_cols, pl$col("^.*_right$"))

  if (validate) {
    expect_equal(
      as.data.frame(pl$scan_parquet(db)$select(pl$len())$collect())[[1]],
      as.data.frame(res$select(pl$len())$collect())[[1]]
    )
  }

  res
}
