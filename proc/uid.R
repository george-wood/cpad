'.__module__.'

box::use(
  polars[pl],
  testthat[expect_equal],
  proc/officer
)

#' join UID to other data
#' @export
join <- function(db, on, validate = TRUE) {
  if (missing(on)) {
    on <- officer$key()
  }

  # take first row when key does not uniquely identify an officer
  keyed <-
    pl$
    read_parquet("private/officer.parquet")$
    group_by(on)$
    first()$
    lazy()

  res <-
    pl$
    scan_parquet(db)$
    join(
      other = keyed,
      on = on,
      how = "left",
      join_nulls = TRUE
    )$
    drop(
      intersect(
        unique(c(on, officer$key())),
        unique(c(pl$scan_parquet(db)$columns, keyed$columns))
      )
    )

  if (validate) {
    expect_equal(
      pl$scan_parquet(db)$select(pl$len())$collect()$item(),
      res$select(pl$len())$collect()$item()
    )
  }

  res
}

#' join UID to other data using asof join
#' @export
join_asof <- function(db, validate = TRUE, tolerance = NULL,
                      left_on = "yob_lower", right_on = "yob", ..., by) {

  res <-
    pl$
    scan_parquet(db)$
    join_asof(
      other = pl$
        scan_parquet("private/officer.parquet")$
        select("uid", by, right_on)$
        unique(),
      left_on = left_on,
      right_on = right_on,
      strategy = "forward",
      tolerance = tolerance,
      by = by,
      how = "left",
      allow_parallel = TRUE,
      force_parallel = FALSE
    )$
    drop(by, left_on, right_on)

  if (validate) {
    expect_equal(
      pl$scan_parquet(db)$select(pl$len())$collect()$item(),
      res$select(pl$len())$collect()$item()
    )
  }

  res
}

