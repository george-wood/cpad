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
#'
#' Two-pass strategy:
#'   1. Forward asof on yob_lower -> yob within the `by`-group. This is
#'      the primary match path.
#'   2. Fallback for rows the asof missed: exact join on `by`-cols only,
#'      accepted iff that by-group has a single unique uid in the officer
#'      table. The asof can fail when an officer's yob is null (any null
#'      comparison evaluates to null, so no asof candidate is found); the
#'      fallback recovers those rows without disambiguation risk because
#'      the by-group is uid-unique.
#' @export
join_asof <- function(db, validate = TRUE, tolerance = NULL,
                      left_on = "yob_lower", right_on = "yob", ..., by,
                      officer_path = "private/officer.parquet") {

  drop_cols <- c(setdiff(by, "appointed"), left_on, right_on)

  officer <- pl$scan_parquet(officer_path)

  # Primary: forward asof within by-group. Both sides must be sorted by
  # the asof key.
  primary <-
    pl$
    scan_parquet(db)$
    sort(left_on)$
    join_asof(
      other = officer$
        select("uid", !!!by, right_on)$
        unique()$
        sort(right_on),
      left_on = left_on,
      right_on = right_on,
      strategy = "forward",
      tolerance = tolerance,
      by = by
    )

  # Fallback: by-group -> uid, only when the group is uid-unique. Coalesce
  # so primary asof matches always win; fallback only fills nulls.
  fallback <- officer$
    select("uid", !!!by)$
    unique()$
    group_by(!!!by)$
    agg(
      pl$col("uid")$first()$alias("uid_fb"),
      pl$len()$alias("n_uids_fb")
    )$
    filter(pl$col("n_uids_fb")$eq(1))$
    drop("n_uids_fb")

  res <- primary$
    join(fallback, on = by, how = "left")$
    with_columns(pl$coalesce("uid", "uid_fb")$alias("uid"))$
    drop("uid_fb", !!!drop_cols, pl$col("^.*_right$"))

  if (validate) {
    expect_equal(
      as.data.frame(pl$scan_parquet(db)$select(pl$len())$collect())[[1]],
      as.data.frame(res$select(pl$len())$collect())[[1]]
    )
  }

  res
}
