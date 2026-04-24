'.__module__.'

box::use(
  polars[pl],
  testthat[expect_equal],
  cli[cli_alert_info]
)

#' Join AID to events using asof join
#' @export
join_asof <- function(
  db,
  validate = TRUE,
  tolerance = "12h30m",
  buffer = "-1h",
  assignment_path = "db/assignment.parquet"
) {
  a <- assignment_path

  #' Scan the assignment data
  #' Prefer assignment in which officer is present via sorting
  assignment <-
    pl$scan_parquet(a)$select(
      "aid",
      "uid",
      "dt_start",
      "dt_end",
      "present_for_duty"
    )$drop_nulls(
      "dt_start"
    )$unique()$filter(
      pl$col("dt_start")$lt(pl$col("dt_end"))
    )$sort(
      "dt_start",
      "present_for_duty"
    )$drop(
      "present_for_duty"
    )

  #' Bounds of assignment data (used to flag in-range events)
  lo <- as.data.frame(
    pl$scan_parquet(a)$select(pl$min("dt_start"))$collect()
  )[[1]]
  hi <- as.data.frame(
    pl$scan_parquet(a)$select(pl$max("dt_end"))$collect()
  )[[1]]

  #' Flag each event as inside/outside assignment date range, then split
  flagged <-
    pl$scan_parquet(db)$with_columns(
      pl$col("dt")$is_between(lower_bound = lo, upper_bound = hi)$alias(
        "inside"
      )
    )

  event_outside <- flagged$filter(pl$col("inside")$not())$drop("inside")

  #' Join in-range events to assignment via asof join.
  #' Add a buffer to the shift start time, e.g. 10:30 -> 09:30.
  #' Set a strict flag; TRUE if event falls within start:end.
  event_inside <-
    flagged$filter(pl$col("inside"))$drop("inside")$sort("dt")$join_asof(
      other = assignment$with_columns(
        pl$col("dt_start")$dt$offset_by(buffer)$alias("dt_start_buffer")
      ),
      left_on = "dt",
      right_on = "dt_start_buffer",
      strategy = "backward",
      tolerance = tolerance,
      by = "uid"
    )$with_columns(
      pl$col("dt")$is_between(
        lower_bound = pl$col("dt_start"),
        upper_bound = pl$col("dt_end")
      )$alias("strict")
    )$drop("dt_start", "dt_end", "dt_start_buffer")

  #' Calculate join rate on officers we actually have UIDs for
  rate <-
    as.data.frame(
      event_inside$filter(pl$col("uid")$is_not_null())$select(
        pl$col("aid")$is_not_null()$cast(pl$Float32)$sum() / pl$len()
      )$collect()
    )[[1]]

  cli_alert_info(
    "Valid AID rate: {.val {round(rate, 3) * 100}}%"
  )

  #' Concatenate the in-range and out-of-range events
  event <-
    pl$concat(event_inside, event_outside, how = "diagonal")$sort("dt")

  if (validate) {
    expect_equal(
      as.data.frame(pl$scan_parquet(db)$select(pl$len())$collect())[[1]],
      as.data.frame(event$select(pl$len())$collect())[[1]]
    )
  }

  event
}
