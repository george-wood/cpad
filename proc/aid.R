'.__module__.'

box::use(
  polars[pl],
  testthat[expect_equal],
  cli[cli_alert_info]
)

#' Join AID to events using asof join
#' @export
join_asof <- function(db, validate = TRUE, tolerance = "12h30m",
                      buffer = "-1h") {

  a <- "db/assignment.parquet"

  #' Scan the assignment data
  #' Prefer assignment in which officer is present via sorting
  assignment <-
    pl$
    scan_parquet(a)$
    select(
      "aid", "uid", "dt_start", "dt_end", "present_for_duty"
    )$
    drop_nulls(
      "dt_start"
    )$
    unique()$
    filter(
      pl$col("dt_start")$lt(pl$col("dt_end"))
    )$
    sort(
      "dt_start", "present_for_duty"
    )$
    drop(
      "present_for_duty"
    )

  #' Split event data by whether event dates fall inside assignment range
  event <-
    pl$
    read_parquet(db)$
    with_columns(
      pl$
        col("dt")$
        is_between(
          lower_bound = pl$
            scan_parquet(a)$
            select(pl$min("dt_start"))$
            collect()$
            item(),
          upper_bound = pl$
            scan_parquet(a)$
            select(pl$max("dt_end"))$
            collect()$
            item()
        )$
        alias("inside")
    )$
    sort("inside")$
    partition_by("inside")

  #' Join the events that fall inside date range of assignment data
  #' Add a buffer to the shift start time, e.g. 10:30 -> 09:30
  #' Set a strict flag; TRUE if event falls within start:end
  event[[2]] <-
    event[[2]]$
    sort("dt")$
    join_asof(
      other = assignment$
        with_columns(
          pl$col("dt_start")$dt$offset_by(buffer)$alias("dt_start_buffer")
        ),
      left_on = "dt",
      right_on = "dt_start_buffer",
      strategy = "backward",
      tolerance = tolerance,
      by = "uid",
      how = "left",
      allow_parallel = TRUE,
      force_parallel = FALSE
    )$
    # set strict flag:
    with_columns(
      pl$
        col("dt")$
        is_between(
          lower_bound = pl$col("dt_start"),
          upper_bound = pl$col("dt_end")
        )$
        alias("strict")
    )$
    drop(
      "dt_start", "dt_end", "dt_start_buffer"
    )

  #' Calculate join rate
  rate <-
    event[[2]]$
    filter(
      pl$col("uid")$is_not_null()
    )$
    select(
      pl$col("aid")$is_not_null()$cast(pl$Float32)$sum()$div(pl$len())
    )$
    item()

  cli_alert_info(
    "Valid AID rate: {.val {round(rate, 3) * 100}}%"
  )

  #' Concatenate the event data
  event <- pl$concat(event, how = "diagonal")$sort("dt")

  if (validate) {
    expect_equal(
      pl$scan_parquet(db)$select(pl$len())$collect()$item(),
      event$select(pl$len())$item()
    )
  }

  event$lazy()

}

