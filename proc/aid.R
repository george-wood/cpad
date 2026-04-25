'.__module__.'

box::use(
  polars[pl],
  testthat[expect_equal],
  cli[cli_alert_info]
)

#' Join AID to events via nearest-shift asof (bidirectional).
#'
#' For every event, find the closest shift held by the same officer
#' and attach its AID. "Closest" means smallest gap between event.dt
#' and the shift interval [dt_start, dt_end]:
#'   - if event.dt is inside [dt_start, dt_end], gap = 0
#'   - otherwise gap is the distance to the nearer endpoint
#' Only shifts within `tolerance` of the event on the relevant side
#' are considered; events with no shift in range get aid = null.
#'
#' `strict` flag: TRUE iff event.dt falls in [dt_start - 1h, dt_end + 1h]
#' of the matched shift. FALSE means the match is a looser
#' nearest-neighbor attach (officer had a nearby shift but wasn't on
#' one at the event time). NULL when aid is null.
#'
#' Rate is reported on the panel denominator: in-bounds events with a
#' known UID. Comparable to the aid_diagnostic.R tables.
#' @export
join_asof <- function(
  db,
  validate = TRUE,
  tolerance = "24h",
  assignment_path = "db/assignment.parquet"
) {
  a <- assignment_path

  # Shifts, deduped, positive-duration. Sort by (uid, dt_start,
  # present_for_duty) so that on ties, a present_for_duty = TRUE row
  # wins the asof pick (sort is stable, drop preserves order).
  assignment <-
    pl$scan_parquet(a)$select(
      "aid", "uid", "dt_start", "dt_end", "present_for_duty"
    )$drop_nulls(
      "dt_start"
    )$filter(
      pl$col("dt_start")$lt(pl$col("dt_end"))
    )$unique()$sort(
      "uid", "dt_start", "present_for_duty"
    )$drop("present_for_duty")

  # Two rename'd copies of the shift table so the backward and forward
  # asof joins can both write their matched shift's columns onto the
  # left frame without colliding.
  sh_b <- assignment$rename(
    aid = "aid_b", dt_start = "dt_start_b", dt_end = "dt_end_b"
  )
  sh_f <- assignment$rename(
    aid = "aid_f", dt_start = "dt_start_f", dt_end = "dt_end_f"
  )

  events <- pl$scan_parquet(db)$sort("uid", "dt")

  # Backward: latest shift starting at or before event.dt. Captures
  # both "event inside a shift" and "event after a shift ended".
  # Forward: earliest shift starting at or after event.dt.
  joined <-
    events$join_asof(
      other = sh_b,
      left_on = "dt", right_on = "dt_start_b",
      strategy = "backward", by = "uid",
      tolerance = tolerance
    )$join_asof(
      other = sh_f,
      left_on = "dt", right_on = "dt_start_f",
      strategy = "forward", by = "uid",
      tolerance = tolerance
    )

  # Per-side gap in seconds. For the backward shift, if the event is
  # inside [dt_start_b, dt_end_b] gap is 0; otherwise it's
  # dt - dt_end_b. For the forward shift, gap is dt_start_f - dt
  # (forward shifts are always in the future, so never containing).
  # Null on either side when no shift was found within tolerance.
  with_gaps <- joined$with_columns(
    pl$when(
      pl$col("dt")$le(pl$col("dt_end_b"))
    )$then(
      pl$lit(0L)$cast(pl$Int64)
    )$otherwise(
      pl$col("dt")$sub(pl$col("dt_end_b"))$dt$total_seconds()
    )$alias("gap_b"),
    pl$col("dt_start_f")$sub(pl$col("dt"))$dt$total_seconds()$alias("gap_f")
  )

  # Pick the closer side. On ties, prefer backward (arbitrary but
  # stable). The `strict` expression mirrors the aid pick exactly so
  # the flag always refers to the same shift whose aid got attached.
  strict_b <- pl$col("dt")$is_between(
    lower_bound = pl$col("dt_start_b")$dt$offset_by("-1h"),
    upper_bound = pl$col("dt_end_b")$dt$offset_by("1h")
  )
  strict_f <- pl$col("dt")$is_between(
    lower_bound = pl$col("dt_start_f")$dt$offset_by("-1h"),
    upper_bound = pl$col("dt_end_f")$dt$offset_by("1h")
  )

  pick_aid <-
    pl$when(pl$col("gap_b")$is_null())$then(pl$col("aid_f"))$
      when(pl$col("gap_f")$is_null())$then(pl$col("aid_b"))$
      when(pl$col("gap_b")$le(pl$col("gap_f")))$then(pl$col("aid_b"))$
      otherwise(pl$col("aid_f"))

  pick_strict <-
    pl$when(pl$col("gap_b")$is_null())$then(strict_f)$
      when(pl$col("gap_f")$is_null())$then(strict_b)$
      when(pl$col("gap_b")$le(pl$col("gap_f")))$then(strict_b)$
      otherwise(strict_f)

  picked <- with_gaps$with_columns(
    pick_aid$alias("aid"),
    pick_strict$alias("strict")
  )$drop(
    "aid_b", "aid_f",
    "dt_start_b", "dt_end_b", "dt_start_f", "dt_end_f",
    "gap_b", "gap_f"
  )

  # Panel-denominator match rate: aid non-null among (in-bounds events
  # with uid). Also report what fraction of matches are strict.
  lo <- as.data.frame(
    pl$scan_parquet(a)$select(pl$min("dt_start"))$collect()
  )[[1]]
  hi <- as.data.frame(
    pl$scan_parquet(a)$select(pl$max("dt_end"))$collect()
  )[[1]]

  panel <- picked$filter(
    pl$col("uid")$is_not_null() &
      pl$col("dt")$is_between(lo, hi)
  )

  stats <- as.data.frame(
    panel$select(
      (pl$col("aid")$is_not_null()$cast(pl$Float32)$sum() / pl$len())$
        alias("match_rate"),
      (pl$col("strict")$fill_null(FALSE)$cast(pl$Float32)$sum() /
         pl$col("aid")$is_not_null()$cast(pl$Float32)$sum())$
        alias("strict_of_matched")
    )$collect()
  )

  cli_alert_info(
    "Panel AID match rate: {.val {round(100 * stats$match_rate, 1)}}% \\
    (of matches, strict: {.val {round(100 * stats$strict_of_matched, 1)}}%)"
  )

  event <- picked$sort("dt")

  if (validate) {
    expect_equal(
      as.data.frame(pl$scan_parquet(db)$select(pl$len())$collect())[[1]],
      as.data.frame(event$select(pl$len())$collect())[[1]]
    )
  }

  event
}
