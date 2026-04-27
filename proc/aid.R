'.__module__.'

box::use(
  polars[pl],
  testthat[expect_equal],
  cli[cli_alert_info]
)

#' Join AID to events via per-pfd-class bidirectional asof.
#'
#' For every event we look for shifts held by the same officer that
#' cover the event under one of two windows around each shift's
#' interval [dt_start, dt_end]:
#'
#'   strict:  [dt_start - 1h, dt_end + 1h]
#'   loose:   [dt_start - 1h, dt_end + 3h]
#'
#' Events that fall in neither window of any shift get aid = null
#' (no nearest-neighbor attach beyond the loose window).
#'
#' Two passes are run, one per `present_for_duty` class. Within a pass,
#' the closer of the backward and forward asof candidates wins; across
#' passes, the pfd=TRUE result is preferred whenever it found anything,
#' so a present_for_duty shift beats a not-present shift wherever both
#' would cover the event.
#'
#' `strict` flag is TRUE iff the picked shift covers the event under
#' the strict window, FALSE under loose-only, null when aid is null.
#' @export
join_asof <- function(
  db,
  validate = TRUE,
  assignment_path = "db/assignment.parquet"
) {
  a <- assignment_path

  asg_base <- pl$scan_parquet(a)$select(
    "aid", "uid", "dt_start", "dt_end", "present_for_duty"
  )$drop_nulls("dt_start")$
    filter(pl$col("dt_start")$lt(pl$col("dt_end")))$
    unique()

  events <- pl$scan_parquet(db)$sort("uid", "dt")

  # One asof pass against shifts of a given pfd-class. Adds three
  # columns named with the supplied suffix:
  #   aid_<sfx>     matched shift's aid (or null)
  #   strict_<sfx>  TRUE if event is in strict window, FALSE if in
  #                 loose-only band, null if no match
  #   gap_<sfx>     distance to picked shift in seconds (null if no
  #                 match) — kept around so the diagnostic / debugging
  #                 surface has it; dropped before returning.
  pass <- function(lf, pfd, sfx) {
    asg <- asg_base$
      filter(pl$col("present_for_duty")$eq(pfd))$
      drop("present_for_duty")$
      sort("uid", "dt_start")

    sh_b <- asg$rename(aid = "aid_b", dt_start = "ds_b", dt_end = "de_b")
    sh_f <- asg$rename(aid = "aid_f", dt_start = "ds_f", dt_end = "de_f")

    # Tolerance must cover (max shift length + 3h post-shift slack) on
    # the backward side so an event in a shift's loose tail is still
    # within reach. 30h is comfortable; the window check below is what
    # actually enforces the cutoff.
    j <- lf$
      join_asof(
        other = sh_b,
        left_on = "dt", right_on = "ds_b",
        strategy = "backward", by = "uid", tolerance = "30h"
      )$
      join_asof(
        other = sh_f,
        left_on = "dt", right_on = "ds_f",
        strategy = "forward", by = "uid", tolerance = "30h"
      )

    # Distance from event.dt to each candidate shift's interval, in s.
    # Backward shift: 0 if event in [ds_b, de_b], else dt - de_b.
    # Forward shift: ds_f - dt (always positive; forward shifts are by
    # construction in the future, so never containing).
    gap_b <- pl$when(pl$col("dt")$le(pl$col("de_b")))$
      then(pl$lit(0L)$cast(pl$Int64))$
      otherwise(pl$col("dt")$sub(pl$col("de_b"))$dt$total_seconds())
    gap_f <- pl$col("ds_f")$sub(pl$col("dt"))$dt$total_seconds()

    # Window membership.
    #   backward: ds_b <= dt is guaranteed by the asof, so the left
    #     edge (ds_b - 1h) is auto-satisfied; we just check the right
    #     edge against gap_b.
    #   forward: ds_f >= dt is guaranteed; the right edge (de_f + ...)
    #     is auto-satisfied; we check the left edge (ds_f - dt <= 1h).
    #     Pre-shift events have the same -1h padding under both
    #     windows, so a forward match is always strict.
    in_strict_b <- gap_b$le(3600L)
    in_loose_b  <- gap_b$le(3L * 3600L)
    in_strict_f <- gap_f$le(3600L)
    in_loose_f  <- in_strict_f

    valid_b <- pl$col("ds_b")$is_not_null() & in_loose_b
    valid_f <- pl$col("ds_f")$is_not_null() & in_loose_f

    # Pick the side with the smaller gap. Tie -> backward (arbitrary
    # but stable; gap_b == gap_f == 0 can only happen when the event
    # is on the boundary of two adjacent shifts).
    pick_b <- valid_b & (valid_f$not() | gap_b$le(gap_f))
    pick_f <- valid_f & valid_b$not() | (valid_f & valid_b & gap_f$lt(gap_b))

    aid <- pl$when(pick_b)$then(pl$col("aid_b"))$
      when(pick_f)$then(pl$col("aid_f"))
    strict <- pl$when(pick_b)$then(in_strict_b)$
      when(pick_f)$then(in_strict_f)
    gap <- pl$when(pick_b)$then(gap_b)$
      when(pick_f)$then(gap_f)

    j$with_columns(
      aid$alias(paste0("aid_", sfx)),
      strict$alias(paste0("strict_", sfx)),
      gap$alias(paste0("gap_", sfx))
    )$drop("aid_b", "aid_f", "ds_b", "de_b", "ds_f", "de_f")
  }

  joined <- pass(pass(events, TRUE, "T"), FALSE, "F")

  # Combine: prefer pfd=TRUE result if it found anything.
  picked <- joined$with_columns(
    pl$coalesce("aid_T", "aid_F")$alias("aid"),
    pl$when(pl$col("aid_T")$is_not_null())$
      then(pl$col("strict_T"))$
      otherwise(pl$col("strict_F"))$
      alias("strict")
  )$drop(
    "aid_T", "aid_F", "strict_T", "strict_F", "gap_T", "gap_F"
  )

  # Panel-denominator match rate: aid non-null among in-bounds events
  # with uid. Strict share is conditional on having an aid.
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
