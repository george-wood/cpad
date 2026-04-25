'.__module__.'

box::use(
  polars[pl],
  proc/utility[scan_aliased, data_files]
)

#' Path to data. Four FOIA packets, each with its own column conventions:
#'   p061715 - early release (~2008-2015); LAST_NME / PPS_AWARD_DETAIL_ID.
#'   p456632 - later release overlapping p061715; LAST_NME / AWARD_ID.
#'   p506887 - 2017-2018 release; AWARDEE_* columns, AWARD_REF_NUMBER.
#'   p583900 - 2018+ release; AWARDEE_* columns, ISO dates, REQ_NAME.
path <- function() {
  list(
    p061715 = data_files("award/p061715"),
    p456632 = data_files("award/p456632"),
    p506887 = data_files("award/p506887"),
    p583900 = data_files("award/p583900")
  )
}

# ---------------------------------------------------------------------------
# Per-packet schemas and aliases.
#
# Every packet carries the same logical fields but under different names
# and with different date formats. We scan each packet with its own
# schema/alias and normalize dates post-scan, then diagonal-concat. This
# mirrors the multi-group approach in contact.R / arrest.R.
# ---------------------------------------------------------------------------

schema_p061715 <- function() {
  list(
    LAST_NME            = pl$String,
    FIRST_NME           = pl$String,
    MIDDLE_INITIAL      = pl$String,
    BIRTH_YEAR          = pl$Int32,
    GENDER              = pl$String,
    RACE                = pl$String,
    STAR                = pl$String,
    APPOINTED_DATE      = pl$String,
    RANK                = pl$String,
    LAST_PROMOTION_DATE = pl$String,
    RESIGNATION_DATE    = pl$String,
    PPS_AWARD_DETAIL_ID = pl$String,
    AWARD_TYPE          = pl$String,
    AWARD_REQUEST_DATE  = pl$String,
    TRACKING_NO         = pl$String,
    CURRENT_STATUS      = pl$String,
    REQUESTER           = pl$String,
    INCIDENT_START_DATE = pl$String,
    INCIDENT_END_DATE   = pl$String,
    CEREMONY_DATE       = pl$String
  )
}

alias_p061715 <- function() {
  list(
    LAST_NME            = "last_name",
    FIRST_NME           = "first_name",
    MIDDLE_INITIAL      = "middle_initial",
    BIRTH_YEAR          = "yob",
    GENDER              = "gender",
    RACE                = "race",
    STAR                = "star",
    APPOINTED_DATE      = "appointed",
    RANK                = "rank",
    LAST_PROMOTION_DATE = "last_promotion",
    RESIGNATION_DATE    = "resigned",
    PPS_AWARD_DETAIL_ID = "uid_award",
    AWARD_TYPE          = "award_type",
    AWARD_REQUEST_DATE  = "award_requested",
    TRACKING_NO         = "tracking_no",
    CURRENT_STATUS      = "status",
    REQUESTER           = "requester",
    INCIDENT_START_DATE = "incident_start",
    INCIDENT_END_DATE   = "incident_end",
    CEREMONY_DATE       = "ceremony_date"
  )
}

#' p456632 shares nearly all columns with p061715; only the award-ID
#' column name differs, plus DELAYED_REASON is new. Deriving from the
#' p061715 maps keeps the common fields in lockstep.
schema_p456632 <- function() {
  s <- schema_p061715()
  s$PPS_AWARD_DETAIL_ID <- NULL
  c(s, list(
    AWARD_ID       = pl$String,
    DELAYED_REASON = pl$String
  ))
}

alias_p456632 <- function() {
  a <- alias_p061715()
  a$PPS_AWARD_DETAIL_ID <- NULL
  c(a, list(
    AWARD_ID       = "uid_award",
    DELAYED_REASON = "delayed_reason"
  ))
}

schema_p506887 <- function() {
  list(
    AWARDEE_LASTN       = pl$String,
    AWARDEE_FIRSTN      = pl$String,
    AWARDEE_MI          = pl$String,
    BIRTHYEAR           = pl$Int32,
    SEX                 = pl$String,
    RACE                = pl$String,
    CPD_STAR_NO         = pl$String,
    APPOINTED_DATE      = pl$String,
    SENIORITY_DATE      = pl$String,
    RESIGNATION_DATE    = pl$String,
    AWARDEE_RANK        = pl$String,
    AWARD_REF_NUMBER    = pl$String,
    AWARD_TYPE          = pl$String,
    AWARD_REQUEST_DATE  = pl$String,
    CURRENT_STATUS      = pl$String,
    REQ_LASTN           = pl$String,
    REQ_FIRSTN          = pl$String,
    REQ_MI              = pl$String,
    INCIDENT_START_DATE = pl$String,
    INCIDENT_END_DATE   = pl$String,
    INCIDENT_DESCR      = pl$String,
    DELAYED_REASON      = pl$String,
    CEREMONY_DATE       = pl$String
  )
}

alias_p506887 <- function() {
  list(
    AWARDEE_LASTN       = "last_name",
    AWARDEE_FIRSTN      = "first_name",
    AWARDEE_MI          = "middle_initial",
    BIRTHYEAR           = "yob",
    SEX                 = "gender",
    RACE                = "race",
    CPD_STAR_NO         = "star",
    APPOINTED_DATE      = "appointed",
    SENIORITY_DATE      = "last_promotion",
    RESIGNATION_DATE    = "resigned",
    AWARDEE_RANK        = "rank",
    AWARD_REF_NUMBER    = "uid_award",
    AWARD_TYPE          = "award_type",
    AWARD_REQUEST_DATE  = "award_requested",
    CURRENT_STATUS      = "status",
    # REQ_LASTN/FIRSTN/MI are scratch columns — concatenated into
    # `requester` post-scan then dropped, so the final schema matches
    # the free-text `requester` the other packets produce.
    REQ_LASTN           = "req_last_name",
    REQ_FIRSTN          = "req_first_name",
    REQ_MI              = "req_middle_initial",
    INCIDENT_START_DATE = "incident_start",
    INCIDENT_END_DATE   = "incident_end",
    INCIDENT_DESCR      = "incident_description",
    DELAYED_REASON      = "delayed_reason",
    CEREMONY_DATE       = "ceremony_date"
  )
}

schema_p583900 <- function() {
  list(
    AWARDEE_LASTN                 = pl$String,
    AWARDEE_FIRSTN                = pl$String,
    AWARDEE_MI                    = pl$String,
    AWARDEE_BIRTHYEAR             = pl$Int32,
    AWARDEE_SEX                   = pl$String,
    AWARDEE_RACE                  = pl$String,
    AWARDEE_STAR_NO               = pl$String,
    AWARDEE_TITLE_CURR            = pl$String,
    APPOINTED_DATE                = pl$String,
    LAST_PROMOTION_SENIORITY_DATE = pl$String,
    AWARD_TYPE                    = pl$String,
    AWARD_REQUEST_DATE            = pl$String,
    AWARD_REF_NUMBER              = pl$String,
    CURRENT_STATUS                = pl$String,
    REQ_NAME                      = pl$String,
    INCIDENT_START_DATE           = pl$String,
    INCIDENT_END_DATE             = pl$String,
    INCIDENT_DESCR                = pl$String,
    DELAYED_REASON                = pl$String,
    CEREMONY_DATE                 = pl$String
  )
}

alias_p583900 <- function() {
  list(
    AWARDEE_LASTN                 = "last_name",
    AWARDEE_FIRSTN                = "first_name",
    AWARDEE_MI                    = "middle_initial",
    AWARDEE_BIRTHYEAR             = "yob",
    AWARDEE_SEX                   = "gender",
    AWARDEE_RACE                  = "race",
    AWARDEE_STAR_NO               = "star",
    AWARDEE_TITLE_CURR            = "rank",
    APPOINTED_DATE                = "appointed",
    LAST_PROMOTION_SENIORITY_DATE = "last_promotion",
    AWARD_TYPE                    = "award_type",
    AWARD_REQUEST_DATE            = "award_requested",
    AWARD_REF_NUMBER              = "uid_award",
    CURRENT_STATUS                = "status",
    REQ_NAME                      = "requester",
    INCIDENT_START_DATE           = "incident_start",
    INCIDENT_END_DATE             = "incident_end",
    INCIDENT_DESCR                = "incident_description",
    DELAYED_REASON                = "delayed_reason",
    CEREMONY_DATE                 = "ceremony_date"
  )
}

# ---------------------------------------------------------------------------
# Post-scan date normalization. Three distinct formats across packets.
# Each helper parses to datetime first and (for date-only canonical cols)
# drops the time component, so `%H:%M` suffixes are tolerated but ignored.
# ---------------------------------------------------------------------------

#' p061715 & p456632: American with time, e.g. `9/26/2005 0:00`.
#' All date cols here carry an `H:M` suffix that isn't semantically
#' meaningful, so we parse as datetime and immediately cast to date —
#' giving a uniform `Date` dtype across packets for diagonal concat.
post_legacy <- function(lf) {
  fmt <- "%m/%d/%Y %H:%M"
  date_cols <- c(
    "appointed", "last_promotion", "resigned", "award_requested",
    "incident_start", "incident_end", "ceremony_date"
  )
  lf$with_columns(
    pl$col(!!!date_cols)$str$to_datetime(format = fmt, strict = FALSE)$dt$date()
  )
}

#' p506887: American no time, e.g. `9/26/2005`.
#' Also synthesize `requester` from the three REQ_* columns.
post_p506887 <- function(lf) {
  fmt <- "%m/%d/%Y"
  date_cols <- c(
    "appointed", "last_promotion", "resigned", "award_requested",
    "incident_start", "incident_end", "ceremony_date"
  )
  lf$
    with_columns(
      pl$col(!!!date_cols)$str$to_date(format = fmt, strict = FALSE),
      # Build "LAST, FIRST M" to match the free-text `requester` produced
      # by the other packets. fill_null keeps the concat non-null when
      # any of the three parts is missing.
      pl$concat_str(
        pl$col("req_last_name")$fill_null(pl$lit("")),
        pl$lit(", "),
        pl$col("req_first_name")$fill_null(pl$lit("")),
        pl$lit(" "),
        pl$col("req_middle_initial")$fill_null(pl$lit(""))
      )$str$strip_chars()$alias("requester")
    )$
    drop("req_last_name", "req_first_name", "req_middle_initial")
}

#' p583900: ISO, e.g. `2005-09-26`. Also carries six structured requester
#' columns (REQ_RACE/REQ_BIRTH_YEAR/...) that aren't in the other packets;
#' drop them so the diagonal concat's schema stays clean.
post_p583900 <- function(lf) {
  fmt <- "%Y-%m-%d"
  date_cols <- c(
    "appointed", "last_promotion", "award_requested",
    "incident_start", "incident_end", "ceremony_date"
  )
  extra_cols <- c(
    "REQ_RACE", "REQ_BIRTH_YEAR", "REQ_SEX",
    "REQ_APPOINTED_DATE", "REQ_STAR_NO_AT_TIME", "REQ_CURR_TITLE"
  )
  lf$
    with_columns(
      pl$col(!!!date_cols)$str$to_date(format = fmt, strict = FALSE)
    )$
    drop(!!!extra_cols)
}

#' Read all four packets, normalize, concat, and dedupe.
#'
#' Packets overlap by `uid_award` (same system-wide IDs recur across
#' releases). We concat in release order (oldest -> newest) and dedupe
#' with `keep = "last"` so the most recent release's values win on shared
#' IDs. Rows with a null `uid_award` are dropped — they can't be joined
#' to anything downstream.
#' @export
build <- function() {
  src <- path()

  parts <- list()
  if (length(src$p061715)) {
    parts$p061715 <- post_legacy(
      scan_aliased(src$p061715, schema_p061715(), alias_p061715())
    )
  }
  if (length(src$p456632)) {
    parts$p456632 <- post_legacy(
      scan_aliased(src$p456632, schema_p456632(), alias_p456632())
    )
  }
  if (length(src$p506887)) {
    parts$p506887 <- post_p506887(
      scan_aliased(src$p506887, schema_p506887(), alias_p506887())
    )
  }
  if (length(src$p583900)) {
    parts$p583900 <- post_p583900(
      scan_aliased(src$p583900, schema_p583900(), alias_p583900())
    )
  }
  stopifnot(length(parts) > 0L)

  unified <- if (length(parts) == 1L) {
    parts[[1]]
  } else {
    pl$concat(!!!parts, how = "diagonal")
  }

  unified$
    filter(pl$col("uid_award")$is_not_null())$
    unique("uid_award", keep = "last", maintain_order = TRUE)
}
