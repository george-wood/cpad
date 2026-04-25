'.__module__.'

#' Shared source specifications for the CPAD pipeline. Single source of
#' truth for: the set of sources, each source's join kind, its join
#' key columns, whether it gets an event (AID) stage, and where each of
#' its stage outputs lives on disk.
#'
#' Consumed by `_targets.R` to build the DAG programmatically — change
#' the table here and both the target graph and the path layout pick it
#' up automatically.

box::use(
  proc/officer
)

#' Per-source spec.
#'
#' Fields:
#'   name     character. Source name (also the final parquet stem).
#'   key      "none" | "regular" | "asof". Whether/how the UID stage joins.
#'   on       character vector. Join key for regular UID joins.
#'   by       character vector. Asof-join `by` columns for asof UID joins.
#'   event    logical. Whether the source gets an AID (event) stage.
#' @export
sources <- function() {
  k <- officer$key()
  list(
    list(name = "arrest",     key = "regular", on = k,                                        event = TRUE),
    list(name = "assignment", key = "regular", on = k,                                        event = FALSE),
    list(name = "award",      key = "regular", on = k,                                        event = FALSE),
    list(name = "beat",       key = "none",                                                   event = FALSE),
    list(name = "contact",    key = "asof",    by = c("last_name", "first_name"),             event = TRUE),
    list(name = "force",      key = "asof",    by = c("last_name", "first_name", "appointed"),event = TRUE),
    list(name = "military",   key = "regular", on = k,                                        event = FALSE),
    list(name = "roster",     key = "none",                                                   event = FALSE),
    list(name = "stop",       key = "regular", on = k,                                        event = TRUE),
    list(name = "warrant",    key = "regular", on = gsub("appointed", "appointed_year", k),   event = FALSE)
  )
}

#' Final (canonical) output path — flat, regardless of stage count.
#' @export
final_path <- function(name) paste0("db/", name, ".parquet")

#' Raw (stage-1) output path. If a source has no keyed and no event stage,
#' raw IS the final stage, so it goes straight to db/<name>.parquet.
#' @export
raw_path <- function(s) {
  if (s$key == "none" && !s$event) final_path(s$name)
  else paste0("db/stages/raw/", s$name, ".parquet")
}

#' Keyed (stage-2) output path. If a source has no event stage, keyed
#' IS the final stage, so it goes straight to db/<name>.parquet.
#' Only defined for sources with key != "none".
#' @export
keyed_path <- function(s) {
  if (s$event) paste0("db/stages/keyed/", s$name, ".parquet")
  else final_path(s$name)
}

#' Event (stage-3) output path. Final stage for event sources.
#' Only defined for sources with event = TRUE.
#' @export
event_path <- function(s) final_path(s$name)

#' Look up a single source spec by name. Errors if unknown.
#' @export
get <- function(name) {
  for (s in sources()) if (identical(s$name, name)) return(s)
  stop("unknown source: ", name)
}
