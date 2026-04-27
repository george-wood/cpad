'.__module__.'

#' Per-event-source match coverage report.
#'
#' Builds a small table of match rates across event sources and writes
#' it to a markdown file. Also prints a fixed-width version to stdout
#' so the numbers are visible in the {targets} log when the diagnostic
#' target runs.
#'
#' Columns:
#'   rows          row count
#'   in timeframe  % with non-null uid AND dt inside the assignment
#'                 coverage window. The population for which a shift
#'                 match is even possible.
#'   strict match  among in-timeframe rows, % with non-null aid where
#'                 event.dt falls in the strict window
#'                 [dt_start - 1h, dt_end + 1h] of the matched shift.
#'   loose match   among in-timeframe rows, % with non-null aid where
#'                 event.dt falls in the loose-only band
#'                 (dt_end + 1h, dt_end + 3h] of the matched shift
#'                 (i.e., matched but outside the strict window).

box::use(
  polars[pl]
)

assignment_bounds <- function(path) {
  as.data.frame(
    pl$scan_parquet(path)$select(
      pl$min("dt_start")$alias("lo"),
      pl$max("dt_end")$alias("hi")
    )$collect()
  )
}

rates <- function(path, bounds) {
  lf <- pl$scan_parquet(path)
  in_tf <- pl$col("uid")$is_not_null() &
    pl$col("dt")$is_between(bounds$lo, bounds$hi)

  top <- as.data.frame(
    lf$select(
      pl$len()$alias("total"),
      in_tf$cast(pl$Float64)$mean()$alias("in_timeframe_rate")
    )$collect()
  )

  panel <- as.data.frame(
    lf$filter(in_tf)$select(
      (pl$col("aid")$is_not_null() & pl$col("strict")$eq(TRUE))$
        cast(pl$Float64)$mean()$alias("strict_rate"),
      (pl$col("aid")$is_not_null() & pl$col("strict")$eq(FALSE))$
        cast(pl$Float64)$mean()$alias("loose_rate")
    )$collect()
  )

  c(
    total        = top$total,
    in_timeframe = top$in_timeframe_rate,
    strict       = panel$strict_rate,
    loose        = panel$loose_rate
  )
}

build_rows <- function(event_paths, results) {
  lapply(names(event_paths), function(s) {
    c(
      source    = s,
      rows      = format(results["total", s], big.mark = ","),
      timeframe = sprintf("%.1f%%", 100 * results["in_timeframe", s]),
      strict    = sprintf("%.1f%%", 100 * results["strict", s]),
      loose     = sprintf("%.1f%%", 100 * results["loose", s])
    )
  })
}

#' Render assignment bounds as a date range (no time-of-day).
fmt_window <- function(bounds) {
  sprintf(
    "%s to %s",
    format(as.Date(bounds$lo), "%Y-%m-%d"),
    format(as.Date(bounds$hi), "%Y-%m-%d")
  )
}

print_text <- function(bounds, rows) {
  cat(sprintf("Assignment window: %s\n\n", fmt_window(bounds)))

  cols <- c("source", "rows", "in timeframe", "strict match", "loose match")
  widths <- pmax(
    nchar(cols),
    vapply(seq_along(cols), function(i) {
      max(nchar(vapply(rows, `[[`, character(1), i)))
    }, integer(1))
  )

  pad <- function(x, w) format(x, width = w)
  cat(paste(mapply(pad, cols, widths), collapse = "  "), "\n", sep = "")
  cat(paste(strrep("-", widths), collapse = "  "), "\n", sep = "")
  for (r in rows) {
    cat(paste(mapply(pad, unname(r), widths), collapse = "  "), "\n", sep = "")
  }
}

write_markdown <- function(path, bounds, rows) {
  lines <- c(
    "# CPAD pipeline diagnostics",
    "",
    sprintf("_Assignment window: %s._", fmt_window(bounds)),
    "",
    "| source | rows | in timeframe | strict match | loose match |",
    "|---|---:|---:|---:|---:|",
    vapply(rows, function(r) {
      sprintf("| %s | %s | %s | %s | %s |", r[[1]], r[[2]], r[[3]], r[[4]], r[[5]])
    }, character(1))
  )
  writeLines(lines, path)
}

#' Build the diagnostics report.
#'
#' @param event_paths Named character vector: names are source labels
#'   (used as table row labels), values are paths to event parquets.
#' @param assignment_path Path to assignment.parquet (for the coverage
#'   bounds).
#' @param out_path Where to write the markdown file.
#' @return out_path (so callers — including the {targets} target — can
#'   chain on it for `format = "file"` tracking).
#' @export
build <- function(event_paths, assignment_path, out_path) {
  bounds <- assignment_bounds(assignment_path)
  results <- vapply(event_paths, \(p) rates(p, bounds), numeric(4))
  colnames(results) <- names(event_paths)
  rows <- build_rows(event_paths, results)

  print_text(bounds, rows)
  write_markdown(out_path, bounds, rows)
  out_path
}
