'.__module__.'

box::use(
  fs[dir_ls],
  polars[pl]
)

#' List data files under a subpath of the raw-data root.
#'
#' Renamed from `ls` to avoid shadowing `base::ls` — reading
#' `ls("arrest/p701162")` in an import module looked like an R primitive.
#' @export
data_files <- function(path, reg = "\\.csv$",
                       base = "~/Documents/data/cpd/", ...) {
  dir_ls(
    path    = paste0(base, path),
    regexp  = reg,
    recurse = TRUE,
    ...
  )
}

#' Rename `lf`'s columns per `alias`, dropping entries whose old names are
#' absent from the frame so the same alias can cover heterogeneous sources.
#'
#' `alias` is a named list in the form `list(OLD = "new", ...)`.
#' @export
rename_aliased <- function(lf, alias) {
  present <- alias[intersect(names(alias), names(lf))]
  do.call(lf$rename, present)
}

#' Scan a CSV (or multiple CSVs) and rename its columns per an alias.
#'
#' Extra `...` are forwarded to `pl$scan_csv()` (e.g. `missing_columns`,
#' `null_values`).
#' @export
scan_aliased <- function(source, schema, alias, ...) {
  lf <- pl$scan_csv(
    source,
    schema_overrides = schema,
    try_parse_dates = FALSE,
    ...
  )
  rename_aliased(lf, alias)
}

#' Apply a `should rarely fire` filter and warn if the dropout rate
#' exceeds `threshold`. Returns the filtered lazy frame.
#'
#' Intended for canary filters like `dt is_not_null` after a melt,
#' where a high dropout rate signals upstream schema drift (the kind
#' that produced the contact 2014/2015 data-loss bug). `label` is
#' surfaced in the warning so callers know which site fired.
#' @export
assert_low_drop <- function(lf, keep, label, threshold = 0.05) {
  result <- lf$filter(keep)
  before <- as.data.frame(lf$select(pl$len())$collect())[[1]]
  after  <- as.data.frame(result$select(pl$len())$collect())[[1]]
  dropped <- before - after
  if (before > 0L && dropped / before > threshold) {
    warning(sprintf(
      paste0(
        "%s: filter dropped %d of %d rows (%.1f%%). ",
        "Unusually high — possible upstream schema drift."
      ),
      label, dropped, before, 100 * dropped / before
    ))
  }
  result
}
