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
