'.__module__.'

#' List files in a directory with regex option
#' @export
ls <- function(path, reg = "\\.csv$", base = "~/Documents/data/cpd/",
               ...) {
  box::use(fs[dir_ls])

  dir_ls(
    path    = paste0(base, path),
    regexp  = reg,
    recurse = TRUE,
    ...
  )
}

#' @export
glob <- function(path, base = "~/Documents/data/cpd/") {
  paste0(base, path)
}

#' Write dataframe to parquet dataset
#' @export
write <- function(x, group = "dt", dir) {

  box::use(
    arrow[write_dataset],
    clock[get_year],
    dplyr[group_by, if_else],
    glue[glue],
    polars[pl],
    rlang[sym]
  )

  x <-
    x$drop(
      c("last_name",
        "first_name",
        "middle_initial",
        "star")
    )

  if (is.null(group)) {
    x <- x$to_data_frame()
  } else {
    x <- group_by(x$to_data_frame(), year = get_year(!!sym(group)))
  }

  write_dataset(
    x,
    path = glue("db/{dir}"),
    format = "parquet"
  )

}

#' Extract foia references from path
#' @export
get_reference <- function(path) {
  box::use(stringr[str_extract])
  str_extract(path, pattern = "p\\d{6}")
}

#' Extract sheet number from path
#' @export
get_sheet <- function(path) {
  box::use(stringr[str_extract])
  str_extract(path, pattern = "p\\d{6}_(\\d{1})", group = 1)
}

#' Add leading zero to time
#' @export
pad_time <- function(x) {
  sprintf("%04s", x)
}

#' Parse date and datetime values
#' @export
parse_dt <- function(x,
                     format = "%d-%b-%Y %H%M",
                     zone = "America/Chicago") {

  box::use(clock[date_parse, date_time_parse])

  if (all(grepl(format, pattern = "%H"))) {
    date_time_parse(
      x,
      format = format,
      zone = zone,
      ambiguous = "latest",
      nonexistent = "shift-forward"
    )
  } else {
    date_parse(
      x,
      format = format
    )
  }

}

# Convert to polars and remove time zone from datetime cols
#' @export
polarize <- function(x, sort_by = "dt") {
  box::use(polars[pl])

  pl$DataFrame(
    x
  )$with_columns(
    pl$col(
      pl$Datetime(
        "ms",
        tz = "America/Chicago"
      )
    )$dt$replace_time_zone(
      NULL
    )
  )$sort(
    sort_by
  )$unique()
}

