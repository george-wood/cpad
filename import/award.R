'.__module__.'

box::use(
  polars[pl],
  hash[hash],
  proc/utility[scan_aliased, ls]
)

#' Source of data
#' @export
source <- function() {
  hash(
    p061715 = 0,
    p456632 = 0,
    p506887 = 0,
    p583900 = 0
  )
}

#' Path to data
path <- function() {
  list(
    p061715 = ls("award/p061715"),
    p456632 = ls("award/p456632"),
    p506887 = ls("award/p506887"),
    p583900 = ls("award/p583900")
  )
}

#' Define the schema
#' @export
get_schema <- function() {
  list(
    # p061715 and p456632
    LAST_NME = pl$String,
    # p506887 and p583900
    AWARDEE_LASTN = pl$String
  )
}

#' Alias for column names
alias <- function() {
  list(
    # p061715 and p456632
    LAST_NME = "last_name",
    # p506887 and p583900
    AWARDEE_LASTN = "last_name"
  )
}

#' Key columns (shared across sources after rename)
key <- function() {
  unique(unlist(alias(), use.names = FALSE))
}


#' Read the data, apply schema, and wrangle
#' @export
build <- function() {
  pl$concat(
    !!!lapply(
      path(),
      \(x)
      scan_aliased(x, get_schema(), alias())$
        select(key())
    ),
    how = "vertical"
  )
}
