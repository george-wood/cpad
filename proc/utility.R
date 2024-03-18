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
