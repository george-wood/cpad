'.__module__.'

#' Source of data
#' @export
source <- function() {
  box::use(hash[hash])

  hash::hash(
    p621077 = 1
  )
}

#' Define the schema
#' @export
get_schema <- function() {

  box::use(
    arrow[schema, string, timestamp]
  )

  schema(
    beat = string(),
    description = string(),
    unit = string(),
    unit_name = string(),
    radio_zone = string(),
    dt_start = timestamp(),
    dt_end = timestamp()
  )

}

#' Read the data, apply schema, and write dataset
#' @export
build <- function(p621077) {

  box::use(
    arrow[read_csv_arrow],
    proc/utility[polarize]
  )

  read_csv_arrow(
    file = p621077,
    schema = get_schema(),
    skip = 1
  ) |>
    polarize(sort_by = "beat")

}
