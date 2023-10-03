'.__module__.'

#' Source of data
#' @export
source <- function() {
  box::use(hash[hash])

  hash::hash(
    p606699 = 3
  )
}

#' Define the schema
#' @export
get_schema <- function() {

  box::use(
    arrow[schema, string, date32]
  )

  schema(
    last_name = string(),
    middle_initial = string(),
    first_name = string(),
    description = string(),
    star = string(),
    race = string(),
    gender = string(),
    yob = double(),
    appointed = date32(),
    active_branch = string(),
    active_rank = string(),
    active_discharged = date32(),
    active_discharge_type = string(),
    reserve_start = date32(),
    reserve_branch = string(),
    reserve_rank = string(),
    reserve_discharged = date32(),
    reserve_discharge_type = string()
  )

}

#' Read the data, apply schema, and write dataset
#' @export
build <- function(p606699) {

  box::use(
    arrow[read_csv_arrow],
    proc/utility[polarize]
  )

  read_csv_arrow(
    file = p606699,
    schema = get_schema(),
    skip = 1
  ) |>
    polarize(sort_by = "appointed")

}
