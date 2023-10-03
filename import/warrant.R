'.__module__.'

#' Source of data
#' @export
source <- function() {
  box::use(hash[hash])

  hash(
    p638148 = 1
  )
}

#' Define the schema
#' @export
get_schema <- function() {

  box::use(
    arrow[schema, string]
  )

  schema(
    uid_warrant = string(),
    type = string(),
    dt = string(),
    city = string(),
    state = string(),
    zip = string(),
    beat = string(),
    area = string(),
    street_number = string(),
    street_direction = string(),
    street = string(),
    last_name = string(),
    first_name = string(),
    middle_initial = string(),
    yob = double(),
    appointed_year = double(),
    star = string(),
    unit = string(),
    beat_assignment = string(),
    role = string(),
    issued = string(),
    arrest = string(),
    property_recovered = string()
  )

}

#' Read the data, apply schema, and write dataset
#' @export
build <- function(p638148) {

  box::use(
    arrow[read_csv_arrow],
    dplyr[mutate],
    proc/utility[parse_dt, polarize]
  )

  read_csv_arrow(
    file = p638148,
    schema = get_schema(),
    skip = 1
  ) |>
    mutate(
      dt = parse_dt(dt, format = "%d-%b-%y"),
      issued = parse_dt(issued, format = "%d-%b-%y"),
      across(c(arrest, property_recovered), as.logical)
    ) |>
    polarize()

}
