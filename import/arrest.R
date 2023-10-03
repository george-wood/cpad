'.__module__.'

#' Source of data
#' @export
source <- function() {
  box::use(hash[hash])

  hash::hash(
    p701162 = 2:6,
    p708085 = 1:5
  )
}

#' Define the schema
#' @export
get_schema <- function(reference) {

  box::use(
    arrow[schema, string, timestamp, date32]
  )

  schm <- list(
    p701162 = schema(
      first_name = string(),
      middle_initial = string(),
      last_name = string(),
      race = string(),
      gender = string(),
      role = string(),
      uid_arrest = string(),
      rd = string(),
      dt = timestamp(),
      civilian_race = string(),
      civilian_age = double(),
      civilian_gender = string(),
      statute = string(),
      charge_type = string()
    ),
    p708085 = schema(
      uid_arrest = string(),
      dt = timestamp(),
      role = string(),
      first_name = string(),
      last_name = string(),
      appointed = date32(),
      yob = double()
    )
  )

  if (missing(reference)) {
    schm
  } else {
    schm[[reference]]
  }

}

#' Read the data, apply schema, bind and filter
reader <- function(path) {

  box::use(
    arrow[read_csv_arrow],
    purrr[map, list_rbind],
    dplyr[filter, distinct],
    proc/utility[get_reference]
  )

  map(
    path,
    \(x)
    read_csv_arrow(
      file = x,
      schema = get_schema(
        get_reference(x)
      ),
      timestamp_parsers = c(
        "%d-%b-%Y %H:%M",
        "%Y-%m-%d %H:%M:%S"
      ),
      skip = 1
    )
  ) |>
    list_rbind() |>
    filter(uid_arrest != "J") |>
    distinct()

}

#' Execute read, bind, filter and write dataset
#' @export
build <- function(p701162, p708085) {

  box::use(
    dplyr[left_join],
    proc/utility[polarize]
  )

  left_join(
    reader(p701162),
    reader(p708085),
    by = c("uid_arrest", "dt", "role", "last_name", "first_name")
  ) |>
    polarize()

}
