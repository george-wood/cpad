'.__module__.'

#' Source of data
#' @export
source <- function() {
  box::use(hash[hash])

  hash(
    p058306 = 2012:2015
  )
}

#' Define the schema
#' @export
get_schema <- function() {

  box::use(
    arrow[schema, string]
  )

  schema(
    date = string(),
    dt = string(),
    street_number = string(),
    direction = string(),
    street = string(),
    district = string(),
    beat = string(),
    contact_type = string(),
    first.last_name = string(),
    first.first_name = string(),
    first.gender = string(),
    first.race = string(),
    first.age = double(),
    second.last_name = string(),
    second.first_name = string(),
    second.gender = string(),
    second.race = string(),
    second.age = double(),
    civilian_gender = string(),
    civilian_race = string(),
    civilian_age = double(),
    civilian_height = double(),
    civilian_weight = double(),
    civilian_build = string(),
    civilian_hair_color = string(),
    civilian_hairstyle = string(),
    civilian_complexion = string(),
    civilian_clothing = string()
  )

}

#' Read the data, apply schema, and write dataset
#' @export
build <- function(p058306) {

  box::use(
    arrow[read_csv_arrow],
    clock[as_date, get_year],
    dplyr[mutate, n],
    purrr[map, list_rbind],
    tidyr[pivot_longer],
    proc/utility[parse_dt, pad_time, polarize]
  )

  map(
    p058306,
    \(x)
    read_csv_arrow(
      file = x,
      schema = get_schema(),
      na = c("", " "),
      skip = 1
    )
  ) |>
    list_rbind() |>
    mutate(
      uid_contact = paste0("c", 1:n()),
      dt = parse_dt(
        paste(date, pad_time(dt)),
        format = c(
          "%d-%b-%y %H:%M",
          "%Y-%m-%d %H:%M:%S"
        )
      ),
      date = as_date(dt)
    ) |>
    pivot_longer(
      cols = contains("."),
      names_to = c("role", ".value"),
      names_pattern = "^(first|second)\\.(.*)"
    ) |>
    mutate(
      yob_lower = get_year(dt) - age - 1
    ) |>
    polarize()

}
