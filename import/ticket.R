'.__module__.'

#' Source of data
#' @export
source <- function() {
  "www.propublica.org/datastore/dataset/chicago-parking-ticket-data"
}

#' Define the schema
#' @export
get_schema <- function() {

  box::use(
    arrow[schema, date32, string, timestamp]
  )

  schema(
    uid_ticket = string(),
    dt = timestamp(),
    violation_location = string(),
    license_plate_number = string(),
    license_plate_state = string(),
    license_plate_type = string(),
    zipcode = string(),
    violation_code = string(),
    violation_description = string(),
    unit = string(),
    unit_description = string(),
    vehicle_make = string(),
    fine_level1_amount = double(),
    fine_level2_amount = double(),
    current_amount_due = double(),
    total_payments = double(),
    ticket_queue = string(),
    ticket_queue_date = timestamp(),
    notice_level = string(),
    notice_number = string(),
    hearing_disposition = string(),
    star = string(),
    normalized_address = string(),
    year = double(),
    month = double(),
    hour = double(),
    ward = string(),
    tract_id = string(),
    blockgroup_geoid = string(),
    community_area_number = string(),
    community_area_name = string(),
    geocode_accuracy = double(),
    geocode_accuracy_type = string(),
    geocoded_address = string(),
    longitude = double(),
    latitude = double()
  )

}

#' Read the data, apply schema, and write dataset
#' @export
build <- function(path) {

  box::use(
    arrow[read_csv_arrow],
    dplyr[filter],
    polars[pl],
    proc/utility[polarize]
  )

  res <-
    read_csv_arrow(
      file = path,
      schema = get_schema(),
      skip = 1
    ) |>
    filter(
      year >= 2014
    ) |>
    polarize()

  res$with_columns(pl$col("star")$str$replace("^0+", ""))

}

