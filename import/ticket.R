'.__module__.'

box::use(polars[pl])

#' Source of data
#' @export
source <- function() {
  "www.propublica.org/datastore/dataset/chicago-parking-ticket-data"
}

#' Define the schema
#' @export
get_schema <- function() {
  list(
    ticket_number = "character",
    issue_date = "character", # timestamp
    violation_location = "character",
    license_plate_number = "character",
    license_plate_state = "character",
    license_plate_type = "character",
    zipcode = "character",
    violation_code = "character",
    violation_description = "character",
    unit = "character",
    unit_description = "character",
    vehicle_make = "character",
    fine_level1_amount = "float64",
    fine_level2_amount = "float64",
    current_amount_due = "float64",
    total_payments = "float64",
    ticket_queue = "character",
    ticket_queue_date = "character", # timestamp
    notice_level = "character",
    notice_number = "character",
    hearing_disposition = "character",
    officer = "character",
    normalized_address = "character",
    year = "integer",
    month = "integer",
    hour = "integer",
    ward = "character",
    tract_id = "character",
    blockgroup_geoid = "character",
    community_area_number = "character",
    community_area_name = "character",
    geocode_accuracy = "float64",
    geocode_accuracy_type = "character",
    geocoded_address = "character",
    geocoded_lng = "float64",
    geocoded_lat = "float64"
  )
}

#' Alias for column names
alias <- function() {
  list(
    uid_ticket = "ticket_number",
    dt = "issue_date",
    star = "officer",
    longitude = "geocoded_lng",
    latitude = "geocoded_lat"
  )
}

#' Read the data, apply schema, and write dataset
#' @export
build <- function(path) {
  pl$
    scan_csv(
      path,
      dtypes = get_schema(),
      try_parse_dates = FALSE
    )$
    rename(
      alias()
    )$
    filter(
      pl$col("year")$gt(2013)
    )$
    with_columns(
      pl$
        col("issue_date")$
        str$to_datetime(
          format = "%Y-%m-%d %H:%M:%S",
          strict = FALSE
        ),
      pl$
        col("ticket_queue_date")$
        str$to_datetime(
          format = "%Y-%m-%d %H:%M:%S",
          strict = FALSE
        ),
      pl$
        col("star")$
        str$replace("^0+", "")
    )
}

