'.__module__.'

box::use(
  polars[pl],
  proc/utility[scan_aliased, data_files]
)

#' Path to data
path <- function() {
  list(
    parking = data_files("ticket", reg = "parking_tickets.csv")
  )
}

#' Define the schema
get_schema <- function() {
  list(
    ticket_number = pl$String,
    issue_date = pl$String,
    violation_location = pl$String,
    license_plate_number = pl$String,
    license_plate_state = pl$String,
    license_plate_type = pl$String,
    zipcode = pl$String,
    violation_code = pl$String,
    violation_description = pl$String,
    unit = pl$String,
    unit_description = pl$String,
    vehicle_make = pl$String,
    fine_level1_amount = pl$Float64,
    fine_level2_amount = pl$Float64,
    current_amount_due = pl$Float64,
    total_payments = pl$Float64,
    ticket_queue = pl$String,
    ticket_queue_date = pl$String,
    notice_level = pl$String,
    notice_number = pl$String,
    hearing_disposition = pl$String,
    officer = pl$String,
    normalized_address = pl$String,
    year = pl$Int32,
    month = pl$Int32,
    hour = pl$Int32,
    ward = pl$String,
    tract_id = pl$String,
    blockgroup_geoid = pl$String,
    community_area_number = pl$String,
    community_area_name = pl$String,
    geocode_accuracy = pl$Float64,
    geocode_accuracy_type = pl$String,
    geocoded_address = pl$String,
    geocoded_lng = pl$Float64,
    geocoded_lat = pl$Float64
  )
}

#' Alias for column names
alias <- function() {
  list(
    ticket_number = "uid_ticket",
    issue_date = "dt",
    officer = "star",
    geocoded_lng = "longitude",
    geocoded_lat = "latitude"
  )
}

#' Read the data, apply schema, and write dataset
#' @export
build <- function() {
  scan_aliased(path()$parking, get_schema(), alias())$
    filter(
      pl$col("year")$gt(2013)
    )$
    with_columns(
      pl$
        col("dt")$
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
    )$
    filter(
      pl$col("dt")$is_not_null()
    )
}
