'.__module__.'

#' Source of data
#' @export
source <- function() {
  box::use(hash[hash])

  hash(
    p046360 = c(0, 2),
    p456008 = c(0, 2),
    p461899 = c(0, 2),
    p583646 = c(1, 2)
  )
}

#' Define the schema
#' @export
get_schema <- function(reference, sheet) {

  box::use(
    arrow[schema, string, timestamp, date32],
    cli[cli_abort]
  )

  schema_A <- schema(
    uid_force = string(),
    rd = string(),
    cr = string(),
    cb = string(),
    event = string(),
    beat = string(),
    block = string(),
    street_direction = string(),
    street = string(),
    location = string(),
    date = string(),
    time = string(),
    outdoor = string(),
    lighting_condition = string(),
    weather_condition = string(),
    notify_oemc = string(),
    notify_district_sergeant = string(),
    notify_operational_command = string(),
    notify_detective_division = string(),
    weapons_discharged = double(),
    party_fired_first = string(),
    last_name = string(),
    first_name = string(),
    gender = string(),
    race = string(),
    age = double(),
    appointed = string(),
    assigned_unit = string(),
    assigned_unit_detail = string(),
    assigned_beat = string(),
    rank = string(),
    duty_status = string(),
    officer_injured = string(),
    in_uniform = string(),
    civilian_gender = string(),
    civilian_race = string(),
    civilian_age = double(),
    civilian_yob = double(),
    civilian_armed = string(),
    civilian_injured = string(),
    civilian_alleged_injury = string()
  )

  schema_B <- schema(
    uid_force = string(),
    rd = string(),
    cr = string(),
    cb = string(),
    ir = string(),
    event = string(),
    beat = string(),
    block = string(),
    street_direction = string(),
    street = string(),
    location = string(),
    dt = string(),
    outdoor = string(),
    lighting_condition = string(),
    weather_condition = string(),
    notify_oemc = string(),
    notify_district_sergeant = string(),
    notify_operational_command = string(),
    notify_detective_division = string(),
    weapons_discharged = double(),
    party_fired_first = string(),
    last_name = string(),
    first_name = string(),
    middle_initial = string(),
    gender = string(),
    race = string(),
    yob = double(),
    appointed = string(),
    assigned_unit = string(),
    assigned_unit_detail = string(),
    assigned_beat = string(),
    rank = string(),
    duty_status = string(),
    officer_injured = string(),
    in_uniform = string(),
    current_rank = string(),
    current_unit = string(),
    current_beat_assigned = string(),
    civilian_gender = string(),
    civilian_race = string(),
    civilian_yob = double(),
    civilian_armed = string(),
    civilian_injured = string(),
    civilian_alleged_injury = string(),
    report_created = timestamp(),
    policy_compliance = string(),
  )

  schema_C <- schema(
    uid_force = string(),
    person = string(),
    resistance_type = string(),
    action = string(),
    description = string()
  )

  schema_D <- schema(
    uid_force = string(),
    person = string(),
    resistance_type = string(),
    action = string()
  )

  schm <- list(
    p046360_0 = schema_A,
    p046360_2 = schema_C,
    p456008_0 = schema_A,
    p456008_2 = schema_C,
    p461899_0 = schema_A,
    p461899_2 = schema_C,
    p583646_1 = schema_B,
    p583646_2 = schema_D
  )

  if (missing(reference)) {
    schm
  } else {
    if (missing(sheet)) {
      cli_abort("Specify a sheet, e.g. `sheet = 0`")
    }
    schm[[paste(reference, sheet, sep = "_")]]
  }

}


#' Read the data, apply schema, and write dataset
#' @export
build <- function(report, action) {

  box::use(
    arrow[read_csv_arrow],
    clock[get_year],
    dplyr[coalesce, distinct, if_else, left_join, mutate],
    purrr[map, list_rbind],
    tidyselect[starts_with],
    proc/utility[get_reference, get_sheet, parse_dt, pad_time, polarize]
  )

  report <-
    map(
      report,
      \(x)
      read_csv_arrow(
        file = x,
        schema = get_schema(
          reference = get_reference(x),
          sheet = get_sheet(x)
        ),
        timestamp_parsers = c(
          "%Y-%b-%d %H%M",
          "%m/%d/%y %H%M",
          "%Y-%b-%d %H%M",
          "%Y-%m-%dT%H:%M:%S"
        ),
        skip = 1
      )
    ) |>
    list_rbind() |>
    mutate(
      appointed = parse_dt(
        appointed,
        format = c("%Y-%b-%d",
                   "%Y-%m-%d",
                   "%Y/%m/%d")
      ),
      dt = parse_dt(
        coalesce(dt, paste(date, pad_time(time))),
        format = c("%Y-%m-%d %H%M",
                   "%m/%d/%y %H%M",
                   "%Y-%b-%d %H%M",
                   "%Y/%m/%d %H%M")
      ),
      across(
        where(
          function(x)
            all(x[!is.na(x)] %in% c("True", "TRUE", "False", "FALSE"))
        ),
        function(z) z %in% c("True", "TRUE")
      ),
      yob_lower = if_else(
        is.na(yob),
        get_year(appointed) - age - 1,
        yob
      ),
      .keep = "unused"
    ) |>
    distinct()

  action <-
    map(
      action,
      \(x)
      read_csv_arrow(
        file = x,
        schema = get_schema(
          reference = get_reference(x),
          sheet = get_sheet(x)
        ),
        skip = 1
      )
    ) |>
    list_rbind() |>
    distinct()

  left_join(
    report,
    action,
    by = "uid_force"
  ) |>
    polarize()

}

