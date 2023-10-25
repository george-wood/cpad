'.__module__.'

#' Source of data
#' @export
source <- function() {
  box::use(hash[hash])

  hash(
    p644845 = 0
  )
}

#' Key
#' @export
key <- function() {
  c("last_name",
    "middle_initial",
    "first_name",
    "star",
    "appointed",
    "yob")
}

#' Define the schema
#' @export
get_schema <- function() {

  box::use(
    arrow[schema, string, timestamp]
  )

  schema(
    uid_contact = string(),
    contact_type = string(),
    contact_type_description = string(),
    card = string(),
    dt = string(),
    first.last_name = string(),
    first.middle_initial = string(),
    first.first_name = string(),
    first.star = string(),
    first.appointed = string(),
    first.gender = string(),
    first.race = string(),
    first.yob = double(),
    second.last_name = string(),
    second.middle_initial = string(),
    second.first_name = string(),
    second.star = string(),
    second.appointed = string(),
    second.gender = string(),
    second.race = string(),
    second.yob = double(),
    supervisor.last_name = string(),
    supervisor.middle_initial = string(),
    supervisor.first_name = string(),
    supervisor.star = string(),
    supervisor.appointed = string(),
    supervisor.gender = string(),
    supervisor.race = string(),
    supervisor.yob = double(),
    civilian_clothing = string(),
    rd = string(),
    event = string(),
    event_assigneed_by_cd = string(),
    hotspot = string(),
    mission = string(),
    unit = string(),
    created_by = string(),
    created_dt = string(),
    modified_by = string(),
    modified_dt = string(),
    submitting_beat = string(),
    submitting_unit = string(),
    street_number = string(),
    street_direction = string(),
    street = string(),
    city = string(),
    state = string(),
    zip = string(),
    district = string(),
    sector = string(),
    beat = string(),
    area = string(),
    ward = string(),
    location = string(),
    civilian_age = double(),
    civilian_age_to = double(),
    civilian_gender = string(),
    civilian_race = string(),
    civilian_height = string(),
    civilian_weight = string(),
    civilian_build = string(),
    civilian_eye_color = string(),
    civilian_hair_color = string(),
    civilian_hairstyle = string(),
    civilian_complexion = string(),
    civilian_facial_hair = string(),
    residence_district = string(),
    residence_sector = string(),
    residence_beat = string(),
    residence_area = string(),
    residence_ward = string(),
    bus_area = string(),
    bus_beat = string(),
    bus_district = string(),
    bus_sector = string(),
    bus_ward = string(),
    gang_name = string(),
    faction_name = string(),
    gang_lookout = string(),
    gang_narcotic_related = string(),
    gang_other_name = string(),
    gang_other = string(),
    gang_security = string(),
    intimidation = string(),
    civilian_clothing_description = string(),
    name_verified = string(),
    completion = string(),
    contact_card_status = string(),
    vehicle_make = string(),
    vehicle_description = string(),
    vehicle_style = string(),
    vehicle_style_description = string(),
    vehicle_type = string(),
    vehicle_model = string(),
    vehicle_color_top = string(),
    vehicle_color_bottom = string(),
    handcuffed = string(),
    vehicle_involved = string(),
    dispersal_time = string(),
    number_of_persons_dispersed = string(),
    suspect_narcotic_activity = string(),
    enforcement_action_taken = string(),
    indicative_casing = string(),
    fits_description_offender = string(),
    other_factor = string(),
    pat_down = string(),
    pat_down_consent = string(),
    pat_down_receipt_given = string(),
    verbal_threats = string(),
    knowledge_of_prior = string(),
    actions_indicative_violence = string(),
    violent_crime = string(),
    suspicious_object = string(),
    other_reasonable_suspicion = string(),
    weapons_or_contraband_found = string(),
    firearm = string(),
    cocaine = string(),
    cocaine_amount = string(),
    heroin = string(),
    heroin_amount = string(),
    other_contraband = string(),
    other_contraband_description = string(),
    other_weapon = string(),
    other_weapon_description = string(),
    cannabis = string(),
    cannabis_amount = string(),
    other_controlled_substance = string(),
    other_controlled_substance_description = string(),
    other_controlled_substance_amount = string(),
    search = string(),
    search_contraband_found = string(),
    search_firearm = string(),
    search_cocaine = string(),
    search_cocaine_amount = string(),
    search_heroin = string(),
    search_heroin_amount = string(),
    search_other_contraband = string(),
    search_other_contraband_description = string(),
    search_cannabis = string(),
    search_cannabis_amount = string(),
    search_other_controlled_substance = string(),
    search_other_controlled_substance_description = string(),
    search_other_controlled_substance_amount = string(),
    other_description = string(),
    other_inventory_number = string(),
    other_weapon_inventory_number = string(),
    para = string(),
    para_inventory_number = string(),
    search_consent = string(),
    search_other_weapon_description = string(),
    search_other_weapon = string(),
    search_property = string(),
    stolen_property = string(),
    stolen_property_inventory_number = string(),
    search_alcohol = string(),
    search_alcohol_inventory_number = string(),
    search_firearm_inventory_number = string(),
    search_other_decription = string(),
    search_other = string(),
    search_other_inventory_number = string(),
    search_other_weapon_inventory_number = string(),
    search_para = string(),
    search_para_inventory_number = string(),
    search_stolen_property = string(),
    search_stolen_property_inventory_number = string(),
    vehicle_stopped = string(),
    vehicle_year = double(),
    proximity_to_crime = string(),
    body_camera = string(),
    car_camera = string(),
    information_refused = string(),
    cannabis_inventory_number = string(),
    cocaine_inventory_number = string(),
    heroin_inventory_number = string(),
    other_controlled_substance_inventory_number = string(),
    search_cannabis_inventory_number = string(),
    search_cocaine_inventory_number = string(),
    search_heroin_inventory_number = string(),
    search_other_controlled_substance_inventory_number = string(),
    firearm_inventory_number = string(),
    alcohol = string(),
    alcohol_inventory_number = string(),
    enforcement_type = string(),
    cited_violations = string(),
    enforcement_id = string()
  )

}

#' Read the data, apply schema, and wrangle
#' @export
build <- function(p646845) {

  box::use(
    arrow[read_csv_arrow],
    dplyr[mutate, across, contains],
    tidyr[pivot_longer],
    proc/utility[parse_dt, polarize]
  )

  read_csv_arrow(
    file = p646845,
    schema = get_schema(),
    na = c("", " "),
    skip = 1
  ) |>
    mutate(
      across(
        ends_with("dt"),
        function(x)
          parse_dt(
            x,
            format = c(
              "%d-%b-%Y %H:%M",
              "%Y/%m/%d %H:%M:%S"
            )
          )
      ),
      across(
        ends_with("appointed"),
        function(x)
          parse_dt(
            x,
            format = "%Y/%m/%d"
          )
      ),
      across(
        where(function(x) all(x[!is.na(x)] %in% c("Y", "N"))),
        function(z) z == "Y"
      )
    ) |>
    pivot_longer(
      cols = contains("."),
      names_to = c("role", ".value"),
      names_pattern = "^(first|second|supervisor)\\.(.*)"
    ) |>
    polarize()

}
