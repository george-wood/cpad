'.__module__.'

box::use(polars[pl])

#' Source of data
#' @export
source <- function() {
  box::use(hash[hash])

  hash(
    p644845 = 0
  )
}

#' Define the schema
#' @export
get_schema <- function() {
  list(
    CONTACT_CARD_ID = "character",
    CONTACT_CARD_ID = "character",
    CONTACT_TYPE_DESCR = "character",
    CARD_NO = "character",
    CONTACT_DATE_TIME = "character",
    FO_LAST = "character",
    FO_MIDDLE_INITIAL = "character",
    FO_FIRST = "character",
    FO_STAR = "character",
    FO_APPOINTED_DT = "character",
    FO_SEX = "character",
    FO_RACE = "character",
    FO_BIRTH_YR = "float64",
    SO_LAST = "character",
    SO_MIDDLE_INITIAL = "character",
    SO_FIRST = "character",
    SO_STAR = "character",
    SO_APPOINTED_DATE = "character",
    SO_SEX = "character",
    SO_RACE = "character",
    SO_BIRTH_YR = "float64",
    SUPV_LAST = "character",
    SUPV_MIDDLE_INITIAL = "character",
    SUPV_FIRST = "character",
    SUPV_STAR = "character",
    SUPV_APPOINTED_DATE = "character",
    SUPV_SEX = "character",
    SUPV_RACE = "character",
    SUPV_BIRTH_YR = "float64",
    CLOTHING_DESCR = "character",
    RD = "character",
    EVENT_NO = "character",
    EVENT_ASSIGNEED_BY_CD = "character",
    HOTSPOT_NO = "character",
    MISSION_NO = "character",
    CPD_UNIT_NO = "character",
    CREATED_BY_NAME = "character",
    CREATED_DATE = "character",
    MODIFIED_BY_NAME = "character",
    MODIFIED_DATE = "character",
    SUBMITTING_BEAT_CD = "character",
    SUBMITTING_UNIT = "character",
    STREET_NO = "character",
    STREET_DIRECTION_CD = "character",
    STREET_NME = "character",
    CITY = "character",
    STATE_CD = "character",
    ZIP_CD = "character",
    DISTRICT = "character",
    SECTOR = "character",
    BEAT = "character",
    AREA = "character",
    WARD = "character",
    LOCATION_CD = "character",
    AGE = "float64",
    AGE_TO = "float64",
    SEX_CODE_CD = "character",
    RACE = "character",
    HEIGHT = "character",
    WEIGHT = "character",
    BUILD_CODE_CD = "character",
    EYE_COLOR_CODE_CD = "character",
    HAIR_COLOR_CODE_CD = "character",
    HAIR_STYLE_CODE_CD = "character",
    COMPLEXION_CODE_CD = "character",
    FACIAL_HAIR_CD = "character",
    RES_DISTRICT = "character",
    RES_SECTOR = "character",
    RES_BEAT = "character",
    RES_AREA = "character",
    RES_WARD = "character",
    BUS_AREA = "character",
    BUS_BEAT = "character",
    BUS_DISTRICT = "character",
    BUS_SECTOR = "character",
    BUS_WARD = "character",
    GANG_NAME = "character",
    FACTION_NAME = "character",
    GANG_LOOKOUT_I = "character",
    GANG_NARCOTIC_RELATED_I = "character",
    GANG_OTHER = "character",
    GANG_OTHER_I = "character",
    GANG_SECURITY_I = "character",
    INTIMIDATION_I = "character",
    CLOTHING_DESCR_1 = "character",
    NAME_VERIFIED_I = "character",
    COMPLETION_I = "character",
    CONTACT_CARD_STATUS_CD = "character",
    MAKE_CD = "character",
    MAKE_DESCR = "character",
    STYLE_CD = "character",
    STYLE_DESCR = "character",
    TYPE_CD = "character",
    MODEL_DESCR = "character",
    COLOR_TOP = "character",
    COLOR_BOTTOM = "character",
    HANDCUFFED_I = "character",
    VEHICLE_INVOLVED_I = "character",
    DISPERSAL_TIME = "character",
    NUMBER_OF_PERSONS_DISPERSED = "character",
    SUSPECT_NARCOTIC_ACTIVITY_I = "character",
    ENFORCEMENT_ACTION_TAKEN_I = "character",
    INDICATIVE_CASING_I = "character",
    FITS_DESCRIPTION_OFFENDER_I = "character",
    OTHER_FACTOR_I = "character",
    PAT_DOWN_I = "character",
    PAT_DOWN_CONSENT_I = "character",
    PAT_DOWN_RECEIPT_GIVEN_I = "character",
    VERBAL_THREATS_I = "character",
    KNOWLEDGE_OF_PRIOR_I = "character",
    ACTIONS_INDICATIVE_VIOLENCE_I = "character",
    VIOLENT_CRIME_I = "character",
    SUSPICIOUS_OBJECT_I = "character",
    OTHER_REASONABLE_SUSPICION_I = "character",
    WEAPON_OR_CONTRABAND_FOUND_I = "character",
    FIREARM_I = "character",
    COCAINE_I = "character",
    COCAINE_AMOUNT = "character",
    HEROIN_I = "character",
    HEROIN_AMOUNT = "character",
    OTHER_CONTRABAND_I = "character",
    OTHER_CONTRABAND_DESCR = "character",
    OTHER_WEAPON_I = "character",
    OTHER_WEAPON_DESCR = "character",
    CANNABIS_I = "character",
    CANNABIS_AMOUNT = "character",
    OTHER_CON_SUB_I = "character",
    OTHER_CON_SUB = "character",
    OTHER_CON_SUB_AMT = "character",
    SEARCH_I = "character",
    SEARCH_CONTRABAND_FOUND_I = "character",
    SEARCH_FIREARM_I = "character",
    SEARCH_COCAINE_I = "character",
    SEARCH_COCAINE_AMOUNT = "character",
    SEARCH_HEROIN_I = "character",
    SEARCH_HEROIN_AMOUNT = "character",
    SEARCH_OTHER_CONTRABAND_I = "character",
    SEARCH_OTHER_CONTRABAND_DESCR = "character",
    SEARCH_CANNABIS_I = "character",
    SEARCH_CANNABIS_AMOUNT = "character",
    SEARCH_OTHER_CON_SUB_I = "character",
    SEARCH_OTHER_CON_SUB_DESCR = "character",
    SEARCH_OTHER_CON_SUB_AMT = "character",
    OTHER_DESCR = "character",
    OTHER_INVENTORY_NO = "character",
    OTHER_WEAPON_INVENTORY_NO = "character",
    PARA_I = "character",
    PARA_INVENTORY_NO = "character",
    SEARCH_CONSENT_I = "character",
    SEARCH_OTHER_WEAPON_DESCR = "character",
    SEARCH_OTHER_WEAPON_I = "character",
    SEARCH_PROPERTY_I = "character",
    STOLEN_PROPERTY_I = "character",
    STOLEN_PROPERTY_INVENTORY_NO = "character",
    S_ALCOHOL_I = "character",
    S_ALCOHOL_INVENTORY_NO = "character",
    S_FIREARM_INVENTORY_NO = "character",
    S_OTHER_DESCR = "character",
    S_OTHER_I = "character",
    S_OTHER_INVENTORY_NO = "character",
    S_OTHER_WEAPON_INVENTORY_NO = "character",
    S_PARA_I = "character",
    S_PARA_INVENTORY_NO = "character",
    S_STOLEN_PROPERTY_I = "character",
    S_STOLEN_PROPERTY_INVENTORY_NO = "character",
    VEHICLE_STOPPED_I = "character",
    V_YEAR = "float64",
    PROXIMITY_TO_CRIME_I = "character",
    BODY_CAMERA_I = "character",
    CAR_CAMERA_I = "character",
    INFORMATION_REFUSED_I = "character",
    CANNABIS_INVENTORY_NO = "character",
    COCAINE_INVENTORY_NO = "character",
    HEROIN_INVENTORY_NO = "character",
    OTHER_CON_SUB_INVENTORY_NO = "character",
    S_CANNABIS_INVENTORY_NO = "character",
    S_COCAINE_INVENTORY_NO = "character",
    S_HEROIN_INVENTORY_NO = "character",
    S_OTHER_CON_SUB_INVENTORY_NO = "character",
    FIREARM_INVENTORY_NO = "character",
    ALCOHOL_I = "character",
    ALCOHOL_INVENTORY_NO = "character",
    ENFORCEMENT_TYPE_CD = "character",
    CITED_VIOLATIONS_CD = "character",
    ENFORCEMENT_ID_NO = "character"
  )
}

#' Alias for column names
alias <- function() {
  list(
    uid_contact = "CONTACT_CARD_ID",
    contact_type = "CONTACT_TYPE_CD",
    contact_type_description = "CONTACT_TYPE_DESCR",
    card = "CARD_NO",
    dt = "CONTACT_DATE_TIME",
    first.last_name = "FO_LAST",
    first.middle_initial = "FO_MIDDLE_INITIAL",
    first.first_name = "FO_FIRST",
    first.star = "FO_STAR",
    first.appointed = "FO_APPOINTED_DT",
    first.gender = "FO_SEX",
    first.race = "FO_RACE",
    first.yob = "FO_BIRTH_YR",
    second.last_name = "SO_LAST",
    second.middle_initial = "SO_MIDDLE_INITIAL",
    second.first_name = "SO_FIRST",
    second.star = "SO_STAR",
    second.appointed = "SO_APPOINTED_DATE",
    second.gender = "SO_SEX",
    second.race = "SO_RACE",
    second.yob = "SO_BIRTH_YR",
    supervisor.last_name = "SUPV_LAST",
    supervisor.middle_initial = "SUPV_MIDDLE_INITIAL",
    supervisor.first_name = "SUPV_FIRST",
    supervisor.star = "SUPV_STAR",
    supervisor.appointed = "SUPV_APPOINTED_DATE",
    supervisor.gender = "SUPV_SEX",
    supervisor.race = "SUPV_RACE",
    supervisor.yob = "SUPV_BIRTH_YR",
    civilian_clothing = "CLOTHING_DESCR",
    rd = "RD",
    event = "EVENT_NO",
    event_assigneed_by_cd = "EVENT_ASSIGNEED_BY_CD",
    hotspot = "HOTSPOT_NO",
    mission = "MISSION_NO",
    unit = "CPD_UNIT_NO",
    created_by = "CREATED_BY_NAME",
    created_dt = "CREATED_DATE",
    modified_by = "MODIFIED_BY_NAME",
    modified_dt = "MODIFIED_DATE",
    submitting_beat = "SUBMITTING_BEAT_CD",
    submitting_unit = "SUBMITTING_UNIT",
    street_number = "STREET_NO",
    street_direction = "STREET_DIRECTION_CD",
    street = "STREET_NME",
    city = "CITY",
    state = "STATE_CD",
    zip = "ZIP_CD",
    district = "DISTRICT",
    sector = "SECTOR",
    beat = "BEAT",
    area = "AREA",
    ward = "WARD",
    location = "LOCATION_CD",
    civilian_age = "AGE",
    civilian_age_to = "AGE_TO",
    civilian_gender = "SEX_CODE_CD",
    civilian_race = "RACE",
    civilian_height = "HEIGHT",
    civilian_weight = "WEIGHT",
    civilian_build = "BUILD_CODE_CD",
    civilian_eye_color = "EYE_COLOR_CODE_CD",
    civilian_hair_color = "HAIR_COLOR_CODE_CD",
    civilian_hairstyle = "HAIR_STYLE_CODE_CD",
    civilian_complexion = "COMPLEXION_CODE_CD",
    civilian_facial_hair = "FACIAL_HAIR_CD",
    residence_district = "RES_DISTRICT",
    residence_sector = "RES_SECTOR",
    residence_beat = "RES_BEAT",
    residence_area = "RES_AREA",
    residence_ward = "RES_WARD",
    bus_area = "BUS_AREA",
    bus_beat = "BUS_BEAT",
    bus_district = "BUS_DISTRICT",
    bus_sector = "BUS_SECTOR",
    bus_ward = "BUS_WARD",
    gang_name = "GANG_NAME",
    faction_name = "FACTION_NAME",
    gang_lookout = "GANG_LOOKOUT_I",
    gang_narcotic_related = "GANG_NARCOTIC_RELATED_I",
    gang_other_name = "GANG_OTHER",
    gang_other = "GANG_OTHER_I",
    gang_security = "GANG_SECURITY_I",
    intimidation = "INTIMIDATION_I",
    civilian_clothing_description = "CLOTHING_DESCR_1",
    name_verified = "NAME_VERIFIED_I",
    completion = "COMPLETION_I",
    contact_card_status = "CONTACT_CARD_STATUS_CD",
    vehicle_make = "MAKE_CD",
    vehicle_description = "MAKE_DESCR",
    vehicle_style = "STYLE_CD",
    vehicle_style_description = "STYLE_DESCR",
    vehicle_type = "TYPE_CD",
    vehicle_model = "MODEL_DESCR",
    vehicle_color_top = "COLOR_TOP",
    vehicle_color_bottom = "COLOR_BOTTOM",
    handcuffed = "HANDCUFFED_I",
    vehicle_involved = "VEHICLE_INVOLVED_I",
    dispersal_time = "DISPERSAL_TIME",
    number_of_persons_dispersed = "NUMBER_OF_PERSONS_DISPERSED",
    suspect_narcotic_activity = "SUSPECT_NARCOTIC_ACTIVITY_I",
    enforcement_action_taken = "ENFORCEMENT_ACTION_TAKEN_I",
    indicative_casing = "INDICATIVE_CASING_I",
    fits_description_offender = "FITS_DESCRIPTION_OFFENDER_I",
    other_factor = "OTHER_FACTOR_I",
    pat_down = "PAT_DOWN_I",
    pat_down_consent = "PAT_DOWN_CONSENT_I",
    pat_down_receipt_given = "PAT_DOWN_RECEIPT_GIVEN_I",
    verbal_threats = "VERBAL_THREATS_I",
    knowledge_of_prior = "KNOWLEDGE_OF_PRIOR_I",
    actions_indicative_violence = "ACTIONS_INDICATIVE_VIOLENCE_I",
    violent_crime = "VIOLENT_CRIME_I",
    suspicious_object = "SUSPICIOUS_OBJECT_I",
    other_reasonable_suspicion = "OTHER_REASONABLE_SUSPICION_I",
    weapons_or_contraband_found = "WEAPON_OR_CONTRABAND_FOUND_I",
    firearm = "FIREARM_I",
    cocaine = "COCAINE_I",
    cocaine_amount = "COCAINE_AMOUNT",
    heroin = "HEROIN_I",
    heroin_amount = "HEROIN_AMOUNT",
    other_contraband = "OTHER_CONTRABAND_I",
    other_contraband_description = "OTHER_CONTRABAND_DESCR",
    other_weapon = "OTHER_WEAPON_I",
    other_weapon_description = "OTHER_WEAPON_DESCR",
    cannabis = "CANNABIS_I",
    cannabis_amount = "CANNABIS_AMOUNT",
    other_controlled_substance = "OTHER_CON_SUB_I",
    other_controlled_substance_description = "OTHER_CON_SUB",
    other_controlled_substance_amount = "OTHER_CON_SUB_AMT",
    search = "SEARCH_I",
    search_contraband_found = "SEARCH_CONTRABAND_FOUND_I",
    search_firearm = "SEARCH_FIREARM_I",
    search_cocaine = "SEARCH_COCAINE_I",
    search_cocaine_amount = "SEARCH_COCAINE_AMOUNT",
    search_heroin = "SEARCH_HEROIN_I",
    search_heroin_amount = "SEARCH_HEROIN_AMOUNT",
    search_other_contraband = "SEARCH_OTHER_CONTRABAND_I",
    search_other_contraband_description = "SEARCH_OTHER_CONTRABAND_DESCR",
    search_cannabis = "SEARCH_CANNABIS_I",
    search_cannabis_amount = "SEARCH_CANNABIS_AMOUNT",
    search_other_controlled_substance = "SEARCH_OTHER_CON_SUB_I",
    search_other_controlled_substance_description = "SEARCH_OTHER_CON_SUB_DESCR",
    search_other_controlled_substance_amount = "SEARCH_OTHER_CON_SUB_AMT",
    other_description = "OTHER_DESCR",
    other_inventory_number = "OTHER_INVENTORY_NO",
    other_weapon_inventory_number = "OTHER_WEAPON_INVENTORY_NO",
    para = "PARA_I",
    para_inventory_number = "PARA_INVENTORY_NO",
    search_consent = "SEARCH_CONSENT_I",
    search_other_weapon_description = "SEARCH_OTHER_WEAPON_DESCR",
    search_other_weapon = "SEARCH_OTHER_WEAPON_I",
    search_property = "SEARCH_PROPERTY_I",
    stolen_property = "STOLEN_PROPERTY_I",
    stolen_property_inventory_number = "STOLEN_PROPERTY_INVENTORY_NO",
    search_alcohol = "S_ALCOHOL_I",
    search_alcohol_inventory_number = "S_ALCOHOL_INVENTORY_NO",
    search_firearm_inventory_number = "S_FIREARM_INVENTORY_NO",
    search_other_decription = "S_OTHER_DESCR",
    search_other = "S_OTHER_I",
    search_other_inventory_number = "S_OTHER_INVENTORY_NO",
    search_other_weapon_inventory_number = "S_OTHER_WEAPON_INVENTORY_NO",
    search_para = "S_PARA_I",
    search_para_inventory_number = "S_PARA_INVENTORY_NO",
    search_stolen_property = "S_STOLEN_PROPERTY_I",
    search_stolen_property_inventory_number = "S_STOLEN_PROPERTY_INVENTORY_NO",
    vehicle_stopped = "VEHICLE_STOPPED_I",
    vehicle_year = "V_YEAR",
    proximity_to_crime = "PROXIMITY_TO_CRIME_I",
    body_camera = "BODY_CAMERA_I",
    car_camera = "CAR_CAMERA_I",
    information_refused = "INFORMATION_REFUSED_I",
    cannabis_inventory_number = "CANNABIS_INVENTORY_NO",
    cocaine_inventory_number = "COCAINE_INVENTORY_NO",
    heroin_inventory_number = "HEROIN_INVENTORY_NO",
    other_controlled_substance_inventory_number = "OTHER_CON_SUB_INVENTORY_NO",
    search_cannabis_inventory_number = "S_CANNABIS_INVENTORY_NO",
    search_cocaine_inventory_number = "S_COCAINE_INVENTORY_NO",
    search_heroin_inventory_number = "S_HEROIN_INVENTORY_NO",
    search_other_controlled_substance_inventory_number = "S_OTHER_CON_SUB_INVENTORY_NO",
    firearm_inventory_number = "FIREARM_INVENTORY_NO",
    alcohol = "ALCOHOL_I",
    alcohol_inventory_number = "ALCOHOL_INVENTORY_NO",
    enforcement_type = "ENFORCEMENT_TYPE_CD",
    cited_violations = "CITED_VIOLATIONS_CD",
    enforcement_id = "ENFORCEMENT_ID_NO"
  )
}

#' Scan csv with schema, wrangle, and create identifier
query <- function(x) {
  pl$
    scan_csv(
      x,
      dtypes = get_schema(),
      null_values = c("", " "),
      try_parse_dates = FALSE
    )$
    with_columns(
      pl$col("^.*_I$")$eq("Y")
    )$
    rename(
      alias()
    )$
    with_columns(
      pl$
        col("^.*dt$")$
        map_batches(
          \(x)
          pl$coalesce(
            x$str$to_datetime(
              format = "%d-%b-%Y %H:%M",
              time_zone = "UTC",
              #ambiguous = "earliest",
              strict = FALSE
            ),
            x$str$to_datetime(
              format = "%Y/%m/%d %H:%M:%S",
              time_zone = "UTC",
              #ambiguous = "earliest",
              strict = FALSE
            )
          )
        ),
      pl$
        col("^.*appointed$")$
        map_batches(
          \(x)
          x$str$to_date(
            format = "%Y/%m/%d",
            strict = FALSE
          )
        )
    )
}

#' Melt the officer information, pivot by role, and rejoin
melt <- function(q) {
  q$
    select(
      grep("\\.", names(alias()), invert = TRUE, value = TRUE)
    )$
    join(
      other =
        q$
        melt(
          id_vars = "uid_contact",
          value_vars = grep("\\.", names(alias()), value = TRUE)
        )$
        with_columns(
          pl$col("variable")$str$split_exact(by = ".", 1)
        )$
        unnest(
          "variable"
        )$
        collect()$
        pivot(
          index = c("uid_contact", "field_0"),
          columns = "field_1",
          values = "value",
          aggregate_function = pl$element()$first()
        )$
        rename(
          role = "field_0"
        )$
        lazy(),
      how = "left",
      on = "uid_contact"
    )
}

#' Wrapper to scan the data, apply schema, and wrangle
#' @export
build <- function(p646845) {
  melt(
    query(
      p646845
    )
  )
}
