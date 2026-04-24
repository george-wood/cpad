'.__module__.'

box::use(
  polars[pl],
  hash[hash],
  proc/utility[rename_aliased, ls]
)

#' Source of data
#' @export
source <- function() {
  hash(
    p644845 = 0
  )
}

#' Path to data
path <- function() {
  list(
    p646845 = ls("isr")
  )
}

#' Define the schema
#' @export
get_schema <- function() {
  list(
    CONTACT_CARD_ID = pl$String,
    CONTACT_CARD_ID = pl$String,
    CONTACT_TYPE_DESCR = pl$String,
    CARD_NO = pl$String,
    CONTACT_DATE_TIME = pl$String,
    FO_LAST = pl$String,
    FO_MIDDLE_INITIAL = pl$String,
    FO_FIRST = pl$String,
    FO_STAR = pl$String,
    FO_APPOINTED_DT = pl$String,
    FO_SEX = pl$String,
    FO_RACE = pl$String,
    FO_BIRTH_YR = pl$Int32,
    SO_LAST = pl$String,
    SO_MIDDLE_INITIAL = pl$String,
    SO_FIRST = pl$String,
    SO_STAR = pl$String,
    SO_APPOINTED_DATE = pl$String,
    SO_SEX = pl$String,
    SO_RACE = pl$String,
    SO_BIRTH_YR = pl$Int32,
    SUPV_LAST = pl$String,
    SUPV_MIDDLE_INITIAL = pl$String,
    SUPV_FIRST = pl$String,
    SUPV_STAR = pl$String,
    SUPV_APPOINTED_DATE = pl$String,
    SUPV_SEX = pl$String,
    SUPV_RACE = pl$String,
    SUPV_BIRTH_YR = pl$Int32,
    CLOTHING_DESCR = pl$String,
    RD = pl$String,
    EVENT_NO = pl$String,
    EVENT_ASSIGNEED_BY_CD = pl$String,
    HOTSPOT_NO = pl$String,
    MISSION_NO = pl$String,
    CPD_UNIT_NO = pl$String,
    CREATED_BY_NAME = pl$String,
    CREATED_DATE = pl$String,
    MODIFIED_BY_NAME = pl$String,
    MODIFIED_DATE = pl$String,
    SUBMITTING_BEAT_CD = pl$String,
    SUBMITTING_UNIT = pl$String,
    STREET_NO = pl$String,
    STREET_DIRECTION_CD = pl$String,
    STREET_NME = pl$String,
    CITY = pl$String,
    STATE_CD = pl$String,
    ZIP_CD = pl$String,
    DISTRICT = pl$String,
    SECTOR = pl$String,
    BEAT = pl$String,
    AREA = pl$String,
    WARD = pl$String,
    LOCATION_CD = pl$String,
    AGE = pl$Int32,
    AGE_TO = pl$Int32,
    SEX_CODE_CD = pl$String,
    RACE = pl$String,
    HEIGHT = pl$String,
    WEIGHT = pl$String,
    BUILD_CODE_CD = pl$String,
    EYE_COLOR_CODE_CD = pl$String,
    HAIR_COLOR_CODE_CD = pl$String,
    HAIR_STYLE_CODE_CD = pl$String,
    COMPLEXION_CODE_CD = pl$String,
    FACIAL_HAIR_CD = pl$String,
    RES_DISTRICT = pl$String,
    RES_SECTOR = pl$String,
    RES_BEAT = pl$String,
    RES_AREA = pl$String,
    RES_WARD = pl$String,
    BUS_AREA = pl$String,
    BUS_BEAT = pl$String,
    BUS_DISTRICT = pl$String,
    BUS_SECTOR = pl$String,
    BUS_WARD = pl$String,
    GANG_NAME = pl$String,
    FACTION_NAME = pl$String,
    GANG_LOOKOUT_I = pl$String,
    GANG_NARCOTIC_RELATED_I = pl$String,
    GANG_OTHER = pl$String,
    GANG_OTHER_I = pl$String,
    GANG_SECURITY_I = pl$String,
    INTIMIDATION_I = pl$String,
    CLOTHING_DESCR_1 = pl$String,
    NAME_VERIFIED_I = pl$String,
    COMPLETION_I = pl$String,
    CONTACT_CARD_STATUS_CD = pl$String,
    MAKE_CD = pl$String,
    MAKE_DESCR = pl$String,
    STYLE_CD = pl$String,
    STYLE_DESCR = pl$String,
    TYPE_CD = pl$String,
    MODEL_DESCR = pl$String,
    COLOR_TOP = pl$String,
    COLOR_BOTTOM = pl$String,
    HANDCUFFED_I = pl$String,
    VEHICLE_INVOLVED_I = pl$String,
    DISPERSAL_TIME = pl$String,
    NUMBER_OF_PERSONS_DISPERSED = pl$String,
    SUSPECT_NARCOTIC_ACTIVITY_I = pl$String,
    ENFORCEMENT_ACTION_TAKEN_I = pl$String,
    INDICATIVE_CASING_I = pl$String,
    FITS_DESCRIPTION_OFFENDER_I = pl$String,
    OTHER_FACTOR_I = pl$String,
    PAT_DOWN_I = pl$String,
    PAT_DOWN_CONSENT_I = pl$String,
    PAT_DOWN_RECEIPT_GIVEN_I = pl$String,
    VERBAL_THREATS_I = pl$String,
    KNOWLEDGE_OF_PRIOR_I = pl$String,
    ACTIONS_INDICATIVE_VIOLENCE_I = pl$String,
    VIOLENT_CRIME_I = pl$String,
    SUSPICIOUS_OBJECT_I = pl$String,
    OTHER_REASONABLE_SUSPICION_I = pl$String,
    WEAPON_OR_CONTRABAND_FOUND_I = pl$String,
    FIREARM_I = pl$String,
    COCAINE_I = pl$String,
    COCAINE_AMOUNT = pl$String,
    HEROIN_I = pl$String,
    HEROIN_AMOUNT = pl$String,
    OTHER_CONTRABAND_I = pl$String,
    OTHER_CONTRABAND_DESCR = pl$String,
    OTHER_WEAPON_I = pl$String,
    OTHER_WEAPON_DESCR = pl$String,
    CANNABIS_I = pl$String,
    CANNABIS_AMOUNT = pl$String,
    OTHER_CON_SUB_I = pl$String,
    OTHER_CON_SUB = pl$String,
    OTHER_CON_SUB_AMT = pl$String,
    SEARCH_I = pl$String,
    SEARCH_CONTRABAND_FOUND_I = pl$String,
    SEARCH_FIREARM_I = pl$String,
    SEARCH_COCAINE_I = pl$String,
    SEARCH_COCAINE_AMOUNT = pl$String,
    SEARCH_HEROIN_I = pl$String,
    SEARCH_HEROIN_AMOUNT = pl$String,
    SEARCH_OTHER_CONTRABAND_I = pl$String,
    SEARCH_OTHER_CONTRABAND_DESCR = pl$String,
    SEARCH_CANNABIS_I = pl$String,
    SEARCH_CANNABIS_AMOUNT = pl$String,
    SEARCH_OTHER_CON_SUB_I = pl$String,
    SEARCH_OTHER_CON_SUB_DESCR = pl$String,
    SEARCH_OTHER_CON_SUB_AMT = pl$String,
    OTHER_DESCR = pl$String,
    OTHER_INVENTORY_NO = pl$String,
    OTHER_WEAPON_INVENTORY_NO = pl$String,
    PARA_I = pl$String,
    PARA_INVENTORY_NO = pl$String,
    SEARCH_CONSENT_I = pl$String,
    SEARCH_OTHER_WEAPON_DESCR = pl$String,
    SEARCH_OTHER_WEAPON_I = pl$String,
    SEARCH_PROPERTY_I = pl$String,
    STOLEN_PROPERTY_I = pl$String,
    STOLEN_PROPERTY_INVENTORY_NO = pl$String,
    S_ALCOHOL_I = pl$String,
    S_ALCOHOL_INVENTORY_NO = pl$String,
    S_FIREARM_INVENTORY_NO = pl$String,
    S_OTHER_DESCR = pl$String,
    S_OTHER_I = pl$String,
    S_OTHER_INVENTORY_NO = pl$String,
    S_OTHER_WEAPON_INVENTORY_NO = pl$String,
    S_PARA_I = pl$String,
    S_PARA_INVENTORY_NO = pl$String,
    S_STOLEN_PROPERTY_I = pl$String,
    S_STOLEN_PROPERTY_INVENTORY_NO = pl$String,
    VEHICLE_STOPPED_I = pl$String,
    V_YEAR = pl$Int32,
    PROXIMITY_TO_CRIME_I = pl$String,
    BODY_CAMERA_I = pl$String,
    CAR_CAMERA_I = pl$String,
    INFORMATION_REFUSED_I = pl$String,
    CANNABIS_INVENTORY_NO = pl$String,
    COCAINE_INVENTORY_NO = pl$String,
    HEROIN_INVENTORY_NO = pl$String,
    OTHER_CON_SUB_INVENTORY_NO = pl$String,
    S_CANNABIS_INVENTORY_NO = pl$String,
    S_COCAINE_INVENTORY_NO = pl$String,
    S_HEROIN_INVENTORY_NO = pl$String,
    S_OTHER_CON_SUB_INVENTORY_NO = pl$String,
    FIREARM_INVENTORY_NO = pl$String,
    ALCOHOL_I = pl$String,
    ALCOHOL_INVENTORY_NO = pl$String,
    ENFORCEMENT_TYPE_CD = pl$String,
    CITED_VIOLATIONS_CD = pl$String,
    ENFORCEMENT_ID_NO = pl$String
  )
}

#' Alias for column names
alias <- function() {
  list(
    CONTACT_CARD_ID = "uid_contact",
    CONTACT_TYPE_CD = "contact_type",
    CONTACT_TYPE_DESCR = "contact_type_description",
    CARD_NO = "card",
    CONTACT_DATE_TIME = "dt",
    FO_LAST = "first.last_name",
    FO_MIDDLE_INITIAL = "first.middle_initial",
    FO_FIRST = "first.first_name",
    FO_STAR = "first.star",
    FO_APPOINTED_DT = "first.appointed",
    FO_SEX = "first.gender",
    FO_RACE = "first.race",
    FO_BIRTH_YR = "first.yob",
    SO_LAST = "second.last_name",
    SO_MIDDLE_INITIAL = "second.middle_initial",
    SO_FIRST = "second.first_name",
    SO_STAR = "second.star",
    SO_APPOINTED_DATE = "second.appointed",
    SO_SEX = "second.gender",
    SO_RACE = "second.race",
    SO_BIRTH_YR = "second.yob",
    SUPV_LAST = "supervisor.last_name",
    SUPV_MIDDLE_INITIAL = "supervisor.middle_initial",
    SUPV_FIRST = "supervisor.first_name",
    SUPV_STAR = "supervisor.star",
    SUPV_APPOINTED_DATE = "supervisor.appointed",
    SUPV_SEX = "supervisor.gender",
    SUPV_RACE = "supervisor.race",
    SUPV_BIRTH_YR = "supervisor.yob",
    CLOTHING_DESCR = "civilian_clothing",
    RD = "rd",
    EVENT_NO = "event",
    EVENT_ASSIGNEED_BY_CD = "event_assigneed_by_cd",
    HOTSPOT_NO = "hotspot",
    MISSION_NO = "mission",
    CPD_UNIT_NO = "unit",
    CREATED_BY_NAME = "created_by",
    CREATED_DATE = "created_dt",
    MODIFIED_BY_NAME = "modified_by",
    MODIFIED_DATE = "modified_dt",
    SUBMITTING_BEAT_CD = "submitting_beat",
    SUBMITTING_UNIT = "submitting_unit",
    STREET_NO = "street_number",
    STREET_DIRECTION_CD = "street_direction",
    STREET_NME = "street",
    CITY = "city",
    STATE_CD = "state",
    ZIP_CD = "zip",
    DISTRICT = "district",
    SECTOR = "sector",
    BEAT = "beat",
    AREA = "area",
    WARD = "ward",
    LOCATION_CD = "location",
    AGE = "civilian_age",
    AGE_TO = "civilian_age_to",
    SEX_CODE_CD = "civilian_gender",
    RACE = "civilian_race",
    HEIGHT = "civilian_height",
    WEIGHT = "civilian_weight",
    BUILD_CODE_CD = "civilian_build",
    EYE_COLOR_CODE_CD = "civilian_eye_color",
    HAIR_COLOR_CODE_CD = "civilian_hair_color",
    HAIR_STYLE_CODE_CD = "civilian_hairstyle",
    COMPLEXION_CODE_CD = "civilian_complexion",
    FACIAL_HAIR_CD = "civilian_facial_hair",
    RES_DISTRICT = "residence_district",
    RES_SECTOR = "residence_sector",
    RES_BEAT = "residence_beat",
    RES_AREA = "residence_area",
    RES_WARD = "residence_ward",
    BUS_AREA = "bus_area",
    BUS_BEAT = "bus_beat",
    BUS_DISTRICT = "bus_district",
    BUS_SECTOR = "bus_sector",
    BUS_WARD = "bus_ward",
    GANG_NAME = "gang_name",
    FACTION_NAME = "faction_name",
    GANG_LOOKOUT_I = "gang_lookout",
    GANG_NARCOTIC_RELATED_I = "gang_narcotic_related",
    GANG_OTHER = "gang_other_name",
    GANG_OTHER_I = "gang_other",
    GANG_SECURITY_I = "gang_security",
    INTIMIDATION_I = "intimidation",
    CLOTHING_DESCR_1 = "civilian_clothing_description",
    NAME_VERIFIED_I = "name_verified",
    COMPLETION_I = "completion",
    CONTACT_CARD_STATUS_CD = "contact_card_status",
    MAKE_CD = "vehicle_make",
    MAKE_DESCR = "vehicle_description",
    STYLE_CD = "vehicle_style",
    STYLE_DESCR = "vehicle_style_description",
    TYPE_CD = "vehicle_type",
    MODEL_DESCR = "vehicle_model",
    COLOR_TOP = "vehicle_color_top",
    COLOR_BOTTOM = "vehicle_color_bottom",
    HANDCUFFED_I = "handcuffed",
    VEHICLE_INVOLVED_I = "vehicle_involved",
    DISPERSAL_TIME = "dispersal_time",
    NUMBER_OF_PERSONS_DISPERSED = "number_of_persons_dispersed",
    SUSPECT_NARCOTIC_ACTIVITY_I = "suspect_narcotic_activity",
    ENFORCEMENT_ACTION_TAKEN_I = "enforcement_action_taken",
    INDICATIVE_CASING_I = "indicative_casing",
    FITS_DESCRIPTION_OFFENDER_I = "fits_description_offender",
    OTHER_FACTOR_I = "other_factor",
    PAT_DOWN_I = "pat_down",
    PAT_DOWN_CONSENT_I = "pat_down_consent",
    PAT_DOWN_RECEIPT_GIVEN_I = "pat_down_receipt_given",
    VERBAL_THREATS_I = "verbal_threats",
    KNOWLEDGE_OF_PRIOR_I = "knowledge_of_prior",
    ACTIONS_INDICATIVE_VIOLENCE_I = "actions_indicative_violence",
    VIOLENT_CRIME_I = "violent_crime",
    SUSPICIOUS_OBJECT_I = "suspicious_object",
    OTHER_REASONABLE_SUSPICION_I = "other_reasonable_suspicion",
    WEAPON_OR_CONTRABAND_FOUND_I = "weapons_or_contraband_found",
    FIREARM_I = "firearm",
    COCAINE_I = "cocaine",
    COCAINE_AMOUNT = "cocaine_amount",
    HEROIN_I = "heroin",
    HEROIN_AMOUNT = "heroin_amount",
    OTHER_CONTRABAND_I = "other_contraband",
    OTHER_CONTRABAND_DESCR = "other_contraband_description",
    OTHER_WEAPON_I = "other_weapon",
    OTHER_WEAPON_DESCR = "other_weapon_description",
    CANNABIS_I = "cannabis",
    CANNABIS_AMOUNT = "cannabis_amount",
    OTHER_CON_SUB_I = "other_controlled_substance",
    OTHER_CON_SUB = "other_controlled_substance_description",
    OTHER_CON_SUB_AMT = "other_controlled_substance_amount",
    SEARCH_I = "search",
    SEARCH_CONTRABAND_FOUND_I = "search_contraband_found",
    SEARCH_FIREARM_I = "search_firearm",
    SEARCH_COCAINE_I = "search_cocaine",
    SEARCH_COCAINE_AMOUNT = "search_cocaine_amount",
    SEARCH_HEROIN_I = "search_heroin",
    SEARCH_HEROIN_AMOUNT = "search_heroin_amount",
    SEARCH_OTHER_CONTRABAND_I = "search_other_contraband",
    SEARCH_OTHER_CONTRABAND_DESCR = "search_other_contraband_description",
    SEARCH_CANNABIS_I = "search_cannabis",
    SEARCH_CANNABIS_AMOUNT = "search_cannabis_amount",
    SEARCH_OTHER_CON_SUB_I = "search_other_controlled_substance",
    SEARCH_OTHER_CON_SUB_DESCR = "search_other_controlled_substance_description",
    SEARCH_OTHER_CON_SUB_AMT = "search_other_controlled_substance_amount",
    OTHER_DESCR = "other_description",
    OTHER_INVENTORY_NO = "other_inventory_number",
    OTHER_WEAPON_INVENTORY_NO = "other_weapon_inventory_number",
    PARA_I = "para",
    PARA_INVENTORY_NO = "para_inventory_number",
    SEARCH_CONSENT_I = "search_consent",
    SEARCH_OTHER_WEAPON_DESCR = "search_other_weapon_description",
    SEARCH_OTHER_WEAPON_I = "search_other_weapon",
    SEARCH_PROPERTY_I = "search_property",
    STOLEN_PROPERTY_I = "stolen_property",
    STOLEN_PROPERTY_INVENTORY_NO = "stolen_property_inventory_number",
    S_ALCOHOL_I = "search_alcohol",
    S_ALCOHOL_INVENTORY_NO = "search_alcohol_inventory_number",
    S_FIREARM_INVENTORY_NO = "search_firearm_inventory_number",
    S_OTHER_DESCR = "search_other_decription",
    S_OTHER_I = "search_other",
    S_OTHER_INVENTORY_NO = "search_other_inventory_number",
    S_OTHER_WEAPON_INVENTORY_NO = "search_other_weapon_inventory_number",
    S_PARA_I = "search_para",
    S_PARA_INVENTORY_NO = "search_para_inventory_number",
    S_STOLEN_PROPERTY_I = "search_stolen_property",
    S_STOLEN_PROPERTY_INVENTORY_NO = "search_stolen_property_inventory_number",
    VEHICLE_STOPPED_I = "vehicle_stopped",
    V_YEAR = "vehicle_year",
    PROXIMITY_TO_CRIME_I = "proximity_to_crime",
    BODY_CAMERA_I = "body_camera",
    CAR_CAMERA_I = "car_camera",
    INFORMATION_REFUSED_I = "information_refused",
    CANNABIS_INVENTORY_NO = "cannabis_inventory_number",
    COCAINE_INVENTORY_NO = "cocaine_inventory_number",
    HEROIN_INVENTORY_NO = "heroin_inventory_number",
    OTHER_CON_SUB_INVENTORY_NO = "other_controlled_substance_inventory_number",
    S_CANNABIS_INVENTORY_NO = "search_cannabis_inventory_number",
    S_COCAINE_INVENTORY_NO = "search_cocaine_inventory_number",
    S_HEROIN_INVENTORY_NO = "search_heroin_inventory_number",
    S_OTHER_CON_SUB_INVENTORY_NO = "search_other_controlled_substance_inventory_number",
    FIREARM_INVENTORY_NO = "firearm_inventory_number",
    ALCOHOL_I = "alcohol",
    ALCOHOL_INVENTORY_NO = "alcohol_inventory_number",
    ENFORCEMENT_TYPE_CD = "enforcement_type",
    CITED_VIOLATIONS_CD = "cited_violations",
    ENFORCEMENT_ID_NO = "enforcement_id"
  )
}

#' Scan csv with schema, wrangle, and create identifier
#' @export
query <- function() {
  lf <- pl$scan_csv(
    path()$p646845,
    schema_overrides = get_schema(),
    null_values = c("", " "),
    try_parse_dates = FALSE
  )$
    with_columns(
      pl$col("^.*_I$")$eq("Y")
    )

  rename_aliased(lf, alias())$
    with_columns(
      pl$
        col("dt")$
        str$to_datetime(
          format = "%d-%b-%Y %H:%M",
          strict = FALSE
        ),
      pl$
        col("^.*_dt$")$
        str$to_datetime(
          format = "%Y/%m/%d %H:%M:%S",
          strict = FALSE
        )
    )
}

#' Melt the officer information, pivot by role, and rejoin
#' @export
melt <- function(q) {
  new_names <- unname(unlist(alias()))
  role_cols <- grep("\\.", new_names, value = TRUE)
  base_cols <- grep("\\.", new_names, invert = TRUE, value = TRUE)

  q$
    select(
      base_cols
    )$
    join(
      other =
        q$
        unpivot(
          index = "uid_contact",
          on = role_cols
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
          on = "field_1",
          values = "value",
          aggregate_function = pl$element()$first()
        )$
        rename(
          field_0 = "role"
        )$
        with_columns(
          pl$
            col("^.*appointed$")$
            str$to_date(
              format = "%Y/%m/%d",
              strict = FALSE
            ),
          pl$
            col("yob")$
            cast(pl$Int32)
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
    query()
  )$
    filter(
      pl$col("dt")$is_not_null()
    )
}
