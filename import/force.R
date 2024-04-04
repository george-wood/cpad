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

#' Path to data
path <- function() {
  box::use(../proc/utility[ls])

  list(
    report = ls("trr/", reg = "_0|p583646_1"),
    action = ls("trr/", reg = "_2")
  )
}

#' Define the schema
#' @export
get_schema <- function() {
  list(
    # report
    TRR_REPORT_ID = "character",
    RD_NO = "character",
    CR_NO_OBTAINED = "character",
    SUBJECT_CB_NO = "character",
    SUBJECT_IR_NO = "character",
    EVENT_NO = "character",
    BEAT = "character",
    BLK = "character",
    DIR = "character",
    STN = "character",
    LOC = "character",
    DATETIME = "character",
    DTE = "character",
    TMEMIL = "character",
    INDOOR_OR_OUTDOOR = "character",
    LIGHTING_CONDITION = "character",
    WEATHER_CONDITION = "character",
    NOTIFY_OEMC = "character",
    NOTIFY_DIST_SERGEANT = "character",
    NOTIFY_OP_COMMAND = "character",
    NOTIFY_DET_DIV = "character",
    NUMBER_OF_WEAPONS_DISCHARGED = "character",
    PARTY_FIRED_FIRST = "character",
    POLAST = "character",
    POFIRST = "character",
    POMI = "character",
    POGNDR = "character",
    PORACE = "character",
    POYRofBIRTH = "integer",
    POAGE = "integer",
    APPOINTED_DATE = "character",
    UNITASSG = "character",
    UNITDETAIL = "character",
    ASSGNBEAT = "character",
    RANK = "character",
    DUTYSTATUS = "character",
    POINJURED = "character",
    MEMBER_IN_UNIFORM = "character",
    CURRRANK = "character",
    CURRUNIT = "character",
    CURRBEATASSG = "character",
    SUBGNDR = "character",
    SUBRACE = "character",
    SUBYEARDOB = "integer",
    SUBAGE = "integer",
    SUBJECT_ARMED = "character",
    SUBJECT_INJURED = "character",
    SUBJECT_ALLEGED_INJURY = "character",
    TRR_CREATED = "character",
    POLICYCOMPLIANCE = "character",
    # action
    PERSON = "character",
    `Resistance Type` = "character",
    ACTION = "character",
    OTHERDESCR = "character"
  )
}

#' Alias for column names
alias <- function() {
  list(
    # report
    uid_force = "TRR_REPORT_ID",
    rd = "RD_NO",
    cr = "CR_NO_OBTAINED",
    cb = "SUBJECT_CB_NO",
    ir = "SUBJECT_IR_NO",
    event = "EVENT_NO",
    beat = "BEAT",
    block = "BLK",
    street_direction = "DIR",
    street = "STN",
    location = "LOC",
    dt = "DATETIME",
    date = "DTE",
    time = "TMEMIL",
    outdoor = "INDOOR_OR_OUTDOOR",
    lighting_condition = "LIGHTING_CONDITION",
    weather_condition = "WEATHER_CONDITION",
    notify_oemc = "NOTIFY_OEMC",
    notify_district_sergeant = "NOTIFY_DIST_SERGEANT",
    notify_operational_command = "NOTIFY_OP_COMMAND",
    notify_detective_division = "NOTIFY_DET_DIV",
    weapons_discharged = "NUMBER_OF_WEAPONS_DISCHARGED",
    party_fired_first = "PARTY_FIRED_FIRST",
    last_name = "POLAST",
    first_name = "POFIRST",
    middle_initial = "POMI",
    gender = "POGNDR",
    race = "PORACE",
    yob = "POYRofBIRTH",
    age = "POAGE",
    appointed = "APPOINTED_DATE",
    assigned_unit = "UNITASSG",
    assigned_unit_detail = "UNITDETAIL",
    assigned_beat = "ASSGNBEAT",
    rank = "RANK",
    duty_status = "DUTYSTATUS",
    officer_injured = "POINJURED",
    in_uniform = "MEMBER_IN_UNIFORM",
    current_rank = "CURRRANK",
    current_unit = "CURRUNIT",
    current_beat = "CURRBEATASSG",
    civilian_gender = "SUBGNDR",
    civilian_race = "SUBRACE",
    civilian_yob = "SUBYEARDOB",
    civilian_age = "SUBAGE",
    civilian_armed = "SUBJECT_ARMED",
    civilian_injured = "SUBJECT_INJURED",
    civilian_alleged_injury = "SUBJECT_ALLEGED_INJURY",
    report_created = "TRR_CREATED",
    policy_compliance = "POLICYCOMPLIANCE",
    # action
    person = "PERSON",
    resistance_type = "Resistance Type",
    action = "ACTION",
    description = "OTHERDESCR"
  )
}

#' Read the data, apply schema, and write dataset
#' @export
build <- function() {
  box::use(polars[pl])

  pl$concat(
    lapply(
      path()$report,
      \(x)
      pl$
        scan_csv(
          x,
          dtypes = get_schema(),
          try_parse_dates = FALSE
        )$
        rename(
          intersect(alias(), pl$scan_csv(x)$columns)
        )
    ),
    how = "diagonal"
  )$
    with_columns(
      pl$
        col(
          c("notify_oemc",
            "notify_district_sergeant",
            "notify_operational_command",
            "notify_detective_division",
            "duty_status",
            "officer_injured",
            "in_uniform",
            "civilian_armed",
            "civilian_injured",
            "civilian_alleged_injury")
        )$
        is_in(
          c("Y", "True", "TRUE")
        ),
      # dates
      pl$coalesce(
        pl$col("appointed")$str$to_date(format = "%Y-%m-%d", strict = FALSE),
        pl$col("appointed")$str$to_date(format = "%Y/%m/%d", strict = FALSE),
        pl$col("appointed")$str$to_date(format = "%Y-%b-%d", strict = FALSE)
      ),
      pl$coalesce(
        pl$
          col("dt")$
          str$to_datetime(
            format = "%Y-%b-%d %H%M",
            strict = FALSE
          ),
        pl$
          concat_str(pl$col("time")$str$zfill(4), pl$col("date"))$
          str$to_datetime(
            format = "%H%M%Y-%m-%d",
            strict = FALSE
          ),
        pl$
          concat_str(pl$col("time")$str$zfill(4), pl$col("date"))$
          str$to_datetime(
            format = "%H%M%Y/%m/%d",
            strict = FALSE
          )
      ),
      pl$col("report_created")$
        str$to_datetime(format = "%Y-%m-%dT%H:%M:%S", strict = FALSE)
    )$
    with_columns(
      pl$
        when(
          pl$col("yob")$is_null()
        )$
        then(
          pl$
            col("dt")$dt$year()$
            sub(pl$col("age")$cast(pl$Int32))$
            sub(1)$
            cast(pl$Int32)
        )$
        otherwise(
          pl$col("yob")
        )$
        alias("yob_lower")
    )$
    drop(
      c("date", "time")
    )$
    unique()$
    join(
      other =
        pl$concat(
          lapply(
            path()$action,
            \(x)
            pl$
              scan_csv(
                x,
                dtypes = get_schema(),
                try_parse_dates = FALSE
              )$
              rename(
                intersect(alias(), pl$scan_csv(x)$columns)
              )
          ),
          how = "diagonal"
        )$
        unique(),
      on = "uid_force",
      how = "left"
    )
}
