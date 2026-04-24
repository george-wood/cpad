'.__module__.'

box::use(
  polars[pl],
  hash[hash],
  proc/utility[scan_aliased, ls]
)

#' Source of data
#' @export
source <- function() {
  hash(
    p046360 = c(0, 2),
    p456008 = c(0, 2),
    p461899 = c(0, 2),
    p583646 = c(1, 2)
  )
}

#' Path to data
path <- function() {
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
    TRR_REPORT_ID = pl$String,
    RD_NO = pl$String,
    CR_NO_OBTAINED = pl$String,
    SUBJECT_CB_NO = pl$String,
    SUBJECT_IR_NO = pl$String,
    EVENT_NO = pl$String,
    BEAT = pl$String,
    BLK = pl$String,
    DIR = pl$String,
    STN = pl$String,
    LOC = pl$String,
    DATETIME = pl$String,
    DTE = pl$String,
    TMEMIL = pl$String,
    INDOOR_OR_OUTDOOR = pl$String,
    LIGHTING_CONDITION = pl$String,
    WEATHER_CONDITION = pl$String,
    NOTIFY_OEMC = pl$String,
    NOTIFY_DIST_SERGEANT = pl$String,
    NOTIFY_OP_COMMAND = pl$String,
    NOTIFY_DET_DIV = pl$String,
    NUMBER_OF_WEAPONS_DISCHARGED = pl$Int32,
    PARTY_FIRED_FIRST = pl$String,
    POLAST = pl$String,
    POFIRST = pl$String,
    POMI = pl$String,
    POGNDR = pl$String,
    PORACE = pl$String,
    POYRofBIRTH = pl$Int32,
    POAGE = pl$Int32,
    APPOINTED_DATE = pl$String,
    UNITASSG = pl$String,
    UNITDETAIL = pl$String,
    ASSGNBEAT = pl$String,
    RANK = pl$String,
    DUTYSTATUS = pl$String,
    POINJURED = pl$String,
    MEMBER_IN_UNIFORM = pl$String,
    CURRRANK = pl$String,
    CURRUNIT = pl$String,
    CURRBEATASSG = pl$String,
    SUBGNDR = pl$String,
    SUBRACE = pl$String,
    SUBYEARDOB = pl$Int32,
    SUBAGE = pl$Int32,
    SUBJECT_ARMED = pl$String,
    SUBJECT_INJURED = pl$String,
    SUBJECT_ALLEGED_INJURY = pl$String,
    TRR_CREATED = pl$String,
    POLICYCOMPLIANCE = pl$String,
    # action
    PERSON = pl$String,
    `Resistance Type` = pl$String,
    ACTION = pl$String,
    OTHERDESCR = pl$String
  )
}

#' Alias for column names
alias <- function() {
  list(
    # report
    TRR_REPORT_ID = "uid_force",
    RD_NO = "rd",
    CR_NO_OBTAINED = "cr",
    SUBJECT_CB_NO = "cb",
    SUBJECT_IR_NO = "ir",
    EVENT_NO = "event",
    BEAT = "beat",
    BLK = "block",
    DIR = "street_direction",
    STN = "street",
    LOC = "location",
    DATETIME = "dt",
    DTE = "date",
    TMEMIL = "time",
    INDOOR_OR_OUTDOOR = "outdoor",
    LIGHTING_CONDITION = "lighting_condition",
    WEATHER_CONDITION = "weather_condition",
    NOTIFY_OEMC = "notify_oemc",
    NOTIFY_DIST_SERGEANT = "notify_district_sergeant",
    NOTIFY_OP_COMMAND = "notify_operational_command",
    NOTIFY_DET_DIV = "notify_detective_division",
    NUMBER_OF_WEAPONS_DISCHARGED = "weapons_discharged",
    PARTY_FIRED_FIRST = "party_fired_first",
    POLAST = "last_name",
    POFIRST = "first_name",
    POMI = "middle_initial",
    POGNDR = "gender",
    PORACE = "race",
    POYRofBIRTH = "yob",
    POAGE = "age",
    APPOINTED_DATE = "appointed",
    UNITASSG = "assigned_unit",
    UNITDETAIL = "assigned_unit_detail",
    ASSGNBEAT = "assigned_beat",
    RANK = "rank",
    DUTYSTATUS = "duty_status",
    POINJURED = "officer_injured",
    MEMBER_IN_UNIFORM = "in_uniform",
    CURRRANK = "current_rank",
    CURRUNIT = "current_unit",
    CURRBEATASSG = "current_beat",
    SUBGNDR = "civilian_gender",
    SUBRACE = "civilian_race",
    SUBYEARDOB = "civilian_yob",
    SUBAGE = "civilian_age",
    SUBJECT_ARMED = "civilian_armed",
    SUBJECT_INJURED = "civilian_injured",
    SUBJECT_ALLEGED_INJURY = "civilian_alleged_injury",
    TRR_CREATED = "report_created",
    POLICYCOMPLIANCE = "policy_compliance",
    # action
    PERSON = "person",
    `Resistance Type` = "resistance_type",
    ACTION = "action",
    OTHERDESCR = "description"
  )
}

#' Read the data, apply schema, and write dataset
#' @export
build <- function() {
  pl$concat(
    !!!lapply(
      path()$report,
      \(x) scan_aliased(x, get_schema(), alias())
    ),
    how = "diagonal"
  )$
    with_columns(
      pl$
        col(
          !!!c("notify_oemc",
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
          pl$lit(c("Y", "True", "TRUE"))$implode()
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
          !!!lapply(
            path()$action,
            \(x) scan_aliased(x, get_schema(), alias())
          ),
          how = "diagonal"
        )$
        unique(),
      on = "uid_force",
      how = "left"
    )$
    filter(
      pl$col("dt")$is_not_null()
    )
}
