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
    p058155 = 0,
    p596580 = c(2, 4)
  )
}

#' Path to data
path <- function() {
  list(
    p058155   = ls("roster/", reg = "p058155_0"),
    p596580_4 = ls("roster/", reg = "p596580_4"),
    p596580_2 = ls("roster/", reg = "p596580_2")
  )
}

#' Define the schema
#' @export
get_schema <- function() {
  list(
    # p596580
    FIRST_NME = pl$String,
    MIDDLE_INITIAL = pl$String,
    LAST_NME = pl$String,
    APPOINTED_DATE = pl$String,
    YOB = pl$Int32,
    RACE_DESCR = pl$String,
    SEX_CODE_CD = pl$String,
    RESIGNATION_DATE = pl$String,
    CURR_TITLE = pl$String,
    CURR_DESCR = pl$String,
    TITLE_TO = pl$String,
    TO_DESCR = pl$String,
    TITLE_EFFECTIVE_DATE = pl$String,
    CHANGE_DATE = pl$String,
    CURR_UNIT_ASSIGNED = pl$String,
    CURR_UNIT_DETAIL = pl$String,
    ASSIGNED_EFFECTIVE_DATE = pl$String,
    ASSIGNED_END_DATE = pl$String,
    UNIT_ASSIGNED_TO = pl$String,
    UNIT_DETAILED_TO = pl$String,
    DETAIL_EFFECTIVE_DATE = pl$String,
    DETAIL_END_DATE = pl$String,
    CURR_STAR_NO = pl$String,
    CPD_STAR_NO = pl$String,
    STAR_NO_TO = pl$String,
    STAR_EFFECTIVE_DATE = pl$String,
    EFFECTIVE_DATE = pl$String,
    STAR_END_DATE = pl$String,
    END_DATE = pl$String,
    CPD_STAR_NO_TYPE = pl$String,
    # p058155
    `First Name` = pl$String,
    `Last Name` = pl$String,
    `Middle Initital` = pl$String,
    `Gender` = pl$String,
    `Race` = pl$String,
    `D.O.B.` = pl$Int32,
    `Age` = pl$Int32,
    `Status I` = pl$String,
    `Appointed Date` = pl$String,
    `Employee Position Number` = pl$String,
    `Description` = pl$String,
    ` ` = pl$String,
    `Unit Description` = pl$String,
    `Resignation Date` = pl$String,
    `Star 1` = pl$String,
    `Star 2` = pl$String,
    `Star 3` = pl$String,
    `Star 4` = pl$String,
    `Star 5` = pl$String,
    `Star 6` = pl$String,
    `Star 7` = pl$String,
    `Star 8` = pl$String,
    `Star 9` = pl$String,
    `Star 10` = pl$String,
    `Star 11` = pl$String
  )
}

#' Alias for column names
alias <- function() {
  list(
    # p596580
    FIRST_NME = "first_name",
    MIDDLE_INITIAL = "middle_initial",
    LAST_NME = "last_name",
    APPOINTED_DATE = "appointed",
    YOB = "yob",
    RACE_DESCR = "race",
    SEX_CODE_CD = "gender",
    RESIGNATION_DATE = "resigned",
    CURR_TITLE = "title",
    CURR_DESCR = "description",
    TITLE_TO = "title_next",
    TO_DESCR = "description_next",
    TITLE_EFFECTIVE_DATE = "title_effective",
    CHANGE_DATE = "title_start",
    CURR_UNIT_ASSIGNED = "unit",
    CURR_UNIT_DETAIL = "unit_detail",
    ASSIGNED_EFFECTIVE_DATE = "unit_start",
    ASSIGNED_END_DATE = "unit_end",
    UNIT_ASSIGNED_TO = "unit_next",
    UNIT_DETAILED_TO = "unit_next_detail",
    DETAIL_EFFECTIVE_DATE = "unit_next_start",
    DETAIL_END_DATE = "unit_next_end",
    CURR_STAR_NO = "star",
    CPD_STAR_NO = "star",
    STAR_NO_TO = "star_next",
    STAR_EFFECTIVE_DATE = "star_start",
    EFFECTIVE_DATE = "star_start",
    STAR_END_DATE = "star_end",
    END_DATE = "star_end",
    CPD_STAR_NO_TYPE = "type",
    # p058155
    `First Name` = "first_name",
    `Last Name` = "last_name",
    `Middle Initital` = "middle_initial",
    Gender = "gender",
    Race = "race",
    `D.O.B.` = "yob",
    Age = "age",
    `Status I` = "sworn",
    `Appointed Date` = "appointed",
    `Employee Position Number` = "position",
    Description = "position_description",
    ` ` = "unit",
    `Unit Description` = "unit_description",
    `Resignation Date` = "resigned",
    `Star 1` = "star1",
    `Star 2` = "star2",
    `Star 3` = "star3",
    `Star 4` = "star4",
    `Star 5` = "star5",
    `Star 6` = "star6",
    `Star 7` = "star7",
    `Star 8` = "star8",
    `Star 9` = "star9",
    `Star 10` = "star10",
    `Star 11` = "star11"
  )
}

#' Read the data, apply schema, and wrangle
#' @export
build <- function() {

  lf_058155 <- scan_aliased(path()$p058155, get_schema(), alias())
  non_star <- grep("^star[0-9]+$", names(lf_058155), invert = TRUE, value = TRUE)

  pl$
    concat(
      lf_058155$
        unpivot(
          index = non_star,
          variable_name = "star_index",
          value_name = "star"
        )$
        with_columns(
          pl$
            col("sworn")$
            eq("Y"),
          pl$
            col(!!!c("appointed", "resigned"))$
            str$to_date(format = "%m/%d/%y", strict = FALSE)
        )$
        drop("star_index"),
      scan_aliased(path()$p596580_2, get_schema(), alias())$
        with_columns(
          pl$
            col(!!!c("appointed",
                     "resigned",
                     "title_effective",
                     "title_start",
                     "unit_start",
                     "unit_end",
                     "unit_next_start",
                     "unit_next_end",
                     "star_start",
                     "star_end"))$
            str$to_date(format = "%Y/%m/%d", strict = FALSE)
        )$
        join(
          other =
            scan_aliased(path()$p596580_4, get_schema(), alias())$
              with_columns(
                pl$
                  col(!!!c("appointed",
                           "star_start",
                           "star_end"))$
                  str$to_date(format = "%Y/%m/%d", strict = FALSE)
              ),
          how = "left",
          on = c("first_name",
                 "middle_initial",
                 "last_name",
                 "appointed",
                 "yob",
                 "race",
                 "gender")
        ),
      how = "diagonal"
    )$
    unique()
}
