'.__module__.'

#' Source of data
#' @export
source <- function() {
  box::use(hash[hash])

  hash(
    p058155 = 0,
    p596580 = c(2, 4)
  )
}

#' Define the schema
#' @export
get_schema <- function() {
  list(
    # p596580
    `FIRST_NME` = "character",
    `MIDDLE_INITIAL` = "character",
    `LAST_NME` = "character",
    `APPOINTED_DATE` = "character",
    `YOB` = "integer",
    `RACE_DESCR` = "character",
    `SEX_CODE_CD` = "character",
    `RESIGNATION_DATE` = "character",
    `CURR_TITLE` = "character",
    `CURR_DESCR` = "character",
    `TITLE_TO` = "character",
    `TO_DESCR` = "character",
    `TITLE_EFFECTIVE_DATE` = "character",
    `CHANGE_DATE` = "character",
    `CURR_UNIT_ASSIGNED` = "character",
    `CURR_UNIT_DETAIL` = "character",
    `ASSIGNED_EFFECTIVE_DATE` = "character",
    `ASSIGNED_END_DATE` = "character",
    `UNIT_ASSIGNED_TO` = "character",
    `UNIT_DETAILED_TO` = "character",
    `DETAIL_EFFECTIVE_DATE` = "character",
    `DETAIL_END_DATE` = "character",
    `CURR_STAR_NO` = "character",
    `CPD_STAR_NO` = "character",
    `STAR_NO_TO` = "character",
    `STAR_EFFECTIVE_DATE` = "character",
    `EFFECTIVE_DATE` = "character",
    `STAR_END_DATE` = "character",
    `END_DATE` = "character",
    `CPD_STAR_NO_TYPE` = "character",
    # p058155
    `First Name` = "character",
    `Last Name` = "character",
    `Middle Initital` = "character",
    `Gender` = "character",
    `Race` = "character",
    `D.O.B.` = "integer",
    `Age` = "integer",
    `Status I` = "character",
    `Appointed Date` = "character",
    `Employee Position Number` = "character",
    `Description` = "character",
    ` `  = "character",
    `Unit Description` = "character",
    `Resignation Date` = "character",
    `Star 1` = "character",
    `Star 2` = "character",
    `Star 3` = "character",
    `Star 4` = "character",
    `Star 5` = "character",
    `Star 6` = "character",
    `Star 7` = "character",
    `Star 8` = "character",
    `Star 9` = "character",
    `Star 10` = "character",
    `Star 11` = "character"
  )
}

#' Alias for column names
alias <- function(reference) {
  list(
    # p596580
    first_name = "FIRST_NME",
    middle_initial = "MIDDLE_INITIAL",
    last_name = "LAST_NME",
    appointed = "APPOINTED_DATE",
    yob = "YOB",
    race = "RACE_DESCR",
    gender = "SEX_CODE_CD",
    resigned = "RESIGNATION_DATE",
    title = "CURR_TITLE",
    description = "CURR_DESCR",
    title_next = "TITLE_TO",
    description_next = "TO_DESCR",
    title_effective = "TITLE_EFFECTIVE_DATE",
    title_start = "CHANGE_DATE",
    unit = "CURR_UNIT_ASSIGNED",
    unit_detail = "CURR_UNIT_DETAIL",
    unit_start = "ASSIGNED_EFFECTIVE_DATE",
    unit_end = "ASSIGNED_END_DATE",
    unit_next = "UNIT_ASSIGNED_TO",
    unit_next_detail = "UNIT_DETAILED_TO",
    unit_next_start = "DETAIL_EFFECTIVE_DATE",
    unit_next_end = "DETAIL_END_DATE",
    star = "CURR_STAR_NO",
    star = "CPD_STAR_NO",
    star_next = "STAR_NO_TO",
    star_start = "STAR_EFFECTIVE_DATE",
    star_start = "EFFECTIVE_DATE",
    star_end = "STAR_END_DATE",
    star_end = "END_DATE",
    type = "CPD_STAR_NO_TYPE",
    # p058155
    first_name = "First Name",
    last_name = "Last Name",
    middle_initial = "Middle Initital",
    gender = "Gender",
    race = "Race",
    yob = "D.O.B.",
    age = "Age",
    sworn = "Status I",
    appointed = "Appointed Date",
    position = "Employee Position Number",
    position_description = "Description",
    unit = " ",
    unit_description = "Unit Description",
    resigned = "Resignation Date",
    star1 = "Star 1",
    star2 = "Star 2",
    star3 = "Star 3",
    star4 = "Star 4",
    star5 = "Star 5",
    star6 = "Star 6",
    star7 = "Star 7",
    star8 = "Star 8",
    star9 = "Star 9",
    star10 = "Star 10",
    star11 = "Star 11"
  )
}

#' Read the data, apply schema, and wrangle
#' @export
build <- function(p058155, p596580_2, p596580_4) {

  pl$
    concat(
      # p058155
      pl$
        scan_csv(
          p058155,
          dtypes = get_schema(),
          try_parse_dates = FALSE
        )$
        rename(
          intersect(alias(), pl$scan_csv(p058155)$columns)
        )$
        melt(
          id_vars = grep(
            pattern = "star[0-9]{1}",
            x = names(intersect(alias(), pl$scan_csv(p058155)$columns)),
            value = TRUE,
            invert = TRUE
          ),
          variable_name = "star_index",
          value_name = "star"
        )$
        with_columns(
          pl$
            col("sworn")$
            is_in("Y"),
          pl$
            col(c("appointed", "resigned"))$
            str$to_date(format = "%m/%d/%y", strict = FALSE)
        )$
        drop("star_index"),
      # p596580
      pl$
        scan_csv(
          p596580_2,
          dtypes = get_schema(),
          try_parse_dates = FALSE
        )$
        rename(
          intersect(alias(), pl$scan_csv(p596580_2)$columns)
        )$
        with_columns(
          pl$
            col(c("appointed",
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
            pl$
            scan_csv(
              p596580_4,
              dtypes = get_schema(),
              try_parse_dates = FALSE
            )$
            rename(
              intersect(alias(), pl$scan_csv(p596580_4)$columns)
            )$
            with_columns(
              pl$
                col(c("appointed",
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

