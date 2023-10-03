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

#' Define key
#' @export
key <- function() {
  c("first_name",
    "middle_initial",
    "last_name",
    "appointed",
    "yob",
    "race",
    "gender")
}

#' Define the schema
#' @export
get_schema <- function(reference = "p596580", sheet) {

  box::use(
    arrow[schema, string],
    cli[cli_abort]
  )

  schema_A <- schema(
    last_name = string(),
    first_name = string(),
    middle_initial = string(),
    gender = string(),
    race = string(),
    yob = double(),
    age = string(),
    sworn = string(),
    appointed = string(),
    position = string(),
    position_description = string(),
    unit = string(),
    unit_description = string(),
    resigned = string(),
    star1 = string(),
    star2 = string(),
    star3 = string(),
    star4 = string(),
    star5 = string(),
    star6 = string(),
    star7 = string(),
    star8 = string(),
    star9 = string(),
    star10 = string(),
    star11 = string()
  )

  schema_B <- schema(
    first_name = string(),
    middle_initial = string(),
    last_name = string(),
    appointed = string(),
    yob = double(),
    race = string(),
    gender = string(),
    resigned = string(),
    title = string(),
    description = string(),
    title_next = string(),
    description_next = string(),
    title_effective = string(),
    title_start = string(),
    unit = string(),
    unit_detail = string(),
    unit_next = string(),
    unit_start = string(),
    unit_end = string(),
    unit_next_detail = string(),
    unit_next_detail_start = string(),
    unit_next_detail_end = string(),
    star = string(),
    star_next = string(),
    star_start = string(),
    star_end = string()
  )

  schema_C <- schema(
    first_name = string(),
    middle_initial = string(),
    last_name = string(),
    appointed = string(),
    yob = double(),
    race = string(),
    gender = string(),
    star = string(),
    star_start = string(),
    star_end = string(),
    type = string()
  )

  schm <- list(
    p058155_0 = schema_A,
    p596580_2 = schema_B,
    p596580_4 = schema_C
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

#' Read the data, apply schema, and wrangle
#' @export
build <- function(p058155, p596580_4, p596580_2) {

  box::use(
    arrow[read_csv_arrow],
    dplyr[across, bind_rows, distinct, filter, left_join,
          mutate, select, starts_with],
    polars[pl],
    tidyr[pivot_longer],
    rlang[syms],
    proc/utility[get_reference, get_sheet, parse_dt]
  )

  l <-
    lapply(
      list("p058155"   = p058155,
           "p596580_2" = p596580_2,
           "p596580_4" = p596580_4),
      function(x)
        read_csv_arrow(
          file = x,
          schema = get_schema(
            reference = get_reference(x),
            sheet = get_sheet(x)
          ),
          na = c("", " "),
          skip = 1
        )
    )

  p058155 <-
    l[["p058155"]] |>
    pivot_longer(
      cols = starts_with("star"),
      names_to = NULL,
      values_to = "star"
    ) |>
    mutate(
      across(
        c(appointed,
          resigned),
        function(x)
          parse_dt(x, format = c("%Y/%m/%d")))
    )

  p596580_4 <-
    l[["p596580_4"]] |>
    mutate(
      across(
        c(appointed,
          star_start,
          star_end),
        function(x)
          parse_dt(x, format = "%Y/%m/%d")
      )
    )

  p596580_2 <-
    l[["p596580_2"]] |>
    select(
      !!!syms(key()),
      resigned
    ) |>
    mutate(
      across(
        c(appointed,
          resigned),
        function(x)
          parse_dt(x, format = "%Y/%m/%d")
      )
    ) |>
    filter(
      !is.na(resigned)
    )

  left_join(
    p596580_2,
    p596580_4,
    by = key(),
    relationship = "many-to-many"
  ) |>
    bind_rows(
      p058155
    ) |>
    distinct(
      !!!syms(key()),
      resigned,
      star
    ) |>
    pl$DataFrame()

}
