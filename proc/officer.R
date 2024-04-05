'.__module__.'

box::use(
  polars[pl],
  fs[dir_ls]
)

#' Key columns
#' @export
key <- function() {
  c("first_name",
    "middle_initial",
    "last_name",
    "appointed",
    "yob")
}

#' Locate data frames with all key columns
locate_key <- function() {
  sapply(
    dir_ls("db"),
    function(x)
      all(key() %in% pl$scan_parquet(x)$columns)
  )
}

#' Concatenate located data frames and return officers with UID
#' @export
generate_key <- function() {
  pl$
    concat(
      lapply(
        dir_ls("db")[locate_key()],
        \(x)
        pl$
          scan_parquet(x)$
          select(key())$
          unique()
      ),
      how = "vertical"
    )$
    drop_nulls(
      subset = setdiff(key(), c("middle_initial", "yob"))
    )$
    # fill null yob for otherwise distinct keys
    with_columns(
      pl$
        col("yob")$
        forward_fill()$
        backward_fill()$
        over(setdiff(key(), c("middle_initial", "yob")))
    )$
    # store all middle initials for otherwise distinct keys
    group_by(
      setdiff(key(), "middle_initial")
    )$
    agg(
      pl$col("middle_initial")$unique()
    )$
    unique(
      setdiff(key(), "middle_initial")
    )$
    sort(
      "appointed",
      "yob"
    )$
    with_row_index(
      "uid"
    )$
    with_columns(
      pl$col("uid")$cast(pl$Utf8)$str$zfill(5),
      pl$col("appointed")$dt$year()$cast(pl$Int32)$alias("appointed_year")
    )$
    explode(
      "middle_initial"
    )
}

#' Example without non-null yob and middle_initial steps
#' Three individuals (probably) are allocated five distinct UID
# ┌───────┬────────────┬────────────────┬────────────┬────────────┬──────┐
# │ uid   ┆ first_name ┆ middle_initial ┆ last_name  ┆ appointed  ┆ yob  │
# │ ---   ┆ ---        ┆ ---            ┆ ---        ┆ ---        ┆ ---  │
# │ str   ┆ str        ┆ str            ┆ str        ┆ str        ┆ i32  │
# ╞═══════╪════════════╪════════════════╪════════════╪════════════╪══════╡
# │ 25853 ┆ WILLIAM    ┆ null           ┆ BETANCOURT ┆ 1996-05-06 ┆ null │
# │ 25931 ┆ WILLIAM    ┆ null           ┆ BETANCOURT ┆ 1996-05-06 ┆ 1965 │
# │ 28929 ┆ HERBERT    ┆ null           ┆ BETANCOURT ┆ 1998-10-26 ┆ null │
# │ 28974 ┆ HERBERT    ┆ I              ┆ BETANCOURT ┆ 1998-10-26 ┆ 1969 │
# │ 48554 ┆ LYNETTE    ┆ E              ┆ BETANCOURT ┆ 2018-11-27 ┆ 1995 │
# └───────┴────────────┴────────────────┴────────────┴────────────┴──────┘


#' Example of year of birth ambiguity
# ┌────────────┬────────────────┬───────────┬────────────┬──────┐
# │ first_name ┆ middle_initial ┆ last_name ┆ appointed  ┆ yob  │
# │ ---        ┆ ---            ┆ ---       ┆ ---        ┆ ---  │
# │ str        ┆ str            ┆ str       ┆ date       ┆ i32  │
# ╞════════════╪════════════════╪═══════════╪════════════╪══════╡
# │ KEVIN      ┆ M              ┆ CONNORS   ┆ 1999-09-13 ┆ 1975 │
# │ KEVIN      ┆ M              ┆ CONNORS   ┆ 1999-09-13 ┆ 1967 │
# │ KEVIN      ┆ M              ┆ CONNORS   ┆ 1999-09-13 ┆ 1967 │
# │ KEVIN      ┆ M              ┆ CONNORS   ┆ 1999-09-13 ┆ 1975 │
# └────────────┴────────────────┴───────────┴────────────┴──────┘
