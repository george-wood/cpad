options(box.path = box::file())

box::use(
  polars[pl],
  proc/uid
)

box::reload(uid)

#' Check
uid$
  join(
    db = "db/arrest.parquet"
  )



#' TODO create function in join module for below operation
arrest <-
  pl$
  scan_parquet("db/arrest.parquet")$
  join(
    other = key$officers(),
    on = key$key(),
    how = "left",
    join_nulls = TRUE
  )$
  collect()

arrest$null_count()

#' TODO run join operation on all imported data
pl$
  scan_parquet("db/arrest.parquet")$
  join(
    other = key$officers(),
    on = key$key(),
    how = "left",
    join_nulls = TRUE
  )$
  collect()$
  write_parquet("db/arrest_keyed.parquet")

