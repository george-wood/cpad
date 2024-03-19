options(box.path = box::file())

box::use(
  polars[pl],
  proc/key
)

box::reload(key)

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

z <-
  arrest$
  filter(
    pl$col("last_name")$eq("RUIZ") &
      pl$col("first_name")$eq("ROLANDO")
  )$
  select(
    key$key(),
    "uid"
  )$
  unique()


xx <- pl$DataFrame(
  first_name = "ROLANDO",
  middle_initial = NA_character_,
  last_name = "RUIZ"
)

yy <- pl$DataFrame(
  first_name = "ROLANDO",
  middle_initial = NA_character_,
  last_name = "RUIZ",
  uid = "1"
)

xx
yy

xx$join(yy,
        on = c("first_name", "middle_initial", "last_name"),
        how = "inner",
        join_nulls = TRUE)

dplyr::left_join(xx$to_data_frame(), yy$to_data_frame(),
                 by = c("first_name", "middle_initial", "last_name"))

the_key <-
  key$
  officers()$
  filter(
    pl$col("last_name")$eq("RUIZ") &
      pl$col("first_name")$eq("ROLANDO")
  )$
  collect()$
  to_data_frame()


z$to_data_frame()
the_key

z$join(other = key$officers()$collect(), on = key$key(), how = "left")

