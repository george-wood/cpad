box::use(polars[pl])

pl$
  scan_parquet("db/assignment/*/*.parquet")$
  sink_csv("csv/assignment.csv")

pl$
  scan_parquet("db/arrest/*/*.parquet")$
  sink_csv("csv/arrest.csv")

pl$
  scan_parquet("db/force/*/*.parquet")$
  sink_csv("csv/force.csv")

pl$
  scan_parquet("db/stop/*/*.parquet")$
  sink_csv("csv/stop.csv")

# assignment data with names
pl$
  read_parquet("db/assignment/*/*.parquet")$
  join(
    other = df_personnel$unique("uid")$drop(c("star", "appointed_year")),
    on = c("uid", "appointed", "yob"),
    how = "left"
  )$
  write_csv("csv/assignment_names.csv")


