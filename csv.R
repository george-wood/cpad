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

pl$
  scan_parquet("private/assignment/*/*.parquet")$
  sink_csv("csv/assignment_names.csv")

