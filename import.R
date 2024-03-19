options(box.path = box::file())

box::use(
  polars[pl],
  import/arrest,
  import/assignment,
  import/beat,
  import/contact,
  import/force,
  import/military,
  import/roster,
  import/stop,
  import/warrant
)

arrest$
  build()$
  sink_parquet("db/arrest.parquet")

assignment$
  build()$
  collect()$
  write_parquet("db/assignment.parquet")

beat$
  build()$
  sink_parquet("db/beat.parquet")

contact$
  build()$
  collect()$
  write_parquet("db/contact.parquet")

force$
  build()$
  collect()$
  write_parquet("db/force.parquet")

military$
  build()$
  sink_parquet("db/military.parquet")

roster$
  build()$
  sink_parquet("db/roster.parquet")

stop$
  build()$
  collect()$
  write_parquet("db/stop.parquet")

warrant$
  build()$
  sink_parquet("db/warrant.parquet")


