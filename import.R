options(box.path = box::file())

box::use(
  polars[pl],
  proc/utility[ls],
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
  build(
    p701162 = ls("arrest/p701162"),
    p708085 = ls("arrest/p708085")
  )$
  sink_parquet(
    path = "db/arrest.parquet"
  )

assignment$
  build(
    p602033 = ls("assignment/p602033")
  )$
  collect()$
  write_parquet(
    path = "db/assignment.parquet"
  )

beat$
  build(
    p621077 = ls("beat/p621077", reg = "_1")
  )$
  sink_parquet(
    path = "db/beat.parquet"
  )

contact$
  build(
    p058306 = ls("contact/p058306")
  )$
  collect()$
  write_parquet(
    path = "db/contact.parquet"
  )

military$
  build(
    p606699 = ls("military", reg = "_3")
  )$
  sink_parquet(
    path = "db/military.parquet"
  )

force$
  build(
    report = ls("trr/", reg = "_0|p583646_1"),
    action = ls("trr/", reg = "_2")
  )$
  collect()$
  write_parquet(
    "db/force.parquet"
  )

roster$
  build(
    p058155 = ls("roster/p058155"),
    p540798 = ls("roster/p540798"),
    p596580 = ls("roster/p596580")
  )$
  sink_parquet(
    "db/roster.parquet"
  )

stop$
  build(
    p646845 = ls("isr")
  )$
  collect()$
  write_parquet(
    path = "db/stop.parquet"
  )

warrant$
  build(
    p638148 = ls("warrant/p638148", reg = "_1")
  )$
  sink_parquet(
    path = "db/warrant.parquet"
  )
