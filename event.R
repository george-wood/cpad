options(box.path = box::file())

box::use(
  polars[pl],
  proc/aid
)

#' Helper
join_aid_and_write <- function(db, ...) {
  aid$join_asof(db, ...)$sink_parquet(db)
}

#' Join AID to event data
sapply(
  c("db/arrest.parquet",
    "db/contact.parquet",
    "db/force.parquet",
    "db/stop.parquet"),
  join_aid_and_write
)
