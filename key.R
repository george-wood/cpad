options(box.path = box::file())

box::use(
  polars[pl],
  tibble[tribble],
  purrr[pmap_vec],
  cli[cli_alert_success],
  proc/officer,
  proc/uid
)

#' Helpers
join_and_write <- function(db, on) {
  uid$join(db, on)$collect()$write_parquet(db)
  cli_alert_success("Joined key to {.file {db}}")
}

join_asof_and_write <- function(db, by) {
  uid$join_asof(db, by = by)$collect()$write_parquet(db)
  cli_alert_success("Joined key to {.file {db}}")
}

#' Generate officers
officer$
  generate_key()$
  collect()$
  write_parquet("private/officer.parquet")

#' Join specification
spec <-
  tribble(
    ~db,                     ~on,
    "db/arrest.parquet",     officer$key(),
    "db/assignment.parquet", officer$key(),
    "db/military.parquet",   officer$key(),
    "db/stop.parquet",       officer$key(),
    "db/warrant.parquet",    gsub("appointed", "appointed_year", officer$key())
  )

spec_asof <-
  tribble(
    ~db,                  ~by,
    "db/contact.parquet", c("last_name", "first_name"),
    "db/force.parquet",   c("last_name", "first_name", "appointed")
  )

#' Join UID to imported data
pmap_vec(spec, join_and_write)
pmap_vec(spec_asof, join_asof_and_write)
