options(box.path = box::file())

box::use(
  proc/utility,
  proc/join,
  proc/personnel,
  import/arrest,
  import/assignment,
  import/beat,
  import/contact,
  import/force,
  import/military,
  import/roster,
  import/stop,
  import/ticket,
  import/warrant
)

# import
df_assignment <-
  assignment$build(
    p602033 = utility$ls("assignment/p602033")
  )

df_roster <-
  roster$build(
    p058155   = utility$ls("roster/", reg = "p058155_0"),
    p596580_4 = utility$ls("roster/", reg = "p596580_4"),
    p596580_2 = utility$ls("roster/", reg = "p596580_2")
  )

df_stop <-
  stop$build(
    p646845 = utility$ls("isr/")
  )

df_arrest <-
  arrest$build(
    p701162 = utility$ls("arrest/p701162"),
    p708085 = utility$ls("arrest/p708085")
  )

df_beat <-
  beat$build(
    p621077 = utility$ls("beat/p621077", reg = "1\\.csv$")
  )

df_contact <-
  contact$build(
    p058306 = utility$ls("contact/p058306")
  )

df_force <-
  force$build(
    report = utility$ls("trr/", reg = "0\\.csv$|p583646.*1\\.csv$"),
    action = utility$ls("trr/", reg = "2\\.csv$")
  )

df_military <-
  military$build(
    p606699 = utility$ls("military", reg = "p606699_3")
  )

df_warrant <-
  warrant$build(
    p638148 = utility$ls("warrant/p638148", reg = "1\\.csv$")
  )

# build personnel frame
df_personnel <-
  personnel$build(
    list(
      df_roster,
      df_assignment,
      df_stop
    )
  )

# join uid
join_cols <- c(
  "last_name",
  "first_name",
  "middle_initial",
  "appointed",
  "yob",
  "star"
)

df_assignment <-
  join$uid(
    df_assignment,
    df_personnel,
    on = join_cols
  )

df_arrest <-
  join$uid(
    df_arrest,
    df_personnel,
    on = c("last_name", "first_name", "appointed", "yob", "middle_initial")
  )

df_contact <-
  join$uid_asof(
    df_contact,
    df_personnel,
    by = c("last_name", "first_name"),
    left_on = "yob_lower",
    right_on = "yob"
  )

df_force <-
  join$uid_asof(
    df_force,
    df_personnel,
    by = c("last_name", "first_name", "appointed"),
    left_on = "yob_lower",
    right_on = "yob"
  )

df_military <-
  join$uid(
    df_military,
    df_personnel,
    on = c("last_name", "first_name", "appointed", "yob")
  )

df_roster <-
  join$uid(
    df_roster,
    df_personnel,
    on = join_cols
  )

df_stop <-
  join$uid(
    df_stop,
    df_personnel,
    on = join_cols
  )

df_warrant <-
  join$uid_asof(
    df_warrant,
    df_personnel,
    by = c("last_name", "first_name", "yob", "star"),
    left_on = "appointed_year",
    right_on = "appointed_year"
  )

# join aid
df_arrest <-
  join$aid_asof(
    df_arrest,
    df_assignment
  )

df_contact <-
  join$aid_asof(
    df_contact,
    df_assignment
  )

df_force <-
  join$aid_asof(
    df_force,
    df_assignment
  )

df_stop <-
  join$aid_asof(
    df_stop,
    df_assignment
  )

# write parquet
utility$write(df_arrest, dir = "arrest")
utility$write(df_assignment, group = "dt_start", dir = "assignment")
utility$write(df_beat, group = NULL, dir = "beat")
utility$write(df_contact, dir = "contact")
utility$write(df_force, dir = "force")
utility$write(df_military, group = NULL, dir = "military")
utility$write(df_roster, group = NULL, dir = "roster")
utility$write(df_stop, dir = "stop")
utility$write(df_warrant, dir = "warrant")

