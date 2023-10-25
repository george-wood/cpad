options(box.path = box::file())

box::use(
  reticulate[py_run_file, source_python],
  polars[pl]
)

# import assignment
source_python("py/assignment.py")
df <- scan(utility$glob("assignment/p602033/*.csv"))$collect()
df$select("dt_start")

# import stop
source_python("py/stop.py")
df <- scan(utility$ls("isr/"))
df <- df$collect()

