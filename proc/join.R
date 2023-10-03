'.__module__.'

#' @export
uid <- function(x, personnel, on = NULL) {

  box::use(
    polars[pl],
    testthat[expect_equal]
  )

  res <-
    x$join(
      other = personnel$select("uid", on)$unique(),
      on = on,
      how = "left",
      suffix = "_right",
      allow_parallel = TRUE,
      force_parallel = FALSE
    )$select(
      "uid", pl$all()$exclude("uid")
    )

  expect_equal(nrow(x), nrow(res))
  expect_equal(ncol(x), ncol(res) - 1)
  res

}

#' @export
uid_asof <- function(x, personnel, by, left_on = NULL,
                     right_on = NULL, tolerance = NULL) {

  box::use(
    polars[pl],
    testthat[expect_equal]
  )

  vallies <- x$drop_nulls(
    c(by, left_on)
  )

  nullies <- x$join(
    other = vallies,
    on = intersect(colnames(x), colnames(vallies)),
    how = "anti"
  )

  res <-
    pl$concat(
      list(
        vallies$join_asof(
          other = personnel$select("uid", by, right_on)$unique(),
          left_on = left_on,
          right_on = right_on,
          strategy = "forward",
          tolerance = tolerance,
          by = by,
          how = "left",
          allow_parallel = TRUE,
          force_parallel = FALSE
        )$select(
          "uid", pl$all()$exclude(c("uid", right_on))
        ),
        nullies
      ),
      how = "diagonal"
    )

  expect_equal(nrow(x), nrow(res))
  expect_equal(ncol(x), ncol(res) - 1)
  res

}

#' @export
aid_asof <- function(x, assignment, by = "uid", left_on = "dt",
                     right_on = "dt_start", tolerance = "12h30m") {

  box::use(
    polars[pl],
    testthat[expect_equal]
  )

  vallies <-
    x$filter(
      pl$col(left_on)$gt_eq(assignment$get_column(right_on)$min())
    )$sort(
      left_on
    )

  nullies <-
    x$join(
      other = vallies,
      on = intersect(colnames(x), colnames(vallies)),
      how = "anti"
    )

  assignment <-
    assignment$drop_nulls(
      right_on
    )$filter(
      pl$col(right_on)$lt(pl$col("dt_end"))
    )$sort(
      # prefer the assignment for which officer was present
      right_on, "present_for_duty"
    )$select(
      c(union("uid", by), "aid", right_on, "dt_end")
    )

  res <-
    pl$concat(
      list(
        vallies$join_asof(
          other = assignment,
          by = by,
          left_on = left_on,
          right_on = right_on,
          tolerance = tolerance,
          strategy = "backward",
          how = "left",
          allow_parallel = TRUE,
          force_parallel = FALSE
        )$with_columns(
          pl$col(left_on)$is_between(
            start = pl$col(right_on),
            end = pl$col("dt_end"),
            include_bounds = TRUE
          )$alias("aid_strict")
        )$select(
          "uid",
          "aid",
          pl$all()$exclude(c("uid", "aid", right_on, "dt_end"))
        ),
        nullies
      ),
      how = "diagonal"
    )

  expect_equal(
    nrow(x),
    nrow(res)
  )
  expect_equal(
    ncol(x),
    ncol(res) - (3 - sum(colnames(x) %in% c("uid", "aid")))
  )

  res

}
