import polars as pl

def scan(path):
    df = (
        pl.scan_csv(
            path,
            infer_schema_length=0,
            new_columns=[
                "date",
                "unit",
                "watch",
                "beat",
                "vehicle",
                "dt_start",
                "dt_end",
                "last_name",
                "first_name",
                "middle_initial",
                "rank",
                "star",
                "gender",
                "race",
                "yob",
                "appointed",
                "present_for_duty",
                "absence_code",
                "absence_description",
                "modified_by_last",
                "modified_by_first",
                "modified_date",
            ],
        )
        .with_columns(
            pl.concat_str(
                pl.col("dt_start").str.zfill(4),
                pl.col("date"),
            ).str.strptime(
                dtype=pl.Datetime(),
                format="%H%M%d-%b-%Y",
                strict=True,
            ),
            pl.concat_str(
                pl.col("dt_end").str.zfill(4),
                pl.col("date"),
            ).str.strptime(
                dtype=pl.Datetime(),
                format="%H%M%d-%b-%Y",
                strict=True,
            ),
            pl.col("date").str.strptime(pl.Date, format="%d-%b-%Y"),
            pl.col("appointed").str.strptime(pl.Date, format="%Y-%m-%d"),
            pl.col("present_for_duty").str.contains("True"),
            pl.col("beat").str.replace_all(pattern=" |[[:punct:]]", value=""),
            pl.col("vehicle").str.strip_chars(),
        )
        .with_columns(
            pl
            .when(pl.col("dt_end").lt(pl.col("dt_start")))
            .then(pl.col("dt_end").dt.offset_by("1d"))
            .otherwise(pl.col("dt_end"))
        )
        .group_by(pl.all().exclude("^modified.*$"))
        .agg(pl.all().sort_by("modified_date").last())
        .with_row_count("aid")
        .with_columns(pl.col("aid").cast(pl.Utf8).str.zfill(8))
        .sort(["dt_start", "dt_end"])
    )

    return df
