# Databricks notebook source
"""Silver layer for typed, validated, deduplicated BLS and population data."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _latest_per_key(df, keys):
    window = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").desc(), F.col("_source_file").desc())
    return df.withColumn("_row_number", F.row_number().over(window)).filter("_row_number = 1").drop("_row_number")


@dp.materialized_view(name="quest.bls_silver.silver_pr_observations", comment="Typed and deduplicated BLS PR observations.")
@dp.expect_or_fail("non_null_observation_key", "series_id IS NOT NULL AND year IS NOT NULL AND period IS NOT NULL")
@dp.expect_or_fail("valid_period", "period IN ('Q01', 'Q02', 'Q03', 'Q04', 'Q05')")
@dp.expect_or_fail("valid_value", "value IS NOT NULL")
@dp.expect_or_fail("reasonable_year", "year BETWEEN 1900 AND year(current_date()) + 1")
def silver_pr_observations():
    typed = (
        spark.read.table("quest.bls_bronze.bronze_pr_observations")
        .select(
            F.trim("series_id").alias("series_id"),
            F.trim("year").cast("int").alias("year"),
            F.upper(F.trim("period")).alias("period"),
            F.trim("value").cast("decimal(18,6)").alias("value"),
            F.trim("footnote_codes").alias("footnote_codes"),
            "_source_file",
            "_ingested_at"
        )
    )
    return _latest_per_key(typed, ["series_id", "year", "period"])


@dp.materialized_view(name="quest.bls_silver.silver_pr_series", comment="BLS PR series enriched with human-readable mapping labels.")
@dp.expect_or_fail("non_null_series_key", "series_id IS NOT NULL")
@dp.expect_or_fail("complete_series_codes", "sector_code IS NOT NULL AND class_code IS NOT NULL AND measure_code IS NOT NULL AND duration_code IS NOT NULL AND seasonal_code IS NOT NULL")
def silver_pr_series():
    series = (
        spark.read.table("quest.bls_bronze.bronze_pr_series")
        .select(
            F.trim("series_id").alias("series_id"),
            F.trim("sector_code").alias("sector_code"),
            F.trim("class_code").alias("class_code"),
            F.trim("measure_code").alias("measure_code"),
            F.trim("duration_code").alias("duration_code"),
            F.trim("seasonal").alias("seasonal_code"),
            F.trim("base_year").alias("base_year"),
            F.trim("footnote_codes").alias("footnote_codes"),
            F.trim("begin_year").cast("int").alias("begin_year"),
            F.upper(F.trim("begin_period")).alias("begin_period"),
            F.trim("end_year").cast("int").alias("end_year"),
            F.upper(F.trim("end_period")).alias("end_period"),
            "_source_file", "_ingested_at"
        )
    )
    classes = spark.read.table("quest.bls_bronze.bronze_pr_class").select(F.trim("class_code").alias("class_code"), F.trim("class_text").alias("class_text"))
    durations = spark.read.table("quest.bls_bronze.bronze_pr_duration").select(F.trim("duration_code").alias("duration_code"), F.trim("duration_text").alias("duration_text"))
    measures = spark.read.table("quest.bls_bronze.bronze_pr_measure").select(F.trim("measure_code").alias("measure_code"), F.trim("measure_text").alias("measure_text"))
    seasonal = spark.read.table("quest.bls_bronze.bronze_pr_seasonal").select(F.trim("seasonal_code").alias("seasonal_code"), F.trim("seasonal_text").alias("seasonal_text"))
    sectors = spark.read.table("quest.bls_bronze.bronze_pr_sector").select(F.trim("sector_code").alias("sector_code"), F.trim("sector_name").alias("sector_name"))

    enriched = (
        series.join(sectors, "sector_code", "left")
        .join(classes, "class_code", "left")
        .join(measures, "measure_code", "left")
        .join(durations, "duration_code", "left")
        .join(seasonal, "seasonal_code", "left")
        .withColumn(
            "series_label",
            F.concat_ws(" | ", "measure_text", "sector_name", "class_text", "duration_text", "seasonal_text")
        )
    )
    return _latest_per_key(enriched, ["series_id"])


@dp.materialized_view(name="quest.bls_silver.silver_population", comment="Typed and deduplicated annual population by nation.")
@dp.expect_or_fail("non_null_population_key", "nation IS NOT NULL AND year IS NOT NULL")
@dp.expect_or_fail("positive_population", "population > 0")
def silver_population():
    source = spark.read.table("quest.bls_bronze.bronze_population")
    # Data USA JSON commonly returns Nation, Year, and Population. Backtick access
    # preserves the provider's capitalization while Silver standardizes names.
    typed = source.select(
        F.trim(F.col("Nation")).alias("nation"),
        F.trim(F.col("Year")).cast("int").alias("year"),
        F.col("Population").cast("long").alias("population"),
        "_source_file", "_ingested_at"
    )
    return _latest_per_key(typed, ["nation", "year"])


    
@dp.materialized_view(
    name="quest.bls_silver.silver_pr_period",
    comment=(
        "Validated BLS PR period dimension. Q01-Q04 are quarters and Q05 is "
        "the annual average."
    ),
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("non_null_period_key", "period IS NOT NULL")
@dp.expect_or_fail("valid_pr_period", "period IN ('Q01', 'Q02', 'Q03', 'Q04', 'Q05')")
@dp.expect_or_fail("period_name_available", "period_name IS NOT NULL")
def silver_pr_period():
    source = spark.read.table("quest.bls_bronze.bronze_pr_period")

    normalized = source.select(
        F.upper(F.trim(F.col("period"))).alias("period"),
        F.upper(F.trim(F.col("period_abbr"))).alias("period_abbr"),
        F.trim(F.col("period_name")).alias("period_name"),
        F.col("_source_file"),
        F.col("_ingested_at"),
    )

    # Protect the dimension key if a rerun or unexpected source condition
    # produces duplicate rows. The latest ingested source row wins.
    latest = Window.partitionBy("period").orderBy(
        F.col("_ingested_at").desc(),
        F.col("_source_file").desc(),
    )

    return (
        normalized
        .withColumn("_row_number", F.row_number().over(latest))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
        .withColumn(
            "period_number",
            F.when(F.col("period").rlike(r"^Q0[1-5]$"),
                   F.substring(F.col("period"), 2, 2).cast("int")),
        )
        .withColumn("is_quarter", F.col("period").isin("Q01", "Q02", "Q03", "Q04"))
        .withColumn("is_annual_average", F.col("period") == F.lit("Q05"))
        .select(
            "period",
            "period_abbr",
            "period_name",
            "period_number",
            "is_quarter",
            "is_annual_average",
            "_source_file",
            "_ingested_at",
        )
    )
