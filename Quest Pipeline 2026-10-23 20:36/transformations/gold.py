# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
"""Gold layer for the three Rearc Data Quest analytical questions.

PySpark is the primary implementation feeding these Gold tables. Equivalent
Spark SQL alternatives are documented at the bottom of this file.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(name="quest.bls_bronze.gold_population_statistics_2013_2018", comment="Mean and sample/population standard deviations of annual US population, 2013-2018 inclusive.")
def gold_population_statistics_2013_2018():
    return (
        spark.read.table("quest.bls_silver.silver_population")
        .filter((F.col("nation") == "United States") & F.col("year").between(2013, 2018))
        .agg(
            F.count("population").alias("year_count"),
            F.avg("population").alias("mean_population"),
            F.stddev_samp("population").alias("sample_stddev_population"),
            F.stddev_pop("population").alias("population_stddev_population")
        )
    )


@dp.materialized_view(name="quest.bls_gold.gold_bls_best_year_by_series", comment="Best year per BLS series based on the largest sum across Q01-Q04.")
def gold_bls_best_year_by_series():
    annual = (
        spark.read.table("quest.bls_silver.silver_pr_observations")
        .filter(F.col("period").isin("Q01", "Q02", "Q03", "Q04"))
        .groupBy("series_id", "year")
        .agg(
            F.sum("value").alias("annual_value_sum"),
            F.countDistinct("period").alias("quarter_count")
        )
    )
    rank_window = Window.partitionBy("series_id").orderBy(F.col("annual_value_sum").desc(), F.col("year").asc())
    winners = annual.withColumn("year_rank", F.row_number().over(rank_window)).filter("year_rank = 1").drop("year_rank")
    labels = spark.read.table("quest.bls_silver.silver_pr_series").select("series_id", "series_label")
    return (
        winners.join(labels, "series_id", "left")
        .select("series_id", "series_label", F.col("year").alias("best_year"), "annual_value_sum", "quarter_count")
    )


@dp.materialized_view(name="quest.bls_bronze.gold_prs30006032_q01_population", comment="PRS30006032 Q01 values by year with US population where available.")
def gold_prs30006032_q01_population():
    observations = (
        spark.read.table("quest.bls_silver.silver_pr_observations")
        .filter((F.col("series_id") == "PRS30006032") & (F.col("period") == "Q01"))
        .select("series_id", "year", "period", F.col("value").alias("bls_value"))
    )
    population = (
        spark.read.table("quest.bls_silver.silver_population")
        .filter(F.col("nation") == "United States")
        .select("year", "population")
    )
    return observations.join(population, "year", "left").orderBy("year")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark SQL alternatives
# MAGIC
# MAGIC The SQL below is intentionally documented rather than materialized, so each
# MAGIC analytical rule is implemented once in PySpark as the primary version and
# MAGIC once in Spark SQL as the required alternative.
# MAGIC
# MAGIC ### 1. Population statistics
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   COUNT(population) AS year_count,
# MAGIC   AVG(population) AS mean_population,
# MAGIC   STDDEV_SAMP(population) AS sample_stddev_population,
# MAGIC   STDDEV_POP(population) AS population_stddev_population
# MAGIC FROM silver_population
# MAGIC WHERE nation = 'United States' AND year BETWEEN 2013 AND 2018;
# MAGIC ```
# MAGIC
# MAGIC ### 2. Best year for every series
# MAGIC ```sql
# MAGIC WITH annual AS (
# MAGIC   SELECT series_id, year, SUM(value) AS annual_value_sum,
# MAGIC          COUNT(DISTINCT period) AS quarter_count
# MAGIC   FROM silver_pr_observations
# MAGIC   WHERE period IN ('Q01', 'Q02', 'Q03', 'Q04')
# MAGIC   GROUP BY series_id, year
# MAGIC ), ranked AS (
# MAGIC   SELECT *, ROW_NUMBER() OVER (
# MAGIC     PARTITION BY series_id ORDER BY annual_value_sum DESC, year ASC
# MAGIC   ) AS year_rank
# MAGIC   FROM annual
# MAGIC )
# MAGIC SELECT r.series_id, s.series_label, r.year AS best_year,
# MAGIC        r.annual_value_sum, r.quarter_count
# MAGIC FROM ranked r
# MAGIC LEFT JOIN silver_pr_series s USING (series_id)
# MAGIC WHERE r.year_rank = 1;
# MAGIC ```
# MAGIC
# MAGIC ### 3. PRS30006032 Q01 joined to population
# MAGIC ```sql
# MAGIC SELECT o.series_id, o.year, o.period, o.value AS bls_value, p.population
# MAGIC FROM silver_pr_observations o
# MAGIC LEFT JOIN silver_population p
# MAGIC   ON o.year = p.year AND p.nation = 'United States'
# MAGIC WHERE o.series_id = 'PRS30006032' AND o.period = 'Q01'
# MAGIC ORDER BY o.year;
# MAGIC ```