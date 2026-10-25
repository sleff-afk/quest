-- Databricks notebook source
-- Gold layer for the three Rearc Data Quest analytical questions.
-- Spark SQL is the primary implementation in this version.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW quest.bls_gold.gold_population_statistics_2013_2018_sql
COMMENT 'Mean and sample/population standard deviations of annual US population, 2013-2018 inclusive.'
AS
SELECT
    COUNT(population) AS year_count,
    AVG(population) AS mean_population,
    STDDEV_SAMP(population) AS sample_stddev_population,
    STDDEV_POP(population) AS population_stddev_population
FROM quest.bls_silver.silver_population
WHERE nation = 'United States'
  AND year BETWEEN 2013 AND 2018;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW quest.bls_gold.gold_bls_best_year_by_series_sql
COMMENT 'Best year per BLS series based on the largest sum across Q01-Q04.'
AS
WITH annual AS (
    SELECT
        series_id,
        year,
        SUM(value) AS annual_value_sum,
        COUNT(DISTINCT period) AS quarter_count
    FROM quest.bls_silver.silver_pr_observations
    WHERE period IN ('Q01', 'Q02', 'Q03', 'Q04')
    GROUP BY series_id, year
),
ranked AS (
    SELECT
        series_id,
        year,
        annual_value_sum,
        quarter_count,
        ROW_NUMBER() OVER (
            PARTITION BY series_id
            ORDER BY annual_value_sum DESC, year ASC
        ) AS year_rank
    FROM annual
)
SELECT
    r.series_id,
    s.series_label,
    r.year AS best_year,
    r.annual_value_sum,
    r.quarter_count
FROM ranked r
LEFT JOIN quest.bls_silver.silver_pr_series s
    ON r.series_id = s.series_id
WHERE r.year_rank = 1;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW quest.bls_gold.gold_prs30006032_q01_population_sql
COMMENT 'PRS30006032 Q01 values by year with US population where available.'
AS
SELECT
    o.series_id,
    o.year,
    o.period,
    o.value AS bls_value,
    p.population
FROM quest.bls_silver.silver_pr_observations o
LEFT JOIN quest.bls_silver.silver_population p
    ON o.year = p.year
   AND p.nation = 'United States'
WHERE o.series_id = 'PRS30006032'
  AND o.period = 'Q01';