# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
"""Bronze layer for the Rearc Data Quest Spark Declarative Pipeline.

Pipeline configuration required:
  raw_volume_path = /Volumes/<catalog>/<schema>/<volume>

This source assumes Step 1 has landed the complete BLS PR folder under
<raw_volume_path>/bls/pr and the Data USA response under
<raw_volume_path>/population. Bronze preserves source values as strings and
adds lineage metadata. pr.data.1.AllData is the authoritative historical feed;
pr.data.0.Current remains in the Volume but is not unioned into observations.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

RAW_VOLUME_PATH = spark.conf.get("raw_volume_path").rstrip("/")
BLS_PATH = f"{RAW_VOLUME_PATH}"
POPULATION_PATH = f"{RAW_VOLUME_PATH}/population.json"

def _read_tsv(path: str):
    """Read a BLS tab-delimited file and normalize headers and string values."""
    raw = (
        spark.read
        .option("header", "true")
        .option("sep", "\t")
        .csv(path)
    )
    return raw.select(
        *[F.trim(F.col(f"`{c}`")).alias(c.strip()) for c in raw.columns],
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at")
    )


@dp.materialized_view(
    name="quest.bls_bronze.bronze_pr_observations",
    comment="Raw BLS PR historical observations from pr.data.1.AllData."
)
def bronze_pr_observations():
    return _read_tsv(f"{BLS_PATH}/pr.data.1.AllData")


@dp.materialized_view(name="quest.bls_bronze.bronze_pr_series", comment="Raw BLS PR series definitions.")
def bronze_pr_series():
    return _read_tsv(f"{BLS_PATH}/pr.series")


@dp.materialized_view(name="quest.bls_bronze.bronze_pr_class", comment="Raw BLS PR class mapping.")
def bronze_pr_class():
    return _read_tsv(f"{BLS_PATH}/pr.class")


@dp.materialized_view(name="quest.bls_bronze.bronze_pr_duration", comment="Raw BLS PR duration mapping.")
def bronze_pr_duration():
    return _read_tsv(f"{BLS_PATH}/pr.duration")


@dp.materialized_view(name="quest.bls_bronze.bronze_pr_measure", comment="Raw BLS PR measure mapping.")
def bronze_pr_measure():
    return _read_tsv(f"{BLS_PATH}/pr.measure")


@dp.materialized_view(name="quest.bls_bronze.bronze_pr_period", comment="Raw BLS PR period mapping.")
def bronze_pr_period():
    return _read_tsv(f"{BLS_PATH}/pr.period")


@dp.materialized_view(name="quest.bls_bronze.bronze_pr_seasonal", comment="Raw BLS PR seasonal mapping.")
def bronze_pr_seasonal():
    return _read_tsv(f"{BLS_PATH}/pr.seasonal")


@dp.materialized_view(name="quest.bls_bronze.bronze_pr_sector", comment="Raw BLS PR sector mapping.")
def bronze_pr_sector():
    return _read_tsv(f"{BLS_PATH}/pr.sector")


@dp.materialized_view(name="quest.bls_bronze.bronze_population", comment="Raw Data USA population API records.")
def bronze_population():
    # recursiveFileLookup supports timestamped/versioned JSON filenames from ingestion.
    df = spark.read \
        .option("multiLine", "true") \
        .json(POPULATION_PATH)
    # Replaces all spaces ' ' with underscores '_'
    # df_cleaned = df.select([F.col(c).alias(c.replace(' ', '_')) for c in df.columns])
    return (
            df.select(
            F.explode_outer("data").alias("record"),
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),  
        )
        .select("record.*", "_source_file", "_ingested_at").withColumnRenamed("Nation ID", "Nation_ID")
    )