#!/usr/bin/env python3
"""
historic_ingestion.py

Reads CSV files from GCS, cleans and enriches the historical BySykkel trip data,
and writes the result into BigQuery via the Spark–BigQuery connector.

Configuration (with sensible defaults, so no extra job-args are required):
  • INPUT_PATH   – GCS glob of CSVs (default: gs://data-management-2-arun-historic/*.csv)
  • OUTPUT_TABLE – BigQuery table (default: data_management_2_arun.historic_trips_cleaned)
  • TEMP_BUCKET  – GCS bucket for BigQuery temp files (default: gs://data-management-2-arun-temp)

You can override any of these by setting the corresponding environment variable.
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# -----------------------------------------------------------------------------
# SECTION 0: Configuration from environment with defaults
# -----------------------------------------------------------------------------
INPUT_PATH   = os.getenv("INPUT_PATH",   "gs://data-management-2-arun-historic/*.csv")
OUTPUT_TABLE = os.getenv("OUTPUT_TABLE", "data_management_2_arun.historic_trips_cleaned")
TEMP_BUCKET  = os.getenv("TEMP_BUCKET",  "gs://data-management-2-arun-temp")

# -----------------------------------------------------------------------------
# SECTION 1: Spark Session Initialization
# -----------------------------------------------------------------------------
def init_spark(app_name: str, temp_bucket: str) -> SparkSession:
    """
    Initialize a SparkSession configured for the BigQuery connector.
    Pulls in the spark-bigquery connector automatically.
    """
    logging.info("Initializing Spark session")
    spark = (
        SparkSession.builder
          .appName(app_name)
          .config(
            "spark.jars.packages",
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.0"
          )
          .config("spark.sql.catalog.spark_bigquery", "com.google.cloud.spark.bigquery")
          .config("temporaryGcsBucket", temp_bucket)
          .getOrCreate()
    )
    return spark

# -----------------------------------------------------------------------------
# SECTION 2: Read Input CSVs
# -----------------------------------------------------------------------------
def read_data(spark: SparkSession, input_path: str):
    """
    Load all CSVs under input_path into a DataFrame.
    """
    logging.info(f"Reading CSV files from {input_path}")
    df = (
        spark.read
             .option("header", "true")
             .option("inferSchema", "true")
             .csv(input_path)
    )
    logging.info(f"Input schema: {df.schema.simpleString()}")
    logging.info(f"Read {df.count():,} rows")
    return df

# -----------------------------------------------------------------------------
# SECTION 3: Data Cleaning
# -----------------------------------------------------------------------------
def clean_data(df):
    """
    Drop malformed rows, parse timestamps and cast columns.
    """
    logging.info("Starting data cleaning")
    # 3.1 Drop rows missing key columns
    df_clean = df.dropna(subset=["ride_id", "started_at", "ended_at"])
    # 3.2 Parse timestamp strings into TimestampType
    df_clean = (
        df_clean
          .withColumn("start_time", to_timestamp(col("started_at"), "yyyy-MM-dd'T'HH:mm:ss"))
          .withColumn("end_time",   to_timestamp(col("ended_at"),   "yyyy-MM-dd'T'HH:mm:ss"))
    )
    # 3.3 Cast numeric/dimension fields
    df_clean = (
        df_clean
          .withColumn("duration_sec",     col("duration_sec").cast("integer"))
          .withColumn("start_station_id", col("start_station_id").cast("string"))
          .withColumn("end_station_id",   col("end_station_id").cast("string"))
    )
    logging.info("Data cleaning complete")
    return df_clean

# -----------------------------------------------------------------------------
# SECTION 4: Data Enrichment (Placeholder)
# -----------------------------------------------------------------------------
def enrich_data(df):
    """
    Placeholder for station‐metadata joins or external lookups (no-op today).
    """
    logging.info("Starting data enrichment (no-op)")
    # TODO: load & join station_information.json here
    logging.info("Data enrichment complete")
    return df

# -----------------------------------------------------------------------------
# SECTION 5: Write to BigQuery
# -----------------------------------------------------------------------------
def write_to_bigquery(df, output_table: str):
    """
    Write the DataFrame to BigQuery, overwriting the target table.
    """
    logging.info(f"Writing to BigQuery table {output_table}")
    (
        df.write
          .format("bigquery")
          .option("table", output_table)
          .mode("overwrite")
          .save()
    )
    logging.info("Write to BigQuery completed")

# -----------------------------------------------------------------------------
# SECTION 6: Main Workflow
# -----------------------------------------------------------------------------
def main():
    # 6.1 Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )
    logging.info(
        "Starting historic ingestion with "
        f"INPUT_PATH={INPUT_PATH}, OUTPUT_TABLE={OUTPUT_TABLE}, TEMP_BUCKET={TEMP_BUCKET}"
    )

    # 6.2 Initialize Spark
    spark = init_spark("HistoricGBFSIngestion", TEMP_BUCKET)

    # 6.3 Read raw CSVs
    df_raw = read_data(spark, INPUT_PATH)

    # 6.4 Clean data
    df_clean = clean_data(df_raw)

    # 6.5 Enrich data
    df_enriched = enrich_data(df_clean)

    # 6.6 Write to BigQuery
    write_to_bigquery(df_enriched, OUTPUT_TABLE)

    # 6.7 Tear down
    spark.stop()
    logging.info("Historic ingestion job finished successfully")

if __name__ == "__main__":
    main()
