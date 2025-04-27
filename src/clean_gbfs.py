#!/usr/bin/env python3
"""
streaming_ingestion.py

Reads GBFS station_status JSON messages from Pub/Sub, cleans & flattens them,
and writes the result into BigQuery via the Spark–BigQuery connector.

Configuration (with defaults):
  • PUBSUB_SUBSCRIPTION  – Pub/Sub subscription path
                          (default: projects/data-management-2-arun/subscriptions/gbfs-feed-spark-sub)
  • OUTPUT_TABLE         – BigQuery table
                          (default: data_management_2_arun.gbfs_feed_cleaned)
  • TEMP_BUCKET          – GCS bucket for BigQuery temp files
                          (default: gs://data-management-2-arun-temp)
  • CHECKPOINT_LOCATION  – GCS path for streaming checkpointing
                          (default: gs://data-management-2-arun-checkpoints/gbfs_streaming)
  • STATION_INFO_PATH    – (optional) GCS path to station_information.json
                          if you want to join metadata in enrich_data()
  
Override any of these via environment variables.
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, explode, from_unixtime, to_timestamp
)
from pyspark.sql.types import *

# -----------------------------------------------------------------------------
# SECTION 0: Configuration from environment with defaults
# -----------------------------------------------------------------------------
PUBSUB_SUBSCRIPTION = os.getenv(
    "PUBSUB_SUBSCRIPTION",
    "projects/data-management-2-arun/subscriptions/gbfs-feed-spark-sub"
)
OUTPUT_TABLE = os.getenv(
    "OUTPUT_TABLE",
    "data_management_2_arun.gbfs_feed_cleaned"
)
TEMP_BUCKET = os.getenv(
    "TEMP_BUCKET",
    "gs://data-management-2-arun-temp"
)
CHECKPOINT_LOCATION = os.getenv(
    "CHECKPOINT_LOCATION",
    "gs://data-management-2-arun-checkpoints/gbfs_streaming"
)
STATION_INFO_PATH = os.getenv(
    "STATION_INFO_PATH", 
    "gs://data-management-2-arun/gbfs/oslo/station_information.json"
)

# -----------------------------------------------------------------------------
# SECTION 1: Spark Session Initialization
# -----------------------------------------------------------------------------
def init_spark(app_name: str, temp_bucket: str) -> SparkSession:
    logging.info("Initializing Spark session")
    return (
        SparkSession.builder
            .appName(app_name)
            .config("spark.jars.packages",
                    ",".join([
                      "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.0",
                      "org.apache.bahir:spark-sql-streaming-pubsub_2.12:2.4.0"
                    ]))
            .config("spark.sql.catalog.spark_bigquery", "com.google.cloud.spark.bigquery")
            .config("temporaryGcsBucket", temp_bucket)
            .getOrCreate()
    )

# -----------------------------------------------------------------------------
# SECTION 2: Read from Pub/Sub as a streaming DataFrame
# -----------------------------------------------------------------------------
def read_stream(spark: SparkSession, subscription: str):
    logging.info(f"Setting up readStream from {subscription}")
    project = subscription.split("/")[1]
    df = (
        spark.readStream
             .format("pubsub")
             .option("project", project)
             .option("subscription", subscription)
             .load()
    )
    logging.info(f"Incoming schema: {df.schema.simpleString()}")
    return df

# -----------------------------------------------------------------------------
# SECTION 3: Data Cleaning
# -----------------------------------------------------------------------------
def clean_data(df):
    """
    Parse the Pub/Sub payload as JSON, explode the stations array,
    and flatten all fields, converting last_reported to a timestamp.
    """
    logging.info("Starting data cleaning")

    # 1) Cast payload bytes to string
    df1 = df.selectExpr("CAST(data AS STRING) AS json_payload")

    # 2) Define exact station_status schema
    station_status_schema = StructType([
        StructField("last_updated", LongType(), True),
        StructField("ttl", IntegerType(), True),
        StructField("version", StringType(), True),
        StructField("data", StructType([
            StructField("stations", ArrayType(StructType([
                StructField("station_id", StringType(), True),
                StructField("num_bikes_available", IntegerType(), True),
                StructField("vehicle_types_available", ArrayType(StructType([
                    StructField("vehicle_type_id", StringType(), True),
                    StructField("count", IntegerType(), True)
                ])), True),
                StructField("num_docks_available", IntegerType(), True),
                StructField("is_installed", BooleanType(), True),
                StructField("is_renting", BooleanType(), True),
                StructField("is_returning", BooleanType(), True),
                StructField("last_reported", LongType(), True)
            ])), True)
        ]))
    ])

    # 3) Parse JSON and explode stations
    df2 = df1 \
      .select(from_json(col("json_payload"), station_status_schema).alias("j")) \
      .selectExpr("j.last_updated", "j.ttl", "j.version", "explode(j.data.stations) AS station")

    # 4) Flatten and convert last_reported → timestamp
    df_clean = (
        df2
          .select(
            col("station.station_id").alias("station_id"),
            col("station.num_bikes_available").alias("bikes_available"),
            col("station.num_docks_available").alias("docks_available"),
            col("station.is_installed").alias("is_installed"),
            col("station.is_renting").alias("is_renting"),
            col("station.is_returning").alias("is_returning"),
            # convert epoch (seconds) → timestamp:
            to_timestamp(from_unixtime(col("station.last_reported")), "yyyy-MM-dd HH:mm:ss")
              .alias("report_time"),
            col("last_updated"),
            col("ttl"),
            col("version")
          )
    )

    logging.info("Data cleaning complete")
    return df_clean

# -----------------------------------------------------------------------------
# SECTION 4: Data Enrichment (join static station_information)
# -----------------------------------------------------------------------------
def enrich_data(df, spark):
    """
    Load static station_information.json and join on station_id.
    """
    logging.info("Loading station_information for enrichment")
    # read the one-line JSON into a DataFrame
    info_df = (
        spark.read
             .option("multiline", "true")
             .json(STATION_INFO_PATH)
             .selectExpr("explode(data.stations) AS meta")
             .select(
               col("meta.station_id").alias("station_id"),
               col("meta.name").alias("station_name"),
               col("meta.lat").alias("latitude"),
               col("meta.lon").alias("longitude"),
               col("meta.address").alias("address"),
               col("meta.capacity").alias("station_capacity")
             )
    )

    logging.info(f"Joining live feed with {info_df.count():,} station metadata records")
    df_enriched = df.join(info_df, on="station_id", how="left")
    logging.info("Data enrichment complete")
    return df_enriched

# -----------------------------------------------------------------------------
# SECTION 5: Write to BigQuery
# -----------------------------------------------------------------------------
def write_stream(df, output_table: str, checkpoint_location: str):
    logging.info(f"Writing stream to BigQuery table {output_table}")

    def foreach_batch(batch_df, batch_id):
        logging.info(f"Writing batch {batch_id}")
        (
            batch_df.write
                    .format("bigquery")
                    .option("table", output_table)
                    .mode("append")
                    .save()
        )
        logging.info(f"Batch {batch_id} write complete")

    df.writeStream \
      .foreachBatch(foreach_batch) \
      .option("checkpointLocation", checkpoint_location) \
      .trigger(processingTime="1 minute") \
      .start()

    logging.info("Stream started")

# -----------------------------------------------------------------------------
# SECTION 6: Main Workflow
# -----------------------------------------------------------------------------
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )
    logging.info(
        "Starting GBFS streaming ingestion with "
        f"SUBSCRIPTION={PUBSUB_SUBSCRIPTION}, "
        f"OUTPUT_TABLE={OUTPUT_TABLE}, "
        f"TEMP_BUCKET={TEMP_BUCKET}, "
        f"CHECKPOINT={CHECKPOINT_LOCATION}"
    )

    spark = init_spark("GBFSPubSubIngestion", TEMP_BUCKET)
    raw_df = read_stream(spark, PUBSUB_SUBSCRIPTION)
    clean_df = clean_data(raw_df)
    enriched_df = enrich_data(clean_df, spark)
    write_stream(enriched_df, OUTPUT_TABLE, CHECKPOINT_LOCATION)
    spark.streams.awaitAnyTermination()
    logging.info("Streaming ingestion job terminated")

if __name__ == "__main__":
    main()
    logging.info("Script completed successfully")