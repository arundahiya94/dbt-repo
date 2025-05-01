#!/usr/bin/env python3
"""
streaming_ingestion.py

Reads GBFS station_status JSON messages from Pub/Sub (or Pub/Sub Lite),
cleans & flattens them, and writes the result into BigQuery via the
Spark–BigQuery connector with partitioning, clustering, and direct writes.

Improvements included:
  1. Broadcast join of station metadata
  2. Watermarking + de-duplication
  3. Partitioned & clustered BigQuery writes
  4. BigQuery Storage Write API ("direct") usage
  5. Module‐level constants for schema & config
  6. Error handling & graceful shutdown in foreachBatch
  7. Explicit JARs only, no spark.jars.packages in code
  8. Optional switch to Pub/Sub Lite connector
"""

import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, explode, from_unixtime, to_timestamp,
    broadcast
)
from pyspark.sql.types import *

# -------------------------
# SECTION 0: Config & Schema
# -------------------------

# Pub/Sub vs Pub/Sub Lite toggle: if your SUBSCRIPTION string starts with
# "projects/.../locations/...", Spark will auto-pick the pubsublite format.
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
    "gs://data-management-2-arun/gbfs/station_information.json"
)
# How late data we accept (2 minutes) before state cleanup
WATERMARK_DELAY = "2 minutes"

# Exact station_status schema
STATION_STATUS_SCHEMA = StructType([
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

# -------------------------
# SECTION 1: Spark Session
# -------------------------

def init_spark(app_name: str, temp_bucket: str) -> SparkSession:
    logging.info("Initializing Spark session")
    return (
        SparkSession.builder
            .appName(app_name)
            # JARs passed via --jars on the CLI; no spark.jars.packages here
            .config("temporaryGcsBucket", temp_bucket)
            .getOrCreate()
    )

# -------------------------
# SECTION 2: Read Stream
# -------------------------

def read_stream(spark: SparkSession, subscription: str):
    logging.info(f"Setting up readStream from {subscription}")
    sdf = spark.readStream
    # auto‐detect Pub/Sub vs Pub/Sub Lite based on path prefix
    if "locations" in subscription and "subscriptions" not in subscription:
        sdf = sdf.format("pubsublite")
    else:
        sdf = sdf.format("pubsub")
    project = subscription.split("/")[1]
    df = (
        sdf
          .option("project", project)
          .option("subscription", subscription)
          .load()
    )
    logging.info(f"Incoming schema: {df.schema.simpleString()}")
    return df

# -------------------------
# SECTION 3: Clean + Dedupe
# -------------------------

def clean_data(df):
    logging.info("Starting data cleaning")
    # cast bytes to string
    json_df = df.selectExpr("CAST(data AS STRING) AS json_payload")
    # parse & explode
    exploded = (
        json_df
          .select(from_json(col("json_payload"), STATION_STATUS_SCHEMA).alias("j"))
          .selectExpr(
            "j.last_updated", "j.ttl", "j.version",
            "explode(j.data.stations) AS station"
          )
    )
    # flatten, convert epoch → timestamp, watermark & dedupe
    cleaned = (
        exploded
          .select(
            col("station.station_id").alias("station_id"),
            col("station.num_bikes_available").alias("bikes_available"),
            col("station.num_docks_available").alias("docks_available"),
            col("station.is_installed").alias("is_installed"),
            col("station.is_renting").alias("is_renting"),
            col("station.is_returning").alias("is_returning"),
            to_timestamp(
                from_unixtime(col("station.last_reported")),
                "yyyy-MM-dd HH:mm:ss"
            ).alias("report_time"),
            col("last_updated"),
            col("ttl"),
            col("version")
          )
          .withWatermark("report_time", WATERMARK_DELAY)
          .dropDuplicates(["station_id", "report_time"])
    )
    logging.info("Data cleaning complete")
    return cleaned

# -------------------------
# SECTION 4: Enrichment
# -------------------------

def enrich_data(df, spark):
    logging.info("Loading station metadata")
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
    count = info_df.count()
    logging.info(f"Broadcasting {count:,} station metadata records")
    enriched = df.join(
        broadcast(info_df),
        on="station_id",
        how="left"
    )
    logging.info("Data enrichment complete")
    return enriched

# -------------------------
# SECTION 5: Write Stream
# -------------------------

def write_stream(df, output_table: str, checkpoint_location: str):
    logging.info(f"Configuring writeStream to {output_table}")

    def foreach_batch(batch_df, batch_id):
        logging.info(f"[batch {batch_id}] writing {batch_df.count():,} rows")
        try:
            (
                batch_df.write
                        .format("bigquery")
                        .option("table", output_table)
                        .option("writeMethod", "direct")               # Storage Write API
                        .option("partitionField", "report_time")      # daily partition
                        .option("partitionType", "DAY")
                        .option("clusteredFields", "station_id")     # cluster by station
                        .mode("append")
                        .save()
            )
            logging.info(f"[batch {batch_id}] write complete")
        except Exception as e:
            logging.error(f"[batch {batch_id}] write failed: {e}", exc_info=True)
            # stop all streams and exit so Dataproc will retry / alert
            batch_df.sparkSession.streams.stopAll()
            sys.exit(1)

    df.writeStream \
      .foreachBatch(foreach_batch) \
      .option("checkpointLocation", checkpoint_location) \
      .trigger(processingTime="1 minute") \
      .start()
    logging.info("Streaming query started")

# -------------------------
# SECTION 6: Main
# -------------------------

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )
    logging.info(
        "Starting GBFS streaming ingestion: "
        f"SUB={PUBSUB_SUBSCRIPTION}, OUT={OUTPUT_TABLE}, "
        f"TEMP={TEMP_BUCKET}, CKPT={CHECKPOINT_LOCATION}"
    )

    spark = init_spark("GBFSPubSubIngestion", TEMP_BUCKET)
    raw = read_stream(spark, PUBSUB_SUBSCRIPTION)
    cleaned = clean_data(raw)
    enriched = enrich_data(cleaned, spark)
    write_stream(enriched, OUTPUT_TABLE, CHECKPOINT_LOCATION)
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
    logging.info("Script completed successfully")
