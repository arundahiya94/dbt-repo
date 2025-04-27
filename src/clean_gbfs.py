"""
Structured-Streaming job:
  * reads JSON messages from Pub/Sub subscription `gbfs-feed-spark-sub`
  * keeps only the 'station_status' feed
  * adds a TIMESTAMP column
  * writes append-only records to BigQuery table
      data-management-2-arun.gbfs.cleaned_snapshots

Note: requires the Apache Bahir Pub/Sub connector JAR:
  spark-streaming-pubsub_2.12-2.4.0.jar
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType

# -------------------------------------------------------------------------------
PROJECT_ID   = "data-management-2-arun"
SUBSCRIPTION = f"projects/{PROJECT_ID}/subscriptions/gbfs-feed-spark-sub"
BQ_TABLE     = f"{PROJECT_ID}.gbfs.cleaned_snapshots"
CHECKPOINT   = "gs://arun-data-management-bucket/checkpoints/gbfs/"   # ← must be unique & persistent
# -------------------------------------------------------------------------------

# INIT SPARK
spark = (SparkSession.builder
         .appName("clean-oslo-gbfs")
         .getOrCreate())

# set log level so we don’t flood the logs
spark.sparkContext.setLogLevel("WARN")

# 1) Ingest raw Pub/Sub messages using Apache Bahir connector -------------------
raw_df = (spark.readStream
          .format("org.apache.bahir.cloud.pubsub")      # switch to Bahir
          .option("project.id", PROJECT_ID)
          .option("subscription", SUBSCRIPTION)
          .load()
          .selectExpr("CAST(data AS STRING) AS json_str"))

# 2) Parse the envelope created by the Cloud Function --------------------------
schema = (StructType()
          .add("feed_name",    StringType())
          .add("last_updated", IntegerType())
          .add("ttl",          IntegerType())
          .add("payload",      StringType()))          # keep payload as JSON

parsed = (raw_df
          .select(from_json("json_str", schema).alias("m"))
          .select("m.*")
          .filter(col("feed_name") == "station_status")
          .withColumn("event_ts", to_timestamp(col("last_updated"))))

# 3) Example cleaning step ------------------------------------------------------
cleaned = parsed.dropDuplicates(["last_updated"])  # de-dup by timestamp

# 4) Write to BigQuery ----------------------------------------------------------
(cleaned.writeStream
        .format("bigquery")                        # BigQuery connector
        .option("table", BQ_TABLE)
        .option("checkpointLocation", CHECKPOINT)
        .outputMode("append")
        .start()
        .awaitTermination())
