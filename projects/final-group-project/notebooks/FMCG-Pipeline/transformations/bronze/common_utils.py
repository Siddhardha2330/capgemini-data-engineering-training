# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit


def add_metadata(df, source_file, source_system="ERP", batch_id="batch_001"):
    return (
        df.withColumn("_ingest_ts", current_timestamp())
          .withColumn("_source_file", lit(source_file))
          .withColumn("_source_system", lit(source_system))
          .withColumn("_batch_id", lit(batch_id))
    )


