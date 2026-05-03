# Databricks notebook source
import dlt
from pyspark.sql.functions import col, current_timestamp


@dlt.table(name="orders_raw_stream", table_properties={"quality": "bronze"})
def orders_raw_stream():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/fmcg/bronze/orders_stream/")
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )


@dlt.table(name="order_items_raw_stream", table_properties={"quality": "bronze"})
def order_items_raw_stream():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/fmcg/bronze/order_items_stream/")
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )


@dlt.table(name="payments_raw_stream", table_properties={"quality": "bronze"})
def payments_raw_stream():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/fmcg/bronze/payments_stream/")
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )


@dlt.table(name="orders_streaming", table_properties={"quality": "bronze"})
def orders_streaming():
    return dlt.read_stream("orders_raw_stream")


@dlt.table(name="orders_streaming_half", table_properties={"quality": "bronze"})
def orders_streaming_half():
    return dlt.read_stream("orders_raw_stream").filter("abs(hash(order_id)) % 2 = 0")


