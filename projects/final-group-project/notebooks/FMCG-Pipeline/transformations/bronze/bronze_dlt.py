# Databricks notebook source
import dlt
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import *


BASE_PATH = "/Volumes/fmcg/bronze"


def add_metadata(df, source_file, source_system="ERP", batch_id="batch_001"):
    return (
        df.withColumn("_ingest_ts", current_timestamp())
          .withColumn("_source_file", lit(source_file))
          .withColumn("_source_system", lit(source_system))
          .withColumn("_batch_id", lit(batch_id))
    )


customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_unique_id", StringType(), True),
    StructField("customer_zip_code_prefix", IntegerType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
])

orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", TimestampType(), True),
    StructField("order_approved_at", TimestampType(), True),
    StructField("order_delivered_carrier_date", TimestampType(), True),
    StructField("order_delivered_customer_date", TimestampType(), True),
    StructField("order_estimated_delivery_date", TimestampType(), True),
])

order_items_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_item_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("shipping_limit_date", TimestampType(), True),
    StructField("price", DoubleType(), True),
    StructField("freight_value", DoubleType(), True),
])

payments_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DoubleType(), True),
])

products_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_category_name", StringType(), True),
])

sellers_schema = StructType([
    StructField("seller_id", StringType(), True),
    StructField("seller_zip_code_prefix", IntegerType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True),
])


@dlt.view
def customers_source():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(customers_schema)
        .load(f"{BASE_PATH}/customers/customers_raw.csv")
    )


@dlt.table(name="customers_raw", table_properties={"quality": "bronze"})
def customers_raw():
    return add_metadata(dlt.read("customers_source"), "customers_raw.csv")


@dlt.view
def orders_source():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(orders_schema)
        .load(f"{BASE_PATH}/orders/orders_raw.csv")
    )


@dlt.table(name="orders_raw", table_properties={"quality": "bronze"})
def orders_raw():
    return add_metadata(dlt.read("orders_source"), "orders_raw.csv")


@dlt.view
def order_items_source():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(order_items_schema)
        .load(f"{BASE_PATH}/order_items/order_items_raw.csv")
    )


@dlt.table(name="order_items_raw", table_properties={"quality": "bronze"})
def order_items_raw():
    return add_metadata(dlt.read("order_items_source"), "order_items_raw.csv")


@dlt.view
def payments_source():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(payments_schema)
        .load(f"{BASE_PATH}/payments/order_payments_raw.csv")
    )


@dlt.table(name="payments_raw", table_properties={"quality": "bronze"})
def payments_raw():
    return add_metadata(dlt.read("payments_source"), "order_payments_raw.csv")


@dlt.view
def products_source():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(products_schema)
        .load(f"{BASE_PATH}/products/products_raw.csv")
    )


@dlt.table(name="products_raw", table_properties={"quality": "bronze"})
def products_raw():
    return add_metadata(dlt.read("products_source"), "products_raw.csv")


@dlt.view
def sellers_source():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(sellers_schema)
        .load(f"{BASE_PATH}/seller/sellers_raw.csv")
    )


@dlt.table(name="sellers_raw", table_properties={"quality": "bronze"})
def sellers_raw():
    return add_metadata(dlt.read("sellers_source"), "sellers_raw.csv")

