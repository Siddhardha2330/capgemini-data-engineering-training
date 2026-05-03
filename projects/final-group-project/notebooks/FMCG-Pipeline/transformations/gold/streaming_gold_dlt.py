# Databricks notebook source
import dlt
from pyspark.sql.functions import countDistinct, current_timestamp, sum, to_date


@dlt.table(name="silver_sales_stream", table_properties={"quality": "silver"})
def silver_sales_stream():
    orders = dlt.read_stream("orders_raw_stream")
    items = dlt.read_stream("order_items_raw_stream")
    payments = dlt.read_stream("payments_raw_stream")

    df = (
        orders.join(items, "order_id", "inner")
              .join(payments, "order_id", "left")
              .filter("order_id is not null and product_id is not null and seller_id is not null and price > 0")
    )

    return df.selectExpr(
        "order_id as invoice_id",
        "customer_id as retailer_id",
        "seller_id as distributor_id",
        "product_id as sku_id",
        "order_purchase_timestamp as invoice_date",
        "1 as quantity",
        "cast(price as double) as sales_value",
        "cast(payment_value as double) as payment_value",
        "lower(payment_type) as payment_type"
    ).withColumn("_processed_ts", current_timestamp())


@dlt.table(name="sales_summary_stream", table_properties={"quality": "gold"})
def sales_summary_stream():
    return (
        dlt.read_stream("silver_sales_stream")
        .groupBy(to_date("invoice_date").alias("sales_date"))
        .agg(
            sum("sales_value").alias("total_sales"),
            countDistinct("invoice_id").alias("total_orders"),
            sum("quantity").alias("total_quantity"),
        )
        .withColumn("_processed_ts", current_timestamp())
    )

