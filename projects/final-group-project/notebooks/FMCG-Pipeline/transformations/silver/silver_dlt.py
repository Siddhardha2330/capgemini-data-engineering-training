# Databricks notebook source
import dlt
from pyspark.sql.functions import col, coalesce, current_date, current_timestamp, lit, lower, struct, trim, upper, when


@dlt.table(name="customer_master", table_properties={"quality": "silver"})
def customer_master():
    return (
        dlt.read("customers_raw")
        .filter(col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
        .select(
            col("customer_id").alias("retailer_id"),
            col("customer_city").alias("retailer_city"),
            col("customer_state").alias("retailer_state"),
        )
        .withColumn("_processed_ts", current_timestamp())
    )


@dlt.table(name="product_master", table_properties={"quality": "silver"})
def product_master():
    return (
        dlt.read("products_raw")
        .filter(col("product_id").isNotNull())
        .dropDuplicates(["product_id"])
        .select(
            col("product_id").alias("sku_id"),
            "product_category_name",
        )
        .withColumn("_processed_ts", current_timestamp())
    )


@dlt.table(name="seller_master", table_properties={"quality": "silver"})
def seller_master():
    return (
        dlt.read("sellers_raw")
        .filter(col("seller_id").isNotNull())
        .dropDuplicates(["seller_id"])
        .select(
            col("seller_id").alias("distributor_id"),
            col("seller_city").alias("distributor_city"),
            col("seller_state").alias("distributor_state"),
        )
        .withColumn("_processed_ts", current_timestamp())
    )


@dlt.table(name="region_lookup", table_properties={"quality": "silver"})
def region_lookup():
    data = [
        ("SP", "SOUTHEAST"), ("RJ", "SOUTHEAST"), ("MG", "SOUTHEAST"), ("ES", "SOUTHEAST"),
        ("RS", "SOUTH"), ("SC", "SOUTH"), ("PR", "SOUTH"),
        ("BA", "NORTHEAST"), ("PE", "NORTHEAST"), ("CE", "NORTHEAST"),
        ("AM", "NORTH"), ("PA", "NORTH"),
        ("GO", "CENTRAL-WEST"), ("DF", "CENTRAL-WEST"),
    ]
    return spark.createDataFrame(data, ["state", "region"])


@dlt.table(name="customers_quarantine", table_properties={"quality": "silver"})
def customers_quarantine():
    df = dlt.read("customers_raw")
    bad = df.filter(col("customer_id").isNull())
    return bad.select(struct(*bad.columns).alias("record"), lit("missing_customer_id").alias("failure_reason"), current_timestamp().alias("ingest_ts"))


@dlt.table(name="products_quarantine", table_properties={"quality": "silver"})
def products_quarantine():
    df = dlt.read("products_raw")
    bad = df.filter(col("product_id").isNull())
    return bad.select(struct(*bad.columns).alias("record"), lit("missing_product_id").alias("failure_reason"), current_timestamp().alias("ingest_ts"))


@dlt.table(name="sellers_quarantine", table_properties={"quality": "silver"})
def sellers_quarantine():
    df = dlt.read("sellers_raw")
    bad = df.filter(col("seller_id").isNull())
    return bad.select(struct(*bad.columns).alias("record"), lit("missing_seller_id").alias("failure_reason"), current_timestamp().alias("ingest_ts"))


@dlt.table(name="orders_quarantine", table_properties={"quality": "silver"})
def orders_quarantine():
    df = dlt.read("orders_raw")
    bad = df.filter(col("order_id").isNull() | col("customer_id").isNull() | col("order_purchase_timestamp").isNull())
    return bad.select(struct(*bad.columns).alias("record"), lit("invalid_order_record").alias("failure_reason"), current_timestamp().alias("ingest_ts"))


@dlt.table(name="order_items_quarantine", table_properties={"quality": "silver"})
def order_items_quarantine():
    df = dlt.read("order_items_raw")
    bad = df.filter(col("order_id").isNull() | col("product_id").isNull() | col("seller_id").isNull() | (col("price") <= 0))
    return bad.select(struct(*bad.columns).alias("record"), lit("invalid_order_item_record").alias("failure_reason"), current_timestamp().alias("ingest_ts"))


@dlt.table(name="payments_quarantine", table_properties={"quality": "silver"})
def payments_quarantine():
    df = dlt.read("payments_raw")
    bad = df.filter(col("order_id").isNull() | col("payment_type").isNull() | (col("payment_value") < 0))
    return bad.select(struct(*bad.columns).alias("record"), lit("invalid_payment_record").alias("failure_reason"), current_timestamp().alias("ingest_ts"))


@dlt.table(name="quarantine_sales", table_properties={"quality": "silver"})
def quarantine_sales():
    o = dlt.read("orders_raw").alias("o")
    i = dlt.read("order_items_raw").alias("i")
    p = dlt.read("payments_raw").alias("p")

    df = (
        o.join(i, "order_id")
         .join(p, "order_id", "left")
         .select(
             col("o.order_id"),
             col("o.customer_id"),
             col("o.order_purchase_timestamp"),
             col("i.product_id"),
             col("i.seller_id"),
             col("i.price"),
             col("i.freight_value"),
             col("p.payment_type"),
             col("p.payment_value"),
             col("o._ingest_ts"),
             col("o._source_file"),
             col("o._source_system"),
             col("o._batch_id"),
         )
    )

    invalid_df = df.filter(
        (col("price") <= 0) |
        col("order_purchase_timestamp").isNull() |
        (col("order_purchase_timestamp") > current_date())
    )

    return invalid_df.select(
        struct(*invalid_df.columns).alias("record"),
        when(col("price") <= 0, "invalid_price")
        .when(col("order_purchase_timestamp").isNull(), "missing_date")
        .otherwise("future_date").alias("failure_reason"),
        current_timestamp().alias("ingest_ts"),
    )


@dlt.table(name="quarantine_monitoring", table_properties={"quality": "silver"})
def quarantine_monitoring():
    return (
        dlt.read("quarantine_sales")
        .withColumn("_batch_id", lit("batch_001"))
        .groupBy("_batch_id")
        .count()
    )


@dlt.table(name="silver_quarantine_sales", table_properties={"quality": "silver"})
def silver_quarantine_sales():
    return dlt.read("quarantine_sales")


@dlt.table(
    name="silver_sales",
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def silver_sales():
    o = dlt.read("orders_raw")
    i = dlt.read("order_items_raw")
    p = dlt.read("payments_raw")
    customers = dlt.read("customer_master")
    products = dlt.read("product_master")
    sellers = dlt.read("seller_master")
    regions = dlt.read("region_lookup")

    df = (
        o.join(i, "order_id")
         .join(p, "order_id", "left")
         .join(customers, col("customer_id") == col("retailer_id"), "left")
         .join(products, col("product_id") == col("sku_id"), "left")
         .join(sellers, col("seller_id") == col("distributor_id"), "left")
         .select(
             "order_id",
             "customer_id",
             "seller_id",
             "product_id",
             "order_purchase_timestamp",
             "price",
             "freight_value",
             "payment_type",
             "payment_value",
             "retailer_city",
             "retailer_state",
             "distributor_city",
             "distributor_state",
             "product_category_name",
         )
    )

    df = (
        df.withColumn("payment_type", lower(trim(col("payment_type"))))
          .withColumn("quantity", lit(1))
          .withColumn("net_amount", col("price"))
          .withColumn("sales_value", col("quantity") * col("net_amount"))
          .filter(
              (col("price") > 0) &
              col("order_purchase_timestamp").isNotNull() &
              (col("order_purchase_timestamp") <= current_date())
          )
          .dropDuplicates(["order_id", "product_id"])
    )

    df = (
        df.join(regions, col("distributor_state") == col("state"), "left")
          .withColumn("region", upper(coalesce(col("region"), lit("UNKNOWN"))))
          .withColumn("channel", when(col("distributor_state").isin("SP", "RJ"), "MT").otherwise("GT"))
    )

    final_df = df.select(
        col("order_id").alias("invoice_id"),
        col("seller_id").alias("distributor_id"),
        col("customer_id").alias("retailer_id"),
        col("product_id").alias("sku_id"),
        col("order_purchase_timestamp").alias("invoice_date"),
        "quantity",
        "net_amount",
        "sales_value",
        "freight_value",
        "payment_type",
        "payment_value",
        "channel",
        "region",
        "product_category_name",
        "retailer_city",
        "retailer_state",
        "distributor_city",
        "distributor_state",
    )

    return final_df.withColumn("_processed_ts", current_timestamp())


@dlt.table(name="silver_retailer_master", table_properties={"quality": "silver"})
def silver_retailer_master():
    return dlt.read("customer_master")


@dlt.table(name="silver_distributor_master", table_properties={"quality": "silver"})
def silver_distributor_master():
    return dlt.read("seller_master")


@dlt.table(name="silver_sku_master", table_properties={"quality": "silver"})
def silver_sku_master():
    return dlt.read("product_master")


@dlt.table(name="sales_transactions", table_properties={"quality": "silver"})
def sales_transactions():
    return dlt.read("silver_sales")


@dlt.table(name="silver_sales_transactions", table_properties={"quality": "silver"})
def silver_sales_transactions():
    return dlt.read("silver_sales")


@dlt.table(name="ingestion_log", table_properties={"quality": "silver"})
def ingestion_log():
    customers = dlt.read("customers_raw").select(lit("customers_raw").alias("table_name"))
    orders = dlt.read("orders_raw").select(lit("orders_raw").alias("table_name"))
    items = dlt.read("order_items_raw").select(lit("order_items_raw").alias("table_name"))
    payments = dlt.read("payments_raw").select(lit("payments_raw").alias("table_name"))
    products = dlt.read("products_raw").select(lit("products_raw").alias("table_name"))
    sellers = dlt.read("sellers_raw").select(lit("sellers_raw").alias("table_name"))

    unioned = customers.unionByName(orders).unionByName(items).unionByName(payments).unionByName(products).unionByName(sellers)
    return unioned.groupBy("table_name").count().withColumn("logged_at", current_timestamp())

