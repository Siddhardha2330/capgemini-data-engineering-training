# Databricks notebook source
import dlt
from pyspark.sql.functions import (
    abs,
    avg,
    col,
    countDistinct,
    current_date,
    current_timestamp,
    datediff,
    expr,
    lag,
    max,
    min,
    rank,
    round,
    stddev_pop,
    sum,
    trunc,
    when,
)
from pyspark.sql.window import Window


@dlt.table(name="sales_summary", table_properties={"quality": "gold"})
def sales_summary():
    return (
        dlt.read("silver_sales")
        .groupBy(col("invoice_date").alias("sales_date"), "region", "channel")
        .agg(
            sum("quantity").alias("total_quantity"),
            sum("sales_value").alias("total_sales"),
            countDistinct("invoice_id").alias("total_orders"),
            avg("sales_value").alias("avg_order_value"),
        )
        .withColumn("total_revenue", col("total_sales"))
        .withColumn("_processed_ts", current_timestamp())
    )


@dlt.table(name="sku_performance", table_properties={"quality": "gold"})
def sku_performance():
    df = dlt.read("silver_sales")
    total_sales = df.agg(sum("sales_value").alias("overall_revenue"))
    sku_df = (
        df.groupBy("sku_id", "product_category_name")
          .agg(
              sum("quantity").alias("total_quantity"),
              sum("sales_value").alias("total_revenue"),
          )
    )
    window_spec = Window.orderBy(col("total_revenue").desc())
    return (
        sku_df.crossJoin(total_sales)
             .withColumn("revenue_share_percent", (col("total_revenue") / col("overall_revenue")) * 100)
             .withColumn("rank", rank().over(window_spec))
             .withColumn("_processed_ts", current_timestamp())
    )


@dlt.table(name="distributor_performance", table_properties={"quality": "gold"})
def distributor_performance():
    df = dlt.read("silver_sales")
    window_spec = Window.orderBy(col("total_sales").desc())
    return (
        df.groupBy("distributor_id", "region", "channel")
          .agg(
              sum("sales_value").alias("total_sales"),
              countDistinct("invoice_id").alias("order_count"),
              sum("quantity").alias("total_quantity"),
          )
          .withColumn("approx_fill_rate", col("total_quantity") / col("order_count"))
          .withColumn("rank", rank().over(window_spec))
          .withColumn("_processed_ts", current_timestamp())
    )


@dlt.table(name="inventory_snapshot", table_properties={"quality": "gold"})
def inventory_snapshot():
    return (
        dlt.read("silver_sales")
        .groupBy("sku_id", "distributor_id")
        .agg(sum("quantity").alias("estimated_stock"))
        .withColumn("stockout_flag", when(col("estimated_stock") == 0, 1).otherwise(0))
        .withColumn("overstock_flag", when(col("estimated_stock") > 100, 1).otherwise(0))
        .withColumn("_processed_ts", current_timestamp())
    )


@dlt.table(name="stock_aging", table_properties={"quality": "gold"})
def stock_aging():
    df = dlt.read("silver_sales").withColumn("stock_age_days", datediff(current_date(), col("invoice_date")))
    return (
        df.groupBy("sku_id", "distributor_id")
          .agg(
              sum("quantity").alias("qty_at_risk"),
              avg("stock_age_days").alias("avg_stock_age"),
          )
          .withColumn(
              "stock_age_bucket",
              when(col("avg_stock_age") < 30, "<30")
              .when((col("avg_stock_age") >= 30) & (col("avg_stock_age") < 60), "30-60")
              .when((col("avg_stock_age") >= 60) & (col("avg_stock_age") < 90), "60-90")
              .otherwise("90+"),
          )
          .withColumn("_processed_ts", current_timestamp())
    )


@dlt.table(name="revenue_growth", table_properties={"quality": "gold"})
def revenue_growth():
    monthly_sales = (
        dlt.read("silver_sales")
        .withColumn("sales_month", trunc("invoice_date", "MM"))
        .groupBy("sales_month", "region", "channel")
        .agg(
            sum("sales_value").alias("monthly_sales"),
            sum("quantity").alias("monthly_quantity"),
            countDistinct("invoice_id").alias("monthly_orders"),
        )
    )

    growth_window = Window.partitionBy("region", "channel").orderBy("sales_month")

    return (
        monthly_sales
        .withColumn("previous_month_sales", lag("monthly_sales").over(growth_window))
        .withColumn(
            "growth_pct",
            when(
                col("previous_month_sales").isNull() | (col("previous_month_sales") == 0),
                None,
            ).otherwise(((col("monthly_sales") - col("previous_month_sales")) / col("previous_month_sales")) * 100)
        )
        .withColumn("_processed_ts", current_timestamp())
    )


@dlt.table(name="customer_segmentation", table_properties={"quality": "gold"})
def customer_segmentation():
    df = dlt.read("silver_sales")

    customer_metrics = (
        df.groupBy("retailer_id")
        .agg(
            sum("sales_value").alias("customer_revenue"),
            countDistinct("invoice_id").alias("order_count"),
            countDistinct("product_category_name").alias("categories_purchased"),
            min("invoice_date").alias("first_purchase_date"),
            max("invoice_date").alias("last_purchase_date"),
        )
    )

    return (
        customer_metrics
        .withColumn("customer_lifespan_days", datediff(col("last_purchase_date"), col("first_purchase_date")))
        .withColumn("days_since_last_purchase", datediff(current_date(), col("last_purchase_date")))
        .withColumn(
            "customer_segment",
            when((col("customer_revenue") >= 5000) & (col("order_count") >= 5), "High Value Loyal")
            .when((col("customer_revenue") >= 2000) & (col("order_count") >= 3), "Growth Customer")
            .when(col("days_since_last_purchase") > 180, "Dormant")
            .otherwise("Transactional")
        )
        .withColumn(
            "retention_status",
            when(col("days_since_last_purchase") <= 90, "Active")
            .when(col("days_since_last_purchase") <= 180, "At Risk")
            .otherwise("Dormant")
        )
        .withColumn("_processed_ts", current_timestamp())
    )


@dlt.table(name="demand_volatility", table_properties={"quality": "gold"})
def demand_volatility():
    monthly_demand = (
        dlt.read("silver_sales")
        .withColumn("sales_month", trunc("invoice_date", "MM"))
        .groupBy("sales_month", "sku_id", "product_category_name")
        .agg(
            sum("quantity").alias("monthly_quantity"),
            sum("sales_value").alias("monthly_sales"),
        )
    )

    volatility = (
        monthly_demand
        .groupBy("sku_id", "product_category_name")
        .agg(
            avg("monthly_quantity").alias("avg_monthly_quantity"),
            stddev_pop("monthly_quantity").alias("stddev_monthly_quantity"),
            avg("monthly_sales").alias("avg_monthly_sales"),
            stddev_pop("monthly_sales").alias("stddev_monthly_sales"),
        )
    )

    return (
        volatility
        .withColumn(
            "quantity_volatility_pct",
            when(
                col("avg_monthly_quantity").isNull() | (col("avg_monthly_quantity") == 0),
                None,
            ).otherwise((col("stddev_monthly_quantity") / col("avg_monthly_quantity")) * 100)
        )
        .withColumn(
            "sales_volatility_pct",
            when(
                col("avg_monthly_sales").isNull() | (col("avg_monthly_sales") == 0),
                None,
            ).otherwise((col("stddev_monthly_sales") / col("avg_monthly_sales")) * 100)
        )
        .withColumn(
            "demand_stability_band",
            when(col("quantity_volatility_pct") <= 20, "Stable")
            .when(col("quantity_volatility_pct") <= 50, "Moderate")
            .otherwise("Volatile")
        )
        .withColumn("_processed_ts", current_timestamp())
    )


@dlt.table(name="channel_performance", table_properties={"quality": "gold"})
def channel_performance():
    return (
        dlt.read("silver_sales")
        .groupBy("channel")
        .agg(
            sum("sales_value").alias("total_sales"),
            sum("quantity").alias("total_quantity"),
            countDistinct("invoice_id").alias("total_orders"),
        )
        .withColumn("_processed_ts", current_timestamp())
    )


@dlt.table(name="region_performance", table_properties={"quality": "gold"})
def region_performance():
    return (
        dlt.read("silver_sales")
        .groupBy("region")
        .agg(
            sum("sales_value").alias("total_sales"),
            sum("quantity").alias("total_quantity"),
            countDistinct("invoice_id").alias("total_orders"),
        )
        .withColumn("_processed_ts", current_timestamp())
    )


@dlt.table(name="retailer_performance", table_properties={"quality": "gold"})
def retailer_performance():
    return (
        dlt.read("silver_sales")
        .groupBy("retailer_id", "retailer_city", "retailer_state")
        .agg(
            sum("sales_value").alias("total_sales"),
            sum("quantity").alias("total_quantity"),
            countDistinct("invoice_id").alias("total_orders"),
        )
        .withColumn("_processed_ts", current_timestamp())
    )


@dlt.table(name="city_performance", table_properties={"quality": "gold"})
def city_performance():
    return (
        dlt.read("silver_sales")
        .groupBy("retailer_city")
        .agg(
            sum("sales_value").alias("total_sales"),
            sum("quantity").alias("total_quantity"),
            countDistinct("invoice_id").alias("total_orders"),
        )
        .withColumn("_processed_ts", current_timestamp())
    )

