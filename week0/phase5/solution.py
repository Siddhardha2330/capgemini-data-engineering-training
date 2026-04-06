"""
E-Commerce Data Analysis Pipeline
Phase 5 - Advanced Analytics Tasks
"""

# ============================================================================
# SETUP: Load Required Libraries and Data
# ============================================================================

from pyspark.sql.functions import (
    col, sum, count, countDistinct, when, to_date, 
    rank, dense_rank, row_number
)
from pyspark.sql.window import Window

# Load all datasets
base_path = "/Volumes/workspace/default/phase5/"

customers = spark.read.csv(f"{base_path}olist_customers_dataset.csv", header=True, inferSchema=True)
geolocation = spark.read.csv(f"{base_path}olist_geolocation_dataset.csv", header=True, inferSchema=True)
order_items = spark.read.csv(f"{base_path}olist_order_items_dataset.csv", header=True, inferSchema=True)
payments = spark.read.csv(f"{base_path}olist_order_payments_dataset.csv", header=True, inferSchema=True)
reviews = spark.read.csv(f"{base_path}olist_order_reviews_dataset.csv", header=True, inferSchema=True)
orders = spark.read.csv(f"{base_path}olist_orders_dataset.csv", header=True, inferSchema=True)
products = spark.read.csv(f"{base_path}olist_products_dataset.csv", header=True, inferSchema=True)
sellers = spark.read.csv(f"{base_path}olist_sellers_dataset.csv", header=True, inferSchema=True)
category_translation = spark.read.csv(f"{base_path}product_category_name_translation.csv", header=True, inferSchema=True)

print("✓ Data loaded successfully")


# ============================================================================
# TASK 1: Top 3 Customers per City
# ============================================================================
# Calculate total spend per customer and rank within each city

# Join tables to get customer spending data
customer_orders = order_items \
    .join(orders, "order_id") \
    .join(customers, "customer_id")

# Aggregate spend per customer per city
customer_spend = customer_orders \
    .groupBy("customer_city", "customer_id") \
    .agg(sum("price").alias("total_spend"))

# Rank customers within each city
window_spec = Window.partitionBy("customer_city").orderBy(col("total_spend").desc())
ranked_customers = customer_spend.withColumn("rank", rank().over(window_spec))

# Get top 3 per city
task1_result = ranked_customers \
    .filter(col("rank") <= 3) \
    .select(
        col("customer_city").alias("city"),
        "customer_id",
        "total_spend",
        "rank"
    ) \
    .orderBy("city", "rank")

print(f"Task 1 Complete: {task1_result.count():,} rows")


# ============================================================================
# TASK 2: Running Total of Sales
# ============================================================================
# Calculate daily sales and cumulative running total

# Join to get sales with dates
sales_with_dates = order_items \
    .join(orders, "order_id") \
    .select(
        to_date("order_purchase_timestamp").alias("date"),
        "price"
    )

# Calculate daily sales
daily_sales = sales_with_dates \
    .groupBy("date") \
    .agg(sum("price").alias("daily_sales")) \
    .orderBy("date")

# Apply window function for running total
window_spec = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
task2_result = daily_sales \
    .withColumn("running_total", sum("daily_sales").over(window_spec)) \
    .select("date", "daily_sales", "running_total")

print(f"Task 2 Complete: {task2_result.count():,} days analyzed")


# ============================================================================
# TASK 3: Top Products per Category
# ============================================================================
# Rank top 3 products in each category by sales using DENSE_RANK

# Calculate total sales per product
product_sales = order_items \
    .groupBy("product_id") \
    .agg(sum("price").alias("total_sales"))

# Join with products to get category
product_with_category = product_sales \
    .join(products, "product_id") \
    .select("product_category_name", "product_id", "total_sales")

# Rank products within each category
window_spec = Window.partitionBy("product_category_name").orderBy(col("total_sales").desc())
ranked_products = product_with_category \
    .withColumn("rank", dense_rank().over(window_spec))

# Filter top 3 per category
task3_result = ranked_products \
    .filter(col("rank") <= 3) \
    .select(
        col("product_category_name").alias("category"),
        "product_id",
        "total_sales",
        "rank"
    ) \
    .orderBy("category", "rank")

print(f"Task 3 Complete: {task3_result.count():,} rows")


# ============================================================================
# TASK 4: Customer Lifetime Value
# ============================================================================
# Calculate total spend per customer across all orders

# Join to get customer purchases
customer_purchases = order_items \
    .join(orders, "order_id") \
    .select("customer_id", "price")

# Aggregate total spend per customer
task4_result = customer_purchases \
    .groupBy("customer_id") \
    .agg(sum("price").alias("total_spend")) \
    .orderBy(col("total_spend").desc())

print(f"Task 4 Complete: {task4_result.count():,} customers analyzed")


# ============================================================================
# TASK 5: Customer Segmentation
# ============================================================================
# Segment customers into Gold (>10000), Silver (5000-10000), Bronze (<5000)

# Calculate total spend per customer
customer_lifetime = order_items \
    .join(orders, "order_id") \
    .groupBy("customer_id") \
    .agg(sum("price").alias("total_spend"))

# Apply segmentation logic
customer_segments = customer_lifetime.withColumn(
    "segment",
    when(col("total_spend") > 10000, "Gold")
    .when((col("total_spend") >= 5000) & (col("total_spend") <= 10000), "Silver")
    .otherwise("Bronze")
).select("customer_id", "total_spend", "segment")

# Count customers per segment
segment_counts = customer_segments \
    .groupBy("segment") \
    .agg(count("customer_id").alias("customer_count")) \
    .orderBy("segment")

print(f"Task 5 Complete: Segmented {customer_segments.count():,} customers")


# ============================================================================
# TASK 6: Final Reporting Table
# ============================================================================
# Combine customer metrics: customer_id, city, total_spend, segment, total_orders

# Join all necessary tables
customer_data = order_items \
    .join(orders, "order_id") \
    .join(customers, "customer_id")

# Aggregate metrics per customer
customer_summary = customer_data \
    .groupBy("customer_id", "customer_city") \
    .agg(
        sum("price").alias("total_spend"),
        countDistinct("order_id").alias("total_orders")
    )

# Apply segmentation and format final output
task6_result = customer_summary.withColumn(
    "segment",
    when(col("total_spend") > 10000, "Gold")
    .when((col("total_spend") >= 5000) & (col("total_spend") <= 10000), "Silver")
    .otherwise("Bronze")
).select(
    "customer_id",
    col("customer_city").alias("city"),
    "total_spend",
    "segment",
    "total_orders"
).orderBy(col("total_spend").desc())

print(f"Task 6 Complete: Final report with {task6_result.count():,} customers")


