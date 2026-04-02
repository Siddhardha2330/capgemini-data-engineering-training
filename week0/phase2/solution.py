# Phase 2 — Customer Analytics using PySpark 

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round, avg, count, desc


# -----------------------------
# Initialize Spark Session
# -----------------------------
spark = SparkSession.builder.appName("CustomerAnalytics").getOrCreate()


# -----------------------------
# Load Data
# -----------------------------
customers_df = spark.read.option("header", "true").csv("/samples/customers.csv")
sales_df = spark.read.option("header", "true").csv("/samples/sales.csv")


# -----------------------------
# Data Cleaning
# -----------------------------
# Remove rows with null customer_id
customers_df = customers_df.dropna(subset=["customer_id"])
sales_df = sales_df.dropna(subset=["customer_id"])

# Cast columns to correct data types
customers_df = customers_df.withColumn("customer_id", col("customer_id").cast("int"))

sales_df = sales_df.withColumn("customer_id", col("customer_id").cast("int")) \
                   .withColumn("total_amount", col("total_amount").cast("double"))


# -----------------------------
# Join Data
# -----------------------------
joined_df = customers_df.join(sales_df, on="customer_id", how="inner")


# -----------------------------
# Reusable Aggregations
# -----------------------------
total_spent_df = joined_df.groupBy("customer_id") \
    .agg(round(sum("total_amount"), 2).alias("total_spent"))


# -----------------------------
# Queries / Insights
# -----------------------------

# 1. Total order amount per customer
print("Total Order Amount per Customer")
total_spent_df.show()

# 2. Top 3 customers by total spend
print("Top 3 Customers by Total Spend")
total_spent_df.orderBy(desc("total_spent")).limit(3).show()

# 3. Customers with no orders
print("Customers with No Orders")
customers_df.join(sales_df, on="customer_id", how="left_anti").show()

# 4. City-wise total revenue
print("City-wise Total Revenue")
joined_df.groupBy("city") \
    .agg(round(sum("total_amount"), 2).alias("total_revenue")) \
    .show()

# 5. Average order amount per customer
print("Average Order Amount per Customer")
joined_df.groupBy("customer_id") \
    .agg(round(avg("total_amount"), 2).alias("avg_order_amount")) \
    .show()

# 6. Customers with more than one order
print("Customers with More Than One Order")
joined_df.groupBy("customer_id") \
    .agg(count("sale_id").alias("order_count")) \
    .filter(col("order_count") > 1) \
    .show()

# 7. Customers sorted by total spend
print("Customers Sorted by Total Spend")
total_spent_df.orderBy(desc("total_spent")).show()
