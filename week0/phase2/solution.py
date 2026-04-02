from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round, avg, count, desc

# -----------------------------
# 🔹 Initialize Spark
# -----------------------------
spark = SparkSession.builder.appName("CustomerAnalytics").getOrCreate()

# -----------------------------
# 🔹 Load Data
# -----------------------------
customers = spark.read.option("header", "true").csv("/samples/customers.csv")
sales = spark.read.option("header", "true").csv("/samples/sales.csv")

# -----------------------------
# 🔹 Data Cleaning
# -----------------------------
customers = customers.dropna(subset=["customer_id"])
sales = sales.dropna(subset=["customer_id"])

customers = customers.withColumn("customer_id", col("customer_id").cast("int"))

sales = sales.withColumn("customer_id", col("customer_id").cast("int")) \
             .withColumn("total_amount", col("total_amount").cast("double"))

# -----------------------------
# 🔹 Join
# -----------------------------
df = customers.join(sales, on="customer_id", how="inner")

# -----------------------------
# 🔹 Reusable Aggregation
# -----------------------------
total_spent_df = df.groupBy("customer_id") \
                   .agg(round(sum("total_amount"), 2).alias("total_spent"))

# -----------------------------
# 🔹 Queries / Insights
# -----------------------------

# 1. Total order amount per customer
print("Total order amount per customer")
total_spent_df.show()

# 2. Top 3 customers by total spend
print("Top 3 Customers")
total_spent_df.orderBy(desc("total_spent")).limit(3).show()

# 3. Customers with no orders
print("Customers With No Orders")
customers.join(sales, on="customer_id", how="left_anti").show()

# 4. City-wise total revenue
print("City-wise Revenue")
df.groupBy("city") \
  .agg(round(sum("total_amount"), 2).alias("total_revenue")) \
  .show()

# 5. Average order amount per customer
print("Average Order Amount")
df.groupBy("customer_id") \
  .agg(round(avg("total_amount"), 2).alias("avg_order_amount")) \
  .show()

# 6. Customers with more than one order
print("Repeat Customers")
df.groupBy("customer_id") \
  .agg(count("sale_id").alias("order_count")) \
  .filter(col("order_count") > 1) \
  .show()

# 7. Sorted customers by total spend
print("Customers Sorted by Spend")
total_spent_df.orderBy(desc("total_spent")).show()
