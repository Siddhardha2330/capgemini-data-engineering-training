# Phase 4A — Bucketing & Segmentation 

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when
from pyspark.ml.feature import Bucketizer
from pyspark.sql.window import Window
from pyspark.sql.functions import percent_rank

# -----------------------------
# Initialize Spark
# -----------------------------
spark = SparkSession.builder.appName("Phase4A_Segmentation").getOrCreate()

# -----------------------------
# Load Data
# -----------------------------
customers_df = spark.read.option("header", "true").csv("/samples/customers.csv")
sales_df = spark.read.option("header", "true").csv("/samples/sales.csv")

# -----------------------------
# Data Preparation
# -----------------------------
customers_df = customers_df.withColumn("customer_id", col("customer_id").cast("int"))

sales_df = sales_df.withColumn("customer_id", col("customer_id").cast("int")) \
                   .withColumn("total_amount", col("total_amount").cast("double"))

df = customers_df.join(sales_df, "customer_id")

# Total spend per customer
customer_spend_df = df.groupBy("customer_id") \
    .agg(sum("total_amount").alias("total_spend"))

# -----------------------------
# Task 1: Conditional Segmentation
# -----------------------------
segmented_df = customer_spend_df.withColumn(
    "segment",
    when(col("total_spend") > 10000, "Gold")
    .when((col("total_spend") >= 5000) & (col("total_spend") <= 10000), "Silver")
    .otherwise("Bronze")
)

print("Conditional Segmentation")
segmented_df.show()

# -----------------------------
# Task 2: Group by Segment
# -----------------------------
print("Customer Count by Segment")
segmented_df.groupBy("segment").count().show()

# -----------------------------
# Task 3: Quantile-Based Segmentation
# -----------------------------
quantiles = customer_spend_df.approxQuantile("total_spend", [0.33, 0.66], 0)

q1 = quantiles[0]
q2 = quantiles[1]

quantile_df = customer_spend_df.withColumn(
    "segment",
    when(col("total_spend") <= q1, "Bronze")
    .when((col("total_spend") > q1) & (col("total_spend") <= q2), "Silver")
    .otherwise("Gold")
)

print("Quantile Segmentation")
quantile_df.show()

# -----------------------------
# Task 4: Bucketizer (MLlib)
# -----------------------------
splits = [-float("inf"), 5000, 10000, float("inf")]

bucketizer = Bucketizer(
    splits=splits,
    inputCol="total_spend",
    outputCol="bucket"
)

bucket_df = bucketizer.transform(customer_spend_df)

print("Bucketizer Output")
bucket_df.show()

# -----------------------------
# Task 5: Window-Based Ranking
# -----------------------------
window = Window.orderBy("total_spend")

rank_df = customer_spend_df.withColumn(
    "rank_pct",
    percent_rank().over(window)
)

print("Window Ranking")
rank_df.show()
