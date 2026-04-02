# Phase 1 — SQL to PySpark 

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# -----------------------------
# Initialize Spark Session
# -----------------------------
spark = SparkSession.builder.appName("CustomerAnalysis").getOrCreate()


# -----------------------------
# Create Sample Data
# -----------------------------
customer_data = [
    (1, "Ravi", "Hyderabad", 25),
    (2, "Sita", "Chennai", 32),
    (3, "Arun", "Hyderabad", 28)
]

columns = ["customer_id", "customer_name", "city", "age"]

customers_df = spark.createDataFrame(customer_data, columns)


# -----------------------------
# Display Full Dataset
# -----------------------------
print("All Customers")
customers_df.show()


# -----------------------------
# Filter: Customers from Chennai
# -----------------------------
print("Customers from Chennai")
customers_df.filter(col("city") == "Chennai").show()


# -----------------------------
# Filter: Customers with Age > 25
# -----------------------------
print("Customers with Age > 25")
customers_df.filter(col("age") > 25).show()


# -----------------------------
# Select Specific Columns
# -----------------------------
print("Customer Name and City")
customers_df.select("customer_name", "city").show()


# -----------------------------
# Aggregation: City-wise Count
# -----------------------------
print("Customer Count by City")
customers_df.groupBy("city").count().show()
