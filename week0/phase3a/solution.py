from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark Session
spark = SparkSession.builder.appName("Phase3A_DataCleaning").getOrCreate()

# -----------------------------
# Step 0 — Setup Data
# -----------------------------
data = [
    (1, "Ravi", "Hyderabad", 25),
    (2, None, "Chennai", 32),
    (None, "Arun", "Hyderabad", 28),
    (4, "Meena", None, 30),
    (4, "Meena", None, 30),
    (5, "John", "Bangalore", -5)
]

columns = ["customer_id", "name", "city", "age"]

df = spark.createDataFrame(data, columns)

print("Initial Data:")
df.show()

# -----------------------------
# Step 1 — Identify Data Issues
# -----------------------------

# Total rows
print("Total Rows:", df.count())

# Null counts
null_counts = df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df.columns
])
print("Null Values per Column:")
null_counts.show()

# Duplicate rows
duplicate_count = df.count() - df.dropDuplicates().count()
print("Duplicate Rows:", duplicate_count)

# Invalid age
print("Invalid Age Rows:")
df.filter(F.col("age") <= 0).show()

# -----------------------------
# Step 2 — Data Cleaning
# -----------------------------

before_count = df.count()

# Remove null customer_id
df_clean = df.filter(F.col("customer_id").isNotNull())

# Fill missing values
df_clean = df_clean.fillna({
    "name": "Unknown",
    "city": "Unknown"
})

# Remove duplicates
df_clean = df_clean.dropDuplicates()

# Remove invalid age
df_clean = df_clean.filter(F.col("age") > 0)

print("Cleaned Data:")
df_clean.show()

# -----------------------------
# Step 3 — Validation
# -----------------------------

after_count = df_clean.count()

print("Before Cleaning:", before_count)
print("After Cleaning:", after_count)
print("Removed Rows:", before_count - after_count)

# -----------------------------
# Step 4 — Aggregation
# -----------------------------

print("Customers per City:")
df_clean.groupBy("city").count().show()
