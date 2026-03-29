from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round, avg, count, desc

spark = SparkSession.builder.appName("GuidedExercises").getOrCreate()

customers = spark.read.option("header","true").csv("/samples/customers.csv")
sales = spark.read.option("header","true").csv("/samples/sales.csv")

customers = customers.dropna(subset=["customer_id"])
sales = sales.dropna(subset=["customer_id"])

customers = customers.withColumn("customer_id", col("customer_id").cast("int"))
sales = sales.withColumn("customer_id", col("customer_id").cast("int"))
sales = sales.withColumn("total_amount", col("total_amount").cast("double"))

k = customers.join(sales, on="customer_id", how="inner")



# 1.Total order amount for each customer
k.groupBy("customer_id").agg(round(sum("total_amount"), 2).alias("total_spent")).show()


# 2.Top 3 customers by total spend
k.groupBy("customer_id").agg(round(sum("total_amount"), 2).alias("total_spent")).orderBy(desc("total_spent")).limit(3).show()


# 3.Customers with no orders
customers.join(sales, on="customer_id", how="left_anti").show()


# 4. City-wise total revenue
k.groupBy("city") \
 .agg(round(sum("total_amount"), 2).alias("total_revenue")) \
 .show()


# 5. Average order amount per customer
k.groupBy("customer_id").agg(round(avg("total_amount"), 2).alias("avg_order_amount")).show()



# 6. Customers with more than one order
k.groupBy("customer_id").agg(count("sale_id").alias("order_count")) \
 .filter(col("order_count") > 1).show()


# 7. Sort customers by total spend descending
k.groupBy("customer_id").agg(round(sum("total_amount"), 2).alias("total_spent")) \
 .orderBy(desc("total_spent")) \
 .show()
