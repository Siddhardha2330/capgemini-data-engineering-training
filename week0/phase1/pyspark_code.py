from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("TestApp").getOrCreate()

customers = spark.createDataFrame([
 (1, "Ravi", "Hyderabad", 25),
 (2, "Sita", "Chennai", 32),
 (3, "Arun", "Hyderabad", 28)
], ["customer_id", "customer_name", "city", "age"])



#Show all customers
customers.show()
#Show customers from Chennai
customers.filter(col("city") == "Chennai").show()
#Show customers with age > 25
customers.filter(col("age") > 25).show()
#Show only customer_name and city
customers.select("customer_name", "city").show()
#Count customers city-wise
customers.groupBy("city").count().show()
