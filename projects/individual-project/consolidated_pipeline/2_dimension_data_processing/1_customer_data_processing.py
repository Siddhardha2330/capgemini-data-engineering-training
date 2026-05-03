# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

dbutils.widgets.text("catalog","fmcg","Catalog")
dbutils.widgets.text("data_source","customers","Data Source")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")
base_path=f"s3://sportsbardpp/{data_source}/*.csv"


# COMMAND ----------

df=(spark.read.format("csv").option("header",True).option("inferSchema",True).load(base_path).withColumn("read_timestamp",F.current_timestamp()).select("*",F.col("_metadata.file_name"),F.col("_metadata.file_size")))
display(df.limit(10))

# COMMAND ----------

df.write\
.format("delta") \
.option("delta.enableChangeDataFeed", "true") \
.mode("overwrite") \
.saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

df_bronze=spark.sql(f"SELECT * from {catalog}.{bronze_schema}.{data_source};")
df_bronze.show(10)

# COMMAND ----------

df_silver=df_bronze.dropDuplicates(['customer_id'])

# COMMAND ----------

df_silver =df_silver.withColumn(
"customer_name",
F.trim(F.col("customer_name")))

# COMMAND ----------

  #typos correct names
city_mapping={
'Bengaluruu': 'Bengaluru',
'Bengalore': 'Bengaluru',
'Hyderabadd': 'Hyderabad',
'Hyderbad': 'Hyderabad',
'NewDelhi': 'New Delhi',
'NewDheli': 'New Delhi',
'NewDelhee': 'New Delhi'
}
allowed =["Bengaluru", "Hyderabad", "New Delhi"]
df_silver =(
df_silver
.replace(city_mapping, subset=["city"])
.withColumn(
"city",
F.when (F.col("city").isNull(), None)
.when(F.col("city").isin(allowed), F.col("city"))
.otherwise(None)
))

# COMMAND ----------

df_silver = df_silver.withColumn(
"customer_name",
F.when (F.col("customer_name").isNull(), None)
.otherwise (F.initcap ("customer_name"))
)


# COMMAND ----------

customer_city_fix = {
# Sprintx Nutrition
789403: "New Delhi",
#Zenathlete Foods
789420: "Bengaluru",
# Primefuel Nutrition
789521: "Hyderabad",
#Recovery Lane
789603: "Hyderabad"
}
df_fix =spark.createDataFrame(
[(k, v) for k, v in customer_city_fix.items()],
["customer_id", "fixed_city"]
)

# COMMAND ----------

df_silver = (
df_silver
.join(df_fix, "customer_id", "left")
.withColumn(
"city",
F.coalesce("city", "fixed_city")
)
.drop("fixed_city")
)

# COMMAND ----------

df_silver = df_silver.withColumn("customer_id", F.col("customer_id").cast("string"))
print(df_silver.printSchema())

# COMMAND ----------

df_silver = (
    df_silver
    # Build final customer column: "CustomerName-City" or "CustomerName-Unknown"
    .withColumn(
        "customer",
        F.concat_ws("-", "customer_name", F.coalesce(F.col("city"), F.lit("Unknown")))
    )
    # Static attributes aligned with parent data model
    .withColumn("market", F.lit("India"))
    .withColumn("platform", F.lit("Sports Bar"))
    .withColumn("channel", F.lit("Acquisition"))
)

# COMMAND ----------

df_silver.write\
.format("delta") \
.option("delta.enableChangeDataFeed", "true") \
.option("mergeSchema", "true") \
.mode("overwrite") \
.saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

df_silver =spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")
#take req cols only
# "customer_id, customer_name, city, read timestamp, file_name, file_size, customer, market, platform, channel"
df_gold =df_silver.select("customer_id", "customer_name", "city", "customer", "market", "platform", "channel")

# COMMAND ----------

df_gold.write\
.format("delta") \
.option("delta.enableChangeDataFeed", "true") \
.mode("overwrite") \
.saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_customers")
df_child_customers = spark.table("fmcg.gold.sb_dim_customers").select(
F.col("customer_id").alias("customer_code"),
"customer",
"market",
"platform",
"channel"

)

# COMMAND ----------

delta_table.alias("target").merge(
source=df_child_customers.alias("source"),
condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
