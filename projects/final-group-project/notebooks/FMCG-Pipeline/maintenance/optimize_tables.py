# Databricks notebook source
# MAGIC %md
# MAGIC ## Delta Optimization and ZORDER
# MAGIC
# MAGIC This notebook applies storage optimization to the main analytical tables.
# MAGIC It is intended to be run after major pipeline loads or on a schedule.

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# Main Silver fact table used by most downstream Gold logic
spark.sql(f"""
ALTER TABLE {catalog}.silver.silver_sales
SET TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
)
""")

# COMMAND ----------

spark.sql(f"OPTIMIZE {catalog}.silver.silver_sales ZORDER BY (sku_id, distributor_id)")

# COMMAND ----------

# Gold tables commonly filtered by date or product/distributor dimensions
spark.sql(f"OPTIMIZE {catalog}.gold.sales_summary ZORDER BY (sales_date, region, channel)")
spark.sql(f"OPTIMIZE {catalog}.gold.sku_performance ZORDER BY (sku_id)")
spark.sql(f"OPTIMIZE {catalog}.gold.distributor_performance ZORDER BY (distributor_id, region, channel)")
spark.sql(f"OPTIMIZE {catalog}.gold.inventory_snapshot ZORDER BY (sku_id, distributor_id)")
spark.sql(f"OPTIMIZE {catalog}.gold.stock_aging ZORDER BY (sku_id, distributor_id)")

# COMMAND ----------

print("Optimization completed for Silver and key Gold tables.")

