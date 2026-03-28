"""
Phase 3 — Starter PySpark (customers)

- Local script: `python pyspark_code.py` or `spark-submit pyspark_code.py`
- Notebook: create `spark` first, then run everything from `build_customers` downward (skip `main()`).
"""

from pyspark.sql import SparkSession


def build_customers(spark):
    """Starter DataFrame + temp view `customers`."""
    customers = spark.createDataFrame(
        [
            (1, "Ravi", "Hyderabad", 25),
            (2, "Sita", "Chennai", 32),
            (3, "Arun", "Hyderabad", 28),
        ],
        ["customer_id", "customer_name", "city", "age"],
    )
    customers.createOrReplaceTempView("customers")
    customers.show()
    return customers


def main():
    spark = SparkSession.builder.appName("phase3_customers").getOrCreate()
    try:
        build_customers(spark)

        # --- Guided exercises — uncomment and complete -------------------------
        # 1. Show all customers
        # spark.sql("SELECT * FROM customers").show()

        # 2. Show customers from Chennai
        # spark.sql("SELECT * FROM customers WHERE city = 'Chennai'").show()

        # 3. Show customers with age > 25
        # spark.sql("SELECT * FROM customers WHERE age > 25").show()

        # 4. Show only customer_name and city
        # spark.sql("SELECT customer_name, city FROM customers").show()

        # 5. Count customers city-wise
        # spark.sql("SELECT city, COUNT(*) AS cnt FROM customers GROUP BY city").show()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
